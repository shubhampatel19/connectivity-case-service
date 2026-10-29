package cases

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

//go:embed default_rules.json
var defaultRules []byte

// Rule decides when alerts of one code become a support case. An alert no
// rule matches is recorded and opens nothing; that is also what happens to
// codes from agents newer than these rules.
type Rule struct {
	ID       string         `json:"id"`
	Code     wire.AlertCode `json:"code"`
	Title    string         `json:"title"`
	Severity string         `json:"severity"` // case severity: critical, major or minor

	// MinValue: open a case only once an alert's value reaches this. For
	// disk wear, the value is the reallocated sector count.
	MinValue *float64 `json:"min_value,omitempty"`

	// Threshold: open a case only when the device has raised this many
	// alerts of the code within the window. Filters one stray reading from a
	// fault that keeps coming back.
	Threshold *Threshold `json:"threshold,omitempty"`

	// Storm: when this many devices at one site raise the code within the
	// window, open one site case instead of a case per device. Many devices
	// failing together usually share one cause (cooling, power) and want one
	// engineer, not forty tickets.
	Storm *Storm `json:"storm,omitempty"`
}

type Threshold struct {
	Count  int      `json:"count"`
	Window Duration `json:"window"`
}

type Storm struct {
	Devices int      `json:"devices"`
	Window  Duration `json:"window"`
}

// Duration reads "10m"-style strings from JSON.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"10m\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// Rules is a validated rule set, indexed by alert code.
type Rules struct {
	list   []Rule
	byCode map[wire.AlertCode]*Rule
}

// LoadRules reads rules from path, or the built-in defaults when path is "".
func LoadRules(path string) (*Rules, error) {
	data := defaultRules
	if path != "" {
		var err error
		if data, err = os.ReadFile(path); err != nil {
			return nil, fmt.Errorf("read rules: %w", err)
		}
	}
	r, err := ParseRules(data)
	if err != nil && path != "" {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, err
}

// ParseRules parses and validates a rule file. Every problem is reported at
// once, and unknown fields are errors: a misspelled "treshold" that silently
// did nothing would open cases nobody expected.
func ParseRules(data []byte) (*Rules, error) {
	var file struct {
		Rules []Rule `json:"rules"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}

	var errs []error
	r := &Rules{list: file.Rules, byCode: map[wire.AlertCode]*Rule{}}
	ids := map[string]bool{}
	for i := range r.list {
		rule := &r.list[i]
		bad := func(format string, args ...any) {
			errs = append(errs, fmt.Errorf("rule %d (%s): %s", i, rule.ID, fmt.Sprintf(format, args...)))
		}
		switch {
		case rule.ID == "":
			bad("id is required")
		case ids[rule.ID]:
			bad("duplicate id")
		}
		ids[rule.ID] = true
		if !rule.Code.Valid() {
			bad("code %q is not a valid alert code", rule.Code)
		} else if other := r.byCode[rule.Code]; other != nil {
			bad("code %s is already handled by rule %s", rule.Code, other.ID)
		} else {
			r.byCode[rule.Code] = rule
		}
		if rule.Title == "" {
			bad("title is required")
		}
		switch rule.Severity {
		case "critical", "major", "minor":
		default:
			bad("severity must be critical, major or minor, not %q", rule.Severity)
		}
		if t := rule.Threshold; t != nil && (t.Count < 1 || t.Window.Duration <= 0) {
			bad("threshold needs a count of at least 1 and a positive window")
		}
		if s := rule.Storm; s != nil && (s.Devices < 2 || s.Window.Duration <= 0) {
			bad("storm needs at least 2 devices and a positive window")
		}
	}
	if len(r.list) == 0 {
		errs = append(errs, errors.New("no rules"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return r, nil
}

// Match returns the rule for an alert code, or nil.
func (r *Rules) Match(code wire.AlertCode) *Rule { return r.byCode[code] }

func (r *Rules) List() []Rule { return r.list }
