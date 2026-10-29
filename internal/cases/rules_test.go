package cases

import (
	"strings"
	"testing"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

func TestDefaultRulesLoadAndCoverEveryAgentCode(t *testing.T) {
	r, err := LoadRules("")
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []wire.AlertCode{wire.AlertThermalCritical, wire.AlertPSURail, wire.AlertDiskReallocated} {
		if r.Match(code) == nil {
			t.Errorf("no default rule for %s, so it could never open a case", code)
		}
	}
	if r.Match("fan.stalled") != nil {
		t.Error("an unknown code must not match")
	}
	if th := r.Match(wire.AlertPSURail).Threshold; th == nil || th.Count != 3 || th.Window.Duration != 15*time.Minute {
		t.Errorf("PSU threshold not parsed: %+v", th)
	}
}

func TestParseRulesReportsEveryProblem(t *testing.T) {
	_, err := ParseRules([]byte(`{"rules": [
		{"id": "a", "code": "thermal.critical", "title": "t", "severity": "critical"},
		{"id": "a", "code": "thermal.critical", "title": "", "severity": "urgent", "threshold": {"count": 0, "window": "5m"}},
		{"id": "", "code": "nodot", "title": "t", "severity": "minor", "storm": {"devices": 1, "window": "5m"}}
	]}`))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"duplicate id", "already handled by rule a", "title is required", "severity must be", "threshold needs", "id is required", "not a valid alert code", "storm needs"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q:\n%v", want, err)
		}
	}
}

func TestParseRulesRejectsTyposAndBadDurations(t *testing.T) {
	for name, doc := range map[string]string{
		"misspelled field": `{"rules": [{"id": "a", "code": "x.y", "title": "t", "severity": "minor", "treshold": {"count": 2, "window": "5m"}}]}`,
		"bad duration":     `{"rules": [{"id": "a", "code": "x.y", "title": "t", "severity": "minor", "threshold": {"count": 2, "window": "five minutes"}}]}`,
		"numeric duration": `{"rules": [{"id": "a", "code": "x.y", "title": "t", "severity": "minor", "threshold": {"count": 2, "window": 300}}]}`,
		"empty":            `{"rules": []}`,
	} {
		if _, err := ParseRules([]byte(doc)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
