package cases

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// caseStore is what the API needs from the database; tests can fake it.
type caseStore interface {
	ListCases(ctx context.Context, f Filter) ([]Case, error)
	GetCase(ctx context.Context, id int64) (CaseDetail, error)
	ResolveCase(ctx context.Context, id int64, resolution string) (int, error)
	Ping(ctx context.Context) error
}

// API is the support team's view of cases. It has no authentication: it is
// an internal API, reachable only inside the platform's network.
type API struct {
	store caseStore
	rules *Rules
	log   *slog.Logger
	m     *Metrics
}

func NewAPI(st caseStore, rules *Rules, log *slog.Logger, m *Metrics) *API {
	return &API{store: st, rules: rules, log: log, m: m}
}

// Handler wires the routes. metricsHandler may be nil (tests).
func (a *API) Handler(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/cases", a.route("list", a.list))
	mux.Handle("GET /v1/cases/{id}", a.route("get", a.get))
	mux.Handle("POST /v1/cases/{id}/resolve", a.route("resolve", a.resolve))
	mux.Handle("GET /v1/rules", a.route("rules", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"rules": a.rules.List()})
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.store.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unreachable")
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	}
	return mux
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (a *API) route(name string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		a.m.httpRequests.WithLabelValues(name, strconv.Itoa(rec.status)).Inc()
	})
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := Filter{Status: q.Get("status"), SiteID: q.Get("site"), DeviceID: q.Get("device"), RuleID: q.Get("rule"), Limit: 50}
	switch f.Status {
	case "", "open", "resolved", "all":
	default:
		writeError(w, http.StatusBadRequest, "status must be open, resolved or all")
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		f.Limit = n
	}
	cases, err := a.store.ListCases(r.Context(), f)
	if err != nil {
		a.fail(w, "list cases", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cases": cases})
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	d, err := a.store.GetCase(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "no such case")
	case err != nil:
		a.fail(w, "get case", err)
	default:
		writeJSON(w, http.StatusOK, d)
	}
}

func (a *API) resolve(w http.ResponseWriter, r *http.Request) {
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	var body struct {
		Resolution string `json:"resolution"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "malformed JSON: "+err.Error())
		return
	}
	if body.Resolution == "" {
		body.Resolution = "resolved"
	}
	children, err := a.store.ResolveCase(r.Context(), id, body.Resolution)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "no such case")
	case errors.Is(err, ErrAlreadyResolved):
		writeError(w, http.StatusConflict, "case is already resolved")
	case err != nil:
		a.fail(w, "resolve case", err)
	default:
		a.m.casesResolved.Inc()
		a.log.Info("case resolved", "case_id", id, "children", children, "resolution", body.Resolution)
		writeJSON(w, http.StatusOK, map[string]any{"case_id": id, "status": "resolved", "children_resolved": children})
	}
}

func caseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "case id must be a positive integer")
		return 0, false
	}
	return id, true
}

func (a *API) fail(w http.ResponseWriter, what string, err error) {
	a.log.Error(what+" failed", "err", err)
	writeError(w, http.StatusServiceUnavailable, "temporarily unavailable")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
