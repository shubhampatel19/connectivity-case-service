package cases

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

func call(h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, path, bytes.NewReader(b)))
	return rr
}

func TestAPIRejectsBadInputWithoutTouchingTheDatabase(t *testing.T) {
	h := NewAPI(nil, testRules(t), quiet, NewMetrics(prometheus.NewRegistry())).Handler(nil)
	for _, path := range []string{"/v1/cases?status=closed", "/v1/cases?limit=0", "/v1/cases?limit=501", "/v1/cases/abc", "/v1/cases/-3"} {
		if rr := call(h, http.MethodGet, path, nil); rr.Code != http.StatusBadRequest {
			t.Errorf("GET %s: %d, want 400", path, rr.Code)
		}
	}
	if rr := call(h, http.MethodGet, "/v1/rules", nil); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"thermal-critical"`)) {
		t.Errorf("GET /v1/rules: %d %s", rr.Code, rr.Body)
	}
}

func TestAPIListGetResolve(t *testing.T) {
	if os.Getenv("CASES_TEST_DATABASE_URL") == "" {
		t.Skip("CASES_TEST_DATABASE_URL not set")
	}
	st, r := testStore(t), testRules(t)
	h := NewAPI(st, r, quiet, NewMetrics(prometheus.NewRegistry())).Handler(nil)
	opened, err := st.ProcessAlert(context.Background(), alert("d1", "s1", wire.AlertThermalCritical, 93, t0), r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ProcessAlert(context.Background(), alert("d2", "s2", wire.AlertThermalCritical, 93, t0), r); err != nil {
		t.Fatal(err)
	}

	var list struct{ Cases []Case }
	rr := call(h, http.MethodGet, "/v1/cases?site=s1", nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil || rr.Code != 200 || len(list.Cases) != 1 || list.Cases[0].DeviceID != "d1" {
		t.Fatalf("list by site: %d %s", rr.Code, rr.Body)
	}

	var detail CaseDetail
	rr = call(h, http.MethodGet, "/v1/cases/"+itoa(opened.CaseID), nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil || rr.Code != 200 || len(detail.Alerts) != 1 || detail.Logs != LogsNone {
		t.Fatalf("get: %d %s", rr.Code, rr.Body)
	}
	if rr := call(h, http.MethodGet, "/v1/cases/999999", nil); rr.Code != 404 {
		t.Fatalf("missing case: %d", rr.Code)
	}

	if rr := call(h, http.MethodPost, "/v1/cases/"+itoa(opened.CaseID)+"/resolve", map[string]string{"resolution": "fan replaced"}); rr.Code != 200 {
		t.Fatalf("resolve: %d %s", rr.Code, rr.Body)
	}
	if rr := call(h, http.MethodPost, "/v1/cases/"+itoa(opened.CaseID)+"/resolve", nil); rr.Code != http.StatusConflict {
		t.Fatalf("resolve again: %d, want 409", rr.Code)
	}
	rr = call(h, http.MethodGet, "/v1/cases?status=resolved", nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil || len(list.Cases) != 1 || list.Cases[0].Resolution != "fan replaced" {
		t.Fatalf("resolved list: %s", rr.Body)
	}
	if rr := call(h, http.MethodGet, "/readyz", nil); rr.Code != 200 {
		t.Fatalf("readyz with a healthy database: %d", rr.Code)
	}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }
