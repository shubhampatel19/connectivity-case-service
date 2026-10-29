package cases

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	alerts         *prometheus.CounterVec
	alertsRejected *prometheus.CounterVec
	casesOpened    *prometheus.CounterVec
	casesResolved  prometheus.Counter
	adopted        prometheus.Counter
	bundles        *prometheus.CounterVec
	decisionDelay  prometheus.Histogram
	retries        *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	commitErrors   prometheus.Counter
	httpRequests   *prometheus.CounterVec

	openCases          *prometheus.GaugeVec
	waitingForLogs     prometheus.Gauge
	oldestWaiting      prometheus.Gauge
	statsErrors        prometheus.Counter
	statsLastSuccessTS prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		alerts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_case_alerts_total",
			Help: "Alerts processed, by outcome (opened, attached, below_threshold, no_rule, duplicate).",
		}, []string{"outcome"}),
		alertsRejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_case_alerts_rejected_total",
			Help: "Alert records skipped as unprocessable, by reason (bad_json, invalid).",
		}, []string{"reason"}),
		casesOpened: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_case_cases_opened_total",
			Help: "Cases opened, by rule and scope (device or site).",
		}, []string{"rule", "scope"}),
		casesResolved: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_case_cases_resolved_total",
			Help: "Cases resolved through the API (child cases resolved with a parent are not counted).",
		}),
		adopted: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_case_device_cases_adopted_total",
			Help: "Open device cases that became children of a new site case.",
		}),
		bundles: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_case_bundles_indexed_total",
			Help: "Bundle events consumed, by result (stored, duplicate).",
		}, []string{"result"}),
		decisionDelay: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_case_alert_delay_seconds",
			Help:    "Time from the ingest API accepting an alert to the case service deciding on it.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}),
		retries: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_case_retries_total",
			Help: "Records retried after a transient failure (usually the database), by topic.",
		}, []string{"topic"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "connectivity_case_record_duration_seconds",
			Help:    "Time to process one record, including its database transaction.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"topic"}),
		commitErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_case_commit_errors_total",
			Help: "Failed Kafka offset commits. Records since the last commit are replayed, and deduplicated.",
		}),
		httpRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_case_http_requests_total",
			Help: "Case API requests, by route and status code.",
		}, []string{"route", "code"}),

		openCases: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "connectivity_case_open_cases",
			Help: "Open cases, by rule and scope.",
		}, []string{"rule", "scope"}),
		waitingForLogs: f.NewGauge(prometheus.GaugeOpts{
			Name: "connectivity_case_waiting_for_logs",
			Help: "Open cases whose alerts named a log bundle that hasn't arrived.",
		}),
		oldestWaiting: f.NewGauge(prometheus.GaugeOpts{
			Name: "connectivity_case_waiting_for_logs_oldest_seconds",
			Help: "Age of the oldest open case still waiting for its logs.",
		}),
		statsErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_case_stats_errors_total",
			Help: "Failed refreshes of the case gauges. While failing, the gauges keep their last values.",
		}),
		statsLastSuccessTS: f.NewGauge(prometheus.GaugeOpts{
			Name: "connectivity_case_stats_last_success_timestamp_seconds",
			Help: "When the case gauges were last refreshed. Stale gauges look exactly like healthy ones without this.",
		}),
	}
	for _, o := range []Outcome{OutcomeOpened, OutcomeAttached, OutcomeBelowThreshold, OutcomeNoRule, OutcomeDuplicate} {
		m.alerts.WithLabelValues(string(o))
	}
	for _, r := range []string{"bad_json", "invalid"} {
		m.alertsRejected.WithLabelValues(r)
	}
	return m
}
