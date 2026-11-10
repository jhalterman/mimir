// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.yaml.in/yaml/v3"
)

// RuntimeConfig holds everything that can change while the fakes run. It's loaded from a YAML file
// (re-read periodically) and can be replaced over HTTP, so a sweep doesn't need a redeploy.
type RuntimeConfig struct {
	Sizes   SizeConfig    `yaml:"sizes" json:"sizes"`
	Latency LatencyConfig `yaml:"latency" json:"latency"`
	Errors  ErrorConfig   `yaml:"errors" json:"errors"`
	Labels  []LabelSpec   `yaml:"labels" json:"labels"`
}

// SizeConfig controls how many series a selector returns.
type SizeConfig struct {
	// SeriesQuantiles is the distribution of series returned by one selector across the whole cell
	// (all partitions it hits, one replica), as (quantile, series) points interpolated in log space.
	// A selector's position in the distribution comes from a hash of its normalized text, so every
	// partition and every repeat agrees on its size.
	SeriesQuantiles []QuantilePoint `yaml:"series_quantiles" json:"series_quantiles"`
	// TableFile optionally maps normalized selector text to cell-wide series for one replica (JSON,
	// gzipped if it ends in .gz; build it with "fake-ingester sizetable"). It wins over the
	// distribution for selectors it lists.
	TableFile string `yaml:"table_file" json:"table_file"`
	// WriteAmplification is the number of replicas a heavy read-tee copy (matchers ending
	// "(?:_amp[0-9]+)?") spans.
	WriteAmplification int `yaml:"write_amplification" json:"write_amplification"`
	// MaxSeriesPerResponse caps the series one fake returns for one request.
	MaxSeriesPerResponse int `yaml:"max_series_per_response" json:"max_series_per_response"`
	// ScrapeInterval and SamplesPerChunk shape the chunks returned for each series.
	ScrapeInterval  time.Duration `yaml:"scrape_interval" json:"scrape_interval"`
	SamplesPerChunk int           `yaml:"samples_per_chunk" json:"samples_per_chunk"`
	// QueryIngestersWithin bounds how far back fakes return data, like real ingesters' head.
	QueryIngestersWithin time.Duration `yaml:"query_ingesters_within" json:"query_ingesters_within"`
	// NumMetrics is the size of the synthetic metric-name universe for label APIs.
	NumMetrics int `yaml:"num_metrics" json:"num_metrics"`
}

// QuantilePoint is one point of an empirical distribution.
type QuantilePoint struct {
	Q     float64 `yaml:"q" json:"q"`
	Value float64 `yaml:"value" json:"value"`
}

// LabelSpec models one label's values for the label APIs. A shared label returns all its values from
// every partition (like job). A non-shared label's values are sharded across partitions (like pod).
type LabelSpec struct {
	Name   string `yaml:"name" json:"name"`
	Values int    `yaml:"values" json:"values"`
	Shared bool   `yaml:"shared" json:"shared"`
}

// LatencyConfig shapes how long each request takes.
// latency = (base(RPC, u) + per-series and per-chunk cost) × instance slowness × load inflation.
type LatencyConfig struct {
	// Base latency per RPC, as quantile points. RPCs not listed use "default".
	Base map[string][]QuantilePoint `yaml:"base" json:"base"`
	// PerSeries and PerChunk add data-proportional cost.
	PerSeries time.Duration `yaml:"per_series" json:"per_series"`
	PerChunk  time.Duration `yaml:"per_chunk" json:"per_chunk"`
	// Correlation (0–1) between partitions of the same request's latency quantile. 1 means an
	// expensive request is expensive everywhere; 0 draws each partition independently.
	Correlation float64 `yaml:"correlation" json:"correlation"`

	// SlowInstanceFraction of fakes are persistently slower by SlowInstanceFactor.
	SlowInstanceFraction float64 `yaml:"slow_instance_fraction" json:"slow_instance_fraction"`
	SlowInstanceFactor   float64 `yaml:"slow_instance_factor" json:"slow_instance_factor"`
	// Every SlowWindowPeriod, each fake has one SlowWindowDuration window (phase from a hash of its
	// ID) where latency is multiplied by SlowWindowFactor, mimicking head compaction.
	SlowWindowPeriod   time.Duration `yaml:"slow_window_period" json:"slow_window_period"`
	SlowWindowDuration time.Duration `yaml:"slow_window_duration" json:"slow_window_duration"`
	SlowWindowFactor   float64       `yaml:"slow_window_factor" json:"slow_window_factor"`

	// Load: above InflightKnee requests in flight on a fake, a growing share of requests get an
	// extra exponential delay (mean TailDelayMean). The median stays put and the tail grows, which
	// is what real ingesters showed (step 3). Above InflightLimit, requests are rejected. 0 disables.
	InflightKnee  int           `yaml:"inflight_knee" json:"inflight_knee"`
	TailSlope     float64       `yaml:"tail_slope" json:"tail_slope"`
	TailDelayMean time.Duration `yaml:"tail_delay_mean" json:"tail_delay_mean"`
	InflightLimit int           `yaml:"inflight_limit" json:"inflight_limit"`

	// QueryStream splits its delay: FirstBatchShare before the first series batch, SeriesShare
	// across the remaining series batches, and the rest across chunk batches.
	FirstBatchShare float64 `yaml:"first_batch_share" json:"first_batch_share"`
	SeriesShare     float64 `yaml:"series_share" json:"series_share"`
}

// ErrorConfig injects failures.
type ErrorConfig struct {
	// Rate per RPC ("default" for the rest) of requests failing with Code.
	Rate map[string]float64 `yaml:"rate" json:"rate"`
	Code string             `yaml:"code" json:"code"`
	// UnhealthyFraction of fakes (chosen by hash of ID) fail health checks and every RPC.
	UnhealthyFraction float64 `yaml:"unhealthy_fraction" json:"unhealthy_fraction"`
	// UnhealthyInstances lists fakes by ID to make unhealthy, on top of the fraction.
	UnhealthyInstances []string `yaml:"unhealthy_instances" json:"unhealthy_instances"`
}

// DefaultRuntimeConfig is calibrated from the step 1 and step 3 measurements in
// fanout-load-test-plan.md. Override any part of it in the runtime config file.
func DefaultRuntimeConfig() RuntimeConfig {
	ms := func(v float64) float64 { return v / 1000 }
	return RuntimeConfig{
		Sizes: SizeConfig{
			// Series fetched per query (step 1): p50 1–2, p99 6k–11k, p99.9 300k–550k.
			SeriesQuantiles: []QuantilePoint{
				{Q: 0, Value: 0}, {Q: 0.3, Value: 0}, {Q: 0.5, Value: 2}, {Q: 0.9, Value: 300},
				{Q: 0.99, Value: 8000}, {Q: 0.999, Value: 400000}, {Q: 1, Value: 2000000},
			},
			WriteAmplification:   1,
			MaxSeriesPerResponse: 1000000,
			ScrapeInterval:       15 * time.Second,
			SamplesPerChunk:      120,
			QueryIngestersWithin: 13 * time.Hour,
			NumMetrics:           20000,
		},
		Latency: LatencyConfig{
			// Ingester QueryStream (step 1 and ops): p50 ≈ 0.2 ms, p99 3–100 ms, p99.9 0.16–0.3 s.
			Base: map[string][]QuantilePoint{
				"default": {
					{Q: 0, Value: ms(0.08)}, {Q: 0.5, Value: ms(0.22)}, {Q: 0.9, Value: ms(1)},
					{Q: 0.99, Value: ms(100)}, {Q: 0.999, Value: ms(290)}, {Q: 1, Value: ms(2000)},
				},
			},
			PerSeries:            2 * time.Microsecond,
			PerChunk:             500 * time.Nanosecond,
			Correlation:          0.7,
			SlowInstanceFraction: 0.01,
			SlowInstanceFactor:   3,
			SlowWindowPeriod:     2 * time.Hour,
			SlowWindowDuration:   3 * time.Minute,
			SlowWindowFactor:     2,
			// Step 3: medians flat to 50k/s; tails grow from about 25–35k/s, i.e. 6–10 in flight.
			InflightKnee:    8,
			TailSlope:       0.05,
			TailDelayMean:   20 * time.Millisecond,
			InflightLimit:   0,
			FirstBatchShare: 0.4,
			SeriesShare:     0.2,
		},
		Errors: ErrorConfig{Rate: map[string]float64{}, Code: "Unavailable"},
		Labels: []LabelSpec{
			{Name: "job", Values: 200, Shared: true},
			{Name: "namespace", Values: 500, Shared: true},
			{Name: "cluster", Values: 50, Shared: true},
			{Name: "instance", Values: 1000000, Shared: false},
			{Name: "pod", Values: 10000000, Shared: false},
		},
	}
}

// configHolder keeps the current runtime config and its derived state.
type configHolder struct {
	cur atomic.Pointer[runtimeState]
}

type runtimeState struct {
	cfg   RuntimeConfig
	table map[string]float64
}

func (h *configHolder) get() *runtimeState { return h.cur.Load() }

func (h *configHolder) set(cfg RuntimeConfig) error {
	st := &runtimeState{cfg: cfg}
	if cfg.Sizes.TableFile != "" {
		b, err := os.ReadFile(cfg.Sizes.TableFile)
		if err != nil {
			return fmt.Errorf("read size table: %w", err)
		}
		if strings.HasSuffix(cfg.Sizes.TableFile, ".gz") {
			zr, err := gzip.NewReader(bytes.NewReader(b))
			if err != nil {
				return fmt.Errorf("read size table: %w", err)
			}
			if b, err = io.ReadAll(zr); err != nil {
				return fmt.Errorf("read size table: %w", err)
			}
		}
		if err := json.Unmarshal(b, &st.table); err != nil {
			return fmt.Errorf("parse size table: %w", err)
		}
	}
	h.cur.Store(st)
	return nil
}

// loadRuntimeConfig parses YAML on top of the defaults, so a file only needs the fields it changes.
func loadRuntimeConfig(b []byte) (RuntimeConfig, error) {
	cfg := DefaultRuntimeConfig()
	if len(b) == 0 {
		return cfg, nil
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func marshalYAML(cfg RuntimeConfig) ([]byte, error) { return yaml.Marshal(cfg) }
