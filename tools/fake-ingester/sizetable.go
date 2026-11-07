// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// sizeSample is one query-frontend "query stats" log entry, reduced to what sizing needs.
type sizeSample struct {
	Query  string `json:"q"`
	Series int    `json:"fs"`  // fetched_series_count
	Length string `json:"len"` // query length; empty for instant queries
	Count  int    `json:"n"`   // how many identical entries this sample stands for
}

type sizeTableStats struct {
	Samples, Queries, ParseErrors, Selectors, Keys int
}

// buildSizeTable turns query-frontend query stats into a table of normalized selector → cell-wide
// series, the table the fakes look up (sizes.table_file).
//
// Each query's fetched_series_count is split evenly across its selectors, and divided by the number
// of 24h time splits it spans (each split fetches the same series again, and only the most recent
// one reaches ingesters). The table holds the count-weighted median per selector. fetched_series_count
// includes store-gateway series, so it's an upper bound on what ingesters return.
func buildSizeTable(in io.Reader) (map[string]float64, sizeTableStats, error) {
	var st sizeTableStats
	p := parser.NewParser(parser.Options{EnableExperimentalFunctions: true})
	type obs struct {
		series float64
		weight int
	}
	byKey := map[string][]obs{}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var s sizeSample
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, st, fmt.Errorf("parse sample: %w", err)
		}
		st.Samples++
		if s.Query == "" {
			continue
		}
		st.Queries += max(1, s.Count)
		expr, err := p.ParseExpr(s.Query)
		if err != nil {
			st.ParseErrors++
			continue
		}
		sels := parser.ExtractSelectors(expr)
		if len(sels) == 0 {
			continue
		}
		splits := 1.0
		if d, err := time.ParseDuration(s.Length); err == nil && d > 24*time.Hour {
			splits = math.Ceil(float64(d) / float64(24*time.Hour))
		}
		per := float64(s.Series) / float64(len(sels)) / splits
		for _, ms := range sels {
			key := parseSelector(ms).key
			byKey[key] = append(byKey[key], obs{series: per, weight: max(1, s.Count)})
			st.Selectors++
		}
	}
	if err := sc.Err(); err != nil {
		return nil, st, err
	}
	out := make(map[string]float64, len(byKey))
	for k, os := range byKey {
		sort.Slice(os, func(i, j int) bool { return os[i].series < os[j].series })
		total := 0
		for _, o := range os {
			total += o.weight
		}
		acc := 0
		for _, o := range os {
			acc += o.weight
			if 2*acc >= total {
				out[k] = math.Round(o.series)
				break
			}
		}
	}
	st.Keys = len(out)
	return out, st, nil
}
