// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// delayFor returns the total time a request should take on one fake.
//
// The quantile u is drawn with a Gaussian copula: a component shared by every partition the request
// reaches (from a hash of the request key) plus an independent one. With Correlation near 1, an
// expensive request is expensive on every partition, which is what makes the fan-out tail look right.
func delayFor(st *runtimeState, rpc, requestKey, instanceID string, series, chunks int, now time.Time, inflight int) time.Duration {
	lc := st.cfg.Latency
	points, ok := lc.Base[rpc]
	if !ok {
		points = lc.Base["default"]
	}
	rho := math.Max(0, math.Min(1, lc.Correlation))
	zShared := normQuantile(clampUnit(hashUnit(requestKey, now.Truncate(time.Second).String())))
	zOwn := rand.NormFloat64()
	u := normCDF(math.Sqrt(rho)*zShared + math.Sqrt(1-rho)*zOwn)

	sec := interpolate(points, u)
	d := time.Duration(sec*float64(time.Second)) + time.Duration(series)*lc.PerSeries + time.Duration(chunks)*lc.PerChunk

	// Persistently slow instances.
	if lc.SlowInstanceFraction > 0 && hashUnit("slow", instanceID) < lc.SlowInstanceFraction && lc.SlowInstanceFactor > 0 {
		d = time.Duration(float64(d) * lc.SlowInstanceFactor)
	}
	// Periodic slow windows, staggered by instance.
	if lc.SlowWindowPeriod > 0 && lc.SlowWindowDuration > 0 && lc.SlowWindowFactor > 0 {
		phase := time.Duration(hashUnit("window", instanceID) * float64(lc.SlowWindowPeriod))
		pos := (time.Duration(now.UnixNano()) + phase) % lc.SlowWindowPeriod
		if pos < lc.SlowWindowDuration {
			d = time.Duration(float64(d) * lc.SlowWindowFactor)
		}
	}
	// Load: past the knee, a growing share of requests pick up an extra exponential delay.
	if lc.InflightKnee > 0 && inflight > lc.InflightKnee && lc.TailDelayMean > 0 {
		p := math.Min(1, lc.TailSlope*float64(inflight-lc.InflightKnee))
		if rand.Float64() < p {
			d += time.Duration(rand.ExpFloat64() * float64(lc.TailDelayMean))
		}
	}
	return d
}

// injectedError returns a status error when the request should fail.
func injectedError(st *runtimeState, rpc, instanceID string, inflight int) error {
	ec := st.cfg.Errors
	if isUnhealthy(st, instanceID) {
		return status.Error(codes.Unavailable, "fake ingester: instance marked unhealthy")
	}
	if lim := st.cfg.Latency.InflightLimit; lim > 0 && inflight > lim {
		return status.Error(codes.ResourceExhausted, "fake ingester: too many inflight requests")
	}
	rate, ok := ec.Rate[rpc]
	if !ok {
		rate = ec.Rate["default"]
	}
	if rate > 0 && rand.Float64() < rate {
		return status.Error(codeByName(ec.Code), "fake ingester: injected error")
	}
	return nil
}

func isUnhealthy(st *runtimeState, instanceID string) bool {
	ec := st.cfg.Errors
	for _, id := range ec.UnhealthyInstances {
		if id == instanceID {
			return true
		}
	}
	return ec.UnhealthyFraction > 0 && hashUnit("unhealthy", instanceID) < ec.UnhealthyFraction
}

func codeByName(name string) codes.Code {
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		if c.String() == name {
			return c
		}
	}
	return codes.Unavailable
}

// sleepCtx waits for d or until ctx is done. A cancelled or hedged request frees its slot at once,
// and a waiting request costs one parked goroutine and no CPU.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func clampUnit(u float64) float64 { return math.Min(math.Max(u, 1e-12), 1-1e-12) }

func normCDF(z float64) float64 { return 0.5 * math.Erfc(-z/math.Sqrt2) }

// normQuantile is the inverse standard normal CDF (Acklam's approximation, |error| < 1.2e-9).
func normQuantile(p float64) float64 {
	a := [6]float64{-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00}
	b := [5]float64{-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01}
	c := [6]float64{-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00}
	d := [4]float64{7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00}
	const lo, hi = 0.02425, 1 - 0.02425
	switch {
	case p < lo:
		q := math.Sqrt(-2 * math.Log(p))
		return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	case p <= hi:
		q := p - 0.5
		r := q * q
		return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r + a[5]) * q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r + 1)
	default:
		q := math.Sqrt(-2 * math.Log(1-p))
		return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	}
}
