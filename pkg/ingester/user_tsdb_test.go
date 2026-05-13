// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUserTSDB_acquireAppendLock(t *testing.T) {
	t.Run("should allow to acquire the lock during forced compaction if not conflicting with the compaction time range", func(t *testing.T) {
		db := &userTSDB{}

		state, err := db.acquireAppendLock(20)
		require.NoError(t, err)
		db.releaseAppendLock(state)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		_, err = db.acquireAppendLock(20)
		require.ErrorIs(t, err, errTSDBEarlyCompaction)

		state, err = db.acquireAppendLock(21)
		require.NoError(t, err)
		db.releaseAppendLock(state)
	})

	t.Run("should count all acquired locks in the inflight appends", func(t *testing.T) {
		db := &userTSDB{}

		stateActive, err := db.acquireAppendLock(20)
		require.NoError(t, err)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		stateForcedCompaction, err := db.acquireAppendLock(21)
		require.NoError(t, err)

		// Start a goroutine that will signal once in-flight appends are done.
		inFlightAppendsDone := make(chan struct{})
		go func() {
			db.inFlightAppends.Wait()
			close(inFlightAppendsDone)
		}()

		// Releasing the lock acquired while active should not signal in-flight appends done.
		db.releaseAppendLock(stateActive)
		select {
		case <-inFlightAppendsDone:
			t.Fatal("in-flight appends has been signaled as done, but there's still a lock acquired while force compacting")
		case <-time.After(100 * time.Millisecond):
		}

		// Releasing the remaining lock should signal in-flight appends done.
		db.releaseAppendLock(stateForcedCompaction)
		select {
		case <-time.After(100 * time.Millisecond):
			t.Fatal("in-flight appends have no been signaled as done, but all locks has been released")
		case <-inFlightAppendsDone:
		}
	})

	t.Run("should count only locks acquired while not force compacting in the inflight appends started before forced compaction", func(t *testing.T) {
		db := &userTSDB{}

		stateActive, err := db.acquireAppendLock(20)
		require.NoError(t, err)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		stateForcedCompaction, err := db.acquireAppendLock(21)
		require.NoError(t, err)

		// Start a goroutine that will signal once in-flight appends are done.
		inFlightAppendsWithoutForcedCompactionDone := make(chan struct{})
		go func() {
			db.inFlightAppendsStartedBeforeForcedCompaction.Wait()
			close(inFlightAppendsWithoutForcedCompactionDone)
		}()

		// Releasing the lock acquired while active should signal in-flight appends done.
		db.releaseAppendLock(stateActive)
		select {
		case <-time.After(100 * time.Millisecond):
			t.Fatal("in-flight appends started before forced compaction has not been signaled as done")
		case <-inFlightAppendsWithoutForcedCompactionDone:
		}

		db.releaseAppendLock(stateForcedCompaction)
	})
}

func TestNextForcedHeadCompactionRange(t *testing.T) {
	const blockDuration = 10

	tests := map[string]struct {
		headMinTime     int64
		headMaxTime     int64
		forcedMaxTime   int64
		expectedMinTime int64
		expectedMaxTime int64
		expectedIsValid bool
		expectedIsLast  bool
	}{
		"should compact the whole head if the range fits within the block range": {
			headMinTime:     12,
			headMaxTime:     18,
			forcedMaxTime:   50,
			expectedMinTime: 12,
			expectedMaxTime: 18,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should compact the 1st block range period if the range doesn't fit within the block range": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   50,
			expectedMinTime: 12,
			expectedMaxTime: 19,
			expectedIsValid: true,
			expectedIsLast:  false,
		},
		"should honor the forcedMaxTime when the range fits within the block range": {
			headMinTime:     12,
			headMaxTime:     19,
			forcedMaxTime:   15,
			expectedMinTime: 12,
			expectedMaxTime: 15,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should honor the forcedMaxTime when the range doesn't fit within the block range": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   15,
			expectedMinTime: 12,
			expectedMaxTime: 15,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should return no range if forcedMaxTime is smaller than headMinTime": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   11,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
		"should return a range with min time equal to max time if forcedMaxTime is equal to headMinTime": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   12,
			expectedMinTime: 12,
			expectedMaxTime: 12,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should return no range if head has min time not set": {
			headMinTime:     math.MaxInt64,
			headMaxTime:     20,
			forcedMaxTime:   12,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
		"should return no range if head has max time not set": {
			headMinTime:     0,
			headMaxTime:     math.MinInt64,
			forcedMaxTime:   12,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMinTime, actualMaxTime, actualIsValid, actualIsLast := nextForcedHeadCompactionRange(blockDuration, testData.headMinTime, testData.headMaxTime, testData.forcedMaxTime)
			assert.Equal(t, testData.expectedIsValid, actualIsValid)
			assert.Equal(t, testData.expectedIsLast, actualIsLast)

			if testData.expectedIsValid {
				assert.Equal(t, testData.expectedMinTime, actualMinTime)
				assert.Equal(t, testData.expectedMaxTime, actualMaxTime)
			}
		})
	}
}

func TestGetSeriesCountAndMinLocalLimit(t *testing.T) {
	tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, tsdbDB.Close())
	})

	// append some series
	app := tsdbDB.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("hello", "world"), 10, 20)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	db := userTSDB{
		db: tsdbDB,
		ownedState: ownedSeriesState{
			ownedSeriesCount: 555,
			localSeriesLimit: 10000,
		},
	}

	t.Run("using series in Head", func(t *testing.T) {
		db.useOwnedSeriesForLimits = false

		cnt, minLimit := db.getSeriesCountAndMinLocalLimit()
		require.Equal(t, 1, cnt)
		require.Equal(t, 0, minLimit)
	})

	t.Run("using owned series", func(t *testing.T) {
		db.useOwnedSeriesForLimits = true

		cnt, minLimit := db.getSeriesCountAndMinLocalLimit()
		require.Equal(t, 555, cnt)
		require.Equal(t, 10000, minLimit)
	})
}

func TestRecomputeOwnedSeries(t *testing.T) {
	limits := validation.Limits{MaxGlobalSeriesPerUser: 0}
	overrides := validation.NewOverrides(limits, nil)
	limiter := NewLimiter(overrides, newIngesterRingLimiterStrategy(nil, 3, true, "zone", overrides.IngestionTenantShardSize))

	t.Run("happy path", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}
		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			return 10
		})
		require.True(t, success)
		require.Equal(t, 1, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, but within limit", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}
		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += recomputeOwnedSeriesMaxSeriesDiff / 2
			return 10
		})

		require.True(t, success)
		require.Equal(t, 1, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, last increase is within limit", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}

		// All but last modifications of ownedSeries during compute will exceed the limit.
		mods := make([]int, recomputeOwnedSeriesMaxAttempts)
		for i := 0; i < len(mods)-1; i++ {
			mods[i] = 2 * recomputeOwnedSeriesMaxSeriesDiff
		}
		mods[len(mods)-1] = recomputeOwnedSeriesMaxSeriesDiff

		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += mods[0]
			mods = mods[1:]
			return 10
		})
		require.True(t, success)
		require.Equal(t, recomputeOwnedSeriesMaxAttempts, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, last increase is above limit, computation should retry", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}

		// All modifications of ownedSeries will exceed the limit.
		mods := make([]int, recomputeOwnedSeriesMaxAttempts)
		for i := 0; i < len(mods); i++ {
			mods[i] = 2 * recomputeOwnedSeriesMaxSeriesDiff
		}

		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += mods[0]
			mods = mods[1:]
			return 10
		})
		require.False(t, success)
		require.Equal(t, recomputeOwnedSeriesMaxAttempts, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})
}

// fakeOOOPanicker exists so that its method name "compactOOO" appears in panic
// stack traces, exercising the OOO-detection substring match in
// (*userTSDB).compactWithPanicRecovery.
type fakeOOOPanicker struct{}

//nolint:revive // method name intentionally matches the runtime substring we filter on
func (fakeOOOPanicker) compactOOO() error {
	panic("simulated OOO compaction panic")
}

func panicOutsideOOO() error {
	panic("simulated non-OOO panic")
}

func TestUserTSDB_compactWithPanicRecovery(t *testing.T) {
	tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

	u := &userTSDB{
		db:     tsdbDB,
		userID: "test",
		logger: log.NewNopLogger(),
	}

	withFlags := func(t *testing.T, swallow, drop bool) {
		t.Helper()
		origSwallow := swallowOOOCompactionPanic
		origDrop := dropOOOHeadOnCompactionPanic
		t.Cleanup(func() {
			swallowOOOCompactionPanic = origSwallow
			dropOOOHeadOnCompactionPanic = origDrop
		})
		swallowOOOCompactionPanic = swallow
		dropOOOHeadOnCompactionPanic = drop
	}

	t.Run("no panic: inner error is returned as-is", func(t *testing.T) {
		innerErr := errors.New("inner")
		err := u.compactWithPanicRecovery("op", func() error { return innerErr })
		require.ErrorIs(t, err, innerErr)
	})

	t.Run("non-OOO panic is always re-panicked, even when swallow and drop are enabled", func(t *testing.T) {
		withFlags(t, true, true)
		require.PanicsWithValue(t, "simulated non-OOO panic", func() {
			_ = u.compactWithPanicRecovery("op", panicOutsideOOO)
		})
	})

	t.Run("OOO panic is re-panicked when swallow is disabled", func(t *testing.T) {
		withFlags(t, false, true) // drop is irrelevant when swallow is off
		require.PanicsWithValue(t, "simulated OOO compaction panic", func() {
			_ = u.compactWithPanicRecovery("op", fakeOOOPanicker{}.compactOOO)
		})
	})

	t.Run("OOO panic is returned as error when swallow is enabled and drop is disabled", func(t *testing.T) {
		withFlags(t, true, false)
		var err error
		require.NotPanics(t, func() {
			err = u.compactWithPanicRecovery("op", fakeOOOPanicker{}.compactOOO)
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic during OOO compaction")
		require.Contains(t, err.Error(), "simulated OOO compaction panic")
	})

	t.Run("OOO panic is returned as error when swallow and drop are both enabled", func(t *testing.T) {
		withFlags(t, true, true)
		var err error
		require.NotPanics(t, func() {
			err = u.compactWithPanicRecovery("op", fakeOOOPanicker{}.compactOOO)
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic during OOO compaction")
	})
}

func TestUserTSDB_forceDropOOOHead(t *testing.T) {
	opts := tsdb.DefaultOptions()
	opts.OutOfOrderTimeWindow = (30 * time.Minute).Milliseconds()

	tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

	u := &userTSDB{
		db:     tsdbDB,
		userID: "test",
		logger: log.NewNopLogger(),
	}

	const inOrderTS = int64(2_000_000)
	const oooTS = int64(1_900_000) // 100s before in-order, well within the 30m window
	lbls := labels.FromStrings("__name__", "foo")

	app := tsdbDB.Appender(context.Background())
	_, err = app.Append(0, lbls, inOrderTS, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	app = tsdbDB.Appender(context.Background())
	_, err = app.Append(0, lbls, oooTS, 0.5)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Sanity: both samples are queryable before the drop.
	require.ElementsMatch(t,
		[]sample{{t: oooTS, v: 0.5}, {t: inOrderTS, v: 1.0}},
		querySamples(t, tsdbDB, oooTS-1, inOrderTS+1, lbls),
		"both in-order and OOO samples should be queryable before force-drop",
	)

	require.NoError(t, u.forceDropOOOHead())

	// After the drop, only the in-order sample is queryable. The OOO sample is gone.
	require.Equal(t,
		[]sample{{t: inOrderTS, v: 1.0}},
		querySamples(t, tsdbDB, oooTS-1, inOrderTS+1, lbls),
		"only in-order sample should remain after force-drop",
	)

	require.NoError(t, u.db.CompactOOOHead(context.Background()), "subsequent OOO compaction should be a no-op")
}

type sample struct {
	t int64
	v float64
}

func querySamples(t *testing.T, db *tsdb.DB, mint, maxt int64, lbls labels.Labels) []sample {
	t.Helper()
	q, err := db.Querier(mint, maxt)
	require.NoError(t, err)
	// Close immediately - we can't use t.Cleanup here because outstanding queriers
	// hold OOO chunk references and block (*Head).truncateOOO via
	// WaitForPendingReadersForOOOChunksAtOrBefore.
	defer func() { require.NoError(t, q.Close()) }()

	matchers := make([]*labels.Matcher, 0, lbls.Len())
	lbls.Range(func(l labels.Label) {
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, l.Name, l.Value))
	})

	ss := q.Select(context.Background(), false, nil, matchers...)
	var out []sample
	for ss.Next() {
		it := ss.At().Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			out = append(out, sample{t: ts, v: v})
		}
		require.NoError(t, it.Err())
	}
	require.NoError(t, ss.Err())
	return out
}

func TestUserTSDB_forceDropOOOHead_oooDisabled(t *testing.T) {
	tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

	u := &userTSDB{
		db:     tsdbDB,
		userID: "test",
		logger: log.NewNopLogger(),
	}

	require.NoError(t, u.forceDropOOOHead(), "force-drop on a TSDB without OOO enabled should be a no-op")
}
