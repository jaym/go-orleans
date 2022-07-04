package transport

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeadlineHeap(t *testing.T) {
	now := time.Now()

	t.Run("starts off with nothing to expire", func(t *testing.T) {
		h := NewDeadlineHeap()
		h.nowProvider = func() time.Time {
			return now
		}

		now = time.Now().Add(1000 * time.Hour)
		h.Expire(func(typ RequestType, uuid string) {
			require.Fail(t, "there sould be nothing to expire")
		})
	})

	t.Run("expires before add", func(t *testing.T) {
		h := NewDeadlineHeap()
		h.nowProvider = func() time.Time {
			return now
		}

		now = time.Now()
		deadline := now.Add(-1 * time.Hour)
		h.ExpireAndAdd(RegisterObserverRequestType, "uuid1", deadline, func(typ RequestType, uuid string) {
			require.Fail(t, "there sould be nothing to expire")
		})
		expired := false
		h.Expire(func(typ RequestType, uuid string) {
			if typ == RegisterObserverRequestType && uuid == "uuid1" {
				expired = true
			} else {
				require.Fail(t, "incorrect arguments")
			}
		})
		require.True(t, expired)
	})

	t.Run("boundary conditions", func(t *testing.T) {
		h := NewDeadlineHeap()
		h.nowProvider = func() time.Time {
			return now
		}

		now = time.Now()
		h.ExpireAndAdd(RegisterObserverRequestType, "uuid1", now, func(typ RequestType, uuid string) {
			require.Fail(t, "there sould be nothing to expire")
		})
		expired := false
		h.Expire(func(typ RequestType, uuid string) {
			if typ == RegisterObserverRequestType && uuid == "uuid1" {
				expired = true
			} else {
				require.Fail(t, "incorrect arguments")
			}
		})
		require.True(t, expired)
	})

	t.Run("stress", func(t *testing.T) {
		h := NewDeadlineHeap()
		h.nowProvider = func() time.Time {
			return now
		}

		now = time.Now()
		deadline1 := now.Add(time.Minute)
		deadline2 := now.Add(2 * time.Minute)
		numDeadlines := 1000
		interval := time.Minute / time.Duration(numDeadlines+1)

		expired := map[string]struct{}{}
		for i := 0; i < numDeadlines; i++ {
			h.ExpireAndAdd(
				RequestType(i%3+1),
				"uuid-1-"+strconv.Itoa(i),
				now.Add(time.Duration(i+1)*interval),
				func(typ RequestType, uuid string) {
					require.Fail(t, "there sould be nothing to expire")
				})
		}

		for i := 0; i < numDeadlines; i++ {
			h.ExpireAndAdd(
				RequestType(i%3+1),
				"uuid-2-"+strconv.Itoa(i),
				deadline1.Add(time.Duration(i+1)*interval),
				func(typ RequestType, uuid string) {
					require.Fail(t, "there sould be nothing to expire")
				})
		}

		now = deadline1

		h.Expire(func(typ RequestType, uuid string) {
			expired[uuid] = struct{}{}
		})
		require.Len(t, expired, numDeadlines)

		now = deadline2
		h.Expire(func(typ RequestType, uuid string) {
			expired[uuid] = struct{}{}
		})
		require.Len(t, expired, 2*numDeadlines)
	})
}
