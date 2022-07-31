package transport

import (
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
)

func TestDeadlineHeap(t *testing.T) {
	c := clock.NewMock()

	t.Run("starts off with nothing to expire", func(t *testing.T) {
		h := NewDeadlineHeap(c)
		c.Add(1000 * time.Hour)
		h.Expire(func(typ RequestType, uuid string) {
			require.Fail(t, "there sould be nothing to expire")
		})
	})

	t.Run("expires before add", func(t *testing.T) {
		h := NewDeadlineHeap(c)

		deadline := c.Now().Add(-1 * time.Hour)
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
		h := NewDeadlineHeap(c)

		h.ExpireAndAdd(RegisterObserverRequestType, "uuid1", c.Now(), func(typ RequestType, uuid string) {
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
		h := NewDeadlineHeap(c)

		deadline1 := c.Now().Add(time.Minute)
		deadline2 := c.Now().Add(2 * time.Minute)
		numDeadlines := 1000
		interval := time.Minute / time.Duration(numDeadlines+1)

		expired := map[string]struct{}{}
		for i := 0; i < numDeadlines; i++ {
			h.ExpireAndAdd(
				RequestType(i%3+1),
				"uuid-1-"+strconv.Itoa(i),
				c.Now().Add(time.Duration(i+1)*interval),
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

		c.Set(deadline1)

		h.Expire(func(typ RequestType, uuid string) {
			expired[uuid] = struct{}{}
		})
		require.Len(t, expired, numDeadlines)

		c.Set(deadline2)
		h.Expire(func(typ RequestType, uuid string) {
			expired[uuid] = struct{}{}
		})
		require.Len(t, expired, 2*numDeadlines)
	})
}
