package transport

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

type mockTransportHandler struct {
}

func (h *mockTransportHandler) ReceiveInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity,
	method string, payload []byte, promise InvokeMethodPromise) {
	promise.Resolve(InvokeMethodResponse{
		Response: []byte("testresponse-" + string(payload)),
	})
}

func (*mockTransportHandler) ReceiveInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, name string, payload []byte) {

}

type mockTransport struct {
	lock         sync.Mutex
	wg           sync.WaitGroup
	err          error
	h            cluster.TransportHandler
	delayFunc    func(uuid string) time.Duration
	responseFunc func(uuid string) ([]byte, []byte)
	clock        clock.Clock
}

var testError = errors.New("test-error")

func newMockTransport(clock clock.Clock) *mockTransport {
	return &mockTransport{
		clock: clock,
		delayFunc: func(uuid string) time.Duration {
			return 0
		},
		responseFunc: func(uuid string) ([]byte, []byte) {
			if strings.HasPrefix(uuid, "error-") {
				return nil, encodeError(testError)
			}
			return []byte("testresponse-" + uuid), nil
		},
	}
}

func (m *mockTransport) Wait() {
	m.wg.Wait()
}

func (m *mockTransport) Listen(h cluster.TransportHandler) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.h = h
	return m.err
}
func (m *mockTransport) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.err
}

func (m *mockTransport) EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.err != nil {
		return m.err
	}
	if strings.HasPrefix(uuid, "drop-") {
		return nil
	}

	d := m.delayFunc(uuid)
	m.clock.AfterFunc(d, func() {
		payload, errData := m.responseFunc(uuid)
		m.h.ReceiveInvokeMethodResponse(context.Background(), sender, uuid, payload, errData)
	})

	return nil
}

func (m *mockTransport) EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.err
}

func (m *mockTransport) EnqueueInvokeOneWayMethodRequest(ctx context.Context, sender grain.Identity, receivers []grain.Identity, methodName string, payload []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.err
}

var errTransportCreateFail = errors.New("transport create failed")
var errListenFail = errors.New("listen failed")

func TestManager(t *testing.T) {
	handler := &mockTransportHandler{}
	c := clock.NewMock()
	m := NewManager(logr.Discard(), c, handler)
	transports := map[string]*mockTransport{
		"node1": newMockTransport(c),
		"node2": newMockTransport(c),
	}

	t.Run("AddTransport", func(t *testing.T) {
		err := m.AddTransport("node1", func() (cluster.Transport, error) {
			return transports["node1"], nil
		})
		require.NoError(t, err)
		err = m.AddTransport("node2", func() (cluster.Transport, error) {
			return transports["node2"], nil
		})
		require.NoError(t, err)
		err = m.AddTransport("nodefailcreate", func() (cluster.Transport, error) {
			return nil, errTransportCreateFail
		})
		require.Equal(t, errTransportCreateFail, err)
		err = m.AddTransport("nodelistenfail", func() (cluster.Transport, error) {
			mt := newMockTransport(c)
			mt.err = errListenFail
			return mt, nil
		})
		require.Equal(t, errListenFail, err)
	})

	t.Run("InvokeMethod success", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			n := m.RandomNode()
			require.Contains(t, transports, n)
			sender := grain.Anonymous()
			receiver := cluster.GrainAddress{
				Location: cluster.Location(n),
				Identity: grain.Identity{
					GrainType: "Test",
					ID:        n,
				},
			}
			payload := []byte("testpayload")
			expectedResponse := []string{}
			futures := []InvokeMethodFuture{}

			for j := 0; j < 10; j++ {
				uuid := "InvokeMethodTest-" + ksuid.New().String()
				f, err := m.InvokeMethod(context.Background(), sender, receiver, "TestMethod", uuid, payload)
				require.NoError(t, err)
				futures = append(futures, f)
				expectedResponse = append(expectedResponse, "testresponse-"+uuid)
			}
			c.Add(time.Millisecond)

			for j := range futures {
				f := futures[j]
				expected := expectedResponse[j]
				resp, err := f.Await(context.Background())
				require.NoError(t, err)
				require.Equal(t, []byte(expected), resp.Response)
			}
		}
	})

	t.Run("ReceiveInvokeMethod success", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			n := m.RandomNode()
			require.Contains(t, transports, n)
			sender := grain.Anonymous()
			receiver := cluster.GrainAddress{
				Location: cluster.Location(n),
				Identity: grain.Identity{
					GrainType: "Test",
					ID:        n,
				},
			}
			payload := []byte("testpayload")
			expectedResponse := []string{}
			futures := []InvokeMethodFuture{}

			for j := 0; j < 10; j++ {
				uuid := "ReceiveInvokeMethodTest-" + ksuid.New().String()
				transport := transports[n]
				transport.h.ReceiveInvokeMethodRequest(context.Background(), sender, receiver.Identity, "TestMethod",
					uuid, payload, time.Time{})

			}
			c.Add(time.Millisecond)

			for j := range futures {
				f := futures[j]
				expected := expectedResponse[j]
				resp, err := f.Await(context.Background())
				require.NoError(t, err)
				require.Equal(t, []byte(expected), resp.Response)
			}
		}
	})

	t.Run("InvokeMethod error", func(t *testing.T) {
		f, err := m.InvokeMethod(
			context.Background(),
			grain.Anonymous(),
			cluster.GrainAddress{
				Location: cluster.Location("node1"),
				Identity: grain.Identity{
					GrainType: "Test",
					ID:        "grain1",
				},
			},
			"TestMethod",
			"error-uuid1",
			[]byte("testpayload"),
		)
		require.NoError(t, err)
		c.Add(time.Millisecond)
		_, err = f.Await(context.Background())
		require.True(t, errors.Is(err, testError))
	})

	t.Run("InvokeMethod timeout", func(t *testing.T) {
		deadlineCtx, cancel := c.WithDeadline(context.Background(), c.Now().Add(time.Second))
		defer cancel()
		f, err := m.InvokeMethod(
			deadlineCtx,
			grain.Anonymous(),
			cluster.GrainAddress{
				Location: cluster.Location("node1"),
				Identity: grain.Identity{
					GrainType: "Test",
					ID:        "grain1",
				},
			},
			"TestMethod",
			"drop-uuid1",
			[]byte("testpayload"),
		)
		require.NoError(t, err)
		// There is some slack here. The deadline heap is run on a secondly interval
		c.Add(2 * time.Second)
		_, err = f.Await(context.Background())
		require.Equal(t, context.DeadlineExceeded, err)
	})

}
