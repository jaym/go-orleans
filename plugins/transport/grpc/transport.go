package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	gogoproto "github.com/gogo/protobuf/proto"
	ggrpc "google.golang.org/grpc"

	"github.com/go-logr/logr"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/transport/grpc/internal"
	"github.com/jaym/go-orleans/silo/services/cluster"
)

type TransportServer struct {
	internal.UnimplementedTransportServer
	log           logr.Logger
	lock          sync.Mutex
	transports    map[string]*transport
	grpcServer    *ggrpc.Server
	listenAddress string
	thisNode      node
}

type node struct {
	name string
	ip   string
	port int
}

func New(log logr.Logger, nodeName string, listenAddress string) (*TransportServer, error) {
	// TODO: listenAddress should be decoupled from the advertised address
	parts := strings.Split(listenAddress, ":")
	if len(parts) != 2 {
		return nil, errors.New("invalid listen address")
	}
	ip := parts[0]
	port, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return nil, err
	}
	return &TransportServer{
		log:           log,
		transports:    make(map[string]*transport, 16),
		grpcServer:    ggrpc.NewServer(),
		listenAddress: fmt.Sprintf("0.0.0.0:%d", port),
		thisNode: node{
			name: nodeName,
			ip:   ip,
			port: int(port),
		},
	}, nil
}

func (s *TransportServer) Start() error {
	internal.RegisterTransportServer(s.grpcServer, s)
	lis, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return err
	}
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.log.Error(err, "serve failed")
		}
	}()
	return nil
}

func (s *TransportServer) CreateTransport(clusterNode cluster.Node) (cluster.Transport, error) {
	n := node{
		name: string(clusterNode.Name),
		ip:   clusterNode.Addr.To16().String(),
		port: int(clusterNode.RPCPort),
	}
	tp := s.createTransport(n)
	return tp, nil
}

func (s *TransportServer) Send(recv internal.Transport_SendServer) error {
	helloMsg, err := recv.Recv()
	if err != nil {
		return err
	}
	tp, err := s.helloMsg(helloMsg)
	if err != nil {
		return err
	}
	s.log.Info("node connected", "node", tp.dstNode.name, "ip", tp.dstNode.ip, "port", tp.dstNode.port)
	for {
		msg, err := recv.Recv()
		if err != nil {
			if err == io.EOF {
				s.log.Info("messsage channel shutting down")
				return recv.SendAndClose(&internal.SendResponse{})
			}
			s.log.Error(err, "message channel errored")
			return err
		}
		tp.recv(msg)
	}
}

func (s *TransportServer) helloMsg(msg *internal.TransportMessage) (*transport, error) {
	hello := msg.GetHello()
	if hello == nil {
		return nil, errors.New("hello message not received")
	}
	ip := net.ParseIP(hello.Addr)
	if ip == nil {
		return nil, errors.New("invalid ip")
	}
	if hello.Name == "" {
		return nil, errors.New("invalid name")
	}
	if hello.Port <= 0 {
		return nil, errors.New("invalid port")
	}
	n := node{
		name: hello.Name,
		ip:   ip.To16().String(),
		port: int(hello.Port),
	}
	tp := s.createTransport(n)
	return tp, nil
}

func (s *TransportServer) createTransport(n node) *transport {
	s.lock.Lock()
	defer s.lock.Unlock()
	if tp, ok := s.transports[n.name]; ok {
		return tp
	}
	tp := newTransport(s.log, s.thisNode, n)
	s.transports[n.name] = tp
	return tp
}

type transport struct {
	thisNode node
	log      logr.Logger
	dstNode  node
	// inbox contains messages that need to be processed by this node
	inbox chan *internal.TransportMessage
	// outbox contains messages to send to another node
	outbox    chan *internal.TransportMessage
	closeChan chan struct{}
	wg        sync.WaitGroup
}

func newTransport(log logr.Logger, thisNode node, dstNode node) *transport {
	return &transport{
		log:       log.WithName("transport").WithValues("node", dstNode.name, "ip", dstNode.ip, "port", dstNode.port),
		thisNode:  thisNode,
		dstNode:   dstNode,
		inbox:     make(chan *internal.TransportMessage, 128),
		outbox:    make(chan *internal.TransportMessage, 128),
		closeChan: make(chan struct{}),
	}
}

func (t *transport) send(ctx context.Context, msg *internal.TransportMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.outbox <- msg:
	}
	return nil
}

func (t *transport) recv(msg *internal.TransportMessage) {
	t.inbox <- msg
}

func (t *transport) Listen(h cluster.TransportHandler) error {
	t.wg.Add(2)
	go func() {
		defer t.wg.Done()
		ctx := context.TODO()
		for {
			select {
			case msg := <-t.inbox:
				t.handleMsg(ctx, h, msg)
			case <-t.closeChan:
				return
			}
		}
	}()
	go func() {
		defer t.wg.Done()

		ctx := context.TODO()

		addr := fmt.Sprintf("%s:%d", t.dstNode.ip, t.dstNode.port)
		t.log.Info("Connecting to node rpc", "addr", addr)
		cc, err := ggrpc.Dial(addr, ggrpc.WithInsecure())
		if err != nil {
			t.log.Error(err, "failed to dial")
			return
		}
		defer cc.Close()
		tc := internal.NewTransportClient(cc)

		var sender internal.Transport_SendClient

		senderFailed := true
		getSender := func() (internal.Transport_SendClient, error) {
			if senderFailed {
				var err error
				sender, err = tc.Send(ctx)
				if err != nil {
					t.log.Error(err, "failed to start send channel")
					return nil, err
				}
				if err = sender.Send(&internal.TransportMessage{
					Msg: &internal.TransportMessage_Hello{
						Hello: &internal.Hello{
							Name: t.thisNode.name,
							Addr: t.thisNode.ip,
							Port: int64(t.thisNode.port),
						},
					},
				}); err != nil {
					t.log.Error(err, "failed to send hello")
					return nil, err
				}
			}
			senderFailed = false
			return sender, nil
		}

		for {
			select {
			case msg := <-t.outbox:
				sndr, err := getSender()
				if err != nil {
					t.log.Error(err, "failed to send message")
					t.failSendMsg(ctx, msg, h)
					continue
				}
				if err := sndr.Send(msg); err != nil {
					senderFailed = true
					t.log.Error(err, "failed to send message")
					t.failSendMsg(ctx, msg, h)
				}

			case <-t.closeChan:
				if sender != nil {
					if err := sender.CloseSend(); err != nil {
						t.log.Error(err, "failed to close send")
					}
				}
				return
			}
		}
	}()
	return nil
}

var ErrFailedRPC = errors.New("rpc failed")
var ErrFailedRPCBytes = encodeError(ErrFailedRPC)

func encodeError(err error) []byte {
	if err == nil {
		return nil
	}
	encodedErr := errors.EncodeError(context.Background(), err)
	if data, err := gogoproto.Marshal(&encodedErr); err != nil {
		panic(err)
	} else {
		return data
	}
}

func (t *transport) failSendMsg(ctx context.Context, msg *internal.TransportMessage, h cluster.TransportHandler) {
	switch m := msg.Msg.(type) {
	case *internal.TransportMessage_InvokeMethodReq:
		req := m.InvokeMethodReq
		h.ReceiveInvokeMethodResponse(ctx, grainIdent(req.Sender), req.Uuid, nil, ErrFailedRPCBytes)
	case *internal.TransportMessage_InvokeMethodResp:
	case *internal.TransportMessage_RegisterObserver:
		req := m.RegisterObserver
		h.ReceiveAckRegisterObserver(ctx, grainIdent(req.Observer), req.Uuid, ErrFailedRPCBytes)
	case *internal.TransportMessage_AckRegisterObserver:
	case *internal.TransportMessage_ObserverNotification:
	default:
		t.log.Info("invalid message received")
	}
}

func (t *transport) handleMsg(ctx context.Context, h cluster.TransportHandler, msg *internal.TransportMessage) {
	switch m := msg.Msg.(type) {
	case *internal.TransportMessage_InvokeMethodReq:
		req := m.InvokeMethodReq
		h.ReceiveInvokeMethodRequest(ctx, grainIdent(req.Sender), grainIdent(req.Receiver), req.Method, req.Uuid, req.Payload)
	case *internal.TransportMessage_InvokeMethodResp:
		req := m.InvokeMethodResp
		h.ReceiveInvokeMethodResponse(ctx, grainIdent(req.Receiver), req.Uuid, req.Payload, req.Err)
	case *internal.TransportMessage_RegisterObserver:
		req := m.RegisterObserver
		h.ReceiveRegisterObserverRequest(ctx, grainIdent(req.Observer), grainIdent(req.Observable), req.Name, req.Uuid, req.Payload)
	case *internal.TransportMessage_AckRegisterObserver:
		req := m.AckRegisterObserver
		h.ReceiveAckRegisterObserver(ctx, grainIdent(req.Receiver), req.Uuid, req.Err)
	case *internal.TransportMessage_ObserverNotification:
		req := m.ObserverNotification
		h.ReceiveObserverNotification(ctx, grainIdent(req.Sender), grainIdents(req.Receivers), req.ObservableType, req.Name, req.Payload)
	default:
		t.log.Info("invalid message received")
	}
}

func grainIdents(intIds []*internal.GrainIdentity) []grain.Identity {
	out := make([]grain.Identity, len(intIds))
	for i, intId := range intIds {
		out[i] = grainIdent(intId)
	}
	return out
}
func grainIdent(intId *internal.GrainIdentity) grain.Identity {
	return grain.Identity{
		GrainType: intId.GrainType,
		ID:        intId.Id,
	}
}

func internalGrainIdents(ids []grain.Identity) []*internal.GrainIdentity {
	out := make([]*internal.GrainIdentity, len(ids))
	for i, id := range ids {
		out[i] = &internal.GrainIdentity{
			GrainType: id.GrainType,
			Id:        id.ID,
		}
	}
	return out
}

func internalGrainIdent(id grain.Identity) *internal.GrainIdentity {
	return &internal.GrainIdentity{
		GrainType: id.GrainType,
		Id:        id.ID,
	}
}

func (t *transport) Stop() error {
	close(t.closeChan)
	t.wg.Wait()
	return nil
}

func (t *transport) EnqueueInvokeMethodRequest(ctx context.Context, sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error {
	msg := &internal.TransportMessage{
		Msg: &internal.TransportMessage_InvokeMethodReq{
			InvokeMethodReq: &internal.InvokeMethodReq{
				Sender:   internalGrainIdent(sender),
				Receiver: internalGrainIdent(receiver),
				Method:   method,
				Uuid:     uuid,
				Payload:  payload,
			},
		},
	}
	return t.send(ctx, msg)
}

func (t *transport) EnqueueRegisterObserverRequest(ctx context.Context, observer grain.Identity, observable grain.Identity, name string, uuid string, payload []byte) error {
	msg := &internal.TransportMessage{
		Msg: &internal.TransportMessage_RegisterObserver{
			RegisterObserver: &internal.RegisterObserver{
				Observer:   internalGrainIdent(observer),
				Observable: internalGrainIdent(observable),
				Name:       name,
				Uuid:       uuid,
				Payload:    payload,
			},
		},
	}
	return t.send(ctx, msg)
}

func (t *transport) EnqueueObserverNotification(ctx context.Context, sender grain.Identity, receivers []grain.Identity, observableType string, name string, payload []byte) error {
	msg := &internal.TransportMessage{
		Msg: &internal.TransportMessage_ObserverNotification{
			ObserverNotification: &internal.ObserverNotification{
				Sender:         internalGrainIdent(sender),
				Receivers:      internalGrainIdents(receivers),
				ObservableType: observableType,
				Name:           name,
				Payload:        payload,
			},
		},
	}
	return t.send(ctx, msg)
}

func (t *transport) EnqueueAckRegisterObserver(ctx context.Context, receiver grain.Identity, uuid string, errOut []byte) error {
	msg := &internal.TransportMessage{
		Msg: &internal.TransportMessage_AckRegisterObserver{
			AckRegisterObserver: &internal.AckRegisterObserver{
				Receiver: internalGrainIdent(receiver),
				Uuid:     uuid,
				Err:      errOut,
			},
		},
	}
	return t.send(ctx, msg)
}

func (t *transport) EnqueueInvokeMethodResponse(ctx context.Context, receiver grain.Identity, uuid string, payload []byte, err []byte) error {
	msg := &internal.TransportMessage{
		Msg: &internal.TransportMessage_InvokeMethodResp{
			InvokeMethodResp: &internal.InvokeMethodResp{
				Receiver: internalGrainIdent(receiver),
				Uuid:     uuid,
				Payload:  payload,
				Err:      err,
			},
		},
	}
	return t.send(ctx, msg)
}