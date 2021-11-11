package memberlist

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/hashicorp/memberlist"
	hmemberlist "github.com/hashicorp/memberlist"

	"github.com/jaym/go-orleans/silo/services/cluster"
)

type MembershipProtocol struct {
	log            logr.Logger
	nodeName       cluster.Location
	membershipPort int
	wg             sync.WaitGroup
	closeChan      chan struct{}
	memberlist     *hmemberlist.Memberlist
	nodeMetadata   *nodeMetadata
}

type nodeMetadata struct {
	RPCPort int
}

func (m *nodeMetadata) NodeMeta(limit int) []byte {
	b, _ := json.Marshal(m)
	return b
}

func (*nodeMetadata) NotifyMsg([]byte) {}

func (*nodeMetadata) GetBroadcasts(overhead, limit int) [][]byte { return nil }

func (*nodeMetadata) LocalState(join bool) []byte { return nil }

func (*nodeMetadata) MergeRemoteState(buf []byte, join bool) {}

func New(log logr.Logger, nodeName cluster.Location, membershipPort int, rpcPort int) *MembershipProtocol {
	return &MembershipProtocol{
		log:            log,
		nodeName:       nodeName,
		membershipPort: membershipPort,
		closeChan:      make(chan struct{}),
		nodeMetadata: &nodeMetadata{
			RPCPort: rpcPort,
		},
	}
}

func (m *MembershipProtocol) Start(ctx context.Context, d cluster.MembershipDelegate) error {
	config := hmemberlist.DefaultLANConfig()
	ch := make(chan memberlist.NodeEvent, 8)
	config.Events = &channelEventDelegate{
		Ch: ch,
	}
	config.BindPort = int(m.membershipPort)
	config.Name = string(m.nodeName)
	config.Delegate = m.nodeMetadata
	list, err := hmemberlist.Create(config)
	if err != nil {
		return err
	}
	m.memberlist = list

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			if err := list.Leave(time.Minute); err != nil {
				m.log.V(0).Error(err, "failed to leave cluster")
			}
		}()

	LOOP:
		for {
			select {
			case ev := <-ch:
				switch ev.Event {
				case memberlist.NodeJoin:
					if ev.Node.Name == string(m.nodeName) {
						continue
					}
					nodeMeta := nodeMetadata{}
					json.Unmarshal(ev.Node.Meta, &nodeMeta)
					m.log.V(1).Info("node joined", "node", ev.Node.Name, "addr", ev.Node.Addr, "port", ev.Node.Port,
						"metadata", nodeMeta, "state", ev.Node.State)
					d.NotifyJoin(cluster.Node{
						Name:           cluster.Location(ev.Node.Name),
						Addr:           ev.Node.Addr,
						MembershipPort: ev.Node.Port,
						RPCPort:        uint16(nodeMeta.RPCPort),
					})
				case memberlist.NodeLeave:
					if ev.Node.Name == string(m.nodeName) {
						continue
					}
					m.log.V(1).Info("node left", "node", ev.Node, "state", ev.Node.State)
					d.NotifyLeave(cluster.Node{
						Name: cluster.Location(ev.Node.Name),
					})
				case memberlist.NodeUpdate:
					if ev.Node.Name == string(m.nodeName) {
						continue
					}
					m.log.V(1).Info("node updated", "node", ev.Node, "state", ev.Node.State)
				}
			case <-m.closeChan:
				break LOOP
			}
		}
	}()
	return nil
}

func (m *MembershipProtocol) Join(ctx context.Context, nodes []string) error {
	_, err := m.memberlist.Join(nodes)
	return err
}

func (m *MembershipProtocol) Leave(context.Context) error {
	close(m.closeChan)
	m.wg.Wait()
	return nil
}

func (m *MembershipProtocol) ListMembers() ([]cluster.Node, error) {
	members := m.memberlist.Members()
	nodes := make([]cluster.Node, 0, len(members))
	for i := range members {
		if members[i].Name == string(m.nodeName) {
			continue
		}
		nodes = append(nodes, cluster.Node{
			Name:           cluster.Location(members[i].Name),
			Addr:           members[i].Addr,
			MembershipPort: members[i].Port,
		})
	}
	return nodes, nil
}

type channelEventDelegate struct {
	Ch chan<- hmemberlist.NodeEvent
}

func (c *channelEventDelegate) NotifyJoin(n *hmemberlist.Node) {
	node := *n
	select {
	case c.Ch <- hmemberlist.NodeEvent{
		Event: hmemberlist.NodeJoin,
		Node:  &node}:
	default:
	}
}

func (c *channelEventDelegate) NotifyLeave(n *hmemberlist.Node) {
	node := *n
	select {
	case c.Ch <- hmemberlist.NodeEvent{
		Event: hmemberlist.NodeLeave,
		Node:  &node}:
	default:
	}
}

func (c *channelEventDelegate) NotifyUpdate(n *memberlist.Node) {
	node := *n
	select {
	case c.Ch <- memberlist.NodeEvent{
		Event: memberlist.NodeUpdate,
		Node:  &node}:
	default:
	}
}
