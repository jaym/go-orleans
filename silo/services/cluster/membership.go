package cluster

import (
	"context"
	"net"
)

type Node struct {
	Name Location
	Addr net.IP
	Port uint16
}

type MembershipDelegate interface {
	NotifyJoin(Node)
	NotifyLeave(Node)
}

type MembershipProtocolCallbacks struct {
	NotifyJoinFunc  func(Node)
	NotifyLeaveFunc func(Node)
}

func (cb *MembershipProtocolCallbacks) NotifyJoin(node Node) {
	if cb.NotifyJoinFunc != nil {
		cb.NotifyJoinFunc(node)
	}
}

func (cb *MembershipProtocolCallbacks) NotifyLeave(node Node) {
	if cb.NotifyLeaveFunc != nil {
		cb.NotifyLeaveFunc(node)
	}
}

type MembershipProtocol interface {
	Start(context.Context, MembershipDelegate) error
	Join(context.Context, Node) error
	Leave(context.Context) error
	ListMembers() ([]Node, error)
}
