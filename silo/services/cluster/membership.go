package cluster

import (
	"context"
	"net"
)

type Node struct {
	Name string
	Addr net.IP
	Port uint16
}

type MembershipDelegate interface {
	NotifyJoin(Node)
	NotifyLeave(Node)
}

type MembershipProtocol interface {
	Join(context.Context, MembershipDelegate) error
	Leave(context.Context) error
	ListMembers() ([]string, error)
}
