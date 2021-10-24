package cluster

import "github.com/jaym/go-orleans/grain"

type Transport interface {
	EnqueueInvokeMethodRequest(sender grain.Identity, receiver grain.Identity, method string, uuid string, payload []byte) error
	EnqueueRegisterObserverRequest(observer grain.Identity, observable grain.Identity, name string, payload []byte) error
	EnqueueObserverNotification(sender grain.Identity, receivers []grain.Identity, name string, payload []byte) error
}
