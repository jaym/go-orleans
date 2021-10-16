package services

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	"google.golang.org/protobuf/proto"
)

type GrainObserverManager interface {
	List(ctx context.Context, observableName string) ([]grain.RegisteredObserver, error)
	Add(ctx context.Context, observableName string, address grain.Address, val interface{}) (grain.RegisteredObserver, error)
	Notify(ctx context.Context, observableName string, observers []grain.RegisteredObserver, val proto.Message) error
}