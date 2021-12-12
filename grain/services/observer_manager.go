package services

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
	"google.golang.org/protobuf/proto"
)

type GrainObserverManager interface {
	List(ctx context.Context, observableName string) ([]grain.RegisteredObserver, error)
	Add(ctx context.Context, observableName string, identity grain.Identity, registrationTimeout time.Duration, val proto.Message) (grain.RegisteredObserver, error)
	Remove(ctx context.Context, observableName string, identity grain.Identity) error
	Notify(ctx context.Context, observableName string, observers []grain.RegisteredObserver, val proto.Message) error
}
