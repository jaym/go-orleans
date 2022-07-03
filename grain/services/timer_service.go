package services

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

var ErrTimerAlreadyRegistered = errors.New("timer already registered")

type GrainTimerService interface {
	Trigger(ctx context.Context, name string)
	RegisterTimer(name string, d time.Duration, f func(context.Context)) error
	RegisterTicker(name string, d time.Duration, f func(context.Context)) error
	Cancel(name string) bool
}
