package services

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

var ErrTimerAlreadyRegistered = errors.New("timer already registered")

type GrainTimerService interface {
	RegisterTimer(name string, d time.Duration, f func(context.Context))
	RegisterTicker(name string, d time.Duration, f func(context.Context))
	Cancel(name string) bool
}
