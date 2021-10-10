package services

import (
	"context"
	"errors"
	"time"

	"github.com/jaym/go-orleans/grain"
)

var ErrTimerAlreadyRegistered = errors.New("timer already registered")

type TimerService interface {
	Start()
	Stop(context.Context) error
	RegisterTimer(addr grain.Address, name string, d time.Duration) error
	RegisterTicker(addr grain.Address, name string, d time.Duration) error
	Cancel(addr grain.Address, name string) bool
}
