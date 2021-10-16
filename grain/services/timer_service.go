package services

import (
	"errors"
	"time"
)

var ErrTimerAlreadyRegistered = errors.New("timer already registered")

type GrainTimerService interface {
	Trigger(name string)
	RegisterTimer(name string, d time.Duration, f func()) error
	RegisterTicker(name string, d time.Duration, f func()) error
	Cancel(name string) bool
}
