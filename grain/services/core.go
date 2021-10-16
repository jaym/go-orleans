package services

import "github.com/jaym/go-orleans/grain"

type CoreGrainServices interface {
	TimerService() GrainTimerService
	SiloClient() grain.SiloClient
}
