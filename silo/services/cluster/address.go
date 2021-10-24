package cluster

import "github.com/jaym/go-orleans/grain"

type Location string

const (
	Local Location = ""
)

type GrainAddress struct {
	Location Location
	grain.Identity
}
