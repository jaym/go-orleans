package cluster

import "github.com/jaym/go-orleans/grain"

type Location string

type GrainAddress struct {
	Location Location
	grain.Identity
}
