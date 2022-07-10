package resourcemanager

import (
	"github.com/cockroachdb/errors"

	"github.com/jaym/go-orleans/grain"
)

type ResourceManager interface {
	Touch(grainAddr grain.Identity) error
	Remove(grainAddr grain.Identity) error
}

var ErrNoCapacity = errors.New("No capacity")
