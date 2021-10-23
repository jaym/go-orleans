package grain

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidIdentity = errors.New("invalid grain identity")

type Identity struct {
	GrainType string
	ID        string
}

func (m Identity) GetIdentity() Identity {
	return m
}

func (a Identity) String() string {
	return fmt.Sprintf("%s/%s", a.GrainType, a.ID)
}

type GrainReference interface {
	GetIdentity() Identity
}

func (a *Identity) UnmarshalText(text []byte) error {
	s := string(text)
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		a.ID = parts[0]
	} else if len(parts) == 2 {
		a.GrainType = parts[0]
		a.ID = parts[1]
	} else {
		return ErrInvalidIdentity
	}
	return nil
}
