package grain

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidAddress = errors.New("invalid grain address")

type Address struct {
	Location  string
	GrainType string
	ID        string
}

func (m Address) GetAddress() Address {
	return m
}

func (a Address) String() string {
	if a.Location == "" {
		return fmt.Sprintf("%s/%s", a.GrainType, a.ID)
	}
	return fmt.Sprintf("%s/%s/%s", a.Location, a.GrainType, a.ID)
}

type Addressable interface {
	GetAddress() Address
}

func (a *Address) UnmarshalText(text []byte) error {
	s := string(text)
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		a.ID = parts[0]
	} else if len(parts) == 2 {
		a.GrainType = parts[0]
		a.ID = parts[1]
	} else if len(parts) == 3 {
		a.Location = parts[0]
		a.GrainType = parts[1]
		a.ID = parts[2]
	} else {
		return ErrInvalidAddress
	}
	return nil
}
