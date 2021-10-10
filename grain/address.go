package grain

import "fmt"

type Address struct {
	Location  string
	GrainType string
	ID        string
}

func (m Address) GetAddress() Address {
	return m
}

func (a Address) String() string {
	return fmt.Sprintf("%s/%s/%s", a.Location, a.GrainType, a.ID)
}

type Addressable interface {
	GetAddress() Address
}
