// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import (
	descriptor "github.com/jaym/go-orleans/grain/descriptor"
	mock "github.com/stretchr/testify/mock"
)

// Registrar is an autogenerated mock type for the Registrar type
type Registrar struct {
	mock.Mock
}

// Lookup provides a mock function with given fields: grainType
func (_m *Registrar) Lookup(grainType string) (*descriptor.GrainDescription, interface{}, error) {
	ret := _m.Called(grainType)

	var r0 *descriptor.GrainDescription
	if rf, ok := ret.Get(0).(func(string) *descriptor.GrainDescription); ok {
		r0 = rf(grainType)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*descriptor.GrainDescription)
		}
	}

	var r1 interface{}
	if rf, ok := ret.Get(1).(func(string) interface{}); ok {
		r1 = rf(grainType)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(interface{})
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(string) error); ok {
		r2 = rf(grainType)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// Register provides a mock function with given fields: desc, impl
func (_m *Registrar) Register(desc *descriptor.GrainDescription, impl interface{}) {
	_m.Called(desc, impl)
}

type mockConstructorTestingTNewRegistrar interface {
	mock.TestingT
	Cleanup(func())
}

// NewRegistrar creates a new instance of Registrar. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewRegistrar(t mockConstructorTestingTNewRegistrar) *Registrar {
	mock := &Registrar{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}