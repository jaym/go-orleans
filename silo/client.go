package silo

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	gcontext "github.com/jaym/go-orleans/context"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	"github.com/jaym/go-orleans/grain/generic"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/segmentio/ksuid"
)

type siloClientImpl struct {
	log              logr.Logger
	codecV2          codec.CodecV2
	transportManager *transport.Manager
	nodeName         cluster.Location
	grainDirectory   cluster.GrainDirectory
	registrar        descriptor.Registrar
}

func (s *siloClientImpl) getGrainAddress(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	if generic.IsGenericGrain(ident) {
		location, err := generic.ParseIdentity(ident)
		if err != nil {
			return cluster.GrainAddress{}, err
		}
		return cluster.GrainAddress{
			Location: cluster.Location(location),
			Identity: ident,
		}, nil
	}
	ac, err := s.registrar.Lookup(ident.GrainType)
	if err != nil {
		return cluster.GrainAddress{}, err
	}
	if ac.IsStatelessWorker {
		return cluster.GrainAddress{
			Location: cluster.Location(s.nodeName),
			Identity: ident,
		}, nil
	}
	grainAddress, err := s.grainDirectory.Lookup(ctx, ident)
	if err != nil {
		if err == cluster.ErrGrainActivationNotFound {
			return s.placeGrain(ctx, ident)
		}
		return cluster.GrainAddress{}, err
	}
	return grainAddress, nil
}

func (s *siloClientImpl) placeGrain(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
	nodeName := s.transportManager.RandomNode()
	return cluster.GrainAddress{
		Location: cluster.Location(nodeName),
		Identity: ident,
	}, nil
}

func (s *siloClientImpl) InvokeMethodV2(ctx context.Context, receiver grain.Identity, method string,
	ser func(grain.Serializer) error) grain.InvokeMethodFutureV2 {
	id := ksuid.New().String()
	log := s.log.WithValues("uuid", id, "receiver", receiver, "method", method)

	log.V(4).Info("InvokeMethod")

	sender := gcontext.IdentityFromContext(ctx)
	if sender == nil {
		g := grain.Anonymous()
		sender = &g
	}

	fser := s.codecV2.Pack()
	if err := ser(fser); err != nil {
		s.log.V(0).Error(err, "failed to serialize method arguments")
		return invokeMethodFailedFutureV2{err: err}
	}
	bytes, err := fser.ToBytes()
	if err != nil {
		s.log.V(0).Error(err, "failed to serialize method arguments")
		return invokeMethodFailedFutureV2{err: err}
	}

	addr, err := s.getGrainAddress(ctx, receiver)
	if err != nil {
		return invokeMethodFailedFutureV2{err: err}
	}

	f, err := s.transportManager.InvokeMethod(ctx, *sender, addr, method, id, bytes)
	if err != nil {
		return invokeMethodFailedFutureV2{err: err}
	}

	return newInvokeMethodFutureV2(s.codecV2, f)
}

func (s *siloClientImpl) InvokeOneWayMethod(ctx context.Context, receivers []grain.Identity, method string,
	ser func(grain.Serializer) error) {

	if len(receivers) == 0 {
		return
	}

	sender := gcontext.IdentityFromContext(ctx)
	if sender == nil {
		a := grain.Anonymous()
		sender = &a
	}

	fser := s.codecV2.Pack()
	if err := ser(fser); err != nil {
		s.log.V(0).Error(err, "failed to serialize method arguments")
		return
	}
	data, err := fser.ToBytes()
	if err != nil {
		s.log.V(0).Error(err, "failed to serialize method arguments")
		return
	}

	receiverAddrs := make([]cluster.GrainAddress, 0, len(receivers))
	var grainAddrErrs error
	for _, r := range receivers {
		addr, err := s.getGrainAddress(ctx, r)
		if err != nil {
			s.log.V(0).Error(err, "failed to find grain address", "grain", r)
			grainAddrErrs = errors.CombineErrors(grainAddrErrs, err)
			continue
		}
		receiverAddrs = append(receiverAddrs, addr)
	}
	err = s.transportManager.InvokeMethodOneWay(ctx, *sender, receiverAddrs, method, data)
	if err != nil {
		s.log.V(0).Error(err, "failed to invoke one way method")
		return
	}
}
