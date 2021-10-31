package silo

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	gcontext "github.com/jaym/go-orleans/context"
	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/plugins/codec"
	"github.com/jaym/go-orleans/silo/internal/transport"
	"github.com/jaym/go-orleans/silo/services/cluster"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/proto"
)

type siloClientImpl struct {
	log              logr.Logger
	codec            codec.Codec
	transportManager *transport.Manager
	nodeName         cluster.Location
	grainDirectory   cluster.GrainDirectory
}

func (s *siloClientImpl) getGrainAddress(ctx context.Context, ident grain.Identity) (cluster.GrainAddress, error) {
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
	return cluster.GrainAddress{
		Location: cluster.Location(s.nodeName),
		Identity: ident,
	}, nil
}

func (s *siloClientImpl) InvokeMethod(ctx context.Context, receiver grain.Identity, grainType string, method string,
	in proto.Message) grain.InvokeMethodFuture {
	id := ksuid.New().String()
	log := s.log.WithValues("uuid", id, "receiver", receiver, "grainType", grainType, "method", method)

	log.V(4).Info("InvokeMethod")

	sender := gcontext.IdentityFromContext(ctx)
	if sender == nil {
		log.Info("no sender in context")
		// TODO: generate anonymous identity
		panic("no sender")
	}
	bytes, err := proto.Marshal(in)
	if err != nil {
		log.Error(err, "failed to marshal")
		// TODO: error handling
		panic(err)
	}

	addr, err := s.getGrainAddress(ctx, receiver)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	f, err := s.transportManager.InvokeMethod(ctx, *sender, addr, method, id, bytes)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	return newInvokeMethodFuture(s.codec, f)
}

func (s *siloClientImpl) RegisterObserver(ctx context.Context, observer grain.Identity, observable grain.Identity,
	name string, in proto.Message) grain.RegisterObserverFuture {

	id := ksuid.New().String()

	log := s.log.WithValues("uuid", id, "observer", observer, "observable", observable, "observableName", name)
	log.V(4).Info("RegisterObserver")

	data, err := proto.Marshal(in)
	if err != nil {
		log.Error(err, "failed to marshal")
		panic(err)
	}

	addr, err := s.getGrainAddress(ctx, observable)
	if err != nil {
		// TODO: error handling
		panic(err)
	}

	f, err := s.transportManager.RegisterObserver(ctx, observer, addr, name, id, data)
	if err != nil {
		panic(err)
	}

	return newRegisterObserverFuture(s.codec, f)
}

func (s *siloClientImpl) NotifyObservers(ctx context.Context, observableType string, observableName string,
	receivers []grain.Identity, out proto.Message) error {

	log := s.log.WithValues("recievers", receivers)
	log.V(4).Info("NotifyObservers")

	if len(receivers) == 0 {
		return nil
	}

	sender := gcontext.IdentityFromContext(ctx)
	if sender == nil {
		log.Info("no sender in context")
		panic("no sender")
	}

	data, err := proto.Marshal(out)
	if err != nil {
		log.Error(err, "failed to marshal")
		return err
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

	err2 := s.transportManager.ObserverNotificationAsync(ctx, *sender, receiverAddrs, observableType, observableName, data)
	if err2 != nil {
		s.log.V(0).Error(err, "failed to notify grains")
	}
	return errors.CombineErrors(err2, grainAddrErrs)
}
