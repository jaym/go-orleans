package examples

/*
type ChirperGrainRef interface {
	grain.Addressable
	PublishMessage(context.Context, *PublishMessageRequest) (*PublishMessageResponse, error)
	Subscribe(context.Context, ChirpMessageObserverRef, *SubscribeRequest) error
}

type ChirperGrainActivator interface {
	Activate(ctx context.Context, address grain.Address) (ChirperGrainRef, error)
}

type ChirperGrainClient interface {
	GetByID(ctx context.Context, id string) (ChirperGrainRef, error)
	GetByAddress(ctx context.Context, address grain.Address) (ChirperGrainRef, error)
}

type ChirpMessageObserver interface {
	ChirpMessage(context.Context, *ChirpMessage) error
}

type ChirpMessageObserverRef interface {
	grain.Addressable
	ChirpMessageObserver
}

func CreateChirpMessageObserver(ctx context.Context, o ChirpMessageObserver) (ChirpMessageObserverRef, error) {
	existing, ok := o.(ChirpMessageObserverRef)
	if ok {
		return existing, nil
	}
	// Create Local Grain
	return nil, errors.New("Unimplemented")
}

func ChirpMessageObserverFromAddress(ctx context.Context, client silo.SiloClient, address grain.Address) (ChirpMessageObserverRef, error) {
	return nil, errors.New("Unimplemented")
}

func _ChirperGrain_Activate(activator interface{}, ctx context.Context, address grain.Address) (grain.Addressable, error) {
	return activator.(ChirperGrainActivator).Activate(ctx, address)
}

func _ChirperGrain_PublishMessage_GrainHandler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(PublishMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}

	return srv.(ChirperGrainRef).PublishMessage(ctx, in)
}

func _ChirperGrain_Subscribe_GrainObservableHandler(srv interface{}, ctx context.Context,
	client silo.SiloClient, observerAddress grain.Address, dec func(interface{}) error) error {
	in := new(SubscribeRequest)
	if err := dec(in); err != nil {
		return err
	}

	observer, err := ChirpMessageObserverFromAddress(ctx, client, observerAddress)
	if err != nil {
		return err
	}

	return srv.(ChirperGrainRef).Subscribe(ctx, observer, in)
}

func _ChirpMessageObserver_ChirpMessage_GrainHandler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(ChirpMessage)
	if err := dec(in); err != nil {
		return nil, err
	}

	return nil, srv.(ChirpMessageObserverRef).ChirpMessage(ctx, in)
}

var ChirperGrain_GrainDesc = silo.GrainDescription{
	GrainType: "ChirperGrain",
	Activation: silo.ActivationDesc{
		Handler: _ChirperGrain_Activate,
	},
	Methods: []silo.MethodDesc{
		{
			Name:    "PublishMessage",
			Handler: _ChirperGrain_PublishMessage_GrainHandler,
		},
	},
	Observables: []silo.ObservableDesc{
		{
			Name:    "Subscribe",
			Handler: _ChirperGrain_Subscribe_GrainObservableHandler,
		},
	},
}

func RegisterChirperGrainActivator(registrar silo.Registrar, activator ChirperGrainActivator) {
	registrar.Register(&ChirperGrain_GrainDesc, activator)
}

var ChirpMessageObserver_GrainDesc = silo.GrainDescription{
	Methods: []silo.MethodDesc{
		{
			Name:    "ChirpMessage",
			Handler: _ChirpMessageObserver_ChirpMessage_GrainHandler,
		},
	},
}
*/
