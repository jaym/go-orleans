package grain

type RegisteredObserver interface {
	GrainReference
	ObservableName() string
	Get(interface{}) error
}
