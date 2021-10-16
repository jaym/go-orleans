package grain

type RegisteredObserver interface {
	Addressable
	ObservableName() string
	Get(interface{}) error
}
