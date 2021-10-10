package grain

type RegisteredObserver interface {
	Addressable
	UUID() string
	Get(interface{}) error
}
