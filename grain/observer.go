package grain

type RegisteredObserver interface {
	Addressable
	Get(interface{}) error
}
