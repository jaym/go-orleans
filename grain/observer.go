package grain

import "time"

type RegisteredObserver interface {
	GrainReference
	ObservableName() string
	ExpiresAt() time.Time
	Get(interface{}) error
}
