package grain

import "time"

type RegisteredObserver interface {
	GrainReference
	ExpiresAt() time.Time
	Get(interface{}) error
}
