package grain

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

var ErrInvalidIdentity = errors.New("invalid grain identity")

type Identity struct {
	GrainType string
	ID        string
}

func (m Identity) GetIdentity() Identity {
	return m
}

func (a Identity) String() string {
	return fmt.Sprintf("%s/%s", a.GrainType, a.ID)
}

type GrainReference interface {
	GetIdentity() Identity
}

func (a *Identity) UnmarshalText(text []byte) error {
	s := string(text)
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		a.ID = parts[0]
	} else if len(parts) == 2 {
		a.GrainType = parts[0]
		a.ID = parts[1]
	} else {
		return ErrInvalidIdentity
	}
	return nil
}

func Anonymous() Identity {
	return Identity{
		GrainType: "_anonymous_",
	}
}

type ObserverRegistrationToken struct {
	Observer       Identity
	ObservableName string
	ID             string
	Expires        time.Time
}

func (t ObserverRegistrationToken) String() string {
	return fmt.Sprintf("%s#%s#%s#%d", t.ObservableName, t.Observer, t.ID, t.Expires.Unix())
}

func (t *ObserverRegistrationToken) UnmarshalText(text []byte) error {
	parts := bytes.SplitN(text, []byte{'#'}, 4)
	if len(parts) != 3 {
		return errors.New("invalid registration token")
	}
	t.ObservableName = string(parts[0])
	if err := t.Observer.UnmarshalText(parts[1]); err != nil {
		return err
	}
	t.ID = string(parts[2])

	expiresInt, err := strconv.Atoi(string(parts[3]))
	if err != nil {
		return errors.New("invalid registration token")
	}
	t.Expires = time.Unix(int64(expiresInt), 0)
	return nil
}
