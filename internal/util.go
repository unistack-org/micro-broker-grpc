package internal

import (
	"github.com/google/uuid"
)

type AutoId struct {
	uid uuid.UUID
}

func (ai *AutoId) GetID() string {
	return ai.uid.String()
}

func NewAutoId() AutoId {
	uid := uuid.New()
	return AutoId{
		uid: uid,
	}
}
