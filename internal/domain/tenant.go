package domain

import (
	"errors"
	"time"
)

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (t *Tenant) Validate() error {
	if t.ID == "" {
		return errors.New("id is required")
	}
	if t.Name == "" {
		return errors.New("name is required")
	}
	return nil
}
