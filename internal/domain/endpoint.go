package domain

import (
	"errors"
	"net/url"
	"time"
)

type Endpoint struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	URL       string    `json:"url"`
	Secret    string    `json:"secret"`
	RateLimit int       `json:"rate_limit"` // Max requests per second
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (e *Endpoint) Validate() error {
	if e.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if e.URL == "" {
		return errors.New("url is required")
	}
	u, err := url.ParseRequestURI(e.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("invalid url: must have http or https scheme")
	}
	if e.Secret == "" {
		return errors.New("secret is required")
	}
	return nil
}
