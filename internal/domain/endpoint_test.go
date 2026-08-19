package domain_test

import (
	"testing"
	"time"
	"web-hook-project/internal/domain"
)

func TestEndpoint_Validate(t *testing.T) {
	tests := []struct {
		name     string
		endpoint domain.Endpoint
		wantErr  bool
	}{
		{
			name: "valid endpoint",
			endpoint: domain.Endpoint{
				ID:        "ep_123",
				TenantID:  "tenant_abc",
				URL:       "https://api.example.com/webhook",
				Secret:    "whsec_secret_123",
				RateLimit: 100,
				IsActive:  true,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "missing tenant_id",
			endpoint: domain.Endpoint{
				ID:     "ep_123",
				URL:    "https://api.example.com/webhook",
				Secret: "whsec_secret_123",
			},
			wantErr: true,
		},
		{
			name: "missing url",
			endpoint: domain.Endpoint{
				ID:       "ep_123",
				TenantID: "tenant_abc",
				Secret:   "whsec_secret_123",
			},
			wantErr: true,
		},
		{
			name: "invalid url scheme",
			endpoint: domain.Endpoint{
				ID:       "ep_123",
				TenantID: "tenant_abc",
				URL:      "ftp://api.example.com/webhook",
				Secret:   "whsec_secret_123",
			},
			wantErr: true,
		},
		{
			name: "missing secret",
			endpoint: domain.Endpoint{
				ID:       "ep_123",
				TenantID: "tenant_abc",
				URL:      "https://api.example.com/webhook",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.endpoint.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
