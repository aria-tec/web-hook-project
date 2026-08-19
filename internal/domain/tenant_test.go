package domain_test

import (
	"testing"
	"time"
	"web-hook-project/internal/domain"
)

func TestTenant_Validate(t *testing.T) {
	tests := []struct {
		name    string
		tenant  domain.Tenant
		wantErr bool
	}{
		{
			name: "valid tenant",
			tenant: domain.Tenant{
				ID:        "tenant_001",
				Name:      "ACME Corp",
				CreatedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "missing id",
			tenant: domain.Tenant{
				Name: "ACME Corp",
			},
			wantErr: true,
		},
		{
			name: "missing name",
			tenant: domain.Tenant{
				ID: "tenant_001",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tenant.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
