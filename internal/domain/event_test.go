package domain_test

import (
	"testing"
	"time"
	"web-hook-project/internal/domain"
)

func TestEvent_Validate(t *testing.T) {
	tests := []struct {
		name    string
		event   domain.Event
		wantErr bool
	}{
		{
			name: "valid event",
			event: domain.Event{
				ID:             "evt_123",
				TenantID:       "tenant_abc",
				EventType:      "payment.succeeded",
				Payload:        []byte(`{"amount": 1000}`),
				IdempotencyKey: "key_xyz",
				Status:         domain.EventStatusPending,
				CreatedAt:      time.Now(),
			},
			wantErr: false,
		},
		{
			name: "missing tenant",
			event: domain.Event{
				ID:        "evt_123",
				EventType: "payment.succeeded",
				Payload:   []byte(`{"amount": 1000}`),
			},
			wantErr: true,
		},
		{
			name: "missing event_type",
			event: domain.Event{
				ID:       "evt_123",
				TenantID: "tenant_abc",
				Payload:  []byte(`{"amount": 1000}`),
			},
			wantErr: true,
		},
		{
			name: "empty payload",
			event: domain.Event{
				ID:        "evt_123",
				TenantID:  "tenant_abc",
				EventType: "payment.succeeded",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
