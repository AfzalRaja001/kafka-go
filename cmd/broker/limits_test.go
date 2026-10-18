package main

import "testing"

func TestLimitsFromEnv_DefaultsAreUnlimited(t *testing.T) {
	got, err := limitsFromEnv(fakeGetenv(nil))
	if err != nil {
		t.Fatalf("limitsFromEnv: %v", err)
	}
	if got.MaxPartitions != 0 || got.RetentionMaxBytes != 0 {
		t.Errorf("limits = %+v, want both 0 (unlimited, the behaviour before the limits existed)", got)
	}
}

func TestLimitsFromEnv_ReadsBothFromEnv(t *testing.T) {
	got, err := limitsFromEnv(fakeGetenv(map[string]string{
		"KAFKA_MAX_PARTITIONS":      "200",
		"KAFKA_RETENTION_MAX_BYTES": "104857600",
	}))
	if err != nil {
		t.Fatalf("limitsFromEnv: %v", err)
	}
	if got.MaxPartitions != 200 || got.RetentionMaxBytes != 104857600 {
		t.Errorf("limits = %+v, want {200 104857600}", got)
	}
}

func TestLimitsFromEnv_RetentionBytesAcceptsValuesBeyondInt32(t *testing.T) {
	got, err := limitsFromEnv(fakeGetenv(map[string]string{"KAFKA_RETENTION_MAX_BYTES": "10737418240"})) // 10 GiB
	if err != nil {
		t.Fatalf("limitsFromEnv: %v", err)
	}
	if got.RetentionMaxBytes != 10737418240 {
		t.Errorf("RetentionMaxBytes = %d, want 10737418240", got.RetentionMaxBytes)
	}
}

// A mistyped limit must stop the broker, not leave it silently unlimited on
// a machine where the limit was the whole point.
func TestLimitsFromEnv_InvalidValuesFailFast(t *testing.T) {
	cases := []map[string]string{
		{"KAFKA_MAX_PARTITIONS": "lots"},
		{"KAFKA_MAX_PARTITIONS": "-1"},
		{"KAFKA_MAX_PARTITIONS": "2147483648"},
		{"KAFKA_MAX_PARTITIONS": "1.5"},
		{"KAFKA_RETENTION_MAX_BYTES": "100MB"},
		{"KAFKA_RETENTION_MAX_BYTES": "-5"},
		{"KAFKA_RETENTION_MAX_BYTES": "9223372036854775808"},
	}
	for _, env := range cases {
		if _, err := limitsFromEnv(fakeGetenv(env)); err == nil {
			t.Errorf("limitsFromEnv(%v) accepted the value, want an error", env)
		}
	}
}
