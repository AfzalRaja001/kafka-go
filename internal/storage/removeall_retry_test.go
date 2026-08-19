package storage

import (
	"errors"
	"testing"
	"time"
)

func TestRemoveAllWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	remove := func(path string) error {
		calls++
		if calls < 3 {
			return errors.New("directory is not empty")
		}
		return nil
	}

	if err := removeAllWithRetry(remove, "irrelevant", 5, time.Millisecond); err != nil {
		t.Fatalf("removeAllWithRetry: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRemoveAllWithRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	wantErr := errors.New("persistent failure")
	remove := func(path string) error {
		calls++
		return wantErr
	}

	err := removeAllWithRetry(remove, "irrelevant", 3, time.Millisecond)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRemoveAllWithRetry_SucceedsImmediatelyDoesNotSleep(t *testing.T) {
	calls := 0
	remove := func(path string) error {
		calls++
		return nil
	}

	start := time.Now()
	if err := removeAllWithRetry(remove, "irrelevant", 5, 50*time.Millisecond); err != nil {
		t.Fatalf("removeAllWithRetry: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("first-try success took %v, want no sleep at all", elapsed)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}
