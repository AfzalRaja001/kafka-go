package protocol

import "testing"

func TestHandleApiVersions_SupportedVersion(t *testing.T) {
	resp := HandleApiVersions(42, 0)
	dec := NewDecoder(resp)

	correlationID, _ := dec.Int32()
	if correlationID != 42 {
		t.Fatalf("correlation_id = %d, want 42", correlationID)
	}
	errorCode, _ := dec.Int16()
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	count, _ := dec.Int32()
	if int(count) != len(SupportedAPIs) {
		t.Fatalf("api count = %d, want %d", count, len(SupportedAPIs))
	}
	for i := 0; i < int(count); i++ {
		apiKey, _ := dec.Int16()
		minV, _ := dec.Int16()
		maxV, _ := dec.Int16()
		want := SupportedAPIs[i]
		if apiKey != want.APIKey || minV != want.MinVersion || maxV != want.MaxVersion {
			t.Errorf("entry %d = (%d,%d,%d), want (%d,%d,%d)",
				i, apiKey, minV, maxV, want.APIKey, want.MinVersion, want.MaxVersion)
		}
	}
}

func TestHandleApiVersions_UnsupportedVersionFallsBackToV0(t *testing.T) {
	// Client asks for a version far higher than we support.
	resp := HandleApiVersions(7, 9)
	dec := NewDecoder(resp)

	correlationID, _ := dec.Int32()
	if correlationID != 7 {
		t.Fatalf("correlation_id = %d, want 7", correlationID)
	}
	errorCode, _ := dec.Int16()
	if errorCode != ErrUnsupportedVersion {
		t.Fatalf("error_code = %d, want %d (UNSUPPORTED_VERSION)", errorCode, ErrUnsupportedVersion)
	}
	// Even on the error path, the supported API list must still be present
	// and readable - that's what lets the client retry at a version we
	// actually accept, instead of just failing.
	count, err := dec.Int32()
	if err != nil || int(count) != len(SupportedAPIs) {
		t.Fatalf("api count = %d, %v, want %d", count, err, len(SupportedAPIs))
	}
}
