package protocol

// Kafka api_key values this broker implements.
const (
	ApiKeyProduce     int16 = 0
	ApiKeyMetadata    int16 = 3
	ApiKeyApiVersions int16 = 18
)

type SupportedAPI struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// SupportedAPIs lists every API this broker currently implements. Every
// max version is deliberately low (0) so a negotiating client never ends
// up using flexible-version (KIP-482) encoding, which is a fundamentally
// different, varint-based wire format this broker doesn't implement.
// Produce is the one exception to "everything is v0": versions 0-2 use
// Kafka's old message-set format, and only v3+ requires the modern record
// batch format (magic byte 2) this broker implements. min=max=3 - "exactly
// one version, the simplest one that does what we need" - the same pattern
// as every other API here, just landing on a different version.
var SupportedAPIs = []SupportedAPI{
	{APIKey: ApiKeyProduce, MinVersion: 3, MaxVersion: 3},
	{APIKey: ApiKeyApiVersions, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyMetadata, MinVersion: 0, MaxVersion: 0},
}

const maxSupportedApiVersionsVersion = 0

// HandleApiVersions builds an ApiVersions v0 response body (not including
// the 4-byte length prefix broker.WriteMessage adds separately).
//
// requestedVersion is the api_version field from the request header. If
// it's higher than what this broker supports, the response still uses the
// v0 format but with error code ErrUnsupportedVersion - the special-case
// fallback the ApiVersions negotiation protocol requires, so the client
// knows what to retry at instead of just failing to connect.
func HandleApiVersions(correlationID int32, requestedVersion int16) []byte {
	errorCode := ErrNone
	if requestedVersion > maxSupportedApiVersionsVersion {
		errorCode = ErrUnsupportedVersion
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int16(errorCode)
	enc.Int32(int32(len(SupportedAPIs)))
	for _, api := range SupportedAPIs {
		enc.Int16(api.APIKey)
		enc.Int16(api.MinVersion)
		enc.Int16(api.MaxVersion)
	}
	return enc.Result()
}
