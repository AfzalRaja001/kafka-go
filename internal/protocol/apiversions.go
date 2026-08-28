package protocol

// Kafka api_key values this broker implements.
const (
	ApiKeyProduce         int16 = 0
	ApiKeyFetch           int16 = 1
	ApiKeyListOffsets     int16 = 2
	ApiKeyMetadata        int16 = 3
	ApiKeyOffsetCommit    int16 = 8
	ApiKeyOffsetFetch     int16 = 9
	ApiKeyFindCoordinator int16 = 10
	ApiKeyJoinGroup       int16 = 11
	ApiKeyHeartbeat       int16 = 12
	ApiKeyLeaveGroup      int16 = 13
	ApiKeySyncGroup       int16 = 14
	ApiKeyApiVersions     int16 = 18
	ApiKeyCreateTopics    int16 = 19
	ApiKeyDeleteTopics    int16 = 20
)

// apiKeyNames maps every api_key this broker implements to a readable name,
// used to label Prometheus metrics per API. Unknown values fall back to a
// fixed string rather than formatting the raw number in - api_key is
// client-controlled input, and echoing an arbitrary number into a metric
// label would let garbage input create unlimited distinct label
// combinations (a cardinality explosion), which real Prometheus deployments
// have been taken down by.
var apiKeyNames = map[int16]string{
	ApiKeyProduce:         "Produce",
	ApiKeyFetch:           "Fetch",
	ApiKeyListOffsets:     "ListOffsets",
	ApiKeyMetadata:        "Metadata",
	ApiKeyOffsetCommit:    "OffsetCommit",
	ApiKeyOffsetFetch:     "OffsetFetch",
	ApiKeyFindCoordinator: "FindCoordinator",
	ApiKeyJoinGroup:       "JoinGroup",
	ApiKeyHeartbeat:       "Heartbeat",
	ApiKeyLeaveGroup:      "LeaveGroup",
	ApiKeySyncGroup:       "SyncGroup",
	ApiKeyApiVersions:     "ApiVersions",
	ApiKeyCreateTopics:    "CreateTopics",
	ApiKeyDeleteTopics:    "DeleteTopics",
}

// ApiKeyName returns a readable name for apiKey, or "Unknown" for anything
// this broker doesn't implement.
func ApiKeyName(apiKey int16) string {
	if name, ok := apiKeyNames[apiKey]; ok {
		return name
	}
	return "Unknown"
}

type SupportedAPI struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// SupportedAPIs lists every API this broker currently implements. Every
// max version is deliberately low so a negotiating client never ends up
// using flexible-version (KIP-482) encoding, which is a fundamentally
// different, varint-based wire format this broker doesn't implement. Three
// APIs land above v0, all for the same reason - v0 genuinely lacks a field
// real clients need: Produce (min=max=3) needs the modern record batch
// format (magic byte 2), Metadata (min=max=1) needs ControllerId so an
// admin client's CreateTopics/DeleteTopics knows which broker to talk to,
// and OffsetFetch (min=max=2) needs a nullable topics array so a client can
// ask for every offset a group has committed instead of naming each
// topic-partition explicitly. Every other API is min=max=0 - "exactly one
// version, the simplest one that does what we need."
var SupportedAPIs = []SupportedAPI{
	{APIKey: ApiKeyProduce, MinVersion: 3, MaxVersion: 3},
	{APIKey: ApiKeyFetch, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyListOffsets, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyApiVersions, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyMetadata, MinVersion: 1, MaxVersion: 1},
	{APIKey: ApiKeyOffsetCommit, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyOffsetFetch, MinVersion: 2, MaxVersion: 2},
	{APIKey: ApiKeyFindCoordinator, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyJoinGroup, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyHeartbeat, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyLeaveGroup, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeySyncGroup, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyCreateTopics, MinVersion: 0, MaxVersion: 0},
	{APIKey: ApiKeyDeleteTopics, MinVersion: 0, MaxVersion: 0},
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
