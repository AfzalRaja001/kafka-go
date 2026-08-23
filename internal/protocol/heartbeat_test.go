package protocol

import (
	"testing"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

func encodeHeartbeatRequest(groupID string, generationID int32, memberID string) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.Int32(generationID)
	enc.String(memberID)
	return enc.Result()
}

func TestDecodeHeartbeatRequest(t *testing.T) {
	body := encodeHeartbeatRequest("g1", 3, "member-1")

	req, err := DecodeHeartbeatRequest(body)
	if err != nil {
		t.Fatalf("DecodeHeartbeatRequest: %v", err)
	}
	if req.GroupID != "g1" || req.GenerationID != 3 || req.MemberID != "member-1" {
		t.Fatalf("req = %+v, want {g1, 3, member-1}", req)
	}
}

func decodeHeartbeatResponse(t *testing.T, resp []byte) int16 {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	errorCode, _ := dec.Int16()
	return errorCode
}

// joinAndSyncOneMemberViaProtocol is the protocol-layer equivalent of the
// group package's own test helper of the same shape - brings a single
// member all the way to Stable via the real wire-level handlers.
func joinAndSyncOneMemberViaProtocol(t *testing.T, coord *group.Coordinator, groupID string) (memberID string, generationID int32) {
	t.Helper()

	resp, err := HandleJoinGroup(1, encodeJoinGroupRequest(groupID, 100, "", "consumer", []string{"range"}), coord)
	if err != nil {
		t.Fatalf("HandleJoinGroup: %v", err)
	}
	_, generationID, _, _, memberID, _ = decodeJoinGroupResponse(t, resp)

	if _, err := HandleSyncGroup(2, encodeSyncGroupRequest(groupID, generationID, memberID, map[string][]byte{memberID: []byte("a")}), coord); err != nil {
		t.Fatalf("HandleSyncGroup: %v", err)
	}
	return memberID, generationID
}

func TestHandleHeartbeat_KnownMemberStableGroupSucceeds(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	memberID, generationID := joinAndSyncOneMemberViaProtocol(t, coord, "g1")

	resp, err := HandleHeartbeat(1, encodeHeartbeatRequest("g1", generationID, memberID), coord)
	if err != nil {
		t.Fatalf("HandleHeartbeat: %v", err)
	}
	if errCode := decodeHeartbeatResponse(t, resp); errCode != ErrNone {
		t.Errorf("error_code = %d, want %d", errCode, ErrNone)
	}
}

func TestHandleHeartbeat_UnknownMemberReturnsError(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	joinAndSyncOneMemberViaProtocol(t, coord, "g1")

	resp, err := HandleHeartbeat(1, encodeHeartbeatRequest("g1", 1, "never-joined"), coord)
	if err != nil {
		t.Fatalf("HandleHeartbeat: %v", err)
	}
	if errCode := decodeHeartbeatResponse(t, resp); errCode != ErrUnknownMemberID {
		t.Errorf("error_code = %d, want %d", errCode, ErrUnknownMemberID)
	}
}
