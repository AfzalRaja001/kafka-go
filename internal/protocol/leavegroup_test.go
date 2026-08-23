package protocol

import (
	"testing"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

func encodeLeaveGroupRequest(groupID, memberID string) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.String(memberID)
	return enc.Result()
}

func TestDecodeLeaveGroupRequest(t *testing.T) {
	body := encodeLeaveGroupRequest("g1", "member-1")

	req, err := DecodeLeaveGroupRequest(body)
	if err != nil {
		t.Fatalf("DecodeLeaveGroupRequest: %v", err)
	}
	if req.GroupID != "g1" || req.MemberID != "member-1" {
		t.Fatalf("req = %+v, want {g1, member-1}", req)
	}
}

func decodeLeaveGroupResponse(t *testing.T, resp []byte) int16 {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	errorCode, _ := dec.Int16()
	return errorCode
}

func TestHandleLeaveGroup_KnownMemberSucceedsAndRemovesThem(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	memberID, generationID := joinAndSyncOneMemberViaProtocol(t, coord, "g1")

	resp, err := HandleLeaveGroup(1, encodeLeaveGroupRequest("g1", memberID), coord)
	if err != nil {
		t.Fatalf("HandleLeaveGroup: %v", err)
	}
	if errCode := decodeLeaveGroupResponse(t, resp); errCode != ErrNone {
		t.Errorf("error_code = %d, want %d", errCode, ErrNone)
	}

	// The member is gone: its own old heartbeat now reports unknown member.
	hbResp, err := HandleHeartbeat(2, encodeHeartbeatRequest("g1", generationID, memberID), coord)
	if err != nil {
		t.Fatalf("HandleHeartbeat: %v", err)
	}
	if errCode := decodeHeartbeatResponse(t, hbResp); errCode != ErrUnknownMemberID {
		t.Errorf("post-leave heartbeat error_code = %d, want %d", errCode, ErrUnknownMemberID)
	}
}

func TestHandleLeaveGroup_UnknownMemberReturnsError(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	joinAndSyncOneMemberViaProtocol(t, coord, "g1")

	resp, err := HandleLeaveGroup(1, encodeLeaveGroupRequest("g1", "never-joined"), coord)
	if err != nil {
		t.Fatalf("HandleLeaveGroup: %v", err)
	}
	if errCode := decodeLeaveGroupResponse(t, resp); errCode != ErrUnknownMemberID {
		t.Errorf("error_code = %d, want %d", errCode, ErrUnknownMemberID)
	}
}
