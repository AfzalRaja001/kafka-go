package protocol

import (
	"sync"
	"testing"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

func encodeJoinGroupRequest(groupID string, sessionTimeoutMs int32, memberID, protocolType string, protocolNames []string) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.Int32(sessionTimeoutMs)
	enc.String(memberID)
	enc.String(protocolType)
	enc.Int32(int32(len(protocolNames)))
	for _, name := range protocolNames {
		enc.String(name)
		enc.Bytes([]byte("meta-" + name))
	}
	return enc.Result()
}

func TestDecodeJoinGroupRequest(t *testing.T) {
	body := encodeJoinGroupRequest("g1", 5000, "", "consumer", []string{"range"})

	req, err := DecodeJoinGroupRequest(body)
	if err != nil {
		t.Fatalf("DecodeJoinGroupRequest: %v", err)
	}
	if req.GroupID != "g1" || req.SessionTimeoutMs != 5000 || req.MemberID != "" || req.ProtocolType != "consumer" {
		t.Fatalf("req = %+v, want {g1, 5000, \"\", consumer}", req)
	}
	if len(req.Protocols) != 1 || req.Protocols[0].Name != "range" || string(req.Protocols[0].Metadata) != "meta-range" {
		t.Fatalf("Protocols = %+v, want one {range, meta-range}", req.Protocols)
	}
}

func decodeJoinGroupResponse(t *testing.T, resp []byte) (errorCode int16, generationID int32, protocolName, leader, memberID string, members []group.MemberInfo) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	errorCode, _ = dec.Int16()
	generationID, _ = dec.Int32()
	protocolName, _ = dec.String()
	leader, _ = dec.String()
	memberID, _ = dec.String()
	count, _ := dec.Int32()
	for i := int32(0); i < count; i++ {
		id, _ := dec.String()
		meta, _ := dec.Bytes()
		members = append(members, group.MemberInfo{MemberID: id, Metadata: meta})
	}
	return errorCode, generationID, protocolName, leader, memberID, members
}

func TestHandleJoinGroup_SingleMemberBecomesLeader(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	body := encodeJoinGroupRequest("g1", 50, "", "consumer", []string{"range"})

	resp, err := HandleJoinGroup(1, body, coord)
	if err != nil {
		t.Fatalf("HandleJoinGroup: %v", err)
	}

	errorCode, generationID, protocolName, leader, memberID, members := decodeJoinGroupResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if generationID != 1 || protocolName != "range" {
		t.Errorf("generationID, protocolName = %d, %q, want 1, range", generationID, protocolName)
	}
	if leader != memberID {
		t.Errorf("leader = %q, memberID = %q, want equal (only member)", leader, memberID)
	}
	if len(members) != 1 || members[0].MemberID != memberID {
		t.Errorf("members = %+v, want just %q", members, memberID)
	}
}

func TestHandleJoinGroup_NoCommonProtocolReturnsError(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)

	var wg sync.WaitGroup
	var resp1, resp2 []byte
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp1, _ = HandleJoinGroup(1, encodeJoinGroupRequest("g1", 50, "", "consumer", []string{"range"}), coord)
	}()
	go func() {
		defer wg.Done()
		resp2, _ = HandleJoinGroup(2, encodeJoinGroupRequest("g1", 50, "", "consumer", []string{"roundrobin"}), coord)
	}()
	wg.Wait()

	for _, resp := range [][]byte{resp1, resp2} {
		errorCode, _, _, _, _, _ := decodeJoinGroupResponse(t, resp)
		if errorCode != ErrInconsistentGroupProtocol {
			t.Errorf("error_code = %d, want %d", errorCode, ErrInconsistentGroupProtocol)
		}
	}
}
