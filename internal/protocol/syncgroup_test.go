package protocol

import (
	"sync"
	"testing"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

func encodeSyncGroupRequest(groupID string, generationID int32, memberID string, assignments map[string][]byte) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.Int32(generationID)
	enc.String(memberID)
	enc.Int32(int32(len(assignments)))
	for id, data := range assignments {
		enc.String(id)
		enc.Bytes(data)
	}
	return enc.Result()
}

func TestDecodeSyncGroupRequest(t *testing.T) {
	body := encodeSyncGroupRequest("g1", 3, "member-1", map[string][]byte{"member-1": []byte("assign-1")})

	req, err := DecodeSyncGroupRequest(body)
	if err != nil {
		t.Fatalf("DecodeSyncGroupRequest: %v", err)
	}
	if req.GroupID != "g1" || req.GenerationID != 3 || req.MemberID != "member-1" {
		t.Fatalf("req = %+v, want {g1, 3, member-1}", req)
	}
	if len(req.Assignments) != 1 || req.Assignments[0].MemberID != "member-1" || string(req.Assignments[0].Data) != "assign-1" {
		t.Fatalf("Assignments = %+v, want one {member-1, assign-1}", req.Assignments)
	}
}

func decodeSyncGroupResponse(t *testing.T, resp []byte) (errorCode int16, assignment []byte) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	errorCode, _ = dec.Int16()
	assignment, _ = dec.Bytes()
	return errorCode, assignment
}

// joinTwoMembersViaProtocol brings two members through a real JoinGroup
// round using the protocol-layer handler (not the group package directly),
// so SyncGroup tests exercise the actual wire path end to end.
func joinTwoMembersViaProtocol(t *testing.T, coord *group.Coordinator, groupID string) (leaderMemberID string, leaderGen int32, followerMemberID string, followerGen int32) {
	t.Helper()

	var wg sync.WaitGroup
	var resp1, resp2 []byte
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp1, _ = HandleJoinGroup(1, encodeJoinGroupRequest(groupID, 100, "", "consumer", []string{"range"}), coord)
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		resp2, _ = HandleJoinGroup(2, encodeJoinGroupRequest(groupID, 100, "", "consumer", []string{"range"}), coord)
	}()
	wg.Wait()

	_, gen1, _, leader1, member1, _ := decodeJoinGroupResponse(t, resp1)
	_, gen2, _, _, member2, _ := decodeJoinGroupResponse(t, resp2)

	if leader1 == member1 {
		return member1, gen1, member2, gen2
	}
	return member2, gen2, member1, gen1
}

func TestHandleSyncGroup_LeaderDistributesAssignments(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	leaderID, gen, followerID, _ := joinTwoMembersViaProtocol(t, coord, "g1")

	var wg sync.WaitGroup
	var leaderResp, followerResp []byte
	wg.Add(2)
	go func() {
		defer wg.Done()
		followerResp, _ = HandleSyncGroup(1, encodeSyncGroupRequest("g1", gen, followerID, nil), coord)
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		leaderResp, _ = HandleSyncGroup(2, encodeSyncGroupRequest("g1", gen, leaderID, map[string][]byte{
			leaderID:   []byte("leader-assignment"),
			followerID: []byte("follower-assignment"),
		}), coord)
	}()
	wg.Wait()

	errCode, assignment := decodeSyncGroupResponse(t, leaderResp)
	if errCode != ErrNone || string(assignment) != "leader-assignment" {
		t.Errorf("leader: error_code=%d assignment=%q, want %d leader-assignment", errCode, assignment, ErrNone)
	}
	errCode, assignment = decodeSyncGroupResponse(t, followerResp)
	if errCode != ErrNone || string(assignment) != "follower-assignment" {
		t.Errorf("follower: error_code=%d assignment=%q, want %d follower-assignment", errCode, assignment, ErrNone)
	}
}

func TestHandleSyncGroup_WrongGenerationReturnsError(t *testing.T) {
	coord := group.NewCoordinator(20 * time.Millisecond)
	leaderID, gen, _, _ := joinTwoMembersViaProtocol(t, coord, "g1")

	resp, err := HandleSyncGroup(1, encodeSyncGroupRequest("g1", gen+1, leaderID, map[string][]byte{leaderID: []byte("x")}), coord)
	if err != nil {
		t.Fatalf("HandleSyncGroup: %v", err)
	}
	errCode, _ := decodeSyncGroupResponse(t, resp)
	if errCode != ErrIllegalGeneration {
		t.Errorf("error_code = %d, want %d", errCode, ErrIllegalGeneration)
	}
}
