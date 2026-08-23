package group

import (
	"sync"
	"testing"
	"time"
)

// joinTwoMembers is a test helper: brings a fresh group through a
// successful two-member JoinGroup round and returns both results, with
// results[0] guaranteed to be the leader (it joined first).
func joinTwoMembers(t *testing.T, c *Coordinator, groupID string) (leader, follower JoinResult) {
	t.Helper()

	var wg sync.WaitGroup
	results := make([]JoinResult, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		var err error
		results[0], err = c.JoinGroup(groupID, "", 100*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
		if err != nil {
			t.Errorf("member 0 JoinGroup: %v", err)
		}
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		var err error
		results[1], err = c.JoinGroup(groupID, "", 100*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
		if err != nil {
			t.Errorf("member 1 JoinGroup: %v", err)
		}
	}()
	wg.Wait()

	if results[0].Leader != results[0].MemberID {
		t.Fatalf("expected member 0 (the first joiner) to be leader; results = %+v", results)
	}
	return results[0], results[1]
}

func TestCoordinator_SyncGroup_LeaderDistributesAssignments(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	leader, follower := joinTwoMembers(t, c, "g1")

	var wg sync.WaitGroup
	var leaderAssignment, followerAssignment []byte
	var leaderErr, followerErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		followerAssignment, followerErr = c.SyncGroup("g1", follower.MemberID, follower.GenerationID, nil)
	}()
	time.Sleep(5 * time.Millisecond) // follower waits first, then leader submits
	go func() {
		defer wg.Done()
		leaderAssignment, leaderErr = c.SyncGroup("g1", leader.MemberID, leader.GenerationID, []Assignment{
			{MemberID: leader.MemberID, Data: []byte("leader-assignment")},
			{MemberID: follower.MemberID, Data: []byte("follower-assignment")},
		})
	}()
	wg.Wait()

	if leaderErr != nil {
		t.Fatalf("leader SyncGroup: %v", leaderErr)
	}
	if followerErr != nil {
		t.Fatalf("follower SyncGroup: %v", followerErr)
	}
	if string(leaderAssignment) != "leader-assignment" {
		t.Errorf("leader assignment = %q, want leader-assignment", leaderAssignment)
	}
	if string(followerAssignment) != "follower-assignment" {
		t.Errorf("follower assignment = %q, want follower-assignment", followerAssignment)
	}
}

func TestCoordinator_SyncGroup_WrongGenerationReturnsError(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	leader, _ := joinTwoMembers(t, c, "g1")

	_, err := c.SyncGroup("g1", leader.MemberID, leader.GenerationID+1, []Assignment{
		{MemberID: leader.MemberID, Data: []byte("x")},
	})
	if err != ErrIllegalGeneration {
		t.Errorf("err = %v, want ErrIllegalGeneration", err)
	}
}

func TestCoordinator_SyncGroup_UnknownMemberReturnsError(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	leader, _ := joinTwoMembers(t, c, "g1")

	_, err := c.SyncGroup("g1", "never-joined", leader.GenerationID, nil)
	if err != ErrUnknownMember {
		t.Errorf("err = %v, want ErrUnknownMember", err)
	}
}
