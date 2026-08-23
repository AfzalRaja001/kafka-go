package group

import (
	"testing"
	"time"
)

// joinAndSyncOneMember brings a single member all the way to Stable -
// JoinGroup then SyncGroup, since Heartbeat only makes sense once a group
// has actually finished a rebalance.
func joinAndSyncOneMember(t *testing.T, c *Coordinator, groupID string) JoinResult {
	t.Helper()

	result, err := c.JoinGroup(groupID, "", 50*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if _, err := c.SyncGroup(groupID, result.MemberID, result.GenerationID, []Assignment{
		{MemberID: result.MemberID, Data: []byte("assignment")},
	}); err != nil {
		t.Fatalf("SyncGroup: %v", err)
	}
	return result
}

func TestCoordinator_Heartbeat_KnownMemberStableGroupSucceeds(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	m := joinAndSyncOneMember(t, c, "g1")

	if err := c.Heartbeat("g1", m.MemberID, m.GenerationID); err != nil {
		t.Errorf("Heartbeat: %v", err)
	}
}

func TestCoordinator_Heartbeat_UnknownMemberReturnsError(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	joinAndSyncOneMember(t, c, "g1")

	if err := c.Heartbeat("g1", "never-joined", 1); err != ErrUnknownMember {
		t.Errorf("err = %v, want ErrUnknownMember", err)
	}
}

func TestCoordinator_Heartbeat_WrongGenerationReturnsError(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	m := joinAndSyncOneMember(t, c, "g1")

	if err := c.Heartbeat("g1", m.MemberID, m.GenerationID+1); err != ErrIllegalGeneration {
		t.Errorf("err = %v, want ErrIllegalGeneration", err)
	}
}

func TestCoordinator_LeaveGroup_RemovesMemberAndTriggersRebalance(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	m := joinAndSyncOneMember(t, c, "g1")

	if err := c.LeaveGroup("g1", m.MemberID); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}

	// The member is gone: even its own old generation now reports unknown
	// member, since it left.
	if err := c.Heartbeat("g1", m.MemberID, m.GenerationID); err != ErrUnknownMember {
		t.Errorf("Heartbeat after leaving = %v, want ErrUnknownMember", err)
	}
}

func TestCoordinator_LeaveGroup_RemainingMembersMustRejoin(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)

	// Two members join and sync together.
	results := make(chan JoinResult, 2)
	go func() {
		r, _ := c.JoinGroup("g1", "", 100*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
		results <- r
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		r, _ := c.JoinGroup("g1", "", 100*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
		results <- r
	}()
	r1, r2 := <-results, <-results
	var leader, follower JoinResult
	if r1.Leader == r1.MemberID {
		leader, follower = r1, r2
	} else {
		leader, follower = r2, r1
	}
	c.SyncGroup("g1", leader.MemberID, leader.GenerationID, []Assignment{
		{MemberID: leader.MemberID, Data: []byte("a")},
		{MemberID: follower.MemberID, Data: []byte("b")},
	})
	c.SyncGroup("g1", follower.MemberID, follower.GenerationID, nil)

	// The leader leaves. The follower is still a known member, but the
	// group is no longer Stable - its next Heartbeat must say so.
	if err := c.LeaveGroup("g1", leader.MemberID); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}
	if err := c.Heartbeat("g1", follower.MemberID, follower.GenerationID); err != ErrRebalanceInProgress {
		t.Errorf("follower Heartbeat after leader left = %v, want ErrRebalanceInProgress", err)
	}
}

func TestCoordinator_ReapExpiredMembers_EvictsStaleMemberAndTriggersRebalance(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	m := joinAndSyncOneMember(t, c, "g1")

	// Reap "as of" a time far past the member's session timeout.
	c.ReapExpiredMembers(time.Now().Add(time.Hour))

	if err := c.Heartbeat("g1", m.MemberID, m.GenerationID); err != ErrUnknownMember {
		t.Errorf("Heartbeat after reap = %v, want ErrUnknownMember (evicted)", err)
	}
}

func TestCoordinator_ReapExpiredMembers_LeavesFreshMembersAlone(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)
	m := joinAndSyncOneMember(t, c, "g1")

	c.ReapExpiredMembers(time.Now())

	if err := c.Heartbeat("g1", m.MemberID, m.GenerationID); err != nil {
		t.Errorf("Heartbeat after no-op reap = %v, want nil (member still fresh)", err)
	}
}
