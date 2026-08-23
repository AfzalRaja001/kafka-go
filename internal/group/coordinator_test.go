package group

import (
	"sync"
	"testing"
	"time"
)

// TestCoordinator_JoinGroup_UsesInitialRebalanceDelayNotSessionTimeout
// pins down a bug live-testing found: the join window must be governed by
// the broker's own initial-rebalance-delay setting (matching real Kafka's
// group.initial.rebalance.delay.ms, 3s by default), not by whatever
// SessionTimeoutMs the client happens to send. kafka-python's real default
// session timeout is 30 seconds - if that governed the window, a first-time
// joiner would wait 30 seconds for a response, and any client-side poll
// timeout shorter than that (every real one is) gives up first.
func TestCoordinator_JoinGroup_UsesInitialRebalanceDelayNotSessionTimeout(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)

	done := make(chan JoinResult, 1)
	go func() {
		result, err := c.JoinGroup("g1", "", 10*time.Second, "consumer", []Protocol{{Name: "range"}})
		if err != nil {
			t.Errorf("JoinGroup: %v", err)
		}
		done <- result
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("JoinGroup did not return within 200ms - it's waiting on the 10s SessionTimeoutMs instead of the short initial rebalance delay")
	}
}

func TestCoordinator_JoinGroup_SingleMemberBecomesLeader(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)

	result, err := c.JoinGroup("g1", "", 50*time.Millisecond, "consumer", []Protocol{{Name: "range", Metadata: []byte("meta")}})
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}

	if result.MemberID == "" {
		t.Fatal("MemberID should be assigned when the request sent an empty one")
	}
	if result.Leader != result.MemberID {
		t.Errorf("Leader = %q, want %q (the only member)", result.Leader, result.MemberID)
	}
	if result.GenerationID != 1 {
		t.Errorf("GenerationID = %d, want 1 (first successful round)", result.GenerationID)
	}
	if result.ProtocolName != "range" {
		t.Errorf("ProtocolName = %q, want range", result.ProtocolName)
	}
	if len(result.Members) != 1 || result.Members[0].MemberID != result.MemberID {
		t.Errorf("Members = %+v, want just the leader itself", result.Members)
	}
}

// TestCoordinator_JoinGroup_MultipleMembersJoinWithinWindow_AllGetSameGeneration
// is the core concurrency behavior: several goroutines calling JoinGroup for
// the same group must all unblock together, once, with a consistent
// generation - not one at a time, and not with mismatched generations.
func TestCoordinator_JoinGroup_MultipleMembersJoinWithinWindow_AllGetSameGeneration(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)

	const n = 3
	results := make([]JoinResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.JoinGroup("g1", "", 100*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
		}(i)
		time.Sleep(5 * time.Millisecond) // stagger joins so they land within the same window, not simultaneously
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("member %d: JoinGroup: %v", i, err)
		}
	}

	gen := results[0].GenerationID
	leaderCount := 0
	seenMemberIDs := map[string]bool{}
	for i, r := range results {
		if r.GenerationID != gen {
			t.Errorf("member %d generation = %d, want %d (all members in one round share a generation)", i, r.GenerationID, gen)
		}
		if seenMemberIDs[r.MemberID] {
			t.Errorf("member %d got a duplicate MemberID %q", i, r.MemberID)
		}
		seenMemberIDs[r.MemberID] = true
		if r.Leader == r.MemberID {
			leaderCount++
			if len(r.Members) != n {
				t.Errorf("leader's Members = %+v, want all %d members", r.Members, n)
			}
		} else if len(r.Members) != 0 {
			t.Errorf("member %d (not leader) got a non-empty Members list: %+v", i, r.Members)
		}
	}
	if leaderCount != 1 {
		t.Errorf("leaderCount = %d, want exactly 1", leaderCount)
	}
}

func TestCoordinator_JoinGroup_NoCommonProtocolReturnsError(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = c.JoinGroup("g1", "", 50*time.Millisecond, "consumer", []Protocol{{Name: "range"}})
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		_, errs[1] = c.JoinGroup("g1", "", 50*time.Millisecond, "consumer", []Protocol{{Name: "roundrobin"}})
	}()
	wg.Wait()

	for i, err := range errs {
		if err != ErrInconsistentProtocol {
			t.Errorf("member %d error = %v, want ErrInconsistentProtocol", i, err)
		}
	}
}

func TestCoordinator_JoinGroup_PicksProtocolCommonToAllMembers(t *testing.T) {
	c := NewCoordinator(20 * time.Millisecond)

	var wg sync.WaitGroup
	results := make([]JoinResult, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0], _ = c.JoinGroup("g1", "", 50*time.Millisecond, "consumer",
			[]Protocol{{Name: "range"}, {Name: "roundrobin"}})
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		results[1], _ = c.JoinGroup("g1", "", 50*time.Millisecond, "consumer",
			[]Protocol{{Name: "roundrobin"}})
	}()
	wg.Wait()

	for i, r := range results {
		if r.ProtocolName != "roundrobin" {
			t.Errorf("member %d ProtocolName = %q, want roundrobin (the only name both members support)", i, r.ProtocolName)
		}
	}
}
