package group

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrUnknownMember        = errors.New("unknown member")
	ErrIllegalGeneration    = errors.New("illegal generation")
	ErrInconsistentProtocol = errors.New("no protocol common to every member")
	ErrRebalanceInProgress  = errors.New("rebalance in progress")
	ErrSyncTimedOut         = errors.New("timed out waiting for the group leader to sync")
)

// Protocol is one (name, opaque metadata) pair a member advertises in
// JoinGroup - the broker never parses Metadata, only compares names, the
// same "store bytes verbatim" principle every other API here follows for
// record batches and topic configs.
type Protocol struct {
	Name     string
	Metadata []byte
}

// MemberInfo is what a group's leader needs about every member to compute
// an assignment - present only in the leader's own JoinResult.
type MemberInfo struct {
	MemberID string
	Metadata []byte
}

type JoinResult struct {
	MemberID     string
	GenerationID int32
	ProtocolName string
	Leader       string
	Members      []MemberInfo
}

// Assignment is one member's slice of a leader-computed partition
// assignment, submitted via SyncGroup - opaque bytes, same as Protocol.
type Assignment struct {
	MemberID string
	Data     []byte
}

// Coordinator manages every consumer group this broker knows about. One
// Coordinator lives for the broker's lifetime; internal/group depends on
// nothing else in this project, so it knows nothing about the Kafka wire
// format - protocol.HandleJoinGroup and friends translate between this
// package's plain Go errors and real wire error codes.
type Coordinator struct {
	mu                    sync.Mutex
	groups                map[string]*group
	nextIDNum             int64
	initialRebalanceDelay time.Duration
}

// NewCoordinator constructs a Coordinator. initialRebalanceDelay is how
// long a join window stays open waiting for other members before
// finalizing - matching real Kafka's group.initial.rebalance.delay.ms,
// a broker-side setting, deliberately not derived from any client-supplied
// timeout. main.go uses a real default (a few seconds); tests pass
// something short so they don't take real wall-clock seconds to run.
func NewCoordinator(initialRebalanceDelay time.Duration) *Coordinator {
	return &Coordinator{groups: make(map[string]*group), initialRebalanceDelay: initialRebalanceDelay}
}

type groupState int

const (
	stateEmpty groupState = iota
	statePreparingRebalance
	stateCompletingRebalance
	stateStable
)

type group struct {
	mu           sync.Mutex
	state        groupState
	generation   int32
	protocolType string
	protocolName string
	joinOrder    []string           // member IDs, in the order they joined this round
	members      map[string]*member // this round's roster
	leader       string

	joinBarrier chan struct{} // closed when the join window ends

	syncResults map[string][]byte // memberID -> assignment, filled by the leader's SyncGroup
	syncBarrier chan struct{}     // closed when the leader's SyncGroup arrives
}

type member struct {
	id             string
	protocols      []Protocol
	sessionTimeout time.Duration
	lastHeartbeat  time.Time
}

// JoinGroup registers memberID (assigning a new one if empty) into groupID's
// current join window, starting a new window if the group is idle, then
// blocks until that window closes. Every member that joins within the same
// window unblocks together, with the same generation.
//
// Deliberately unlike Fetch's per-request context.WithTimeout: the window's
// deadline is a group-shared property (c.initialRebalanceDelay), started
// once by the first joiner, not a separate timeout per caller - so there's
// nothing here for an individual JoinGroup call to time out on its own.
// sessionTimeout is stored on the member for a different purpose entirely -
// Heartbeat's expiry threshold and SyncGroup's follower-wait bound - not
// the join window. Conflating the two was a real bug live testing found:
// kafka-python's default SessionTimeoutMs is 30 seconds, and if the join
// window waited that long, every real client's own poll timeout (always
// much shorter) would give up first.
func (c *Coordinator) JoinGroup(groupID, memberID string, sessionTimeout time.Duration, protocolType string, protocols []Protocol) (JoinResult, error) {
	g := c.getOrCreateGroup(groupID)

	g.mu.Lock()
	if memberID == "" {
		memberID = c.newMemberID()
	}

	if g.state == stateEmpty || g.state == stateStable {
		g.state = statePreparingRebalance
		g.protocolType = protocolType
		g.joinOrder = nil
		g.members = make(map[string]*member)
		g.joinBarrier = make(chan struct{})
		barrier := g.joinBarrier
		time.AfterFunc(c.initialRebalanceDelay, func() { c.finalizeJoin(g, barrier) })
	}

	g.joinOrder = append(g.joinOrder, memberID)
	g.members[memberID] = &member{id: memberID, protocols: protocols, sessionTimeout: sessionTimeout, lastHeartbeat: time.Now()}
	barrier := g.joinBarrier
	g.mu.Unlock()

	<-barrier

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.protocolName == "" {
		return JoinResult{}, ErrInconsistentProtocol
	}

	result := JoinResult{
		MemberID:     memberID,
		GenerationID: g.generation,
		ProtocolName: g.protocolName,
		Leader:       g.leader,
	}
	if memberID == g.leader {
		for _, id := range g.joinOrder {
			m := g.members[id]
			result.Members = append(result.Members, MemberInfo{MemberID: m.id, Metadata: protocolMetadata(m.protocols, g.protocolName)})
		}
	}
	return result, nil
}

// SyncGroup submits the group's assignment (if memberID is the leader) or
// waits to receive its own slice of it (if memberID is a follower). The
// leader's call never blocks - it already has everything it needs to
// answer immediately. A follower's wait is bounded by its own session
// timeout (captured at JoinGroup time), so a leader that crashes between
// JoinGroup and SyncGroup doesn't leave followers blocked forever - a real
// gap real Kafka has more machinery to close, scoped out here since no
// test scenario exercises a mid-rebalance leader crash.
func (c *Coordinator) SyncGroup(groupID, memberID string, generationID int32, assignments []Assignment) ([]byte, error) {
	g := c.getOrCreateGroup(groupID)

	g.mu.Lock()
	m, ok := g.members[memberID]
	if !ok {
		g.mu.Unlock()
		return nil, ErrUnknownMember
	}
	if generationID != g.generation {
		g.mu.Unlock()
		return nil, ErrIllegalGeneration
	}

	if memberID == g.leader {
		for _, a := range assignments {
			g.syncResults[a.MemberID] = a.Data
		}
		g.state = stateStable
		result := g.syncResults[memberID]
		m.lastHeartbeat = time.Now() // entering Stable counts as a check-in
		close(g.syncBarrier)
		g.mu.Unlock()
		return result, nil
	}

	barrier := g.syncBarrier
	timeout := m.sessionTimeout
	g.mu.Unlock()

	select {
	case <-barrier:
	case <-time.After(timeout):
		return nil, ErrSyncTimedOut
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	m.lastHeartbeat = time.Now() // entering Stable counts as a check-in
	return g.syncResults[memberID], nil
}

// finalizeJoin closes a join window: picks the leader (the first member to
// join this round), selects the protocol every member supports, bumps the
// generation, and releases every goroutine blocked on barrier. Runs on its
// own timer goroutine, so it takes the group's lock itself rather than
// assuming a caller already holds it.
func (c *Coordinator) finalizeJoin(g *group, barrier chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.joinBarrier != barrier {
		return // a newer round already started and replaced this one
	}

	g.leader = g.joinOrder[0]
	g.protocolName = selectProtocol(g.joinOrder, g.members) // "" if no common protocol
	if g.protocolName == "" {
		// A failed round must not leave the group stuck mid-rebalance
		// forever - reset to Empty so the next JoinGroup starts a clean
		// round rather than silently joining a round that already failed.
		g.state = stateEmpty
	} else {
		g.generation++
		g.state = stateCompletingRebalance
		g.syncResults = make(map[string][]byte)
		g.syncBarrier = make(chan struct{})
	}
	close(g.joinBarrier)
}

// selectProtocol picks the protocol name every member supports, breaking
// ties toward whichever name appears earliest in the first joiner's list -
// a simplification of real Kafka's full cross-member voting scheme, correct
// for the common case this project's clients actually exercise: every
// member proposing the same protocol name(s).
func selectProtocol(joinOrder []string, members map[string]*member) string {
	if len(joinOrder) == 0 {
		return ""
	}

	common := map[string]bool{}
	for _, p := range members[joinOrder[0]].protocols {
		common[p.Name] = true
	}
	for _, id := range joinOrder[1:] {
		supported := map[string]bool{}
		for _, p := range members[id].protocols {
			supported[p.Name] = true
		}
		for name := range common {
			if !supported[name] {
				delete(common, name)
			}
		}
	}

	for _, p := range members[joinOrder[0]].protocols {
		if common[p.Name] {
			return p.Name
		}
	}
	return ""
}

func protocolMetadata(protocols []Protocol, name string) []byte {
	for _, p := range protocols {
		if p.Name == name {
			return p.Metadata
		}
	}
	return nil
}

// Heartbeat validates a still-alive member and refreshes its expiry clock.
// It fails with ErrRebalanceInProgress whenever the group isn't Stable -
// that's the signal real Kafka clients use to know they must call
// JoinGroup again, whether the rebalance was triggered by another member
// leaving, being reaped, or a round simply still being in progress.
func (c *Coordinator) Heartbeat(groupID, memberID string, generationID int32) error {
	g := c.getOrCreateGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	m, ok := g.members[memberID]
	if !ok {
		return ErrUnknownMember
	}
	if generationID != g.generation {
		return ErrIllegalGeneration
	}
	if g.state != stateStable {
		return ErrRebalanceInProgress
	}
	m.lastHeartbeat = time.Now()
	return nil
}

// LeaveGroup removes memberID and resets the group to Empty - the same
// state a JoinGroup checks for to start a brand-new round, so the next
// member to join (typically one of the ones left behind, once its next
// Heartbeat tells it to) starts a clean rebalance rather than joining a
// round that never properly started.
func (c *Coordinator) LeaveGroup(groupID, memberID string) error {
	g := c.getOrCreateGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.members[memberID]; !ok {
		return ErrUnknownMember
	}
	delete(g.members, memberID)
	g.state = stateEmpty
	return nil
}

// ReapExpiredMembers evicts any member of any Stable group whose last
// heartbeat is older than its own session timeout, and resets affected
// groups to Empty - the same eviction-triggers-rebalance behavior
// LeaveGroup has. Intended to be called periodically (main.go runs it on a
// ticker), not per-request, since it scans every group.
func (c *Coordinator) ReapExpiredMembers(now time.Time) {
	c.mu.Lock()
	groups := make([]*group, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.Unlock()

	for _, g := range groups {
		g.mu.Lock()
		if g.state == stateStable {
			var expired []string
			for id, m := range g.members {
				if now.Sub(m.lastHeartbeat) > m.sessionTimeout {
					expired = append(expired, id)
				}
			}
			if len(expired) > 0 {
				for _, id := range expired {
					delete(g.members, id)
				}
				g.state = stateEmpty
			}
		}
		g.mu.Unlock()
	}
}

func (c *Coordinator) getOrCreateGroup(groupID string) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		g = &group{state: stateEmpty}
		c.groups[groupID] = g
	}
	return g
}

func (c *Coordinator) newMemberID() string {
	n := atomic.AddInt64(&c.nextIDNum, 1)
	return fmt.Sprintf("member-%d", n)
}
