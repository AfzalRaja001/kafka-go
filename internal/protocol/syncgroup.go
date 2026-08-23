package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type SyncGroupAssignmentRequest struct {
	MemberID string
	Data     []byte
}

type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignmentRequest
}

// DecodeSyncGroupRequest decodes a SyncGroup v0 request body.
func DecodeSyncGroupRequest(buf []byte) (SyncGroupRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return SyncGroupRequest{}, fmt.Errorf("group_id: %w", err)
	}
	generationID, err := dec.Int32()
	if err != nil {
		return SyncGroupRequest{}, fmt.Errorf("generation_id: %w", err)
	}
	memberID, err := dec.String()
	if err != nil {
		return SyncGroupRequest{}, fmt.Errorf("member_id: %w", err)
	}

	assignmentCount, err := dec.Int32()
	if err != nil {
		return SyncGroupRequest{}, fmt.Errorf("assignment count: %w", err)
	}
	var assignments []SyncGroupAssignmentRequest
	for i := int32(0); i < assignmentCount; i++ {
		id, err := dec.String()
		if err != nil {
			return SyncGroupRequest{}, fmt.Errorf("assignment %d member_id: %w", i, err)
		}
		data, err := dec.Bytes()
		if err != nil {
			return SyncGroupRequest{}, fmt.Errorf("assignment %d data: %w", i, err)
		}
		assignments = append(assignments, SyncGroupAssignmentRequest{MemberID: id, Data: data})
	}

	return SyncGroupRequest{GroupID: groupID, GenerationID: generationID, MemberID: memberID, Assignments: assignments}, nil
}

// HandleSyncGroup builds a SyncGroup v0 response body. Like JoinGroup, this
// can block - a follower's call waits for the leader's own SyncGroup to
// arrive on a different connection entirely, coordinated through
// group.Coordinator rather than anything in this package.
func HandleSyncGroup(correlationID int32, requestBody []byte, coord *group.Coordinator) ([]byte, error) {
	req, err := DecodeSyncGroupRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("sync_group request: %w", err)
	}

	assignments := make([]group.Assignment, len(req.Assignments))
	for i, a := range req.Assignments {
		assignments[i] = group.Assignment{MemberID: a.MemberID, Data: a.Data}
	}

	assignment, syncErr := coord.SyncGroup(req.GroupID, req.MemberID, req.GenerationID, assignments)

	enc := NewEncoder()
	enc.Int32(correlationID)

	if syncErr != nil {
		enc.Int16(syncGroupErrorCode(syncErr))
		enc.Bytes([]byte{})
		return enc.Result(), nil
	}

	enc.Int16(ErrNone)
	enc.Bytes(assignment)
	return enc.Result(), nil
}

func syncGroupErrorCode(err error) int16 {
	switch err {
	case group.ErrUnknownMember:
		return ErrUnknownMemberID
	case group.ErrIllegalGeneration:
		return ErrIllegalGeneration
	case group.ErrSyncTimedOut:
		// No exact wire code for "the leader never showed up" - telling the
		// client a rebalance is in progress is the closest real Kafka error
		// that prompts the same correct client behavior: call JoinGroup again.
		return ErrRebalanceInProgress
	default:
		return ErrUnknownServerError
	}
}
