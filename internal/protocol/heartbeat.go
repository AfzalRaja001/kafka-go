package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

// DecodeHeartbeatRequest decodes a Heartbeat v0 request body.
func DecodeHeartbeatRequest(buf []byte) (HeartbeatRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return HeartbeatRequest{}, fmt.Errorf("group_id: %w", err)
	}
	generationID, err := dec.Int32()
	if err != nil {
		return HeartbeatRequest{}, fmt.Errorf("generation_id: %w", err)
	}
	memberID, err := dec.String()
	if err != nil {
		return HeartbeatRequest{}, fmt.Errorf("member_id: %w", err)
	}

	return HeartbeatRequest{GroupID: groupID, GenerationID: generationID, MemberID: memberID}, nil
}

// HandleHeartbeat builds a Heartbeat v0 response body.
func HandleHeartbeat(correlationID int32, requestBody []byte, coord *group.Coordinator) ([]byte, error) {
	req, err := DecodeHeartbeatRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("heartbeat request: %w", err)
	}

	hbErr := coord.Heartbeat(req.GroupID, req.MemberID, req.GenerationID)

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int16(groupErrorCode(hbErr))
	return enc.Result(), nil
}

// groupErrorCode maps the plain Go errors group.Coordinator returns to
// real Kafka wire error codes - shared by Heartbeat and LeaveGroup, which
// both only ever produce ErrUnknownMember/ErrIllegalGeneration/
// ErrRebalanceInProgress or nil.
func groupErrorCode(err error) int16 {
	switch err {
	case nil:
		return ErrNone
	case group.ErrUnknownMember:
		return ErrUnknownMemberID
	case group.ErrIllegalGeneration:
		return ErrIllegalGeneration
	case group.ErrRebalanceInProgress:
		return ErrRebalanceInProgress
	default:
		return ErrUnknownServerError
	}
}
