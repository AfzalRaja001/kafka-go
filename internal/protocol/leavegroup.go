package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// DecodeLeaveGroupRequest decodes a LeaveGroup v0 request body.
func DecodeLeaveGroupRequest(buf []byte) (LeaveGroupRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return LeaveGroupRequest{}, fmt.Errorf("group_id: %w", err)
	}
	memberID, err := dec.String()
	if err != nil {
		return LeaveGroupRequest{}, fmt.Errorf("member_id: %w", err)
	}

	return LeaveGroupRequest{GroupID: groupID, MemberID: memberID}, nil
}

// HandleLeaveGroup builds a LeaveGroup v0 response body.
func HandleLeaveGroup(correlationID int32, requestBody []byte, coord *group.Coordinator) ([]byte, error) {
	req, err := DecodeLeaveGroupRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("leave_group request: %w", err)
	}

	leaveErr := coord.LeaveGroup(req.GroupID, req.MemberID)

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int16(groupErrorCode(leaveErr))
	return enc.Result(), nil
}
