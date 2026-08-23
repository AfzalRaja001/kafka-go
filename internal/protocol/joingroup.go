package protocol

import (
	"fmt"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type JoinGroupProtocolRequest struct {
	Name     string
	Metadata []byte
}

type JoinGroupRequest struct {
	GroupID          string
	SessionTimeoutMs int32
	MemberID         string
	ProtocolType     string
	Protocols        []JoinGroupProtocolRequest
}

// DecodeJoinGroupRequest decodes a JoinGroup v0 request body.
func DecodeJoinGroupRequest(buf []byte) (JoinGroupRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return JoinGroupRequest{}, fmt.Errorf("group_id: %w", err)
	}
	sessionTimeoutMs, err := dec.Int32()
	if err != nil {
		return JoinGroupRequest{}, fmt.Errorf("session_timeout_ms: %w", err)
	}
	memberID, err := dec.String()
	if err != nil {
		return JoinGroupRequest{}, fmt.Errorf("member_id: %w", err)
	}
	protocolType, err := dec.String()
	if err != nil {
		return JoinGroupRequest{}, fmt.Errorf("protocol_type: %w", err)
	}

	protocolCount, err := dec.Int32()
	if err != nil {
		return JoinGroupRequest{}, fmt.Errorf("protocol count: %w", err)
	}
	var protocols []JoinGroupProtocolRequest
	for i := int32(0); i < protocolCount; i++ {
		name, err := dec.String()
		if err != nil {
			return JoinGroupRequest{}, fmt.Errorf("protocol %d name: %w", i, err)
		}
		metadata, err := dec.Bytes()
		if err != nil {
			return JoinGroupRequest{}, fmt.Errorf("protocol %d metadata: %w", i, err)
		}
		protocols = append(protocols, JoinGroupProtocolRequest{Name: name, Metadata: metadata})
	}

	return JoinGroupRequest{
		GroupID:          groupID,
		SessionTimeoutMs: sessionTimeoutMs,
		MemberID:         memberID,
		ProtocolType:     protocolType,
		Protocols:        protocols,
	}, nil
}

// HandleJoinGroup builds a JoinGroup v0 response body. This call blocks -
// deliberately - for as long as the group's rebalance window stays open,
// the same long-hanging-request shape Fetch's long-polling already
// established, just driven by group.Coordinator's own timer instead of a
// per-request one.
func HandleJoinGroup(correlationID int32, requestBody []byte, coord *group.Coordinator) ([]byte, error) {
	req, err := DecodeJoinGroupRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("join_group request: %w", err)
	}

	protocols := make([]group.Protocol, len(req.Protocols))
	for i, p := range req.Protocols {
		protocols[i] = group.Protocol{Name: p.Name, Metadata: p.Metadata}
	}

	result, joinErr := coord.JoinGroup(
		req.GroupID, req.MemberID,
		time.Duration(req.SessionTimeoutMs)*time.Millisecond,
		req.ProtocolType, protocols,
	)

	enc := NewEncoder()
	enc.Int32(correlationID)

	if joinErr != nil {
		enc.Int16(joinGroupErrorCode(joinErr))
		enc.Int32(-1) // generation_id: no valid generation
		enc.String("")
		enc.String("")
		enc.String("")
		enc.Int32(0) // members: empty
		return enc.Result(), nil
	}

	enc.Int16(ErrNone)
	enc.Int32(result.GenerationID)
	enc.String(result.ProtocolName)
	enc.String(result.Leader)
	enc.String(result.MemberID)
	enc.Int32(int32(len(result.Members)))
	for _, m := range result.Members {
		enc.String(m.MemberID)
		enc.Bytes(m.Metadata)
	}
	return enc.Result(), nil
}

func joinGroupErrorCode(err error) int16 {
	switch err {
	case group.ErrInconsistentProtocol:
		return ErrInconsistentGroupProtocol
	default:
		return ErrUnknownServerError
	}
}
