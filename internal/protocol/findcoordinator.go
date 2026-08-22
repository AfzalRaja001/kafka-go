package protocol

import "fmt"

type FindCoordinatorRequest struct {
	Key string
}

// DecodeFindCoordinatorRequest decodes a FindCoordinator v0 request body.
func DecodeFindCoordinatorRequest(buf []byte) (FindCoordinatorRequest, error) {
	dec := NewDecoder(buf)

	key, err := dec.String()
	if err != nil {
		return FindCoordinatorRequest{}, fmt.Errorf("key: %w", err)
	}

	return FindCoordinatorRequest{Key: key}, nil
}

// HandleFindCoordinator builds a FindCoordinator v0 response body. self is
// always the answer: on a single-node broker, the one broker there is is
// trivially the coordinator for every group, so no group name is ever
// rejected and no lookup is needed.
func HandleFindCoordinator(correlationID int32, requestBody []byte, self Broker) ([]byte, error) {
	if _, err := DecodeFindCoordinatorRequest(requestBody); err != nil {
		return nil, fmt.Errorf("find_coordinator request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int16(ErrNone)
	enc.Int32(self.NodeID)
	enc.String(self.Host)
	enc.Int32(self.Port)
	return enc.Result(), nil
}
