package protocol

import "testing"

func encodeFindCoordinatorRequest(groupID string) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	return enc.Result()
}

func TestDecodeFindCoordinatorRequest(t *testing.T) {
	body := encodeFindCoordinatorRequest("my-group")

	req, err := DecodeFindCoordinatorRequest(body)
	if err != nil {
		t.Fatalf("DecodeFindCoordinatorRequest: %v", err)
	}
	if req.Key != "my-group" {
		t.Fatalf("Key = %q, want my-group", req.Key)
	}
}

func TestHandleFindCoordinator_AlwaysReturnsSelf(t *testing.T) {
	self := Broker{NodeID: 1, Host: "localhost", Port: 9092}

	body := encodeFindCoordinatorRequest("my-group")
	resp, err := HandleFindCoordinator(1, body, self)
	if err != nil {
		t.Fatalf("HandleFindCoordinator: %v", err)
	}

	dec := NewDecoder(resp)
	correlationID, _ := dec.Int32()
	errorCode, _ := dec.Int16()
	nodeID, _ := dec.Int32()
	host, _ := dec.String()
	port, _ := dec.Int32()

	if correlationID != 1 {
		t.Errorf("correlation_id = %d, want 1", correlationID)
	}
	if errorCode != ErrNone {
		t.Errorf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if nodeID != 1 || host != "localhost" || port != 9092 {
		t.Errorf("coordinator = (%d, %q, %d), want (1, localhost, 9092)", nodeID, host, port)
	}
}

// TestHandleFindCoordinator_AnyGroupNameGetsTheSameAnswer pins down that
// this single-node broker never rejects a group name - it's trivially the
// coordinator for every group there is, since there's only one broker.
func TestHandleFindCoordinator_AnyGroupNameGetsTheSameAnswer(t *testing.T) {
	self := Broker{NodeID: 1, Host: "localhost", Port: 9092}

	for _, group := range []string{"", "a-very-unusual-group-name", "123"} {
		resp, err := HandleFindCoordinator(1, encodeFindCoordinatorRequest(group), self)
		if err != nil {
			t.Fatalf("HandleFindCoordinator(%q): %v", group, err)
		}
		dec := NewDecoder(resp)
		dec.Int32() // correlation_id
		errorCode, _ := dec.Int16()
		if errorCode != ErrNone {
			t.Errorf("group %q: error_code = %d, want %d", group, errorCode, ErrNone)
		}
	}
}
