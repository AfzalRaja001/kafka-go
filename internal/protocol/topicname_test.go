package protocol

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func TestValidateTopicName(t *testing.T) {
	valid := []string{
		"orders",
		"Orders",
		"orders.v1",
		"orders_v1",
		"orders-v1",
		"a",
		"0",
		"-",
		"_",
		"...",     // only "." and ".." are special, not every run of dots
		".hidden", // a leading dot is allowed by Kafka
		"__consumer_offsets",
		strings.Repeat("a", 249),
	}
	for _, name := range valid {
		if err := ValidateTopicName(name); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []struct {
		label string
		name  string
	}{
		{"empty", ""},
		{"single dot", "."},
		{"double dot", ".."},
		{"too long by one", strings.Repeat("a", 250)},
		{"much too long", strings.Repeat("a", 100000)},
		{"parent directory escape", "../escape"},
		{"nested parent escape", "a/../../escape"},
		{"forward slash", "a/b"},
		{"leading slash", "/etc/passwd"},
		{"backslash", `a\b`},
		{"windows drive", `C:\evil`},
		{"colon", "a:b"},
		{"space", "my topic"},
		{"trailing newline", "orders\n"},
		{"tab", "or\tders"},
		{"NUL byte", "ord\x00ers"},
		{"asterisk", "orders*"},
		{"question mark", "orders?"},
		{"non-ASCII letter", "caf\u00e9"},
		{"emoji", "orders\U0001F680"},
		{"invalid UTF-8", "ord\xffers"},
	}
	for _, tc := range invalid {
		if err := ValidateTopicName(tc.name); err == nil {
			t.Errorf("%s: ValidateTopicName(%.40q) = nil, want an error", tc.label, tc.name)
		}
	}
}

// TestHandleCreateTopics_InvalidNameRejected sends hostile names through the
// real handler and checks all three places a bad create could leave a mark:
// the response code, the registry, and the storage layer.
func TestHandleCreateTopics_InvalidNameRejected(t *testing.T) {
	for _, name := range []string{"../escape", "a/b", `a\b`, "..", ".", "bad name", strings.Repeat("a", 250)} {
		registry := NewTopicRegistry()
		fake := storage.NewFakeLog()

		resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest(name, 1, 1), registry, fake)
		if err != nil {
			t.Fatalf("%.40q: HandleCreateTopics: %v", name, err)
		}
		_, errorCode := decodeCreateTopicsResponseOneTopic(t, resp)
		if errorCode != ErrInvalidTopic {
			t.Errorf("%.40q: error_code = %d, want %d (INVALID_TOPIC_EXCEPTION)", name, errorCode, ErrInvalidTopic)
		}
		if _, ok := registry.Get(name); ok {
			t.Errorf("%.40q: registered despite being invalid", name)
		}
		if _, err := fake.LatestOffset(name, 0); err == nil {
			t.Errorf("%.40q: storage partition was created despite the name being invalid", name)
		}
	}
}

// TestHandleCreateTopics_PathTraversalCreatesNothingOnDisk is the regression
// test for the original bug: against a real DiskLog, "../escape" used to
// create a directory next to the data directory, outside it.
func TestHandleCreateTopics_PathTraversalCreatesNothingOnDisk(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	diskLog := storage.NewDiskLog(dataDir, 1024*1024, 4096, 0)
	defer diskLog.Close()
	registry := NewTopicRegistry()

	for _, name := range []string{"../escape", "../../escape", "sub/dir"} {
		resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest(name, 1, 1), registry, diskLog)
		if err != nil {
			t.Fatalf("%q: HandleCreateTopics: %v", name, err)
		}
		if _, code := decodeCreateTopicsResponseOneTopic(t, resp); code != ErrInvalidTopic {
			t.Errorf("%q: error_code = %d, want %d", name, code, ErrInvalidTopic)
		}
	}

	// Nothing may have been created anywhere under root, data directory
	// included: walk everything and fail on any entry other than root itself
	// and the (possibly pre-created, empty) data directory.
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || path == root || path == dataDir {
			return nil
		}
		t.Errorf("unexpected entry on disk: %s", path)
		return nil
	})
}

func TestHandleCreateTopics_ValidDottedNameStillAccepted(t *testing.T) {
	registry := NewTopicRegistry()
	resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest("orders.v1_a-b", 2, 1), registry, storage.NewFakeLog())
	if err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}
	if _, code := decodeCreateTopicsResponseOneTopic(t, resp); code != ErrNone {
		t.Fatalf("error_code = %d, want %d", code, ErrNone)
	}
}

// A hand-edited topics file is untrusted input too: a name that
// CreateTopics would refuse must not be accepted at startup, where it would
// become a directory path.
func TestOpenTopicRegistry_RejectsInvalidTopicNames(t *testing.T) {
	for _, name := range []string{"../x", "a/b", ".", "..", "bad name", strings.Repeat("a", 250)} {
		path := topicsPath(t)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		content := `{"version":1,"topics":[{"name":"` + strings.ReplaceAll(name, `\`, `\\`) + `","partitions":1}]}`
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenTopicRegistry(path, 1); err == nil {
			t.Errorf("OpenTopicRegistry accepted topic name %.40q, want an error", name)
		}
	}
}
