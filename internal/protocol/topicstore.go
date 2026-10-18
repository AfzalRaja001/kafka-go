package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

// topicsFileVersion is the only on-disk format this broker reads or writes.
// A file with any other version is refused rather than guessed at.
const topicsFileVersion = 1

// topicRecord is all the topic list persists: a name and a partition count.
// Everything else Metadata reports (leader, replicas, ISR) is derived from
// the node ID in the broker's configuration, so it cannot go stale inside
// the file when that configuration changes.
type topicRecord struct {
	Name       string `json:"name"`
	Partitions int32  `json:"partitions"`
}

type topicsFile struct {
	Version int           `json:"version"`
	Topics  []topicRecord `json:"topics"`
}

// loadTopicsFile reads the topic list from path. A file that does not exist
// is not an error - it is a broker that has never created a topic. Anything
// else wrong with the file is, because the alternative is starting empty,
// which every client would experience as every topic having been deleted.
func loadTopicsFile(path string) ([]topicRecord, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var file topicsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("not valid JSON: %w", err)
	}
	if file.Version != topicsFileVersion {
		return nil, fmt.Errorf("unsupported version %d, this broker reads version %d", file.Version, topicsFileVersion)
	}

	seen := make(map[string]bool, len(file.Topics))
	for _, t := range file.Topics {
		// The same rule CreateTopics applies: a hand-edited file must not be
		// able to smuggle in a name that becomes a path outside the data
		// directory.
		if err := ValidateTopicName(t.Name); err != nil {
			return nil, fmt.Errorf("invalid topic name %.60q: %w", t.Name, err)
		}
		// The partition cap doubles as a guard on what gets allocated: a
		// doctored file must not be able to make the broker build billions
		// of partitions at startup, the way a hostile CreateTopics once could.
		if t.Partitions < 1 || t.Partitions > MaxPartitionsPerTopic {
			return nil, fmt.Errorf("topic %q has %d partitions, want 1 to %d", t.Name, t.Partitions, MaxPartitionsPerTopic)
		}
		if seen[t.Name] {
			return nil, fmt.Errorf("topic %q is listed twice", t.Name)
		}
		seen[t.Name] = true
	}
	return file.Topics, nil
}

// saveTopicsFile replaces the file at path with records, atomically: the new
// content is written and fsynced to a temporary file beside it, renamed over
// the real one, and the directory is fsynced so the rename itself survives a
// power loss. A crash at any point leaves either the complete old file or the
// complete new one, never a half-written mix - the property that makes it
// safe to treat this file as the source of truth.
func saveTopicsFile(path string, records []topicRecord) error {
	sorted := append([]topicRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	if sorted == nil {
		sorted = []topicRecord{}
	}

	data, err := json.MarshalIndent(topicsFile{Version: topicsFileVersion, Topics: sorted}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := writeFileSynced(tmp, data); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

func writeFileSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// syncDir fsyncs a directory so a rename inside it is durable. Windows does
// not allow flushing a directory handle, and has no equivalent step to take,
// so it is skipped there; the Linux deployment target does it.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
