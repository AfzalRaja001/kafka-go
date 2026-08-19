package storage

import "time"

// removeAllWithRetry works around a Windows-specific race: os.RemoveAll can
// transiently fail with "directory is not empty" immediately after closing
// a file that lived in that directory, because NTFS hasn't finished
// releasing the just-closed handle's directory entry yet - live testing
// against a real broker reproduced this on roughly 2 of every 3
// DeletePartition calls. It's the same class of OS quirk documented for
// Segment.Truncate in docs/issues.md. Linux (where real Kafka runs) doesn't
// have this race, so the retry loop costs nothing extra there - remove
// always succeeds on the first attempt.
//
// remove is injected rather than calling os.RemoveAll directly so this can
// be tested against a fake that fails on command, instead of depending on
// actually reproducing a real, timing-sensitive Windows race in a unit
// test.
func removeAllWithRetry(remove func(path string) error, path string, attempts int, delay time.Duration) error {
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(delay)
		}
		err = remove(path)
		if err == nil {
			return nil
		}
	}
	return err
}
