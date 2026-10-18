package protocol

import "fmt"

// MaxTopicNameLength is the longest topic name Kafka accepts. Matching it
// exactly means any name the official tools allow works here too.
const MaxTopicNameLength = 249

// ValidateTopicName reports whether name may be used as a topic name. The
// rules are Kafka's own: 1 to 249 characters, each one of the ASCII letters
// and digits, '.', '_' or '-', and not exactly "." or "..".
//
// This is more than cosmetics. A topic's partitions are stored in a
// directory named "<topic>-<partition>" inside the data directory, so the
// name is spliced straight into a file path. Allowing '/', '\' or ".." would
// let a client name a directory outside the data directory. The allowed
// character set contains no path separator and no drive letter colon, and
// "." / ".." are refused because on their own they are path components.
//
// The check is byte-wise on purpose: every allowed character is ASCII, so
// any byte at or above 0x80 (part of a multi-byte or invalid UTF-8
// sequence) is rejected without decoding anything.
func ValidateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name is empty")
	}
	if len(name) > MaxTopicNameLength {
		return fmt.Errorf("topic name is %d bytes long, the maximum is %d", len(name), MaxTopicNameLength)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("topic name cannot be %q", name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("topic name contains the illegal byte %q at position %d; only ASCII letters, digits, '.', '_' and '-' are allowed", c, i)
		}
	}
	return nil
}
