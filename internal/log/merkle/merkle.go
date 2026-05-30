package merkle

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"hash"
)

// Domain separation prefixes for second-preimage resistance.
var (
	leafPrefix     = []byte{0x00}
	internalPrefix = []byte{0x01}
)

// LeafHash computes the hash of a single log event leaf.
// Hash = SHA-256(0x00 || event_id || system_time || type || payload)
func LeafHash(eventID string, systemTime []byte, eventType []byte, payload []byte) []byte {
	h := sha256.New()
	h.Write(leafPrefix)
	writeField(h, []byte(eventID))
	writeField(h, systemTime)
	writeField(h, eventType)
	writeField(h, payload)
	return h.Sum(nil)
}

func writeField(h hash.Hash, data []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	h.Write(lenBuf[:])
	h.Write(data)
}

// ComputeRoot computes the Merkle root from a list of leaf hashes.
// For odd-numbered levels, the last node is doubled.
func ComputeRoot(leaves [][]byte) []byte {
	if len(leaves) == 0 {
		return nil
	}
	if len(leaves) == 1 {
		return leaves[0]
	}

	// Work up the tree.
	level := make([][]byte, len(leaves))
	copy(level, leaves)

	for len(level) > 1 {
		var next [][]byte
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			var right []byte
			if i+1 < len(level) {
				right = level[i+1]
			} else {
				right = left // duplicate last node for odd count
			}
			next = append(next, internalHash(left, right))
		}
		level = next
	}

	return level[0]
}

// Verify recomputes the Merkle root from leaves and compares against the expected root.
// Returns false if either root is nil (empty batches are never considered valid).
func Verify(leaves [][]byte, expectedRoot []byte) bool {
	computed := ComputeRoot(leaves)
	if computed == nil || expectedRoot == nil {
		return false
	}
	return subtle.ConstantTimeCompare(computed, expectedRoot) == 1
}

func internalHash(left, right []byte) []byte {
	h := sha256.New()
	h.Write(internalPrefix)
	h.Write(left)
	h.Write(right)
	return h.Sum(nil)
}
