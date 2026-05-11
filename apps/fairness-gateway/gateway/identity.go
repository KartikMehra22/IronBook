package gateway

import (
	"crypto/sha256"
	"encoding/binary"
)

// SessionTokenFor strips the bot identity by replacing it with the SHA-256 of
// `bot_id || run_secret`. Stable within a run (so the divergence-detector can
// join by token) but opaque to the submission (so it can't recognise specific
// bots and game them).
func SessionTokenFor(botID uint64, runSecret []byte) [32]byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], botID)
	h := sha256.New()
	_, _ = h.Write(buf[:])
	_, _ = h.Write(runSecret)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// PackClientOrderID encodes (bot_id, local_seq) as a 16-byte big-endian
// payload, matching the matching engine's `ClientOrderId(u128)`.
func PackClientOrderID(botID, localSeq uint64) [16]byte {
	var out [16]byte
	binary.BigEndian.PutUint64(out[0:8], botID)
	binary.BigEndian.PutUint64(out[8:16], localSeq)
	return out
}
