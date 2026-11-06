package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Kinds of the notifications of an incident (§4.7).
const (
	KindFiring   = "firing"
	KindResolved = "resolved"
)

// Incident is (rule, group, T_f) of §4.7, with T_f the instant of the
// transition to firing.
type Incident struct {
	Rule    string
	Group   string
	FiredAt int64
}

// ID returns the incident_id: the first 32 hex characters of the SHA-256 of
// rule, group and T_f in decimal, separated by a zero byte.
func (i Incident) ID() string {
	h := sha256.New()
	h.Write([]byte(i.Rule))
	h.Write([]byte{0})
	h.Write([]byte(i.Group))
	h.Write([]byte{0})
	h.Write(strconv.AppendInt(nil, i.FiredAt, 10))
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// IdempotencyKey returns the key of the notification of the given kind.
func (i Incident) IdempotencyKey(kind string) string {
	return i.ID() + "/" + kind
}
