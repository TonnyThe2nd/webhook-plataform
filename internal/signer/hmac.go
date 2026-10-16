package signer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

func Sign(secret string, timestamp int64, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
