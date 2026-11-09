//go:build !linux

package endpoint

// KUBEHZ-PATCH P5 — see kubehz_listen.go and kubehz/MERGE-GUIDE.md#p5.

import (
	"net"
	"time"
)

// setUserTimeout does nothing: gRPC sets TCP_USER_TIMEOUT only on Linux.
func setUserTimeout(net.Conn, time.Duration) error { return nil }
