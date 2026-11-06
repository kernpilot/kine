//go:build linux

package endpoint

// KUBEHZ-PATCH P5 — see kubehz_listen.go and kubehz/MERGE-GUIDE.md#p5.

import (
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// setUserTimeout sets TCP_USER_TIMEOUT on a TCP connection, as gRPC does on
// Linux for its keepalive timeout. Other connections are left as they are.
func setUserTimeout(c net.Conn, d time.Duration) error {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return nil
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := raw.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(d/time.Millisecond))
	}); err != nil {
		return err
	}
	return serr
}
