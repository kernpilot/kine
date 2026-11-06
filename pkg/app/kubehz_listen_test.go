package app

// KUBEHZ-PATCH P5 test: --listen-handshake-timeout,
// --listen-max-connections and --listen-max-pending-handshakes exist,
// default to 0 (upstream's behaviour) and land in endpoint.Config.

import (
	"flag"
	"testing"
	"time"

	"github.com/urfave/cli/v2"
)

func TestKubehzP5ListenFlags(t *testing.T) {
	var timeout *cli.DurationFlag
	var limit, pending *cli.IntFlag
	for _, f := range New().Flags {
		var ok bool
		switch f.Names()[0] {
		case "listen-handshake-timeout":
			if timeout, ok = f.(*cli.DurationFlag); !ok {
				t.Fatalf("--listen-handshake-timeout is a %T, want *cli.DurationFlag", f)
			}
		case "listen-max-connections":
			if limit, ok = f.(*cli.IntFlag); !ok {
				t.Fatalf("--listen-max-connections is a %T, want *cli.IntFlag", f)
			}
		case "listen-max-pending-handshakes":
			if pending, ok = f.(*cli.IntFlag); !ok {
				t.Fatalf("--listen-max-pending-handshakes is a %T, want *cli.IntFlag", f)
			}
		}
	}
	if timeout == nil || limit == nil || pending == nil {
		t.Fatal("no --listen-handshake-timeout, --listen-max-connections or --listen-max-pending-handshakes flag")
	}
	if timeout.Value != 0 || limit.Value != 0 || pending.Value != 0 {
		t.Fatalf("defaults %s, %d and %d, want 0, 0 and 0 (gRPC's timeout, no limit, off)", timeout.Value, limit.Value, pending.Value)
	}
	if timeout.Destination != &config.ListenHandshakeTimeout || limit.Destination != &config.ListenMaxConnections || pending.Destination != &config.ListenMaxPendingHandshakes {
		t.Fatal("the flags do not write endpoint.Config.ListenHandshakeTimeout, ListenMaxConnections and ListenMaxPendingHandshakes")
	}
	if len(pending.EnvVars) != 1 || pending.EnvVars[0] != "KINE_LISTEN_MAX_PENDING_HANDSHAKES" {
		t.Fatalf("--listen-max-pending-handshakes reads %v", pending.EnvVars)
	}

	set := flag.NewFlagSet("kine", flag.ContinueOnError)
	for _, f := range []cli.Flag{timeout, limit, pending} {
		if err := f.Apply(set); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.Parse([]string{"--listen-handshake-timeout=10s", "--listen-max-connections=64", "--listen-max-pending-handshakes=16"}); err != nil {
		t.Fatal(err)
	}
	if config.ListenHandshakeTimeout != 10*time.Second || config.ListenMaxConnections != 64 || config.ListenMaxPendingHandshakes != 16 {
		t.Fatalf("parsed %s, %d and %d, want 10s, 64 and 16", config.ListenHandshakeTimeout, config.ListenMaxConnections, config.ListenMaxPendingHandshakes)
	}
	config.ListenHandshakeTimeout, config.ListenMaxConnections, config.ListenMaxPendingHandshakes = 0, 0, 0
}
