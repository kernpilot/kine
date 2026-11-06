package app

// KUBEHZ-PATCH P5 test: --listen-handshake-timeout and
// --listen-max-connections exist, default to 0 (upstream's behaviour) and
// land in endpoint.Config.

import (
	"flag"
	"testing"
	"time"

	"github.com/urfave/cli/v2"
)

func TestKubehzP5ListenFlags(t *testing.T) {
	var timeout *cli.DurationFlag
	var limit *cli.IntFlag
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
		}
	}
	if timeout == nil || limit == nil {
		t.Fatal("no --listen-handshake-timeout or no --listen-max-connections flag")
	}
	if timeout.Value != 0 || limit.Value != 0 {
		t.Fatalf("defaults %s and %d, want 0 and 0 (gRPC's timeout, no limit)", timeout.Value, limit.Value)
	}
	if timeout.Destination != &config.ListenHandshakeTimeout || limit.Destination != &config.ListenMaxConnections {
		t.Fatal("the flags do not write endpoint.Config.ListenHandshakeTimeout and ListenMaxConnections")
	}

	set := flag.NewFlagSet("kine", flag.ContinueOnError)
	for _, f := range []cli.Flag{timeout, limit} {
		if err := f.Apply(set); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.Parse([]string{"--listen-handshake-timeout=10s", "--listen-max-connections=64"}); err != nil {
		t.Fatal(err)
	}
	if config.ListenHandshakeTimeout != 10*time.Second || config.ListenMaxConnections != 64 {
		t.Fatalf("parsed %s and %d, want 10s and 64", config.ListenHandshakeTimeout, config.ListenMaxConnections)
	}
	config.ListenHandshakeTimeout, config.ListenMaxConnections = 0, 0
}
