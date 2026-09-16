package app

// KUBEHZ-PATCH P2 test: the --quota-bytes flag exists, defaults to 0 (no
// limit) and lands in endpoint.Config.QuotaBytes.

import (
	"flag"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestKubehzP2QuotaFlag(t *testing.T) {
	app := New()
	var quotaFlag *cli.Int64Flag
	for _, f := range app.Flags {
		if f.Names()[0] != "quota-bytes" {
			continue
		}
		var ok bool
		if quotaFlag, ok = f.(*cli.Int64Flag); !ok {
			t.Fatalf("--quota-bytes is a %T, want *cli.Int64Flag", f)
		}
	}
	if quotaFlag == nil {
		t.Fatal("no --quota-bytes flag")
	}
	if quotaFlag.Value != 0 {
		t.Fatalf("default is %d, want 0 (no limit)", quotaFlag.Value)
	}
	if quotaFlag.Destination != &config.QuotaBytes {
		t.Fatal("--quota-bytes does not write endpoint.Config.QuotaBytes")
	}

	// parse it the way urfave/cli does, without running the app
	set := flag.NewFlagSet("kine", flag.ContinueOnError)
	if err := quotaFlag.Apply(set); err != nil {
		t.Fatal(err)
	}
	if err := set.Parse([]string{"--quota-bytes=2147483648"}); err != nil {
		t.Fatal(err)
	}
	if config.QuotaBytes != 2147483648 {
		t.Fatalf("parsed value %d, want 2147483648", config.QuotaBytes)
	}
	config.QuotaBytes = 0
}
