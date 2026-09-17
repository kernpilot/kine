package pgsql

// KUBEHZ-PATCH P2 test: the pgsql dialect offers a live-bytes source. The
// query itself needs a PostgreSQL and is not run here; this pins that the
// method exists on the type handed to sqllog.New, so a rebase that loses
// kubehz_quota.go turns the limit into a file-size limit loudly, not
// silently.

import (
	"testing"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

func TestKubehzP2LiveSizer(t *testing.T) {
	var d server.Dialect = &notifyingDialect{Generic: &generic.Generic{}}
	l, ok := d.(server.LiveSizer)
	if !ok {
		t.Fatal("the pgsql dialect does not offer server.LiveSizer")
	}
	if l.LiveSize() == nil {
		t.Fatal("the pgsql dialect offers LiveSizer but returns no source")
	}
}
