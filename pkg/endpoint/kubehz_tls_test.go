package endpoint

// KUBEHZ-PATCH P4 tests: the failed TLS handshakes of the client listener,
// counted by reason. Real handshakes against the server TLS config that
// Listen builds, on TLS 1.2 and 1.3, and a real kine on sqlite through
// Listen. unit.yml lists these tests by name before it runs them, so a
// rebase that drops the wrapper or this file fails CI.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	cryptotls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/metrics"
	kinetls "github.com/k3s-io/kine/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/credentials"
)

// testCA issues the certificates of these tests.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, pem: pemBlock("CERTIFICATE", der)}
}

// issue signs a leaf certificate for 127.0.0.1 and localhost with one
// extended key usage. A notAfter in the past makes it expired.
func (ca *testCA) issue(t *testing.T, name string, usage x509.ExtKeyUsage, notAfter time.Time) (certPEM, keyPEM []byte, pair cryptotls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-2 * time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM = pemBlock("CERTIFICATE", der), pemBlock("EC PRIVATE KEY", keyDER)
	pair, err = cryptotls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certPEM, keyPEM, pair
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

// testPKI is the tenant CA with kine's serving certificate and the client
// certificates of the cases, and a second CA.
type testPKI struct {
	ca, other *testCA
	// the files Listen reads (--cert-file, --key-file, --trusted-ca-file)
	caFile, serverCert, serverKey string
	// client: signed by ca; otherClient: by the other CA; expired: by ca, not
	// valid since an hour; serverOnly: by ca, for server auth only
	client, otherClient, expired, serverOnly cryptotls.Certificate
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	p := &testPKI{ca: newTestCA(t, "tenant CA"), other: newTestCA(t, "other CA")}
	later := time.Now().Add(time.Hour)
	certPEM, keyPEM, _ := p.ca.issue(t, "kine", x509.ExtKeyUsageServerAuth, later)
	p.caFile = filepath.Join(dir, "ca.crt")
	p.serverCert = filepath.Join(dir, "server.crt")
	p.serverKey = filepath.Join(dir, "server.key")
	for file, data := range map[string][]byte{p.caFile: p.ca.pem, p.serverCert: certPEM, p.serverKey: keyPEM} {
		if err := os.WriteFile(file, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, _, p.client = p.ca.issue(t, "client", x509.ExtKeyUsageClientAuth, later)
	_, _, p.otherClient = p.other.issue(t, "client", x509.ExtKeyUsageClientAuth, later)
	_, _, p.expired = p.ca.issue(t, "client", x509.ExtKeyUsageClientAuth, time.Now().Add(-time.Hour))
	_, _, p.serverOnly = p.ca.issue(t, "client", x509.ExtKeyUsageServerAuth, later)
	return p
}

func (p *testPKI) serverConfig() kinetls.Config {
	return kinetls.Config{CertFile: p.serverCert, KeyFile: p.serverKey, TrustedCAFile: p.caFile}
}

func pool(ca *testCA) *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// handshakeCounts reads every reason of the counter.
func handshakeCounts() map[string]float64 {
	counts := map[string]float64{}
	for _, reason := range handshakeFailureReasons {
		counts[reason] = testutil.ToFloat64(metrics.TLSHandshakeFailures.WithLabelValues(reason))
	}
	return counts
}

// countedSince returns the reasons that went up since before, and by how much.
func countedSince(before map[string]float64) map[string]float64 {
	diff := map[string]float64{}
	for reason, n := range handshakeCounts() {
		if d := n - before[reason]; d != 0 {
			diff[reason] = d
		}
	}
	return diff
}

// tlsProbe is a TLS client with one certificate (or none) that trusts roots.
// It sends its certificate also when the server asks for another CA, as an
// etcd client does (GetClientCertificate); with Certificates, crypto/tls
// would send none. When the handshake completes on its side it reads once:
// in TLS 1.3 the server refuses a client certificate after the client's
// side of the handshake is done, so only the read shows the refusal. With
// accept, it expects no refusal and does not read.
func tlsProbe(version uint16, roots *x509.CertPool, cert *cryptotls.Certificate, alpn, accept bool) func(net.Conn) error {
	return func(raw net.Conn) error {
		cfg := &cryptotls.Config{MinVersion: version, MaxVersion: version, RootCAs: roots, ServerName: "localhost"}
		if cert != nil {
			cfg.GetClientCertificate = func(*cryptotls.CertificateRequestInfo) (*cryptotls.Certificate, error) {
				return cert, nil
			}
		}
		if alpn {
			cfg.NextProtos = []string{"h2"}
		}
		c := cryptotls.Client(raw, cfg)
		if err := c.Handshake(); err != nil || accept {
			return err
		}
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err := c.Read(make([]byte, 1))
		return err
	}
}

// readOnce reads from the server until it closes the connection.
func readOnce(raw net.Conn) error {
	_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := raw.Read(make([]byte, 1))
	return err
}

// TestKubehzP4Reasons: each failed handshake counts once, under its reason,
// on TLS 1.2 and 1.3, and the client sees a refusal. A valid client connects
// and counts nothing. The server is the credentials Listen builds, on kine's
// own server TLS config, with the deadline gRPC sets on a new connection.
func TestKubehzP4Reasons(t *testing.T) {
	pki := newTestPKI(t)
	serverTLS, err := pki.serverConfig().ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	creds := countHandshakeFailures(credentials.NewTLS(serverTLS), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type handshakeCase struct {
		name     string
		client   func(net.Conn) error
		deadline time.Duration // the server's handshake deadline; 0 is 10 s
		want     string        // the reason counted; "" when the handshake succeeds
	}
	trusted, untrusted := pool(pki.ca), pool(pki.other)
	var cases []handshakeCase
	for _, v := range []struct {
		name    string
		version uint16
	}{{"TLS 1.2", cryptotls.VersionTLS12}, {"TLS 1.3", cryptotls.VersionTLS13}} {
		cases = append(cases,
			handshakeCase{v.name + " valid client", tlsProbe(v.version, trusted, &pki.client, true, true), 0, ""},
			handshakeCase{v.name + " no client certificate", tlsProbe(v.version, trusted, nil, true, false), 0, handshakeNoClientCert},
			handshakeCase{v.name + " certificate from another CA", tlsProbe(v.version, trusted, &pki.otherClient, true, false), 0, handshakeUnknownAuthority},
			handshakeCase{v.name + " expired certificate", tlsProbe(v.version, trusted, &pki.expired, true, false), 0, handshakeExpired},
			handshakeCase{v.name + " certificate for server auth only", tlsProbe(v.version, trusted, &pki.serverOnly, true, false), 0, handshakeBadCertificate},
			handshakeCase{v.name + " client does not trust the server", tlsProbe(v.version, untrusted, &pki.client, true, false), 0, handshakeClientAlert},
			// gRPC refuses a client with no ALPN after the TLS handshake
			// (GRPC_ENFORCE_ALPN_ENABLED, on by default)
			handshakeCase{v.name + " valid client without h2", tlsProbe(v.version, trusted, &pki.client, false, false), 0, handshakeOther},
		)
	}
	cases = append(cases,
		handshakeCase{"plain HTTP/2", func(raw net.Conn) error {
			if _, err := raw.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")); err != nil {
				return err
			}
			return readOnce(raw)
		}, 0, handshakeNotTLS},
		handshakeCase{"closed before the handshake", func(raw net.Conn) error {
			_ = raw.Close()
			return readOnce(raw)
		}, 0, handshakeClosed},
		handshakeCase{"no bytes until the deadline", readOnce, 200 * time.Millisecond, handshakeTimeout},
	)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := handshakeCounts()
			result := make(chan error, 1)
			go func() {
				raw, err := ln.Accept()
				if err != nil {
					result <- err
					return
				}
				defer raw.Close()
				deadline := tc.deadline
				if deadline == 0 {
					deadline = 10 * time.Second
				}
				_ = raw.SetDeadline(time.Now().Add(deadline)) // as gRPC does
				conn, _, err := creds.ServerHandshake(raw)
				if conn != nil {
					_ = conn.Close()
				}
				result <- err
			}()
			raw, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			clientErr := tc.client(raw)
			var serverErr error
			select {
			case serverErr = <-result:
			case <-time.After(15 * time.Second):
				t.Fatal("the server handshake did not return")
			}

			counted := countedSince(before)
			if tc.want == "" {
				if serverErr != nil || clientErr != nil {
					t.Fatalf("a valid client: server %v, client %v; want both nil", serverErr, clientErr)
				}
				if len(counted) != 0 {
					t.Fatalf("a valid client counted %v", counted)
				}
				return
			}
			if serverErr == nil {
				t.Fatal("the server accepted the handshake")
			}
			if clientErr == nil {
				t.Fatal("the client saw no refusal")
			}
			if len(counted) != 1 || counted[tc.want] != 1 {
				t.Fatalf("counted %v, want one under %q (the server error: %v)", counted, tc.want, serverErr)
			}
		})
	}
}

// TestKubehzP4ReasonTable: the errors the real handshakes in
// TestKubehzP4Reasons do not make.
func TestKubehzP4ReasonTable(t *testing.T) {
	verifyErr := func(err error) error { return &cryptotls.CertificateVerificationError{Err: err} }
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, handshakeTimeout},
		{&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, handshakeClosed},
		{fmt.Errorf("write: %w", syscall.EPIPE), handshakeClosed},
		{io.ErrUnexpectedEOF, handshakeClosed},
		{net.ErrClosed, handshakeClosed},
		{verifyErr(x509.UnknownAuthorityError{}), handshakeUnknownAuthority},
		{verifyErr(x509.CertificateInvalidError{Reason: x509.Expired}), handshakeExpired},
		{verifyErr(x509.CertificateInvalidError{Reason: x509.NotAuthorizedToSign}), handshakeBadCertificate},
		{errors.New("tls: failed to parse client certificate: x509: malformed certificate"), handshakeBadCertificate},
		{errors.New("tls: client sent certificate containing RSA key larger than 8192 bits"), handshakeBadCertificate},
		{errors.New("tls: invalid signature by the client certificate: crypto/ecdsa: verification error"), handshakeBadCertificate},
		// etcd's check of allowed common names (not set by kine's flags)
		{errors.New("client certificate authentication failed"), handshakeBadCertificate},
		{errors.New("tls: unsupported SSLv2 handshake received"), handshakeOther},
	} {
		if got := handshakeFailureReason(tc.err); got != tc.want {
			t.Errorf("%v: reason %q, want %q", tc.err, got, tc.want)
		}
	}
}

// fakeCreds returns what the test sets, and counts its clones.
type fakeCreds struct {
	credentials.TransportCredentials
	conn   net.Conn
	info   credentials.AuthInfo
	err    error
	clones int
}

func (f *fakeCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return f.conn, f.info, f.err
}

func (f *fakeCreds) Clone() credentials.TransportCredentials {
	f.clones++
	return f
}

func (f *fakeCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "fake"}
}

// fakeAuthInfo is an AuthInfo the test can compare.
type fakeAuthInfo struct{ name string }

func (a *fakeAuthInfo) AuthType() string { return a.name }

// TestKubehzP4Unchanged: the wrapper returns what the wrapped credentials
// return, the same values; it counts a failure and nothing else; a
// connection handed off (ErrConnDispatched) is no failure; Clone keeps the
// wrapper and Info passes through.
func TestKubehzP4Unchanged(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	info := &fakeAuthInfo{name: "fake"}
	refused := errors.New("refused")
	for _, tc := range []struct {
		name string
		fake *fakeCreds
		want string
	}{
		{"success", &fakeCreds{conn: server, info: info}, ""},
		{"failure", &fakeCreds{err: refused}, handshakeOther},
		{"a failure that returns a connection", &fakeCreds{conn: server, err: refused}, handshakeOther},
		{"handed off", &fakeCreds{err: credentials.ErrConnDispatched}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := handshakeCounts()
			wrapped := countHandshakeFailures(tc.fake, nil)
			conn, gotInfo, err := wrapped.ServerHandshake(client)
			if conn != tc.fake.conn || gotInfo != tc.fake.info || err != tc.fake.err {
				t.Fatalf("got (%v, %v, %v), want (%v, %v, %v)", conn, gotInfo, err, tc.fake.conn, tc.fake.info, tc.fake.err)
			}
			counted := countedSince(before)
			if tc.want == "" && len(counted) != 0 {
				t.Fatalf("counted %v, want nothing", counted)
			}
			if tc.want != "" && (len(counted) != 1 || counted[tc.want] != 1) {
				t.Fatalf("counted %v, want one under %q", counted, tc.want)
			}
		})
	}

	fake := &fakeCreds{err: refused}
	clone := countHandshakeFailures(fake, nil).Clone()
	if _, ok := clone.(handshakeCounter); !ok || fake.clones != 1 {
		t.Fatalf("Clone returned %T after %d clones of the wrapped credentials, want a handshakeCounter after 1", clone, fake.clones)
	}
	before := handshakeCounts()
	if _, _, err := clone.ServerHandshake(client); err != refused {
		t.Fatalf("the clone's handshake: %v, want %v", err, refused)
	}
	if counted := countedSince(before); counted[handshakeOther] != 1 {
		t.Fatalf("the clone counted %v, want one under %q", counted, handshakeOther)
	}
	if got := clone.Info().SecurityProtocol; got != "fake" {
		t.Fatalf("Info: %q, want the wrapped credentials' %q", got, "fake")
	}
}

// TestKubehzP4Listen: a real kine on sqlite with --trusted-ca-file. A valid
// etcd client writes and reads; a client with no certificate and one with a
// certificate from another CA are refused, and each counts once under its
// reason. The counter is registered with every reason at 0. Without server
// TLS it is not registered.
func TestKubehzP4Listen(t *testing.T) {
	pki := newTestPKI(t)
	reg := prometheus.NewRegistry()
	// the counter is global, and the other tests made every series: only
	// Listen may make them here
	metrics.TLSHandshakeFailures.Reset()
	e := listenP4(t, pki.serverConfig(), reg)
	n, err := testutil.GatherAndCount(reg, "kine_tls_handshake_failures_total")
	if err != nil {
		t.Fatal(err)
	}
	if n != len(handshakeFailureReasons) {
		t.Fatalf("kine_tls_handshake_failures_total has %d series after Listen, want %d (every reason at 0)", n, len(handshakeFailureReasons))
	}

	before := handshakeCounts()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		TLS:         &cryptotls.Config{Certificates: []cryptotls.Certificate{pki.client}, RootCAs: pool(pki.ca)},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, "/a", "v"); err != nil {
		t.Fatalf("Put with a valid client certificate: %v", err)
	}
	if got, err := client.Get(ctx, "/a"); err != nil || len(got.Kvs) != 1 {
		t.Fatalf("Get with a valid client certificate: %v, %v", got, err)
	}
	_ = client.Close()
	if counted := countedSince(before); len(counted) != 0 {
		t.Fatalf("a valid client counted %v", counted)
	}

	address := strings.TrimPrefix(e.Endpoints[0], "https://")
	for _, cert := range []*cryptotls.Certificate{nil, &pki.otherClient} {
		raw, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		if err := tlsProbe(cryptotls.VersionTLS13, pool(pki.ca), cert, true, false)(raw); err == nil {
			t.Fatal("kine accepted a client without a certificate from its CA")
		}
		_ = raw.Close()
	}
	want := map[string]float64{handshakeNoClientCert: 1, handshakeUnknownAuthority: 1}
	deadline := time.Now().Add(5 * time.Second)
	for {
		counted := countedSince(before)
		if fmt.Sprint(counted) == fmt.Sprint(want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("counted %v, want %v", counted, want)
		}
		time.Sleep(10 * time.Millisecond)
	}

	plain := prometheus.NewRegistry()
	listenP4(t, kinetls.Config{}, plain)
	n, err = testutil.GatherAndCount(plain, "kine_tls_handshake_failures_total")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("kine without server TLS registered kine_tls_handshake_failures_total (%d series)", n)
	}
}

// listenP4 starts a kine on sqlite on a loopback TCP port with server TLS
// (none when serverTLS is empty) and reg as its metrics registerer. Each
// option changes the config before Listen.
func listenP4(t *testing.T, serverTLS kinetls.Config, reg prometheus.Registerer, options ...func(*Config)) ETCDConfig {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	config := Config{
		WaitGroup:         wg,
		Listener:          "tcp://127.0.0.1:0",
		Endpoint:          "sqlite://" + dir + "/state.db?" + sqlite.DefaultParams,
		ServerTLSConfig:   serverTLS,
		MetricsRegisterer: reg,
		NotifyInterval:    5 * time.Second,
		// the flag defaults; the backend refuses zero values for these
		CompactTimeout:   5 * time.Second,
		CompactMinRetain: 1000,
		CompactBatchSize: 1000,
		PollBatchSize:    500,
	}
	for _, option := range options {
		option(&config)
	}
	e, err := Listen(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
