package metrics

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestServerEndpoints(t *testing.T) {
	reg := NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "test_events_total", Help: "test"})
	reg.MustRegister(c)
	c.Add(3)

	addr := freeAddr(t)
	srv := NewServer(addr, reg, slog.New(slog.DiscardHandler))
	srv.HandleFunc("GET /extra", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("extra")) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	base := "http://" + addr
	deadline := time.Now().Add(5 * time.Second)
	for Probe(addr) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("server did not become healthy")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if code, _ := get(t, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("readyz before SetReady = %d", code)
	}
	srv.SetReady(true)
	if code, _ := get(t, base+"/readyz"); code != http.StatusOK {
		t.Errorf("readyz after SetReady = %d", code)
	}
	if _, body := get(t, base+"/metrics"); !strings.Contains(body, "tradewatch_test_events_total 3") || !strings.Contains(body, "go_goroutines") {
		t.Errorf("metrics output missing expected series:\n%s", body)
	}
	if _, body := get(t, base+"/extra"); body != "extra" {
		t.Errorf("extra route = %q", body)
	}
	if code, body := get(t, base+"/debug/pprof/"); code != http.StatusOK || !strings.Contains(body, "goroutine") {
		t.Errorf("pprof index = %d:\n%s", code, body)
	}
	if code, body := get(t, base+"/debug/pprof/heap?debug=1"); code != http.StatusOK || !strings.Contains(body, "heap profile") {
		t.Errorf("heap profile = %d:\n%.200s", code, body)
	}
	if code, body := get(t, base+"/debug/pprof/profile?seconds=1"); code != http.StatusOK || len(body) == 0 {
		t.Errorf("CPU profile = %d, %d bytes", code, len(body))
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if Probe(addr) == 0 {
		t.Error("probe succeeded after shutdown")
	}
}
