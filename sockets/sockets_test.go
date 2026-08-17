package sockets

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTheEndpointIsBuiltFromWhateverBaseTheCallerHas(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"http://127.0.0.1:54321", "ws://127.0.0.1:54321/realtime/v1/websocket?apikey=k&vsn=2.0.0"},
		{"http://127.0.0.1:54321/", "ws://127.0.0.1:54321/realtime/v1/websocket?apikey=k&vsn=2.0.0"},
		{"ws://host/realtime/v1/websocket", "ws://host/realtime/v1/websocket?apikey=k&vsn=2.0.0"},
		{"http://host/acme-prod", "ws://host/acme-prod/realtime/v1/websocket?apikey=k&vsn=2.0.0"},
	} {
		got, err := wsURL(c.in, "k")
		if err != nil || got != c.want {
			t.Errorf("%s gave %q, %v", c.in, got, err)
		}
	}
	if _, err := wsURL("https://host", "k"); err == nil {
		t.Error("tls was accepted by a driver that cannot speak it")
	}
	if _, err := wsURL("", "k"); err == nil {
		t.Error("an empty base url was accepted")
	}

	// Several ports is the same endpoint with the port swapped, since the
	// node serves one api on all of them.
	got, err := wsURLs("http://host:54321/acme", "k", []int{54321, 54322})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ws://host:54321/acme/realtime/v1/websocket?apikey=k&vsn=2.0.0",
		"ws://host:54322/acme/realtime/v1/websocket?apikey=k&vsn=2.0.0",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("ports gave %v", got)
	}
	if _, err := wsURLs("http://host:54321", "k", []int{0}); err == nil {
		t.Error("port zero was accepted as somewhere to open a socket")
	}
}

func TestASocketRunRefusesAShapeItCannotMeasure(t *testing.T) {
	if _, err := Run(context.Background(), Options{Sockets: 0}); err == nil {
		t.Error("a run with no sockets was accepted")
	}
	// More shards than sockets would leave shards nobody joined, and rows
	// written to them would read as missing deliveries.
	if _, err := Run(context.Background(), Options{Sockets: 4, Shards: 8}); err == nil {
		t.Error("more shards than sockets was accepted")
	}
}

// fakeRealtime is the server half: it upgrades, answers a join, and
// remembers which shard each socket asked for so the fake database can
// push to them.
type fakeRealtime struct {
	mu      sync.Mutex
	byShard map[int][]*Conn
	onPort  map[int]int
	joins   int
	addr    string
	// ports is every port this fake answers on, the one in addr first,
	// the way a node with `--http a,b,c` answers.
	ports []int
}

func newFakeRealtime(t *testing.T, doors int) *fakeRealtime {
	t.Helper()
	f := &fakeRealtime{byShard: map[int][]*Conn{}, onPort: map[int]int{}}
	for range doors {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		if f.addr == "" {
			f.addr = ln.Addr().String()
		}
		f.ports = append(f.ports, ln.Addr().(*net.TCPAddr).Port)
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go f.serve(c)
			}
		}()
	}
	return f
}

func (f *fakeRealtime) serve(c net.Conn) {
	if at, ok := c.LocalAddr().(*net.TCPAddr); ok {
		f.mu.Lock()
		f.onPort[at.Port]++
		f.mu.Unlock()
	}
	r := bufio.NewReaderSize(c, 4096)
	var key string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			c.Close()
			return
		}
		if v, ok := header(line, "Sec-WebSocket-Key"); ok {
			key = v
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(sum[:])+"\r\n\r\n")
	// The client type reads either direction's frames, so the server side
	// borrows it rather than growing a second frame reader for a test.
	conn := &Conn{c: c, r: r}
	for {
		msg, err := conn.Read()
		if err != nil {
			return
		}
		event, payload := eventOf(msg)
		if event != "phx_join" {
			continue
		}
		topic, _ := topicOf(msg)
		shard := shardOf(payload)
		f.mu.Lock()
		f.byShard[shard] = append(f.byShard[shard], conn)
		f.joins++
		f.mu.Unlock()
		serverText(c, fmt.Sprintf(
			`["1","1",%q,"phx_reply",{"status":"ok","response":{"postgres_changes":[{"id":1}]}}]`, topic))
	}
}

func (f *fakeRealtime) changed(shard int, sentUS int64) {
	f.mu.Lock()
	socks := append([]*Conn(nil), f.byShard[shard]...)
	f.mu.Unlock()
	frame := fmt.Sprintf(`[null,null,"realtime:shard-%d","postgres_changes",`+
		`{"ids":[1],"data":{"type":"INSERT","table":"pulse","record":{"shard":%d,"sent_us":"%d"}}}]`,
		shard, shard, sentUS)
	for _, conn := range socks {
		serverText(conn.c, frame)
	}
}

func header(line, name string) (string, bool) {
	if !strings.HasPrefix(strings.ToLower(line), strings.ToLower(name)+":") {
		return "", false
	}
	return strings.TrimSpace(line[len(name)+1:]), true
}

// serverText writes one unmasked text frame, which is what a server
// sends. Nothing here needs a payload past 64k.
func serverText(c net.Conn, text string) {
	head := []byte{0x81}
	n := len(text)
	switch {
	case n < 126:
		head = append(head, byte(n))
	default:
		head = append(head, 126, byte(n>>8), byte(n))
	}
	c.Write(append(head, text...))
}

func topicOf(msg []byte) (string, bool) {
	parts := strings.SplitN(string(msg), `","`, 4)
	if len(parts) < 4 {
		return "", false
	}
	return parts[2], true
}

func shardOf(payload map[string]any) int {
	config, _ := payload["config"].(map[string]any)
	wants, _ := config["postgres_changes"].([]any)
	if len(wants) == 0 {
		return 0
	}
	first, _ := wants[0].(map[string]any)
	filter, _ := first["filter"].(string)
	n, _ := strconv.Atoi(strings.TrimPrefix(filter, "shard=eq."))
	return n
}

// fakePG speaks enough of the wire protocol for the writer, and pushes
// every row it is handed at the sockets that asked for that shard, which
// is what closes the loop in this test without a database in it.
func newFakePG(t *testing.T, rt *fakeRealtime) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				if err := pgHandshake(c); err != nil {
					return
				}
				for {
					sql, err := pgQuery(c)
					if err != nil {
						return
					}
					for _, row := range rows(sql) {
						rt.changed(int(row[0]), row[1])
					}
					c.Write(append(pgMsg('C', append([]byte("INSERT 0 1"), 0)...), pgMsg('Z', 'I')...))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func pgHandshake(c net.Conn) error {
	var head [4]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return err
	}
	body := make([]byte, binary.BigEndian.Uint32(head[:])-4)
	if _, err := io.ReadFull(c, body); err != nil {
		return err
	}
	_, err := c.Write(append(pgMsg('R', 0, 0, 0, 0), pgMsg('Z', 'I')...))
	return err
}

func pgQuery(c net.Conn) (string, error) {
	var head [5]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return "", err
	}
	body := make([]byte, binary.BigEndian.Uint32(head[1:])-4)
	if _, err := io.ReadFull(c, body); err != nil {
		return "", err
	}
	return strings.TrimRight(string(body), "\x00"), nil
}

func pgMsg(kind byte, payload ...byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:], uint32(4+len(payload)))
	copy(out[5:], payload)
	return out
}

// rows pulls the (shard,sent_us) pairs back out of the insert the writer
// sent, which is also a check that the statement is the shape it claims.
func rows(sql string) [][2]int64 {
	i := strings.Index(sql, "values ")
	if !strings.HasPrefix(sql, "insert into public.pulse (shard, sent_us) values ") || i < 0 {
		return nil
	}
	var out [][2]int64
	for _, tuple := range strings.Split(sql[i+len("values "):], "),(") {
		tuple = strings.Trim(tuple, "()")
		pair := strings.Split(tuple, ",")
		if len(pair) != 2 {
			continue
		}
		shard, err1 := strconv.ParseInt(pair[0], 10, 64)
		stamp, err2 := strconv.ParseInt(pair[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, [2]int64{shard, stamp})
	}
	return out
}

// The whole loop: sockets join shards, rows are committed, and every
// delivery the accounting expected is a delivery that arrived.
func TestEveryRowReachesEverySocketOnItsShard(t *testing.T) {
	rt := newFakeRealtime(t, 1)
	dsn := newFakePG(t, rt)
	out, err := Run(context.Background(), Options{
		BaseURL:  "http://" + rt.addr,
		APIKey:   "anon",
		Sockets:  8,
		Shards:   2,
		Dialers:  4,
		Table:    "pulse",
		WriteDSN: dsn,
		Writers:  2,
		Rows:     40,
		Batch:    2,
		Duration: 1500 * time.Millisecond,
		Drain:    300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Held != 8 || out.Refused != 0 || out.Lost != 0 {
		t.Fatalf("held %d, refused %d, lost %d: %v", out.Held, out.Refused, out.Lost, out.Failures)
	}
	if out.Rows < 20 {
		t.Fatalf("only %d rows in a second and a half at forty a second", out.Rows)
	}
	if out.Expected != out.Rows*4 {
		t.Errorf("%d rows over shards with four sockets each should owe %d deliveries, not %d",
			out.Rows, out.Rows*4, out.Expected)
	}
	if out.Delivered != out.Expected || out.Missing != 0 {
		t.Errorf("expected %d deliveries, saw %d, missing %d", out.Expected, out.Delivered, out.Missing)
	}
	if _, ok := out.DeliveryMS["p99"]; !ok {
		t.Error("a run with deliveries in it published no p99")
	}
	if out.PerShard.Empty != 0 || out.PerShard.Shards != 2 {
		t.Errorf("shard spread is wrong: %+v", out.PerShard)
	}
	if len(out.Timeline) == 0 {
		t.Error("a second and a half of deliveries left no timeline")
	}
	if out.ConnectMS["p50"] == 0 && out.JoinMS["p50"] == 0 {
		t.Error("neither the dial nor the join was timed")
	}
}

// One client address has about 64k ports to one destination port, so a
// run past that has to spread its sockets over the ports the node
// answers on, evenly, or the arithmetic that says it fits is wrong.
func TestSocketsAreSpreadOverEveryPortTheNodeAnswersOn(t *testing.T) {
	rt := newFakeRealtime(t, 3)
	out, err := Run(context.Background(), Options{
		BaseURL:  "http://" + rt.addr,
		APIKey:   "anon",
		Ports:    rt.ports,
		Sockets:  9,
		Shards:   3,
		Dialers:  3,
		Table:    "pulse",
		Duration: 300 * time.Millisecond,
		Drain:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Held != 9 {
		t.Fatalf("held %d of 9: %v", out.Held, out.Failures)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, port := range rt.ports {
		if rt.onPort[port] != 3 {
			t.Errorf("port %d took %d of the nine sockets, not three: %v", port, rt.onPort[port], rt.onPort)
		}
	}
}

// A row committed before the window opens belongs to nobody's
// percentile, and the count of those is published rather than dropped.
func TestRowsFromOutsideTheWindowAreCountedApart(t *testing.T) {
	rt := newFakeRealtime(t, 1)
	dsn := newFakePG(t, rt)
	out, err := Run(context.Background(), Options{
		BaseURL:  "http://" + rt.addr,
		APIKey:   "anon",
		Sockets:  2,
		Shards:   1,
		Dialers:  2,
		Table:    "pulse",
		WriteDSN: dsn,
		Writers:  1,
		Rows:     20,
		Batch:    1,
		Warmup:   600 * time.Millisecond,
		Duration: 600 * time.Millisecond,
		Drain:    200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Stray == 0 {
		t.Error("a warmup that wrote rows left nothing outside the window")
	}
	if out.Delivered != out.Expected {
		t.Errorf("the measured window still has to balance: %d of %d", out.Delivered, out.Expected)
	}
}

// A socket that goes away mid run is a lost socket and says why, because
// a run that quietly ended up holding half of what it claims is the one
// number nobody should have to work out afterwards.
func TestASocketThatDiesMidRunIsReported(t *testing.T) {
	rt := newFakeRealtime(t, 1)
	go func() {
		// Wait for the joins, then cut them.
		for range 100 {
			rt.mu.Lock()
			socks := rt.byShard[0]
			rt.mu.Unlock()
			if len(socks) == 2 {
				time.Sleep(50 * time.Millisecond)
				for _, conn := range socks {
					conn.c.Close()
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	out, err := Run(context.Background(), Options{
		BaseURL:  "http://" + rt.addr,
		APIKey:   "anon",
		Sockets:  2,
		Shards:   1,
		Dialers:  2,
		Table:    "pulse",
		Duration: 500 * time.Millisecond,
		Drain:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Lost != 2 || len(out.Failures) == 0 {
		t.Fatalf("lost %d with reasons %v", out.Lost, out.Failures)
	}
}
