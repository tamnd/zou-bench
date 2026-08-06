package pgwire

import (
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// sockDir makes a short-lived directory with a short name under the
// system temp dir, because t.TempDir() embeds the test name and the
// resulting socket path overflows the roughly 104 byte unix socket
// path limit on macOS. Windows has no /tmp at all, which is why the
// base comes from the system rather than being spelled out.
func sockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pgw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// fakeServer speaks just enough of the server side to exercise the
// client: reads the startup packet, answers with the given script.
func fakeServer(t *testing.T, handle func(c net.Conn)) string {
	t.Helper()
	sock := filepath.Join(sockDir(t), ".s.PGSQL.5432")
	ln, err := net.Listen("unix", sock)
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
			go handle(c)
		}
	}()
	return sock
}

func msg(kind byte, payload ...byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:], uint32(4+len(payload)))
	copy(out[5:], payload)
	return out
}

func authOK() []byte { return msg('R', 0, 0, 0, 0) }
func ready() []byte  { return msg('Z', 'I') }
func readLen(c net.Conn) []byte {
	var head [4]byte
	readFull(c, head[:])
	body := make([]byte, binary.BigEndian.Uint32(head[:])-4)
	readFull(c, body)
	return body
}

func TestQueryReturnsTheFirstValue(t *testing.T) {
	sock := fakeServer(t, func(c net.Conn) {
		startup := readLen(c)
		if binary.BigEndian.Uint32(startup) != 196608 {
			t.Error("wrong protocol version")
		}
		c.Write(append(authOK(), ready()...))
		var kind [1]byte
		readFull(c, kind[:])
		readLen(c)
		row := append([]byte{0, 1, 0, 0, 0, 2}, "42"...)
		c.Write(msg('D', row...))
		c.Write(msg('C', 'S', 'E', 'L', 0))
		c.Write(ready())
	})
	conn, err := Dial(sock, "bench", "postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	got, err := conn.Query("select 42")
	if err != nil || got != "42" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestStartingUpIsRetriableAndOthersAreNot(t *testing.T) {
	starting := append([]byte{'C'}, "57P03"...)
	starting = append(starting, 0)
	starting = append(starting, 'M')
	starting = append(starting, "the database system is starting up"...)
	starting = append(starting, 0, 0)
	sock := fakeServer(t, func(c net.Conn) {
		readLen(c)
		c.Write(msg('E', starting...))
		c.Close()
	})
	_, err := Dial(sock, "bench", "postgres")
	if !Starting(err) {
		t.Fatalf("57P03 not retriable: %v", err)
	}

	fatal := append([]byte{'C'}, "28000"...)
	fatal = append(fatal, 0)
	fatal = append(fatal, 'M')
	fatal = append(fatal, "no such role"...)
	fatal = append(fatal, 0, 0)
	sock2 := fakeServer(t, func(c net.Conn) {
		readLen(c)
		c.Write(msg('E', fatal...))
		c.Close()
	})
	if _, err := WaitReady(sock2, "bench", "postgres", time.Now().Add(5*time.Second)); err == nil {
		t.Fatal("fatal error retried to the deadline")
	} else if Starting(err) {
		t.Fatal("28000 marked retriable")
	}
}

func TestWaitReadyOutlivesARefusedSocket(t *testing.T) {
	sock := filepath.Join(sockDir(t), ".s.PGSQL.5432")
	go func() {
		time.Sleep(50 * time.Millisecond)
		ln, err := net.Listen("unix", sock)
		if err != nil {
			return
		}
		defer ln.Close()
		c, err := ln.Accept()
		if err != nil {
			return
		}
		readLen(c)
		c.Write(append(authOK(), ready()...))
		var kind [1]byte
		readFull(c, kind[:])
		readLen(c)
		c.Write(msg('C', 'S', 'E', 'L', 0))
		c.Write(ready())
	}()
	conn, err := WaitReady(sock, "bench", "postgres", time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}
