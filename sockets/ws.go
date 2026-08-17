package sockets

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Conn is one websocket, opened by hand.
//
// A hundred thousand sockets is the whole point of this driver, so the
// client is the smallest thing that can hold one honestly: a dial, an
// upgrade, and RFC 6455 frames. A library would bring its own buffer
// sizes, and buffer sizes are what a hundred thousand of anything is
// made of. Here they are the caller's, which is what lets the generator
// hold as many sockets as the box has memory for rather than as many as
// a default read buffer allows.
//
// Not a general client: no permessage deflate, no fragmentation on the
// way out, and a close is a close.
type Conn struct {
	c    net.Conn
	r    *bufio.Reader
	mu   sync.Mutex
	mask [4]byte
}

// Dial opens a websocket at rawurl, from local when it is not nil.
//
// The local address is how one box holds a hundred thousand sockets to
// one port: an ephemeral port space belongs to a source address, so a
// generator with three addresses on it has three of them. Passing nil
// lets the kernel choose, which is right up to about sixty thousand.
func Dial(rawurl string, local *net.TCPAddr, timeout time.Duration, readBuf int) (*Conn, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "80")
	}
	dialer := net.Dialer{Timeout: timeout, LocalAddr: local}
	c, err := dialer.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	// Nagle off, because a join is one small frame and the reply to it
	// is what the run times.
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		c.Close()
		return nil, err
	}
	nonce := base64.StdEncoding.EncodeToString(key)
	path := u.RequestURI()
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + nonce + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		c.Close()
		return nil, err
	}
	if _, err := io.WriteString(c, req); err != nil {
		c.Close()
		return nil, err
	}
	if readBuf < 512 {
		readBuf = 512
	}
	conn := &Conn{c: c, r: bufio.NewReaderSize(c, readBuf)}
	if _, err := rand.Read(conn.mask[:]); err != nil {
		c.Close()
		return nil, err
	}
	status, err := conn.r.ReadString('\n')
	if err != nil {
		c.Close()
		return nil, err
	}
	if !strings.Contains(status, " 101 ") {
		// The body says why, and a run that cannot open a socket wants
		// the server's own sentence rather than "unexpected status".
		rest, _ := io.ReadAll(io.LimitReader(conn.r, 512))
		c.Close()
		return nil, fmt.Errorf("upgrade refused: %s%s", strings.TrimSpace(status), trimBody(string(rest)))
	}
	for {
		line, err := conn.r.ReadString('\n')
		if err != nil {
			c.Close()
			return nil, err
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	// The deadline was the handshake's. A socket that is subscribed and
	// waiting for a row must not time out for being quiet.
	if err := c.SetDeadline(time.Time{}); err != nil {
		c.Close()
		return nil, err
	}
	return conn, nil
}

func trimBody(body string) string {
	body = strings.TrimSpace(body)
	if i := strings.Index(body, "\r\n\r\n"); i >= 0 {
		body = strings.TrimSpace(body[i+4:])
	}
	if body == "" {
		return ""
	}
	return ", " + body
}

// WriteText sends one text frame, masked as a client must.
func (conn *Conn) WriteText(payload []byte) error {
	return conn.write(0x1, payload)
}

func (conn *Conn) write(opcode byte, payload []byte) error {
	head := make([]byte, 0, 14)
	head = append(head, 0x80|opcode)
	n := len(payload)
	switch {
	case n < 126:
		head = append(head, 0x80|byte(n))
	case n < 1<<16:
		head = append(head, 0x80|126, byte(n>>8), byte(n))
	default:
		head = append(head, 0x80|127)
		var wide [8]byte
		binary.BigEndian.PutUint64(wide[:], uint64(n))
		head = append(head, wide[:]...)
	}
	head = append(head, conn.mask[:]...)
	body := make([]byte, n)
	for i := range payload {
		body[i] = payload[i] ^ conn.mask[i%4]
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if _, err := conn.c.Write(append(head, body...)); err != nil {
		return err
	}
	return nil
}

// Read returns the next text or binary message, answering pings on the
// way so a quiet subscriber is not closed for being quiet.
//
// A fragmented message is reassembled, because a server is allowed to
// send one and a client that could not read it would be a client that
// works until the day something upstream changes its buffer size.
func (conn *Conn) Read() ([]byte, error) {
	var whole []byte
	for {
		final, opcode, payload, err := conn.frame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x9: // ping
			if err := conn.write(0xA, payload); err != nil {
				return nil, err
			}
		case 0xA: // pong
		case 0x8: // close
			_ = conn.write(0x8, nil)
			return nil, io.EOF
		case 0x0, 0x1, 0x2:
			whole = append(whole, payload...)
			if final {
				return whole, nil
			}
		default:
			return nil, fmt.Errorf("opcode %d", opcode)
		}
	}
}

func (conn *Conn) frame() (bool, byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(conn.r, head[:]); err != nil {
		return false, 0, nil, err
	}
	final := head[0]&0x80 != 0
	opcode := head[0] & 0x0F
	masked := head[1]&0x80 != 0
	n := uint64(head[1] & 0x7F)
	switch n {
	case 126:
		var wide [2]byte
		if _, err := io.ReadFull(conn.r, wide[:]); err != nil {
			return false, 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(wide[:]))
	case 127:
		var wide [8]byte
		if _, err := io.ReadFull(conn.r, wide[:]); err != nil {
			return false, 0, nil, err
		}
		n = binary.BigEndian.Uint64(wide[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(conn.r, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(conn.r, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return final, opcode, payload, nil
}

// Close ends the socket, politely when it can.
func (conn *Conn) Close() error {
	_ = conn.write(0x8, []byte{0x03, 0xE8})
	return conn.c.Close()
}
