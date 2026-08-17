// Package pgwire is the smallest postgres client that can time an
// attach honestly. pg_ctl -w polls readiness ten times a second and
// psql pays process startup, both are noise on the same order as the
// thing a cold attach measurement wants to see, so this speaks just
// enough of the v3 wire protocol to connect over trust auth and run
// one simple query, and nothing more. Not a general client: no other
// auth, no TLS, no extended protocol.
package pgwire

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
)

// Conn is one open backend connection past its startup handshake.
type Conn struct {
	c        net.Conn
	password string
}

// Dial connects and completes the startup handshake. Addr is a unix
// socket path like /tmp/.s.PGSQL.5432 or a host:port pair. A path
// separator is what tells them apart, either kind, because on Windows
// a socket path starts with a drive letter rather than a slash. The
// server must trust the connection, any authentication request is an
// error.
func Dial(addr, user, database string) (*Conn, error) {
	return DialPassword(addr, user, database, "")
}

// DialPassword is the same with a password to answer a cleartext ask
// with, which is what a zou node's postgres door wants: the password
// there is the project's api key, and asking for it in the clear is the
// point rather than a weakness, since a bearer token proves nothing
// until it is read. A postmaster on trust never asks and the password
// goes unused.
func DialPassword(addr, user, database, password string) (*Conn, error) {
	network := "tcp"
	if strings.ContainsAny(addr, `/\`) {
		network = "unix"
	}
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	conn := &Conn{c: c, password: password}
	if err := conn.startup(user, database); err != nil {
		c.Close()
		return nil, err
	}
	return conn, nil
}

func (conn *Conn) Close() error { return conn.c.Close() }

func (conn *Conn) startup(user, database string) error {
	body := []byte{0, 3, 0, 0} // protocol 3.0
	for _, kv := range [][2]string{{"user", user}, {"database", database}} {
		body = append(body, kv[0]...)
		body = append(body, 0)
		body = append(body, kv[1]...)
		body = append(body, 0)
	}
	body = append(body, 0)
	msg := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(msg, uint32(len(msg)))
	copy(msg[4:], body)
	if _, err := conn.c.Write(msg); err != nil {
		return err
	}
	for {
		kind, payload, err := conn.read()
		if err != nil {
			return err
		}
		switch kind {
		case 'R':
			switch code := binary.BigEndian.Uint32(payload); code {
			case 0: // AuthenticationOk
			case 3: // cleartext password
				if conn.password == "" {
					return fmt.Errorf("server wants a password and none was given")
				}
				if err := conn.sendPassword(); err != nil {
					return err
				}
			default:
				return fmt.Errorf("server wants auth method %d, pgwire does trust and cleartext", code)
			}
		case 'E':
			return errorResponse(payload)
		case 'Z':
			return nil
		// Parameter status, backend key data, notices: all fine.
		case 'S', 'K', 'N':
		default:
			return fmt.Errorf("unexpected %c during startup", kind)
		}
	}
}

// sendPassword answers a cleartext ask, which is a PasswordMessage
// carrying the password with its terminator.
func (conn *Conn) sendPassword() error {
	msg := make([]byte, 5+len(conn.password)+1)
	msg[0] = 'p'
	binary.BigEndian.PutUint32(msg[1:], uint32(len(msg)-1))
	copy(msg[5:], conn.password)
	_, err := conn.c.Write(msg)
	return err
}

// Query runs one simple query and returns the first column of the
// first row, empty when the query returned no rows.
func (conn *Conn) Query(sql string) (string, error) {
	msg := make([]byte, 5+len(sql)+1)
	msg[0] = 'Q'
	binary.BigEndian.PutUint32(msg[1:], uint32(len(msg)-1))
	copy(msg[5:], sql)
	value, queryErr := "", error(nil)
	if _, err := conn.c.Write(msg); err != nil {
		return "", err
	}
	for {
		kind, payload, err := conn.read()
		if err != nil {
			return "", err
		}
		switch kind {
		case 'D':
			if value == "" && binary.BigEndian.Uint16(payload) > 0 {
				n := binary.BigEndian.Uint32(payload[2:])
				if n != 0xFFFFFFFF { // -1 is a null
					value = string(payload[6 : 6+n])
				}
			}
		case 'E':
			queryErr = errorResponse(payload)
		case 'Z':
			return value, queryErr
		// Row description, command complete, empty query, notices.
		case 'T', 'C', 'I', 'N', 'S':
		default:
			return "", fmt.Errorf("unexpected %c in query response", kind)
		}
	}
}

func (conn *Conn) read() (byte, []byte, error) {
	var head [5]byte
	if _, err := readFull(conn.c, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[1:]) - 4
	payload := make([]byte, n)
	if _, err := readFull(conn.c, payload); err != nil {
		return 0, nil, err
	}
	return head[0], payload, nil
}

func readFull(c net.Conn, buf []byte) (int, error) {
	got := 0
	for got < len(buf) {
		n, err := c.Read(buf[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

// Err is a decoded ErrorResponse. The code matters to callers polling
// a starting server: 57P03 is "the database system is starting up".
type Err struct {
	Code    string
	Message string
}

func (e *Err) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Starting reports whether the error means the server is up but not
// yet accepting queries, which a readiness poll treats as keep going.
func Starting(err error) bool {
	pgerr, ok := err.(*Err)
	return ok && pgerr.Code == "57P03"
}

func errorResponse(payload []byte) error {
	e := &Err{}
	for len(payload) > 0 && payload[0] != 0 {
		field := payload[0]
		end := 1
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		switch field {
		case 'C':
			e.Code = string(payload[1:end])
		case 'M':
			e.Message = string(payload[1:end])
		}
		payload = payload[end+1:]
	}
	return e
}

// WaitReady polls until a connection both opens and answers a trivial
// query, then returns it still open. Dial refusals, missing sockets,
// dropped connections, and 57P03 all mean keep trying until the
// deadline; any other server error is permanent and returns at once.
func WaitReady(addr, user, database string, deadline time.Time) (*Conn, error) {
	for time.Now().Before(deadline) {
		conn, err := Dial(addr, user, database)
		if err == nil {
			if _, err = conn.Query("select 1"); err == nil {
				return conn, nil
			}
			conn.Close()
		}
		if pgerr, ok := err.(*Err); ok && !Starting(err) {
			return nil, pgerr
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("server never came up at %s", addr)
}
