// Package probe measures a real object store's latency curve and
// writes a calibration file that zou's ZOU_STORE_SIM loads in place of
// its built in profiles. The point is that simulated numbers should
// come from a measurement someone actually ran, from the machine that
// will run zou, not from a marketing page.
//
// The client here is a minimal SigV4 signer over net/http, which keeps
// the harness dependency free and works against anything speaking the
// S3 dialect: AWS, R2, GCS interop, B2, Wasabi, MinIO.
package probe

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Client is a path style S3 client for one bucket. Path style because
// virtual hosted style breaks on MinIO and on any endpoint reached by
// IP, and every provider still accepts path style requests.
type Client struct {
	Endpoint  string // scheme://host[:port], no trailing slash
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	HTTP      *http.Client
}

// do sends one signed request and returns the HTTP status. A transport
// failure is an error, a bad status is not, the caller decides what a
// 503 means because for the probe a SlowDown is data, not a failure.
func (c *Client) do(method, key string, query url.Values, body []byte) (int, string, error) {
	path := "/" + c.Bucket
	if key != "" {
		path += "/" + key
	}
	u := strings.TrimSuffix(c.Endpoint, "/") + uriEncode(path, true)
	if qs := canonicalQuery(query); qs != "" {
		u += "?" + qs
	}
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	sign(req, c.AccessKey, c.SecretKey, c.Region, hexSHA256(body), time.Now())
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.StatusCode, string(snippet), nil
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, "", err
}

func (c *Client) put(key string, body []byte) (int, string, error) {
	return c.do(http.MethodPut, key, nil, body)
}

func (c *Client) get(key string) (int, string, error) {
	return c.do(http.MethodGet, key, nil, nil)
}

func (c *Client) list(prefix string, maxKeys int) (int, string, error) {
	q := url.Values{}
	q.Set("list-type", "2")
	q.Set("max-keys", fmt.Sprint(maxKeys))
	q.Set("prefix", prefix)
	return c.do(http.MethodGet, "", q, nil)
}

func (c *Client) delete(key string) (int, string, error) {
	return c.do(http.MethodDelete, key, nil, nil)
}

// sign adds SigV4 headers to req. Every header already on the request
// is signed, plus host, x-amz-date, and x-amz-content-sha256, which is
// the full set the probe ever sends.
func sign(req *http.Request, access, secret, region, payloadHash string, t time.Time) {
	amzDate := t.UTC().Format("20060102T150405Z")
	scopeDate := t.UTC().Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	headers := map[string]string{"host": host}
	for k, vs := range req.Header {
		headers[strings.ToLower(k)] = strings.TrimSpace(strings.Join(vs, ","))
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k)
		canonHeaders.WriteByte(':')
		canonHeaders.WriteString(headers[k])
		canonHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")
	canonical := strings.Join([]string{
		req.Method,
		uriEncode(req.URL.Path, true),
		canonicalQuery(req.URL.Query()),
		canonHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := scopeDate + "/" + region + "/s3/aws4_request"
	toSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256([]byte(canonical)),
	}, "\n")
	key := hmacSHA256([]byte("AWS4"+secret), scopeDate)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+access+"/"+scope+
			",SignedHeaders="+signedHeaders+",Signature="+sig)
}

// uriEncode is the AWS flavor of percent encoding: unreserved bytes
// pass through, everything else becomes uppercase %XX, and the slash
// survives only in paths. url.PathEscape is close but not identical,
// and a signature mismatch is a miserable thing to debug, so this is
// spelled out.
func uriEncode(s string, keepSlash bool) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~',
			keepSlash && c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// canonicalQuery encodes a query string the way the signature expects:
// keys sorted, values sorted within a key, both percent encoded. The
// request uses the same string, so what the server verifies is exactly
// what was signed.
func canonicalQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := append([]string(nil), q[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k, false)+"="+uriEncode(v, false))
		}
	}
	return strings.Join(parts, "&")
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}
