package resthttp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"time"
)

// Mint signs an HS256 token the way GoTrue and zou do, so a run needs
// the project's jwt secret and nothing else. Both projects verify the
// same way and neither reads anything the header does not carry, so a
// token minted here is a token either accepts.
func Mint(secret string, claims map[string]any) string {
	enc := base64.RawURLEncoding.EncodeToString
	head, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	body, _ := json.Marshal(claims)
	signing := enc(head) + "." + enc(body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	return signing + "." + enc(mac.Sum(nil))
}

// KeyToken is an anon or service_role api key, which is a token with a
// role and a ten year life and no subject.
func KeyToken(secret, role string) string {
	now := time.Now().Unix()
	return Mint(secret, map[string]any{
		"iss":  "zou",
		"role": role,
		"iat":  now,
		"exp":  now + 10*365*24*3600,
	})
}

// UserToken is what a signed in user carries, with the claim set the
// RLS policies read: sub for auth.uid, role, email, and an hour of
// life, long enough that a run cannot outlive it.
func UserToken(secret, sub, email string) string {
	now := time.Now().Unix()
	return Mint(secret, map[string]any{
		"sub":   sub,
		"role":  "authenticated",
		"aud":   "authenticated",
		"email": email,
		"iat":   now,
		"exp":   now + 3600,
	})
}
