package auth

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var secret = []byte("0123456789abcdef0123456789abcdef")

func newAuth(t *testing.T, users ...User) *Authenticator {
	t.Helper()
	if len(users) == 0 {
		users = []User{mustUser(t, "ana", RoleAnalyst, "correct horse battery"), mustUser(t, "root", RoleAdmin, "staple battery horse")}
	}
	a, err := New(Config{Secret: secret, TTL: time.Hour, Issuer: "tradewatch", Audience: "tradewatch-api", Users: users})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustUser(t *testing.T, name string, role Role, password string) User {
	t.Helper()
	u, err := NewUser(name, role, password)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestLoginIssuesAVerifiableToken(t *testing.T) {
	a := newAuth(t)
	token, claims, err := a.Login("ana", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if got.Subject != "ana" || got.Role != RoleAnalyst || got.ID != claims.ID || got.ID == "" ||
		got.ExpiresAt.Sub(got.IssuedAt.Time) != time.Hour || got.Issuer != "tradewatch" {
		t.Errorf("claims = %+v", got)
	}
}

func TestLoginRejectsWrongPasswordsAndUnknownUsersAlike(t *testing.T) {
	a := newAuth(t)
	for _, c := range [][2]string{{"ana", "wrong password!"}, {"nobody", "correct horse battery"}, {"ana", ""}} {
		if _, _, err := a.Login(c[0], c[1]); !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("Login(%q) = %v", c[0], err)
		}
	}
}

func TestRepeatedFailuresLockTheUsername(t *testing.T) {
	a := newAuth(t)
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	for range 5 {
		if _, _, err := a.Login("ana", "guess"); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatal(err)
		}
	}
	if _, _, err := a.Login("ana", "correct horse battery"); !errors.Is(err, ErrTooManyAttempts) {
		t.Fatalf("sixth attempt with the right password: %v", err)
	}
	if _, _, err := a.Login("root", "staple battery horse"); err != nil {
		t.Fatalf("another user is not affected: %v", err)
	}
	now = now.Add(15 * time.Minute)
	if _, _, err := a.Login("ana", "correct horse battery"); err != nil {
		t.Fatalf("after the window: %v", err)
	}
}

func TestVerifyRejectsForgedAndStaleTokens(t *testing.T) {
	a := newAuth(t)
	good, _, err := a.Login("ana", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claims := func(mod func(*Claims)) *Claims {
		c := &Claims{RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "tradewatch", Subject: "ana", Audience: jwt.ClaimStrings{"tradewatch-api"},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		}, Role: RoleAnalyst}
		mod(c)
		return c
	}
	sign := func(m jwt.SigningMethod, key any, c *Claims) string {
		s, err := jwt.NewWithClaims(m, c).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	parts := strings.Split(good, ".")
	elevated := sign(jwt.SigningMethodHS256, []byte("x"), claims(func(c *Claims) { c.Role = RoleAdmin }))
	cases := map[string]string{
		"tampered payload":    parts[0] + "." + strings.Split(elevated, ".")[1] + "." + parts[2],
		"other secret":        sign(jwt.SigningMethodHS256, []byte("another secret that is long enough!!"), claims(func(*Claims) {})),
		"alg none":            sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims(func(*Claims) {})),
		"other HMAC":          sign(jwt.SigningMethodHS512, secret, claims(func(*Claims) {})),
		"expired":             sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Minute)) })),
		"no expiry":           sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.ExpiresAt = nil })),
		"wrong audience":      sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.Audience = jwt.ClaimStrings{"elsewhere"} })),
		"wrong issuer":        sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.Issuer = "someone" })),
		"issued in future":    sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.IssuedAt = jwt.NewNumericDate(now.Add(time.Hour)) })),
		"unknown user":        sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.Subject = "mallory" })),
		"role not configured": sign(jwt.SigningMethodHS256, secret, claims(func(c *Claims) { c.Role = RoleAdmin })),
		"garbage":             "not.a.token",
	}
	for name, token := range cases {
		if _, err := a.Verify(token); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: Verify = %v, want ErrInvalidToken", name, err)
		}
	}
}

func TestRemovingAUserRevokesTheirTokens(t *testing.T) {
	ana := mustUser(t, "ana", RoleAnalyst, "correct horse battery")
	token, _, err := newAuth(t, ana).Login("ana", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	// The same secret, but ana is no longer configured.
	others := newAuth(t, mustUser(t, "root", RoleAdmin, "staple battery horse"))
	if _, err := others.Verify(token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("token of a removed user: %v", err)
	}
}

func TestConfigurationIsValidated(t *testing.T) {
	u := mustUser(t, "ana", RoleAnalyst, "correct horse battery")
	if _, err := New(Config{Secret: []byte("short"), TTL: time.Hour, Issuer: "i", Audience: "a", Users: []User{u}}); err == nil {
		t.Error("a short secret was accepted")
	}
	if _, err := New(Config{Secret: secret, TTL: time.Hour, Issuer: "i", Audience: "a", Users: []User{u, u}}); err == nil {
		t.Error("a duplicate user was accepted")
	}
	if _, err := NewUser("ana", RoleAnalyst, "too short"); err == nil {
		t.Error("a short password was accepted")
	}
	if _, err := NewUser("ana", Role("superuser"), "long enough password"); err == nil {
		t.Error("an unknown role was accepted")
	}
	if !RoleAdmin.Allows(RoleAnalyst) || RoleAnalyst.Allows(RoleAdmin) || !RoleAnalyst.Allows(RoleAnalyst) {
		t.Error("role hierarchy")
	}
}
