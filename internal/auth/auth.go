// Package auth issues and verifies the API's access tokens: HS256 JWTs that
// carry the user's role, signed with a secret taken from the environment.
//
// Users and their passwords are configuration, not code: the API reads the
// passwords from environment variables at start-up and keeps only bcrypt
// hashes in memory. Tokens are short-lived and checked strictly: the
// signing algorithm is pinned, issuer, audience and expiry are required,
// and the user and role in a token must still match the configuration, so
// removing a user or changing a role takes effect immediately.
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// Role is what a user may do.
type Role string

// Roles. An admin may do everything an analyst may, and also control the
// simulator.
const (
	RoleAnalyst Role = "analyst"
	RoleAdmin   Role = "admin"
)

// Allows reports whether a user with role r may act with role need.
func (r Role) Allows(need Role) bool { return r == need || r == RoleAdmin }

// Errors returned by Login and Verify.
var (
	ErrInvalidCredentials = errors.New("auth: invalid username or password")
	ErrTooManyAttempts    = errors.New("auth: too many failed logins; try again later")
	ErrInvalidToken       = errors.New("auth: invalid token")
)

// MinSecretBytes and MinPasswordBytes are the shortest accepted signing
// secret and password.
const (
	MinSecretBytes   = 32
	MinPasswordBytes = 12
)

// User is a configured account.
type User struct {
	Name string
	Role Role
	hash []byte
}

// NewUser hashes a password for a configured account.
func NewUser(name string, role Role, password string) (User, error) {
	if role != RoleAnalyst && role != RoleAdmin {
		return User{}, fmt.Errorf("auth: unknown role %q", role)
	}
	if len(password) < MinPasswordBytes {
		return User{}, fmt.Errorf("auth: the password of %s must be at least %d bytes", name, MinPasswordBytes)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return User{}, err
	}
	return User{Name: name, Role: role, hash: hash}, nil
}

// Config configures an Authenticator.
type Config struct {
	Secret   []byte
	TTL      time.Duration
	Issuer   string
	Audience string
	Users    []User
	// MaxFailures failed logins within FailureWindow lock a username until
	// the oldest of them leaves the window.
	MaxFailures   int
	FailureWindow time.Duration
}

// Claims are the claims of an access token.
type Claims struct {
	jwt.RegisteredClaims
	Role Role `json:"role"`
}

// Authenticator logs users in and verifies their tokens.
type Authenticator struct {
	cfg   Config
	users map[string]User
	// dummy is compared against when the username is unknown, so a login
	// takes as long for unknown users as for wrong passwords.
	dummy []byte
	now   func() time.Time

	mu       sync.Mutex
	failures map[string][]time.Time
}

// New validates the configuration and returns an Authenticator.
func New(cfg Config) (*Authenticator, error) {
	if len(cfg.Secret) < MinSecretBytes {
		return nil, fmt.Errorf("auth: the signing secret must be at least %d bytes", MinSecretBytes)
	}
	if cfg.TTL <= 0 || cfg.Issuer == "" || cfg.Audience == "" || len(cfg.Users) == 0 {
		return nil, errors.New("auth: TTL, issuer, audience and at least one user are required")
	}
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 5
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = 15 * time.Minute
	}
	a := &Authenticator{cfg: cfg, users: map[string]User{}, now: time.Now, failures: map[string][]time.Time{}}
	for _, u := range cfg.Users {
		if _, dup := a.users[u.Name]; dup {
			return nil, fmt.Errorf("auth: user %s configured twice", u.Name)
		}
		a.users[u.Name] = u
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	dummy, err := bcrypt.GenerateFromPassword(random[:], bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	a.dummy = dummy
	return a, nil
}

// TTL is the lifetime of issued tokens.
func (a *Authenticator) TTL() time.Duration { return a.cfg.TTL }

// Login checks a username and password and returns a signed token.
func (a *Authenticator) Login(username, password string) (string, *Claims, error) {
	now := a.now()
	if a.locked(username, now) {
		return "", nil, ErrTooManyAttempts
	}
	u, known := a.users[username]
	hash := a.dummy
	if known {
		hash = u.hash
	}
	if bcrypt.CompareHashAndPassword(hash, []byte(password)) != nil || !known {
		a.fail(username, now)
		return "", nil, ErrInvalidCredentials
	}
	a.mu.Lock()
	delete(a.failures, username)
	a.mu.Unlock()
	return a.issue(u, now)
}

func (a *Authenticator) locked(username string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.prune(username, now)) >= a.cfg.MaxFailures
}

func (a *Authenticator) fail(username string, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failures[username] = append(a.prune(username, now), now)
}

// prune drops failures older than the window; the caller holds mu.
func (a *Authenticator) prune(username string, now time.Time) []time.Time {
	fs := a.failures[username]
	i := 0
	for i < len(fs) && now.Sub(fs[i]) >= a.cfg.FailureWindow {
		i++
	}
	fs = fs[i:]
	if len(fs) == 0 {
		delete(a.failures, username)
	} else {
		a.failures[username] = fs
	}
	return fs
}

func (a *Authenticator) issue(u User, now time.Time) (string, *Claims, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", nil, err
	}
	c := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: a.cfg.Issuer, Subject: u.Name, Audience: jwt.ClaimStrings{a.cfg.Audience},
			IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(a.cfg.TTL)), ID: hex.EncodeToString(id[:]),
		},
		Role: u.Role,
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(a.cfg.Secret)
	if err != nil {
		return "", nil, err
	}
	return token, c, nil
}

// Verify checks a token and returns its claims. Every failure is reported
// as ErrInvalidToken, wrapping the reason.
func (a *Authenticator) Verify(token string) (*Claims, error) {
	var c Claims
	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) { return a.cfg.Secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(a.cfg.Issuer), jwt.WithAudience(a.cfg.Audience),
		jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(30*time.Second), jwt.WithTimeFunc(a.now))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	u, ok := a.users[c.Subject]
	if !ok || u.Role != c.Role {
		return nil, fmt.Errorf("%w: user or role no longer configured", ErrInvalidToken)
	}
	return &c, nil
}
