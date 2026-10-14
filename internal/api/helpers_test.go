package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/ZofSpades/tradewatch/internal/auth"
)

var quiet = slog.New(slog.DiscardHandler)

const (
	analystPassword = "analyst password 123"
	adminPassword   = "admin password 456"
)

func mustAuth(t *testing.T) *auth.Authenticator {
	t.Helper()
	users, err := Config{AnalystPassword: analystPassword, AdminPassword: adminPassword}.Users()
	if err != nil {
		t.Fatal(err)
	}
	a, err := auth.New(auth.Config{Secret: []byte(strings.Repeat("s", 32)), TTL: time.Hour, Issuer: "tradewatch",
		Audience: "tradewatch-api", Users: users})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// specRouter matches requests against the spec, served at base.
func specRouter(t testing.TB, base string) routers.Router {
	t.Helper()
	doc := loadSpec(t)
	doc.Servers = openapi3.Servers{{URL: base}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return router
}

// checkResponse requires the response to a request to match the spec: the
// status code must be documented for the operation and the body must match
// its schema. A request outside the spec must have been answered 404.
func checkResponse(t testing.TB, router routers.Router, req *http.Request, status int, header http.Header, body []byte) {
	t.Helper()
	route, params, err := router.FindRoute(req)
	if err != nil {
		if status != http.StatusNotFound {
			t.Errorf("%s %s: not in the spec but answered %d", req.Method, req.URL.Path, status)
		}
		return
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: req, PathParams: params, Route: route,
			Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
		},
		Status: status, Header: header, Body: io.NopCloser(bytes.NewReader(body)),
		Options: &openapi3filter.Options{IncludeResponseStatus: true},
	}
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Errorf("%s %s -> %d does not match the spec: %v\nbody: %s", req.Method, req.URL.Path, status, err, body)
	}
}
