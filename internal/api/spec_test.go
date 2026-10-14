package api

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func loadSpec(t testing.TB) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromData(Spec())
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("openapi.yaml is not a valid OpenAPI document: %v", err)
	}
	return doc
}

// Every route the server registers is documented, and every documented
// operation is served, with the documented security.
func TestSpecMatchesTheRoutes(t *testing.T) {
	doc := loadSpec(t)
	var documented []string
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			pattern := method + " " + path
			documented = append(documented, pattern)
			public := op.Security != nil && len(*op.Security) == 0
			if public != slices.Contains([]string{"POST /api/v1/auth/login", "GET /api/v1/openapi.yaml"}, pattern) {
				t.Errorf("%s: documented as public=%v", pattern, public)
			}
		}
	}
	var routed []string
	for _, rt := range (&Server{}).routes() {
		routed = append(routed, rt.pattern)
		if strings.HasPrefix(rt.pattern, "GET /api/v1/sim") || strings.Contains(rt.pattern, " /api/v1/sim/") {
			if rt.role != "admin" {
				t.Errorf("%s is not restricted to admins", rt.pattern)
			}
		}
	}
	slices.Sort(documented)
	slices.Sort(routed)
	if !slices.Equal(documented, routed) {
		t.Fatalf("documented operations:\n%s\nrouted:\n%s", strings.Join(documented, "\n"), strings.Join(routed, "\n"))
	}
}
