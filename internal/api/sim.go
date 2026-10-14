package api

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// newSimProxy forwards admin requests to the simulator's control endpoints.
// The simulator listens only on the internal network and has no
// authentication of its own; this proxy is the authenticated way in. The
// caller's token is not passed on.
func newSimProxy(target *url.URL, log *slog.Logger) http.Handler {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Header.Del("Authorization")
			pr.Out.Header.Del("Cookie")
			if c := claimsOf(pr.In); c != nil {
				log.Info("simulator control", "method", pr.In.Method, "path", pr.In.URL.Path, "by", c.Subject)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Warn("the simulator is unreachable", "path", r.URL.Path, "err", err)
			problem(w, http.StatusBadGateway, "the simulator could not be reached")
		},
	}
}
