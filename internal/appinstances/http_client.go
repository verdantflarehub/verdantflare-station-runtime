package appinstances

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var serviceName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.svc\.cluster\.local$`)

func internalOrigin(value string) bool {
	u, e := url.Parse(value)
	if e != nil || u.Scheme != "http" || u.User != nil || !serviceName.MatchString(u.Hostname()) ||
		u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	parts := strings.Split(u.Hostname(), ".")
	if !label(parts[0]) || !label(parts[1]) || (u.Port() == "" && u.Host != u.Hostname()) {
		return false
	}
	if u.Port() != "" {
		port, e := strconv.Atoi(u.Port())
		if e != nil || port < 1 || port > 65535 || strconv.Itoa(port) != u.Port() {
			return false
		}
	}
	return true
}

// No environment proxy, cookies, redirects or implicit compressed responses.
func InternalHTTPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second, CheckRedirect: noRedirect,
		Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
			DisableCompression: true, ResponseHeaderTimeout: 3 * time.Second, MaxResponseHeaderBytes: 16 << 10,
			IdleConnTimeout: 30 * time.Second, MaxIdleConns: 16}}
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func controlledRequest(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	copy := *client
	copy.CheckRedirect, copy.Jar = noRedirect, nil
	return copy.Do(req.WithContext(ctx))
}
