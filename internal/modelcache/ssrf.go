package modelcache

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const MaxDownloadRedirects = 5

// DefaultAllowedHosts defines the HTTPS:443 source allowlist for open-source model weights.
var DefaultAllowedHosts = []string{
	"huggingface.co",
	"cdn-lfs.huggingface.co",
	"cdn-lfs-us-1.huggingface.co",
	"cdn-lfs-eu-1.huggingface.co",
	"cas-bridge.xethub.hf.co",
	"civitai.com",
	"github.com",
	"objects.githubusercontent.com",
}

var blockedPrefixes = mustParsePrefixes(
	// IPv4 non-global / private / loopback / CGNAT / link-local / metadata / reserved
	"0.0.0.0/8",
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.88.99.0/24",
	"192.168.0.0/16",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"224.0.0.0/4",
	"240.0.0.0/4",
	// IPv6 loopback / unspecified / v4-compatible / NAT64 / Teredo / 6to4 / ULA / link-local / multicast
	"::/128",
	"::1/128",
	"::/96",
	"64:ff9b::/96",
	"64:ff9b:1::/48",
	"100::/64",
	"2001::/32",
	"2001:2::/48",
	"2001:db8::/32",
	"2002::/16",
	"fc00::/7",
	"fe80::/10",
	"fec0::/10",
	"ff00::/8",
)

func mustParsePrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		out = append(out, netip.MustParsePrefix(c))
	}
	return out
}

// ClassifyIP enforces public unicast routing and unwraps IPv4-mapped IPv6 addresses
// (e.g. ::ffff:127.0.0.1, ::ffff:169.254.169.254) before prefix evaluation.
func ClassifyIP(addr netip.Addr) (bool, string) {
	if !addr.IsValid() {
		return false, "invalid_ip"
	}
	addr = addr.WithZone("")
	unmapped := addr.Unmap()
	if !unmapped.IsValid() {
		return false, "invalid_unmapped_ip"
	}
	if unmapped.IsUnspecified() {
		return false, "unspecified_ip"
	}
	if unmapped.IsLoopback() {
		return false, "loopback_ip"
	}
	if unmapped.IsPrivate() {
		return false, "private_ip"
	}
	if unmapped.IsLinkLocalUnicast() || unmapped.IsLinkLocalMulticast() || unmapped.IsInterfaceLocalMulticast() {
		return false, "link_local_ip"
	}
	if unmapped.IsMulticast() {
		return false, "multicast_ip"
	}
	for _, pfx := range blockedPrefixes {
		if pfx.Contains(unmapped) || (addr.Is6() && pfx.Contains(addr)) {
			return false, "blocked_prefix_" + pfx.String()
		}
	}
	if !unmapped.IsGlobalUnicast() {
		return false, "non_global_unicast"
	}
	return true, ""
}

// IPResolver resolves all A and AAAA records for a hostname.
type IPResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type VerifiedTarget struct {
	URL      *url.URL
	Hostname string
	Addrs    []netip.Addr
}

type ProgressFunc func(bytesDownloaded, partBytesOnDisk, speedBps int64, etaSeconds *int, etag string) error

type DownloadOptions struct {
	SourceURL    string
	RootDir      string
	RelPartPath  string
	PartPath     string
	ExpectedSize int64
	ResumeETag   string
	BearerToken  string
	OnProgress   ProgressFunc
}

type DownloadResult struct {
	BytesWritten int64
	ETag         string
	FinalHost    string
	PartIdentity *FileIdentity
}

// SafeDownloader enforces two-stage SSRF validation, manual redirect re-verification,
// cross-origin credential stripping, verified-IP pinning (including over HTTP CONNECT
// egress proxies so proxy-side DNS rebinding is impossible), and strict byte bounds.
type SafeDownloader struct {
	AllowedHosts   map[string]bool
	Resolver       IPResolver
	EgressProxyURL *url.URL
	TLSConfig      *tls.Config
	ConnectTimeout time.Duration
	// TransportForTarget is an optional test seam; in production nil builds a
	// verified-IP-pinned *http.Transport per hop.
	TransportForTarget func(target VerifiedTarget) (http.RoundTripper, func(), error)
}

func NewSafeDownloader(extraHosts []string, egressProxyRaw string) (*SafeDownloader, error) {
	allowed := make(map[string]bool, len(DefaultAllowedHosts)+len(extraHosts))
	for _, h := range DefaultAllowedHosts {
		allowed[strings.ToLower(strings.TrimSpace(h))] = true
	}
	for _, h := range extraHosts {
		clean := strings.ToLower(strings.TrimSpace(h))
		if clean == "" {
			continue
		}
		if _, err := netip.ParseAddr(clean); err == nil {
			return nil, errors.New("raw IP literal is forbidden in model host allowlist")
		}
		allowed[clean] = true
	}
	var proxyURL *url.URL
	if strings.TrimSpace(egressProxyRaw) != "" {
		u, err := url.Parse(strings.TrimSpace(egressProxyRaw))
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return nil, errors.New("invalid STATION_MODEL_EGRESS_PROXY_URL")
		}
		proxyURL = u
	}
	return &SafeDownloader{
		AllowedHosts:   allowed,
		Resolver:       net.DefaultResolver,
		EgressProxyURL: proxyURL,
		ConnectTimeout: 15 * time.Second,
	}, nil
}

func (d *SafeDownloader) isAllowedHost(hostname string) bool {
	if d == nil || len(d.AllowedHosts) == 0 {
		return false
	}
	return d.AllowedHosts[strings.ToLower(hostname)]
}

// ValidateSourceURL performs synchronous Stage-1 SSRF verification:
// HTTPS:443 only, no userinfo/fragment, no raw IP literal, exact allowlist host,
// and full DNS A/AAAA resolution where EVERY returned IP passes ClassifyIP.
func (d *SafeDownloader) ValidateSourceURL(ctx context.Context, rawURL string) (VerifiedTarget, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" || len(rawURL) > 2048 || strings.ContainsAny(rawURL, " \t\r\n") {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "invalid or disallowed model source URL")
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "model source URL must be clean HTTPS:443 without credentials or fragments")
	}
	if !strings.EqualFold(u.Scheme, "https") {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "only https:// scheme is permitted for model downloads")
	}
	port := u.Port()
	if port != "" && port != "443" {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "only port 443 is permitted for model downloads")
	}
	hostname := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if hostname == "" || strings.Contains(hostname, "..") {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "empty or malformed model source hostname")
	}
	if _, ipErr := netip.ParseAddr(hostname); ipErr == nil {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "raw IP literal URLs are forbidden for model downloads")
	}
	if !d.isAllowedHost(hostname) {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", fmt.Sprintf("hostname %q is not in controlled model source allowlist", hostname))
	}

	resolver := d.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ipAddrs, err := resolver.LookupIPAddr(rCtx, hostname)
	if err != nil || len(ipAddrs) == 0 {
		return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", fmt.Sprintf("DNS resolution failed or empty for %q", hostname))
	}

	verified := make([]netip.Addr, 0, len(ipAddrs))
	for _, rec := range ipAddrs {
		addr, ok := netip.AddrFromSlice(rec.IP)
		if !ok {
			return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "DNS returned malformed IP address")
		}
		addr = addr.Unmap()
		if allowed, reason := ClassifyIP(addr); !allowed {
			return VerifiedTarget{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", fmt.Sprintf("resolved IP blocked by SSRF policy (%s)", reason))
		}
		verified = append(verified, addr)
	}
	return VerifiedTarget{
		URL:      u,
		Hostname: hostname,
		Addrs:    verified,
	}, nil
}

type prebufferedConn struct {
	net.Conn
	leftover []byte
}

func (c *prebufferedConn) Read(p []byte) (int, error) {
	if len(c.leftover) > 0 {
		n := copy(p, c.leftover)
		c.leftover = c.leftover[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

func proxyHostPort(proxyURL *url.URL) string {
	host := proxyURL.Hostname()
	port := proxyURL.Port()
	if port == "" {
		if strings.EqualFold(proxyURL.Scheme, "https") {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(host, port)
}

// dialConnectTunnelPinned dials the controlled egress proxy and sends
// CONNECT <verified_ip>:443 HTTP/1.1 using ONLY a pre-verified IP literal from
// ValidateSourceURL (never a hostname), preventing any second-stage DNS resolution
// or DNS-rebinding attack inside the proxy (T32-H01). TLS SNI & X.509 hostname
// verification remain bound to target.Hostname end-to-end.
func dialConnectTunnelPinned(
	ctx context.Context,
	dialer *net.Dialer,
	proxyURL *url.URL,
	pinnedAddrs []netip.Addr,
	originTLS *tls.Config,
	timeout time.Duration,
) (net.Conn, error) {
	if len(pinnedAddrs) == 0 {
		return nil, errors.New("no verified IP addresses available for proxy CONNECT")
	}
	proxyAddr := proxyHostPort(proxyURL)
	var lastErr error
	for _, ip := range pinnedAddrs {
		targetAddrPort := net.JoinHostPort(ip.String(), "443")
		rawConn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			lastErr = err
			continue
		}
		var proxyConn net.Conn = rawConn
		if strings.EqualFold(proxyURL.Scheme, "https") {
			pTLS := &tls.Config{
				MinVersion: tls.VersionTLS12,
				ServerName: proxyURL.Hostname(),
			}
			tConn := tls.Client(rawConn, pTLS)
			if hErr := tConn.HandshakeContext(ctx); hErr != nil {
				_ = rawConn.Close()
				lastErr = hErr
				continue
			}
			proxyConn = tConn
		}

		_ = proxyConn.SetDeadline(time.Now().Add(timeout))
		connectWire := fmt.Sprintf(
			"CONNECT %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: VerdantFlare-Station-ModelProvisioner/1.0\r\nProxy-Connection: Keep-Alive\r\n\r\n",
			targetAddrPort, targetAddrPort,
		)
		if _, wErr := io.WriteString(proxyConn, connectWire); wErr != nil {
			_ = proxyConn.Close()
			lastErr = wErr
			continue
		}

		limReader := &io.LimitedReader{R: proxyConn, N: 16384}
		br := bufio.NewReaderSize(limReader, 4096)
		resp, rErr := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
		if rErr != nil {
			_ = proxyConn.Close()
			lastErr = rErr
			continue
		}
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		if resp.StatusCode != http.StatusOK {
			_ = proxyConn.Close()
			lastErr = fmt.Errorf("egress proxy CONNECT %s rejected with HTTP %d", targetAddrPort, resp.StatusCode)
			continue
		}

		var tunnelConn net.Conn = proxyConn
		if buffered := br.Buffered(); buffered > 0 {
			extra := make([]byte, buffered)
			if _, bErr := io.ReadFull(br, extra); bErr != nil {
				_ = proxyConn.Close()
				lastErr = bErr
				continue
			}
			tunnelConn = &prebufferedConn{Conn: proxyConn, leftover: extra}
		}
		_ = proxyConn.SetDeadline(time.Time{})

		tlsConn := tls.Client(tunnelConn, originTLS.Clone())
		if hErr := tlsConn.HandshakeContext(ctx); hErr != nil {
			_ = proxyConn.Close()
			lastErr = hErr
			continue
		}
		return tlsConn, nil
	}
	if lastErr == nil {
		lastErr = errors.New("proxy CONNECT failed for all verified IP addresses")
	}
	return nil, lastErr
}

func (d *SafeDownloader) buildHopTransport(target VerifiedTarget) (http.RoundTripper, func(), error) {
	if d.TransportForTarget != nil {
		return d.TransportForTarget(target)
	}
	timeout := d.ConnectTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: target.Hostname,
	}
	if d.TLSConfig != nil {
		cloned := d.TLSConfig.Clone()
		if cloned.MinVersion < tls.VersionTLS12 {
			cloned.MinVersion = tls.VersionTLS12
		}
		cloned.ServerName = target.Hostname
		tlsCfg = cloned
	}

	pinnedAddrs := append([]netip.Addr(nil), target.Addrs...)
	tr := &http.Transport{
		Proxy:                 nil,
		TLSClientConfig:       tlsCfg,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}
	if d.EgressProxyURL != nil {
		proxyCopy := *d.EgressProxyURL
		http1TLS := tlsCfg.Clone()
		http1TLS.NextProtos = []string{"http/1.1"}
		tr.ForceAttemptHTTP2 = false
		tr.TLSClientConfig = http1TLS
		tr.DialTLSContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialConnectTunnelPinned(ctx, dialer, &proxyCopy, pinnedAddrs, http1TLS, timeout)
		}
		tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("plaintext HTTP dial is forbidden for model downloads")
		}
	} else {
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			var lastErr error
			for _, ip := range pinnedAddrs {
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), "443"))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = errors.New("no verified IP addresses available")
			}
			return nil, lastErr
		}
	}
	return tr, tr.CloseIdleConnections, nil
}

func sameOriginHostScheme(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	portA := a.Port()
	if portA == "" {
		portA = "443"
	}
	portB := b.Port()
	if portB == "" {
		portB = "443"
	}
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(strings.TrimSuffix(a.Hostname(), "."), strings.TrimSuffix(b.Hostname(), ".")) &&
		portA == portB
}

func parseContentRangeStart(header string) (int64, int64, bool) {
	// Format: "bytes <start>-<end>/<total>"
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(strings.ToLower(header), "bytes ") {
		return 0, 0, false
	}
	rest := strings.TrimSpace(header[6:])
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return 0, 0, false
	}
	rangePart, totalPart := rest[:slash], rest[slash+1:]
	dash := strings.IndexByte(rangePart, '-')
	if dash <= 0 {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(rangePart[:dash], 10, 64)
	end, err2 := strconv.ParseInt(rangePart[dash+1:], 10, 64)
	total, err3 := strconv.ParseInt(totalPart, 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || start < 0 || end < start || total <= end {
		return 0, 0, false
	}
	return start, total, true
}

func resolveDownloadPartTarget(opts DownloadOptions) (string, string, error) {
	if strings.TrimSpace(opts.RootDir) != "" && strings.TrimSpace(opts.RelPartPath) != "" {
		cleanRel, err := cleanRelUnderRoot(opts.RelPartPath)
		if err != nil {
			return "", "", err
		}
		return opts.RootDir, cleanRel, nil
	}
	if strings.TrimSpace(opts.PartPath) == "" {
		return "", "", NewError(400, "INVALID_PARAMETERS", "missing staging .part destination")
	}
	stagingDir := filepath.Dir(opts.PartPath)
	rootDir := filepath.Dir(stagingDir)
	relPart, err := cleanRelUnderRoot(filepath.ToSlash(filepath.Join(filepath.Base(stagingDir), filepath.Base(opts.PartPath))))
	if err != nil {
		return "", "", err
	}
	return rootDir, relPart, nil
}

// DownloadToPart streams a verified HTTPS source to .staging/<sha256>.<fence>.part
// only AFTER every redirect hop passes full SSRF validation, opening the .part file
// via symlink-free openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV)
// with O_CREAT|O_EXCL|O_NOFOLLOW and nlink == 1 verification (T32-H02).
func (d *SafeDownloader) DownloadToPart(ctx context.Context, opts DownloadOptions) (DownloadResult, error) {
	if opts.ExpectedSize <= 0 || opts.ExpectedSize > MaxCatalogModelBytes {
		return DownloadResult{}, NewError(400, "INVALID_PARAMETERS", "invalid expected model size_bytes")
	}
	rootDir, relPart, locErr := resolveDownloadPartTarget(opts)
	if locErr != nil {
		return DownloadResult{}, locErr
	}
	existingOffset := InspectResumeCandidate(rootDir, relPart, opts.ExpectedSize, opts.ResumeETag)

	currentURL := opts.SourceURL
	includeAuth := opts.BearerToken != ""
	var originURL *url.URL

	for hop := 0; hop <= MaxDownloadRedirects; hop++ {
		target, err := d.ValidateSourceURL(ctx, currentURL)
		if err != nil {
			return DownloadResult{}, err
		}
		if hop == 0 {
			originURL = target.URL
		} else if !sameOriginHostScheme(originURL, target.URL) {
			includeAuth = false
		}

		rt, cleanup, err := d.buildHopTransport(target)
		if err != nil {
			return DownloadResult{}, NewError(502, "DOWNLOAD_TRANSPORT_ERROR", "failed to initialize transport")
		}

		client := &http.Client{
			Transport: rt,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL.String(), nil)
		if err != nil {
			cleanup()
			return DownloadResult{}, NewError(400, "INVALID_PARAMETERS", "invalid source request URL")
		}
		req.Header.Set("User-Agent", "VerdantFlare-Station-ModelProvisioner/1.0")
		req.Header.Set("Accept-Encoding", "identity")
		// Explicitly guarantee zero ambient credentials on cross-origin hops.
		req.Header.Del("Authorization")
		req.Header.Del("Cookie")
		req.Header.Del("Proxy-Authorization")
		if includeAuth && opts.BearerToken != "" {
			req.Header.Set("Authorization", "Bearer "+opts.BearerToken)
		}
		if existingOffset > 0 && opts.ResumeETag != "" {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existingOffset))
			req.Header.Set("If-Range", opts.ResumeETag)
		}

		resp, err := client.Do(req)
		if err != nil {
			cleanup()
			return DownloadResult{}, NewError(502, "MODEL_DOWNLOAD_INTERRUPTED", "upstream model connection failed")
		}

		if resp.StatusCode == http.StatusMovedPermanently ||
			resp.StatusCode == http.StatusFound ||
			resp.StatusCode == http.StatusSeeOther ||
			resp.StatusCode == http.StatusTemporaryRedirect ||
			resp.StatusCode == http.StatusPermanentRedirect {
			loc := strings.TrimSpace(resp.Header.Get("Location"))
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			cleanup()
			if loc == "" {
				return DownloadResult{}, NewError(502, "MODEL_DOWNLOAD_FAILED", "upstream redirect missing Location header")
			}
			if hop == MaxDownloadRedirects {
				return DownloadResult{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "redirect chain exceeded maximum hop limit")
			}
			nextURL, parseErr := target.URL.Parse(loc)
			if parseErr != nil {
				return DownloadResult{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "malformed redirect Location URL")
			}
			if !sameOriginHostScheme(target.URL, nextURL) {
				includeAuth = false
			}
			currentURL = nextURL.String()
			continue
		}

		defer func() {
			_ = resp.Body.Close()
			cleanup()
		}()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			return DownloadResult{}, NewError(502, "MODEL_DOWNLOAD_FAILED", fmt.Sprintf("upstream returned HTTP %d", resp.StatusCode))
		}

		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		var startOffset int64
		if resp.StatusCode == http.StatusPartialContent && existingOffset > 0 {
			rangeStart, rangeTotal, ok := parseContentRangeStart(resp.Header.Get("Content-Range"))
			if !ok || rangeStart != existingOffset || rangeTotal != opts.ExpectedSize {
				return DownloadResult{}, NewError(502, "MODEL_DOWNLOAD_FAILED", "invalid upstream Content-Range on resume")
			}
			startOffset = existingOffset
			if etag == "" {
				etag = opts.ResumeETag
			}
		} else if resp.StatusCode == http.StatusPartialContent {
			return DownloadResult{}, NewError(502, "MODEL_DOWNLOAD_FAILED", "unexpected HTTP 206 Partial Content without Range request")
		} else {
			startOffset = 0
			if resp.ContentLength > opts.ExpectedSize {
				return DownloadResult{}, NewError(422, "MODEL_SHA256_MISMATCH", "upstream Content-Length exceeds declared catalog size_bytes")
			}
		}

		f, actualStart, openedID, openErr := OpenStagingPartForWriteWithIdentity(rootDir, relPart, startOffset)
		if openErr != nil {
			if mErr, ok := AsError(openErr); ok {
				return DownloadResult{}, mErr
			}
			return DownloadResult{}, NewError(500, "STORAGE_IO_ERROR", "failed to open staging .part file safely")
		}
		idCopy := openedID
		partDir := filepath.ToSlash(filepath.Dir(filepath.FromSlash(relPart)))
		partBase := filepath.Base(filepath.FromSlash(relPart))
		cleanupOpenedPart := func() error {
			_, swapped, qErr := quarantineUnlinkWithIdentity(rootDir, partDir, partBase, &idCopy, "", "")
			if qErr != nil {
				return qErr
			}
			if swapped {
				return NewError(500, "STORAGE_IO_ERROR", "concurrent staging .part replacement detected during download failure cleanup; preserved intact")
			}
			return nil
		}

		if startOffset > 0 && actualStart != startOffset {
			// Resume candidate vanished or mutated during network round-trip while server sent 206 body slice.
			_ = f.Close()
			if cErr := cleanupOpenedPart(); cErr != nil {
				return DownloadResult{PartIdentity: &idCopy}, cErr
			}
			return DownloadResult{PartIdentity: &idCopy}, NewError(502, "MODEL_DOWNLOAD_INTERRUPTED", "resume candidate changed before HTTP 206 stream append; retry cleanly")
		}

		written := actualStart
		startedAt := time.Now()
		var lastReport time.Time
		buf := make([]byte, 256*1024)

		for {
			n, rErr := resp.Body.Read(buf)
			if n > 0 {
				if written+int64(n) > opts.ExpectedSize {
					_ = f.Close()
					if cErr := cleanupOpenedPart(); cErr != nil {
						return DownloadResult{PartIdentity: &idCopy}, cErr
					}
					return DownloadResult{PartIdentity: &idCopy}, NewError(422, "MODEL_SHA256_MISMATCH", "upstream stream exceeded declared catalog size_bytes")
				}
				// Verify fence ownership via OnProgress BEFORE and AFTER chunk flush so superseded writers abort immediately (R2-H01).
				if lastReport.IsZero() && opts.OnProgress != nil {
					if pErr := opts.OnProgress(written, written, 0, nil, etag); pErr != nil {
						_ = f.Close()
						return DownloadResult{PartIdentity: &idCopy}, pErr
					}
				}
				if _, wErr := f.Write(buf[:n]); wErr != nil {
					_ = f.Close()
					return DownloadResult{PartIdentity: &idCopy}, NewError(500, "STORAGE_IO_ERROR", "failed writing to staging .part file")
				}
				written += int64(n)

				now := time.Now()
				if opts.OnProgress != nil && (lastReport.IsZero() || now.Sub(lastReport) >= 200*time.Millisecond || written == opts.ExpectedSize) {
					elapsed := now.Sub(startedAt).Seconds()
					var speed int64
					if elapsed > 0 {
						speed = int64(float64(written-actualStart) / elapsed)
					}
					if speed <= 0 && written > actualStart {
						speed = written - actualStart
					}
					var eta *int
					if speed > 0 && opts.ExpectedSize >= written {
						rem := int((opts.ExpectedSize - written) / speed)
						eta = &rem
					}
					if pErr := opts.OnProgress(written, written, speed, eta, etag); pErr != nil {
						_ = f.Close()
						return DownloadResult{PartIdentity: &idCopy}, pErr
					}
					lastReport = now
				}
			}
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				_ = f.Sync()
				_ = f.Close()
				return DownloadResult{BytesWritten: written, ETag: etag, FinalHost: target.Hostname, PartIdentity: &idCopy}, NewError(502, "MODEL_DOWNLOAD_INTERRUPTED", "network stream interrupted during download")
			}
		}

		if syncErr := f.Sync(); syncErr != nil {
			_ = f.Close()
			return DownloadResult{PartIdentity: &idCopy}, NewError(500, "STORAGE_IO_ERROR", "failed syncing staging .part file")
		}
		if closeErr := f.Close(); closeErr != nil {
			return DownloadResult{PartIdentity: &idCopy}, NewError(500, "STORAGE_IO_ERROR", "failed closing staging .part file")
		}
		if written != opts.ExpectedSize {
			if cErr := cleanupOpenedPart(); cErr != nil {
				return DownloadResult{PartIdentity: &idCopy}, cErr
			}
			return DownloadResult{PartIdentity: &idCopy}, NewError(422, "MODEL_SHA256_MISMATCH", fmt.Sprintf("downloaded size %d does not match expected %d", written, opts.ExpectedSize))
		}
		return DownloadResult{
			BytesWritten: written,
			ETag:         etag,
			FinalHost:    target.Hostname,
			PartIdentity: &idCopy,
		}, nil
	}
	return DownloadResult{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "redirect limit exceeded")
}
