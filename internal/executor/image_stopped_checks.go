package executor

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
)

func (s *stoppedHTTPChecks) checkImage(ctx context.Context, b ManagementBinding, r *StoppedReplacement, c ManagementCheckpoint, target, idle bool) error {
	release, t := b.Source, r.Source
	if target {
		release, t = b.Target, r.Target
	}
	// Only versions preserving unknown legacy submissions can be upgrade targets.
	if b.Target.AppVersion != "0.2.18" && b.Target.AppVersion != "0.2.20" {
		return ErrManagementReplacement
	}
	u, err := url.Parse(t.ActivityURL)
	if err != nil || u.Scheme != "http" || u.Host != t.Deployment.Name+"."+t.Deployment.Namespace+".svc.cluster.local:8000" || u.Path != "/api/tasks/stats" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.User != nil || u.Fragment != "" {
		return ErrManagementReplacement
	}
	token := os.Getenv(t.ActivityTokenEnv)
	if !validManagementToken(token) {
		return ErrManagementMaintenanceUnavailable
	}
	before, err := stoppedPod(ctx, r, c, target, release.AppImageDigest)
	if err != nil {
		return err
	}
	busy, err := imageStoppedEvidence(func(path string) ([]byte, error) {
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(before.Status.PodIP, "8000")+path, nil)
		if e != nil {
			return nil, ErrManagementReplacement
		}
		req.Header.Set("Authorization", "Bearer "+token)
		response, e := s.client.Do(req)
		if e != nil {
			return nil, ErrManagementMaintenanceUnavailable
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, ErrManagementReplacement
		}
		raw, e := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if e != nil || len(raw) > 1<<20 {
			return nil, ErrManagementReplacement
		}
		return raw, nil
	}, release)
	if err != nil {
		return err
	}
	after, err := stoppedPod(ctx, r, c, target, release.AppImageDigest)
	if err != nil {
		return err
	}
	if before.UID != after.UID || before.Status.PodIP != after.Status.PodIP || !reflect.DeepEqual(before.Status.ContainerStatuses, after.Status.ContainerStatuses) {
		return ErrManagementMaintenanceUnavailable
	}
	if busy && idle {
		return ErrManagementInstallationBusy
	}
	return nil
}
