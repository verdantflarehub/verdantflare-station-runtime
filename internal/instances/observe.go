package instances

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"
)

type HTTPObserver struct{ Client *http.Client }

func InternalHTTPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect denied") }}
}

func (o HTTPObserver) Observe(ctx context.Context, in CreateRequest, helper HelperObservation, token string) (*PreparationProof, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, helper.FileEndpoint+"/internal/workspace/status", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := o.Client
	if client == nil {
		client = InternalHTTPClient()
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Content-Encoding") != "" {
		return nil, errors.New("WORKSPACE_STATUS_UNAVAILABLE")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (32<<10)+1))
	if err != nil || len(raw) > 32<<10 {
		return nil, ErrBinding
	}
	var result struct {
		InstanceID  string            `json:"instance_id"`
		PodUID      string            `json:"pod_uid"`
		Preparation *PreparationProof `json:"preparation"`
	}
	if json.Unmarshal(raw, &result) != nil || result.InstanceID != in.InstanceID || result.PodUID != helper.PodUID {
		return nil, ErrBinding
	}
	p := result.Preparation
	if p == nil {
		return nil, nil
	}
	if p.PrepareID != in.OperationID || p.InstanceID != in.InstanceID || p.ProjectID != in.ProjectID || p.SourceRevisionID != in.SourceRevisionID || p.Size != in.SourceSize {
		return nil, ErrBinding
	}
	if in.EmptySource {
		if p.AssetID != nil || p.SHA256 != nil {
			return nil, ErrBinding
		}
	} else if p.SHA256 == nil || *p.SHA256 != in.SourceSHA256 || p.AssetID == nil || !regexp.MustCompile(`^inbox/[A-Za-z0-9_-]{16}/restore\.blend$`).MatchString(*p.AssetID) {
		return nil, ErrBinding
	}
	if p.State == "preparing" {
		return nil, nil
	}
	at, err := time.Parse(time.RFC3339Nano, p.PreparedAt)
	if p.State != "prepared" || err != nil || at.After(time.Now().Add(5*time.Second)) {
		return nil, ErrBinding
	}
	return p, nil
}
