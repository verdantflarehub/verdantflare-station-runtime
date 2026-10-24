package instances

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"reflect"
	"strconv"
)

// ObserveWorkingCopy reads a candidate from a UID-fenced, quiescent file helper.
// The caller must establish exclusive workspace ownership before calling it.
// This does not authorize replacing a start request or claim Blender loaded it.
// In particular, the preparation source is checked independently of the bytes
// subsequently saved by the editor. Neither the source nor journal is written.
func (o HTTPObserver) ObserveWorkingCopy(ctx context.Context, created CreateRequest, helper HelperObservation, token string, maxBytes int64) (*WorkerSource, error) {
	if maxBytes < 12 || maxBytes == int64(^uint64(0)>>1) || !helper.Ready || helper.PodUID == "" || token == "" {
		return nil, ErrBinding
	}
	client := o.Client
	if client == nil {
		client = InternalHTTPClient()
	}
	local := *client
	local.CheckRedirect = InternalHTTPClient().CheckRedirect
	o.Client = &local
	prepared, err := o.Observe(ctx, created, helper, token)
	if err != nil || prepared == nil {
		if err != nil {
			return nil, err
		}
		return nil, ErrBinding
	}
	first, err := o.readWorkingCopy(ctx, created.SourceRevisionID, helper, token, maxBytes)
	if err != nil {
		return nil, err
	}
	second, err := o.readWorkingCopy(ctx, created.SourceRevisionID, helper, token, maxBytes)
	if err != nil {
		return nil, err
	}
	if first != second {
		return nil, ErrBinding
	}
	after, err := o.Observe(ctx, created, helper, token)
	if err != nil {
		return nil, err
	}
	if after == nil || !reflect.DeepEqual(after, prepared) {
		return nil, ErrBinding
	}
	return &first, nil
}

func (o HTTPObserver) readWorkingCopy(ctx context.Context, revision string, helper HelperObservation, token string, maxBytes int64) (WorkerSource, error) {
	result := WorkerSource{SourceRevisionID: revision}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, helper.FileEndpoint+"/assets/project/main.blend", nil)
	if err != nil {
		return result, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := o.Client
	if client == nil {
		client = InternalHTTPClient()
	}
	// A caller-supplied transport must not permit redirects to leak credentials.
	local := *client
	local.CheckRedirect = InternalHTTPClient().CheckRedirect
	response, err := local.Do(req)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Uncompressed || response.Header.Get("Content-Encoding") != "" || response.ContentLength < 12 || response.ContentLength > maxBytes {
		return result, ErrBinding
	}
	headerSize := response.ContentLength
	if response.Header.Get("Content-Length") != strconv.FormatInt(headerSize, 10) {
		return result, ErrBinding
	}
	expected := response.Header.Get("X-Asset-SHA256")
	decoded, err := hex.DecodeString(expected)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != expected {
		return result, ErrBinding
	}
	sum := sha256.New()
	count, err := io.Copy(sum, io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return result, err
	}
	result.Size, result.SHA256 = count, hex.EncodeToString(sum.Sum(nil))
	if count != headerSize || result.SHA256 != expected || !result.valid(maxBytes) {
		return result, ErrBinding
	}
	return result, nil
}
