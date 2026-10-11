// Package strictjson decodes bounded, exact-shape internal protocol messages.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

var ErrInvalid = errors.New("invalid JSON shape")

// Decode requires a fresh destination with explicit json tags. The round-trip
// shape check enforces required fields and exact case, including nested values.
// Optional fields must use omitempty; explicit empty optional values are rejected.
func Decode(r io.Reader, limit int64, dst any) error {
	if limit <= 0 {
		return ErrInvalid
	}
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(b)) > limit || !utf8.Valid(b) {
		return ErrInvalid
	}
	scan := json.NewDecoder(bytes.NewReader(b))
	scan.UseNumber()
	if err := scanValue(scan, 0, false); err != nil {
		return ErrInvalid
	}
	if _, err := scan.Token(); err != io.EOF {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return ErrInvalid
	}
	canonical, err := json.Marshal(dst)
	if err != nil {
		return ErrInvalid
	}
	var original, normalized any
	if json.Unmarshal(b, &original) != nil || json.Unmarshal(canonical, &normalized) != nil || !shape(original, normalized) {
		return ErrInvalid
	}
	return nil
}

// ReadValue validates an opaque App JSON value without imposing platform field
// names. Unlike Decode's closed protocol objects, App values may contain null.
func ReadValue(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, ErrInvalid
	}
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(b)) > limit || !utf8.Valid(b) {
		return nil, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if scanValue(d, 0, true) != nil {
		return nil, ErrInvalid
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	return b, nil
}

func scanValue(d *json.Decoder, depth int, allowNull bool) error {
	if depth > 32 {
		return ErrInvalid
	}
	t, err := d.Token()
	if err != nil || (t == nil && !allowNull) {
		return ErrInvalid
	}
	switch t {
	case json.Delim('{'):
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return ErrInvalid
			}
			seen[name] = true
			if err := scanValue(d, depth+1, allowNull); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalid
		}
	case json.Delim('['):
		for d.More() {
			if err := scanValue(d, depth+1, allowNull); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalid
		}
	default:
		if number, ok := t.(json.Number); ok && allowNull {
			if _, err := strconv.ParseFloat(number.String(), 64); err != nil {
				return ErrInvalid
			}
		}
		if _, ok := t.(json.Delim); ok {
			return ErrInvalid
		}
	}
	return nil
}
func shape(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !shape(v, w) {
				return false
			}
		}
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !shape(x[i], y[i]) {
				return false
			}
		}
	case string:
		_, ok := b.(string)
		return ok
	case bool:
		_, ok := b.(bool)
		return ok
	case float64:
		_, ok := b.(float64)
		return ok
	default:
		return false
	}
	return true
}
