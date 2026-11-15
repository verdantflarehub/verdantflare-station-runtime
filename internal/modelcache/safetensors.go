package modelcache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math/bits"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

var safetensorsDtypeWidths = map[string]uint64{
	"BOOL":    1,
	"U8":      1,
	"I8":      1,
	"F8_E4M3": 1,
	"F8_E5M2": 1,
	"I16":     2,
	"U16":     2,
	"F16":     2,
	"BF16":    2,
	"I32":     4,
	"U32":     4,
	"F32":     4,
	"F64":     8,
	"I64":     8,
	"U64":     8,
}

// ValidateModelFilename enforces controlled .safetensors filenames and blocks
// path traversal (../, /, \) and unsafe pickle/archive extensions (.ckpt/.pt/.bin).
func ValidateModelFilename(filename string) error {
	if filename == "" || strings.Contains(filename, "..") || strings.ContainsAny(filename, `/\`) || strings.ContainsRune(filename, 0) {
		return NewError(400, "INVALID_PARAMETERS", "model filename contains invalid path characters")
	}
	lower := strings.ToLower(filename)
	if !strings.HasSuffix(lower, ".safetensors") {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "only .safetensors model files are permitted; pickle/archive formats (.ckpt/.pt/.bin) are forbidden")
	}
	if !filenamePattern.MatchString(filename) {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "model filename does not match controlled .safetensors naming contract")
	}
	return nil
}

type safetensorSpan struct {
	name  string
	begin uint64
	end   uint64
}

// ValidateSafetensorsFile performs full binary-frame verification per §4.3.1:
// 1. st_size > 8, LE uint64 header_len in [2, 100 MiB], 8 + header_len <= st_size
//    (does NOT reject byte 0 == 0x80 when header_len == 128!).
// 2. [8 : 8 + header_len] is valid UTF-8 without NUL, first non-whitespace byte '{',
//    strict JSON object with zero duplicate keys (rejecting disguised ZIP PK\x03\x04 & Pickle).
// 3. Optional "__metadata__" is map[string]string; every tensor entry has exact keys
//    {dtype, shape, data_offsets}, overflow-checked prod(shape)*dtype_width == end-begin,
//    sorted contiguous offsets [0..max_end] without gaps or overlaps, and exact file size
//    st_size == 8 + header_len + max_end (zero trailing bytes).
func ValidateSafetensorsFile(f *os.File, expectedSize int64) error {
	if f == nil || expectedSize <= 8 {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors file too small")
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() != expectedSize {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors file size mismatch")
	}

	var lenBuf [8]byte
	if _, err := f.ReadAt(lenBuf[:], 0); err != nil {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "failed reading safetensors header length")
	}
	headerLen := binary.LittleEndian.Uint64(lenBuf[:])
	if headerLen < 2 || headerLen > MaxSafetensorsHeaderBytes {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors header_len out of allowed [2, 100MiB] bounds")
	}
	if headerLen > uint64(expectedSize-8) {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors header_len exceeds file size")
	}

	headerBytes := make([]byte, int(headerLen))
	if _, err := f.ReadAt(headerBytes, 8); err != nil {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "failed reading safetensors JSON header")
	}
	if !utf8.Valid(headerBytes) || bytes.IndexByte(headerBytes, 0) >= 0 {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors header contains invalid UTF-8 or NUL bytes")
	}

	trimmed := bytes.TrimLeft(headerBytes, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors header does not start with JSON object '{'")
	}

	spans, err := parseSafetensorsHeaderJSON(headerBytes)
	if err != nil {
		return err
	}
	if len(spans) == 0 {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors file contains zero tensor entries")
	}

	sort.Slice(spans, func(i, j int) bool {
		if spans[i].begin != spans[j].begin {
			return spans[i].begin < spans[j].begin
		}
		return spans[i].end < spans[j].end
	})

	var cursor uint64
	for _, sp := range spans {
		if sp.begin != cursor {
			return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors data_offsets contain gap or overlap")
		}
		cursor = sp.end
	}

	totalExpected, carry := bits.Add64(8+headerLen, cursor, 0)
	if carry != 0 || totalExpected != uint64(expectedSize) {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors total file size does not equal 8 + header_len + max_end (trailing or truncated bytes)")
	}
	return nil
}

func parseSafetensorsHeaderJSON(headerBytes []byte) ([]safetensorSpan, error) {
	dec := json.NewDecoder(bytes.NewReader(headerBytes))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, NewError(400, "UNSAFE_MODEL_FORMAT", "invalid safetensors root JSON object")
	}

	seenRootKeys := map[string]bool{}
	var spans []safetensorSpan

	for dec.More() {
		kTok, kErr := dec.Token()
		key, ok := kTok.(string)
		if kErr != nil || !ok || key == "" || seenRootKeys[key] {
			return nil, NewError(400, "UNSAFE_MODEL_FORMAT", "duplicate or empty key in safetensors header")
		}
		seenRootKeys[key] = true

		if key == "__metadata__" {
			if err := parseMetadataMap(dec); err != nil {
				return nil, err
			}
			continue
		}

		sp, err := parseTensorInfo(dec, key)
		if err != nil {
			return nil, err
		}
		spans = append(spans, sp)
	}

	endTok, err := dec.Token()
	if err != nil || endTok != json.Delim('}') {
		return nil, NewError(400, "UNSAFE_MODEL_FORMAT", "unterminated safetensors root JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, NewError(400, "UNSAFE_MODEL_FORMAT", "unexpected trailing tokens in safetensors JSON header")
	}
	return spans, nil
}

func parseMetadataMap(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "__metadata__ must be a string-to-string JSON object")
	}
	seen := map[string]bool{}
	for dec.More() {
		kTok, kErr := dec.Token()
		key, ok := kTok.(string)
		if kErr != nil || !ok || seen[key] {
			return NewError(400, "UNSAFE_MODEL_FORMAT", "duplicate key in __metadata__")
		}
		seen[key] = true

		vTok, vErr := dec.Token()
		if vErr != nil {
			return NewError(400, "UNSAFE_MODEL_FORMAT", "malformed value in __metadata__")
		}
		if _, ok := vTok.(string); !ok {
			return NewError(400, "UNSAFE_MODEL_FORMAT", "__metadata__ values must all be strings")
		}
	}
	endTok, err := dec.Token()
	if err != nil || endTok != json.Delim('}') {
		return NewError(400, "UNSAFE_MODEL_FORMAT", "unterminated __metadata__ object")
	}
	return nil
}

func parseStrictUint64Token(tok any) (uint64, bool) {
	num, ok := tok.(json.Number)
	if !ok {
		return 0, false
	}
	raw := num.String()
	if raw == "" || strings.ContainsAny(raw, ".eE+-") {
		return 0, false
	}
	if len(raw) > 1 && raw[0] == '0' {
		return 0, false
	}
	val, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func parseTensorInfo(dec *json.Decoder, tensorName string) (safetensorSpan, error) {
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "tensor entry must be a JSON object")
	}

	seen := map[string]bool{}
	var dtype string
	var shape []uint64
	var offsets [2]uint64
	var hasDtype, hasShape, hasOffsets bool

	for dec.More() {
		kTok, kErr := dec.Token()
		field, ok := kTok.(string)
		if kErr != nil || !ok || seen[field] {
			return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "duplicate or invalid field in tensor entry")
		}
		seen[field] = true

		switch field {
		case "dtype":
			vTok, vErr := dec.Token()
			s, sOk := vTok.(string)
			if vErr != nil || !sOk {
				return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "tensor dtype must be string")
			}
			dtype = s
			hasDtype = true

		case "shape":
			aTok, aErr := dec.Token()
			if aErr != nil || aTok != json.Delim('[') {
				return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "tensor shape must be array")
			}
			if len(shape) == 0 {
				shape = []uint64{}
			}
			for dec.More() {
				dTok, dErr := dec.Token()
				if dErr != nil {
					return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "invalid tensor shape element")
				}
				dim, dOk := parseStrictUint64Token(dTok)
				if !dOk {
					return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "tensor shape dimensions must be non-negative integers")
				}
				if len(shape) >= 32 {
					return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "tensor rank exceeds 32")
				}
				shape = append(shape, dim)
			}
			endArr, endErr := dec.Token()
			if endErr != nil || endArr != json.Delim(']') {
				return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "unterminated tensor shape array")
			}
			hasShape = true

		case "data_offsets":
			aTok, aErr := dec.Token()
			if aErr != nil || aTok != json.Delim('[') {
				return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "data_offsets must be 2-element array")
			}
			count := 0
			for dec.More() {
				if count >= 2 {
					return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "data_offsets must have exactly 2 elements")
				}
				oTok, oErr := dec.Token()
				if oErr != nil {
					return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "invalid data_offsets element")
				}
				val, oOk := parseStrictUint64Token(oTok)
				if !oOk {
					return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "data_offsets must be non-negative integers")
				}
				offsets[count] = val
				count++
			}
			endArr, endErr := dec.Token()
			if endErr != nil || endArr != json.Delim(']') || count != 2 {
				return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "data_offsets must have exactly 2 integer elements")
			}
			hasOffsets = true

		default:
			return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "unexpected field in safetensors tensor entry")
		}
	}

	endObj, err := dec.Token()
	if err != nil || endObj != json.Delim('}') || !hasDtype || !hasShape || !hasOffsets {
		return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "incomplete safetensors tensor entry")
	}

	width, ok := safetensorsDtypeWidths[dtype]
	if !ok {
		return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "unsupported safetensors dtype")
	}

	if offsets[1] < offsets[0] {
		return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors data_offsets end < begin")
	}

	numElements := uint64(1)
	for _, dim := range shape {
		if dim == 0 {
			numElements = 0
			break
		}
		hi, lo := bits.Mul64(numElements, dim)
		if hi != 0 {
			return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors tensor shape product overflow")
		}
		numElements = lo
	}
	hi, expectedBytes := bits.Mul64(numElements, width)
	if hi != 0 {
		return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors tensor byte length overflow")
	}
	if offsets[1]-offsets[0] != expectedBytes {
		return safetensorSpan{}, NewError(400, "UNSAFE_MODEL_FORMAT", "safetensors tensor shape*dtype byte count does not match data_offsets span")
	}

	return safetensorSpan{
		name:  tensorName,
		begin: offsets[0],
		end:   offsets[1],
	}, nil
}
