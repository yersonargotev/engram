package taskbriefing

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"unicode"
)

const maximumGitTermBytes = 64 * 1024

// collectTerms retains only the bounded vocabulary used for selection. Once the
// vocabulary is full, omitted occurrences are counted without retaining their
// values, so repository input cannot grow the tracked term set without bound.
func collectTerms(input io.Reader, limit int, byteLimit int64) ([]string, int, bool, error) {
	var limited *byteBoundedReader
	if byteLimit > 0 {
		limited = &byteBoundedReader{source: input, remaining: byteLimit, complete: true}
		input = limited
	}
	reader := bufio.NewReader(input)
	capacity := min(limit, 64)
	retained := make([]string, 0, capacity)
	retainedSet := make(map[string]struct{}, capacity)
	omitted := 0
	var token strings.Builder
	overflowed := false
	flush := func() {
		if overflowed {
			omitted++
			overflowed = false
			token.Reset()
			return
		}
		if token.Len() == 0 {
			return
		}
		term := strings.ToLower(token.String())
		token.Reset()
		if len([]rune(term)) < 2 {
			return
		}
		if _, exists := retainedSet[term]; exists {
			return
		}
		if len(retained) < limit {
			retained = append(retained, term)
			retainedSet[term] = struct{}{}
			return
		}
		omitted++
	}

	for {
		r, size, err := reader.ReadRune()
		if errors.Is(err, io.EOF) {
			complete := limited == nil || limited.complete
			if complete {
				flush()
			}
			return retained, omitted, complete, nil
		}
		if err != nil {
			return nil, 0, false, err
		}
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			flush()
			continue
		}
		if overflowed {
			continue
		}
		if token.Len()+size > maximumGitTermBytes {
			overflowed = true
			token.Reset()
			continue
		}
		token.WriteRune(r)
	}
}

type byteBoundedReader struct {
	source    io.Reader
	remaining int64
	complete  bool
}

func (r *byteBoundedReader) Read(buffer []byte) (int, error) {
	if r.remaining > 0 {
		if int64(len(buffer)) > r.remaining {
			buffer = buffer[:r.remaining]
		}
		read, err := r.source.Read(buffer)
		r.remaining -= int64(read)
		return read, err
	}
	var probe [1]byte
	read, err := r.source.Read(probe[:])
	if read > 0 {
		r.complete = false
		return 0, io.EOF
	}
	return 0, err
}
