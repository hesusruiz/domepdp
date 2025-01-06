// Copyright 2020 The Bazel Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package json defines utilities for converting Starlark values
// to/from JSON strings. The most recent IETF standard for JSON is
// https://www.ietf.org/rfc/rfc7159.txt.
package pdp

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.starlark.net/starlark"
)

// JsonToStarlark:
// The JsonToStarlark function has one required positional parameter, a JSON string.
// It returns the Starlark value that the string denotes.
//   - Numbers are parsed as int or float, depending on whether they
//     contain a decimal point.
//   - JSON objects are parsed as new unfrozen Starlark dicts.
//   - JSON arrays are parsed as new unfrozen Starlark lists.
//
// If x is not a valid JSON string, the behavior depends on the "default"
// parameter: if present, Decode returns its value; otherwise, Decode fails.
func JsonToStarlark(s string, d starlark.Value) (v starlark.Value, err error) {

	// The decoder necessarily makes certain representation choices
	// such as list vs tuple, struct vs dict, int vs float.
	// In principle, we could parameterize it to allow the caller to
	// control the returned types, but there's no compelling need yet.

	// Use panic/recover with a distinguished type (failure) for error handling.
	// If "default" is set, we only want to return it when encountering invalid
	// json - not for any other possible causes of panic.
	// In particular, if we ever extend the json.decode API to take a callback,
	// a distinguished, private failure type prevents the possibility of
	// json.decode with "default" becoming abused as a try-catch mechanism.
	type failure string
	fail := func(format string, args ...interface{}) {
		panic(failure(fmt.Sprintf(format, args...)))
	}

	i := 0

	// skipSpace consumes leading spaces, and reports whether there is more input.
	skipSpace := func() bool {
		for ; i < len(s); i++ {
			b := s[i]
			if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
				return true
			}
		}
		return false
	}

	// next consumes leading spaces and returns the first non-space.
	// It panics if at EOF.
	next := func() byte {
		if skipSpace() {
			return s[i]
		}
		fail("unexpected end of file")
		panic("unreachable")
	}

	// parse returns the next JSON value from the input.
	// It consumes leading but not trailing whitespace.
	// It panics on error.
	var parse func() starlark.Value
	parse = func() starlark.Value {
		b := next()
		switch b {
		case '"':
			// string

			// Find end of quotation.
			// Also, record whether trivial unquoting is safe.
			// Non-trivial unquoting is handled by Go's encoding/json.
			safe := true
			closed := false
			j := i + 1
			for ; j < len(s); j++ {
				b := s[j]
				if b == '\\' {
					safe = false
					j++ // skip x in \x
				} else if b == '"' {
					closed = true
					j++ // skip '"'
					break
				} else if b >= utf8.RuneSelf {
					safe = false
				}
			}
			if !closed {
				fail("unclosed string literal")
			}

			r := s[i:j]
			i = j

			// unquote
			if safe {
				r = r[1 : len(r)-1]
			} else if err := json.Unmarshal([]byte(r), &r); err != nil {
				fail("%s", err)
			}
			return starlark.String(r)

		case 'n':
			if strings.HasPrefix(s[i:], "null") {
				i += len("null")
				return starlark.None
			}

		case 't':
			if strings.HasPrefix(s[i:], "true") {
				i += len("true")
				return starlark.True
			}

		case 'f':
			if strings.HasPrefix(s[i:], "false") {
				i += len("false")
				return starlark.False
			}

		case '[':
			// array
			var elems []starlark.Value

			i++ // '['
			b = next()
			if b != ']' {
				for {
					elem := parse()
					elems = append(elems, elem)
					b = next()
					if b != ',' {
						if b != ']' {
							fail("got %q, want ',' or ']'", b)
						}
						break
					}
					i++ // ','
				}
			}
			i++ // ']'
			return starlark.NewList(elems)

		case '{':
			// object
			dict := new(starlark.Dict)

			i++ // '{'
			b = next()
			if b != '}' {
				for {
					key := parse()
					if _, ok := key.(starlark.String); !ok {
						fail("got %s for object key, want string", key.Type())
					}
					b = next()
					if b != ':' {
						fail("after object key, got %q, want ':' ", b)
					}
					i++ // ':'
					value := parse()
					dict.SetKey(key, value) // can't fail
					b = next()
					if b != ',' {
						if b != '}' {
							fail("in object, got %q, want ',' or '}'", b)
						}
						break
					}
					i++ // ','
				}
			}
			i++ // '}'
			return dict

		default:
			// number?
			if isdigit(b) || b == '-' {
				// scan literal. Allow [0-9+-eE.] for now.
				float := false
				var j int
				for j = i + 1; j < len(s); j++ {
					b = s[j]
					if isdigit(b) {
						// ok
					} else if b == '.' ||
						b == 'e' ||
						b == 'E' ||
						b == '+' ||
						b == '-' {
						float = true
					} else {
						break
					}
				}
				num := s[i:j]
				i = j

				// Unlike most C-like languages,
				// JSON disallows a leading zero before a digit.
				digits := num
				if num[0] == '-' {
					digits = num[1:]
				}
				if digits == "" || digits[0] == '0' && len(digits) > 1 && isdigit(digits[1]) {
					fail("invalid number: %s", num)
				}

				// parse literal
				if float {
					x, err := strconv.ParseFloat(num, 64)
					if err != nil {
						fail("invalid number: %s", num)
					}
					return starlark.Float(x)
				} else {
					x, ok := new(big.Int).SetString(num, 10)
					if !ok {
						fail("invalid number: %s", num)
					}
					return starlark.MakeBigInt(x)
				}
			}
		}
		fail("unexpected character %q", b)
		panic("unreachable")
	}
	defer func() {
		x := recover()
		switch x := x.(type) {
		case failure:
			if d != nil {
				v = d
			} else {
				err = fmt.Errorf("json.decode: at offset %d, %s", i, x)
			}
		case nil:
			// nop
		default:
			panic(x) // unexpected panic
		}
	}()
	v = parse()
	if skipSpace() {
		fail("unexpected character %q after value", s[i])
	}
	return v, nil
}

func isdigit(b byte) bool {
	return b >= '0' && b <= '9'
}
