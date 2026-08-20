package luacmd

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	lua "github.com/yuin/gopher-lua"
)

// Values crossing between Go and Lua: a command's inputs going in, a
// child's inputs going out, a log record's fields coming back. All of it
// is JSON at the wire, because that is what SubmitCommand and the command
// log already carry.

// maxConvertDepth bounds how deeply nested a value may be in either
// direction. Deep enough for any real inputs shape, shallow enough that
// the conversion cannot run the Go stack out on hostile input.
const maxConvertDepth = 12

// MaxInputBytes bounds the JSON a script may hand to kv.submit. The
// child's own request record carries it, replicated to every node forever,
// so this is smaller than a script's own source limit rather than larger.
const MaxInputBytes = 16 << 10

// toLua converts a decoded JSON value into a Lua value. Objects become
// tables keyed by string, arrays become tables keyed 1..n (which is what
// ipairs and the # operator expect), and null becomes nil.
func toLua(state *lua.LState, v any, depth int) (lua.LValue, error) {
	if depth > maxConvertDepth {
		return nil, fmt.Errorf("luacmd: value nests deeper than %d levels", maxConvertDepth)
	}
	switch value := v.(type) {
	case nil:
		return lua.LNil, nil
	case bool:
		return lua.LBool(value), nil
	case float64:
		return lua.LNumber(value), nil
	case string:
		return lua.LString(value), nil
	case []any:
		table := state.NewTable()
		for i, item := range value {
			converted, err := toLua(state, item, depth+1)
			if err != nil {
				return nil, err
			}
			table.RawSetInt(i+1, converted)
		}
		return table, nil
	case map[string]any:
		table := state.NewTable()
		for key, item := range value {
			converted, err := toLua(state, item, depth+1)
			if err != nil {
				return nil, err
			}
			table.RawSetString(key, converted)
		}
		return table, nil
	default:
		return nil, fmt.Errorf("luacmd: cannot represent %T in Lua", v)
	}
}

// fromLua converts a Lua value into something encoding/json can marshal.
//
// A table becomes an array if its keys are exactly 1..n and an object
// otherwise -- the usual Lua ambiguity, resolved the usual way, and worth
// knowing about when writing a script: an empty table encodes as an empty
// object, and a table with a hole in its integer keys encodes as an object
// with numeric keys as strings.
//
// seen carries the tables already being converted on this path, so a table
// that contains itself is reported rather than followed forever.
func fromLua(v lua.LValue, depth int, seen map[*lua.LTable]bool) (any, error) {
	if depth > maxConvertDepth {
		return nil, fmt.Errorf("luacmd: value nests deeper than %d levels", maxConvertDepth)
	}
	switch value := v.(type) {
	case *lua.LNilType:
		return nil, nil
	case lua.LBool:
		return bool(value), nil
	case lua.LNumber:
		return float64(value), nil
	case lua.LString:
		return string(value), nil
	case *lua.LTable:
		if seen[value] {
			return nil, fmt.Errorf("luacmd: table refers to itself")
		}
		seen[value] = true
		defer delete(seen, value)
		return tableFromLua(value, depth, seen)
	default:
		return nil, fmt.Errorf("luacmd: cannot convert a %s value", v.Type().String())
	}
}

func tableFromLua(table *lua.LTable, depth int, seen map[*lua.LTable]bool) (any, error) {
	entries := map[string]any{}
	intKeys := make([]int, 0, table.Len())
	nonIntKey := false

	var iterErr error
	table.ForEach(func(k, v lua.LValue) {
		if iterErr != nil {
			return
		}
		converted, err := fromLua(v, depth+1, seen)
		if err != nil {
			iterErr = err
			return
		}
		switch key := k.(type) {
		case lua.LNumber:
			whole := int(key)
			if lua.LNumber(whole) == key && whole >= 1 {
				intKeys = append(intKeys, whole)
			} else {
				nonIntKey = true
			}
			entries[strconv.FormatFloat(float64(key), 'f', -1, 64)] = converted
		case lua.LString:
			nonIntKey = true
			entries[string(key)] = converted
		default:
			iterErr = fmt.Errorf("luacmd: table has a %s key, which has no JSON equivalent", k.Type().String())
		}
	})
	if iterErr != nil {
		return nil, iterErr
	}

	// An array is a table whose keys are exactly 1..n and nothing else.
	if !nonIntKey && len(intKeys) == len(entries) && len(intKeys) > 0 {
		sort.Ints(intKeys)
		isSequence := true
		for i, key := range intKeys {
			if key != i+1 {
				isSequence = false
				break
			}
		}
		if isSequence {
			items := make([]any, 0, len(intKeys))
			for _, key := range intKeys {
				items = append(items, entries[strconv.Itoa(key)])
			}
			return items, nil
		}
	}
	return entries, nil
}

// inputsToLua decodes an inputs JSON object into a Lua table, dropping the
// bookkeeping keys this package adds (see depthKey) so a script sees only
// what its submitter wrote. A blank or "null" inputs is an empty table
// rather than nil, so that kv.inputs.whatever is always a lookup and never
// an error.
func inputsToLua(state *lua.LState, inputsJSON string) (*lua.LTable, error) {
	if inputsJSON == "" {
		return state.NewTable(), nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(inputsJSON), &decoded); err != nil {
		return nil, fmt.Errorf("luacmd: decode inputs: %w", err)
	}
	if decoded == nil {
		return state.NewTable(), nil
	}
	value, err := toLua(state, decoded, 0)
	if err != nil {
		return nil, err
	}
	table, ok := value.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("luacmd: inputs must be a JSON object, got %s", value.Type().String())
	}
	table.RawSetString(depthKey, lua.LNil)
	return table, nil
}

// luaToInputs encodes a Lua value as the inputs JSON for a child dispatch.
// nil means "no inputs" rather than the JSON literal null, since that is
// what a command with nothing to say expects to receive.
func luaToInputs(v lua.LValue) (string, error) {
	if v == nil || v == lua.LNil {
		return "", nil
	}
	decoded, err := fromLua(v, 0, map[*lua.LTable]bool{})
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return "", fmt.Errorf("luacmd: encode inputs: %w", err)
	}
	if len(encoded) > MaxInputBytes {
		return "", fmt.Errorf("luacmd: inputs exceed %d bytes", MaxInputBytes)
	}
	return string(encoded), nil
}

// luaToFields converts a Lua table into the flat string map a log entry's
// fields are. Scalars are stringified (a number as it would print, a
// boolean as true/false); anything nested is refused, because the log
// record it is going into is flat and quietly flattening it here would
// lose the difference between a nested value and a string that looks like
// one.
func luaToFields(v lua.LValue) (map[string]string, error) {
	if v == nil || v == lua.LNil {
		return nil, nil
	}
	table, ok := v.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("luacmd: fields must be a table, got %s", v.Type().String())
	}

	fields := map[string]string{}
	var iterErr error
	table.ForEach(func(k, value lua.LValue) {
		if iterErr != nil {
			return
		}
		key, ok := k.(lua.LString)
		if !ok {
			iterErr = fmt.Errorf("luacmd: field names must be strings, got a %s", k.Type().String())
			return
		}
		switch value.(type) {
		case lua.LString, lua.LNumber, lua.LBool:
			fields[string(key)] = value.String()
		default:
			iterErr = fmt.Errorf("luacmd: field %q is a %s; a log entry's fields hold only strings, numbers and booleans", string(key), value.Type().String())
		}
	})
	if iterErr != nil {
		return nil, iterErr
	}
	return fields, nil
}

// fieldsToLua is luaToFields' inverse, for handing a child's recorded
// fields back to the script that asked for them.
func fieldsToLua(state *lua.LState, fields map[string]string) *lua.LTable {
	table := state.NewTable()
	for key, value := range fields {
		table.RawSetString(key, lua.LString(value))
	}
	return table
}
