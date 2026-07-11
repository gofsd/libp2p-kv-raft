package shmevent

import "sync"

// Registry is the server-side id<->string interning table backing
// EventSetKey/EventGetKey and the SourceID-addressed forms of
// EventSetField/EventGetField/EventAdd -- see this package's doc comment.
//
// Lifetime: entries live for the daemon process's lifetime (there is no
// explicit eviction), bounded implicitly by ids being client-chosen
// uint16s (at most 65536 live entries at once). Register overwrites on a
// colliding id rather than erroring, so a client that reuses ids (e.g. a
// small rotating counter) self-bounds its own usage. There is currently no
// cross-caller isolation: the id space is global to whichever Registry a
// daemon runs, shared by every local caller.
type Registry struct {
	mu      sync.RWMutex
	entries map[uint16][]byte
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[uint16][]byte)}
}

// Register stores value under id, overwriting any previous entry.
func (r *Registry) Register(id uint16, value []byte) {
	stored := make([]byte, len(value))
	copy(stored, value)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[id] = stored
}

// Lookup returns the value registered under id, if any.
func (r *Registry) Lookup(id uint16) ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.entries[id]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}
