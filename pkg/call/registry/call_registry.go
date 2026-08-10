package call_registry

import (
	"sync"

	"github.com/purpshell/meowcaller"
)

// entry pairs a live call with the instance it belongs to, so Get can refuse to hand
// back a call to a caller authenticated as a different instance.
type entry struct {
	instanceID string
	call       *meowcaller.Call
	outgoing   bool
}

// CallRegistry tracks in-progress meowcaller calls by call ID, scoped by instance.
// It is written from the whatsmeow event-handling goroutine (on incoming call) and
// read/written from HTTP handler goroutines (answer/hangup/stream), so all access is
// mutex-guarded.
type CallRegistry struct {
	mu      sync.RWMutex
	entries map[string]entry
}

// NewCallRegistry returns an empty registry.
func NewCallRegistry() *CallRegistry {
	return &CallRegistry{entries: make(map[string]entry)}
}

// Store records call under its own ID (meowcaller.Call.ID()), tagged with instanceID.
func (r *CallRegistry) Store(instanceID string, call *meowcaller.Call) {
	r.store(instanceID, call, false)
}

// StoreOutgoing records a call placed through /call/dial. Outgoing calls need an
// explicit peer-accept signal before browser clients may treat inbound media as live.
func (r *CallRegistry) StoreOutgoing(instanceID string, call *meowcaller.Call) {
	r.store(instanceID, call, true)
}

func (r *CallRegistry) store(instanceID string, call *meowcaller.Call, outgoing bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[call.ID()] = entry{instanceID: instanceID, call: call, outgoing: outgoing}
}

// IsOutgoing reports whether callID was placed through /call/dial by instanceID.
func (r *CallRegistry) IsOutgoing(instanceID, callID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[callID]
	return ok && e.instanceID == instanceID && e.outgoing
}

// Get returns the call for callID, but only if it was stored under instanceID.
func (r *CallRegistry) Get(instanceID, callID string) (*meowcaller.Call, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[callID]
	if !ok || e.instanceID != instanceID {
		return nil, false
	}
	return e.call, true
}

// DeleteIf removes callID only when it still belongs to instanceID and points to
// expected. This prevents a late teardown callback from deleting a newer call
// that reused the same ID.
func (r *CallRegistry) DeleteIf(instanceID, callID string, expected *meowcaller.Call) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[callID]
	if !ok || e.instanceID != instanceID || e.call != expected {
		return false
	}
	delete(r.entries, callID)
	return true
}

// DeleteInstance removes all calls owned by instanceID, for disconnect/cache
// teardown. It returns the number of entries removed.
func (r *CallRegistry) DeleteInstance(instanceID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := 0
	for callID, e := range r.entries {
		if e.instanceID == instanceID {
			delete(r.entries, callID)
			removed++
		}
	}
	return removed
}

// Delete removes callID regardless of which instance it belongs to.
func (r *CallRegistry) Delete(callID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, callID)
}
