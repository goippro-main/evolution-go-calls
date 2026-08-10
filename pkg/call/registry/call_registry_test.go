package call_registry

import (
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestStoreAndGet(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}

	r.Store("instance-a", call)

	got, ok := r.Get("instance-a", callIDOf(call))
	if !ok {
		t.Fatal("expected call to be found")
	}
	if got != call {
		t.Fatal("expected the same call pointer back")
	}
}

func TestGetWrongInstanceFails(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}
	r.Store("instance-a", call)

	_, ok := r.Get("instance-b", callIDOf(call))
	if ok {
		t.Fatal("expected lookup from a different instance to fail")
	}
}

func TestDeleteRemovesEntry(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}
	r.Store("instance-a", call)
	r.Delete(callIDOf(call))

	_, ok := r.Get("instance-a", callIDOf(call))
	if ok {
		t.Fatal("expected entry to be gone after Delete")
	}
}

func TestOutgoingMetadataIsScopedByInstance(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}
	r.StoreOutgoing("instance-a", call)

	if !r.IsOutgoing("instance-a", callIDOf(call)) {
		t.Fatal("expected outgoing call metadata")
	}
	if r.IsOutgoing("instance-b", callIDOf(call)) {
		t.Fatal("expected outgoing metadata lookup from a different instance to fail")
	}
}

func TestStoreDefaultsToIncoming(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}
	r.Store("instance-a", call)

	if r.IsOutgoing("instance-a", callIDOf(call)) {
		t.Fatal("expected Store to record an incoming call")
	}
}

func TestDeleteIfDoesNotDeleteReplacement(t *testing.T) {
	r := NewCallRegistry()
	first := &meowcaller.Call{}
	second := &meowcaller.Call{}
	r.Store("instance-a", first)
	r.Store("instance-a", second)

	if r.DeleteIf("instance-a", callIDOf(first), first) {
		t.Fatal("stale call must not delete replacement")
	}
	if got, ok := r.Get("instance-a", callIDOf(second)); !ok || got != second {
		t.Fatal("replacement call should remain registered")
	}
}

func TestDeleteInstanceIsScoped(t *testing.T) {
	r := NewCallRegistry()
	a := &meowcaller.Call{}
	r.Store("instance-a", a)

	if got := r.DeleteInstance("instance-b"); got != 0 {
		t.Fatalf("removed %d calls from another instance, want 0", got)
	}
	if got := r.DeleteInstance("instance-a"); got != 1 {
		t.Fatalf("removed %d calls, want 1", got)
	}
	if _, ok := r.Get("instance-a", callIDOf(a)); ok {
		t.Fatal("instance-a call should be removed")
	}
}

// callIDOf mirrors what CallRegistry.Store keys entries by: meowcaller.Call.ID().
// A zero-value *meowcaller.Call has an empty string ID, which is a perfectly valid
// (if degenerate) key for exercising Store/Get/Delete without a live call.
func callIDOf(call *meowcaller.Call) string {
	return call.ID()
}
