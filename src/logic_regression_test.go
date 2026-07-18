package src

import (
	"testing"
	"time"
)

func TestUser_AddSessionCache_StoresValue(t *testing.T) {
	um := NewUserManager()

	user, err := um.AddNewUser(30*time.Minute, 24*time.Hour, 100)
	if err != nil {
		t.Fatalf("AddNewUser failed: %v", err)
	}

	session, err := user.AddSessionCache(user.CurrentSessionId, "alpha", "value")
	if err != nil {
		t.Fatalf("AddSessionCache returned error: %v", err)
	}
	if session == nil {
		t.Fatal("expected a session to be returned")
	}

	sessionCache := user.Sessions[user.CurrentSessionId].cache
	if sessionCache == nil {
		t.Fatal("expected session cache to be initialized")
	}
	item, ok := sessionCache.Store["alpha"]
	if !ok {
		t.Fatalf("expected cache entry for key %q to exist", "alpha")
	}
	if item.Value != "value" {
		t.Fatalf("expected cached value %q, got %#v", "value", item.Value)
	}
}

func TestRuneTrie_GetAfterPut(t *testing.T) {
	trie := NewRuneTrie()

	inserted := trie.Put("abc", 42)
	if !inserted {
		t.Fatal("expected first insert to succeed")
	}

	got := trie.Get("abc")
	if got != 42 {
		t.Fatalf("expected value 42, got %#v", got)
	}
}
