package fs

import (
	"os"
	"testing"

	"github.com/thegrumpylion/libkv/store"
	"github.com/thegrumpylion/libkv/testutils"
)

func makeFSclient(t *testing.T) store.Store {
	kv, err := New([]string{"/tmp/not_exist_dir/fs_backend"}, nil)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestFSStore(t *testing.T) {
	kv := makeFSclient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunCleanup(t, kv)

	kv.Close()

	os.RemoveAll("/tmp/not_exist_dir/fs_backend")
}

func TestIsValid(t *testing.T) {

	invalid := []string{
		"..",
		"/..",
		"one/../../../../..",
		"/one/../../../../..",
	}

	valid := []string{
		"one",
		"one",
		"one/two/three",
		"/one/two/three",
	}

	for _, p := range invalid {
		if isValid(p) {
			t.Errorf("Path %s should not be valid\n", p)
		}
	}

	for _, p := range valid {
		if !isValid(p) {
			t.Errorf("Path %s should be valid\n", p)
		}
	}
}
