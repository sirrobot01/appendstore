package appendstore_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirrobot01/appendstore"
)

func Example() {
	dir, err := os.MkdirTemp("", "appendstore-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	store, err := appendstore.Open(filepath.Join(dir, "example.db"), appendstore.Options{
		IndexedFields: []string{"kind"},
	})
	if err != nil {
		panic(err)
	}
	defer store.Close()

	if err := store.Put("greeting", []byte("hello"), &appendstore.PutOptions{
		Attributes: map[string]string{"kind": "message"},
	}); err != nil {
		panic(err)
	}
	value, err := store.Get("greeting")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))
	fmt.Println(store.KeysBy("kind", "message"))

	// Output:
	// hello
	// [greeting]
}
