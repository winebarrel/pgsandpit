package pgsandpit

import (
	"fmt"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = map[string]*Sandpit{}
)

// Register makes sp available under name, so that tests can choose a database
// by name with Lookup. Each test package runs in its own process, so a package
// shared by the tests can register the names in its init function:
//
//	func init() {
//		pgsandpit.Register("testdb1", pgsandpit.New("postgres://..."))
//	}
//
// Register panics if sp is nil or name is already registered. Register and
// Lookup are safe for concurrent use.
func Register(name string, sp *Sandpit) {
	if sp == nil {
		panic("pgsandpit: Register of a nil Sandpit")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("pgsandpit: Register called twice for %q", name))
	}
	registry[name] = sp
}

// Lookup returns the Sandpit registered under name. It panics if name is not
// registered.
func Lookup(name string) *Sandpit {
	registryMu.RLock()
	defer registryMu.RUnlock()
	sp, ok := registry[name]
	if !ok {
		panic(fmt.Sprintf("pgsandpit: unknown name %q (is the package that registers it imported?)", name))
	}
	return sp
}
