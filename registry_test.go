package pgsandpit_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgsandpit"
)

var registryNames atomic.Int64

// uniqueName returns a name that is not registered yet. The registry cannot be
// emptied, so a fixed name would panic when the tests run with -count above 1.
func uniqueName() string {
	return fmt.Sprintf("name-%d", registryNames.Add(1))
}

func TestRegisterLookup(t *testing.T) {
	t.Parallel()

	name := uniqueName()
	sp := pgsandpit.New(dsn())
	t.Cleanup(func() { assert.NoError(t, sp.Close()) })
	pgsandpit.Register(name, sp)

	assert.Same(t, sp, pgsandpit.Lookup(name))

	var one int
	require.NoError(t, pgsandpit.Lookup(name).Session(t).DB().QueryRow("SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}

func TestRegisterSeparatesNames(t *testing.T) {
	t.Parallel()

	nameA, nameB := uniqueName(), uniqueName()
	a := pgsandpit.New(dsn())
	b := pgsandpit.New(dsn())
	pgsandpit.Register(nameA, a)
	pgsandpit.Register(nameB, b)

	assert.Same(t, a, pgsandpit.Lookup(nameA))
	assert.Same(t, b, pgsandpit.Lookup(nameB))
}

func TestRegisterConcurrentNames(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			name := uniqueName()
			sp := pgsandpit.New(dsn())
			pgsandpit.Register(name, sp)
			assert.Same(t, sp, pgsandpit.Lookup(name))
		})
	}
	wg.Wait()
}

func TestRegisterConcurrentSameName(t *testing.T) {
	t.Parallel()

	name := uniqueName()
	var registered, panicked atomic.Int32
	var winner atomic.Pointer[pgsandpit.Sandpit]

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			defer func() {
				if recover() != nil {
					panicked.Add(1)
				}
			}()
			sp := pgsandpit.New(dsn())
			pgsandpit.Register(name, sp)
			winner.Store(sp)
			registered.Add(1)
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), registered.Load())
	assert.Equal(t, int32(99), panicked.Load())
	assert.Same(t, winner.Load(), pgsandpit.Lookup(name))
}

func TestRegisterPanicsOnDuplicate(t *testing.T) {
	t.Parallel()

	name := uniqueName()
	pgsandpit.Register(name, pgsandpit.New(dsn()))

	want := fmt.Sprintf("pgsandpit: Register called twice for %q", name)
	assert.PanicsWithValue(t, want, func() {
		pgsandpit.Register(name, pgsandpit.New(dsn()))
	})
}

func TestRegisterPanicsOnNil(t *testing.T) {
	t.Parallel()

	assert.PanicsWithValue(t, "pgsandpit: Register of a nil Sandpit", func() {
		pgsandpit.Register(uniqueName(), nil)
	})
}

func TestLookupPanicsOnUnknownName(t *testing.T) {
	t.Parallel()

	name := uniqueName()
	want := fmt.Sprintf("pgsandpit: unknown name %q (is the package that registers it imported?)", name)
	assert.PanicsWithValue(t, want, func() {
		pgsandpit.Lookup(name)
	})
}
