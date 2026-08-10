package fleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// State is what a fleet run remembers between invocations.
//
// Provisioning a thousand tenants is initdb a thousand times, and that
// is half an hour of work that must not be repeated because a phase
// after it failed, or because somebody wants the steady phase again
// with a different ceiling. So the refs that are ready are written down
// as they become ready, and a second run against the same store and the
// same state file starts from what is already there.
type State struct {
	// Store is the target the tenants were made in. A state file
	// pointing at a different store describes tenants that are not
	// there, and the run refuses rather than trusting it.
	Store  string `json:"store"`
	Secret string `json:"secret"`
	// Ready is every ref that has a database, a table, and rows.
	Ready []string `json:"ready"`

	path string
	mu   sync.Mutex
	have map[string]bool
}

// LoadState reads a state file, or returns an empty one when there is
// none, which is what the first run of a fleet sees.
func LoadState(path, store, secret string) (*State, error) {
	s := &State{Store: store, Secret: secret, path: path, have: map[string]bool{}}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var on State
	if err := json.Unmarshal(raw, &on); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if on.Store != store {
		return nil, fmt.Errorf("%s describes tenants in %s, not %s", path, on.Store, store)
	}
	if on.Secret != "" && secret != "" && on.Secret != secret {
		return nil, fmt.Errorf("%s was written with a different secret, the tokens would not verify", path)
	}
	if on.Secret != "" {
		s.Secret = on.Secret
	}
	s.Ready = on.Ready
	for _, ref := range on.Ready {
		s.have[ref] = true
	}
	return s, nil
}

// Has reports whether a ref is already provisioned.
func (s *State) Has(ref string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.have[ref]
}

// Add records a ref as ready and writes the file. Written on every
// tenant rather than at the end, because the run this protects against
// is the one that is killed halfway.
func (s *State) Add(ref string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.have[ref] {
		return nil
	}
	s.have[ref] = true
	s.Ready = append(s.Ready, ref)
	sort.Strings(s.Ready)
	return s.save()
}

// Count is how many tenants are ready.
func (s *State) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Ready)
}

// save writes through a temp file and a rename, so a kill in the middle
// leaves the previous state rather than half of this one.
func (s *State) save() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(struct {
		Store  string   `json:"store"`
		Secret string   `json:"secret"`
		Ready  []string `json:"ready"`
	}{s.Store, s.Secret, s.Ready}, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Refs names the tenants of a fleet of size n. Fixed width so they sort
// the way they were made, and prefixed so a store can hold a fleet next
// to whatever else is in it.
func Refs(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, fmt.Sprintf("%s%04d", prefix, i+1))
	}
	return out
}
