/*
Copyright The Platform Mesh Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package cascade provides a thread-safe cache of every known Cascade's reach,
// used to decide whether a given workspace is covered by any Cascade.
package cascade

import (
	"strings"
	"sync"

	"github.com/kcp-dev/logicalcluster/v3"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// key identifies a Cascade by the logical cluster it lives in and its
// (cluster-scoped) name.
type key struct {
	hash multicluster.ClusterName
	name string
}

// Entry is the cached reach of a single Cascade: the workspace path it lives in
// and how many levels of descendants it covers.
type Entry struct {
	Hash     multicluster.ClusterName // cascade's own logical cluster (hash)
	Name     string                   // cascade object name (cluster-scoped)
	Path     string                   // cascade's own workspace path, e.g. "root:org"
	MaxDepth int32                    // >= 1; 1 == direct children only
}

// Cache is a thread-safe registry of every known Cascade's reach. The cascade
// reconciler writes it; the workspace reconciler reads it.
type Cache struct {
	mu    sync.RWMutex
	items map[key]Entry
}

// NewCache returns an empty Cache.
func NewCache() *Cache {
	return &Cache{items: map[key]Entry{}}
}

// Upsert inserts or replaces the entry for a Cascade.
func (c *Cache) Upsert(e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key{e.Hash, e.Name}] = e
}

// Delete removes the entry for a Cascade, if present.
func (c *Cache) Delete(hash multicluster.ClusterName, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key{hash, name})
}

// Match returns every cascade whose workspace is a strict ancestor of childPath
// within its MaxDepth. depth is the number of path segments between the cascade
// and the child (1 == direct child), and must satisfy 1 <= depth <= MaxDepth.
func (c *Cache) Match(childPath string) []Entry {
	if childPath == "" {
		return nil
	}
	child := logicalcluster.NewPath(childPath)
	childDepth := len(strings.Split(childPath, ":"))

	c.mu.RLock()
	defer c.mu.RUnlock()

	var out []Entry
	for _, e := range c.items {
		if e.Path == "" || e.Path == childPath {
			continue
		}
		if !child.HasPrefix(logicalcluster.NewPath(e.Path)) {
			continue
		}
		depth := childDepth - len(strings.Split(e.Path, ":"))
		if depth >= 1 && depth <= int(e.MaxDepth) {
			out = append(out, e)
		}
	}
	return out
}
