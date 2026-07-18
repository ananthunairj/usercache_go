package src

import "sync"

type Trier interface {
	Get(key string) (string, error)
	Put(key string, value any) bool
	Delete(key string) bool
}

type RuneTrie struct {
	value    any
	children map[rune]*RuneTrie
	mu       sync.RWMutex
}

type nodeRune struct {
	node *RuneTrie
	r    rune
}

func NewRuneTrie() *RuneTrie {
	return new(RuneTrie)
}

func (trie *RuneTrie) Get(key string) any {
	node := trie
	for _, r := range key {
		node.mu.RLock()
		next := node.children[r]
		node.mu.RUnlock()
		if next == nil {
			return nil
		}
		node = next
	}
	return node.value
}

func (trie *RuneTrie) Put(key string, value any) bool {
	node := trie
	for _, r := range key {
		child := node.children[r]
		if child == nil {
			if node.children == nil {
				node.children = map[rune]*RuneTrie{}
			}
			child = new(RuneTrie)
			node.mu.Lock()
			node.children[r] = child
			node.mu.Unlock()
		}
		node = child
	}
	isNewVal := node.value == nil
	node.mu.Lock()
	node.value = value
	node.mu.Unlock()
	return isNewVal

}

func (trie *RuneTrie) Delete(key string) bool {
	path := make([]nodeRune, len(key))
	node := trie
	for i, r := range key {
		path[i] = nodeRune{r: r, node: node}
		node = node.children[r]
		if node == nil {
			return false
		}
	}
	node.value = nil
	if node.isLeaf() {
		for i := len(key) - 1; i >= 0; i-- {
			if path[i].node == nil {
				continue
			}
			parent := path[i].node
			r := path[i].r
			delete(parent.children, r)
			if !parent.isLeaf() {
				break
			}
			parent.children = nil
			if parent.value != nil {
				break
			}

		}
	}
	return true
}

func (trie *RuneTrie) isLeaf() bool {
	return len(trie.children) == 0
}
