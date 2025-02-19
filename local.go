package cache

import (
	"math/rand"
	"sync"
	"time"

	"github.com/vmihailenco/go-tinylfu"
)

type LocalCache interface {
	Set(key string, data []byte)
	MSet(keys []string, b [][]byte)
	Get(key string) ([]byte, bool)
	MGet(keys []string) ([][]byte, []string, []int)
	Del(key string)
}

type TinyLFU struct {
	mu     sync.Mutex
	rand   *rand.Rand
	lfu    *tinylfu.T
	ttl    time.Duration
	offset time.Duration
}

var _ LocalCache = (*TinyLFU)(nil)

func NewTinyLFU(size int, ttl time.Duration) *TinyLFU {
	const maxOffset = 10 * time.Second

	offset := ttl / 10
	if offset > maxOffset {
		offset = maxOffset
	}

	return &TinyLFU{
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
		lfu:    tinylfu.New(size, 100000),
		ttl:    ttl,
		offset: offset,
	}
}

func (c *TinyLFU) UseRandomizedTTL(offset time.Duration) {
	c.offset = offset
}

func (c *TinyLFU) Set(key string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.set(key, b)
}

func (c *TinyLFU) MSet(keys []string, b [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, key := range keys {
		if len(b[i]) == 0 {
			continue
		}
		c.set(key, b[i])
	}
}

func (c *TinyLFU) set(key string, b []byte) {
	ttl := c.ttl
	if c.offset > 0 {
		ttl += time.Duration(c.rand.Int63n(int64(c.offset)))
	}

	c.lfu.Set(&tinylfu.Item{
		Key:      key,
		Value:    b,
		ExpireAt: time.Now().Add(ttl),
	})
}

func (c *TinyLFU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, ok := c.lfu.Get(key)
	if !ok {
		return nil, false
	}

	b := val.([]byte)
	return b, true
}

func (c *TinyLFU) MGet(keys []string) ([][]byte, []string, []int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := make([][]byte, 0, len(keys))
	notFoundKeys := make([]string, len(keys))
	notFoundIdx := make([]int, len(keys))
	for i, key := range keys {
		val, ok := c.lfu.Get(key)
		if ok {
			b[i] = val.([]byte)
			continue
		}
		notFoundKeys = append(notFoundKeys, key)
		notFoundIdx = append(notFoundIdx, i)
	}

	return b, notFoundKeys, notFoundIdx
}

func (c *TinyLFU) Del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lfu.Del(key)
}
