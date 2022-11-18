package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/singleflight"
)

const (
	compressionThreshold = 64
	timeLen              = 4
)

const (
	noCompression = 0x0
	s2Compression = 0x1
)

var (
	ErrCacheMiss          = errors.New("cache: key is missing")
	errRedisNil           = errors.New("cache: Redis is nil")
	errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")
)

type rediser interface {
	Set(ctx context.Context, key string, value any, ttl time.Duration) *redis.StatusCmd
	SetXX(ctx context.Context, key string, value any, ttl time.Duration) *redis.BoolCmd
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) *redis.BoolCmd

	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	Pipeline() redis.Pipeliner
	TxPipeline() redis.Pipeliner
}

type pipelinedRediser interface {
	rediser
	Exec(ctx context.Context) ([]redis.Cmder, error)
}

type Item struct {
	Ctx context.Context

	Key   string
	Value any

	// TTL is the cache expiration time.
	// Default TTL is 1 hour.
	TTL time.Duration

	// Do returns value to be cached.
	Do func(*Item) (any, error)

	// SetXX only sets the key if it already exists.
	SetXX bool

	// SetNX only sets the key if it does not already exist.
	SetNX bool

	// SkipLocalCache skips local cache as if it is not set.
	SkipLocalCache bool
}

func (item *Item) Context() context.Context {
	if item.Ctx == nil {
		return context.Background()
	}
	return item.Ctx
}

func (item *Item) value() (any, error) {
	if item.Do != nil {
		return item.Do(item)
	}
	if item.Value != nil {
		return item.Value, nil
	}
	return nil, nil
}

func (item *Item) ttl() time.Duration {
	const defaultTTL = time.Hour

	if item.TTL < 0 {
		return 0
	}

	if item.TTL != 0 {
		if item.TTL < time.Second {
			log.Printf("too short TTL for key=%q: %s", item.Key, item.TTL)
			return defaultTTL
		}
		return item.TTL
	}

	return defaultTTL
}

//------------------------------------------------------------------------------

type (
	MarshalFunc   func(any) ([]byte, error)
	UnmarshalFunc func([]byte, any) error
)

type Options struct {
	Redis        rediser
	LocalCache   LocalCache
	StatsEnabled bool
	Marshal      MarshalFunc
	Unmarshal    UnmarshalFunc
}

type Cache struct {
	redis rediser
	*cache
}

type cache struct {
	localCache LocalCache

	group singleflight.Group

	marshal   MarshalFunc
	unmarshal UnmarshalFunc

	statsEnabled bool
	hits         uint64
	misses       uint64
}

func New(opt *Options) *Cache {
	if opt.Marshal == nil {
		opt.Marshal = _marshal
	}
	if opt.Unmarshal == nil {
		opt.Unmarshal = _unmarshal
	}

	cacher := &Cache{
		redis: opt.Redis,
		cache: &cache{
			localCache:   opt.LocalCache,
			statsEnabled: opt.StatsEnabled,
			marshal:      opt.Marshal,
			unmarshal:    opt.Unmarshal,
		},
	}
	return cacher
}

type PipelinedCache struct {
	redis pipelinedRediser
	*cache
}

func (cd *Cache) Pipeline() (*PipelinedCache, error) {
	return cd.pipeline(rediser.Pipeline)
}

func (cd *Cache) TxPipeline() (*PipelinedCache, error) {
	return cd.pipeline(rediser.TxPipeline)
}

func (cd *Cache) pipeline(fn func(rediser) redis.Pipeliner) (*PipelinedCache, error) {
	if cd.redis == nil {
		return nil, errRedisNil
	}

	cacher := &PipelinedCache{
		redis: fn(cd.redis),
		cache: cd.cache,
	}
	return cacher, nil
}

// Set caches the item.
func (cd *Cache) Set(item *Item) error {
	_, _, err := set(cd.redis, cd.cache, item)
	return err
}

// Set caches the item.
func (cp *PipelinedCache) Set(item *Item) error {
	_, _, err := set(cp.redis, cp.cache, item)
	return err
}

func set(redis rediser, cache *cache, item *Item) ([]byte, bool, error) {
	value, err := item.value()
	if err != nil {
		return nil, false, err
	}

	b, err := cache.marshal(value)
	if err != nil {
		return nil, false, err
	}

	if cache.localCache != nil && !item.SkipLocalCache {
		cache.localCache.Set(item.Key, b)
	}

	if redis == nil {
		if cache.localCache == nil {
			return b, true, errRedisLocalCacheNil
		}
		return b, true, nil
	}

	ttl := item.ttl()
	if ttl == 0 {
		return b, true, nil
	}

	if item.SetXX {
		return b, true, redis.SetXX(item.Context(), item.Key, b, ttl).Err()
	}
	if item.SetNX {
		return b, true, redis.SetNX(item.Context(), item.Key, b, ttl).Err()
	}
	return b, true, redis.Set(item.Context(), item.Key, b, ttl).Err()
}

// Exists reports whether value for the given key exists.
func (cd *Cache) Exists(ctx context.Context, key string) bool {
	_, err := cd.getBytes(ctx, key, false)
	return err == nil
}

// Get gets the value for the given key.
func (cd *Cache) Get(ctx context.Context, dest any, key string) error {
	return cd.get(ctx, false, dest, key)
}

// GetSkippingLocalCache gets the value for the given key skipping local cache.
func (cd *Cache) GetSkippingLocalCache(
	ctx context.Context,
	dest any,
	key string,
) error {

	return cd.get(ctx, true, dest, key)
}

func (cd *Cache) get(
	ctx context.Context,
	skipLocalCache bool,
	dest any,
	key string,
) error {

	b, err := cd.getBytes(ctx, key, skipLocalCache)
	if err != nil {
		return err
	}
	return cd.unmarshal(b, dest)
}

func (cd *Cache) getBytes(ctx context.Context, key string, skipLocalCache bool) ([]byte, error) {
	if !skipLocalCache && cd.localCache != nil {
		b, ok := cd.localCache.Get(key)
		if ok {
			return b, nil
		}
	}

	if cd.redis == nil {
		if cd.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	b, err := cd.redis.Get(ctx, key).Bytes()
	if err != nil {
		if cd.statsEnabled {
			atomic.AddUint64(&cd.misses, 1)
		}
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		return nil, err
	}

	if cd.statsEnabled {
		atomic.AddUint64(&cd.hits, 1)
	}

	if !skipLocalCache && cd.localCache != nil {
		cd.localCache.Set(key, b)
	}
	return b, nil
}

// MGet gets the values for the given keys
// and puts them in a map[string]T
func (cd *Cache) MGet(ctx context.Context, dest any, keys ...string) error {
	return cd.mGet(ctx, true, dest, keys...)
}

// MGetSkippingChace gets the values for the given keys
// skipping local cache and puts them in a map[string]T
func (cd *Cache) MGetSkippingLocalCache(ctx context.Context, dest any, keys ...string) error {
	return cd.mGet(ctx, false, dest, keys...)
}

func (cd *Cache) mGet(
	ctx context.Context,
	skipLocalCache bool,
	destMap any,
	keys ...string,
) error {

	mapV := reflect.ValueOf(destMap).Elem()
	mapValT := mapV.Type().Elem()

	keysToB, err := cd.mGetBytes(ctx, skipLocalCache, keys...)
	if err != nil {
		return err
	}
	for key, b := range keysToB {
		val := reflect.New(mapValT).Interface()
		err := cd.unmarshal(b, val)
		if err != nil {
			return err
		}
		mapV.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(val).Elem())
	}

	return nil
}

func (cd *Cache) mGetBytes(
	ctx context.Context,
	skipLocalCache bool,
	keys ...string,
) (map[string][]byte, error) {

	var keysToB map[string][]byte
	if !skipLocalCache && cd.localCache != nil {
		keysToB = cd.localCache.MGet(keys)
		if len(keysToB) == len(keys) {
			return keysToB, nil
		}
		remainingKeys := make([]string, 0, len(keys)-len(keysToB))
		for _, key := range keys {
			_, ok := keysToB[key]
			if !ok {
				remainingKeys = append(remainingKeys, key)
			}
		}
		keys = remainingKeys
	} else {
		keysToB = make(map[string][]byte, len(keys))
	}

	if cd.redis == nil {
		if cd.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return keysToB, nil
	}

	vals, err := cd.redis.MGet(ctx, keys...).Result()
	if err != nil {
		if cd.statsEnabled {
			atomic.AddUint64(&cd.misses, uint64(len(keys)))
		}
		return nil, err
	}
	for i, val := range vals {
		if val == nil {
			continue
		}
		switch typedVal := val.(type) {
		case string:
			keysToB[keys[i]] = []byte(typedVal)
		case []byte:
			keysToB[keys[i]] = typedVal
		}
	}

	if cd.statsEnabled {
		atomic.AddUint64(&cd.hits, uint64(len(keysToB)))
	}

	if !skipLocalCache && cd.localCache != nil {
		cd.localCache.MSet(keysToB)
	}
	return keysToB, nil
}

// Once gets the item.Value for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Cache) Once(item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(item)
	if err != nil {
		return err
	}

	if item.Value == nil || len(b) == 0 {
		return nil
	}

	if err := cd.unmarshal(b, item.Value); err != nil {
		if cached {
			_ = cd.Delete(item.Context(), item.Key)
			return cd.Once(item)
		}
		return err
	}

	return nil
}

func (cd *Cache) getSetItemBytesOnce(item *Item) (b []byte, cached bool, err error) {
	if cd.localCache != nil {
		b, ok := cd.localCache.Get(item.Key)
		if ok {
			return b, true, nil
		}
	}

	v, err, _ := cd.group.Do(item.Key, func() (any, error) {
		b, err := cd.getBytes(item.Context(), item.Key, item.SkipLocalCache)
		if err == nil {
			cached = true
			return b, nil
		}

		b, ok, err := set(cd.redis, cd.cache, item)
		if ok {
			return b, nil
		}
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return v.([]byte), cached, nil
}

func (cd *Cache) Delete(ctx context.Context, key string) error {
	return delete(cd.redis, cd.cache, ctx, key)
}

func (cp *PipelinedCache) Delete(ctx context.Context, key string) error {
	return delete(cp.redis, cp.cache, ctx, key)
}

func delete(redis rediser, cache *cache, ctx context.Context, key string) error {
	if cache.localCache != nil {
		cache.localCache.Del(key)
	}

	if redis == nil {
		if cache.localCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	_, err := redis.Del(ctx, key).Result()
	return err
}

func (cd *Cache) DeleteFromLocalCache(key string) {
	if cd.localCache != nil {
		cd.localCache.Del(key)
	}
}

func (cd *PipelinedCache) Exec(ctx context.Context) error {
	_, err := cd.redis.(redis.Pipeliner).Exec(ctx)
	return err
}

func (cd *Cache) Marshal(value any) ([]byte, error) {
	return cd.marshal(value)
}

func _marshal(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return nil, err
	}

	return compress(b), nil
}

func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		n := len(data) + 1
		b := make([]byte, n, n+timeLen)
		copy(b, data)
		b[len(b)-1] = noCompression
		return b
	}

	n := s2.MaxEncodedLen(len(data)) + 1
	b := make([]byte, n, n+timeLen)
	b = s2.Encode(b, data)
	b = append(b, s2Compression)
	return b
}

func (cd *Cache) Unmarshal(b []byte, value any) error {
	return cd.unmarshal(b, value)
}

func _unmarshal(b []byte, value any) error {
	if len(b) == 0 {
		return nil
	}

	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]

		var err error
		b, err = s2.Decode(nil, b)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown compression method: %x", c)
	}

	return msgpack.Unmarshal(b, value)
}

//------------------------------------------------------------------------------

type Stats struct {
	Hits   uint64
	Misses uint64
}

// Stats returns cache statistics.
func (cd *Cache) Stats() *Stats {
	if !cd.statsEnabled {
		return nil
	}
	return &Stats{
		Hits:   atomic.LoadUint64(&cd.hits),
		Misses: atomic.LoadUint64(&cd.misses),
	}
}
