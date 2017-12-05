// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?	//lzh: 占用空间大小
  size_t key_length;
  uint32_t refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// lzh: hash table
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { 
	  Resize(); 
  }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  /************************************************************************/
  /* 
	lzh: 插入 h, 返回老的匹配节点. 注意, 如果相同 key, hash 的节点存在, 则 h 会插在
		它的前面, 并用 FindPointer 总是会返回首个匹配到的节点.
  */
  /************************************************************************/
  LRUHandle* Insert(LRUHandle* h) {
	  //lzh: 遍历 list_, 遍历停止于 key 和 hash 都命中. 返回最后遍历到的那个节点的 next_hash 字段的地址
	  //lzh: 注意, 如果是命中的停止, 则返回的
    LRUHandle** ptr = FindPointer(h->key(), h->hash);

    LRUHandle* old = *ptr;
	//lzh: 将 h 插在 old 前面. old 即使是 NULl 也能正确处理
    h->next_hash = (old == NULL ? NULL : old->next_hash);	//lzh: h 的 next_hash 设置为 *ptr
    *ptr = h;	//lzh: 参见 FindPointer 关于"精妙"那一段的注释: 
				//lzh: ptr 实际上是最后遍历到的节点的 next_hash 字段的地址.
				//lzh: 将 *ptr 设置为 h, 等价于将此节点的 next_hash 设置为 h, 也即设置它的下一个节点

    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;	//lzh: ptr 即是最后遍历到的那个节点的 next_hash 字段的地址. 
								//lzh: 当命中时, *ptr/result 的值刚好就是命中的节点的地址. 最后遍历到的节点即是命中节点的上一个节点
	if (result != NULL) {
      *ptr = result->next_hash;	//lzh: result->next_hash 即是命中节点的下一个节点的地址. 
								//lzh: *ptr=result->next_hash 即是设置命中节点的上一个节点的 next_hash 字段为 result->next_hash
								//lzh: 即实现了删除命中节点的功能
      --elems_;					
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
	 //lzh: HandleTable 由一组桶组成, 每个桶即是一个"由 hashed entry 组成的 linked list"
  uint32_t length_;		//lzh: 桶的个数
  uint32_t elems_;		//lzh: 元素的个数
  LRUHandle** list_;	//lzh: 桶地址数组

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.

  // lzh: 注意返回值, LRUHandle** : 存储着"节点地址"的地址. 那么, 哪儿会存储着这个 "节点地址" 呢,
  // lzh: 只有一个地方 -- LRUHandle 中 next_hash 成员. 此函数的精妙之处在于, 返回的就是这个成员的地址 ptr, 类型是 (LRUHandle**)
  // lzh: 直接赋值: *ptr = target; 就相当于设置了最后遍历到的那个节点的 next_hash 字段. 于是就可以实现链表的插入

  // lzh: 给出结论, 此函数返回的是 "遍历到的最后一个节点" (last_entry)的 next_hash 节点的地址.
  // lzh: 当有命中时此值刚好就是命中的那个节点

  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
	  //lzh: 根据 hash 找出当前的 key 处于哪个桶中. 
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
	
	//lzh: 当前桶就是一个 linked list, 依次遍历直到找出 hash 和 key 都相等的那个 entry
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }

	//lzh: 找到了, 或者返回的是一个无效的 entry
    return ptr;
  }

  void Resize() {

	  //lzh: 倍增空间
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
	
	//lzh: 生成 new_length 个桶
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;

	//lzh; 依次遍历每个桶, 复制
    for (uint32_t i = 0; i < length_; i++) {
		//lzh: 遍历当前的桶(linked list), 重新计算 hash 值
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        Slice key = h->key();
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
	//lzh: 孤陋寡闻了, 原来 delete NULL 是合法的 :(
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
/************************************************************************/
/* 
	lzh: 一个 LRU 缓存(leasy recently used, 最近最少使用) 
	lzh: lru_ 往左走(prev)是较新的节点(新的程度依序降低), 往右走(next) 是较旧的(旧的程度依序降低).
			lru_.prev 是最新的, lru_.next 是最旧的
*/
/************************************************************************/
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  port::Mutex mutex_;
  size_t usage_;
  uint64_t last_id_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0),
      last_id_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

/************************************************************************/
/* 
	lzh: 每次插入都是插在 lru_ 的左边(prev), 即 lru_ 是一个圆形的链表, 
			lru_ 往左走(prev)是较新的节点(新的程度依序降低), 往右走(next) 是较旧的(旧的程度依序降低).
			lru_.prev 是最新的, lru_.next 是最旧的
*/
/************************************************************************/
void LRUCache::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  //lzh: 不使用 new 去构造而使用更原始的 malloc 分配空间可以提高运行效率
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));	//lzh: LRUHandle 最后一个成员的定义是 char key_data[1]. 此编程技法可参考 skiplist 的定义
													//lzh: 此处需要减去 1 个字节再加上 key.size
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  LRU_Append(e);
  usage_ += charge;

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
		//lzh: 注意这里没有调用 table_.Remove(old->key(), old->hash); 是因为 table_ 较后插入的节点总是在前面, 
		//lzh: 而 table_ 中查找时也总是优先返回前面的. 这样做的好处是, table_ 中删除了较新的节点后, 还可查到
		//lzh: key, hash 一样的较旧节点
    LRU_Remove(old);
    Unref(old);
  }

  //lzh: 空间过多时, 依次删除较旧的节点
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;		//lzh: lru_ 它是一个圆形的链表, prev 总是最新的, next 最旧
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    Unref(old);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

/************************************************************************/
/* 
	lzh: 分片的 LRU 缓存
*/
/************************************************************************/
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  //lzh: 计算 s 的 hash 值
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  //lzh: 只保留 hash 的高 kNumShardBits 位, 返回值的范围是 0 到 (1<<kNumShardsBits)-1 之间.
  //lzh: 刚好可以映射到 shard_ 的域
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity) {
	  //lzh: 计算每个分片的容量
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;	//lzh: 考虑到除法运算会省去余数, 所以此处需要加上 kNumShards-1
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}
