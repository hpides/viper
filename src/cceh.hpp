/**
 * This code was taken and modified from https://github.com/DICL/CCEH, the original authors of CCEH.
 */

#pragma once

#include <cstring>
#include <cmath>
#include <vector>
#include <stdint.h>
#include <iostream>
#include <cmath>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <atomic>

#include "hash.hpp"

namespace viper {

#define internal_cas(entry, expected, updated) \
    __atomic_compare_exchange_n(entry, expected, updated, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)

#define ATOMIC_LOAD(addr) \
    __atomic_load_n(addr, __ATOMIC_ACQUIRE)

#define requires_fingerprint(KeyType) \
    std::is_same_v<KeyType, std::string> || sizeof(KeyType) > 8


template <typename KeyType>
inline bool CAS(KeyType* key, KeyType* expected, KeyType updated) {
    if constexpr (sizeof(KeyType) == 1) return internal_cas((int8_t*) key, (int8_t*) expected, (int8_t) updated);
    else if constexpr (sizeof(KeyType) == 2) return internal_cas((int16_t*) key, (int16_t*) expected, (int16_t) updated);
    else if constexpr (sizeof(KeyType) == 4) return internal_cas((int32_t*) key, (int32_t*) expected, (int32_t) updated);
    else if constexpr (sizeof(KeyType) == 8) return internal_cas((int64_t*) key, (int64_t*) expected, (int64_t) updated);
    else if constexpr (sizeof(KeyType) == 16) return internal_cas((__int128*) key, (__int128*) expected, (__int128) updated);
    else throw std::runtime_error("CAS not supported for > 16 bytes!");
}


using offset_size_t = uint64_t;
using block_size_t = uint64_t;
using page_size_t = uint8_t;
using data_offset_size_t = uint16_t;

struct KeyValueOffset {
    static constexpr offset_size_t INVALID = 0xFFFFFFFFFFFFFFFF;

    union {
        offset_size_t offset;
        struct {
            bool is_free : 1;
            block_size_t block_number : 44;
            page_size_t page_number : 3;
            data_offset_size_t data_offset : 16;
        };
    };

    KeyValueOffset() : offset{INVALID} {}

    static KeyValueOffset NONE() { return KeyValueOffset{INVALID}; }

    explicit KeyValueOffset(const offset_size_t offset) : offset(offset) {}

    KeyValueOffset(const block_size_t block_number, const page_size_t page_number, const data_offset_size_t slot)
        : is_free{true}, block_number{block_number}, page_number{page_number}, data_offset{slot} {}

    static KeyValueOffset Tombstone() {
        return KeyValueOffset{};
    }

    inline std::tuple<block_size_t, page_size_t, data_offset_size_t> get_offsets() const {
        return {block_number, page_number, data_offset};
    }

    inline bool is_tombstone() const {
        return offset == INVALID;
    }
};

using IndexK = size_t;
using IndexV = KeyValueOffset;
constexpr IndexK SENTINEL = -2; // 11111...110
constexpr IndexK INVALID = -1; // 11111...111

namespace cceh {

#define CACHE_LINE_SIZE 64

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits)-1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 4;

constexpr uint64_t SPLIT_REQUEST_BIT = 1ul << 63;
constexpr uint64_t EXCLUSIVE_LOCK = -1;

template <typename KeyType = IndexK>
struct Pair {
    KeyType key;
    IndexV value;

    Pair(void) : key{INVALID}, value{IndexV::Tombstone()} {}

    Pair(KeyType _key, IndexV _value) : key{_key}, value{_value} {}

    Pair& operator=(const Pair& other) {
        key = other.key;
        value = other.value;
        return *this;
    }

  void* operator new(size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new[](size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

struct CcehAccessor {
  IndexV* offset;
  bool has_lock;
  bool found;

  CcehAccessor() : offset{nullptr}, has_lock{false}, found{false} {}
  ~CcehAccessor() { release(); }

  inline void take(IndexV* value) {
      offset = value;
      found = offset != nullptr && !offset->is_tombstone();
      if (found) acquire();
  }

  IndexV* operator->() { return offset; }
  IndexV& operator*() { return *offset; }

  inline void acquire() {
      IndexV* expected = offset;
      expected->is_free = true;
      IndexV* locked = offset;
      locked->is_free = false;
      while (!CAS(&offset->offset, &expected->offset, locked->offset)) {
          expected->is_free = true;
      }
      has_lock = true;
  }

  inline void release() {
    if (has_lock) {
        IndexV* expected = offset;
        expected->is_free = false;
        IndexV* unlocked = offset;
        unlocked->is_free = true;
        CAS(&offset->offset, &expected->offset, unlocked->offset);
        has_lock = false;
    }
  }
};

static inline void CPUPause(void) {
    __asm__ volatile("pause":::"memory");
}

static inline unsigned long ReadTSC(void) {
    unsigned long var;
    unsigned int hi, lo;
    asm volatile("rdtsc":"=a"(lo),"=d"(hi));
    var = ((unsigned long long int) hi << 32) | lo;
    return var;
}

inline void mfence(void) {
#ifdef CCEH_PERSISTENT
    asm volatile("mfence":::"memory");
#endif
}

inline void clflush(char* data, size_t len) {
#ifdef CCEH_PERSISTENT
    volatile char *ptr = (char*)((unsigned long)data & (~(CACHE_LINE_SIZE-1)));
  mfence();
  for (; ptr < data+len; ptr+=CACHE_LINE_SIZE) {
    asm volatile("clflush %0" : "+m" (*(volatile char*)ptr));
  }
  mfence();
#endif
}

template <typename KeyType>
struct Segment {
    static const size_t kNumSlot = kSegmentSize / sizeof(Pair<KeyType>);

    Segment(void)
        : local_depth{0}
    { }

    Segment(size_t depth)
        :local_depth{depth}
    { }

    ~Segment(void) {
    }

    void* operator new(size_t size) {
        void* ret;
        posix_memalign(&ret, 64, size);
        return ret;
    }

    void* operator new[](size_t size) {
        void* ret;
        posix_memalign(&ret, 64, size);
        return ret;
    }

    template <typename KeyCheckFn>
    int Insert(const KeyType&, IndexV, size_t, size_t, IndexV* old_entry, KeyCheckFn);

    void Insert4split(IndexK, IndexV, size_t);
    Segment** Split(void);

    Pair<IndexK> _[kNumSlot];
    size_t local_depth;
    std::atomic<uint64_t> sema = 0;
    size_t pattern = 0;
    static constexpr bool using_fp_ = requires_fingerprint(KeyType);
};

template <typename KeyType>
struct Directory {
    static const size_t kDefaultDepth = 10;
    Segment<KeyType>** _;
    size_t capacity;
    size_t depth;
    bool lock;

    Directory(void) {
        depth = kDefaultDepth;
        capacity = pow(2, depth);
        _ = new Segment<KeyType>*[capacity];
        lock = false;
    }

    Directory(size_t _depth) {
        depth = _depth;
        capacity = pow(2, depth);
        _ = new Segment<KeyType>*[capacity];
        lock = false;
    }

    ~Directory(void) {
        delete [] _;
    }

    bool Acquire(void) {
        bool unlocked = false;
        return CAS(&lock, &unlocked, true);
    }

    bool Release(void) {
        bool locked = true;
        return CAS(&lock, &locked, false);
    }
};

template <typename KeyType>
class CCEH {
  public:
    static constexpr auto dummy_key_check = [](const KeyType&, IndexV) {
        throw std::runtime_error("Dummy key check should never be used!");
        return true;
    };

    CCEH(void);
    CCEH(size_t);
    ~CCEH(void);

    template <typename KeyCheckFn>
    IndexV Insert(const KeyType&, IndexV, KeyCheckFn);

    template <typename KeyCheckFn>
    bool Get(const KeyType&, CcehAccessor& accessor, KeyCheckFn);

    IndexV Insert(const KeyType&, IndexV);
    bool Delete(const KeyType&);
    bool Get(const KeyType&, CcehAccessor& accessor);
    size_t Capacity(void);
    bool Recovery(void);

    void* operator new(size_t size) {
        void *ret;
        posix_memalign(&ret, 64, size);
        return ret;
    }

  private:
    Directory<KeyType>* dir;
    static constexpr bool using_fp_ = requires_fingerprint(KeyType);
};

extern size_t perfCounter;

template <typename KeyType>
template <typename KeyCheckFn>
int Segment<KeyType>::Insert(const KeyType& key, IndexV value, size_t loc, size_t key_hash,
                             IndexV* old_entry, KeyCheckFn key_check_fn) {
  uint64_t lock = sema.load();
  if (lock == EXCLUSIVE_LOCK) return 2;
  if ((lock & SPLIT_REQUEST_BIT) != 0) return 1;

  const size_t pattern_shift = 8 * sizeof(key_hash) - local_depth;
  if ((key_hash >> pattern_shift) != pattern) return 2;

  int ret = 1;
  while (!sema.compare_exchange_weak(lock, lock+1)) {
      if (lock == EXCLUSIVE_LOCK) return 2;
      if ((lock & SPLIT_REQUEST_BIT) != 0) return 1;
  }

  IndexK LOCK = INVALID;
  IndexK key_checker;
  if constexpr (using_fp_) {
      key_checker = key_hash;
  } else {
      key_checker = *reinterpret_cast<const IndexK*>(&key);
  }

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    auto _key = _[slot].key;

    bool invalidate = _key != INVALID;
    if constexpr (using_fp_) {
        invalidate &= (_key >> pattern_shift) != pattern;
    } else {
        invalidate &= (h(&_key, sizeof(IndexK)) >> pattern_shift) != pattern;
    }

    if (invalidate && CAS(&_[slot].key, &_key, INVALID)) {
        _[slot].value = IndexV::Tombstone();
    }

    if (CAS(&_[slot].key, &LOCK, SENTINEL)) {
        old_entry->offset = _[slot].value.offset;
        _[slot].value = value;
        _[slot].key = key_checker;
        mfence();
        ret = 0;
        break;
    } else if (ATOMIC_LOAD(&_[slot].key) == key_checker) {
        if constexpr (using_fp_) {
            // FPs matched but not necessarily the actual key.
            const bool keys_match = key_check_fn(key, _[slot].value);
            if (!keys_match) continue;
        }

        IndexV old_value = _[slot].value;
        old_value.is_free = true;
        while (!CAS(&_[slot].value.offset, &old_value.offset, value.offset)) {
            // Cannot swap value if it is in use
            old_value.is_free = true;
        }
        old_entry->offset = old_value.offset;
        mfence();
        ret = 0;
        break;
    } else {
        LOCK = INVALID;
    }
  }

  sema.fetch_sub(1);
  return ret;
}
template <typename KeyType>
void Segment<KeyType>::Insert4split(IndexK key, IndexV value, size_t loc) {
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc+i) % kNumSlot;
        if (_[slot].key == INVALID) {
            _[slot].key = key;
            _[slot].value = value;
            return;
        }
    }
}

template <typename KeyType>
Segment<KeyType>** Segment<KeyType>::Split(void) {
  uint64_t lock = 0;
  if (!sema.compare_exchange_strong(lock, EXCLUSIVE_LOCK)) {
      if (lock == EXCLUSIVE_LOCK) {
          return nullptr;
      }

      lock = SPLIT_REQUEST_BIT;
      if (!sema.compare_exchange_strong(lock, EXCLUSIVE_LOCK)) {
          if ((lock & SPLIT_REQUEST_BIT) != 0) {
              return nullptr;
          }
          sema.compare_exchange_strong(lock, lock | SPLIT_REQUEST_BIT);
          return nullptr;
      }
  }

  Segment<KeyType>** split = new Segment<KeyType>*[2];
  split[0] = this;
  split[1] = new Segment<KeyType>(local_depth + 1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    size_t key_hash;
    if constexpr (using_fp_) {
        key_hash = _[i].key;
    } else {
        key_hash = h(&_[i].key, sizeof(IndexK));
    }
    if (key_hash & ((size_t) 1 << ((sizeof(IndexK)*8 - local_depth - 1)))) {
      while (!IndexV{ATOMIC_LOAD(&_[i].value.offset)}.is_free) {
          asm("nop");
      }
      split[1]->Insert4split(_[i].key, _[i].value, (key_hash & kMask)*kNumPairPerCacheLine);
    }
  }

  clflush((char*)split[1], sizeof(Segment));
  local_depth = local_depth + 1;
  clflush((char*)&local_depth, sizeof(size_t));

  return split;
}

template <typename KeyType>
CCEH<KeyType>::CCEH(void)
    : dir{new Directory<KeyType>(0)}
{
    for (unsigned i = 0; i < dir->capacity; ++i) {
        dir->_[i] = new Segment<KeyType>(0);
        dir->_[i]->pattern = i;
    }
}

template <typename KeyType>
CCEH<KeyType>::CCEH(size_t initCap)
    : dir{new Directory<KeyType>(static_cast<size_t>(log2(initCap)))}
{
    for (unsigned i = 0; i < dir->capacity; ++i) {
        dir->_[i] = new Segment<KeyType>(static_cast<size_t>(log2(initCap)));
        dir->_[i]->pattern = i;
    }
}

template <typename KeyType>
CCEH<KeyType>::~CCEH(void)
{ }

template <typename KeyType>
IndexV CCEH<KeyType>::Insert(const KeyType& key, IndexV value) {
    return Insert(key, value, dummy_key_check);
}

template <typename KeyType>
template <typename KeyCheckFn>
IndexV CCEH<KeyType>::Insert(const KeyType& key, IndexV value, KeyCheckFn key_check_fn) {
    size_t key_hash;
    if constexpr (std::is_same_v<KeyType, std::string>) { key_hash = h(key.data(), key.length()); }
    else { key_hash = h(&key, sizeof(key)); }
    auto loc = (key_hash & kMask) * kNumPairPerCacheLine;

    while (true) {
        auto x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
        auto target = dir->_[x];
        IndexV old_entry{};
        auto ret = target->Insert(key, value, loc, key_hash, &old_entry, key_check_fn);

        if (ret == 0) {
            clflush((char*) &dir->_[x]->_[loc], 64);
            return old_entry;
        } else if (ret == 2) {
            continue;
        }

        // Segment is full, need to split.
        Segment<KeyType>** s = target->Split();
        if (s == nullptr) {
            // another thread is doing split
            continue;
        }

        s[0]->pattern = (key_hash >> (8 * sizeof(key_hash) - s[0]->local_depth + 1)) << 1;
        s[1]->pattern = ((key_hash >> (8 * sizeof(key_hash) - s[1]->local_depth + 1)) << 1) + 1;

        // Directory management
        while (!dir->Acquire()) {
            asm("nop");
        }

        { // CRITICAL SECTION - directory update
            x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
            if (dir->_[x]->local_depth - 1 < dir->depth) {  // normal split
                unsigned depth_diff = dir->depth - s[0]->local_depth;
                if (depth_diff == 0) {
                    if (x % 2 == 0) {
                        dir->_[x + 1] = s[1];
                        clflush((char*) &dir->_[x + 1], 8);
                    } else {
                        dir->_[x] = s[1];
                        clflush((char*) &dir->_[x], 8);
                    }
                } else {
                    int chunk_size = pow(2, dir->depth - (s[0]->local_depth - 1));
                    x = x - (x % chunk_size);
                    for (unsigned i = 0; i < chunk_size / 2; ++i) {
                        dir->_[x + chunk_size / 2 + i] = s[1];
                    }
                    clflush((char*) &dir->_[x + chunk_size / 2], sizeof(void*) * chunk_size / 2);
                }
                dir->Release();
            } else {  // directory doubling
                auto dir_old = dir;
                auto d = dir->_;
                auto _dir = new Directory<KeyType>(dir->depth + 1);
                for (unsigned i = 0; i < dir->capacity; ++i) {
                    if (i == x) {
                        _dir->_[2 * i] = s[0];
                        _dir->_[2 * i + 1] = s[1];
                    } else {
                        _dir->_[2 * i] = d[i];
                        _dir->_[2 * i + 1] = d[i];
                    }
                }
                clflush((char*) &_dir->_[0], sizeof(Segment < KeyType > *) * _dir->capacity);
                clflush((char*) &_dir, sizeof(Directory < KeyType > ));
                if (!CAS(&dir, &dir_old, _dir)) {
                    throw std::runtime_error("Could not swap dirs. This should never happen!");
                }
                clflush((char*) &dir, sizeof(void*));
                delete dir_old;
            }
            s[0]->sema.store(0);
        }  // End of critical section
    }
}

// TODO
template <typename KeyType>
bool CCEH<KeyType>::Delete(const KeyType& key) {
    return false;
}

template <typename KeyType>
bool CCEH<KeyType>::Get(const KeyType& key, CcehAccessor& accessor) {
    return Get(key, accessor, dummy_key_check);
}

template <typename KeyType>
template <typename KeyCheckFn>
bool CCEH<KeyType>::Get(const KeyType& key, CcehAccessor& accessor, KeyCheckFn key_check_fn) {
    size_t key_hash;
    if constexpr (std::is_same_v<KeyType, std::string>) { key_hash = h(key.data(), key.length()); }
    else { key_hash = h(&key, sizeof(key)); }
    const size_t loc = (key_hash & kMask) * kNumPairPerCacheLine;

    Segment<KeyType>* segment;
    while (true) {
        const size_t seg_num = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
        segment = dir->_[seg_num];
        auto& sema = segment->sema;

        uint64_t lock = sema.load();
        if ((lock & SPLIT_REQUEST_BIT) != 0 || !sema.compare_exchange_weak(lock, lock + 1)) {
            continue;
        }

        // Could acquire lock
        break;
    }

    IndexK key_checker;
    if constexpr (using_fp_) {
        key_checker = key_hash;
    } else {
        key_checker = *reinterpret_cast<const IndexK*>(&key);
    }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc+i) % Segment<KeyType>::kNumSlot;
        if (segment->_[slot].key == key_checker) {
          if constexpr (using_fp_) {
              const bool keys_match = key_check_fn(key, segment->_[slot].value);
              if (!keys_match) continue;
          }

          accessor.take(&(segment->_[slot].value));
          segment->sema.fetch_sub(1);
          return true;
        }
    }

    segment->sema.fetch_sub(1);
    return false;
}

template <typename KeyType>
size_t CCEH<KeyType>::Capacity(void) {
    std::unordered_map<Segment<KeyType>*, bool> set;
    for (size_t i = 0; i < dir->capacity; ++i) {
        set[dir->_[i]] = true;
    }
    return set.size() * Segment<KeyType>::kNumSlot;
}

template <typename KeyType>
bool CCEH<KeyType>::Recovery(void) {
    bool recovered = false;
    size_t i = 0;
    while (i < dir->capacity) {
        size_t depth_cur = dir->_[i]->local_depth;
        size_t stride = pow(2, dir->depth - depth_cur);
        size_t buddy = i + stride;
        if (buddy == dir->capacity) break;
        for (int j = buddy - 1; i < j; j--) {
            if (dir->_[j]->local_depth != depth_cur) {
                dir->_[j] = dir->_[i];
            }
        }
        i = i+stride;
    }
    if (recovered) {
        clflush((char*)&dir->_[0], sizeof(void*)*dir->capacity);
    }
    return recovered;
}

}  // namespace cceh
}  // namespace viper
