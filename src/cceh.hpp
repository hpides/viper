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

#define requires_fingerprint(KeyType) \
    std::is_same_v<KeyType, std::string> || sizeof(KeyType) > 8


template <typename KeyType>
inline bool CAS(KeyType* key, KeyType* expected, KeyType updated) {
    if constexpr (sizeof(KeyType) == 1) return internal_cas((int8_t *) key, (int8_t *) expected, (int8_t ) updated);
    else if constexpr (sizeof(KeyType) == 2) return internal_cas((int16_t*) key, (int16_t*) expected, (int16_t ) updated);
    else if constexpr (sizeof(KeyType) == 4) return internal_cas((int32_t*) key, (int32_t*) expected, (int32_t ) updated);
    else if constexpr (sizeof(KeyType) == 8) return internal_cas((int64_t*) key, (int64_t*) expected, (int64_t ) updated);
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

#define INPLACE 1
#define CACHE_LINE_SIZE 64

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits)-1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 4;

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
    int64_t sema = 0;
    size_t pattern = 0;
};

template <typename KeyType>
struct Directory {
    static const size_t kDefaultDepth = 10;
    Segment<KeyType>** _;
    size_t capacity;
    size_t depth;
    bool lock;
    int sema = 0 ;

    Directory(void) {
        depth = kDefaultDepth;
        capacity = pow(2, depth);
        _ = new Segment<KeyType>*[capacity];
        lock = false;
        sema = 0;
    }

    Directory(size_t _depth) {
        depth = _depth;
        capacity = pow(2, depth);
        _ = new Segment<KeyType>*[capacity];
        lock = false;
        sema = 0;
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

    void SanityCheck(void*);
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
};

extern size_t perfCounter;

template <typename KeyType>
template <typename KeyCheckFn>
int Segment<KeyType>::Insert(const KeyType& key, IndexV value, size_t loc, size_t key_hash,
                             IndexV* old_entry, KeyCheckFn key_check_fn) {
  const size_t pattern_shift = 8 * sizeof(key_hash) - local_depth;

  if (sema == -1) return 2;
  if ((key_hash >> pattern_shift) != pattern) return 2;

  auto lock = sema;
  int ret = 1;
  while (!CAS(&sema, &lock, lock+1)) {
    lock = sema;
  }

  IndexK LOCK = INVALID;
  constexpr bool using_fp = requires_fingerprint(KeyType);
  IndexK key_checker;
  if constexpr (using_fp) {
      key_checker = key_hash;
  } else {
      key_checker = *reinterpret_cast<const IndexK*>(&key);
  }

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    auto _key = _[slot].key;

    if (i == 15) {
        std::cout << "FULL\n";
    }

    if constexpr (using_fp) {
        if (_key != INVALID && (_[slot].key >> pattern_shift) != pattern) {
            // Key hash does not match this segment anymore.
            CAS(&_[slot].key, &_key, INVALID);
        }
    } else {
        if (_key != INVALID && (h(&_[slot].key, sizeof(IndexK)) >> pattern_shift) != pattern) {
            // Key hash does not match this segment anymore.
            CAS(&_[slot].key, &_key, INVALID);
        }
    }

    if ((IndexK) __atomic_load_n(&_[slot].key, __ATOMIC_ACQUIRE) == key_checker) {
        if constexpr (using_fp) {
            // FPs matched but not necessarily the actual key.
            const bool keys_match = key_check_fn(key, _[slot].value);
            if (!keys_match) {
                continue;
            }
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
    }

    if (CAS(&_[slot].key, &LOCK, SENTINEL)) {
        IndexV old_value = _[slot].value;
        old_value.is_free = true;
        while (!CAS(&_[slot].value.offset, &old_value.offset, value.offset)) {
            // Cannot swap value if it is in use
            old_value.is_free = true;
        }
        old_entry->offset = old_value.offset;
        mfence();
        _[slot].key = key_checker;
        ret = 0;
        break;
    } else {
        LOCK = INVALID;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  if (ret == 1) {
      std::cout << "BAD RET == 1" << std::endl;
  }

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
    using namespace std;
    int64_t lock = 0;
    if (!CAS(&sema, &lock, -1l)) return nullptr;

    Segment<KeyType>** split = new Segment<KeyType>*[2];
    split[0] = this;
    split[1] = new Segment<KeyType>(local_depth+1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    auto key_hash = h(&_[i].key, sizeof(IndexK));
    if (key_hash & ((size_t) 1 << ((sizeof(IndexK)*8 - local_depth - 1)))) {
      split[1]->Insert4split
        (_[i].key, _[i].value, (key_hash & kMask)*kNumPairPerCacheLine);
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
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto x = (key_hash >> (8*sizeof(key_hash)-dir->depth));
    auto target = dir->_[x];
    IndexV old_entry{};
    auto ret = target->Insert(key, value, y, key_hash, &old_entry, key_check_fn);

    if (ret == 0) {
        clflush((char*)&dir->_[x]->_[y], 64);
        return old_entry;
    } else if (ret == 2) {
        goto RETRY;
    }

    Segment<KeyType>** s = target->Split();
    if (s == nullptr) {
        // another thread is doing split
        goto RETRY;
    }

    std::cout << "RESIZE!\n";
    s[0]->pattern = (key_hash >> (8*sizeof(key_hash)-s[0]->local_depth+1)) << 1;
    s[1]->pattern = ((key_hash >> (8*sizeof(key_hash)-s[1]->local_depth+1)) << 1) + 1;

    // Directory management
    while (!dir->Acquire()) {
        asm("nop");
    }
    { // CRITICAL SECTION - directory update
        x = (key_hash >> (8*sizeof(key_hash)-dir->depth));
        if (dir->_[x]->local_depth-1 < dir->depth) {  // normal split
            unsigned depth_diff = dir->depth - s[0]->local_depth;
            if (depth_diff == 0) {
                if (x%2 == 0) {
                    dir->_[x+1] = s[1];
                    clflush((char*) &dir->_[x+1], 8);
                } else {
                    dir->_[x] = s[1];
                    clflush((char*) &dir->_[x], 8);
                }
            } else {
                int chunk_size = pow(2, dir->depth - (s[0]->local_depth - 1));
                x = x - (x % chunk_size);
                for (unsigned i = 0; i < chunk_size/2; ++i) {
                    dir->_[x+chunk_size/2+i] = s[1];
                }
                clflush((char*)&dir->_[x+chunk_size/2], sizeof(void*)*chunk_size/2);
            }
            while (!dir->Release()) {
                asm("nop");
            }
        } else {  // directory doubling
            auto dir_old = dir;
            auto d = dir->_;
            auto _dir = new Directory<KeyType>(dir->depth+1);
            for (unsigned i = 0; i < dir->capacity; ++i) {
                if (i == x) {
                    _dir->_[2*i] = s[0];
                    _dir->_[2*i+1] = s[1];
                } else {
                    _dir->_[2*i] = d[i];
                    _dir->_[2*i+1] = d[i];
                }
            }
            clflush((char*)&_dir->_[0], sizeof(Segment<KeyType>*)*_dir->capacity);
            clflush((char*)&_dir, sizeof(Directory<KeyType>));
            dir = _dir;
            clflush((char*)&dir, sizeof(void*));
            delete dir_old;
        }
        s[0]->sema = 0;
    }  // End of critical section
    goto RETRY;
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

    auto x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

    auto dir_ = dir->_[x];

#ifdef INPLACE
    auto sema = dir->_[x]->sema;
    while (!CAS(&dir->_[x]->sema, &sema, sema+1)) {
      sema = dir->_[x]->sema;
    }
#endif


    IndexK key_checker;
    constexpr bool using_fp = requires_fingerprint(KeyType);
    if constexpr (using_fp) {
        IndexK fingerprint;
        if constexpr (std::is_same_v<KeyType, std::string>) fingerprint = murmur2(key.data(), key.length());
        else if constexpr(sizeof(key) > 8) fingerprint = murmur2(&key, sizeof(KeyType));
        key_checker = fingerprint;
    } else {
        key_checker = *reinterpret_cast<const IndexK*>(&key);
    }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (y+i) % Segment<KeyType>::kNumSlot;
        if (dir_->_[slot].key == key_checker) {
#ifdef INPLACE
          sema = dir->_[x]->sema;
          while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
            sema = dir->_[x]->sema;
          }
#endif
          if constexpr (using_fp) {
              const bool keys_match = key_check_fn(key, dir_->_[slot].value);
              if (!keys_match) continue;
          }

          accessor.take(&(dir_->_[slot].value));
          return true;
        }
    }

#ifdef INPLACE
    sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
    sema = dir->_[x]->sema;
  }
#endif
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

template <typename KeyType>
void Directory<KeyType>::SanityCheck(void* addr) {
    using namespace std;
    for (unsigned i = 0; i < capacity; ++i) {
        if (_[i] == addr) {
            cout << i << " " << _[i]->sema << endl;
            exit(1);
        }
    }
}

}  // namespace cceh
}  // namespace viper
