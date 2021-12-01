/*
 * The code is part of the XIndex project.
 *
 *    Copyright (C) 2020 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about XIndex, visit:
 *     https://ppopp20.sigplan.org/details/PPoPP-2020-papers/13/XIndex-A-Scalable-Learned-Index-for-Multicore-Data-Storage
 */

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>

#if !defined(XINDEX_UTIL_H)
#define XINDEX_UTIL_H

namespace xindex {

static const size_t desired_training_key_n = 10000000;
static const size_t max_model_n = 4;
static const size_t seq_insert_reserve_factor = 2;

struct alignas(CACHELINE_SIZE) RCUStatus;
enum class Result;
struct alignas(CACHELINE_SIZE) BGInfo;
struct IndexConfig;

typedef RCUStatus rcu_status_t;
typedef Result result_t;
typedef BGInfo bg_info_t;
typedef IndexConfig index_config_t;

struct RCUStatus {
  std::atomic<int64_t> status;
  std::atomic<bool> waiting;
};
enum class Result { ok, failed, retry };
struct BGInfo {
  size_t bg_i;  // for calculation responsible range
  size_t bg_n;  // for calculation responsible range
  volatile void *root_ptr;
  volatile bool should_update_array;
  std::atomic<bool> started;
  std::atomic<bool> finished;
  volatile bool running;
};
struct IndexConfig {
  double root_error_bound = 32;
  double root_memory_constraint = 1024 * 1024;
  double group_error_bound = 32;
  double group_error_tolerance = 4;
  size_t buffer_size_bound = 256;
  double buffer_size_tolerance = 3;
  size_t buffer_compact_threshold = 8;
  size_t worker_n = 0;
  std::unique_ptr<rcu_status_t[]> rcu_status;
  volatile bool exited = false;
};

index_config_t config;
std::mutex config_mutex;

// TODO replace it with user space RCU (e.g., qsbr)
void rcu_init() {
  config_mutex.lock();
  if (config.rcu_status.get() == nullptr) {
    config.rcu_status = std::make_unique<rcu_status_t[]>(config.worker_n);
    for (size_t worker_i = 0; worker_i < config.worker_n; worker_i++) {
      config.rcu_status[worker_i].status = 0;
      config.rcu_status[worker_i].waiting = false;
    }
  }
  config_mutex.unlock();
}

void rcu_progress(const uint32_t worker_id) {
  config.rcu_status[worker_id].status++;
}

// wait for all workers
void rcu_barrier() {
  int64_t prev_status[config.worker_n];
  for (size_t w_i = 0; w_i < config.worker_n; w_i++) {
    prev_status[w_i] = config.rcu_status[w_i].status;
  }
  for (size_t w_i = 0; w_i < config.worker_n; w_i++) {
    while (config.rcu_status[w_i].status <= prev_status[w_i] && !config.exited)
      ;
  }
}

// wait for workers whose 'waiting' is false
void rcu_barrier(const uint32_t worker_id) {
  // set myself to waiting for barrier
  config.rcu_status[worker_id].waiting = true;

  int64_t prev_status[config.worker_n];
  for (size_t w_i = 0; w_i < config.worker_n; w_i++) {
    prev_status[w_i] = config.rcu_status[w_i].status;
  }
  for (size_t w_i = 0; w_i < config.worker_n; w_i++) {
    // skipped workers that is wating for barrier (include myself)
    while (config.rcu_status[w_i].status <= prev_status[w_i] &&
           !config.rcu_status[w_i].waiting && !config.exited)
      ;
  }
  config.rcu_status[worker_id].waiting = false;  // restore my state
}

template <class val_t>
struct AtomicVal {
  union ValUnion;
  typedef ValUnion val_union_t;
  typedef val_t value_type;
  union ValUnion {
    val_t val;
    AtomicVal *ptr;
    ValUnion() {}
    ValUnion(val_t val) : val(val) {}
    ValUnion(AtomicVal *ptr) : ptr(ptr) {}
  };

  // 60 bits for version
  static const uint64_t version_mask = 0x0fffffffffffffff;
  static const uint64_t lock_mask = 0x1000000000000000;
  static const uint64_t removed_mask = 0x2000000000000000;
  static const uint64_t pointer_mask = 0x4000000000000000;

  val_union_t val;
  // lock - removed - is_ptr
  volatile uint64_t status;

  AtomicVal() : status(0) {}
  AtomicVal(val_t val) : val(val), status(0) {}
  AtomicVal(AtomicVal *ptr) : val(ptr), status(0) { set_is_ptr(); }

  bool is_ptr(uint64_t status) { return status & pointer_mask; }
  bool removed(uint64_t status) { return status & removed_mask; }
  bool locked(uint64_t status) { return status & lock_mask; }
  uint64_t get_version(uint64_t status) { return status & version_mask; }

  void set_is_ptr() { status |= pointer_mask; }
  void unset_is_ptr() { status &= ~pointer_mask; }
  void set_removed() { status |= removed_mask; }
  void lock() {
    while (true) {
      uint64_t old = status;
      uint64_t expected = old & ~lock_mask;  // expect to be unlocked
      uint64_t desired = old | lock_mask;    // desire to lock
      if (likely(cmpxchg((uint64_t *)&this->status, expected, desired) ==
                 expected)) {
        return;
      }
    }
  }
  void unlock() { status &= ~lock_mask; }
  void incr_version() {
    uint64_t version = get_version(status);
    UNUSED(version);
    status++;
    assert(get_version(status) == version + 1);
  }

  friend std::ostream &operator<<(std::ostream &os, const AtomicVal &leaf) {
    COUT_VAR(leaf.val.val);
    COUT_VAR(leaf.val.ptr);
    COUT_VAR(leaf.is_ptr);
    COUT_VAR(leaf.removed);
    COUT_VAR(leaf.locked);
    COUT_VAR(leaf.verion);
    return os;
  }

  // semantics: atomically read the value and the `removed` flag
  bool read(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();

      uint64_t current_status = this->status;
      memory_fence();

      if (unlikely(locked(current_status))) {  // check lock
        continue;
      }

      if (likely(get_version(status) ==
                 get_version(current_status))) {  // check version
        if (unlikely(is_ptr(status))) {
          assert(!removed(status));
          return val_union.ptr->read(val);
        } else {
          val = val_union.val;
          return !removed(status);
        }
      }
    }
  }
  bool update(const val_t &val) {
    lock();
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->update(val);
    } else if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  bool remove() {
    lock();
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->remove();
    } else if (!removed(status)) {
      set_removed();
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  void replace_pointer() {
    lock();
    uint64_t status = this->status;
    UNUSED(status);
    assert(is_ptr(status));
    assert(!removed(status));
    if (!val.ptr->read(val.val)) {
      set_removed();
    }
    unset_is_ptr();
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
  }
  bool read_ignoring_ptr(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();
      if (unlikely(locked(status))) {
        continue;
      }
      memory_fence();

      uint64_t current_status = this->status;
      if (likely(get_version(status) == get_version(current_status))) {
        val = val_union.val;
        return !removed(status);
      }
    }
  }
  bool update_ignoring_ptr(const val_t &val) {
    lock();
    uint64_t status = this->status;
    bool res;
    if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  bool remove_ignoring_ptr() {
    lock();
    uint64_t status = this->status;
    bool res;
    if (!removed(status)) {
      set_removed();
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
};

}  // namespace xindex

#endif  // XINDEX_UTIL_H
