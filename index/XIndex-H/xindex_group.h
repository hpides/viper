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
 */

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <mutex>

#include "xindex_buffer.h"
#include "xindex_model.h"
#include "xindex_util.h"

#if !defined(XINDEX_GROUP_Hh)
#define XINDEX_GROUP_Hh

namespace xindexh {

template <class key_t, class val_t>
class alignas(CACHELINE_SIZE) HGroup {
  typedef LinearModel<key_t> linear_model_t;
  typedef HGroup<key_t, val_t> group_t;
  typedef AltBtreeBuffer<key_t, val_t> buffer_t;

  template <class key_tt, class val_tt>
  friend class HRoot;
  template <class key_tt, class val_tt>
  friend class XIndex;

  typedef AtomicVal<val_t> atomic_val_t;
  typedef atomic_val_t wrapped_val_t;

  class record_t {
   public:
    record_t(key_t k, wrapped_val_t v) : key(k), val(v), chain(nullptr) {}
    record_t() : key(0), chain(nullptr) {}

   public:
    key_t key;
    wrapped_val_t val;
    buffer_t *chain;
  };

 public:
  ~HGroup();
  void init(const std::vector<key_t> &keys, const std::vector<val_t> &vals,
            const size_t key_size);
  void init(const std::vector<key_t> &keys, const size_t key_size);

  inline result_t get(const key_t &key, val_t &val);
  inline result_t put(const key_t &key, const val_t &val);
  inline result_t remove(const key_t &key);

 private:
  void train_rmi(const std::vector<key_t> &keys);
  inline bool insert_ptr(const key_t &key, wrapped_val_t &val_ptr);
  inline bool upsert_bucket(const key_t &key, const val_t &val,
                            bool update_only, size_t pos);
  size_t predict(const key_t &key);
  void free_data();
  void hash(const std::vector<key_t> &keys, const std::vector<val_t> &vals);
  HGroup *compact_phase_1();
  void compact_phase_2();

  linear_model_t model_t;
  uint32_t array_size;
  std::atomic<uint32_t> bucket_element_cnt;
  record_t *data_array = nullptr;
  HGroup *group_tmp = nullptr;
  bool frozen = false;
  uint32_t conflict = 0;
  uint32_t error = 0;
};

}  // namespace xindex

#endif  // XINDEX_GROUP_H
