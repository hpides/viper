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

#include "xindex_buffer.h"
#include "xindex_model.h"
#include "xindex_util.h"

#if !defined(XINDEX_GROUP_H)
#define XINDEX_GROUP_H

namespace xindex {

template <class key_t, class val_t, bool seq, size_t max_model_n = 4>
class alignas(CACHELINE_SIZE) Group {
  struct ModelInfo;

  typedef LinearModel<key_t> linear_model_t;
  typedef ModelInfo model_info_t;
  typedef AtomicVal<val_t> atomic_val_t;
  typedef atomic_val_t wrapped_val_t;
  typedef AltBtreeBuffer<key_t, val_t> buffer_t;
  typedef uint64_t version_t;
  typedef std::pair<key_t, wrapped_val_t> record_t;

  template <class key_tt, class val_tt, bool sequential>
  friend class XIndex;
  template <class key_tt, class val_tt, bool sequential>
  friend class Root;

  struct ModelInfo {
    key_t pivot;
    linear_model_t model;
  };

  struct ArrayDataSource {
    ArrayDataSource(record_t *data, uint32_t array_size, uint32_t pos);
    void advance_to_next_valid();
    const key_t &get_key();
    const val_t &get_val();

    uint32_t array_size, pos;
    record_t *data;
    bool has_next;
    key_t next_key;
    val_t next_val;
  };

  struct ArrayRefSource {
    ArrayRefSource(record_t *data, uint32_t array_size);
    void advance_to_next_valid();
    const key_t &get_key();
    atomic_val_t &get_val();

    uint32_t array_size, pos;
    record_t *data;
    bool has_next;
    key_t next_key;
    atomic_val_t *next_val_ptr;
  };

 public:
  Group();
  ~Group();
  void init(const typename std::vector<key_t>::const_iterator &keys_begin,
            const typename std::vector<val_t>::const_iterator &vals_begin,
            uint32_t array_size);
  void init(const typename std::vector<key_t>::const_iterator &keys_begin,
            const typename std::vector<val_t>::const_iterator &vals_begin,
            uint32_t model_n, uint32_t array_size);
  const key_t &get_pivot();

  inline result_t get(const key_t &key, val_t &val);
  inline result_t put(const key_t &key, const val_t &val,
                      const uint32_t worker_id);
  inline result_t remove(const key_t &key);
  inline size_t scan(const key_t &begin, const size_t n,
                     std::vector<std::pair<key_t, val_t>> &result);
  inline size_t range_scan(const key_t &begin, const key_t &end,
                           std::vector<std::pair<key_t, val_t>> &result);

  double mean_error_est();
  Group *split_model();
  Group *merge_model();
  Group *split_group_pt1();
  Group *split_group_pt2();
  Group *merge_group(Group &next_group);
  Group *compact_phase_1();
  void compact_phase_2();

  void free_data();
  void free_buffer();

 private:
  inline size_t locate_model(const key_t &key);

  inline bool get_from_array(const key_t &key, val_t &val);
  inline result_t update_to_array(const key_t &key, const val_t &val,
                                  const uint32_t worker_id);
  inline bool remove_from_array(const key_t &key);

  inline size_t get_pos_from_array(const key_t &key);
  inline size_t binary_search_key(const key_t &key, size_t pos_hint,
                                  size_t search_begin, size_t search_end);
  inline size_t exponential_search_key(const key_t &key, size_t pos_hint) const;
  inline size_t exponential_search_key(record_t *const data,
                                       uint32_t array_size, const key_t &key,
                                       size_t pos_hint) const;

  inline bool get_from_buffer(const key_t &key, val_t &val, buffer_t *buffer);
  inline bool update_to_buffer(const key_t &key, const val_t &val,
                               buffer_t *buffer);
  inline void insert_to_buffer(const key_t &key, const val_t &val,
                               buffer_t *buffer);
  inline bool remove_from_buffer(const key_t &key, buffer_t *buffer);

  void init_models(uint32_t model_n);
  inline double train_model(size_t model_i, size_t begin, size_t end);

  inline void merge_refs(record_t *&new_data, uint32_t &new_array_size,
                         int32_t &new_capacity) const;
  inline void merge_refs_n_split(record_t *&new_data_1,
                                 uint32_t &new_array_size_1,
                                 int32_t &new_capacity_1, record_t *&new_data_2,
                                 uint32_t &new_array_size_2,
                                 int32_t &new_capacity_2,
                                 const key_t &key) const;
  inline void merge_refs_with(const Group &next_group, record_t *&new_data,
                              uint32_t &new_array_size,
                              int32_t &new_capacity) const;
  inline void merge_refs_internal(record_t *new_data,
                                  uint32_t &new_array_size) const;
  inline size_t scan_2_way(const key_t &begin, const size_t n, const key_t &end,
                           std::vector<std::pair<key_t, val_t>> &result);
  inline size_t scan_3_way(const key_t &begin, const size_t n, const key_t &end,
                           std::vector<std::pair<key_t, val_t>> &result);
  void seq_lock();
  void seq_unlock();
  inline void enable_seq_insert_opt();
  inline void disable_seq_insert_opt();

  key_t pivot;
  // make array_size atomic because we don't want to acquire lock during `get`.
  // it is okay to obtain a stale (smaller) array_size during `get`.
  uint32_t array_size;
  uint16_t model_n = 0;
  bool buf_frozen = false;
  Group *next = nullptr;
  std::array<model_info_t, max_model_n> models;
  record_t *data = nullptr;
  buffer_t *buffer = nullptr;
  buffer_t *buffer_temp = nullptr;
  double mean_error;
  int32_t capacity = 0;       // used for seqential insertion
  volatile uint8_t lock = 0;  // used for seqential insertion

#ifdef DEBUGGING
  // used to ignore first group's pivot value (which should be considered zero)
  // for debug assertions.
  uint8_t is_first = 0;
#endif
};

}  // namespace xindex

#endif  // XINDEX_GROUP_H
