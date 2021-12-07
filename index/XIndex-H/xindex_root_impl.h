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

#include <algorithm>

#include "xindex_group_impl.h"
#include "xindex_root.h"

#if !defined(XINDEX_ROOT_IMPL_Hh)
#define XINDEX_ROOT_IMPL_Hh

namespace xindexh {

template <class key_t, class val_t>
HRoot<key_t, val_t>::~HRoot() {}

template <class key_t, class val_t>
void HRoot<key_t, val_t>::init(const std::vector<key_t> &keys,
                               const std::vector<val_t> &vals) {
  table_size = keys.size();
  train_rmi(keys);
  groups = std::make_unique<group_t *volatile[]>(group_n);

  std::vector<std::vector<key_t>> keys_dispatched(group_n);
  std::vector<std::vector<val_t>> vals_dispatched(group_n);

  for (size_t key_i = 0; key_i < keys.size(); key_i++) {
    size_t group_idx = locate_group_idx(keys[key_i]);
    keys_dispatched[group_idx].push_back(keys[key_i]);
    vals_dispatched[group_idx].push_back(vals[key_i]);
  }

  for (size_t i = 0; i < group_n; i++) {
    std::vector<key_t> &keys = keys_dispatched[i];
    std::vector<val_t> &vals = vals_dispatched[i];
    groups[i] = new group_t();
    groups[i]->init(keys, vals, keys_dispatched[i].size());
  }
  // calculate conflict rate and error
  size_t conflict = 0, error = 0;
  for (size_t i = 0; i < group_n; i++) {
    conflict += groups[i]->conflict;
    error += groups[i]->error;
  }
  double conflict_rate = (double)conflict / table_size;
  DEBUG_THIS("conflict rate: " << conflict_rate << ", mean error: "
                               << (double)error / table_size);
  while (conflict_rate > config.hash_init_conflict_threshold) {
    conflict_rate = re_init(keys, vals, group_n);
  }
}

template <class key_t, class val_t>
double HRoot<key_t, val_t>::re_init(const std::vector<key_t> &keys,
                                    const std::vector<val_t> &vals,
                                    size_t current_group_n) {
  for (size_t i = 0; i < current_group_n; i++) {
    groups[i]->free_data();
    delete groups[i];
  }
  table_size = keys.size();
  group_n = current_group_n * 2;
  DEBUG_THIS("re_init, group cnt:" << group_n);
  train_rmi(keys);
  groups = std::make_unique<group_t *volatile[]>(group_n);

  std::vector<std::vector<key_t>> keys_dispatched(group_n);
  std::vector<std::vector<val_t>> vals_dispatched(group_n);

  for (size_t key_i = 0; key_i < keys.size(); key_i++) {
    size_t group_idx = locate_group_idx(keys[key_i]);
    keys_dispatched[group_idx].push_back(keys[key_i]);
    vals_dispatched[group_idx].push_back(vals[key_i]);
  }

  for (size_t i = 0; i < group_n; i++) {
    std::vector<key_t> &keys = keys_dispatched[i];
    std::vector<val_t> &vals = vals_dispatched[i];
    groups[i] = new group_t();
    groups[i]->init(keys, vals, keys_dispatched[i].size());
  }
  // calculate conflict rate and error
  size_t conflict = 0, error = 0;
  for (size_t i = 0; i < group_n; i++) {
    conflict += groups[i]->conflict;
    error += groups[i]->error;
  }
  DEBUG_THIS("conflict rate: " << (double)conflict / table_size
                               << ", mean error: "
                               << (double)error / table_size);

  return (double)conflict / table_size;
}

template <class key_t, class val_t>
inline result_t HRoot<key_t, val_t>::get(const key_t &key, val_t &val) {
  return locate_group(key)->get(key, val);
}

template <class key_t, class val_t>
inline result_t HRoot<key_t, val_t>::put(const key_t &key, const val_t &val) {
  return locate_group(key)->put(key, val);
}

template <class key_t, class val_t>
inline result_t HRoot<key_t, val_t>::remove(const key_t &key) {
  return locate_group(key)->remove(key);
}

template <class key_t, class val_t>
void *HRoot<key_t, val_t>::do_maintenance(void *args) {
  size_t bg_i = (((BGInfo *)args)->bg_i);
  size_t bg_num = (((BGInfo *)args)->bg_n);
  volatile bool &running = ((BGInfo *)args)->running;

  size_t hash_resize_group_cnt = 0;

  while (running) {
    sleep(1);
    // read the current root ptr and bg thread's responsible range
    HRoot &root = **(HRoot * volatile *)(((BGInfo *)args)->root_ptr);
    size_t begin_group_i = bg_i * root.group_n / bg_num;
    size_t end_group_i =
        bg_i == bg_num - 1 ? root.group_n : (bg_i + 1) * root.group_n / bg_num;

    // iterate through the array, and do maintenance
    for (size_t group_i = begin_group_i; group_i < end_group_i; group_i++) {
      group_t *volatile *group = &(root.groups[group_i]);
      if (*group != nullptr) {
        size_t buffer_size = 0;
        for (size_t i = 1; i < (*group)->array_size; i++) {
          if ((*group)->data_array[i].chain)
            buffer_size += (*group)->data_array[i].chain->size();
        }
        double ratio = (double)(buffer_size + (*group)->bucket_element_cnt) /
                       (*group)->array_size;
        group_t *old_group = (*group);

        if (ratio > config.hash_resize_tolerance_upper ||
            ratio < config.hash_resize_tolerance_lower) {
          hash_resize_group_cnt++;
          group_t *new_group = old_group->compact_phase_1();
          *group = new_group;
          memory_fence();
          rcu_barrier();
          new_group->compact_phase_2();
          memory_fence();
          rcu_barrier();  // make sure no one is accessing the old data
          old_group->free_data();
          delete old_group;
        }
      }
    }
    DEBUG_THIS("hash resize group cnt: " << hash_resize_group_cnt);
    hash_resize_group_cnt = 0;
  }
  return nullptr;
}

template <class key_t, class val_t>
inline void HRoot<key_t, val_t>::train_rmi(const std::vector<key_t> &keys) {
  std::vector<size_t> positions(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    positions[i] = i;
  }
  // train 1st stage
  model_t.prepare(keys, positions);
}

template <class key_t, class val_t>
inline size_t HRoot<key_t, val_t>::locate_group_idx(const key_t &key) {
  size_t pos_pred = model_t.predict(key);
  size_t group_idx = pos_pred * group_n / table_size;
  if (group_idx >= group_n)
    group_idx = group_n - 1;
  else if (group_idx < 0)
    group_idx = 0;
  return group_idx;
}

template <class key_t, class val_t>
inline typename HRoot<key_t, val_t>::group_t *HRoot<key_t, val_t>::locate_group(
    const key_t &key) {
  size_t group_i = locate_group_idx(key);
  return groups[group_i];
}

}  // namespace xindex

#endif  // XINDEX_ROOT_IMPL_H
