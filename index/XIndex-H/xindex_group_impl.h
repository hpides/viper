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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "xindex_group.h"

#if !defined(XINDEX_GROUP_IMPL_Hh)
#define XINDEX_GROUP_IMPL_Hh

namespace xindexh {
template <class key_t, class val_t>
HGroup<key_t, val_t>::~HGroup() {}

template <class key_t, class val_t>
void HGroup<key_t, val_t>::init(const std::vector<key_t> &keys,
                                const std::vector<val_t> &vals,
                                const size_t key_size) {
  array_size = (key_size == 0 ? 1 : key_size);
  bucket_element_cnt = 0;
  train_rmi(keys);
  data_array = new record_t[array_size];

  for (size_t i = 0; i < array_size; i++) {
    data_array[i].key = 0;
    data_array[i].chain = nullptr;
  }

  hash(keys, vals);
}

template <class key_t, class val_t>
void HGroup<key_t, val_t>::init(const std::vector<key_t> &keys,
                                const size_t key_size) {
  array_size = (key_size == 0 ? 1 : key_size);
  bucket_element_cnt = 0;
  train_rmi(keys);
  data_array = new record_t[array_size];

  for (size_t i = 0; i < array_size; i++) {
    data_array[i].key = 0;
    data_array[i].chain = nullptr;
  }
}

template <class key_t, class val_t>
void HGroup<key_t, val_t>::hash(const std::vector<key_t> &keys,
                                const std::vector<val_t> &vals) {
  for (size_t i = 0; i < keys.size(); i++) {
    size_t pos = predict(keys[i]);
    if (i < pos)
      error += (pos - i);
    else
      error += (i - pos);
    if (data_array[pos].key == 0) {
      data_array[pos].key = keys[i];
      data_array[pos].val = vals[i];
      bucket_element_cnt++;
    } else {
      conflict++;
      if (!data_array[pos].chain) data_array[pos].chain = new buffer_t();
      data_array[pos].chain->insert(keys[i], vals[i]);
    }
  }
}

template <class key_t, class val_t>
inline result_t HGroup<key_t, val_t>::get(const key_t &key, val_t &val) {
  bool res = false;
  size_t pos = predict(key);

  if (data_array[pos].key == key) res = data_array[pos].val.read(val);

  if (!res && data_array[pos].chain != nullptr)
    res = data_array[pos].chain->get(key, val);

  if (!res && group_tmp)
    res = group_tmp->get(key, val) == result_t::ok ? true : false;

  return res ? result_t::ok : result_t::failed;
}

template <class key_t, class val_t>
inline result_t HGroup<key_t, val_t>::put(const key_t &key, const val_t &val) {
  bool update_only = frozen;
  size_t pos = predict(key);

  if (upsert_bucket(key, val, update_only, pos)) {
    return result_t::ok;
  }

  if (!data_array[pos].chain) {
    data_array[pos].val.lock();
    if (!data_array[pos].chain) data_array[pos].chain = new buffer_t();
    data_array[pos].val.unlock();
  }

  if (update_only && data_array[pos].chain->update(key, val))
    return result_t::ok;

  if (!update_only) {
    data_array[pos].chain->insert(key, val);
    return result_t::ok;
  }

  if (!group_tmp) return result_t::retry;

  return group_tmp->put(key, val);
}

template <class key_t, class val_t>
inline bool HGroup<key_t, val_t>::insert_ptr(const key_t &key,
                                             wrapped_val_t &val_ptr) {
  size_t pos = predict(key);

  if (data_array[pos].key == 0) {
    data_array[pos].val.lock();
    if (data_array[pos].key == 0) {
      data_array[pos].key = key;
      data_array[pos].val = wrapped_val_t(&val_ptr);
      bucket_element_cnt++;
      data_array[pos].val.unlock();
      return true;
    }
    data_array[pos].val.unlock();
  }

  if (!data_array[pos].chain) {
    data_array[pos].val.lock();
    if (!data_array[pos].chain) data_array[pos].chain = new buffer_t();
    data_array[pos].val.unlock();
  }
  data_array[pos].chain->insert_ptr(key, &val_ptr);

  return true;
}

template <class key_t, class val_t>
inline bool HGroup<key_t, val_t>::upsert_bucket(const key_t &key,
                                                const val_t &val,
                                                bool update_only, size_t pos) {
  if (data_array[pos].key == 0) {
    data_array[pos].val.lock();
    if (!update_only && data_array[pos].key == 0) {
      data_array[pos].val = val;
      data_array[pos].key = key;
      bucket_element_cnt++;
      data_array[pos].val.unlock();
      return true;
    }
    data_array[pos].val.unlock();
  }

  if (data_array[pos].key == key) {
    data_array[pos].val.update(val);
    return true;
  }

  return false;
}

template <class key_t, class val_t>
inline result_t HGroup<key_t, val_t>::remove(const key_t &key) {
  size_t pos = predict(key);
  if (data_array[pos].key == key) {
    data_array[pos].val.remove();
    return result_t::ok;
  }

  if (data_array[pos].chain && data_array[pos].chain->remove(key))
    return result_t::ok;

  if (group_tmp) return (group_tmp->remove(key));

  return result_t::failed;
}

template <class key_t, class val_t>
void HGroup<key_t, val_t>::free_data() {
  for (size_t i = 0; i < array_size; i++) {
    if (data_array[i].chain) delete data_array[i].chain;
  }
  if (data_array != nullptr) delete data_array;
}

template <class key_t, class val_t>
inline void HGroup<key_t, val_t>::train_rmi(const std::vector<key_t> &keys) {
  std::vector<size_t> positions;
  positions.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    positions.push_back(i);
  }

  // train 1st stage
  model_t.prepare(keys, positions);
}

template <class key_t, class val_t>
inline size_t HGroup<key_t, val_t>::predict(const key_t &key) {
  size_t pos_pred = model_t.predict(key);
  if (pos_pred >= array_size) pos_pred = array_size - 1;
  if (pos_pred < 0) pos_pred = 0;
  return pos_pred;
}

template <class key_t, class val_t>
HGroup<key_t, val_t> *HGroup<key_t, val_t>::compact_phase_1() {
  std::vector<key_t> new_keys;
  for (size_t i = 0; i < array_size; i++) {
    if (data_array[i].key != 0 && !data_array[i].val.removed()) {
      new_keys.push_back(data_array[i].key);
    }
    if (data_array[i].chain) {
      auto buffer_source = typename buffer_t::RefSource(data_array[i].chain);
      buffer_source.advance_to_next_valid();
      while (buffer_source.has_next) {
        const key_t &buf_key = buffer_source.get_key();
        wrapped_val_t &buf_val = buffer_source.get_val();
        if (!buf_val.removed()) new_keys.push_back(buf_key);
        buffer_source.advance_to_next_valid();
      }
    }
  }

  sort(new_keys.begin(), new_keys.end());

  HGroup *new_group = new HGroup();

  new_group->init(new_keys, new_keys.size());

  frozen = true;
  memory_fence();
  rcu_barrier();
  group_tmp = new_group;

  // aquire latest keys and values
  new_keys.clear();
  for (size_t i = 0; i < array_size; i++) {
    if (data_array[i].key != 0 && !data_array[i].val.removed()) {
      key_t &base_key = data_array[i].key;
      wrapped_val_t &base_val = data_array[i].val;
      group_tmp->insert_ptr(base_key, base_val);
    }
    if (data_array[i].chain) {
      auto buffer_source = typename buffer_t::RefSource(data_array[i].chain);
      buffer_source.advance_to_next_valid();
      while (buffer_source.has_next) {
        const key_t &buf_key = buffer_source.get_key();
        wrapped_val_t &buf_val = buffer_source.get_val();
        if (!buf_val.removed()) group_tmp->insert_ptr(buf_key, buf_val);
        buffer_source.advance_to_next_valid();
      }
    }
  }
  return group_tmp;
}

template <class key_t, class val_t>
inline void HGroup<key_t, val_t>::compact_phase_2() {
  for (size_t i = 0; i < array_size; i++) {
    if (data_array[i].key == 0) continue;
    if (data_array[i].val.is_ptr()) {
      data_array[i].val.replace_pointer();
    }
    if (data_array[i].chain) {
      auto buffer_source = typename buffer_t::RefSource(data_array[i].chain);
      buffer_source.advance_to_next_valid();
      while (buffer_source.has_next) {
        const key_t &buf_key = buffer_source.get_key();
        wrapped_val_t &buf_val = buffer_source.get_val();
        if (buf_val.is_ptr()) data_array[i].chain->replace_pointer(buf_key);
        buffer_source.advance_to_next_valid();
      }
    }
  }
}

}  // namespace xindex

#endif  // XINDEX_GROUP_IMPL_H
