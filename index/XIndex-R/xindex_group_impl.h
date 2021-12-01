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

#include "xindex_group.h"

#if !defined(XINDEX_GROUP_IMPL_H)
#define XINDEX_GROUP_IMPL_H

namespace xindex {

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>::Group() {}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>::~Group() {}

template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq, max_model_n>::init(
    const typename std::vector<key_t>::const_iterator &keys_begin,
    const typename std::vector<val_t>::const_iterator &vals_begin,
    uint32_t array_size) {
  init(keys_begin, vals_begin, 1, array_size);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq, max_model_n>::init(
    const typename std::vector<key_t>::const_iterator &keys_begin,
    const typename std::vector<val_t>::const_iterator &vals_begin,
    uint32_t model_n, uint32_t array_size) {
  assert(array_size > 0);
  this->pivot = *keys_begin;
  this->array_size = array_size;
  this->capacity = array_size * seq_insert_reserve_factor;
  this->model_n = model_n;
  data = new record_t[this->capacity]();
  buffer = new buffer_t();

  for (size_t rec_i = 0; rec_i < array_size; rec_i++) {
    data[rec_i].first = *(keys_begin + rec_i);
    data[rec_i].second = wrapped_val_t(*(vals_begin + rec_i));
  }

  for (size_t rec_i = 1; rec_i < array_size; rec_i++) {
    assert(data[rec_i].first >= data[rec_i - 1].first);
  }

  init_models(model_n);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
const key_t &Group<key_t, val_t, seq, max_model_n>::get_pivot() {
  return pivot;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline result_t Group<key_t, val_t, seq, max_model_n>::get(const key_t &key,
                                                           val_t &val) {
  if (get_from_array(key, val)) {
    return result_t::ok;
  }
  if (get_from_buffer(key, val, buffer)) {
    return result_t::ok;
  }
  if (buffer_temp && get_from_buffer(key, val, buffer_temp)) {
    return result_t::ok;
  }
  return result_t::failed;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline result_t Group<key_t, val_t, seq, max_model_n>::put(
    const key_t &key, const val_t &val, const uint32_t worker_id) {
#ifdef DEBUGGING
  assert(is_first || key >= pivot);
#endif
  result_t res;
  res = update_to_array(key, val, worker_id);
  if (res == result_t::ok || res == result_t::retry) {
    return res;
  }

  if (likely(buffer_temp == nullptr)) {
    if (buf_frozen) {
      return result_t::retry;
    }
    insert_to_buffer(key, val, buffer);
    return result_t::ok;
  } else {
    if (update_to_buffer(key, val, buffer)) {
      return result_t::ok;
    }
    insert_to_buffer(key, val, buffer_temp);
    return result_t::ok;
  }
  COUT_N_EXIT("put should not fail!");
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline result_t Group<key_t, val_t, seq, max_model_n>::remove(
    const key_t &key) {
  if (remove_from_array(key)) {
    return result_t::ok;
  }
  if (remove_from_buffer(key, buffer)) {
    return result_t::ok;
  }
  if (buffer_temp && remove_from_buffer(key, buffer_temp)) {
    return result_t::ok;
  }
  return result_t::failed;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::scan(
    const key_t &begin, const size_t n,
    std::vector<std::pair<key_t, val_t>> &result) {
  return buffer_temp ? scan_3_way(begin, n, key_t::max(), result)
                     : scan_2_way(begin, n, key_t::max(), result);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::range_scan(
    const key_t &begin, const key_t &end,
    std::vector<std::pair<key_t, val_t>> &result) {
  size_t old_size = result.size();
  if (buffer_temp) {
    scan_3_way(begin, std::numeric_limits<size_t>::max(), end, result);
  } else {
    scan_2_way(begin, std::numeric_limits<size_t>::max(), end, result);
  }
  return result.size() - old_size;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
double Group<key_t, val_t, seq, max_model_n>::mean_error_est() {
  // we did not disable seq op here so array_size can be changed.
  // however, we only need an estimated error
  uint32_t array_size = this->array_size;

  size_t pos_last_pivot = get_pos_from_array(models[model_n - 1].pivot);
  assert(pos_last_pivot != array_size);
  assert(data[pos_last_pivot].first == models[model_n - 1].pivot);

  // get current last model error
  size_t model_data_size = array_size - pos_last_pivot;
  std::vector<key_t> keys(model_data_size);
  std::vector<size_t> positions(model_data_size);
  for (size_t rec_i = 0; rec_i < model_data_size; rec_i++) {
    keys[rec_i] = data[pos_last_pivot + rec_i].first;
    positions[rec_i] = pos_last_pivot + rec_i;
  }
  double error_last_model_now =
      models[model_n - 1].model.get_error_bound(keys, positions);

  if (model_n == 1) {
    return error_last_model_now;
  } else {
    // est previous last model error
    size_t model_data_size_prev_est = pos_last_pivot / (model_n - 1);
    if (model_data_size_prev_est > model_data_size) {
      model_data_size_prev_est = model_data_size;
    }
    keys.resize(model_data_size_prev_est);
    positions.resize(model_data_size_prev_est);
    double error_last_model_prev =
        models[model_n - 1].model.get_error_bound(keys, positions);

    // est mean error
    return mean_error +
           (error_last_model_now - error_last_model_prev) / model_n;
  }
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>
    *Group<key_t, val_t, seq, max_model_n>::split_model() {
  if (seq) {  // disable seq seq
    disable_seq_insert_opt();
  }

  assert(model_n < max_model_n);

  Group *new_group = new Group();

  new_group->pivot = pivot;
  new_group->array_size = array_size;
  new_group->capacity = capacity;  // keep capacity negative for now
  new_group->data = data;
  new_group->init_models(model_n + 1);
  new_group->buffer = buffer;
  new_group->buffer_temp = buffer_temp;
  new_group->next = next;
#ifdef DEBUGGING
  new_group->is_first = is_first;
  assert(is_first || new_group->data[0].first >= new_group->pivot);
#endif

  return new_group;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>
    *Group<key_t, val_t, seq, max_model_n>::merge_model() {
  if (seq) {  // disable seq seq
    disable_seq_insert_opt();
  }

  assert(model_n > 1);

  Group *new_group = new Group();

  new_group->pivot = pivot;
  new_group->array_size = array_size;
  new_group->capacity = capacity;  // keep capacity negative for now
  new_group->data = data;
  new_group->init_models(model_n - 1);
  new_group->buffer = buffer;
  new_group->buffer_temp = buffer_temp;
  new_group->next = next;
#ifdef DEBUGGING
  new_group->is_first = is_first;
#endif

  return new_group;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>
    *Group<key_t, val_t, seq, max_model_n>::split_group_pt1() {
  if (seq) {  // disable seq seq
    disable_seq_insert_opt();
  }

  Group *new_group_1 = new Group();
  Group *new_group_2 = new Group();

  new_group_1->pivot = pivot;
  new_group_2->pivot = data[array_size / 2].first;
#ifdef DEBUGGING
  assert(is_first || new_group_2->pivot > new_group_1->pivot);
#endif
  new_group_1->data = data;
  new_group_2->data = data;
  new_group_1->array_size = array_size;
  new_group_2->array_size = array_size;
  // mark capacity as negative to let seq insert not inserting to buf
  new_group_1->capacity = capacity;
  new_group_2->capacity = capacity;
  new_group_1->init_models((model_n + 1) / 2);
  new_group_2->init_models((model_n + 1) / 2);
  new_group_1->buf_frozen = true;
  new_group_2->buf_frozen = true;
  new_group_1->buffer = buffer;
  new_group_2->buffer = buffer;
  new_group_1->buffer_temp = new buffer_t();
  new_group_2->buffer_temp = new buffer_t();
  new_group_1->next = new_group_2;
  new_group_2->next = next;
#ifdef DEBUGGING
  new_group_1->is_first = is_first;
  assert(is_first || new_group_1->data[0].first >= new_group_1->pivot);
#endif

  return new_group_1;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>
    *Group<key_t, val_t, seq, max_model_n>::split_group_pt2() {
  // note that now this->data, this->buffer point to the old group's
  // and are shared with this->next
  Group *new_group_1 = new Group();
  Group *new_group_2 = new Group();

  new_group_1->pivot = pivot;
  new_group_2->pivot = this->next->pivot;
  merge_refs_n_split(new_group_1->data, new_group_1->array_size,
                     new_group_1->capacity, new_group_2->data,
                     new_group_2->array_size, new_group_2->capacity,
                     this->next->pivot);
  // mark capacity as negative to let seq insert not inserting to buf
  if (seq) {
    new_group_1->disable_seq_insert_opt();
    new_group_2->disable_seq_insert_opt();
  }
  new_group_1->init_models(model_n);  // model_n is reduced in pt1
  new_group_2->init_models(model_n);
  new_group_1->buffer = buffer_temp;
  new_group_2->buffer = next->buffer_temp;
  new_group_1->next = new_group_2;
  new_group_2->next = next->next;
#ifdef DEBUGGING
  new_group_1->is_first = is_first;
  assert(is_first || new_group_1->data[0].first >= new_group_1->pivot);
  assert(new_group_2->data[0].first >= new_group_2->pivot);
#endif

  return new_group_1;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>
    *Group<key_t, val_t, seq, max_model_n>::merge_group(
        Group<key_t, val_t, seq, max_model_n> &next_group) {
  if (seq) {  // disable seq seq
    disable_seq_insert_opt();
    next_group.disable_seq_insert_opt();
  }

  buf_frozen = true;
  next_group.buf_frozen = true;
  memory_fence();
  rcu_barrier();
  buffer_temp = new buffer_t();
  next_group.buffer_temp = buffer_temp;

  Group *new_group = new Group();
  new_group->model_n = model_n + next_group.model_n;
  if (new_group->model_n > max_model_n) {
    new_group->model_n = max_model_n;
  }

  new_group->pivot = pivot;
  merge_refs_with(next_group, new_group->data, new_group->array_size,
                  new_group->capacity);
  if (seq) {
    // mark capacity as negative to let seq insert not insert to buf
    new_group->disable_seq_insert_opt();
  }
  new_group->init_models(new_group->model_n);
  new_group->buffer = buffer_temp;
  new_group->next = next_group.next;
#ifdef DEBUGGING
  new_group->is_first = is_first;
#endif

  return new_group;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>
    *Group<key_t, val_t, seq, max_model_n>::compact_phase_1() {
  if (seq) {  // disable seq seq
    disable_seq_insert_opt();
  }

  buf_frozen = true;
  memory_fence();
  rcu_barrier();
  buffer_temp = new buffer_t();

  // now merge sort into a new array and train models
  Group *new_group = new Group();

  new_group->pivot = pivot;
  merge_refs(new_group->data, new_group->array_size, new_group->capacity);
  if (seq) {  // mark capacity as negative to let seq insert not insert to buf
    new_group->disable_seq_insert_opt();
  }
  new_group->init_models(model_n);
  new_group->buffer = buffer_temp;
  new_group->next = next;
#ifdef DEBUGGING
  new_group->is_first = is_first;
#endif

  return new_group;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::compact_phase_2() {
  for (size_t rec_i = 0; rec_i < array_size; ++rec_i) {
    data[rec_i].second.replace_pointer();
  }

  if (seq) {
    // enable seq seq, because now all threads only see new node
    enable_seq_insert_opt();
  }
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq, max_model_n>::free_data() {
  delete[] data;
}
template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq, max_model_n>::free_buffer() {
  delete buffer;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::locate_model(
    const key_t &key) {
  assert(model_n >= 1);

  int model_i = 0;
  while (model_i < model_n - 1 && models[model_i + 1].pivot <= key) {
    model_i++;
  }
  return model_i;
}

// semantics: atomically read the value
// only when the key exists and the record (record_t) is not logical removed,
// return true on success
template <class key_t, class val_t, bool seq, size_t max_model_n>
inline bool Group<key_t, val_t, seq, max_model_n>::get_from_array(
    const key_t &key, val_t &val) {
  size_t pos = get_pos_from_array(key);
  return pos != array_size &&         // position is valid (not out-of-range)
         data[pos].first == key &&    // key matches
         data[pos].second.read(val);  // value is not removed
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline result_t Group<key_t, val_t, seq, max_model_n>::update_to_array(
    const key_t &key, const val_t &val, const uint32_t worker_id) {
  if (seq) {
    seq_lock();
    size_t pos = get_pos_from_array(key);
    if (pos != array_size) {  // position is valid (not out-of-range)
      seq_unlock();
      return (/* key matches */ data[pos].first == key &&
              /* record updated */ data[pos].second.update(val))
                 ? result_t::ok
                 : result_t::failed;
    } else {                      // might append
      if (buffer->size() == 0) {  // buf is empty
        if (capacity < 0) {
          seq_unlock();
          return result_t::retry;
        }

        if ((int32_t)array_size == capacity) {
          record_t *prev_data = nullptr;
          capacity = array_size * seq_insert_reserve_factor;
          record_t *new_data = new record_t[capacity]();
          memcpy(new_data, data, array_size * sizeof(record_t));
          prev_data = data;
          data = new_data;

          data[pos].first = key;
          data[pos].second = wrapped_val_t(val);
          array_size++;
          seq_unlock();

          rcu_barrier(worker_id);
          memory_fence();
          delete[] prev_data;
          return result_t::ok;
        } else {
          data[pos].first = key;
          data[pos].second = wrapped_val_t(val);
          array_size++;
          seq_unlock();
          return result_t::ok;
        }
      } else {
        seq_unlock();
        return result_t::failed;
      }
    }
  } else {  // no seq
    size_t pos = get_pos_from_array(key);
    return pos != array_size && data[pos].first == key &&
                   data[pos].second.update(val)
               ? result_t::ok
               : result_t::failed;
  }
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline bool Group<key_t, val_t, seq, max_model_n>::remove_from_array(
    const key_t &key) {
  size_t pos = get_pos_from_array(key);
  return pos != array_size &&        // position is valid (not out-of-range)
         data[pos].first == key &&   // key matches
         data[pos].second.remove();  // value is not removed and is updated
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::get_pos_from_array(
    const key_t &key) {
  size_t model_i = locate_model(key);
  size_t pos = models[model_i].model.predict(key);
  return exponential_search_key(key, pos);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::binary_search_key(
    const key_t &key, size_t pos, size_t search_begin, size_t search_end) {
  // search within the range
  if (unlikely(search_begin > array_size)) {
    search_begin = array_size;
  }
  if (unlikely(search_end > array_size)) {
    search_end = array_size;
  }
  size_t mid = pos >= search_begin && pos < search_end
                   ? pos
                   : (search_begin + search_end) / 2;
  while (search_end != search_begin) {
    if (data[mid].first < key) {
      search_begin = mid + 1;
    } else {
      search_end = mid;
    }
    mid = (search_begin + search_end) / 2;
  }
  assert(search_begin == search_end);
  assert(search_begin == mid);

  return mid;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::exponential_search_key(
    const key_t &key, size_t pos) const {
  return exponential_search_key(data, array_size, key, pos);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::exponential_search_key(
    record_t *const data, uint32_t array_size, const key_t &key,
    size_t pos) const {
  if (array_size == 0) return 0;
  pos = (pos >= array_size ? (array_size - 1) : pos);
  assert(pos < array_size);

  int begin_i = 0, end_i = array_size;
  size_t step = 1;

  if (data[pos].first <= key) {
    begin_i = pos;
    end_i = begin_i + step;
    while (end_i < (int)array_size && data[end_i].first <= key) {
      step *= 2;
      begin_i = end_i;
      end_i = begin_i + step;
    }
    if (end_i >= (int)array_size) {
      end_i = array_size - 1;
    }
  } else {
    end_i = pos;
    begin_i = end_i - step;
    while (begin_i >= 0 && data[begin_i].first > key) {
      step *= 2;
      end_i = begin_i;
      begin_i = end_i - step;
    }
    if (begin_i < 0) {
      begin_i = 0;
    }
  }

  assert(begin_i >= 0);
  assert(end_i < (int)array_size);
  assert(begin_i <= end_i);

  // the real range is [begin_i, end_i], both inclusive.
  // we add 1 to end_i in order to find the insert position when the given key
  // is not exist
  end_i++;
  // find the largest position whose key equal to the given key
  while (end_i > begin_i) {
    // here the +1 term is used to avoid the infinte loop
    // where (end_i = begin_i + 1 && mid = begin_i && data[mid].first <= key)
    int mid = (begin_i + end_i) >> 1;
    if (data[mid].first < key) {
      begin_i = mid + 1;
    } else {
      // we should assign end_i with mid (not mid+1) in case infinte loop
      end_i = mid;
    }
  }

  assert(end_i == begin_i);
  assert(data[end_i].first == key || end_i == 0 || end_i == (int)array_size ||
         (data[end_i - 1].first < key && data[end_i].first > key));

  return end_i;
}

// semantics: atomically read the value
// only when the key exists and the record (record_t) is not logical removed,
// return true on success
template <class key_t, class val_t, bool seq, size_t max_model_n>
inline bool Group<key_t, val_t, seq, max_model_n>::get_from_buffer(
    const key_t &key, val_t &val, buffer_t *buffer) {
  return buffer->get(key, val);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline bool Group<key_t, val_t, seq, max_model_n>::update_to_buffer(
    const key_t &key, const val_t &val, buffer_t *buffer) {
  return buffer->update(key, val);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::insert_to_buffer(
    const key_t &key, const val_t &val, buffer_t *buffer) {
  buffer->insert(key, val);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline bool Group<key_t, val_t, seq, max_model_n>::remove_from_buffer(
    const key_t &key, buffer_t *buffer) {
  return buffer->remove(key);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq, max_model_n>::init_models(uint32_t model_n) {
  assert(model_n >= 1);
  this->model_n = model_n;

  size_t records_per_model = array_size / model_n;
  size_t trailing_n = array_size - records_per_model * model_n;
  size_t begin = 0;
  mean_error = 0;
  for (size_t model_i = 0; model_i < model_n; ++model_i) {
    size_t end = begin + records_per_model;
    if (trailing_n > 0) {
      end++;
      trailing_n--;
    }
    assert(end <= array_size);
    assert((model_i == model_n - 1 && end == array_size) ||
           model_i < model_n - 1);

    models[model_i].pivot = data[begin].first;
    // models[model_i].offset = begin;
    mean_error += train_model(model_i, begin, end);

    begin = end;
  }
  mean_error /= model_n;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline double Group<key_t, val_t, seq, max_model_n>::train_model(size_t model_i,
                                                                 size_t begin,
                                                                 size_t end) {
  assert(end >= begin);
  assert(array_size >= end);

  size_t model_data_size = end - begin;
  std::vector<key_t> keys(model_data_size);
  std::vector<size_t> positions(model_data_size);

  for (size_t rec_i = 0; rec_i < model_data_size; rec_i++) {
    keys[rec_i] = data[begin + rec_i].first;
    positions[rec_i] = begin + rec_i;
  }

  models[model_i].model.prepare(keys, positions);
  return models[model_i].model.get_error_bound(keys, positions);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::seq_lock() {
  while (true) {
    uint8_t expected = 0;
    uint8_t desired = 1;
    if (likely(cmpxchgb((uint8_t *)&lock, expected, desired) == expected)) {
      return;
    }
  }
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::seq_unlock() {
  asm volatile("" : : : "memory");
  lock = 0;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::enable_seq_insert_opt() {
  seq_lock();
  capacity = -capacity;
  INVARIANT(capacity > 0);
  seq_unlock();
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::disable_seq_insert_opt() {
  seq_lock();
  capacity = -capacity;
  INVARIANT(capacity < 0);
  seq_unlock();
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::merge_refs(
    record_t *&new_data, uint32_t &new_array_size,
    int32_t &new_capacity) const {
  size_t est_size = array_size + buffer->size();
  new_capacity = est_size * seq_insert_reserve_factor;
  new_data = new record_t[new_capacity]();
  merge_refs_internal(new_data, new_array_size);
  assert((int32_t)new_array_size <= new_capacity);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::merge_refs_n_split(
    record_t *&new_data_1, uint32_t &new_array_size_1, int32_t &new_capacity_1,
    record_t *&new_data_2, uint32_t &new_array_size_2, int32_t &new_capacity_2,
    const key_t &key) const {
  uint32_t intermediate_size;
  uint32_t est_size = array_size + buffer->size();

  new_capacity_1 = (est_size / 2) * seq_insert_reserve_factor;
  new_capacity_1 =
      (int32_t)est_size > new_capacity_1 ? est_size : new_capacity_1;

  record_t *intermediate = new record_t[new_capacity_1]();
  merge_refs_internal(intermediate, intermediate_size);

  uint32_t split_pos = exponential_search_key(intermediate, intermediate_size,
                                              key, intermediate_size / 2);
  assert(split_pos != intermediate_size &&
         intermediate[split_pos].first >= key);

  new_array_size_1 = split_pos;
  new_data_1 = intermediate;

  new_array_size_2 = intermediate_size - split_pos;
  new_capacity_2 = new_array_size_2 * seq_insert_reserve_factor;
  new_data_2 = new record_t[new_capacity_2]();
  memcpy(new_data_2, intermediate + split_pos,
         new_array_size_2 * sizeof(record_t));

  assert((int32_t)new_array_size_1 <= new_capacity_1);
  assert((int32_t)new_array_size_2 <= new_capacity_2);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::merge_refs_with(
    const Group &next_group, record_t *&new_data, uint32_t &new_array_size,
    int32_t &new_capacity) const {
  size_t est_size = array_size + buffer->size() + next_group.array_size +
                    next_group.buffer->size();
  new_capacity = est_size * seq_insert_reserve_factor;
  new_data = new record_t[new_capacity]();

  uint32_t real_size_1, real_size_2;
  merge_refs_internal(new_data, real_size_1);
  next_group.merge_refs_internal(new_data + real_size_1, real_size_2);

  new_array_size = real_size_1 + real_size_2;

  assert((int32_t)new_array_size <= new_capacity);
}

// no workers should insert into buffer (frozen) now, so no lock needed
template <class key_t, class val_t, bool seq, size_t max_model_n>
inline void Group<key_t, val_t, seq, max_model_n>::merge_refs_internal(
    record_t *new_data, uint32_t &new_array_size) const {
  size_t count = 0;

  auto buffer_source = typename buffer_t::RefSource(buffer);
  auto array_source = ArrayRefSource(data, array_size);
  array_source.advance_to_next_valid();
  buffer_source.advance_to_next_valid();

  while (array_source.has_next && buffer_source.has_next) {
    const key_t &base_key = array_source.get_key();
    wrapped_val_t &base_val = array_source.get_val();
    const key_t &buf_key = buffer_source.get_key();
    wrapped_val_t &buf_val = buffer_source.get_val();

    assert(base_key != buf_key);  // since update are inplaced

    if (base_key < buf_key) {
      new_data[count].first = base_key;
      new_data[count].second = wrapped_val_t(&base_val);
      assert(new_data[count].second.val.ptr->val.val == base_val.val.val);
      array_source.advance_to_next_valid();
    } else {
      new_data[count].first = buf_key;
      new_data[count].second = wrapped_val_t(&buf_val);
      assert(new_data[count].second.val.ptr->val.val == buf_val.val.val);
      buffer_source.advance_to_next_valid();
    }
    count++;
  }

  while (array_source.has_next) {
    const key_t &base_key = array_source.get_key();
    wrapped_val_t &base_val = array_source.get_val();

    new_data[count].first = base_key;
    new_data[count].second = wrapped_val_t(&base_val);
    assert(new_data[count].second.val.ptr->val.val == base_val.val.val);

    array_source.advance_to_next_valid();
    count++;
  }

  while (buffer_source.has_next) {
    const key_t &buf_key = buffer_source.get_key();
    wrapped_val_t &buf_val = buffer_source.get_val();

    new_data[count].first = buf_key;
    new_data[count].second = wrapped_val_t(&buf_val);
    assert(new_data[count].second.val.ptr->val.val == buf_val.val.val);

    buffer_source.advance_to_next_valid();
    count++;
  }

  for (size_t rec_i = 0; rec_i < (count == 0 ? 0 : count - 1); rec_i++) {
    assert(new_data[rec_i].first < new_data[rec_i + 1].first);
    assert(new_data[rec_i].second.status == new_data[rec_i + 1].second.status);
    assert(new_data[rec_i].second.status == 0x4000000000000000);
  }

  new_array_size = count;
  // assert(count > 0);
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::scan_2_way(
    const key_t &begin, const size_t n, const key_t &end,
    std::vector<std::pair<key_t, val_t>> &result) {
  size_t remaining = n;
  bool out_of_range = false;
  uint32_t base_i = get_pos_from_array(begin);
  ArrayDataSource array_source(data, array_size, base_i);
  typename buffer_t::DataSource buffer_source(begin, buffer);

  // first read a not-removed value from array and buffer, to avoid double read
  // during merge
  array_source.advance_to_next_valid();
  buffer_source.advance_to_next_valid();

  while (array_source.has_next && buffer_source.has_next && remaining &&
         !out_of_range) {
    // we are sure that these key has not-removed (pre-read) value
    const key_t &base_key = array_source.get_key();
    const val_t &base_val = array_source.get_val();
    const key_t &buf_key = buffer_source.get_key();
    const val_t &buf_val = buffer_source.get_val();

    assert(base_key != buf_key);  // since update are inplaced

    if (base_key < buf_key) {
      if (base_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(base_key, base_val));
      array_source.advance_to_next_valid();
    } else {
      if (buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(buf_key, buf_val));
      buffer_source.advance_to_next_valid();
    }

    remaining--;
  }

  while (array_source.has_next && remaining && !out_of_range) {
    const key_t &base_key = array_source.get_key();
    const val_t &base_val = array_source.get_val();
    if (base_key >= end) {
      out_of_range = true;
      break;
    }
    result.push_back(std::pair<key_t, val_t>(base_key, base_val));
    array_source.advance_to_next_valid();
    remaining--;
  }

  while (buffer_source.has_next && remaining && !out_of_range) {
    const key_t &buf_key = buffer_source.get_key();
    const val_t &buf_val = buffer_source.get_val();
    if (buf_key >= end) {
      out_of_range = true;
      break;
    }
    result.push_back(std::pair<key_t, val_t>(buf_key, buf_val));
    buffer_source.advance_to_next_valid();
    remaining--;
  }

  return n - remaining;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
inline size_t Group<key_t, val_t, seq, max_model_n>::scan_3_way(
    const key_t &begin, const size_t n, const key_t &end,
    std::vector<std::pair<key_t, val_t>> &result) {
  size_t remaining = n;
  bool out_of_range = false;
  uint32_t base_i = get_pos_from_array(begin);
  ArrayDataSource array_source(data, array_size, base_i);
  typename buffer_t::DataSource buffer_source(begin, buffer);
  typename buffer_t::DataSource temp_buffer_source(begin, buffer_temp);

  // first read a not-removed value from array and buffer, to avoid double read
  // during merge
  array_source.advance_to_next_valid();
  buffer_source.advance_to_next_valid();
  temp_buffer_source.advance_to_next_valid();

  // 3-way
  while (array_source.has_next && buffer_source.has_next &&
         temp_buffer_source.has_next && remaining && !out_of_range) {
    // we are sure that these key has not-removed (pre-read) value
    const key_t &base_key = array_source.get_key();
    const val_t &base_val = array_source.get_val();
    const key_t &buf_key = buffer_source.get_key();
    const val_t &buf_val = buffer_source.get_val();
    const key_t &tmp_buf_key = temp_buffer_source.get_key();
    const val_t &tmp_buf_val = temp_buffer_source.get_val();

    assert(base_key != buf_key);      // since update are inplaced
    assert(base_key != tmp_buf_key);  // and removed values are skipped

    if (base_key < buf_key && base_key < tmp_buf_key) {
      if (base_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(base_key, base_val));
      array_source.advance_to_next_valid();
    } else if (buf_key < base_key && buf_key < tmp_buf_key) {
      if (buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(buf_key, buf_val));
      buffer_source.advance_to_next_valid();
    } else {
      if (tmp_buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(tmp_buf_key, tmp_buf_val));
      temp_buffer_source.advance_to_next_valid();
    }

    remaining--;
  }

  // 2-way trailings
  while (array_source.has_next && buffer_source.has_next && remaining &&
         !out_of_range) {
    // we are sure that these key has not-removed (pre-read) value
    const key_t &base_key = array_source.get_key();
    const val_t &base_val = array_source.get_val();
    const key_t &buf_key = buffer_source.get_key();
    const val_t &buf_val = buffer_source.get_val();

    assert(base_key != buf_key);  // since update are inplaced

    if (base_key < buf_key) {
      if (base_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(base_key, base_val));
      array_source.advance_to_next_valid();
    } else {
      if (buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(buf_key, buf_val));
      buffer_source.advance_to_next_valid();
    }

    remaining--;
  }

  while (buffer_source.has_next && temp_buffer_source.has_next && remaining &&
         !out_of_range) {
    // we are sure that these key has not-removed (pre-read) value
    const key_t &buf_key = buffer_source.get_key();
    const val_t &buf_val = buffer_source.get_val();
    const key_t &tmp_buf_key = temp_buffer_source.get_key();
    const val_t &tmp_buf_val = temp_buffer_source.get_val();

    assert(buf_key != tmp_buf_key);  // and removed values are skipped

    if (buf_key < tmp_buf_key) {
      if (buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(buf_key, buf_val));
      buffer_source.advance_to_next_valid();
    } else {
      if (tmp_buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(tmp_buf_key, tmp_buf_val));
      temp_buffer_source.advance_to_next_valid();
    }

    remaining--;
  }

  while (array_source.has_next && temp_buffer_source.has_next && remaining &&
         !out_of_range) {
    // we are sure that these key has not-removed (pre-read) value
    const key_t &base_key = array_source.get_key();
    const val_t &base_val = array_source.get_val();
    const key_t &tmp_buf_key = temp_buffer_source.get_key();
    const val_t &tmp_buf_val = temp_buffer_source.get_val();

    assert(base_key != tmp_buf_key);  // and removed values are skipped

    if (base_key < tmp_buf_key) {
      if (base_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(base_key, base_val));
      array_source.advance_to_next_valid();
    } else {
      if (tmp_buf_key >= end) {
        out_of_range = true;
        break;
      }
      result.push_back(std::pair<key_t, val_t>(tmp_buf_key, tmp_buf_val));
      temp_buffer_source.advance_to_next_valid();
    }

    remaining--;
  }

  // 1-way trailings
  while (array_source.has_next && remaining && !out_of_range) {
    const key_t &base_key = array_source.get_key();
    const val_t &base_val = array_source.get_val();
    if (base_key >= end) {
      out_of_range = true;
      break;
    }
    result.push_back(std::pair<key_t, val_t>(base_key, base_val));
    array_source.advance_to_next_valid();
    remaining--;
  }

  while (buffer_source.has_next && remaining && !out_of_range) {
    const key_t &buf_key = buffer_source.get_key();
    const val_t &buf_val = buffer_source.get_val();
    if (buf_key >= end) {
      out_of_range = true;
      break;
    }
    result.push_back(std::pair<key_t, val_t>(buf_key, buf_val));
    buffer_source.advance_to_next_valid();
    remaining--;
  }

  while (temp_buffer_source.has_next && remaining && !out_of_range) {
    const key_t &tmp_buf_key = temp_buffer_source.get_key();
    const val_t &tmp_buf_val = temp_buffer_source.get_val();
    if (tmp_buf_key >= end) {
      out_of_range = true;
      break;
    }
    result.push_back(std::pair<key_t, val_t>(tmp_buf_key, tmp_buf_val));
    temp_buffer_source.advance_to_next_valid();
    remaining--;
  }

  return n - remaining;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>::ArrayDataSource::ArrayDataSource(
    record_t *data, uint32_t array_size, uint32_t pos)
    : array_size(array_size), pos(pos), data(data) {}

template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq,
           max_model_n>::ArrayDataSource::advance_to_next_valid() {
  while (pos < array_size) {
    if (data[pos].second.read(next_val)) {
      next_key = data[pos].first;
      has_next = true;
      pos++;
      return;
    }
    pos++;
  }
  has_next = false;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
const key_t &Group<key_t, val_t, seq, max_model_n>::ArrayDataSource::get_key() {
  return next_key;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
const val_t &Group<key_t, val_t, seq, max_model_n>::ArrayDataSource::get_val() {
  return next_val;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
Group<key_t, val_t, seq, max_model_n>::ArrayRefSource::ArrayRefSource(
    record_t *data, uint32_t array_size)
    : array_size(array_size), pos(0), data(data) {}

template <class key_t, class val_t, bool seq, size_t max_model_n>
void Group<key_t, val_t, seq,
           max_model_n>::ArrayRefSource::advance_to_next_valid() {
  while (pos < array_size) {
    val_t temp_val;
    if (data[pos].second.read(temp_val)) {
      next_val_ptr = &data[pos].second;
      next_key = data[pos].first;
      has_next = true;
      pos++;
      return;
    }
    pos++;
  }
  has_next = false;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
const key_t &Group<key_t, val_t, seq, max_model_n>::ArrayRefSource::get_key() {
  return next_key;
}

template <class key_t, class val_t, bool seq, size_t max_model_n>
typename Group<key_t, val_t, seq, max_model_n>::atomic_val_t &
Group<key_t, val_t, seq, max_model_n>::ArrayRefSource::get_val() {
  return *next_val_ptr;
}

}  // namespace xindex

#endif  // XINDEX_GROUP_IMPL_H
