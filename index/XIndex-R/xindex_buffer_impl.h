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

#include <cstdint>
#include <mutex>
#include <vector>

#include "xindex_buffer.h"

#if !defined(xindex_buffer_IMPL_H)
#define xindex_buffer_IMPL_H

namespace xindex {

template <class key_t, class val_t>
AltBtreeBuffer<key_t, val_t>::AltBtreeBuffer() {
  size_est = 0;
  next_node_i = 0;
  allocated_blocks.reserve(1);

  begin = allocate_leaf();
  begin->is_leaf = true;
  begin->key_n = 0;
  root = begin;
  size_est = 0;
}

template <class key_t, class val_t>
AltBtreeBuffer<key_t, val_t>::~AltBtreeBuffer() {
  for (size_t b_i = 0; b_i < allocated_blocks.size(); ++b_i) {
    std::free(allocated_blocks[b_i]);
  }
}

template <class key_t, class val_t>
inline bool AltBtreeBuffer<key_t, val_t>::get(const key_t &key, val_t &val) {
  uint64_t leaf_ver;
  leaf_t *leaf_ptr = locate_leaf(key, leaf_ver);

  while (true) {
    int slot = leaf_ptr->find_first_larger_than_or_equal_to(key);
    bool res = (slot < leaf_ptr->key_n && leaf_ptr->keys[slot] == key)
                   ? leaf_ptr->vals[slot].read_ignoring_ptr(val)
                   : false;
    memory_fence();
    bool locked = leaf_ptr->locked == 1;
    memory_fence();
    bool version_changed = leaf_ver != leaf_ptr->version;

    if (!locked && !version_changed) {
      return res;
    } else {
      // the node is changed, possibly split, so need to check its next
      leaf_ver = leaf_ptr->version;  // read version before reading next
      memory_fence();
      leaf_t *next_ptr = leaf_ptr->next;  // in case this pointer changes
      while (next_ptr && next_ptr->keys[0] <= key) {
        leaf_ptr = next_ptr;
        leaf_ver = leaf_ptr->version;
        memory_fence();
        next_ptr = leaf_ptr->next;
      }
    }
  }
}

template <class key_t, class val_t>
inline bool AltBtreeBuffer<key_t, val_t>::update(const key_t &key,
                                                 const val_t &val) {
  leaf_t *leaf_ptr = locate_leaf_locked(key);
  int slot = leaf_ptr->find_first_larger_than_or_equal_to(key);
  bool res = (slot < leaf_ptr->key_n && leaf_ptr->keys[slot] == key)
                 ? leaf_ptr->vals[slot].update_ignoring_ptr(val)
                 : false;
  // no version changed
  leaf_ptr->unlock();
  return res;
}

template <class key_t, class val_t>
inline void AltBtreeBuffer<key_t, val_t>::insert(const key_t &key,
                                                 const val_t &val) {
  leaf_t *leaf_ptr = locate_leaf_locked(key);
  insert_leaf(key, val, leaf_ptr);  // lock is released within
}

template <class key_t, class val_t>
inline bool AltBtreeBuffer<key_t, val_t>::remove(const key_t &key) {
  leaf_t *leaf_ptr = locate_leaf_locked(key);
  int slot = leaf_ptr->find_first_larger_than_or_equal_to(key);
  bool res = (slot < leaf_ptr->key_n && leaf_ptr->keys[slot] == key)
                 ? leaf_ptr->vals[slot].remove_ignoring_ptr()
                 : false;
  // no version changed
  leaf_ptr->unlock();
  return res;
}

template <class key_t, class val_t>
inline size_t AltBtreeBuffer<key_t, val_t>::scan(
    const key_t &key_begin, const size_t n,
    std::vector<std::pair<key_t, val_t>> &result) {
  result.clear();
  DataSource source(key_begin, this);
  source.advance_to_next_valid();
  size_t remaining = n;
  while (source.has_next && remaining) {
    result.push_back(
        std::pair<key_t, val_t>(source.get_key(), source.get_val()));
    source.advance_to_next_valid();
    remaining--;
  }

  return n - remaining;
}

template <class key_t, class val_t>
inline void AltBtreeBuffer<key_t, val_t>::range_scan(
    const key_t &key_begin, const key_t &key_end,
    std::vector<std::pair<key_t, val_t>> &result) {
  COUT_N_EXIT("not implemented yet");
}

template <class key_t, class val_t>
inline uint32_t AltBtreeBuffer<key_t, val_t>::size() {
  return size_est;
}

template <class key_t, class val_t>
inline typename AltBtreeBuffer<key_t, val_t>::leaf_t *
AltBtreeBuffer<key_t, val_t>::locate_leaf(key_t key, uint64_t &leaf_ver) {
retry:
  // first make sure we start with correct root
  node_t *node_ptr = root;
  uint64_t node_ver = node_ptr->version;
  memory_fence();
  if (node_ptr->parent != nullptr) {
    goto retry;
  }

  while (true) {
    if (node_ptr->is_leaf) {
      leaf_ver = node_ver;
      return (leaf_t *)node_ptr;
    }

    node_t *next_ptr = ((internal_t *)node_ptr)->find_child(key);
    uint64_t next_ver = next_ptr->version;  // read child's version before
    memory_fence();  // validating parent's to avoid reading stale child
    bool locked = node_ptr->locked == 1;
    memory_fence();
    bool version_changed = node_ver != node_ptr->version;

    if (!locked && !version_changed) {
      node_ptr = next_ptr;
      node_ver = next_ver;
    } else {
      goto retry;
    }
  }
}

template <class key_t, class val_t>
inline typename AltBtreeBuffer<key_t, val_t>::leaf_t *
AltBtreeBuffer<key_t, val_t>::locate_leaf_locked(key_t key) {
  uint64_t leaf_ver;
  leaf_t *leaf_ptr = locate_leaf(key, leaf_ver);

  while (true) {
    leaf_ptr->lock();
    if (leaf_ver != leaf_ptr->version) {
      // the node is changed, possibly split, so need to check its next
      leaf_ptr->node_t::unlock();
      leaf_ver = leaf_ptr->version;  // read version before reading next
      memory_fence();
      leaf_t *next_ptr = leaf_ptr->next;  // in case this pointer changes
      while (next_ptr && next_ptr->keys[0] <= key) {
        leaf_ptr = next_ptr;
        leaf_ver = leaf_ptr->version;
        memory_fence();
        next_ptr = leaf_ptr->next;
      }
    } else {
      break;
    }
  }

  return leaf_ptr;
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::insert_leaf(const key_t &key,
                                               const val_t &val,
                                               leaf_t *target) {
  // first try to update inplace (without modifying mem layout)
  int slot = target->find_first_larger_than_or_equal_to(key);
  if (slot < target->key_n && target->keys[slot] == key) {
    if (target->vals[slot].update_ignoring_ptr(val)) {
      memory_fence();
      target->unlock();  // didn't insert anything
      return;
    } else {
      target->vals[slot] = atomic_val_t(val);

      memory_fence();
      target->version++;
      memory_fence();
      target->unlock();
      return;
    }
  }

  // can't update inplace/insert by overwriting
  if (!target->is_full()) {
    target->move_keys_backward(slot, 1);
    target->move_vals_backward(slot, 1);
    target->keys[slot] = key;
    target->vals[slot] = atomic_val_t(val);
    target->key_n++;

    memory_fence();
    target->version++;
    memory_fence();
    target->unlock();
    size_est++;
    return;
  } else {
    split_n_insert_leaf(key, val, slot, target);
    size_est++;
  }
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::split_n_insert_leaf(const key_t &insert_key,
                                                       const val_t &val,
                                                       int slot,
                                                       leaf_t *target) {
  node_t *node_ptr = target;
  node_t *sib_ptr = allocate_leaf();
  sib_ptr->lock();
  sib_ptr->is_leaf = true;

  int mid = node_ptr->key_n / 2;
  if (slot >= mid /* if insert to the second node */ &&
      mid < node_ptr->key_n - mid /* and it is larger  */) {
    mid++;
  }
  if (slot == node_ptr->key_n) {  // if insert to the end
    mid = node_ptr->key_n;        // then split at the end
  }

  if (slot >= mid) {  // insert to the new sibling
    node_ptr->copy_keys(mid, slot, sib_ptr->keys);
    node_ptr->copy_keys(slot, sib_ptr->keys + slot - mid + 1);
    ((leaf_t *)node_ptr)->copy_vals(mid, slot, ((leaf_t *)sib_ptr)->vals);
    ((leaf_t *)node_ptr)
        ->copy_vals(slot, ((leaf_t *)sib_ptr)->vals + slot - mid + 1);

    sib_ptr->keys[slot - mid] = insert_key;
    ((leaf_t *)sib_ptr)->vals[slot - mid] = atomic_val_t(val);

    sib_ptr->key_n = node_ptr->key_n - mid + 1;
    node_ptr->key_n = mid;
  } else {  // insert to old leaf
    node_ptr->copy_keys(mid, sib_ptr->keys);
    ((leaf_t *)node_ptr)->copy_vals(mid, ((leaf_t *)sib_ptr)->vals);
    node_ptr->move_keys_backward(slot, mid, 1);
    ((leaf_t *)node_ptr)->move_vals_backward(slot, mid, 1);

    node_ptr->keys[slot] = insert_key;
    ((leaf_t *)node_ptr)->vals[slot] = atomic_val_t(val);

    sib_ptr->key_n = node_ptr->key_n - mid;
    node_ptr->key_n = mid + 1;
  }
  memory_fence();  // sibling needs to have right data before visible
  ((leaf_t *)sib_ptr)->next = ((leaf_t *)node_ptr)->next;
  ((leaf_t *)node_ptr)->next = (leaf_t *)sib_ptr;

  assert(sib_ptr->is_leaf);
  assert(sib_ptr->locked);
  assert(node_ptr->is_leaf);
  assert(node_ptr->locked);
  assert(sib_ptr->key_n + node_ptr->key_n == alt_buf_fanout);
  assert(sib_ptr->keys[0] > node_ptr->keys[node_ptr->key_n - 1]);
  for (int child_i = 0; child_i < node_ptr->key_n - 1; child_i++) {
    assert(node_ptr->keys[child_i] < node_ptr->keys[child_i + 1]);
  }
  for (int child_i = 0; child_i < sib_ptr->key_n - 1; child_i++) {
    assert(sib_ptr->keys[child_i] < sib_ptr->keys[child_i + 1]);
  }
  key_t split_key = sib_ptr->keys[0];

  while (true) {
    // lock the right parent
    internal_t *parent_ptr;
    while (true) {
      parent_ptr = node_ptr->parent;
      if (!parent_ptr) {
        break;
      }
      parent_ptr->lock();
      if (parent_ptr != node_ptr->parent) {  // when parent is split by others
        parent_ptr->node_t::unlock();
      } else {
        break;
      }
    }

    if (parent_ptr == nullptr) {
      parent_ptr = allocate_internal();
      parent_ptr->lock();
      parent_ptr->is_leaf = false;

      parent_ptr->keys[0] = split_key;
      ((internal_t *)parent_ptr)->children[0] = node_ptr;
      ((internal_t *)parent_ptr)->children[1] = sib_ptr;
      parent_ptr->key_n = 1;

      for (int child_i = 0; child_i < parent_ptr->key_n - 1; child_i++) {
        assert(parent_ptr->keys[child_i] < parent_ptr->keys[child_i + 1]);
      }

      node_ptr->parent = parent_ptr;
      sib_ptr->parent = parent_ptr;
      root = parent_ptr;
      memory_fence();
      parent_ptr->version++;
      node_ptr->version++;
      sib_ptr->version++;
      memory_fence();
      parent_ptr->node_t::unlock();
      node_ptr->node_t::unlock();
      sib_ptr->node_t::unlock();
      return;
    } else if (!parent_ptr->is_full()) {
      int slot = parent_ptr->find_first_larger_than(split_key);
      assert(parent_ptr->find_first_larger_than(split_key) ==
             parent_ptr->find_first_larger_than_or_equal_to(split_key));

      parent_ptr->move_keys_backward(slot, 1);
      ((internal_t *)parent_ptr)->move_children_backward(slot + 1, 1);
      parent_ptr->keys[slot] = split_key;
      ((internal_t *)parent_ptr)->children[slot + 1] = sib_ptr;

      parent_ptr->key_n++;
      sib_ptr->parent = parent_ptr;

      assert(parent_ptr->is_leaf == false);
      assert(parent_ptr->locked);
      assert(parent_ptr->key_n <= alt_buf_fanout - 1);

      for (int child_i = 0; child_i < parent_ptr->key_n - 1; child_i++) {
        assert(parent_ptr->keys[child_i] < parent_ptr->keys[child_i + 1]);
      }
      for (int child_i = 0; child_i < parent_ptr->key_n; child_i++) {
        assert(((internal_t *)parent_ptr)
                   ->children[child_i]
                   ->keys[((internal_t *)parent_ptr)->children[child_i]->key_n -
                          1] < parent_ptr->keys[child_i]);
        assert(((internal_t *)parent_ptr)->children[child_i + 1]->keys[0] >=
               parent_ptr->keys[child_i]);
      }
      for (int child_i = 0; child_i < parent_ptr->key_n + 1; child_i++) {
        assert(((internal_t *)parent_ptr)->children[child_i]->parent ==
               (internal_t *)parent_ptr);
      }

      memory_fence();
      parent_ptr->version++;
      node_ptr->version++;
      sib_ptr->version++;
      memory_fence();
      parent_ptr->node_t::unlock();
      node_ptr->node_t::unlock();
      sib_ptr->node_t::unlock();
      return;
    } else {  // recursive split, knock the parent as the new node to split
      node_t *prev_sib_ptr = sib_ptr;
      node_t *prev_node_ptr = node_ptr;
      key_t prev_split_key = split_key;

      node_ptr = parent_ptr;
      sib_ptr = allocate_internal();
      sib_ptr->lock();
      sib_ptr->is_leaf = false;

      int slot = node_ptr->find_first_larger_than(prev_split_key);
      assert(node_ptr->find_first_larger_than(prev_split_key) ==
             node_ptr->find_first_larger_than_or_equal_to(prev_split_key));
      int mid = node_ptr->key_n / 2;
      if (slot < mid /* if insert to the first node */ &&
          mid > node_ptr->key_n - (mid + 1) /* and it is larger  */) {
        mid--;
      }
      if (slot == node_ptr->key_n) {  // if insert to the end
        mid = node_ptr->key_n - 1;    // then split at the end
      }

      if (slot >= mid) {    // insert to the new sibling
        if (slot == mid) {  // prev split key will be the new split key

          node_ptr->copy_keys(mid, sib_ptr->keys);
          ((internal_t *)node_ptr)
              ->copy_children(mid + 1, ((internal_t *)sib_ptr)->children + 1);
          ((internal_t *)sib_ptr)->children[0] = prev_sib_ptr;

          split_key = prev_split_key;
        } else {  // mid key will be new split key

          node_ptr->copy_keys(mid + 1, slot, sib_ptr->keys);
          node_ptr->copy_keys(slot, sib_ptr->keys + slot - mid);
          ((internal_t *)node_ptr)
              ->copy_children(mid + 1, slot + 1,
                              ((internal_t *)sib_ptr)->children);
          ((internal_t *)node_ptr)
              ->copy_children(
                  slot + 1, ((internal_t *)sib_ptr)->children + slot - mid + 1);
          sib_ptr->keys[slot - mid - 1] = prev_split_key;
          ((internal_t *)sib_ptr)->children[slot - mid] = prev_sib_ptr;

          split_key = node_ptr->keys[mid];
        }

        sib_ptr->key_n = node_ptr->key_n - mid;
        node_ptr->key_n = mid;

      } else {  // insert to old leaf

        node_ptr->copy_keys(mid + 1, sib_ptr->keys);
        ((internal_t *)node_ptr)
            ->copy_children(mid + 1, ((internal_t *)sib_ptr)->children);

        split_key = node_ptr->keys[mid];  // copy it before moving

        node_ptr->move_keys_backward(slot, mid, 1);
        ((internal_t *)node_ptr)->move_children_backward(slot + 1, mid + 1, 1);

        node_ptr->keys[slot] = prev_split_key;
        ((internal_t *)node_ptr)->children[slot + 1] = prev_sib_ptr;

        prev_sib_ptr->parent = (internal_t *)node_ptr;
        sib_ptr->key_n = node_ptr->key_n - mid - 1;
        node_ptr->key_n = mid + 1;
      }
      for (int child_i = 0; child_i < sib_ptr->key_n + 1; child_i++) {
        ((internal_t *)sib_ptr)->children[child_i]->parent =
            (internal_t *)sib_ptr;
      }

      memory_fence();
      prev_node_ptr->version++;
      prev_sib_ptr->version++;
      memory_fence();
      prev_node_ptr->node_t::unlock();
      prev_sib_ptr->node_t::unlock();

      assert(sib_ptr->is_leaf == false);
      assert(sib_ptr->locked);
      assert(node_ptr->is_leaf == false);
      assert(node_ptr->locked);
      assert(sib_ptr->key_n + node_ptr->key_n == alt_buf_fanout - 1);
      assert(sib_ptr->keys[0] > node_ptr->keys[node_ptr->key_n - 1]);

      for (int child_i = 0; child_i < node_ptr->key_n - 1; child_i++) {
        assert(node_ptr->keys[child_i] < node_ptr->keys[child_i + 1]);
      }
      for (int child_i = 0; child_i < node_ptr->key_n; child_i++) {
        assert(
            ((internal_t *)node_ptr)
                ->children[child_i]
                ->keys[((internal_t *)node_ptr)->children[child_i]->key_n - 1] <
            node_ptr->keys[child_i]);
        assert(((internal_t *)node_ptr)->children[child_i + 1]->keys[0] >=
               node_ptr->keys[child_i]);
      }
      for (int child_i = 0; child_i < node_ptr->key_n + 1; child_i++) {
        assert(((internal_t *)node_ptr)->children[child_i]->parent ==
               (internal_t *)node_ptr);
      }

      for (int child_i = 0; child_i < sib_ptr->key_n - 1; child_i++) {
        assert(sib_ptr->keys[child_i] < sib_ptr->keys[child_i + 1]);
      }
      for (int child_i = 0; child_i < sib_ptr->key_n; child_i++) {
        assert(
            ((internal_t *)sib_ptr)
                ->children[child_i]
                ->keys[((internal_t *)sib_ptr)->children[child_i]->key_n - 1] <
            sib_ptr->keys[child_i]);
        assert(((internal_t *)sib_ptr)->children[child_i + 1]->keys[0] >=
               sib_ptr->keys[child_i]);
      }
      for (int child_i = 0; child_i < sib_ptr->key_n + 1; child_i++) {
        assert(((internal_t *)sib_ptr)->children[child_i]->parent ==
               (internal_t *)sib_ptr);
      }
    }
  }
}

template <class key_t, class val_t>
inline void AltBtreeBuffer<key_t, val_t>::allocate_new_block() {
  uint8_t *p = (uint8_t *)std::malloc(node_n_per_block * node_size);
  allocated_blocks.push_back(p);
}

template <class key_t, class val_t>
inline uint8_t *AltBtreeBuffer<key_t, val_t>::allocate_node() {
  size_t index = available_node_index();
  size_t block_i = index / node_n_per_block;
  size_t node_i = index % node_n_per_block;
  return allocated_blocks[block_i] + node_size * node_i;
}

template <class key_t, class val_t>
inline typename AltBtreeBuffer<key_t, val_t>::internal_t *
AltBtreeBuffer<key_t, val_t>::allocate_internal() {
  alloc_mut.lock();
  internal_t *node_ptr = (internal_t *)allocate_node();
  alloc_mut.unlock();
  new (node_ptr) internal_t();
  return node_ptr;
}

template <class key_t, class val_t>
inline typename AltBtreeBuffer<key_t, val_t>::leaf_t *
AltBtreeBuffer<key_t, val_t>::allocate_leaf() {
  alloc_mut.lock();
  leaf_t *node_ptr = (leaf_t *)allocate_node();
  alloc_mut.unlock();
  new (node_ptr) leaf_t();
  return node_ptr;
}

template <class key_t, class val_t>
inline size_t AltBtreeBuffer<key_t, val_t>::available_node_index() {
  if (next_node_i % node_n_per_block == 0) {
    allocate_new_block();  // make sure next_node_i is valid
  }
  size_t id = next_node_i;
  next_node_i++;
  return id;
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Node::lock() {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->locked, unlocked, locked) !=
                  unlocked))
    ;
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Node::unlock() {
  locked = 0;
}

template <class key_t, class val_t>
int AltBtreeBuffer<key_t, val_t>::Node::find_first_larger_than_or_equal_to(
    const key_t &key) {
  uint8_t key_n = this->key_n;
  uint8_t begin_i = 0, end_i = key_n;
  uint8_t mid = (begin_i + end_i) / 2;

  // return the first pivot key > input key
  while (end_i > begin_i) {
    if (keys[mid] >= key) {
      end_i = mid;
    } else {
      begin_i = mid + 1;
    }
    mid = (begin_i + end_i) / 2;
  }
  return mid;
}

template <class key_t, class val_t>
int AltBtreeBuffer<key_t, val_t>::Node::find_first_larger_than(
    const key_t &key) {
  uint8_t key_n = this->key_n;
  uint8_t begin_i = 0, end_i = key_n;
  uint8_t mid = (begin_i + end_i) / 2;

  // return the first pivot key > input key
  while (end_i > begin_i) {
    if (keys[mid] > key) {
      end_i = mid;
    } else {
      begin_i = mid + 1;
    }
    mid = (begin_i + end_i) / 2;
  }
  return mid;
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Node::move_keys_backward(int begin, int end,
                                                            int off) {
  std::copy_backward(keys + begin, keys + end, keys + end + off);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Node::move_keys_backward(int begin,
                                                            int off) {
  std::copy_backward(keys + begin, keys + key_n, keys + key_n + off);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Node::copy_keys(int begin, int end,
                                                   key_t *dst) {
  std::copy(keys + begin, keys + end, dst);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Node::copy_keys(int begin, key_t *dst) {
  std::copy(keys + begin, keys + key_n, dst);
}

template <class key_t, class val_t>
typename AltBtreeBuffer<key_t, val_t>::node_t *
AltBtreeBuffer<key_t, val_t>::Internal::find_child(const key_t &key) {
  return children[node_t::find_first_larger_than(key)];
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Internal::move_children_backward(int begin,
                                                                    int end,
                                                                    int off) {
  std::copy_backward(children + begin, children + end, children + end + off);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Internal::move_children_backward(int begin,
                                                                    int off) {
  std::copy_backward(children + begin, children + key_n + 1,
                     children + key_n + 1 + off);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Internal::copy_children(int begin, int end,
                                                           node_t **dst) {
  std::copy(children + begin, children + end, dst);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Internal::copy_children(int begin,
                                                           node_t **dst) {
  std::copy(children + begin, children + key_n + 1, dst);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Leaf::move_vals_backward(int begin, int end,
                                                            int off) {
  std::copy_backward(vals + begin, vals + end, vals + end + off);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Leaf::move_vals_backward(int begin,
                                                            int off) {
  std::copy_backward(vals + begin, vals + key_n, vals + key_n + off);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Leaf::copy_vals(int begin, int end,
                                                   atomic_val_t *dst) {
  std::copy(vals + begin, vals + end, dst);
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::Leaf::copy_vals(int begin,
                                                   atomic_val_t *dst) {
  std::copy(vals + begin, vals + key_n, dst);
}

template <class key_t, class val_t>
AltBtreeBuffer<key_t, val_t>::DataSource::DataSource(key_t begin,
                                                     AltBtreeBuffer *buffer) {
  uint64_t leaf_ver;
  leaf_t *leaf_ptr = buffer->locate_leaf(begin, leaf_ver);

  while (true) {
    int slot = leaf_ptr->find_first_larger_than_or_equal_to(begin);
    int key_n = leaf_ptr->key_n;
    pos = 0;
    for (int i = slot; i < key_n; i++) {
      if (leaf_ptr->vals[i].read_ignoring_ptr(vals[pos])) {
        keys[pos] = leaf_ptr->keys[i];
        pos++;
      }
    }
    next = leaf_ptr->next;
    memory_fence();
    bool locked = leaf_ptr->locked == 1;
    memory_fence();
    bool version_changed = leaf_ver != leaf_ptr->version;

    if (!locked && !version_changed) {
      n = pos;
      pos = -1;  // because advance_to_next_valid assumes `pos` is already read
      return;
    } else {
      // the node is changed, possibly split, so need to check its next
      leaf_ver = leaf_ptr->version;  // read version before reading next
      memory_fence();
      leaf_t *next_ptr = leaf_ptr->next;  // in case this pointer changes
      while (next_ptr && next_ptr->keys[0] <= begin) {
        leaf_ptr = next_ptr;
        leaf_ver = leaf_ptr->version;
        memory_fence();
        next_ptr = leaf_ptr->next;
      }
    }
  }
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::DataSource::advance_to_next_valid() {
  if (pos < n - 1) {
    pos++;
    has_next = true;
  } else {
    while (true) {
      if (next == nullptr) {
        has_next = false;
        return;
      }

      leaf_t *leaf_ptr = next;
      uint64_t leaf_ver = leaf_ptr->version;
      memory_fence();

      while (true) {
        int key_n = leaf_ptr->key_n;
        pos = 0;
        for (int i = 0; i < key_n; i++) {
          if (leaf_ptr->vals[i].read_ignoring_ptr(vals[pos])) {
            keys[pos] = leaf_ptr->keys[i];
            pos++;
          }
        }
        next = leaf_ptr->next;
        memory_fence();
        bool locked = leaf_ptr->locked == 1;
        memory_fence();
        bool version_changed = leaf_ver != leaf_ptr->version;

        if (!locked && !version_changed) {
          n = pos;
          pos = 0;
          if (n != 0) {
            has_next = true;
            return;
          } else {
            break;  // break from node reading, but continue reading next node
                    // since this node is empty
          }
        } else {
          // the node is changed, possibly split, so need to check its next
          leaf_ver = leaf_ptr->version;  // but we don't care the splits
          memory_fence();
        }
      }
    }
  }
}

template <class key_t, class val_t>
const key_t &AltBtreeBuffer<key_t, val_t>::DataSource::get_key() {
  return keys[pos];
}

template <class key_t, class val_t>
const val_t &AltBtreeBuffer<key_t, val_t>::DataSource::get_val() {
  return vals[pos];
}

template <class key_t, class val_t>
AltBtreeBuffer<key_t, val_t>::RefSource::RefSource(AltBtreeBuffer *buffer)
    : next(buffer->begin) {
  assert(next);
  uint64_t ver;
  UNUSED(ver);
  assert(next == buffer->locate_leaf(key_t::min(), ver));
}

template <class key_t, class val_t>
void AltBtreeBuffer<key_t, val_t>::RefSource::advance_to_next_valid() {
  if (pos < n - 1) {  // pre-fetched array is available for next read
    pos++;
    has_next = true;
  } else {          // fetch from next leaves
    while (true) {  // because next leaf might be empty (all logical removed)
      if (next == nullptr) {
        has_next = false;
        return;
      }

      leaf_t *leaf_ptr = next;
      int key_n = leaf_ptr->key_n;

      pos = 0;
      val_t temp_val;
      for (int i = 0; i < key_n; i++) {
        if (leaf_ptr->vals[i].read_ignoring_ptr(temp_val)) {
          keys[pos] = leaf_ptr->keys[i];
          val_ptrs[pos] = &(leaf_ptr->vals[i]);
          pos++;
        } else {
        }
      }
      n = pos;
      pos = 0;
      next = leaf_ptr->next;
      if (n != 0) {
        has_next = true;
        break;
      }
    }
  }
}

template <class key_t, class val_t>
const key_t &AltBtreeBuffer<key_t, val_t>::RefSource::get_key() {
  return keys[pos];
}

template <class key_t, class val_t>
typename AltBtreeBuffer<key_t, val_t>::RefSource::atomic_val_t &
AltBtreeBuffer<key_t, val_t>::RefSource::get_val() {
  return *(val_ptrs[pos]);
}

}  // namespace xindex

#endif  // xindex_buffer_IMPL_H
