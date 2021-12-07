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

#include "helper.h"
#include "xindex.h"
#include "xindex_buffer_impl.h"
#include "xindex_group_impl.h"
#include "xindex_model_impl.h"
#include "xindex_root_impl.h"

#if !defined(XINDEX_IMPL_Hh)
#define XINDEX_IMPL_Hh

namespace xindexh {

template <class key_t, class val_t>
XIndex<key_t, val_t>::XIndex(const std::vector<key_t> &keys,
                             const std::vector<val_t> &vals, size_t worker_num,
                             size_t bg_n)
    : bg_num(bg_n) {
  config.worker_n = worker_num;
  // sanity checks
  INVARIANT(config.hash_resize_tolerance_upper > 0);
  INVARIANT(config.hash_resize_tolerance_lower > 0);
  INVARIANT(config.hash_init_conflict_threshold > 0);
  INVARIANT(config.worker_n > 0);

  for (size_t key_i = 1; key_i < keys.size(); key_i++) {
    assert(keys[key_i] >= keys[key_i - 1]);
  }
  rcu_init();

  root = new root_t();
  root->init(keys, vals);
  start_bg();
}

template <class key_t, class val_t>
XIndex<key_t, val_t>::~XIndex() {
  terminate_bg();
}
    template <class key_t, class val_t>
    uint64_t XIndex<key_t, val_t>::GetIndexSize(){
        return 0;
    }
    using KeyValueOffset=viper::index::KeyValueOffset;
    template <class key_t, class val_t>
    XIndexH<key_t, val_t>::XIndexH(const std::vector<key_t> &keys,const std::vector<val_t> &vals,size_t worker_num, size_t bg_n){
        std::vector<viper::index::KeyForXindex> ks;
        for(key_t k:keys){
            ks.push_back(viper::index::KeyForXindex(k));
        }
        this->x = new xindexh::XIndex<viper::index::KeyForXindex,val_t>{ks,vals,worker_num,bg_n};
    }
    template <class key_t, class val_t>
    XIndexH<key_t, val_t>::~XIndexH(){
        delete this->x;
    }
    template <class key_t, class val_t>
    uint64_t XIndexH<key_t, val_t>::GetIndexSize(){
        return this->x->GetIndexSize();
    }
    template <class key_t, class val_t>
    KeyValueOffset XIndexH<key_t, val_t>::CoreInsert(const key_t & k, viper::index::KeyValueOffset offset,uint32_t thread_id){
        this->x->put(viper::index::KeyForXindex(k),offset,thread_id);
        return KeyValueOffset::NONE();
    }
    template <class key_t, class val_t>
    KeyValueOffset XIndexH<key_t, val_t>::CoreGet(const key_t & k,uint32_t thread_id){
        KeyValueOffset o ;
        this->x->get(viper::index::KeyForXindex(k),o,thread_id);
        return o;
    }

template <class key_t, class val_t>
inline bool XIndex<key_t, val_t>::get(const key_t &key, val_t &val,
                                      const uint32_t worker_id) {
  rcu_progress(worker_id);
  return root->get(key, val) == result_t::ok;
}

template <class key_t, class val_t>
inline bool XIndex<key_t, val_t>::put(const key_t &key, const val_t &val,
                                      const uint32_t worker_id) {
  result_t res;
  rcu_progress(worker_id);
  while ((res = root->put(key, val)) == result_t::retry) {
    rcu_progress(worker_id);
  }
  return res == result_t::ok;
}

template <class key_t, class val_t>
inline bool XIndex<key_t, val_t>::remove(const key_t &key,
                                         const uint32_t worker_id) {
  rcu_progress(worker_id);
  return root->remove(key) == result_t::ok;
}

template <class key_t, class val_t>
void *XIndex<key_t, val_t>::background(void *this_) {
  volatile XIndex &index = *(XIndex *)this_;

  if (index.bg_num == 0) return nullptr;

  size_t bg_num = index.bg_num;
  std::vector<pthread_t> threads(bg_num);
  std::vector<bg_info_t> info(bg_num);
  INVARIANT(sizeof(bg_info_t) == CACHELINE_SIZE);

  for (size_t bg_i = 0; bg_i < bg_num; bg_i++) {
    info[bg_i].bg_i = bg_i;
    info[bg_i].bg_n = bg_num;
    info[bg_i].root_ptr = &(index.root);
    info[bg_i].running = true;

    int ret = pthread_create(&threads[bg_i], nullptr, root_t::do_maintenance,
                             &info[bg_i]);
    if (ret) {
      COUT_N_EXIT("Error: unable to create bg task thread, " << ret);
    }
  }

  while (index.bg_running)
    ;

  for (size_t bg_i = 0; bg_i < bg_num; bg_i++) {
    info[bg_i].running = false;
  }

  for (size_t bg_i = 0; bg_i < bg_num; bg_i++) {
    void *status;
    int rc = pthread_join(threads[bg_i], &status);
    if (rc) {
      COUT_N_EXIT("Error: unable to join," << rc);
    }
  }
  return nullptr;
}

template <class key_t, class val_t>
void XIndex<key_t, val_t>::start_bg() {
  bg_running = true;
  int ret = pthread_create(&bg_master, nullptr, background, this);
  if (ret) {
    COUT_N_EXIT("Error: unable to create background thread," << ret);
  }
}

template <class key_t, class val_t>
void XIndex<key_t, val_t>::terminate_bg() {
  bg_running = false;
  void *status;
  int rc = pthread_join(bg_master, &status);
  if (rc) {
    COUT_N_EXIT("Error: unable to join," << rc);
  }
}

}  // namespace xindex

#endif  // XINDEX_IMPL_H
