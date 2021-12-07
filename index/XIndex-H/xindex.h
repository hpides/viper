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

#include <memory>
#include <vector>

#include "helper.h"
#include "xindex_root.h"
#include "xindex_util.h"
#include "../common_index.hpp"

#if !defined(XINDEX_Hh)
#define XINDEX_Hh

namespace xindexh {

template <class key_t, class val_t>
class XIndex {
  typedef HRoot<key_t, val_t> root_t;

 public:
  XIndex(const std::vector<key_t> &keys, const std::vector<val_t> &vals,
         size_t worker_num, size_t bg_n);
  ~XIndex();
  uint64_t GetIndexSize();
  inline bool get(const key_t &key, val_t &val, const uint32_t worker_id);
  inline bool put(const key_t &key, const val_t &val, const uint32_t worker_id);
  inline bool remove(const key_t &key, const uint32_t worker_id);

 private:
  void start_bg();
  void terminate_bg();
  static void *background(void *this_);

  root_t *volatile root = nullptr;
  pthread_t bg_master;
  size_t bg_num;
  volatile bool bg_running = true;
};
    template<class K, class V>
    class XIndexH : public viper::index::BaseIndex<K> {
    public:
        using KeyValueOffset = viper::index::KeyValueOffset;

        XIndexH(const std::vector<K> &keys, const std::vector<V> &vals,
                size_t worker_num, size_t bg_n);

        ~XIndexH();

        uint64_t GetIndexSize();

        KeyValueOffset CoreInsert(const K &k, viper::index::KeyValueOffset offset, uint32_t thread_id);

        KeyValueOffset CoreGet(const K &k, uint32_t thread_id);

        xindexh::XIndex<viper::index::KeyForXindex,V> * x;

    };

}  // namespace xindex

#endif  // XINDEX_H
