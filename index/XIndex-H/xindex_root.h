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

#include "xindex_group.h"

#if !defined(XINDEX_ROOT_Hh)
#define XINDEX_ROOT_Hh

namespace xindexh {
/*
 * HRoot
 */
template <class key_t, class val_t>
class HRoot {
  typedef LinearModel<key_t> linear_model_t;
  typedef HGroup<key_t, val_t> group_t;

  template <class key_tt, class val_tt>
  friend class XIndex;

 public:
  ~HRoot();
  void init(const std::vector<key_t> &keys, const std::vector<val_t> &vals);

  inline result_t get(const key_t &key, val_t &val);
  inline result_t put(const key_t &key, const val_t &val);
  inline result_t remove(const key_t &key);

  static void *do_maintenance(void *args);

 private:
  void train_rmi(const std::vector<key_t> &keys);
  size_t locate_group_idx(const key_t &key);
  inline group_t *locate_group(const key_t &key);
  double re_init(const std::vector<key_t> &keys, const std::vector<val_t> &vals,
                 size_t current_group_n);

  linear_model_t model_t;
  std::unique_ptr<group_t *volatile[]> groups;
  size_t group_n = 100;
  size_t table_size;
};

}  // namespace xindex

#endif  // XINDEX_ROOT_H
