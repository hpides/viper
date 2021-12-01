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

#include "mkl.h"
#include "mkl_lapacke.h"

#if !defined(XINDEX_MODEL_H)
#define XINDEX_MODEL_H

namespace xindex {

template <class key_t>
class LinearModel {
  typedef std::array<double, key_t::model_key_size()> model_key_t;
  template <class key_t_, class val_t, bool seq>
  friend class Root;

 public:
  void prepare(const std::vector<key_t> &keys,
               const std::vector<size_t> &positions);
  void prepare(const typename std::vector<key_t>::const_iterator &keys_begin,
               uint32_t size);
  void prepare_model(const std::vector<double *> &model_key_ptrs,
                     const std::vector<size_t> &positions);
  size_t predict(const key_t &key) const;
  size_t get_error_bound(const std::vector<key_t> &keys,
                         const std::vector<size_t> &positions);
  size_t get_error_bound(
      const typename std::vector<key_t>::const_iterator &keys_begin,
      uint32_t size);

 private:
  std::array<double, key_t::model_key_size() + 1> weights;
};

}  // namespace xindex

#endif  // XINDEX_MODEL_H
