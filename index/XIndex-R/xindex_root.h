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

#if !defined(XINDEX_ROOT_H)
#define XINDEX_ROOT_H

namespace xindex {

template <class key_t, class val_t, bool seq>
class Root {
  typedef LinearModel<key_t> linear_model_t;
  typedef Group<key_t, val_t, seq, max_model_n> group_t;

  template <class key_tt, class val_tt, bool sequential>
  friend class XIndex;

 public:
  ~Root();
  void init(const std::vector<key_t> &keys, const std::vector<val_t> &vals);
  void calculate_err(const std::vector<key_t> &keys,
                     const std::vector<val_t> &vals, size_t group_n_trial,
                     double &err_at_percentile, double &max_err,
                     double &avg_err);

  inline result_t get(const key_t &key, val_t &val);
  inline result_t put(const key_t &key, const val_t &val,
                      const uint32_t worker_id);
  inline result_t remove(const key_t &key);
  inline size_t scan(const key_t &begin, const size_t n,
                     std::vector<std::pair<key_t, val_t>> &result);
  inline size_t range_scan(const key_t &begin, const key_t &end,
                           std::vector<std::pair<key_t, val_t>> &result);

  static void *do_adjustment(void *args);
  Root *create_new_root();
  void trim_root();

 private:
  void adjust_rmi();
  void train_rmi(size_t rmi_2nd_stage_model_n);
  size_t pick_next_stage_model(size_t pos_pred);
  size_t predict(const key_t &key);
  inline group_t *locate_group(const key_t &key);
  inline group_t *locate_group_pt1(const key_t &key, int &group_i);
  inline group_t *locate_group_pt2(const key_t &key, group_t *begin);

  linear_model_t rmi_1st_stage;
  linear_model_t *rmi_2nd_stage = nullptr;
  std::unique_ptr<std::pair<key_t, group_t *volatile>[]> groups;
  size_t rmi_2nd_stage_model_n = 0;
  size_t group_n = 0;
};

}  // namespace xindex

#endif  // XINDEX_ROOT_H
