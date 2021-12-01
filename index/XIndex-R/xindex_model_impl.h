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

#include "xindex_model.h"

#if !defined(XINDEX_MODEL_IMPL_H)
#define XINDEX_MODEL_IMPL_H

namespace xindex {

template <class key_t>
void LinearModel<key_t>::prepare(const std::vector<key_t> &keys,
                                 const std::vector<size_t> &positions) {
  assert(keys.size() == positions.size());
  if (keys.size() == 0) return;

  std::vector<model_key_t> model_keys(keys.size());
  std::vector<double *> key_ptrs(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    model_keys[i] = keys[i].to_model_key();
    key_ptrs[i] = model_keys[i].data();
  }

  prepare_model(key_ptrs, positions);
}

template <class key_t>
void LinearModel<key_t>::prepare(
    const typename std::vector<key_t>::const_iterator &keys_begin,
    uint32_t size) {
  if (size == 0) return;

  std::vector<model_key_t> model_keys(size);
  std::vector<double *> key_ptrs(size);
  std::vector<size_t> positions(size);
  for (size_t i = 0; i < size; i++) {
    model_keys[i] = (keys_begin + i)->to_model_key();
    key_ptrs[i] = model_keys[i].data();
    positions[i] = i;
  }

  prepare_model(key_ptrs, positions);
}

template <class key_t>
void LinearModel<key_t>::prepare_model(
    const std::vector<double *> &model_key_ptrs,
    const std::vector<size_t> &positions) {
  size_t key_len = key_t::model_key_size();
  if (positions.size() == 0) return;
  if (positions.size() == 1) {
    LinearModel<key_t>::weights[key_len] = positions[0];
    return;
  }

  if (key_len == 1) {  // use multiple dimension LR when running tpc-c
    double x_expected = 0, y_expected = 0, xy_expected = 0,
           x_square_expected = 0;
    for (size_t key_i = 0; key_i < positions.size(); key_i++) {
      double key = model_key_ptrs[key_i][0];
      x_expected += key;
      y_expected += positions[key_i];
      x_square_expected += key * key;
      xy_expected += key * positions[key_i];
    }
    x_expected /= positions.size();
    y_expected /= positions.size();
    x_square_expected /= positions.size();
    xy_expected /= positions.size();

    weights[0] = (xy_expected - x_expected * y_expected) /
                 (x_square_expected - x_expected * x_expected);
    weights[1] = (x_square_expected * y_expected - x_expected * xy_expected) /
                 (x_square_expected - x_expected * x_expected);
    return;
  }

  // trim down samples to avoid large memory usage
  size_t step = 1;
  if (model_key_ptrs.size() > desired_training_key_n) {
    step = model_key_ptrs.size() / desired_training_key_n;
  }

  std::vector<size_t> useful_feat_index;
  for (size_t feat_i = 0; feat_i < key_len; feat_i++) {
    double first_val = model_key_ptrs[0][feat_i];
    for (size_t key_i = 0; key_i < model_key_ptrs.size(); key_i += step) {
      if (model_key_ptrs[key_i][feat_i] != first_val) {
        useful_feat_index.push_back(feat_i);
        break;
      }
    }
  }
  if (model_key_ptrs.size() != 1 && useful_feat_index.size() == 0) {
    COUT_THIS("all feats are the same");
  }
  size_t useful_feat_n = useful_feat_index.size();
  bool use_bias = true;

  // we may need multiple runs to avoid "not full rank" error
  int fitting_res = -1;
  while (fitting_res != 0) {
    // use LAPACK to solve least square problem, i.e., to minimize ||b-Ax||_2
    // where b is the actual positions, A is inputmodel_keys
    int m = model_key_ptrs.size() / step;                  // number of samples
    int n = use_bias ? useful_feat_n + 1 : useful_feat_n;  // number of features
    double *a = (double *)malloc(m * n * sizeof(double));
    double *b = (double *)malloc(std::max(m, n) * sizeof(double));
    if (a == nullptr || b == nullptr) {
      COUT_N_EXIT("cannot allocate memory for matrix a or b");
    }

    for (int sample_i = 0; sample_i < m; ++sample_i) {
      // we only fit with useful features
      for (size_t useful_feat_i = 0; useful_feat_i < useful_feat_n;
           useful_feat_i++) {
        a[sample_i * n + useful_feat_i] =
            model_key_ptrs[sample_i * step][useful_feat_index[useful_feat_i]];
      }
      if (use_bias) {
        a[sample_i * n + useful_feat_n] = 1;  // the extra 1
      }
      b[sample_i] = positions[sample_i * step];
      assert(sample_i * step < model_key_ptrs.size());
    }

    // fill the rest of b when m < n, otherwise nan value will cause failure
    for (int b_i = m; b_i < n; b_i++) {
      b[b_i] = 0;
    }

    fitting_res = LAPACKE_dgels(LAPACK_ROW_MAJOR, 'N', m, n, 1 /* nrhs */, a,
                                n /* lda */, b, 1 /* ldb, i.e. nrhs */);

    if (fitting_res > 0) {
      // now we need to remove one column in matrix a
      // note that fitting_res indexes starting with 1
      if ((size_t)fitting_res > useful_feat_index.size()) {
        use_bias = false;
      } else {
        size_t feat_i = fitting_res - 1;
        useful_feat_index.erase(useful_feat_index.begin() + feat_i);
        useful_feat_n = useful_feat_index.size();
      }

      if (useful_feat_index.size() == 0 && use_bias == false) {
        COUT_N_EXIT(
            "impossible! cannot fail when there is only 1 bias column in "
            "matrix a");
      }
    } else if (fitting_res < 0) {
      printf("%i-th parameter had an illegal value\n", -fitting_res);
      exit(-2);
    }

    // set weights to all zero
    for (size_t weight_i = 0; weight_i < weights.size(); weight_i++) {
      weights[weight_i] = 0;
    }
    // set weights of useful features
    for (size_t useful_feat_i = 0; useful_feat_i < useful_feat_index.size();
         useful_feat_i++) {
      weights[useful_feat_index[useful_feat_i]] = b[useful_feat_i];
    }
    // set bias
    if (use_bias) {
      size_t key_len = key_t::model_key_size();
      weights[key_len] = b[n - 1];
    }

    free(a);
    free(b);
  }
  assert(fitting_res == 0);
}

template <class key_t>
size_t LinearModel<key_t>::predict(const key_t &key) const {
  model_key_t model_key = key.to_model_key();
  double *model_key_ptr = model_key.data();

  size_t key_len = key_t::model_key_size();
  if (key_len == 1) {
    double res = weights[0] * *model_key_ptr + weights[1];
    return res > 0 ? res : 0;
  } else {
    double res = 0;
    for (size_t feat_i = 0; feat_i < key_len; feat_i++) {
      res += weights[feat_i] * model_key_ptr[feat_i];
    }
    res += weights[key_len];  // the bias term
    return res > 0 ? res : 0;
  }
}

template <class key_t>
size_t LinearModel<key_t>::get_error_bound(
    const std::vector<key_t> &keys, const std::vector<size_t> &positions) {
  int max = 0;

  for (size_t key_i = 0; key_i < keys.size(); ++key_i) {
    long long int pos_actual = positions[key_i];
    long long int pos_pred = predict(keys[key_i]);
    int error = std::abs(pos_actual - pos_pred);

    if (error > max) {
      max = error;
    }
  }

  return max;
}

template <class key_t>
size_t LinearModel<key_t>::get_error_bound(
    const typename std::vector<key_t>::const_iterator &keys_begin,
    uint32_t size) {
  int max = 0;

  for (size_t key_i = 0; key_i < size; ++key_i) {
    long long int pos_actual = key_i;
    long long int pos_pred = predict(*(keys_begin + key_i));
    int error = std::abs(pos_actual - pos_pred);

    if (error > max) {
      max = error;
    }
  }

  return max;
}

}  // namespace xindex

#endif  // XINDEX_MODEL_IMPL_H
