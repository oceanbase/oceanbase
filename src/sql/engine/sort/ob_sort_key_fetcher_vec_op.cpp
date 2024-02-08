/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_sort_key_fetcher_vec_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObSortKeyFetcher::~ObSortKeyFetcher()
{
  reset();
}

void ObSortKeyFetcher::reset()
{
  int64_t sk_cnt = sk_vec_ptrs_.count();
  sk_vec_ptrs_.reset();
  if (nullptr != sk_col_res_list_) {
    for (int64_t i = 0; i < sk_cnt; ++i) {
      sk_col_res_list_[i].reset();
    }
    allocator_.free(sk_col_res_list_);
    sk_col_res_list_ = nullptr;
  }
}

int ObSortKeyFetcher::init_sk_col_result_list(const int64_t sk_cnt, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sk_col_res_list_ = static_cast<SortKeyColResult *>(
                  allocator_.alloc(sizeof(SortKeyColResult) * sk_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc sort key column result", K(ret), K(sk_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sk_cnt; i++) {
      SortKeyColResult *sk_col_res = new (&sk_col_res_list_[i]) SortKeyColResult(allocator_);
      if (OB_FAIL(sk_col_res->init(batch_size))) {
        SQL_ENG_LOG(WARN, "failed to init sort key column result", K(ret), K(batch_size));
      }
    }
  }
  return ret;
}

int ObSortKeyFetcher::init(const common::ObIArray<ObExpr *> &sk_exprs,
                          const ObIArray<ObSortFieldCollation> &sort_collations,
                          const int64_t batch_size, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  max_batch_size_ = batch_size;
  int64_t sort_key_cnt = sort_collations.count();
  if (OB_FAIL(init_sk_col_result_list(sort_key_cnt, batch_size))) {
    SQL_ENG_LOG(WARN, "failed to init sort key column result list", K(ret), K(batch_size));
  } else if (OB_FAIL(sk_vec_ptrs_.init(sort_key_cnt))) {
    SQL_ENG_LOG(WARN, "failed to init sort key vector ptrs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_key_cnt; i++) {
      const ObSortFieldCollation &sort_collation = sort_collations.at(i);
      const ObExpr *e = sk_exprs.at(sort_collation.field_idx_);
      ObIVector *vec = e->get_vector(eval_ctx);
      if (OB_FAIL(sk_vec_ptrs_.push_back(vec))) {
        SQL_ENG_LOG(WARN, "failed to add expr vector", K(ret));
      }
    }
  }
  return ret;
}

int ObSortKeyFetcher::fetch_payload(const ObBatchRows &input_brs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sk_vec_ptrs_.count(); i++) {
    ObIVector *vec = sk_vec_ptrs_.at(i);
    VectorFormat format = vec->get_format();
    sk_col_res_list_[i].format_ = format;
    switch (format) {
    case VEC_FIXED:
      if (OB_FAIL(fetch_fixed_payload(static_cast<const ObFixedLengthBase &>(*vec), input_brs,
                                      sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_DISCRETE:
      if (OB_FAIL(fetch_discrete_payload(static_cast<const ObDiscreteBase &>(*vec), input_brs,
                                         sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_CONTINUOUS:
      if (OB_FAIL(fetch_continuous_payload(static_cast<const ObContinuousBase &>(*vec), input_brs,
                                           sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_UNIFORM:
      if (OB_FAIL(fetch_uniform_payload(static_cast<const ObUniformBase &>(*vec), false, input_brs,
                                        sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_UNIFORM_CONST:
      if (OB_FAIL(fetch_uniform_payload(static_cast<const ObUniformBase &>(*vec), true, input_brs,
                                        sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get wrong vector format", K(format), K(ret));
      break;
    }
  }
  return ret;
}

int ObSortKeyFetcher::fetch_payload(const uint16_t selector[], const int64_t selector_size)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sk_vec_ptrs_.count(); i++) {
    ObIVector *vec = sk_vec_ptrs_.at(i);
    VectorFormat format = vec->get_format();
    sk_col_res_list_[i].format_ = format;
    switch (format) {
    case VEC_FIXED:
      if (OB_FAIL(fetch_fixed_payload(static_cast<const ObFixedLengthBase &>(*vec), selector,
                                      selector_size, sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_DISCRETE:
      if (OB_FAIL(fetch_discrete_payload(static_cast<const ObDiscreteBase &>(*vec), selector,
                                         selector_size, sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_CONTINUOUS:
      if (OB_FAIL(fetch_continuous_payload(static_cast<const ObContinuousBase &>(*vec), selector,
                                           selector_size, sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_UNIFORM:
      if (OB_FAIL(fetch_uniform_payload(static_cast<const ObUniformBase &>(*vec), false, selector,
                                        selector_size, sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    case VEC_UNIFORM_CONST:
      if (OB_FAIL(fetch_uniform_payload(static_cast<const ObUniformBase &>(*vec), true, selector,
                                        selector_size, sk_col_res_list_[i]))) {
        SQL_ENG_LOG(WARN, "failed to batch fetch fixed base payload", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get wrong vector format", K(format), K(ret));
      break;
    }
  }
  return ret;
}

int ObSortKeyFetcher::fetch_fixed_payload(const ObFixedLengthBase &vec, const ObBatchRows &input_brs,
                                         SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  col_result.data_ = vec.get_data();
  col_result.len_array_[0] = vec.get_length();
  col_result.nulls_ = vec.get_nulls();
  return ret;
}

int ObSortKeyFetcher::fetch_discrete_payload(const ObDiscreteBase &vec, const ObBatchRows &input_brs,
                                            SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  char **ptrs = vec.get_ptrs();
  const ObLength *lens = vec.get_lens();
  col_result.nulls_ = vec.get_nulls();
  for (int64_t i = 0; i < input_brs.size_; i++) {
    if (input_brs.skip_->at(i)) {
      continue;
    }
    col_result.payload_array_[i] = ptrs[i];
    col_result.len_array_[i] = lens[i];
  }
  return ret;
}

int ObSortKeyFetcher::fetch_continuous_payload(const ObContinuousBase &vec,
                                              const ObBatchRows &input_brs,
                                              SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  const char *data = vec.get_data();
  const uint32_t *offsets = vec.get_offsets();
  col_result.nulls_ = vec.get_nulls();
  for (int64_t i = 0; i < input_brs.size_; i++) {
    if (input_brs.skip_->at(i)) {
      continue;
    }
    col_result.payload_array_[i] = data + offsets[i];
    col_result.len_array_[i] = offsets[i + 1] - offsets[i];
  }
  return ret;
}

int ObSortKeyFetcher::fetch_uniform_payload(const ObUniformBase &vec, const bool is_const,
                                           const ObBatchRows &input_brs,
                                           SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  col_result.uniform_fmt_nulls_->reset(input_brs.size_);
  const ObDatum *datums = vec.get_datums();
  for (int64_t i = 0; i < input_brs.size_; i++) {
    if (input_brs.skip_->at(i)) {
      continue;
    }
    const ObDatum &datum = datums[is_const ? 0 : i];
    col_result.payload_array_[i] = datum.ptr_;
    col_result.len_array_[i] = datum.len_;
    if (datum.null_) {
      col_result.uniform_fmt_nulls_->set(i);
    }
  }
  return ret;
}

int ObSortKeyFetcher::fetch_fixed_payload(const ObFixedLengthBase &vec, const uint16_t selector[],
                                         const int64_t selector_size, SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  col_result.data_ = vec.get_data();
  col_result.len_array_[0] = vec.get_length();
  col_result.nulls_ = vec.get_nulls();
  return ret;
}

int ObSortKeyFetcher::fetch_discrete_payload(const ObDiscreteBase &vec, const uint16_t selector[],
                                            const int64_t selector_size,
                                            SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  char **ptrs = vec.get_ptrs();
  const ObLength *lens = vec.get_lens();
  col_result.nulls_ = vec.get_nulls();
  for (int64_t i = 0; i < selector_size; i++) {
    int64_t idx = selector[i];
    col_result.payload_array_[idx] = ptrs[idx];
    col_result.len_array_[idx] = lens[idx];
  }
  return ret;
}

int ObSortKeyFetcher::fetch_continuous_payload(const ObContinuousBase &vec,
                                              const uint16_t selector[],
                                              const int64_t selector_size,
                                              SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  const char *data = vec.get_data();
  const uint32_t *offsets = vec.get_offsets();
  col_result.nulls_ = vec.get_nulls();
  for (int64_t i = 0; i < selector_size; i++) {
    int64_t idx = selector[i];
    col_result.payload_array_[idx] = data + offsets[idx];
    col_result.len_array_[idx] = offsets[idx + 1] - offsets[idx];
  }
  return ret;
}

int ObSortKeyFetcher::fetch_uniform_payload(const ObUniformBase &vec, const bool is_const,
                                           const uint16_t selector[], const int64_t selector_size,
                                           SortKeyColResult &col_result)
{
  int ret = OB_SUCCESS;
  col_result.has_null_ = vec.has_null();
  col_result.uniform_fmt_nulls_->reset(max_batch_size_);
  const ObDatum *datums = vec.get_datums();
  for (int64_t i = 0; i < selector_size; i++) {
    int64_t idx = selector[i];
    const ObDatum &datum = datums[is_const ? 0 : idx];
    col_result.payload_array_[idx] = datum.ptr_;
    col_result.len_array_[idx] = datum.len_;
    if (datum.null_) {
      col_result.uniform_fmt_nulls_->set(idx);
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
