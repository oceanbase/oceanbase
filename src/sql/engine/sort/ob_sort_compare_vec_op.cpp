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

#include "sql/engine/sort/ob_sort_compare_vec_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
SortKeyColResult::SortKeyColResult(ObIAllocator &allocator) :
  flag_(0), format_(VectorFormat::VEC_INVALID), allocator_(allocator), data_(nullptr),
  nulls_(nullptr), uniform_fmt_nulls_(nullptr), payload_array_(nullptr), len_array_(nullptr)
{}

SortKeyColResult::~SortKeyColResult()
{
  reset();
}

void SortKeyColResult::reset()
{
  if (nullptr != uniform_fmt_nulls_) {
    allocator_.free(uniform_fmt_nulls_);
    uniform_fmt_nulls_ = nullptr;
  }
  if (nullptr != payload_array_) {
    allocator_.free(payload_array_);
    payload_array_ = nullptr;
  }
  if (nullptr != len_array_) {
    allocator_.free(len_array_);
    len_array_ = nullptr;
  }
}

int SortKeyColResult::init(int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  void *buffer = nullptr;
  if (OB_ISNULL(buffer = allocator_.alloc(sql::ObBitVector::memory_size(max_batch_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc memory", K(ret));
  } else if (OB_ISNULL(len_array_ = static_cast<ObLength *>(
                         allocator_.alloc(sizeof(ObLength) * max_batch_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc lengths", K(ret));
  } else if (OB_ISNULL(payload_array_ = static_cast<const char **>(
                         allocator_.alloc(sizeof(char *) * max_batch_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc ptrs", K(ret));
  } else {
    uniform_fmt_nulls_ = sql::to_bit_vector(buffer);
  }
  return ret;
}

bool SortKeyColResult::is_null(const int64_t idx) const
{
  if (VEC_UNIFORM == format_ || VEC_UNIFORM_CONST == format_) {
    return uniform_fmt_nulls_->at(idx);
  } else {
    return nulls_->at(idx);
  }
}

const char *SortKeyColResult::get_payload(const int64_t idx) const
{
  if (VEC_FIXED == format_) {
    return data_ + len_array_[0] * idx;
  } else {
    return payload_array_[idx];
  }
}

ObLength SortKeyColResult::get_length(const int64_t idx) const
{
  if (VEC_FIXED == format_) {
    return len_array_[0];
  } else {
    return len_array_[idx];
  }
}

CompareBase::CompareBase(ObIAllocator &allocator) :
  allocator_(allocator), ret_(OB_SUCCESS), cmp_sk_exprs_(nullptr), sk_row_meta_(nullptr),
  addon_row_meta_(nullptr), cmp_sort_collations_(nullptr), sk_col_result_list_(nullptr),
  cmp_funcs_(allocator), exec_ctx_(nullptr), encode_sk_state_(CompareBase::DISABLE), cmp_count_(0),
  cmp_start_(0), cmp_end_(0), cnt_(0)
{}

CompareBase::~CompareBase()
{
  cmp_funcs_.reset();
}

int CompareBase::init_cmp_sort_key(const ObIArray<ObExpr *> *cmp_sk_exprs,
                                   const ObIArray<ObSortFieldCollation> *cmp_sort_collations)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cmp_funcs_.init(cmp_sort_collations->count()))) {
    SQL_ENG_LOG(WARN, "failed to init sort collations", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cmp_sort_collations->count(); i++) {
    const ObSortFieldCollation &sort_collation = cmp_sort_collations->at(i);
    const ObExpr *e = cmp_sk_exprs->at(sort_collation.field_idx_);
    NullSafeRowCmpFunc cmp_fun = NULL_FIRST == sort_collation.null_pos_ ?
                           e->basic_funcs_->row_null_first_cmp_ :
                           e->basic_funcs_->row_null_last_cmp_;
    if (OB_FAIL(cmp_funcs_.push_back(cmp_fun))) {
      SQL_ENG_LOG(WARN, "failed to add compare func", K(ret));
    }
  }
  return ret;
}

int CompareBase::init(const ObIArray<ObExpr *> *cmp_sk_exprs, const RowMeta *sk_row_meta,
                      const RowMeta *addon_row_meta,
                      const ObIArray<ObSortFieldCollation> *cmp_sort_collations,
                      ObExecContext *exec_ctx, bool enable_encode_sortkey)
{
  int ret = OB_SUCCESS;
  if (nullptr == cmp_sk_exprs || nullptr == cmp_sort_collations || nullptr == exec_ctx) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(cmp_sort_collations));
  } else if (OB_FAIL(init_cmp_sort_key(cmp_sk_exprs, cmp_sort_collations))) {
    SQL_ENG_LOG(WARN, "failed to init compare sort key", K(ret));
  } else {
    cmp_sk_exprs_ = cmp_sk_exprs;
    sk_row_meta_ = sk_row_meta;
    addon_row_meta_ = addon_row_meta;
    cmp_sort_collations_ = cmp_sort_collations;
    exec_ctx_ = exec_ctx;
    cnt_ = cmp_sort_collations_->count();
    cmp_start_ = 0;
    cmp_end_ = cmp_sort_collations_->count();
    encode_sk_state_ = enable_encode_sortkey ? CompareBase::ENABLE : CompareBase::DISABLE;
  }
  return ret;
}

int CompareBase::fast_check_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((cmp_count_++ & 8191) == 8191)) {
    ret = exec_ctx_->check_status();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
