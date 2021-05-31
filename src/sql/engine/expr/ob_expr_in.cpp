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

#include "sql/engine/expr/ob_expr_in.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"

namespace oceanbase {
using namespace common;
using namespace hash;
namespace sql {
// Calculate the next number of the current binary permutation combination, for example 001->010->100
static unsigned next_perm(unsigned int cur_num)
{
  unsigned int t = cur_num | (cur_num - 1);  // t gets cur_num's least significant 0 bits set to 1
  // Next set to 1 the most significant bit to change,
  // set to 0 the least significant ones, and add the necessary 1 bits.
  return (t + 1) | (((~t & -~t) - 1) >> (__builtin_ctz(cur_num) + 1));
}
// Calculate the next number of the current binary permutation and combination. If the high bits of
// the current number are all 1, then add a 1 and put it to the low
// E.g. 11000->00111
static unsigned next(unsigned int cur_num, unsigned int max)
{
  // The cur | (cur_num-1) that is calculated repeatedly here and in next_perm will be automatically
  // extracted by the compiler during inlining, and will not be calculated again
  return ((cur_num - 1) | cur_num) >= max - 1 ? (1U << (__builtin_popcount(cur_num) + 1)) - 1 : next_perm(cur_num);
}
// Used to solve the last binary permutation combination of cur num, for example
// 111->110->101->011->100->010->001, max is 111
static unsigned last(unsigned int cur_num, unsigned int max)
{
  unsigned int num = (cur_num ^ max);
  return (max ^ next(num, max));
}

template <>
bool Row<ObDatum>::equal_key(const Row<ObDatum>& other, void** cmp_funcs, const int idx) const
{
  bool equal_ret = false;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_)) {
  } else {
    bool is_equal = true;
    int curr_idx = idx;
    for (int i = 0; is_equal && 0 != curr_idx; ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        if (elems_[i].is_null() && other.elems_[i].is_null()) {
          // true
        } else if (elems_[i].is_null() || other.elems_[i].is_null()) {
          is_equal = false;
        } else {
          int cmp_ret = ((DatumCmpFunc)cmp_funcs[i])(elems_[i], other.elems_[i]);
          if (0 != cmp_ret) {
            is_equal = false;
          } else {
            // do nothing
          }
        }
      }
    }
    equal_ret = is_equal;
  }
  return equal_ret;
}

template <>
bool Row<ObObj>::equal_key(const Row<ObObj>& other, void** cmp_funcs, const int idx) const
{
  UNUSED(cmp_funcs); /**nullptr**/
  bool equal_ret = false;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_)) {
  } else {
    bool is_equal = true;
    int curr_idx = idx;
    for (int i = 0; is_equal && 0 != curr_idx; ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        if (elems_[i].is_null() && other.elems_[i].is_null()) {
          // true
        } else if (elems_[i].is_null() || other.elems_[i].is_null()) {
          is_equal = false;
        } else {
          is_equal = elems_[i] == other.elems_[i];
        }
      }
    }
    equal_ret = is_equal;
  }
  return equal_ret;
}

template <>
uint64_t Row<ObDatum>::hash_key(void** hash_funcs, const int idx, uint64_t seed) const
{
  uint64_t hash_ret = 0;
  if (OB_ISNULL(elems_)) {
  } else {
    int curr_idx = idx;
    for (int i = 0; 0 != curr_idx; ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        seed = ((ObExprHashFuncType)hash_funcs[i])(elems_[i], seed);
      } else {
        continue;
      }
    }
    hash_ret = seed;
  }
  return hash_ret;
}

template <>
uint64_t Row<ObObj>::hash_key(void** hash_funcs, const int idx, uint64_t seed) const
{
  UNUSED(hash_funcs); /**nullptr**/
  uint64_t hash_ret = 0;
  if (OB_ISNULL(elems_)) {
  } else {
    int curr_idx = idx;
    for (int i = 0; 0 != curr_idx; ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        seed = elems_[i].hash(seed);
      } else {
        continue;
      }
    }
    hash_ret = seed;
  }
  return hash_ret;
}

template <>
int Row<ObDatum>::compare_with_null(
    const Row<ObDatum>& other, void** cmp_funcs, const int64_t row_dimension, int& exist_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_) || OB_ISNULL(cmp_funcs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer param or function", K(ret));
  } else if (row_dimension > 0) {
    exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_TRUE;
    for (int i = 0; ObExprInHashMap<ObDatum>::HASH_CMP_FALSE != exist_ret && i < row_dimension; ++i) {
      if (elems_[i].is_null() || other.elems_[i].is_null()) {
        exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_UNKNOWN;
      } else {
        int cmp_ret = ((DatumCmpFunc)cmp_funcs[i])(elems_[i], other.elems_[i]);
        if (0 != cmp_ret) {
          exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_FALSE;
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}
// 0 for true, -1 for false, 1 for NULL
template <>
int Row<ObObj>::compare_with_null(
    const Row<ObObj>& other, void** cmp_funcs, const int64_t row_dimension, int& exist_ret) const
{
  UNUSED(cmp_funcs); /**nullptr**/
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer param", K(ret));
  } else if (row_dimension > 0) {
    exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_TRUE;
    for (int i = 0; ObExprInHashMap<ObObj>::HASH_CMP_FALSE != exist_ret && i < row_dimension; ++i) {
      if (elems_[i].is_null() || other.elems_[i].is_null()) {
        exist_ret = ObExprInHashMap<ObObj>::HASH_CMP_UNKNOWN;
      } else {
        int cmp_ret = (elems_[i] == other.elems_[i]) ? 0 : -1;
        if (0 != cmp_ret) {
          exist_ret = ObExprInHashMap<ObObj>::HASH_CMP_FALSE;
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

template <class T>
int Row<T>::set_elem(T* elems)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(elems)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("elem is not inited", K(ret));
  } else {
    elems_ = elems;
  }
  return ret;
}

template <class T>
bool RowKey<T>::operator==(const RowKey<T>& other) const
{
  return row_.equal_key(other.row_, meta_->cmp_funcs_, meta_->idx_);
}

template <class T>
uint64_t RowKey<T>::hash(uint64_t seed) const
{
  return row_.hash_key(meta_->hash_funcs_, meta_->idx_, seed);
}

template <class T>
int ObExprInHashMap<T>::set_refactored(const Row<T>& row)
{
  int ret = OB_SUCCESS;
  ObArray<Row<T>>* arr_ptr = NULL;
  RowKey<T> tmp_row_key;
  tmp_row_key.row_ = row;
  tmp_row_key.meta_ = &meta_;
  if (OB_ISNULL(arr_ptr = const_cast<ObArray<Row<T>>*>(map_.get(tmp_row_key)))) {
    ObArray<Row<T>> arr;
    if (OB_FAIL(arr.push_back(row))) {
      LOG_WARN("failed to load row", K(ret));
    } else {
      ret = map_.set_refactored(tmp_row_key, arr);
    }
  } else {
    int exist = ObExprInHashMap<T>::HASH_CMP_FALSE;
    for (int i = 0; OB_SUCC(ret) && ObExprInHashMap<T>::HASH_CMP_TRUE != exist && i < arr_ptr->count(); ++i) {
      if (OB_FAIL(row.compare_with_null((*arr_ptr)[i], meta_.cmp_funcs_, meta_.row_dimension_, exist))) {
        LOG_WARN("compare with null failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && ObExprInHashMap<T>::HASH_CMP_TRUE != exist) {
      ret = arr_ptr->push_back(row);
    }
  }
  return ret;
}

template <class T>
int ObExprInHashMap<T>::exist_refactored(const Row<T>& row, int& exist_ret)
{
  int ret = OB_SUCCESS;
  RowKey<T> tmp_row_key;
  const Row<T>* tmp_row = &row;
  tmp_row_key.row_ = row;
  tmp_row_key.meta_ = &meta_;
  const ObArray<Row<T>>* arr_ptr = map_.get(tmp_row_key);
  if (OB_ISNULL(arr_ptr)) {
    exist_ret = ObExprInHashMap<T>::HASH_CMP_FALSE;
  } else {
    int exist = ObExprInHashMap<T>::HASH_CMP_FALSE;
    for (int i = 0; 0 != exist_ret && i < arr_ptr->count(); ++i) {
      if (OB_FAIL(row.compare_with_null((*arr_ptr)[i], meta_.cmp_funcs_, meta_.row_dimension_, exist))) {
        LOG_WARN("compare with null failed", K(ret));
      } else if (ObExprInHashMap<T>::HASH_CMP_UNKNOWN == exist || ObExprInHashMap<T>::HASH_CMP_TRUE == exist) {
        exist_ret = exist;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

template <class T>
int ObExprInHashSet<T>::set_refactored(const Row<T>& row)
{
  RowKey<T> tmp_row_key;
  tmp_row_key.row_ = row;
  tmp_row_key.meta_ = &meta_;
  return set_.set_refactored(tmp_row_key);
}

template <class T>
int ObExprInHashSet<T>::exist_refactored(const Row<T>& row, bool& is_exist)
{
  RowKey<T> tmp_row_key;
  tmp_row_key.row_ = row;
  tmp_row_key.meta_ = &meta_;
  int ret = set_.exist_refactored(tmp_row_key);
  if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
    is_exist = true;
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    is_exist = false;
  } else {
    LOG_WARN("failed to search in hashset", K(ret));
  }
  return OB_SUCCESS;
}

int ObExprInOrNotIn::ObExprInCtx::init_hashset(int64_t param_num)
{
  return hashset_.create(param_num * 2);
}

int ObExprInOrNotIn::ObExprInCtx::init_hashset_vecs(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  hashset_vecs_ = NULL;
  int vecs_buf_size = sizeof(ObExprInHashMap<ObObj>) * (1 << row_dimension);
  if (OB_ISNULL(hashset_vecs_ = (ObExprInHashMap<ObObj>*)((exec_ctx->get_allocator()).alloc(vecs_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < (1 << row_dimension); ++i) {
      ObExprInHashMap<ObObj>* hashset_ptr = new (&hashset_vecs_[i]) ObExprInHashMap<ObObj>();
      hashset_vecs_[i].set_meta_idx(i);
      hashset_vecs_[i].set_meta_dimension(row_dimension);
      if (OB_FAIL(hashset_vecs_[i].create(param_num))) {
        LOG_WARN("create static_engine_hashset_vecs failed", K(ret), K(i));
      }
    }
  }
  row_dimension_ = row_dimension;
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::init_static_engine_hashset(int64_t param_num)
{
  static_engine_hashset_.set_meta_idx(1);
  static_engine_hashset_.set_meta_dimension(1);
  row_dimension_ = 1;
  return static_engine_hashset_.create(param_num * 2);
}

int ObExprInOrNotIn::ObExprInCtx::init_static_engine_hashset_vecs(
    int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  static_engine_hashset_vecs_ = NULL;
  int vecs_buf_size = sizeof(ObExprInHashMap<ObDatum>) * (1 << row_dimension);
  if (OB_ISNULL(static_engine_hashset_vecs_ =
                    (ObExprInHashMap<ObDatum>*)((exec_ctx->get_allocator()).alloc(vecs_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < (1 << row_dimension); ++i) {
      ObExprInHashMap<ObDatum>* hashset_ptr = new (&static_engine_hashset_vecs_[i]) ObExprInHashMap<ObDatum>();
      static_engine_hashset_vecs_[i].set_meta_idx(i);
      static_engine_hashset_vecs_[i].set_meta_dimension(row_dimension);
      if (OB_FAIL(static_engine_hashset_vecs_[i].create(param_num))) {
        LOG_WARN("create static_engine_hashset_vecs failed", K(ret), K(i));
      }
    }
  }
  row_dimension_ = row_dimension;
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::add_to_hashset(const ObObj& obj)
{
  int ret = hashset_.set_refactored(obj);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add to hashset", K(ret), K(obj));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::add_to_hashset_vecs(const Row<ObObj>& row, const int idx)
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = hashset_vecs_[idx].set_refactored(row);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add to hashset_vecs", K(ret), K(idx));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::add_to_static_engine_hashset(const Row<common::ObDatum>& row)
{
  int ret = static_engine_hashset_.set_refactored(row);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add to hashset", K(ret));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::add_to_static_engine_hashset_vecs(const Row<common::ObDatum>& row, const int idx)
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = static_engine_hashset_vecs_[idx].set_refactored(row);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add to hashset_vecs", K(ret), K(idx));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::exist_in_hashset(const ObObj& obj, bool& is_exist) const
{
  int ret = hashset_.exist_refactored(obj);
  if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
    is_exist = true;
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    is_exist = false;
  } else {
    LOG_WARN("failed to search in hashset", K(ret), K(obj));
  }
  return OB_SUCCESS;
}

int ObExprInOrNotIn::ObExprInCtx::exist_in_hashset_vecs(const Row<ObObj>& row, const int idx, int& exist_ret) const
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(hashset_vecs_[idx].exist_refactored(row, exist_ret))) {
    LOG_WARN("failed to find in hash map", K(ret));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::exist_in_static_engine_hashset(const Row<ObDatum>& row, bool& is_exist)
{
  return static_engine_hashset_.exist_refactored(row, is_exist);
}

int ObExprInOrNotIn::ObExprInCtx::exist_in_static_engine_hashset_vecs(
    const Row<ObDatum>& row, const int idx, int& exist_ret)
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(static_engine_hashset_vecs_[idx].exist_refactored(row, exist_ret))) {
    LOG_WARN("failed to find in hash map", K(ret));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::set_cmp_types(const ObExprCalcType& cmp_type, const int64_t row_dimension)
{
  int ret = OB_SUCCESS;
  if (cmp_types_.count() < row_dimension) {
    ret = cmp_types_.push_back(cmp_type);
  } else {
    // do nothing
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::init_cmp_types(const int64_t row_dimension, ObExecContext* exec_ctx)
{
  cmp_types_.set_allocator(&(exec_ctx->get_allocator()));
  return cmp_types_.init(row_dimension);
}

const ObExprCalcType& ObExprInOrNotIn::ObExprInCtx::get_cmp_types(const int64_t idx) const
{
  return cmp_types_[idx];
}

int ObExprInOrNotIn::ObExprInCtx::init_hashset_vecs_all_null(const int64_t row_dimension, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  hashset_vecs_all_null_.set_allocator(&(exec_ctx->get_allocator()));
  if (OB_FAIL(hashset_vecs_all_null_.init(1 << row_dimension))) {
    LOG_WARN("failed to init fixed array", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < (1 << row_dimension); ++i) {
      ret = hashset_vecs_all_null_.push_back(false);
    }
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::set_hashset_vecs_all_null_true(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    hashset_vecs_all_null_[idx] = true;
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::get_hashset_vecs_all_null(const int64_t idx, bool& is_all_null) const
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_all_null = hashset_vecs_all_null_[idx];
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::init_right_objs(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  right_objs_ = NULL;
  int objs_buf_size = sizeof(ObObj*) * param_num;
  if (OB_ISNULL(right_objs_ = (ObObj**)((exec_ctx->get_allocator()).alloc(objs_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObObj **", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_ISNULL(right_objs_[i] =
                        static_cast<ObObj*>(((exec_ctx->get_allocator()).alloc(sizeof(ObObj) * row_dimension))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ObObj *", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::init_right_datums(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  right_datums_ = NULL;
  int datums_buf_size = sizeof(ObDatum*) * param_num;
  if (OB_ISNULL(right_datums_ = (ObDatum**)((exec_ctx->get_allocator()).alloc(datums_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDatum **", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_ISNULL(right_datums_[i] =
                        static_cast<ObDatum*>(((exec_ctx->get_allocator()).alloc(sizeof(ObDatum) * row_dimension))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ObDatum *", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::set_right_obj(
    int64_t row_num, int64_t col_num, const int right_param_num, const common::ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(right_objs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("right_datums is not init", K(ret));
  } else if (row_num < 0 || row_num >= right_param_num || col_num < 0 || col_num >= row_dimension_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_num or col_num out of bounds", K(ret));
  } else {
    right_objs_[row_num][col_num] = obj;
  }
  return ret;
}
int ObExprInOrNotIn::ObExprInCtx::set_right_datum(
    int64_t row_num, int64_t col_num, const int right_param_num, const common::ObDatum& datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(right_datums_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("right_datums is not init", K(ret));
  } else if (row_num < 0 || row_num >= right_param_num || col_num < 0 || col_num >= row_dimension_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_num or col_num out of bounds", K(ret));
  } else {
    right_datums_[row_num][col_num] = datum;
  }
  return ret;
}

ObExprInOrNotIn::ObExprInOrNotIn(ObIAllocator& alloc, ObExprOperatorType type, const char* name)
    : ObVectorExprOperator(alloc, type, name, 2, 1), param_flags_(0)
{
  param_lazy_eval_ = true;
  need_charset_convert_ = false;
}

int ObExprInOrNotIn::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = ObVectorExprOperator::calc_result_typeN(type, types, param_num, type_ctx);
  if (OB_SUCC(ret)) {
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  }
  return ret;
}

int ObExprInOrNotIn::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExprCalcType>& cmp_types = result_type_.get_row_calc_cmp_types();
  if (OB_ISNULL(expr_ctx.exec_ctx_) && false == expr_ctx.is_pre_calculation_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. exec_ctx should not be null in pre calculation",
        K(ret),
        K(expr_ctx.exec_ctx_),
        K(expr_ctx.is_pre_calculation_));
  }
  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode() && is_param_is_ext_type_oracle()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      const bool hashset_lookup = need_hash(expr_ctx.exec_ctx_);
      bool fall_back = false;
      if (hashset_lookup) {
        if (row_dimension_ == 1) {
          ret = hash_calc(result, objs, cmp_types, param_num, expr_ctx, fall_back);
        } else if (row_dimension_ > 1) {
          ret = hash_calc_for_vector(result, objs, cmp_types, param_num, expr_ctx, fall_back);
        } else {
          // do nothing
        }
      }
      if (OB_SUCC(ret) && (!hashset_lookup || fall_back)) {
        EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
        EXPR_DEFINE_CMP_CTX(result_type_.get_calc_meta(), false, expr_ctx);
        if (!is_param_is_subquery()) {
          ret = calc(result, objs, cmp_types, param_num, cmp_ctx, expr_ctx, cast_ctx);
        } else {
          ret = calc_for_subquery(result, objs, cmp_types, param_num, cmp_ctx, expr_ctx, cast_ctx);
        }
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::calc(ObObj& result, const ObObj* objs, const ObIArray<ObExprCalcType>& cmp_types,
    int64_t param_num, ObCompareCtx& cmp_ctx, ObExprCtx& expr_ctx, ObCastCtx& cast_ctx) const
{
  int ret = OB_SUCCESS;
  int32_t row_num = real_param_num_;
  int64_t cmp_type_count = cmp_types.count();
  if (OB_ISNULL(objs) || OB_UNLIKELY(param_num < 0) || OB_UNLIKELY(row_num < 0) || OB_UNLIKELY(row_dimension_ < 0) ||
      OB_UNLIKELY(param_num != static_cast<int64_t>(row_dimension_) * row_num) ||
      OB_UNLIKELY((cmp_type_count < row_dimension_ * (row_num - 1)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(objs), K(row_num), K(row_dimension_), K(param_num), K(cmp_type_count));
  } else {
    bool set_cnt_equal = false;
    bool set_cnt_null = false;
    if (OB_FAIL(calc_for_row(objs, cmp_types, cmp_ctx, expr_ctx, cast_ctx, nullptr, set_cnt_equal, set_cnt_null))) {
      LOG_WARN("failed to calc for row", K(ret));
    }
    if (OB_SUCC(ret)) {
      set_result(result, set_cnt_equal, set_cnt_null);
    }
  }
  return ret;
}

int ObExprInOrNotIn::calc_for_subquery(ObObj& result, const ObObj* objs, const ObIArray<ObExprCalcType>& cmp_types,
    int64_t param_num, ObCompareCtx& cmp_ctx, ObExprCtx& expr_ctx, ObCastCtx& cast_ctx) const
{
  // param_num represents the size of the stack,
  // for the in expression of the subquery on the left, stack[0] is the subquery,
  // Right_objs is behind, so param_num-1 + row_dimension = row_dimension * row_num
  int ret = OB_SUCCESS;
  int row_num = real_param_num_;
  int64_t cmp_type_count = cmp_types.count();
  if (OB_ISNULL(objs) || OB_UNLIKELY(param_num < 0) || OB_UNLIKELY(row_num < 0) || OB_UNLIKELY(row_dimension_ < 0) ||
      OB_UNLIKELY(param_num - 1 + row_dimension_ != static_cast<int64_t>(row_dimension_) * row_num) ||
      OB_UNLIKELY(cmp_type_count < row_dimension_ * (row_num - 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(objs), K(row_num), K(row_dimension_), K(param_num), K(cmp_type_count));
  } else {
    bool set_cnt_equal = false;
    bool set_cnt_null = false;
    // first get left row
    ObNewRow tmp_row;
    ObNewRow* left_row = NULL;
    ObNewRowIterator* left_row_iter = NULL;
    if (row_dimension_ > 1) {
      if (OB_ISNULL(expr_ctx.subplan_iters_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expr_ctx.subplan_iters_ is NULL", K(ret));
      } else if (expr_ctx.subplan_iters_->count() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subplan_iters count is invalid", K(ret), K(expr_ctx.subplan_iters_->count()));
      } else if (OB_ISNULL(left_row_iter = expr_ctx.subplan_iters_->at(0))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get row iterator failed", K(ret));
      } else if (OB_FAIL(left_row_iter->get_next_row(left_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          result.set_null();
        } else {
          LOG_WARN("get next row from left row iterator falied", K(ret));
        }
      }
    } else {
      tmp_row.cells_ = const_cast<ObObj*>(&objs[0]);
      tmp_row.count_ = 1;
      left_row = &tmp_row;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(left_row)) {
      if (OB_FAIL(calc_for_row(objs, cmp_types, cmp_ctx, expr_ctx, cast_ctx, left_row, set_cnt_equal, set_cnt_null))) {
        LOG_WARN("failed to calc for row", K(ret), K(left_row));
      }
    }
    // for the comparison of subquery, if the left param is row iterator,
    // we must check the row count of the left row iterator
    // the left row iterator can only return one row
    if (OB_SUCC(ret)) {
      ObNewRow* tmp_left_row = NULL;
      if (OB_FAIL(left_row_iter->get_next_row(tmp_left_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          // The data is iterated for the first time, and the second row of data cannot be iterated.
          // It conforms to the semantics of with none and returns OB_SUCCESS
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row from iter failed", K(ret));
        }
      } else {
        // The data is iterated for the second time, which does not meet the semantics of the vector,
        // so an error should be reported externally
        ret = OB_SUBQUERY_TOO_MANY_ROW;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(left_row)) {
      set_result(result, set_cnt_equal, set_cnt_null);
    }
  }
  return ret;
}

int ObExprInOrNotIn::calc_for_row(const ObObj* objs, const ObIArray<ObExprCalcType>& cmp_types, ObCompareCtx& cmp_ctx,
    ObExprCtx& expr_ctx, ObCastCtx& cast_ctx, ObNewRow* left_row, bool& set_cnt_equal, bool& set_cnt_null) const
{
  int ret = OB_SUCCESS;
  ObObj cmp;
  int64_t right_elem_count = real_param_num_ - 1;
  int64_t left_start_idx = -1;
  int64_t right_start_idx = -1;
  int64_t left_evaluated = 0;
  if (OB_ISNULL(left_row)) {
    left_start_idx = 0;
    right_start_idx = row_dimension_;
  } else {
    left_start_idx = 0;
    right_start_idx = 1;              // 0 for left_iter, 1: for right
    left_evaluated = row_dimension_;  // for sub_iter, all are evaled
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_elem_count; ++i, right_start_idx += row_dimension_) {
    bool row_cnt_null = false;
    bool row_is_equal = true;
    for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension_; ++j) {
      const ObExprCalcType& cmp_type = cmp_types.at(row_dimension_ * i + left_start_idx + j);
      cmp_ctx.cmp_type_ = cmp_type.get_type();
      cmp_ctx.cmp_cs_type_ = cmp_type.get_collation_type();
      const ObObj* left_obj = nullptr;
      if (OB_ISNULL(left_row)) {
        left_obj = &(objs[left_start_idx + j]);
      } else {
        left_obj = &(left_row->get_cell(left_start_idx + j));
      }
      const ObObj& right_obj = objs[right_start_idx + j];
      if (j >= left_evaluated) {
        // only evaluate once for left objs.
        if (OB_FAIL(param_eval(expr_ctx, *left_obj, left_start_idx + j))) {
          LOG_WARN("param evaluate failed", K(ret), K(left_start_idx + j));
        } else {
          left_evaluated += 1;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(param_eval(expr_ctx, right_obj, right_start_idx + j))) {
        LOG_WARN("param evaluate failed", K(ret), K(right_start_idx + j));
      } else if (OB_FAIL(ObExprEqual::calc(cmp, *left_obj, right_obj, cmp_ctx, cast_ctx))) {
        LOG_WARN("Compare expression failed", K(ret), K(*left_obj), K(right_obj));
      } else if (cmp.is_true()) {
        continue;
      } else if (cmp.is_false()) {
        row_is_equal = false;
        row_cnt_null = false;
        break;
      } else if (cmp.is_null()) {
        row_cnt_null = true;
      }
    }  // end inner loop

    // outer loop...
    if (OB_SUCC(ret)) {
      if (row_is_equal && !row_cnt_null) {
        set_cnt_equal = true;
        break;
      } else if (row_cnt_null) {
        set_cnt_null = true;
      }
    }

  }  // end outer loop
  return ret;
}

int ObExprInOrNotIn::hash_calc(ObObj& result, const ObObj* objs, const ObIArray<ObExprCalcType>& cmp_types,
    int64_t param_num, ObExprCtx& expr_ctx, bool& fall_back) const
{
  int ret = OB_SUCCESS;
  ObExprInCtx* in_ctx = NULL;
  ObExecContext* exec_ctx = expr_ctx.exec_ctx_;
  ObObj out_obj;
  if (OB_ISNULL(objs) || OB_ISNULL(exec_ctx) || OB_UNLIKELY(param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("objs or exec_ctx is null, or param num is too small", K(ret), K(objs), K(exec_ctx), K(param_num));
  }
  if (OB_SUCC(ret)) {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
    if (NULL == (in_ctx = static_cast<ObExprInCtx*>(exec_ctx->get_expr_op_ctx(get_id())))) {
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(get_id(), in_ctx))) {
        LOG_WARN("failed to create operator ctx", K(ret));
      } else if (OB_FAIL(in_ctx->init_hashset(param_num))) {
        LOG_WARN("failed to init hashset", K(ret));
      } else {
        // evaluate parameters && check fall back
        for (int i = 1; OB_SUCC(ret) && i < param_num; i++) {
          if (OB_FAIL(param_eval(expr_ctx, objs[i], i))) {
            LOG_DEBUG("param evaluate fail, hash set lookup disabled for in expr", K(ret), K(i));
            in_ctx->disable_hash_calc();
          }
        }
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret) && !in_ctx->is_hash_calc_disabled()) {
        ObIAllocator* row_allocator = cast_ctx.allocator_v2_;
        cast_ctx.allocator_v2_ = &exec_ctx->get_allocator();
        for (int i = 1; OB_SUCC(ret) && i < param_num; ++i) {
          if (!objs[i].is_null()) {
            const ObExprCalcType& cmp_type = cmp_types.at(i - 1);
            in_ctx->set_cmp_type(cmp_type);
            if (OB_FAIL(to_type(cmp_type.get_type(), cmp_type.get_collation_type(), cast_ctx, objs[i], out_obj))) {
              LOG_WARN("failed to cast obj", K(ret));
            } else if (OB_FAIL(in_ctx->add_to_hashset(out_obj))) {
              LOG_WARN("failed to add to hashset", K(ret));
            }
          } else {
            in_ctx->set_param_exist_null(true);
          }
        }
        cast_ctx.allocator_v2_ = row_allocator;
      }
    }
    // second we search in hashset.
    if (OB_SUCC(ret) && OB_NOT_NULL(in_ctx)) {
      if (OB_UNLIKELY(in_ctx->is_hash_calc_disabled())) {
        fall_back = true;
      } else {
        const ObExprCalcType& cmp_type = in_ctx->get_cmp_type();
        if (OB_FAIL(param_eval(expr_ctx, objs[0], 0))) {
          LOG_WARN("param evaluate failed", K(ret));
        } else {
          if (!objs[0].is_null() && !cmp_type.is_null()) {
            bool is_exist = false;
            if (OB_FAIL(to_type(cmp_type.get_type(), cmp_type.get_collation_type(), cast_ctx, objs[0], out_obj))) {
              LOG_WARN("failed to cast obj", K(ret));
            } else if (OB_FAIL(in_ctx->exist_in_hashset(out_obj, is_exist))) {
              LOG_WARN("failed to search in hashset", K(ret));
            } else {
              set_result(result, is_exist, in_ctx->is_param_exist_null());
            }
          } else {
            result.set_null();
          }
        }
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::hash_calc_for_vector(ObObj& result, const ObObj* objs, const ObIArray<ObExprCalcType>& cmp_types,
    int64_t param_num, ObExprCtx& expr_ctx, bool& fall_back) const
{
  int ret = OB_SUCCESS;
  int row_num = real_param_num_;
  int64_t cmp_type_count = cmp_types.count();

  ObExprInCtx* in_ctx = NULL;
  ObExecContext* exec_ctx = expr_ctx.exec_ctx_;
  ObObj out_obj;

  bool is_completely_cmp = false;
  bool is_null_cmp = false;
  bool left_has_null = false;
  bool is_right_all_null = false;
  bool is_left_all_null = false;

  int64_t right_param_num = row_num - 1;
  int row_dimension = row_dimension_;

  if (OB_ISNULL(objs) || OB_ISNULL(exec_ctx) || OB_UNLIKELY(param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("objs or exec_ctx is null, or param num is too small", K(ret), KP(objs), K(exec_ctx), K(param_num));
  } else if (OB_UNLIKELY(param_num < 0) || OB_UNLIKELY(row_num < 0) || OB_UNLIKELY(row_dimension < 0) ||
             OB_UNLIKELY(param_num != static_cast<int64_t>(row_dimension) * row_num) ||
             OB_UNLIKELY((cmp_type_count < row_dimension * (row_num - 1)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(row_num), K(row_dimension), K(param_num), K(cmp_type_count));
  }

  if (OB_SUCC(ret)) {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
    // otherwise ObObjCaster::to_type() will use cast_ctx.dest_collation_ as dst cs type
    cast_ctx.dest_collation_ = CS_TYPE_INVALID;
    if (NULL == (in_ctx = static_cast<ObExprInCtx*>(exec_ctx->get_expr_op_ctx(get_id())))) {
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(get_id(), in_ctx))) {
        LOG_WARN("failed to create operator ctx", K(ret));
      } else if (OB_FAIL(in_ctx->init_hashset_vecs(right_param_num, row_dimension, exec_ctx))) {
        LOG_WARN("failed to init hashset", K(ret));
      } else if (OB_FAIL(in_ctx->init_cmp_types(row_dimension, exec_ctx))) {
        LOG_WARN("failed to init cmp_types array", K(ret));
      } else if (OB_FAIL(in_ctx->init_hashset_vecs_all_null(row_dimension, exec_ctx))) {
        LOG_WARN("failed to init hashset vecs all null", K(ret));
      } else if (OB_FAIL(in_ctx->init_right_objs(right_param_num, row_dimension, exec_ctx))) {
        LOG_WARN("failed to init right objs", K(ret));
      } else {
        // evaluate parameters && check fall back
        for (int i = row_dimension; OB_SUCC(ret) && i < param_num; i++) {
          if (OB_FAIL(param_eval(expr_ctx, objs[i], i))) {
            LOG_DEBUG("param evaluate fail, hash set lookup disabled for in expr", K(ret), K(i));
            in_ctx->disable_hash_calc();
          }
        }
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret) && !in_ctx->is_hash_calc_disabled()) {
        ObIAllocator* row_allocator = cast_ctx.allocator_v2_;
        cast_ctx.allocator_v2_ = &exec_ctx->get_allocator();

        int64_t left_start_idx = 0;
        int64_t right_start_idx = row_dimension;

        for (int64_t i = 0; OB_SUCC(ret) && i < right_param_num; ++i, right_start_idx += row_dimension) {
          int null_idx = 0;
          for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension; ++j) {
            const ObObj& right_obj = objs[right_start_idx + j];
            const ObExprCalcType& cmp_type = cmp_types.at(row_dimension * i + j);
            in_ctx->set_cmp_types(cmp_type, row_dimension);
            if (OB_FAIL(to_type(cmp_type.get_type(), cmp_type.get_collation_type(), cast_ctx, right_obj, out_obj))) {
              LOG_WARN("failed to cast obj", K(ret));
            } else {
              if (right_obj.is_null()) {
                null_idx = null_idx ^ (1 << j);
                in_ctx->right_has_null = true;
                if (null_idx == ((1 << row_dimension) - 1)) {
                  is_right_all_null = true;
                }
              } else {
                // do nothing
              }
              if (OB_FAIL(in_ctx->set_right_obj(i, j, right_param_num, out_obj))) {
                LOG_WARN("failed to load right", K(ret), K(i), K(j));
              } else {
                // do nothing
              }
            }
          }
          /*
           *Iterate from 1~2^col, when the selected values are all non-null values, record this idx,
           *Set it into the row, this idx is used for the calculation of the hash value,
           *and the subscript into the hashset, and operator == is not used for compare_with_null
           *For the row with idx set, enter the corresponding hashtable,
           *At this time operator == requires the key value to match exactly
           */
          Row<ObObj> tmp_row;
          for (int64_t k = 1; OB_SUCC(ret) && k < (1 << row_dimension); ++k) {
            int hash_idx = k;
            if (0 == (k & null_idx)) {
              if (OB_FAIL(tmp_row.set_elem(in_ctx->get_obj_row(i)))) {
                LOG_WARN("failed to set elem", K(ret));
              }
              // This arrangement enters the corresponding position of the hash table
              if (OB_SUCC(ret)) {
                if (OB_FAIL(in_ctx->add_to_hashset_vecs(tmp_row, hash_idx))) {
                  LOG_WARN("failed to add hashset", K(ret));
                } else {
                  // do nothing
                }
              }
            } else if (null_idx == (k | null_idx)) {
              if (OB_FAIL(in_ctx->set_hashset_vecs_all_null_true(k))) {
                LOG_WARN("failed to set hahset set vecs all null true", K(ret));
              }
            } else {
              // This arrangement does not enter the hash set
            }
          }
        }
        cast_ctx.allocator_v2_ = row_allocator;
      }
    }
    // second we search in hashset.
    if (OB_SUCC(ret) && OB_NOT_NULL(in_ctx)) {
      if (OB_UNLIKELY(in_ctx->is_hash_calc_disabled())) {
        fall_back = true;
      } else {
        int null_idx = 0;
        // First traverse to extract the left vector
        Row<ObObj> tmp_row;
        ObObj obj_ptr[row_dimension];
        for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension; ++j) {
          const ObExprCalcType& cmp_type = in_ctx->get_cmp_types(j);
          if (OB_FAIL(param_eval(expr_ctx, objs[j], j))) {
            LOG_WARN("param evaluate failed", K(ret));
          } else {
            if (OB_FAIL(to_type(cmp_type.get_type(), cmp_type.get_collation_type(), cast_ctx, objs[j], out_obj))) {
              LOG_WARN("failed to cast obj", K(ret), K(j), K(cmp_type.get_type()));
            }
            // Detect the position of the null element and record
            if (objs[j].is_null() || cmp_type.is_null()) {
              null_idx = null_idx ^ (1 << j);
              left_has_null = true;
              if (null_idx == (1 << row_dimension) - 1) {
                is_left_all_null = true;
              }
            } else {
              // do nothing
            }
            obj_ptr[j] = out_obj;
          }
        }
        if (OB_SUCC(ret)) {
          tmp_row.set_elem(obj_ptr);
          if (null_idx != 0 && OB_FAIL(in_ctx->get_hashset_vecs_all_null(
                                   (1 << row_dimension) - 1 - null_idx /* invert*/, is_null_cmp))) {
            LOG_WARN("failed to get hashset vecs all null", K(ret));
          }

          // The left table takes out all non-nulls for permutation and combination,
          // and queries whether the hash value exists according to the assigned hashkey,
          // If it exists, take out this bucket for traversal,
          // follow the method cmp_with_null to reach the final conclusion, true ends directly
          // 0 for true, -1 for false, 1 for NULL
          int exist_ret = ObExprInHashMap<ObObj>::HASH_CMP_FALSE;
          for (int64_t k = (1 << row_dimension) - 1; !is_null_cmp && !is_completely_cmp && OB_SUCC(ret) && k >= 1;
               k = static_cast<int64_t>(
                   last(k, (1 << row_dimension) - 1))) {  // k represents the selected column, that is, idx
            if (0 == (k & null_idx)) {                    // k does not contain null columns
              if (OB_FAIL(in_ctx->exist_in_hashset_vecs(tmp_row, k, exist_ret))) {
                LOG_WARN("failed to search in hashset", K(ret));
              } else {
                if (ObExprInHashMap<ObObj>::HASH_CMP_TRUE == exist_ret) {
                  is_completely_cmp = true;
                } else if (ObExprInHashMap<ObObj>::HASH_CMP_UNKNOWN == exist_ret) {
                  is_null_cmp = true;
                } else {
                  // do nothing
                }
              }
              if (!left_has_null && !in_ctx->right_has_null) {
                break;
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (!is_completely_cmp && (is_null_cmp || is_right_all_null || is_left_all_null)) {
            result.set_null();
          } else {
            set_result(result, is_completely_cmp, false);
          }
        }
      }
    }
  }

  return ret;
}

inline int ObExprInOrNotIn::to_type(const ObObjType expect_type, const ObCollationType expect_cs_type,
    ObCastCtx& cast_ctx, const ObObj& in_obj, ObObj& out_obj) const
{
  int ret = OB_SUCCESS;
  if (ob_is_string_type(expect_type) && in_obj.is_string_type() && in_obj.get_collation_type() == expect_cs_type) {
    out_obj = in_obj;
    out_obj.set_type(expect_type);
    out_obj.set_collation_type(expect_cs_type);
  } else {
    // otherwise ObObjCaster::to_type() will use cast_ctx.dest_collation_ as dst cs type
    cast_ctx.dest_collation_ = CS_TYPE_INVALID;
    ret = ObObjCaster::to_type(expect_type, expect_cs_type, cast_ctx, in_obj, out_obj);
  }
  return ret;
}

inline bool ObExprInOrNotIn::need_hash(ObExecContext* exec_ctx) const
{
  return is_param_all_const() && is_param_all_same_type() && is_param_all_same_cs_type() && NULL != exec_ctx &&
         exec_ctx->is_expr_op_ctx_inited();
}

OB_SERIALIZE_MEMBER(ObExprInOrNotIn, row_dimension_, real_param_num_, result_type_, input_types_, id_, param_flags_);

ObExprIn::ObExprIn(ObIAllocator& alloc) : ObExprInOrNotIn(alloc, T_OP_IN, N_IN)
{}

void ObExprIn::set_result(ObObj& result, bool is_exist, bool param_exist_null) const
{
  if (!is_exist && param_exist_null) {
    result.set_null();
  } else {
    result.set_int(static_cast<int64_t>(is_exist));
  }
}

ObExprNotIn::ObExprNotIn(ObIAllocator& alloc) : ObExprInOrNotIn(alloc, T_OP_NOT_IN, N_NOT_IN)
{}

void ObExprNotIn::set_result(ObObj& result, bool is_exist, bool param_exist_null) const
{
  if (!is_exist && param_exist_null) {
    result.set_null();
  } else {
    result.set_int(static_cast<int64_t>(!is_exist));
  }
}

int ObExprInOrNotIn::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != raw_expr.get_param_count()) || OB_ISNULL(expr_cg_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for in expr", K(ret));
  } else if (OB_ISNULL(raw_expr.get_param_expr(0)) || OB_ISNULL(raw_expr.get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null param expr", K(ret));
  } else if (T_REF_QUERY == raw_expr.get_param_expr(1)->get_expr_type()) {
    // xx in (subquery) has been transformed to xx =ANY()
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid right expr type", K(ret));
  } else if (T_REF_QUERY == raw_expr.get_param_expr(0)->get_expr_type()
             // output column == 1 The subplan filter is responsible for iterating the data
             && raw_expr.get_param_expr(0)->get_output_column() > 1) {
    ret = cg_expr_with_subquery(*expr_cg_ctx.allocator_, raw_expr, rt_expr);
  } else if (T_OP_ROW == raw_expr.get_param_expr(0)->get_expr_type()) {
    ret = cg_expr_with_row(*expr_cg_ctx.allocator_, raw_expr, rt_expr);
  } else {
    ret = cg_expr_without_row(*expr_cg_ctx.allocator_, raw_expr, rt_expr);
  }
  return ret;
}

int ObExprInOrNotIn::cg_expr_without_row(ObIAllocator& allocator, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) ||
      OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    rt_expr.inner_func_cnt_ = rt_expr.args_[1]->arg_cnt_;
    void** func_buf = NULL;
    int func_buf_size = sizeof(void*) * rt_expr.inner_func_cnt_;
    if (OB_ISNULL(func_buf = (void**)allocator.alloc(func_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      ObObjType left_type = rt_expr.args_[0]->datum_meta_.type_;
      ObCollationType left_cs = rt_expr.args_[0]->datum_meta_.cs_type_;
      ObObjType right_type = rt_expr.args_[1]->args_[0]->datum_meta_.type_;
      rt_expr.inner_functions_ = func_buf;
      DatumCmpFunc func_ptr =
          ObExprCmpFuncsHelper::get_datum_expr_cmp_func(left_type, right_type, lib::is_oracle_mode(), left_cs);
      for (int i = 0; i < rt_expr.inner_func_cnt_; i++) {
        rt_expr.inner_functions_[i] = (void*)func_ptr;
      }
      if (!is_param_all_const()) {
        rt_expr.eval_func_ = &ObExprInOrNotIn::eval_in_without_row_fallback;
      } else {
        rt_expr.eval_func_ = &ObExprInOrNotIn::eval_in_without_row;
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::cg_expr_with_row(ObIAllocator& allocator, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) ||
      OB_ISNULL(rt_expr.args_[1]) || OB_ISNULL(rt_expr.args_[1]->args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSEArray<ObObjType, 8> left_types;
    ObSEArray<ObCollationType, 8> left_cs_arr;
    ObSEArray<ObObjType, 8> right_types;

#define LEFT_ROW rt_expr.args_[0]
#define LEFT_ROW_ELE(i) rt_expr.args_[0]->args_[i]
#define RIGHT_ROW(i) rt_expr.args_[1]->args_[i]
#define RIGHT_ROW_ELE(i, j) rt_expr.args_[1]->args_[i]->args_[j]

    for (int i = 0; OB_SUCC(ret) && i < LEFT_ROW->arg_cnt_; i++) {
      if (OB_ISNULL(LEFT_ROW_ELE(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null args", K(ret));
      } else if (OB_FAIL(left_types.push_back(LEFT_ROW_ELE(i)->datum_meta_.type_))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(left_cs_arr.push_back(LEFT_ROW_ELE(i)->datum_meta_.cs_type_))) {
        LOG_WARN("failed to push back element", K(ret));
      } else { /* do nothing */
      }
    }  // end for

    for (int i = 0; OB_SUCC(ret) && i < RIGHT_ROW(0)->arg_cnt_; i++) {
      if (OB_FAIL(right_types.push_back(RIGHT_ROW_ELE(0, i)->datum_meta_.type_))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      void** func_buf = NULL;
      int func_buf_size = sizeof(void*) * LEFT_ROW->arg_cnt_;
      rt_expr.inner_func_cnt_ = LEFT_ROW->arg_cnt_;
      if (OB_ISNULL(func_buf = (void**)allocator.alloc(func_buf_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        for (int i = 0; i < left_types.count(); i++) {
          DatumCmpFunc func_ptr = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
              left_types.at(i), right_types.at(i), lib::is_oracle_mode(), left_cs_arr.at(i));
          func_buf[i] = (void*)func_ptr;
        }  // end for
        if (!is_param_all_const()) {
          rt_expr.eval_func_ = &ObExprInOrNotIn::eval_in_with_row_fallback;
        } else {
          rt_expr.eval_func_ = &ObExprInOrNotIn::eval_in_with_row;
        }
        rt_expr.inner_functions_ = func_buf;
      }
    }
#undef LEFT_ROW
#undef LEFT_ROW_ELE
#undef RIGHT_ROW
#undef RIGHT_ROW_ELE
  }
  return ret;
}
#undef GET_CS_TYPE

int ObExprInOrNotIn::cg_expr_with_subquery(
    common::ObIAllocator& allocator, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) ||
      OB_ISNULL(rt_expr.args_[1]) || OB_ISNULL(rt_expr.args_[1]->args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
#define RIGHT_ROW(i) rt_expr.args_[1]->args_[i]
#define RIGHT_ROW_ELE(i, j) rt_expr.args_[1]->args_[i]->args_[j]
    ObSEArray<ObObjMeta, 1> left_types;
    ObSEArray<ObObjMeta, 1> right_types;
    void** funcs = NULL;

    CK(2 == raw_expr.get_param_count());
    CK(NULL != raw_expr.get_param_expr(0));
    CK(NULL != raw_expr.get_param_expr(1));

    OZ(get_param_types(*raw_expr.get_param_expr(0), true, left_types));
    // OZ(get_param_types(*raw_expr.get_param_expr(1), false, right_types));

    if (OB_FAIL(ret)) {
    } else if (left_types.empty() || left_types.count() != RIGHT_ROW(0)->arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operand cnt mismatch", K(ret), K(left_types.count()), K(RIGHT_ROW(0)->arg_cnt_));
    } else if (OB_ISNULL(funcs = (void**)allocator.alloc(sizeof(void*) * left_types.count()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      rt_expr.inner_func_cnt_ = left_types.count();
      rt_expr.inner_functions_ = funcs;

      for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.inner_func_cnt_; i++) {
        auto& l = left_types.at(i);
        auto& r = RIGHT_ROW_ELE(0, i)->obj_meta_;
        if (ObDatumFuncs::is_string_type(l.get_type()) && ObDatumFuncs::is_string_type(r.get_type())) {
          CK(l.get_collation_type() == r.get_collation_type());
        }
        if (OB_SUCC(ret)) {
          funcs[i] = (void*)ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
              l.get_type(), r.get_type(), lib::is_oracle_mode(), l.get_collation_type());
          CK(NULL != funcs[i]);
        }
      }
      if (OB_SUCC(ret)) {
        rt_expr.eval_func_ = &eval_in_with_subquery;
      }
    }
#undef RIGHT_ROW
#undef RIGHT_ROW_ELE
  }
  return ret;
}

int ObExprInOrNotIn::eval_in_with_row_fallback(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return calc_for_row_static_engine(expr, ctx, expr_datum, nullptr);
}

int ObExprInOrNotIn::eval_in_without_row_fallback(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  // TODO: The original In or NotIn implementation, if there is no vector, and meet the
  // condition of need_hash will first calculate the node values of all the right children and
  // construct the hash table, now only the short-circuit logic comparison is implemented
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool cnt_null = false;
  bool is_equal = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
    LOG_WARN("failed to eval left", K(ret));
  } else if (left->is_null()) {
    cnt_null = true;
  } else {
    int cmp_ret = 0;
    for (int i = 0; OB_SUCC(ret) && !is_equal && i < expr.inner_func_cnt_; i++) {
      if (OB_ISNULL(expr.args_[1]->args_[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null arg", K(ret), K(expr.args_[1]->args_[i]), K(i));
      } else if (OB_FAIL(expr.args_[1]->args_[i]->eval(ctx, right))) {
        LOG_WARN("failed to eval right datum", K(ret));
      } else if (right->is_null()) {
        cnt_null = true;
      } else {
        cmp_ret = ((DatumCmpFunc)expr.inner_functions_[0])(*left, *right);
        if (0 == cmp_ret) {
          is_equal = true;
        } else {
          // do nothing
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_datum_result(T_OP_IN == expr.type_, is_equal, cnt_null, expr_datum);
  }
  return ret;
}

int ObExprInOrNotIn::eval_in_with_row(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  ObExprInCtx* in_ctx = NULL;
  ObExecContext* exec_ctx = &ctx.exec_ctx_;
  uint64_t in_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  bool is_completely_cmp = false;
  bool is_null_cmp = false;
  bool left_has_null = false;
  bool is_right_all_null = false;
  bool is_left_all_null = false;

  bool fallback = false;

#define LEFT_ROW expr.args_[0]
#define LEFT_ROW_ELE(i) expr.args_[0]->args_[i]
#define RIGHT_ROW(i) expr.args_[1]->args_[i]
#define RIGHT_ROW_ELE(i, j) expr.args_[1]->args_[i]->args_[j]
  int64_t right_param_num = expr.args_[1]->arg_cnt_;
  int64_t row_dimension = expr.inner_func_cnt_;
  if (row_dimension > 3) {
    fallback = true;
  }

  ObIAllocator& calc_alloc = exec_ctx->get_allocator();
  if (!fallback && OB_SUCC(ret) && NULL == (in_ctx = static_cast<ObExprInCtx*>(exec_ctx->get_expr_op_ctx(in_id)))) {
    if (OB_FAIL(exec_ctx->create_expr_op_ctx(in_id, in_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret));
    } else if (OB_FAIL(in_ctx->init_static_engine_hashset_vecs(right_param_num, row_dimension, exec_ctx))) {
      LOG_WARN("failed to init hashset", K(ret));
    } else if (OB_FAIL(in_ctx->init_hashset_vecs_all_null(row_dimension, exec_ctx))) {
      LOG_WARN("failed to init hashset_vecs_all_null", K(ret));
    } else if (OB_FAIL(in_ctx->init_right_datums(right_param_num, row_dimension, exec_ctx))) {
      LOG_WARN("failed to init right datums", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < right_param_num; ++i) {
        if (OB_ISNULL(RIGHT_ROW(i))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null arg", K(ret), K(RIGHT_ROW(i)), K(i));
        } else {
          int null_idx = 0;
          // Traverse the entire vector and record the position of the null element;
          // arrange the remaining non-null values and insert the corresponding position
          // after bitwise XOR with the null element
          for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension; ++j) {
            if (OB_ISNULL(RIGHT_ROW_ELE(i, j))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid null arg", K(ret), K(RIGHT_ROW_ELE(i, j)), K(i), K(j));
            } else if (OB_FAIL(RIGHT_ROW_ELE(i, j)->eval(ctx, right))) {
              LOG_DEBUG("param evaluate fail, hash set lookup disabled for in expr", K(ret), K(i));
              in_ctx->disable_hash_calc();
            } else if (!in_ctx->is_hash_calc_disabled()) {
              if (right->is_null()) {
                null_idx = null_idx ^ (1 << j);
                in_ctx->right_has_null = true;
                if (null_idx == ((1 << row_dimension) - 1)) {
                  in_ctx->ctx_hash_null_ = true;
                  is_right_all_null = true;
                }
              } else {
                // do nothing
              }
              if (OB_FAIL(in_ctx->set_right_datum(i, j, right_param_num, *right))) {
                LOG_WARN("failed to load right", K(ret), K(i), K(j));
              } else {
                if (OB_ISNULL(in_ctx->hash_func_buff_)) {
                  int func_buf_size = sizeof(void*) * row_dimension;
                  if (OB_ISNULL(in_ctx->hash_func_buff_ = (void**)(exec_ctx->get_allocator()).alloc(func_buf_size))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("failed to allocate memory", K(ret));
                  }
                }
                if (OB_SUCC(ret)) {
                  in_ctx->hash_func_buff_[j] = (void*)(RIGHT_ROW_ELE(i, j)->basic_funcs_->murmur_hash_);
                }
              }
            } else {
              // do nothing
            }
          }
          if (OB_SUCC(ret) && !in_ctx->funcs_ptr_set) {
            for (int i = 0; i < (1 << row_dimension); ++i) {
              in_ctx->set_hash_funcs_ptr(i, in_ctx->hash_func_buff_);
              in_ctx->set_cmp_funcs_ptr(i, expr.inner_functions_);
            }
            in_ctx->funcs_ptr_set = true;
          }

          /*
           *Iterate from 1~2^col, when the selected values are all non-null values, record this idx,
           *Set it into the row, this idx is used for the calculation of the hash value,
           *and the subscript into the hashset, and operator == is not used for compare_with_null
           *For the row with idx set, enter the corresponding hashtable,
           *At this time operator == requires the key value to match exactly
           */
          Row<ObDatum> tmp_row;
          for (int64_t k = 1; OB_SUCC(ret) && k < (1 << row_dimension); ++k) {
            int hash_idx = k;
            if (0 == (k & null_idx)) {
              if (OB_FAIL(tmp_row.set_elem(in_ctx->get_datum_row(i)))) {
                LOG_WARN("failed to set elem", K(ret));
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(in_ctx->add_to_static_engine_hashset_vecs(tmp_row, hash_idx))) {
                  LOG_WARN("failed to add hashset", K(ret));
                } else {
                  // do nothing
                }
              }
            } else if (null_idx ==
                       (k | null_idx)) {  // The column selected by k is a subset of null, set all null here to true
              if (OB_FAIL(in_ctx->set_hashset_vecs_all_null_true(k))) {
                LOG_WARN("failed to set hashset vecs all null true", K(ret));
              }
            } else {
              // This time the arrangement is not in the hash table
            }
          }
        }
      }
    }
  }
  // second we search in hashset
  if (!fallback && OB_SUCC(ret) && OB_NOT_NULL(in_ctx)) {
    if (OB_UNLIKELY(in_ctx->is_hash_calc_disabled())) {
      // fall_back = true;//TODO : lack param fallback
    } else if (!fallback) {
      // Traverse and extract the left vector
      int null_idx = 0;
      Row<ObDatum> tmp_row;
      ObDatum datum_ptr[row_dimension];
      for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension; ++j) {
        if (OB_ISNULL(LEFT_ROW_ELE(j))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null arg", K(LEFT_ROW_ELE(j)), K(j));
        } else if (OB_FAIL(LEFT_ROW_ELE(j)->eval(ctx, left))) {
          LOG_WARN("failed to eval", K(ret));
        } else {
          if (left->is_null()) {
            null_idx = null_idx ^ (1 << j);
            left_has_null = true;
            if (null_idx == ((1 << row_dimension) - 1)) {
              is_left_all_null = true;
            }
          } else {
            // do nothing
          }
          datum_ptr[j] = *left;
        }
      }
      if (OB_SUCC(ret)) {
        tmp_row.set_elem(datum_ptr);
        if (null_idx != 0 &&
            OB_FAIL(in_ctx->get_hashset_vecs_all_null((1 << row_dimension) - 1 - null_idx /* invert */, is_null_cmp))) {
          LOG_WARN("failed to get hashset vecs all null", K(ret));
        }

        // The left table takes out all non-nulls for permutation and combination,
        // and queries whether the hash value exists according to the assigned hashkey,
        // If it exists, take out this bucket for traversal,
        // follow the method cmp_with_null to reach the final conclusion, true ends directly
        int exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_FALSE;
        for (int64_t k = (1 << row_dimension) - 1; !is_null_cmp && !is_completely_cmp && OB_SUCC(ret) && k >= 1;
             k = static_cast<int64_t>(
                 last(k, (1 << row_dimension) - 1))) {  // k represents the selected column, that is, idx
          if (0 == (k & null_idx)) {                    // k does not contain null columns
            if (OB_FAIL(in_ctx->exist_in_static_engine_hashset_vecs(tmp_row, k, exist_ret))) {
              LOG_WARN("failed to search in hashset", K(ret));
            } else {
              if (ObExprInHashMap<ObDatum>::HASH_CMP_TRUE == exist_ret) {
                is_completely_cmp = true;
              } else if (ObExprInHashMap<ObDatum>::HASH_CMP_UNKNOWN == exist_ret) {
                is_null_cmp = true;
              } else {
                // do nothing
              }
            }
            if (!left_has_null && !in_ctx->right_has_null) {
              break;
            }
          }
        }
      }
    }
  }

#undef LEFT_ROW
#undef LEFT_ROW_ELE
#undef RIGHT_ROW
#undef RIGHT_ROW_ELE
  if (!fallback && OB_SUCC(ret)) {
    if (OB_NOT_NULL(in_ctx) && in_ctx->ctx_hash_null_) {
      is_null_cmp = true;
    }
    if (!is_completely_cmp && (is_null_cmp || is_right_all_null || is_left_all_null)) {
      expr_datum.set_null();
    } else {
      set_datum_result(T_OP_IN == expr.type_, is_completely_cmp, false, expr_datum);
    }
  }
  if (fallback) {
    ret = eval_in_with_row_fallback(expr, ctx, expr_datum);
  }
  return ret;
}

int ObExprInOrNotIn::eval_in_without_row(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObExprInCtx* in_ctx = NULL;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool cnt_null = false;
  bool is_exist = false;
  ObExecContext* exec_ctx = &ctx.exec_ctx_;

  uint64_t in_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  bool fallback = false;
  ObIAllocator& calc_alloc = exec_ctx->get_allocator();

  if (!fallback && OB_SUCC(ret)) {
    if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
      LOG_WARN("failed to eval left", K(ret));
    } else if (left->is_null()) {
      is_exist = false;
      cnt_null = true;
    } else {
      int64_t right_param_num = expr.inner_func_cnt_;
      int64_t row_dimension = 1;
      if (NULL == (in_ctx = static_cast<ObExprInCtx*>(exec_ctx->get_expr_op_ctx(in_id)))) {
        if (OB_FAIL(exec_ctx->create_expr_op_ctx(in_id, in_ctx))) {
          LOG_WARN("failed to create operator ctx", K(ret));
        } else if (OB_FAIL(in_ctx->init_static_engine_hashset(right_param_num))) {
          LOG_WARN("failed to init hashset", K(ret));
        } else if (OB_FAIL(in_ctx->init_right_datums(right_param_num, row_dimension, exec_ctx))) {
          LOG_WARN("failed to init right datums", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < right_param_num; ++i) {
            if (OB_ISNULL(expr.args_[1]->args_[i])) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid null arg", K(ret), K(expr.args_[1]->args_[i]), K(i));
            } else if (OB_FAIL(expr.args_[1]->args_[i]->eval(ctx, right))) {
              LOG_DEBUG("param evaluate fail, hash set lookup disabled for in expr", K(ret), K(i));
              in_ctx->disable_hash_calc();
            } else if (right->is_null()) {
              cnt_null = true;
              in_ctx->ctx_hash_null_ = true;
            } else if (!in_ctx->is_hash_calc_disabled()) {
              if (OB_FAIL(in_ctx->set_right_datum(i, 0, right_param_num, *right))) {
                LOG_WARN("failed to load right", K(ret), K(i));
              } else {
                if (OB_ISNULL(in_ctx->hash_func_buff_)) {
                  int func_buf_size = sizeof(void*) * 1;
                  if (OB_ISNULL(in_ctx->hash_func_buff_ = (void**)(exec_ctx->get_allocator()).alloc(func_buf_size))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("failed to allocate memory", K(ret));
                  }
                }
                if (OB_SUCC(ret)) {
                  in_ctx->hash_func_buff_[0] = (void*)(expr.args_[1]->args_[i]->basic_funcs_->murmur_hash_);
                }
              }
              Row<ObDatum> tmp_row;
              // All hash functions and cmp functions have been loaded here,
              // set the function pointer of tmp_row
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(tmp_row.set_elem(in_ctx->get_datum_row(i)))) {
                LOG_WARN("failed to load datum", K(ret), K(i));
              } else {
                in_ctx->set_hash_funcs_ptr_for_set(in_ctx->hash_func_buff_);
                in_ctx->set_cmp_funcs_ptr_for_set(expr.inner_functions_);
              }
              if (OB_SUCC(ret) && OB_FAIL(in_ctx->add_to_static_engine_hashset(tmp_row))) {
                LOG_WARN("failed to add to hashset", K(ret));
              }
            } else {
              // do nothing
            }
          }
        }
      }

      // second we search in hashset.
      if (OB_SUCC(ret) && OB_NOT_NULL(in_ctx)) {
        if (OB_UNLIKELY(in_ctx->is_hash_calc_disabled())) {
          // do nothing
        } else if (!left->is_null()) {
          int null_idx = 0;
          Row<ObDatum> tmp_row;
          ObDatum* datum_ptr = left;

          if (OB_FAIL(ret)) {

          } else if (OB_FAIL(tmp_row.set_elem(datum_ptr))) {
            LOG_WARN("failed to load left", K(ret));
          } else if (0 != in_ctx->get_static_engine_hashset_size() &&
                     OB_FAIL(in_ctx->exist_in_static_engine_hashset(tmp_row, is_exist))) {
            LOG_WARN("failed to search in hashset", K(ret));
          } else {
            // do nothing
          }
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(in_ctx) && in_ctx->ctx_hash_null_) {
        cnt_null = true;
      }
      if (!is_exist && cnt_null) {
        expr_datum.set_null();
      } else {
        set_datum_result(T_OP_IN == expr.type_, is_exist, false, expr_datum);
      }
    }
  } else if (OB_SUCC(ret)) {
    ret = eval_in_without_row_fallback(expr, ctx, expr_datum);
  } else {
    // do nothing
  }
  return ret;
}

int ObExprInOrNotIn::eval_in_with_subquery(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_datum);
  ObSubQueryIterator* l_iter = NULL;
  ObExpr** l_row = NULL;
  if (OB_FAIL(setup_row(expr.args_, ctx, true, expr.inner_func_cnt_, l_iter, l_row))) {
    LOG_WARN("setup left row failed", K(ret));
  } else if (OB_ISNULL(l_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null row", K(ret));
  } else {
    bool l_end = false;
    if (OB_NOT_NULL(l_iter)) {
      if (OB_FAIL(l_iter->get_next_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          l_end = true;
          // set row to NULL
          for (int64_t i = 0; i < expr.inner_func_cnt_; ++i) {
            l_row[i]->locate_expr_datum(ctx).set_null();
            l_row[i]->get_eval_info(ctx).evaluated_ = true;
          }
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_for_row_static_engine(expr, ctx, expr_datum, l_row))) {
      LOG_WARN("calc for row failed", K(ret), K(l_row));
    }
    if (OB_SUCC(ret) && NULL != l_iter && !l_end) {
      if (OB_FAIL(l_iter->get_next_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else {
        // only one row expected for left row
        ret = OB_SUBQUERY_TOO_MANY_ROW;
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::calc_for_row_static_engine(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, ObExpr** l_row)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_datum);
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool set_cnt_null = false;
  bool set_cnt_equal = false;
#define RIGHT_ROW(i) expr.args_[1]->args_[i]
#define RIGHT_ROW_ELE(i, j) expr.args_[1]->args_[i]->args_[j]
  for (int i = 0; OB_SUCC(ret) && !set_cnt_equal && i < expr.args_[1]->arg_cnt_; ++i) {
    if (OB_ISNULL(RIGHT_ROW(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null arg", K(ret), K(RIGHT_ROW(i)), K(i));
    } else {
      bool row_is_equal = true;
      bool row_cnt_null = false;
      ObExpr* left_expr = nullptr;
      for (int j = 0; OB_SUCC(ret) && row_is_equal && j < expr.inner_func_cnt_; ++j) {
        if (OB_ISNULL(l_row)) {
          left_expr = expr.args_[0]->args_[j];
        } else {
          left_expr = l_row[j];
        }
        if (OB_ISNULL(left_expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null arg", K(ret));
        } else if (OB_FAIL(left_expr->eval(ctx, left))) {
          LOG_WARN("failed to eval", K(ret));
        } else if (left->is_null()) {
          row_cnt_null = true;
        } else if (OB_ISNULL(RIGHT_ROW_ELE(i, j))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null arg", K(ret), K(RIGHT_ROW_ELE(i, j)), K(i), K(j));
        } else if (OB_FAIL(RIGHT_ROW_ELE(i, j)->eval(ctx, right))) {
          LOG_WARN("failed to eval", K(ret));
        } else if (right->is_null()) {
          row_cnt_null = true;
        } else {
          int cmp_ret = ((DatumCmpFunc)expr.inner_functions_[j])(*left, *right);
          if (0 != cmp_ret) {
            row_is_equal = false;
            row_cnt_null = false;
          }
        }
      }  // inner loop
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (row_is_equal && !row_cnt_null) {
        set_cnt_equal = true;
      } else if (row_cnt_null) {
        set_cnt_null = true;
      }
    }
  }
#undef RIGHT_ROW
#undef RIGHT_ROW_ELE
  if (OB_SUCC(ret)) {
    set_datum_result(T_OP_IN == expr.type_, set_cnt_equal, set_cnt_null, expr_datum);
  }
  return ret;
}

void ObExprInOrNotIn::set_datum_result(
    const bool is_expr_in, const bool is_exist, const bool param_exist_null, ObDatum& expr_datum)
{
  if (!is_exist && param_exist_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_int(is_expr_in ? is_exist : !is_exist);
  }
}

int ObExprInOrNotIn::setup_row(ObExpr** expr, ObEvalCtx& ctx, const bool is_iter, const int64_t cmp_func_cnt,
    ObSubQueryIterator*& iter, ObExpr**& row)
{
  int ret = OB_SUCCESS;
  if (is_iter) {
    ObDatum* v = NULL;
    if (OB_FAIL(expr[0]->eval(ctx, v))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else if (v->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL subquery ref info returned", K(ret));
    } else if (OB_FAIL(ObExprSubQueryRef::get_subquery_iter(
                   ctx, ObExprSubQueryRef::ExtraInfo::get_info(v->get_int()), iter))) {
      LOG_WARN("get subquery iterator failed", K(ret));
    } else if (OB_ISNULL(iter) || cmp_func_cnt != iter->get_output().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL subquery iterator", K(ret), KP(iter), K(cmp_func_cnt));
    } else if (OB_FAIL(iter->start())) {
      LOG_WARN("start iterate failed", K(ret));
    } else {
      row = &const_cast<ExprFixedArray&>(iter->get_output()).at(0);
    }
  } else if (T_OP_ROW == expr[0]->type_) {
    if (cmp_func_cnt != expr[0]->arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp function count mismatch", K(ret), K(cmp_func_cnt), K(*expr[0]));
    } else {
      row = expr[0]->args_;
    }
  } else {
    row = expr;
  }
  return ret;
}

int ObExprInOrNotIn::get_param_types(const ObRawExpr& param, const bool is_iter, ObIArray<ObObjMeta>& types) const
{
  int ret = OB_SUCCESS;
  if (param.get_expr_type() == T_OP_ROW) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.get_param_count(); i++) {
      const ObRawExpr* e = param.get_param_expr(i);
      CK(NULL != e);
      OZ(types.push_back(e->get_result_meta()));
    }
  } else if (param.get_expr_type() == T_REF_QUERY && is_iter) {
    const ObQueryRefRawExpr& ref = static_cast<const ObQueryRefRawExpr&>(param);
    FOREACH_CNT_X(t, ref.get_column_types(), OB_SUCC(ret))
    {
      OZ(types.push_back(*t));
    }
  } else {
    OZ(types.push_back(param.get_result_meta()));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
