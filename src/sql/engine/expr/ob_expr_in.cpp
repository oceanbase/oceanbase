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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/expr/ob_expr_in.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"

namespace oceanbase
{
using namespace common;
using namespace hash;
namespace sql
{
//计算当前数字二进制排列组合的下一个数， 例如 001->010->100
static unsigned next_perm(unsigned int cur_num)
{
    unsigned int t = cur_num | (cur_num - 1); // t gets cur_num's least significant 0 bits set to 1
    // Next set to 1 the most significant bit to change,
    // set to 0 the least significant ones, and add the necessary 1 bits.
    return (t + 1) | (((~t & -~t) - 1) >> (__builtin_ctz(cur_num) + 1));
}
//计算当前数字二进制排列组合的下一个数，如果当前数字的高位全部为1，则增加一个1并放到低位
//例如 11000->00111
static unsigned next(unsigned int cur_num, unsigned int max)
{
  // 这里和next_perm中重复计算的cur | (cur_num - 1)会被编译器自动在内联时提取出来，不会额外计算一次
  return ((cur_num - 1) | cur_num) >= max - 1
         ? (1U << (__builtin_popcount(cur_num) + 1)) - 1 : next_perm(cur_num);
}
//用于求解cur num的上一个二进制排列组合， 例如 111->110->101->011->100->010->001， max为111
static unsigned last(unsigned int cur_num, unsigned int max)
{
  unsigned int num = (cur_num ^ max);
  return (max ^ next(num, max));
}

template <>
bool Row<ObDatum>::equal_key(const Row<ObDatum> &other, void **cmp_funcs, const int idx) const
{
  bool equal_ret = false;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_)) {
  } else {
    bool is_equal = true;
    int curr_idx = idx;
    for (int i = 0; is_equal && 0 != curr_idx; ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        if (elems_[i].is_null() && other.elems_[i].is_null()) {
          //true
        } else if (elems_[i].is_null() || other.elems_[i].is_null()) {
          is_equal = false;
        } else {
          int cmp_ret = 0;
          // lob type will not use in expr with hash, can ignore ret here
          (void)((DatumCmpFunc)cmp_funcs[i])(elems_[i], other.elems_[i], cmp_ret);
          if (0 != cmp_ret) {
            is_equal = false;
          } else {
            //do nothing
          }
        }
      }
    }
    equal_ret = is_equal;
  }
  return equal_ret;
}

template <>
bool Row<ObObj>::equal_key(const Row<ObObj> &other, void **cmp_funcs, const int idx) const
{
  UNUSED(cmp_funcs);/**nullptr**/
  bool equal_ret = false;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_)) {
  } else {
    bool is_equal = true;
    int curr_idx = idx;
    for (int i = 0; is_equal && 0 != curr_idx; ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        if (elems_[i].is_null() && other.elems_[i].is_null()) {
          //true
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
int Row<ObDatum>::hash_key(void **hash_funcs, const int idx, uint64_t seed, uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = 0;
  if (OB_ISNULL(elems_)) {
  } else {
    int curr_idx = idx;
    for (int i = 0; 0 != curr_idx && OB_SUCC(ret); ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        ret = ((ObExprHashFuncType)hash_funcs[i])(elems_[i], seed, seed);
      } else {
        continue;
      }
    }
    hash_val = seed;
  }
  return ret;
}

template <>
int Row<ObObj>::hash_key(void **hash_funcs, const int idx, uint64_t seed, uint64_t &hash_val) const
{
  UNUSED(hash_funcs);/**nullptr**/
  int ret = OB_SUCCESS;
  hash_val = 0;
  if (OB_ISNULL(elems_)) {
  } else {
    int curr_idx = idx;
    for (int i = 0; 0 != curr_idx && OB_SUCC(ret); ++i, curr_idx = curr_idx >> 1) {
      if (1 == (curr_idx & 1)) {
        ret = elems_[i].hash(seed, seed);
      } else {
        continue;
      }
    }
    hash_val = seed;
  }
  return ret;
}

template <>
int Row<ObDatum>::compare_with_null(const Row<ObDatum> &other,
                                    void **cmp_funcs,
                                    const int64_t row_dimension,
                                    int &exist_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_) || OB_ISNULL(cmp_funcs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer param or function", K(ret));
  } else if (row_dimension > 0) {
    exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_TRUE;
    for (int i = 0;
         ObExprInHashMap<ObDatum>::HASH_CMP_FALSE != exist_ret && i < row_dimension && OB_SUCC(ret); ++i) {
      if (elems_[i].is_null() || other.elems_[i].is_null()) {
        exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_UNKNOWN;
      } else {
        int cmp_ret = 0;
        if (OB_FAIL(((DatumCmpFunc)cmp_funcs[i])(elems_[i], other.elems_[i], cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (0 != cmp_ret) {
          exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_FALSE;
        } else {
          //do nothing
        }
      }
    }
  }
  return ret;
}
//0 for true, -1 for false, 1 for NULL
template <>
int Row<ObObj>::compare_with_null(const Row<ObObj> &other,
                                  void **cmp_funcs,
                                  const int64_t row_dimension,
                                  int &exist_ret) const
{
  UNUSED(cmp_funcs);/**nullptr**/
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.elems_) || OB_ISNULL(elems_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer param", K(ret));
  } else if (row_dimension > 0) {
    exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_TRUE;
    for (int i = 0;
         ObExprInHashMap<ObObj>::HASH_CMP_FALSE != exist_ret && i < row_dimension; ++i) {
      if (elems_[i].is_null() || other.elems_[i].is_null()) {
        exist_ret = ObExprInHashMap<ObObj>::HASH_CMP_UNKNOWN;
      } else {
        int cmp_ret = (elems_[i] == other.elems_[i]) ? 0 : -1;
        if (0 != cmp_ret) {
          exist_ret = ObExprInHashMap<ObObj>::HASH_CMP_FALSE;
        } else {
          //do nothing
        }
      }
    }
  }
  return ret;
}

template <class T>
int Row<T>::set_elem(T *elems)
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
bool RowKey<T>::operator==(const RowKey<T> &other) const
{
  return row_.equal_key(other.row_, meta_->cmp_funcs_, meta_->idx_);
}

template <class T>
int RowKey<T>::hash(uint64_t &hash_val, uint64_t seed) const
{
  return row_.hash_key(meta_->hash_funcs_, meta_->idx_, seed, hash_val);
}

template <class T>
int ObExprInHashMap<T>::set_refactored(const Row<T> &row)
{
  int ret = OB_SUCCESS;
  ObArray<Row<T>> *arr_ptr = NULL;
  RowKey<T> tmp_row_key;
  tmp_row_key.row_= row;
  tmp_row_key.meta_ = &meta_;
  if (OB_ISNULL(arr_ptr = const_cast<ObArray<Row<T>> *> (map_.get(tmp_row_key)))) {
    ObArray<Row<T>> arr;
    if (OB_FAIL(arr.push_back(row))) {
      LOG_WARN("failed to load row", K(ret));
    } else {
      ret = map_.set_refactored(tmp_row_key, arr);
    }
  } else {
    int exist = ObExprInHashMap<T>::HASH_CMP_FALSE;
    //去重
    for (int i = 0; OB_SUCC(ret)
                    && ObExprInHashMap<T>::HASH_CMP_TRUE != exist
                    && i < arr_ptr->count(); ++i) {
      if (OB_FAIL(row.compare_with_null((*arr_ptr)[i],
                                         meta_.cmp_funcs_,
                                         meta_.row_dimension_,
                                         exist))) {
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
int ObExprInHashMap<T>::exist_refactored(const Row<T> &row, int &exist_ret)
{
  int ret = OB_SUCCESS;
  RowKey<T> tmp_row_key;
  tmp_row_key.row_= row;
  tmp_row_key.meta_ = &meta_;
  const ObArray<Row<T>> *arr_ptr = map_.get(tmp_row_key);
  if (OB_ISNULL(arr_ptr)) {
    exist_ret = ObExprInHashMap<T>::HASH_CMP_FALSE;  //在hash表中不存在
  } else {
    int exist = ObExprInHashMap<T>::HASH_CMP_FALSE;
    for (int i=0; 0 != exist_ret && i < arr_ptr->count(); ++i) {
      if (OB_FAIL(row.compare_with_null((*arr_ptr)[i],
                                         meta_.cmp_funcs_,
                                         meta_.row_dimension_,
                                         exist))) {
        LOG_WARN("compare with null failed", K(ret));
      } else if (ObExprInHashMap<T>::HASH_CMP_UNKNOWN == exist
                 || ObExprInHashMap<T>::HASH_CMP_TRUE == exist) {
        exist_ret = exist;
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

template <class T>
int ObExprInHashSet<T>::set_refactored(const Row<T> &row)
{
  RowKey<T> tmp_row_key;
  tmp_row_key.row_= row;
  tmp_row_key.meta_ = &meta_;
  return set_.set_refactored(tmp_row_key);
}

template <class T>
int ObExprInHashSet<T>::exist_refactored(const Row<T> &row, bool &is_exist)
{
  RowKey<T> tmp_row_key;
  tmp_row_key.row_= row;
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

int ObExprInOrNotIn::ObExprInCtx::init_hashset_vecs(int64_t param_num,
                                                    int64_t row_dimension,
                                                    ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  hashset_vecs_ = NULL;
  int vecs_buf_size = sizeof(ObExprInHashMap<ObObj> ) * (1 << row_dimension);
  if (OB_ISNULL(hashset_vecs_ =
            (ObExprInHashMap<ObObj>  *)
              ((exec_ctx->get_allocator()).alloc(vecs_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    for (int64_t i = 0; i < (1 << row_dimension); ++i) {
      new (&hashset_vecs_[i]) ObExprInHashMap<ObObj> ();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < (1 << row_dimension); ++i) {
      hashset_vecs_[i].set_meta_idx(i);
      hashset_vecs_[i].set_meta_dimension(row_dimension);
      if (OB_FAIL(hashset_vecs_[i].create(param_num))){
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

 int ObExprInOrNotIn::ObExprInCtx::init_static_engine_hashset_vecs(int64_t param_num,
                                                                   int64_t row_dimension,
                                                                   ObExecContext *exec_ctx)
 {
   int ret = OB_SUCCESS;
   static_engine_hashset_vecs_ = NULL;
   int64_t vecs_buf_size = sizeof(ObExprInHashMap<ObDatum> ) * (1 << row_dimension);
   if (OB_ISNULL(static_engine_hashset_vecs_ =
              (ObExprInHashMap<ObDatum>  *)
               ((exec_ctx->get_allocator()).alloc(vecs_buf_size)))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("failed to allocate memory", K(ret));
   } else {
    for (int64_t i = 0; i < (1 << row_dimension); ++i) {
      new (&static_engine_hashset_vecs_[i]) ObExprInHashMap<ObDatum> ();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < (1 << row_dimension); ++i) {
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

int ObExprInOrNotIn::ObExprInCtx::add_to_hashset(const ObObj &obj)
{
  int ret = hashset_.set_refactored(obj);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add to hashset", K(ret), K(obj));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::add_to_hashset_vecs(const Row<ObObj> &row,
                                                      const int idx)
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

int ObExprInOrNotIn::ObExprInCtx::add_to_static_engine_hashset(const Row<common::ObDatum> &row)
{
  int ret = static_engine_hashset_.set_refactored(row);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add to hashset", K(ret));
  }
  return ret;
}


int ObExprInOrNotIn::ObExprInCtx::
add_to_static_engine_hashset_vecs(const Row<common::ObDatum> &row, const int idx)
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

int ObExprInOrNotIn::ObExprInCtx::exist_in_hashset(const ObObj &obj, bool &is_exist) const
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

int ObExprInOrNotIn::ObExprInCtx::exist_in_hashset_vecs(const Row<ObObj> &row,
                                                         const int idx,
                                                         int &exist_ret) const
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(hashset_vecs_[idx].exist_refactored(row, exist_ret))){
    LOG_WARN("failed to find in hash map", K(ret));
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::exist_in_static_engine_hashset(const Row<ObDatum> &row,
                                                                 bool &is_exist)
{
  return static_engine_hashset_.exist_refactored(row, is_exist);
}


int ObExprInOrNotIn::ObExprInCtx::
exist_in_static_engine_hashset_vecs(const Row<ObDatum> &row,
                                     const int idx,
                                     int &exist_ret)
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(static_engine_hashset_vecs_[idx].exist_refactored(row, exist_ret))){
    LOG_WARN("failed to find in hash map", K(ret));
  }
  return ret;
}


int ObExprInOrNotIn::ObExprInCtx::set_cmp_types(const ObExprCalcType &cmp_type,
                                                const int64_t row_dimension)
{
  int ret = OB_SUCCESS;
  if (cmp_types_.count() < row_dimension) {
    ret = cmp_types_.push_back(cmp_type);
  } else {
    //do nothing
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::init_cmp_types(const int64_t row_dimension,
                                                 ObExecContext *exec_ctx)
{
  cmp_types_.set_allocator(&(exec_ctx->get_allocator()));
  return cmp_types_.init(row_dimension);
}


const ObExprCalcType &ObExprInOrNotIn::ObExprInCtx::get_cmp_types(const int64_t idx) const
{
  return cmp_types_[idx];
}


int ObExprInOrNotIn::ObExprInCtx::
init_hashset_vecs_all_null(const int64_t row_dimension, ObExecContext *exec_ctx)
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

int ObExprInOrNotIn::ObExprInCtx::
get_hashset_vecs_all_null(const int64_t idx,  bool &is_all_null) const
{
  int ret = OB_SUCCESS;
  if (idx >= (1 << row_dimension_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_all_null = hashset_vecs_all_null_[idx];
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::
init_right_objs(int64_t param_num,
                int64_t row_dimension,
                ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  right_objs_ = NULL;
  int objs_buf_size = sizeof(ObObj *) * param_num; //ObObj *指针数组大小
  if (OB_ISNULL(right_objs_ =
              (ObObj **)
               ((exec_ctx->get_allocator()).alloc(objs_buf_size)))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("failed to allocate memory for ObObj **", K(ret));
  } else {
    for (int i =0; OB_SUCC(ret) && i < param_num; ++i) {//初始化每个ObObj *
       if (OB_ISNULL(right_objs_[i] =
           static_cast<ObObj *> (((exec_ctx->get_allocator()).alloc(sizeof(ObObj) * row_dimension)))) ){
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for ObObj *", K(ret), K(i));
       }
     }
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::
init_right_datums(int64_t param_num,
                int64_t row_dimension,
                ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  right_datums_ = NULL;
  int64_t datums_buf_size = sizeof(ObDatum *) * param_num; //ObDatum *指针数组大小
  if (OB_ISNULL(right_datums_ =
              (ObDatum **)
               ((exec_ctx->get_allocator()).alloc(datums_buf_size)))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("failed to allocate memory for ObDatum **", K(ret));
  } else {
    for (int i =0; OB_SUCC(ret) && i < param_num; ++i) {//初始化每个ObDatum *
       if (OB_ISNULL(right_datums_[i] =
             static_cast<ObDatum *> (((exec_ctx->get_allocator()).alloc(sizeof(ObDatum) * row_dimension))))) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("failed to allocate memory for ObDatum *", K(ret), K(i));
       }
     }
  }
  return ret;
}

int ObExprInOrNotIn::ObExprInCtx::set_right_obj(int64_t row_num,
                                                int64_t col_num,
                                                const int right_param_num,
                                                const common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(right_objs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("right_datums is not init", K(ret));
  } else if (row_num < 0 || row_num >= right_param_num
             || col_num < 0 || col_num >= row_dimension_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_num or col_num out of bounds", K(ret));
  } else {
    right_objs_[row_num][col_num] = obj;
  }
  return ret;

}
int ObExprInOrNotIn::ObExprInCtx::set_right_datum(int64_t row_num,
                                                  int64_t col_num,
                                                  const int right_param_num,
                                                  const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(right_datums_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("right_datums is not init", K(ret));
  } else if (row_num < 0 || row_num >= right_param_num
             || col_num < 0 || col_num >= row_dimension_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_num or col_num out of bounds", K(ret));
  } else {
    right_datums_[row_num][col_num] = datum;
  }
  return ret;
}

ObExprInOrNotIn::ObExprInOrNotIn(ObIAllocator &alloc,
                                 ObExprOperatorType type,
                                 const char *name)
  : ObVectorExprOperator(alloc, type, name, 2, 1),
    param_flags_(0)
{
  param_lazy_eval_ = true;
  need_charset_convert_ = false;
}

int ObExprInOrNotIn::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = ObVectorExprOperator::calc_result_typeN(type, types, param_num, type_ctx);
  if (OB_SUCC(ret)) {
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  }
  return ret;
}

/* 比较规则：
 * 1、去除null之后相等，但是c1或c2包含null，结果是null
 * 2、c2未初始化，结果是null
 * 3、c1未初始化，结果是null
 * 4、c1 in (c2,c3,c4), 如果其中某个结果是null，则整体结果是null
 * 5、除上面情况，不相等，false
 * 6、其它情况，true
 * 7、nt1 in nt1, true, 但是，两个未初始化集合比较，结果为null，空集和空集比较，true
*/
int ObExprInOrNotIn::eval_pl_udt_in(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  CollectionPredRes res = CollectionPredRes::COLL_PRED_INVALID;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  pl::ObPLCollection *coll = NULL;
  OZ(expr.args_[0]->eval(ctx, left));
  if (OB_SUCC(ret)) {
    coll = reinterpret_cast<pl::ObPLCollection *>(left->get_ext());
    if (OB_NOT_NULL(coll) && coll->is_contain_null_val()) {
      set_datum_result(T_OP_IN == expr.type_, false, true, expr_datum);
    } else {
      const ObExpr *row = expr.args_[1];
      for (int64_t i = 0; OB_SUCC(ret) && i < row->arg_cnt_; ++i) {
        OZ(row->args_[i]->eval(ctx, right));
        CollectionPredRes eq_cmp_res = COLL_PRED_INVALID;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObRelationalExprOperator::pl_udt_compare2(
                      eq_cmp_res, *left->extend_obj_, *right->extend_obj_,
                      ctx.exec_ctx_, CO_EQ))) {
            LOG_WARN("failed to compare to nest table", K(ret));
          } else {
            res = static_cast<CollectionPredRes>(eq_cmp_res);
            if (COLL_PRED_TRUE == res) {
              break;
            } else if (COLL_PRED_NULL == res) {
              break;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (COLL_PRED_NULL == res) {
          set_datum_result(T_OP_IN == expr.type_, false, true, expr_datum);
        } else if (COLL_PRED_TRUE == res) {
          set_datum_result(T_OP_IN == expr.type_, true, false, expr_datum);
        } else if (COLL_PRED_FALSE == res) {
          set_datum_result(T_OP_IN == expr.type_, false, false, expr_datum);
        } else {
          set_datum_result(T_OP_IN == expr.type_, false, false, expr_datum);
        }
      }
    }
  }
  return ret;
}

inline int ObExprInOrNotIn::to_type(const ObObjType expect_type,
                                    const ObCollationType expect_cs_type,
                                    ObCastCtx &cast_ctx,
                                    const ObObj &in_obj,
                                    ObObj &out_obj) const
{
  int ret = OB_SUCCESS;
  if (ob_is_string_type(expect_type) && in_obj.is_string_type()
      && in_obj.get_collation_type() == expect_cs_type) {
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

inline bool ObExprInOrNotIn::need_hash(ObExecContext *exec_ctx) const
{
  return is_param_all_const() && is_param_all_same_type() && is_param_all_same_cs_type()
         && NULL != exec_ctx && exec_ctx->is_expr_op_ctx_inited();
}

OB_SERIALIZE_MEMBER(ObExprInOrNotIn,
                    row_dimension_,
                    real_param_num_,
                    result_type_,
                    input_types_,
                    id_,
                    param_flags_);

ObExprIn::ObExprIn(ObIAllocator &alloc)
  : ObExprInOrNotIn(alloc, T_OP_IN, N_IN)
{}

void ObExprIn::set_result(ObObj &result, bool is_exist, bool param_exist_null) const
{
  if (!is_exist && param_exist_null) {
    result.set_null();
  } else {
    result.set_int(static_cast<int64_t>(is_exist));
  }
}

ObExprNotIn::ObExprNotIn(ObIAllocator &alloc)
  : ObExprInOrNotIn(alloc, T_OP_NOT_IN, N_NOT_IN)
{}

void ObExprNotIn::set_result(ObObj &result, bool is_exist, bool param_exist_null) const
{
  if (!is_exist && param_exist_null) {
    result.set_null();
  } else {
    result.set_int(static_cast<int64_t>(!is_exist));
  }
}

int ObExprInOrNotIn::cg_expr(ObExprCGCtx &expr_cg_ctx,
                             const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != raw_expr.get_param_count()) ||
      OB_ISNULL(expr_cg_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for in expr", K(ret));
  } else if (OB_ISNULL(raw_expr.get_param_expr(0)) ||
             OB_ISNULL(raw_expr.get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null param expr", K(ret));
  } else if (lib::is_oracle_mode() && is_param_is_ext_type_oracle()) {
    rt_expr.eval_func_ = &eval_pl_udt_in;
  } else if (T_REF_QUERY == raw_expr.get_param_expr(1)->get_expr_type()) {
    //xx in (subquery) has been transformed to xx =ANY()
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid right expr type", K(ret));
  } else if (T_REF_QUERY == raw_expr.get_param_expr(0)->get_expr_type()
             //output column == 1 由subplan filter负责迭代数据
             && raw_expr.get_param_expr(0)->get_output_column() > 1) {
    ret = cg_expr_with_subquery(*expr_cg_ctx.allocator_, raw_expr, rt_expr);
  } else if (T_OP_ROW == raw_expr.get_param_expr(0)->get_expr_type()) {
    ret = cg_expr_with_row(*expr_cg_ctx.allocator_, raw_expr, rt_expr);
  } else {
    ret = cg_expr_without_row(*expr_cg_ctx.allocator_, raw_expr, rt_expr);
  }
  return ret;
}

int ObExprInOrNotIn::cg_expr_without_row(ObIAllocator &allocator,
                                         const ObRawExpr &raw_expr,
                                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_) ||
      OB_ISNULL(rt_expr.args_) ||
      OB_ISNULL(rt_expr.args_[0]) ||
      OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    rt_expr.inner_func_cnt_ = rt_expr.args_[1]->arg_cnt_;
    void **func_buf = NULL;
    int64_t func_buf_size = sizeof(void *) * rt_expr.inner_func_cnt_;
    if (OB_ISNULL(func_buf = (void **)allocator.alloc(func_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      ObObjType left_type = rt_expr.args_[0]->datum_meta_.type_;
      ObCollationType left_cs = rt_expr.args_[0]->datum_meta_.cs_type_;
      ObObjType right_type = rt_expr.args_[1]->args_[0]->datum_meta_.type_;
      const bool has_lob_header = rt_expr.args_[0]->obj_meta_.has_lob_header() ||
                                  rt_expr.args_[1]->args_[0]->obj_meta_.has_lob_header();
      ObScale scale1 = rt_expr.args_[0]->datum_meta_.scale_;
      ObScale scale2 = rt_expr.args_[1]->datum_meta_.scale_;
      rt_expr.inner_functions_ = func_buf;
      DatumCmpFunc func_ptr = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
          left_type, right_type, scale1, scale2, lib::is_oracle_mode(), left_cs, has_lob_header);
      for (int i = 0; i < rt_expr.inner_func_cnt_; i++) {
        rt_expr.inner_functions_[i] = (void *)func_ptr;
      }
      // expr in/not will use same hash func for both left/right param
      // string and text type cannot share hash func now
      bool is_string_text_cmp = (ob_is_string_tc(left_type) && ob_is_text_tc(right_type)) ||
                                (ob_is_text_tc(left_type) && ob_is_string_tc(right_type));
      if (!is_param_all_const() || rt_expr.inner_func_cnt_ <= 2 || is_string_text_cmp ||
          (ob_is_json(left_type) || ob_is_json(right_type)) ||
          (ob_is_urowid(left_type) || ob_is_urowid(right_type))) {
        rt_expr.eval_func_ = &ObExprInOrNotIn::eval_in_without_row_fallback;
      } else {
        rt_expr.eval_func_ = &ObExprInOrNotIn::eval_in_without_row;
      }
      //now only support c1 in (1,2,3,4...) to be vectorized
      if (is_param_can_vectorized()) {
        //目前认为右边参数 <= 2时， nest_loop算法的效果一定比hash更好
        if (rt_expr.inner_func_cnt_ <= 2 || is_string_text_cmp ||
            ob_is_urowid(left_type) || ob_is_urowid(right_type)) {
          rt_expr.eval_batch_func_ = &ObExprInOrNotIn::eval_batch_in_without_row_fallback;
        } else {
          rt_expr.eval_batch_func_ = &ObExprInOrNotIn::eval_batch_in_without_row;
        }
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::cg_expr_with_row(ObIAllocator &allocator,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_) ||
      OB_ISNULL(rt_expr.args_) ||
      OB_ISNULL(rt_expr.args_[0]) ||
      OB_ISNULL(rt_expr.args_[1]) ||
      OB_ISNULL(rt_expr.args_[1]->args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSEArray<ObObjType, 8> left_types;
    ObSEArray<ObCollationType, 8> left_cs_arr;
    ObSEArray<ObObjType, 8> right_types;
    ObSEArray<bool, 8> has_lob_headers;
    ObSEArray<ObScale, 8> left_scales;
    ObSEArray<ObScale, 8> right_scales;

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
      } else if (OB_FAIL(left_cs_arr.push_back(
                           LEFT_ROW_ELE(i)->datum_meta_.cs_type_))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(has_lob_headers.push_back(
                         LEFT_ROW_ELE(i)->obj_meta_.has_lob_header()))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(left_scales.push_back(LEFT_ROW_ELE(i)->datum_meta_.scale_))) {
        LOG_WARN("failed to push back element", K(ret));
      } else { /* do nothing */ }
    } // end for

    for (int i = 0; OB_SUCC(ret) && i < RIGHT_ROW(0)->arg_cnt_; i++) {
      if (OB_FAIL(right_types.push_back(RIGHT_ROW_ELE(0, i)->datum_meta_.type_))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(right_scales.push_back(RIGHT_ROW_ELE(0, i)->datum_meta_.scale_))) {
        LOG_WARN("failed to push back element", K(ret));
      } else {
        has_lob_headers.at(i) = has_lob_headers.at(i) || (RIGHT_ROW_ELE(0, i)->obj_meta_.has_lob_header());
      }
    }
    if (OB_SUCC(ret)) {
      void **func_buf = NULL;
      int func_buf_size = sizeof(void *) * LEFT_ROW->arg_cnt_ ; //这里初始化row_dimension
      rt_expr.inner_func_cnt_ = LEFT_ROW->arg_cnt_;
      if (OB_ISNULL(func_buf = (void **)allocator.alloc(func_buf_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        // expr in/not will use same hash func for both left/right param
        // string and text type cannot share hash func now
        bool is_string_text_cmp = false;
        for (int i = 0; i < left_types.count(); i++) {
          DatumCmpFunc func_ptr = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
              left_types.at(i), right_types.at(i), left_scales.at(i), right_scales.at(i),
              lib::is_oracle_mode(), left_cs_arr.at(i), has_lob_headers.at(i));
          func_buf[i] = (void *)func_ptr;
          is_string_text_cmp |= (ob_is_string_tc(left_types.at(i)) && ob_is_text_tc(right_types.at(i))) ||
                                (ob_is_text_tc(left_types.at(i)) && ob_is_string_tc(right_types.at(i)));
        }  // end for
        if (!is_param_all_const() || is_string_text_cmp) {
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

int ObExprInOrNotIn::cg_expr_with_subquery(common::ObIAllocator &allocator,
                                           const ObRawExpr &raw_expr,
                                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_) ||
      OB_ISNULL(rt_expr.args_) ||
      OB_ISNULL(rt_expr.args_[0]) ||
      OB_ISNULL(rt_expr.args_[1]) ||
      OB_ISNULL(rt_expr.args_[1]->args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    #define RIGHT_ROW(i) rt_expr.args_[1]->args_[i]
    #define RIGHT_ROW_ELE(i, j) rt_expr.args_[1]->args_[i]->args_[j]
    ObSEArray<ObObjMeta, 1> left_types;
    ObSEArray<ObObjMeta, 1> right_types;
    void **funcs = NULL;

    CK(2 == raw_expr.get_param_count());
    CK(NULL != raw_expr.get_param_expr(0));
    CK(NULL != raw_expr.get_param_expr(1));

    OZ(get_param_types(*raw_expr.get_param_expr(0), true, left_types));
    //OZ(get_param_types(*raw_expr.get_param_expr(1), false, right_types));

    if (OB_FAIL(ret)) {
    } else if (left_types.empty() || left_types.count() != RIGHT_ROW(0)->arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operand cnt mismatch",
              K(ret), K(left_types.count()), K(RIGHT_ROW(0)->arg_cnt_));
    } else if (OB_ISNULL(funcs = (void **)allocator.alloc(
                sizeof(void *) * left_types.count()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      rt_expr.inner_func_cnt_ = left_types.count();
      rt_expr.inner_functions_ = funcs;

      for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.inner_func_cnt_; i++) {
        auto &l = left_types.at(i);
        auto &r = RIGHT_ROW_ELE(0, i)->obj_meta_;
        bool has_lob_header = l.has_lob_header() || r.has_lob_header();
        if (ObDatumFuncs::is_string_type(l.get_type())
            && ObDatumFuncs::is_string_type(r.get_type())) {
          CK(l.get_collation_type() == r.get_collation_type());
        }
        if (OB_SUCC(ret)) {
          funcs[i] = (void *)ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
              l.get_type(), r.get_type(), l.get_scale(), r.get_scale(),
              lib::is_oracle_mode(), l.get_collation_type(), has_lob_header);
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

int ObExprInOrNotIn::eval_in_with_row_fallback(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &expr_datum)
{
  return calc_for_row_static_engine(expr, ctx, expr_datum, nullptr);
}

int ObExprInOrNotIn::eval_in_without_row_fallback(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  // TODO [zongmei.zzm] 原先的In或者NotIn实现，如果没有向量，并且满足need_hash的条件
  // 会先计算出所有的右孩子的节点值并构建hash表，现在只实现了短路逻辑比较
  ObDatum *left = NULL;
  ObDatum *right = NULL;
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
      }
      else if (OB_FAIL(expr.args_[1]->args_[i]->eval(ctx, right))) {
        LOG_WARN("failed to eval right datum", K(ret));
      } else if (right->is_null()) {
        cnt_null = true;
      } else {
        if (OB_FAIL(((DatumCmpFunc)expr.inner_functions_[0])(*left, *right, cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (0 == cmp_ret) {
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

int ObExprInOrNotIn::eval_batch_in_without_row_fallback(const ObExpr &expr,
                                                        ObEvalCtx &ctx,
                                                        const ObBitVector &skip,
                                                        const int64_t batch_size)
{
  LOG_DEBUG("eval_batch_in start: batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObDatum* input_left;
    if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch param values", K(ret));
    } else {
      input_left = expr.args_[0]->locate_batch_datums(ctx);
      ObDatum *right = nullptr;
      ObDatum *left = nullptr;
      ObDatum *right_store[expr.inner_func_cnt_]; //store all right param ptrs
      bool cnt_null = false; //right param has null
      /*
      * CAN_CMP_MEM used for common short path 
      * the params of left and right 
      * both are string type
      * both are CS_TYPE_UTF8MB4_BIN
      * both dont have null value
      * both dont have tailing space
      * right params count is 2(> 2 will turn to hash calc)
      */
      bool can_cmp_mem = expr.args_[0]->obj_meta_.is_string_type() 
                         && CS_TYPE_UTF8MB4_BIN == expr.args_[0]->obj_meta_.get_collation_type();
      //eval all right params
      for (int64_t j = 0; OB_SUCC(ret) && j < expr.inner_func_cnt_; ++j) {
        if (OB_FAIL(expr.args_[1]->args_[j]->eval(ctx, right_store[j]))) {
          LOG_WARN("failed to eval right datum", K(ret), K(j));
        } else {
          check_right_can_cmp_mem(*right_store[j], expr.args_[1]->args_[j]->obj_meta_, 
                                  can_cmp_mem, cnt_null);
        }
      }
      if (OB_SUCC(ret)) {
        static const char SPACE = ' ';
        check_left_can_cmp_mem(expr, input_left, skip, eval_flags, batch_size, can_cmp_mem);
        if (can_cmp_mem) {
          const char *ptr0 = right_store[0]->ptr_;
          const char *ptr1 = right_store[1]->ptr_;
          uint32_t len0 = right_store[0]->len_;
          uint32_t len1 = right_store[1]->len_;
          for (int64_t i = 0; i < batch_size; ++i) {
            if (input_left[i].is_null()) {
              results[i].set_null();
            } else if (input_left[i].len_ > 0 && SPACE == input_left[i].ptr_[input_left[i].len_ - 1]) {
              can_cmp_mem = false;
              break;
            } else {
              bool is_equal = false;
              left = &input_left[i];
              is_equal = (left->len_ >= len0
                          && 0 == MEMCMP(ptr0, left->ptr_, len0)
                          && is_all_space(left->ptr_ + len0, left->len_ - len0));
              is_equal = is_equal || (left->len_ >= len1
                                      && 0 == MEMCMP(ptr1, left->ptr_, len1)
                                      && is_all_space(left->ptr_ + len1, left->len_ - len1));
              results[i].set_int(is_equal);
            }
          }
          if (can_cmp_mem) {
            eval_flags.set_all(batch_size);
          }
        }
        if (!can_cmp_mem) {
          for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
            if (skip.at(i) || eval_flags.at(i)) {
              continue;
            }
            bool is_equal = false;
            int cmp_ret = 0;
            left = &input_left[i];
            for (int64_t j = 0; OB_SUCC(ret) && j < expr.inner_func_cnt_; ++j) {
              right = right_store[j];
              if (!left->is_null() && !right->is_null()) {
                if (OB_FAIL(((DatumCmpFunc)expr.inner_functions_[0])(*left, *right, cmp_ret))) {
                  LOG_WARN("failed to compare", K(ret));
                } else {
                  is_equal |= !(cmp_ret);
                }
              }
            }
            if (OB_SUCC(ret)) {
              set_datum_result(T_OP_IN == expr.type_,
                              is_equal, cnt_null | left->is_null(), results[i]);
              eval_flags.set(i);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprInOrNotIn::eval_in_with_row(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  ObExprInCtx *in_ctx = NULL;
  ObExecContext *exec_ctx = &ctx.exec_ctx_;
  uint64_t in_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  bool is_completely_cmp = false;//完全匹配，in返回true，not in返回false
  bool is_null_cmp = false;//第二轮null值匹配，匹配上至少返回null
  bool left_has_null = false;//左边是否存在null
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
  if (!fallback &&
      OB_SUCC(ret) &&
      NULL == (in_ctx = static_cast<ObExprInCtx *> (exec_ctx->get_expr_op_ctx(in_id)))) {
    if (OB_FAIL(exec_ctx->create_expr_op_ctx(in_id, in_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret));
    } else if (OB_FAIL(in_ctx->init_static_engine_hashset_vecs(right_param_num,
                                                               row_dimension,
                                                               exec_ctx))) { //hashset集合
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
          //遍历整个向量，记录null元素的位置； 将剩余非null值进行全排列，与null元素按位异或后插入对应位置
          for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension; ++j) {
            if (OB_ISNULL(RIGHT_ROW_ELE(i, j))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid null arg", K(ret), K(RIGHT_ROW_ELE(i, j)),K(i), K(j));
            } else if (OB_FAIL(RIGHT_ROW_ELE(i, j)->eval(ctx, right))) {
              LOG_DEBUG("param evaluate fail, hash set lookup disabled for in expr", K(ret), K(i));
              in_ctx->disable_hash_calc();
            } else if (!in_ctx->is_hash_calc_disabled()) {//遍历确定null_idx
            //探测null元素的位置并记录
              if (right->is_null()) {
                null_idx = null_idx ^ (1 << j);
                in_ctx->right_has_null = true;
                if (null_idx == ((1 << row_dimension) - 1)) {
                  in_ctx->ctx_hash_null_ = true;
                  is_right_all_null = true;
                }
              } else {
                //do nothing
              }
              if (OB_FAIL(in_ctx->set_right_datum(i, j, right_param_num, *right))) {
                LOG_WARN("failed to load right", K(ret), K(i), K(j));
              } else {
                if (OB_ISNULL(in_ctx->hash_func_buff_)) {
                  int func_buf_size = sizeof(void *) * row_dimension;
                  if (OB_ISNULL(in_ctx->hash_func_buff_ =
                     (void **)(exec_ctx->get_allocator()).alloc(func_buf_size))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("failed to allocate memory", K(ret));
                  }
                }
                //设置ObDatum的hash函数
                if (OB_SUCC(ret)) {
                  in_ctx->hash_func_buff_[j] =
                           (void *)(RIGHT_ROW_ELE(i, j)->basic_funcs_->murmur_hash_v2_);
                }
              }
            } else {
                //do nothing
            }
          }
          //这里对所有hash表进行函数指针的设定
          if (OB_SUCC(ret) && !in_ctx->funcs_ptr_set) {
            for (int i = 0; i < (1 << row_dimension); ++i) {
              in_ctx->set_hash_funcs_ptr(i, in_ctx->hash_func_buff_);
              in_ctx->set_cmp_funcs_ptr(i, expr.inner_functions_);
            }
            in_ctx->funcs_ptr_set = true;
          }

          /*
          *从 1~2^col迭代，选取的全为非null值的时候，，记录这个idx，
          *将其设置进row中,这个idx用于hash值的计算，以及进入hashset的下标，和operator == 不用于compare_with_null
          *对设置好idx的row，进入对应的hashtable，
          *此时operator == 要求key值完全匹配
          */
          Row<ObDatum> tmp_row;
          for (int64_t k = 1; OB_SUCC(ret) && k < (1 << row_dimension); ++k) {
            int hash_idx = k;
            if (0 == (k & null_idx)) {//k代表选取的列，这些列不能包含null
              if (OB_FAIL(tmp_row.set_elem(in_ctx->get_datum_row(i)))) {
                LOG_WARN("failed to set elem", K(ret));
              }
              //此次排列进入hash表对应位置
              if (OB_SUCC(ret)) {
                if (OB_FAIL(in_ctx->add_to_static_engine_hashset_vecs(tmp_row, hash_idx))) {
                  LOG_WARN("failed to add hashset", K(ret));
                } else {
                  //do nothing
                }
              }
            } else if (null_idx == (k | null_idx)) {//k选取的列为null的子集，将这里的全null置为true
              if (OB_FAIL(in_ctx->set_hashset_vecs_all_null_true(k))) {
                LOG_WARN("failed to set hashset vecs all null true", K(ret));
              }
            } else {
              //此次排列不入hash表
            }
          }
        }
      }
    }
  }
  //second we search in hashset
  if (!fallback && OB_SUCC(ret) && OB_NOT_NULL(in_ctx)) {
    if (OB_UNLIKELY(in_ctx->is_hash_calc_disabled())) {
      //fall_back = true;//TODO : lack param fallback
    } else if (!fallback) {
      //遍历提取左向量
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
          //探测null元素的位置并记录
          if (left->is_null()) {
            null_idx = null_idx ^ (1 << j);
            left_has_null = true;
            if (null_idx == ((1 << row_dimension) - 1)) {
              is_left_all_null = true;
            }
          } else {
            //do nothing
          }
          datum_ptr[j] = *left;
        }
      }
      if (OB_SUCC(ret)) {
        tmp_row.set_elem(datum_ptr);
        //首先检查左边是否有null，有null则检查反面的hashset是否为全null
        if (null_idx != 0 &&
            OB_FAIL(in_ctx->get_hashset_vecs_all_null((1 <<row_dimension) - 1 - null_idx/*取反*/,
                                                      is_null_cmp))) {
          LOG_WARN("failed to get hashset vecs all null", K(ret));
        }

        //左表取出所有非null进行排列组合，按照赋予的hashkey查询hash值是否存在，
        //如果存在，则取出这个桶进行遍历，按照方法cmp_with_null得出最后的结论，true直接结束
        int exist_ret = ObExprInHashMap<ObDatum>::HASH_CMP_FALSE;
        for (int64_t k = (1 << row_dimension) - 1;
             !is_null_cmp && !is_completely_cmp && OB_SUCC(ret) && k >= 1;
             k = static_cast<int64_t>(last(k, (1 << row_dimension) -1))) { //k 代表选取的列，即idx
          if (0 == (k & null_idx)) {//k不包含null列
           if (OB_FAIL(in_ctx->exist_in_static_engine_hashset_vecs(tmp_row, k, exist_ret))) {
              LOG_WARN("failed to search in hashset", K(ret));
            } else {
              if (ObExprInHashMap<ObDatum>::HASH_CMP_TRUE == exist_ret) {
                is_completely_cmp = true;
              } else if (ObExprInHashMap<ObDatum>::HASH_CMP_UNKNOWN == exist_ret) {
                is_null_cmp = true;
              } else {
                //do nothing
              }
            }
            if (!left_has_null && !in_ctx->right_has_null) {//左右均没有null值，第一次探测完成后直接退出
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
      set_datum_result(T_OP_IN == expr.type_, is_completely_cmp, false,
                     expr_datum);
    }
  }
  if (fallback) {
    ret = eval_in_with_row_fallback(expr, ctx, expr_datum);
  }
  return ret;
}


int ObExprInOrNotIn::eval_in_without_row(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObExprInCtx *in_ctx = NULL;
  ObDatum *left = NULL;
  bool cnt_null = false;
  bool is_exist = false;
  ObExecContext *exec_ctx = &ctx.exec_ctx_;

  uint64_t in_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  bool fallback = false;

  if (!fallback && OB_SUCC(ret)) {
    if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
      LOG_WARN("failed to eval left", K(ret));
    } else if (left->is_null()) {
      is_exist = false;
      cnt_null = true;
    } else {
      int64_t right_param_num = expr.inner_func_cnt_;
      //first build hash table for right params
      if (OB_FAIL(build_right_hash_without_row(in_id, right_param_num,
                                               expr, ctx, exec_ctx, in_ctx, cnt_null))) {
        LOG_WARN("failed to build hash table for right params", K(ret));
      }
      //second we search in hashset.
      if (OB_SUCC(ret) && OB_NOT_NULL(in_ctx)) {
        if (OB_UNLIKELY(in_ctx->is_hash_calc_disabled())) {
          //do nothing
        } else if (!left->is_null()){
          Row<ObDatum> tmp_row;
          ObDatum *datum_ptr = left;

          if (OB_FAIL(ret)) {

          } else if (OB_FAIL(tmp_row.set_elem(datum_ptr))) {
            LOG_WARN("failed to load left", K(ret));
          } else if (0 != in_ctx->get_static_engine_hashset_size()
                     && OB_FAIL(in_ctx->exist_in_static_engine_hashset(tmp_row, is_exist))) {
            LOG_WARN("failed to search in hashset", K(ret));
          } else {
            //do nothing
          }
        } else {
          //do nothing
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(in_ctx) && !in_ctx->is_hash_calc_disabled()) {
      if (OB_NOT_NULL(in_ctx) && in_ctx->ctx_hash_null_) {
        cnt_null = true;
      }
      if (!is_exist && cnt_null) {
        expr_datum.set_null();
      } else {
        set_datum_result(T_OP_IN == expr.type_, is_exist, false,
                      expr_datum);
      }
    } else if (OB_SUCC(ret) && OB_ISNULL(in_ctx)) {
      if (!left->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in_ctx is not init", K(ret));
      } else {
        expr_datum.set_null();
      }
    } else if (OB_SUCC(ret)) {
      ret = eval_in_without_row_fallback(expr, ctx, expr_datum);
    }
  } else if (OB_SUCC(ret)){
    ret = eval_in_without_row_fallback(expr, ctx, expr_datum);
  } else {
    //do nothing
  }
  return ret;
}

int ObExprInOrNotIn::eval_batch_in_without_row(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               const ObBitVector &skip,
                                               const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("eval_batch_in_hash start: batch mode");
  ObDatum *results = expr.locate_batch_datums(ctx);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results frame is not init", K(ret));
  } else {
    ObExprInCtx *in_ctx = NULL;
    ObExecContext *exec_ctx = &ctx.exec_ctx_;
    uint64_t in_id = expr.expr_ctx_id_;
    if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch param values", K(ret));
    } else {
      ObDatum *input_left = expr.args_[0]->locate_batch_datums(ctx);
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
      bool fallback = false; //建hash表过程中eval 失败，需要尝试nest_loop
      Row<ObDatum> tmp_row; //放置left
      ObDatum *left = nullptr;
      int64_t right_param_num = expr.inner_func_cnt_;
      bool right_has_null = false;
      if (OB_FAIL(build_right_hash_without_row(in_id, right_param_num, expr,
                                               ctx, exec_ctx, in_ctx, right_has_null))) {
         LOG_WARN("failed to build hash table for right params", K(ret));
      } else {
        fallback = in_ctx->is_hash_calc_disabled();
      }
      for (int64_t left_idx = 0; OB_SUCC(ret) && !fallback && left_idx < batch_size; ++left_idx) {
        if (skip.at(left_idx) || eval_flags.at(left_idx)) {
          continue;
        }
        bool is_exist = false;
        bool has_null = false;
        left = &input_left[left_idx];
        if (left->is_null()) {
          is_exist = false;
          has_null = true;
        } else {
          //second we search in hashset.
          if (OB_SUCC(ret) && OB_NOT_NULL(in_ctx) && !fallback) {
            if (OB_FAIL(tmp_row.set_elem(left))) {
              LOG_WARN("failed to load left", K(ret));
            } else if (0 != in_ctx->get_static_engine_hashset_size()
                      && OB_FAIL(in_ctx->exist_in_static_engine_hashset(tmp_row, is_exist))) {
              LOG_WARN("failed to search in hashset", K(ret));
            } else {
              //do nothing
            }
          }
        }
        if (OB_SUCC(ret) && !fallback) {
          has_null = has_null || (OB_NOT_NULL(in_ctx) && in_ctx->ctx_hash_null_);
          if (!is_exist && has_null) {
            results[left_idx].set_null();
          } else {
            set_datum_result(T_OP_IN == expr.type_, is_exist, false, results[left_idx]);
          }
          eval_flags.set(left_idx);
        }
      }
      if (fallback) {
        ret = eval_batch_in_without_row_fallback(expr, ctx, skip, batch_size);
      }
    }
  }

  return ret;
}


int ObExprInOrNotIn::eval_in_with_subquery(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_datum);
  ObSubQueryIterator *l_iter = NULL;
  ObExpr **l_row = NULL;
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
            l_row[i]->set_evaluated_projected(ctx);
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
        //only one row expected for left row
        ret = OB_SUBQUERY_TOO_MANY_ROW;
      }
    }
  }
  return ret;
}


int ObExprInOrNotIn::calc_for_row_static_engine(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum,
                                  ObExpr **l_row)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_datum);
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  bool set_cnt_null = false;
  bool set_cnt_equal = false;
#define RIGHT_ROW(i) expr.args_[1]->args_[i]
#define RIGHT_ROW_ELE(i, j) expr.args_[1]->args_[i]->args_[j]
  for (int i = 0; OB_SUCC(ret) && ! set_cnt_equal && i < expr.args_[1]->arg_cnt_; ++i) {
    if (OB_ISNULL(RIGHT_ROW(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null arg", K(ret), K(RIGHT_ROW(i)), K(i));
    } else {
      bool row_is_equal = true;
      bool row_cnt_null = false;
      ObExpr *left_expr = nullptr;
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
          int cmp_ret = 0;
          if (OB_FAIL(((DatumCmpFunc)expr.inner_functions_[j])(*left, *right, cmp_ret))) {
            LOG_WARN("failed to compare", K(ret));
          } else if (0 != cmp_ret) {
            //如果在向量的比较中，有明确的false，表明这个向量不成立，所以应该将has_null置为false
            row_is_equal = false;
            row_cnt_null = false;
          }
        }
      } //inner loop
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (row_is_equal && ! row_cnt_null) {
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



void ObExprInOrNotIn::set_datum_result(const bool is_expr_in,
                                       const bool is_exist,
                                       const bool param_exist_null,
                                       ObDatum &expr_datum) {
  if (!is_exist && param_exist_null) {
    expr_datum.set_null();
  } else  {
    expr_datum.set_int(!(is_expr_in ^ is_exist));
  }
}

int ObExprInOrNotIn::setup_row(ObExpr **expr,
                               ObEvalCtx &ctx,
                               const bool is_iter, const
                               int64_t cmp_func_cnt,
                               ObSubQueryIterator *&iter,
                               ObExpr **&row)
{
  int ret = OB_SUCCESS;
  if (is_iter) {
    ObDatum *v = NULL;
    if (OB_FAIL(expr[0]->eval(ctx, v))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else if (v->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL subquery ref info returned", K(ret));
    } else if (OB_FAIL(ObExprSubQueryRef::get_subquery_iter(
                ctx, ObExprSubQueryRef::Extra::get_info(v->get_int()), iter))) {
      LOG_WARN("get subquery iterator failed", K(ret));
    } else if (OB_ISNULL(iter) || cmp_func_cnt != iter->get_output().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL subquery iterator", K(ret), KP(iter), K(cmp_func_cnt));
    } else if (OB_FAIL(iter->rewind())) {
      LOG_WARN("start iterate failed", K(ret));
    } else {
      row = &const_cast<ExprFixedArray &>(iter->get_output()).at(0);
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

int ObExprInOrNotIn::get_param_types(
    const ObRawExpr &param, const bool is_iter, ObIArray<ObObjMeta> &types) const
{
  int ret = OB_SUCCESS;
  if (param.get_expr_type() == T_OP_ROW) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.get_param_count(); i++) {
      const ObRawExpr *e = param.get_param_expr(i);
      CK(NULL != e);
      OZ(types.push_back(e->get_result_meta()));
    }
  } else if (param.get_expr_type() == T_REF_QUERY && is_iter) {
    const ObQueryRefRawExpr &ref = static_cast<const ObQueryRefRawExpr &>(param);
    FOREACH_CNT_X(t, ref.get_column_types(), OB_SUCC(ret)) {
      OZ(types.push_back(*t));
    }
  } else {
    OZ(types.push_back(param.get_result_meta()));
  }
  return ret;
}

int ObExprInOrNotIn::build_right_hash_without_row(const int64_t in_id,
                                          const int64_t right_param_num,
                                          const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObExecContext *exec_ctx,
                                          ObExprInCtx *&in_ctx,
                                          bool &cnt_null)
{
  int ret = OB_SUCCESS;
  ObDatum *right = NULL;
  int64_t row_dimension = 1;
  if (OB_ISNULL(in_ctx = static_cast<ObExprInCtx *> (exec_ctx->get_expr_op_ctx(in_id)))) {
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
          ret = OB_SUCCESS;
          in_ctx->disable_hash_calc();
          LOG_DEBUG("param eval failed, try nest_loop", K(ret), K(i));
        } else if (right->is_null()) {
          cnt_null = true;
          in_ctx->ctx_hash_null_ = true;
        } else {
          if (OB_FAIL(in_ctx->set_right_datum(i, 0, right_param_num, *right))) {
            LOG_WARN("failed to load right", K(ret), K(i));
          } else {
            if (OB_ISNULL(in_ctx->hash_func_buff_)) {
              int64_t func_buf_size = sizeof(void *) * 1;
              if (OB_ISNULL(in_ctx->hash_func_buff_ = (void **)
                            (exec_ctx->get_allocator()).alloc(func_buf_size))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to allocate memory", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              in_ctx->hash_func_buff_[0] = (void *)
                              (expr.args_[1]->args_[i]->basic_funcs_->murmur_hash_v2_);
            }
          }
          Row<ObDatum> tmp_row;
          //这里所有hash函数和cmp函数已经加载完毕，设置tmp_row的函数指针
          if (OB_FAIL(ret)){
          } else if(OB_FAIL(tmp_row.set_elem(in_ctx->get_datum_row(i)))) {
            LOG_WARN("failed to load datum", K(ret), K(i));
          } else {
            in_ctx->set_hash_funcs_ptr_for_set(in_ctx->hash_func_buff_);
            in_ctx->set_cmp_funcs_ptr_for_set(expr.inner_functions_);
          }
          if (OB_SUCC(ret) && OB_FAIL(in_ctx->add_to_static_engine_hashset(tmp_row))) {
            LOG_WARN("failed to add to hashset", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

void ObExprInOrNotIn::check_right_can_cmp_mem(const ObDatum &datum, 
                                              const ObObjMeta &meta, 
                                              bool &can_cmp_mem, 
                                              bool &cnt_null)
{
  static const char SPACE = ' ';
  if (!meta.is_string_type() || CS_TYPE_UTF8MB4_BIN != meta.get_collation_type()) {
    cnt_null = cnt_null || datum.is_null();
    can_cmp_mem = false;
  } else {
    cnt_null = cnt_null || datum.is_null();
    can_cmp_mem = can_cmp_mem && !cnt_null;
    if (datum.len_ > 0 && SPACE == datum.ptr_[datum.len_ - 1]) {
      can_cmp_mem = false;
    }
  }
}
void ObExprInOrNotIn::check_left_can_cmp_mem(const ObExpr &expr, 
                                             const ObDatum *datum, 
                                             const ObBitVector &skip, 
                                             const ObBitVector &eval_flags, 
                                             const int64_t batch_size, 
                                             bool &can_cmp_mem)
{
  UNUSED(datum);
  can_cmp_mem = can_cmp_mem && T_OP_IN == expr.type_ && 2 == expr.inner_func_cnt_ 
                && ObBitVector::bit_op_zero(skip, eval_flags, batch_size, 
                               [](const uint64_t l, const uint64_t r) { return (l | r); });
}

bool ObExprInOrNotIn::is_all_space(const char *ptr, const int64_t remain_len)
{
  bool ret = true;
  int64_t len = remain_len;
  int64_t pos = 0; 
  const static char *space64 = "                                                                ";
  int64_t size = 64;
  while(len > 0 && ret) {
    int64_t min_cmp_len = min(len, size);
    ret = (0 == MEMCMP(ptr + pos, space64, min_cmp_len));
    pos += min_cmp_len;
    len -= size;
  }
  return ret;
}

}
}

