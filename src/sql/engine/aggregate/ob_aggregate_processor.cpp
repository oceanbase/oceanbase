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
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_less_than.h"
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"
#include "sql/parser/ob_item_type_str.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

namespace oceanbase {
using namespace common;
using namespace common::number;
namespace sql {

OB_DEF_SERIALIZE(ObAggrInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      expr_,
      real_aggr_type_,
      has_distinct_,
      is_implicit_first_aggr_,
      has_order_by_,
      param_exprs_,
      distinct_collations_,
      distinct_cmp_funcs_,
      group_concat_param_count_,
      sort_collations_,
      sort_cmp_funcs_,
      separator_expr_,
      linear_inter_expr_);
  return ret;
}

OB_DEF_DESERIALIZE(ObAggrInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      expr_,
      real_aggr_type_,
      has_distinct_,
      is_implicit_first_aggr_,
      has_order_by_,
      param_exprs_,
      distinct_collations_,
      distinct_cmp_funcs_,
      group_concat_param_count_,
      sort_collations_,
      sort_cmp_funcs_,
      separator_expr_,
      linear_inter_expr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAggrInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      expr_,
      real_aggr_type_,
      has_distinct_,
      is_implicit_first_aggr_,
      has_order_by_,
      param_exprs_,
      distinct_collations_,
      distinct_cmp_funcs_,
      group_concat_param_count_,
      sort_collations_,
      sort_cmp_funcs_,
      separator_expr_,
      linear_inter_expr_);
  return len;
}

ObAggrInfo::~ObAggrInfo()
{}

int64_t ObAggrInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_AGGR_FUNC,
      ob_aggr_func_str(get_expr_type()),
      K_(real_aggr_type),
      KPC_(expr),
      N_DISTINCT,
      has_distinct_,
      K_(has_order_by),
      K_(is_implicit_first_aggr),
      K_(param_exprs),
      K_(distinct_collations),
      K_(distinct_cmp_funcs),
      K_(group_concat_param_count),
      K_(sort_collations),
      K_(sort_cmp_funcs),
      KPC_(separator_expr),
      KPC_(linear_inter_expr));
  J_OBJ_END();
  return pos;
}

ObAggregateProcessor::AggrCell::~AggrCell()
{
  destroy();
}

// arena allocator alloc memory, it don't free
void ObAggregateProcessor::AggrCell::destroy()
{
  if (NULL != extra_) {
    auto*& extra = extra_;
    extra->~ExtraResult();
    alloc_->free(extra);
    extra_ = NULL;
  }
  curr_row_results_.reset();
}

int ObAggregateProcessor::AggrCell::collect_result(
    const ObObjTypeClass tc, ObEvalCtx& eval_ctx, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  ObDatum& result = aggr_info.expr_->locate_datum_for_write(eval_ctx);
  aggr_info.expr_->get_eval_info(eval_ctx).evaluated_ = true;
  // sum(count(*)) should return same type as count(*), which is bigint in mysql mode
  if (!share::is_oracle_mode() && T_FUN_COUNT_SUM == aggr_info.get_expr_type()) {
    if (ObIntTC == tc && is_tiny_num_used_) {
      result.set_int(tiny_num_int_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count sum should be int", K(ret), K(tc), K(is_tiny_num_used_));
    }
  } else if (is_tiny_num_used_ && (ObIntTC == tc || ObUIntTC == tc)) {
    ObNumStackAllocator<2> tmp_alloc;
    ObNumber result_nmb;
    const bool strict_mode = false;  // this is tmp allocator, so we can ues non-strinct mode
    ObNumber right_nmb;
    if (ObIntTC == tc) {
      if (OB_FAIL(right_nmb.from(tiny_num_int_, tmp_alloc))) {
        LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(tc));
      }
    } else {
      if (OB_FAIL(right_nmb.from(tiny_num_uint_, tmp_alloc))) {
        LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(tc));
      }
    }

    if (OB_SUCC(ret)) {
      if (iter_result_.is_null()) {
        result.set_number(right_nmb);
      } else {
        ObNumber left_nmb(iter_result_.get_number());
        if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, strict_mode))) {
          LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
        } else {
          result.set_number(result_nmb);
        }
      }
    }
  } else {
    ret = aggr_info.expr_->deep_copy_datum(eval_ctx, iter_result_);
  }
  return ret;
}

int64_t ObAggregateProcessor::AggrCell::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(row_count),
      K_(tiny_num_int),
      K_(tiny_num_uint),
      K_(is_tiny_num_used),
      K_(llc_bitmap),
      K_(iter_result),
      KPC_(extra));
  J_OBJ_END();
  return pos;
}

void ObAggregateProcessor::ExtraResult::reuse()
{
  if (NULL != unique_sort_op_) {
    unique_sort_op_->reuse();
  }
}

ObAggregateProcessor::ExtraResult::~ExtraResult()
{
  if (NULL != unique_sort_op_) {
    unique_sort_op_->~ObUniqueSortImpl();
    alloc_.free(unique_sort_op_);
    unique_sort_op_ = NULL;
  }
}

int ObAggregateProcessor::ExtraResult::init_distinct_set(
    const uint64_t tenant_id, const ObAggrInfo& aggr_info, ObEvalCtx& eval_ctx, const bool need_rewind)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_ISNULL(unique_sort_op_ = static_cast<ObUniqueSortImpl*>(alloc_.alloc(sizeof(ObUniqueSortImpl))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fall to alloc buff", "size", sizeof(ObUniqueSortImpl), K(ret));
  } else {
    new (unique_sort_op_) ObUniqueSortImpl();
    if (OB_FAIL(unique_sort_op_->init(
            tenant_id, &aggr_info.distinct_collations_, &aggr_info.distinct_cmp_funcs_, &eval_ctx, need_rewind))) {
      LOG_WARN("init distinct set failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != unique_sort_op_) {
      unique_sort_op_->~ObUniqueSortImpl();
      alloc_.free(unique_sort_op_);
      unique_sort_op_ = NULL;
    }
  }
  return ret;
}

int ObAggregateProcessor::GroupConcatExtraResult::init(const uint64_t tenant_id,
  const ObAggrInfo& aggr_info, ObEvalCtx& eval_ctx, const bool need_rewind, int64_t dir_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    row_count_ = 0;
    iter_idx_ = 0;

    if (aggr_info.has_order_by_) {
      if (OB_ISNULL(sort_op_ = static_cast<ObSortOpImpl*>(alloc_.alloc(sizeof(ObSortOpImpl))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fall to alloc buff", "size", sizeof(ObSortOpImpl), K(ret));
      } else {
        new (sort_op_) ObSortOpImpl();
        if (OB_FAIL(sort_op_->init(
                tenant_id, &aggr_info.sort_collations_, &aggr_info.sort_cmp_funcs_, &eval_ctx, false, need_rewind))) {
          LOG_WARN("init sort_op_ failed", K(ret));
        }
      }
    } else {
      int64_t sort_area_size = 0;
      if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(SORT_WORK_AREA, tenant_id, sort_area_size))) {
        LOG_WARN("failed to get workarea size", K(ret), K(tenant_id));
      } else if (OB_FAIL(row_store_.init(sort_area_size,
                     tenant_id,
                     ObCtxIds::WORK_AREA,
                     ObModIds::OB_SQL_AGGR_FUN_GROUP_CONCAT,
                     true /* enable dump */))) {
        LOG_WARN("row store failed", K(ret));
      } else {
        row_store_.set_dir_id(dir_id);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != sort_op_) {
      sort_op_->~ObSortOpImpl();
      alloc_.free(sort_op_);
      sort_op_ = NULL;
    }
  }
  return ret;
}

ObAggregateProcessor::GroupConcatExtraResult::~GroupConcatExtraResult()
{
  if (sort_op_ != NULL) {
    sort_op_->~ObSortOpImpl();
    alloc_.free(sort_op_);
    sort_op_ = NULL;
  } else {
    row_store_.reset();
  }
}

void ObAggregateProcessor::GroupConcatExtraResult::reuse_self()
{
  if (sort_op_ != NULL) {
    sort_op_->reuse();
  } else {
    row_store_iter_.reset();
    row_store_.reset();
  }
  bool_mark_.reset();
  row_count_ = 0;
  iter_idx_ = 0;
};

void ObAggregateProcessor::GroupConcatExtraResult::reuse()
{
  reuse_self();
  ExtraResult::reuse();
}

int ObAggregateProcessor::GroupConcatExtraResult::finish_add_row()
{
  iter_idx_ = 0;
  int ret = OB_SUCCESS;
  if (sort_op_ != NULL) {
    ret = sort_op_->sort();
  } else {
    row_store_iter_.reset();
    ret = row_store_iter_.init(&row_store_, ObChunkDatumStore::BLOCK_SIZE);
  }
  return ret;
}

int ObAggregateProcessor::GroupConcatExtraResult::rewind()
{
  int ret = OB_SUCCESS;
  if (sort_op_ != NULL) {
    if (OB_FAIL(sort_op_->rewind())) {
      LOG_WARN("rewind failed", K(ret));
    }
  } else {
    row_store_iter_.reset();
    if (OB_FAIL(row_store_iter_.init(&row_store_, ObChunkDatumStore::BLOCK_SIZE))) {
      LOG_WARN("row store iterator init failed", K(ret));
    }
  }
  iter_idx_ = 0;
  return ret;
}

int64_t ObAggregateProcessor::GroupConcatExtraResult::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(row_count), K_(iter_idx), K_(row_store), KP_(sort_op), KP_(unique_sort_op));
  J_OBJ_END();
  return pos;
}


int ObAggregateProcessor::GroupConcatExtraResult::get_bool_mark(int64_t col_index, bool &is_bool)
{
  INIT_SUCC(ret);
  if (col_index + 1 > bool_mark_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is overflow", K(ret), K(col_index), K(bool_mark_.count()));
  } else {
    is_bool = bool_mark_[col_index];
  }
  return ret;
}

int ObAggregateProcessor::GroupConcatExtraResult::set_bool_mark(int64_t col_index, bool is_bool)
{
  INIT_SUCC(ret);
  if (col_index > bool_mark_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is overflow", K(ret), K(col_index), K(bool_mark_.count()));
  } else if (col_index == bool_mark_.count()) {
    bool_mark_.push_back(is_bool);
  } else {
    bool_mark_[col_index] = is_bool;
  }
  return ret;
}

int64_t ObAggregateProcessor::ExtraResult::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(unique_sort_op));
  J_OBJ_END();
  return pos;
}

int ObAggrInfo::eval_aggr(ObChunkDatumStore::ShadowStoredRow& curr_row_results, ObEvalCtx& ctx) const
{
  int ret = OB_SUCCESS;
  if (param_exprs_.empty() && T_FUN_COUNT == get_expr_type()) {
    // do nothing
  } else {
    if (OB_FAIL(curr_row_results.shadow_copy(param_exprs_, ctx))) {
      LOG_WARN("shadow_copy failed", K(ret));
    }
  }
  return ret;
}

ObAggregateProcessor::ObAggregateProcessor(ObEvalCtx& eval_ctx, ObIArray<ObAggrInfo>& aggr_infos)
    : has_distinct_(false),
      has_order_by_(false),
      has_group_concat_(false),
      in_window_func_(false),
      has_extra_(false),
      eval_ctx_(eval_ctx),
      aggr_alloc_(ObModIds::OB_SQL_AGGR_FUNC_ROW, common::OB_MALLOC_MIDDLE_BLOCK_SIZE, OB_SERVER_TENANT_ID,
          ObCtxIds::WORK_AREA),
      aggr_infos_(aggr_infos),
      group_rows_(),
      concat_str_max_len_(OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE),
      cur_concat_buf_len_(0),
      concat_str_buf_(NULL),
      dir_id_(-1)
{}

int ObAggregateProcessor::init()
{
  int ret = OB_SUCCESS;
  // add aggr columns
  has_distinct_ = false;
  has_order_by_ = false;
  has_group_concat_ = false;
  set_tenant_id(eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id());

  if (OB_ISNULL(eval_ctx_.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(group_rows_.reserve(1))) {
    LOG_WARN("failed to reserve", K(ret));
  }
  int64_t result_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
    const ObAggrInfo& aggr_info = aggr_infos_.at(i);
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(aggr_info), K(i), K(ret));
    } else {
      has_distinct_ |= aggr_info.has_distinct_;
      has_group_concat_ |=
          (T_FUN_GROUP_CONCAT == aggr_info.get_expr_type() || 
           T_FUN_KEEP_WM_CONCAT == aggr_info.get_expr_type() ||
           T_FUN_WM_CONCAT == aggr_info.get_expr_type() || 
           T_FUN_JSON_ARRAYAGG == aggr_info.get_expr_type() ||
           T_FUN_JSON_OBJECTAGG == aggr_info.get_expr_type());
      has_order_by_ |= aggr_info.has_order_by_;
      if (!has_extra_) {
        has_extra_ |= aggr_info.has_distinct_;
        has_extra_ |= need_extra_info(aggr_info.get_expr_type());
      }
    }
  }  // end for

  LOG_DEBUG("succ to init ObAggregateProcessor", K(ret));

  //  if (OB_SUCC(ret)) {
  //    const int64_t bucket_size = common::hash::cal_next_prime(udf_metas.count() + 1);
  //    if (OB_FAIL(aggr_udf_.create(bucket_size,
  //                                common::ObModIds::OB_SQL_UDF,
  //                                common::ObModIds::OB_SQL_UDF))) {
  //      LOG_WARN("init map failed", K(ret));
  //    } else if (OB_FAIL(aggr_udf_metas_.assign(udf_metas))) {
  //      LOG_WARN("failed to assign udf metas", K(ret));
  //    }
  //  }
  return ret;
}

void ObAggregateProcessor::destroy()
{
  for (int64_t i = 0; i < group_rows_.count(); ++i) {
    group_rows_.at(i)->destroy();
  }
  if (concat_str_buf_ != NULL) {
    aggr_alloc_.free(concat_str_buf_);
    concat_str_buf_ = NULL;
  }
  group_rows_.reset();
  aggr_alloc_.reset();
  //  if (aggr_udf_.created()) {
  //    common::hash::ObHashMap<int64_t, ObAggUdfExeUnit, common::hash::NoPthreadDefendMode>::iterator iter =
  //    aggr_udf_.begin(); for ( ; iter != aggr_udf_.end(); iter ++) {
  //      ObAggUdfExeUnit &aggr_unit = iter->second;
  //      if (OB_NOT_NULL(aggr_unit.aggr_func_) && OB_NOT_NULL(aggr_unit.udf_ctx_)) {
  //        IGNORE_RETURN aggr_unit.aggr_func_->process_deinit_func(*aggr_unit.udf_ctx_);
  //      }
  //    }
  //  }
  //  aggr_udf_.destroy();
  //  aggr_udf_buf_.reset();
}

void ObAggregateProcessor::reuse()
{
  if (has_extra_) {
    for (int64_t i = 0; i < group_rows_.count(); ++i) {
      group_rows_.at(i)->destroy();
    }
  }
  if (concat_str_buf_ != NULL) {
    aggr_alloc_.free(concat_str_buf_);
    concat_str_buf_ = NULL;
  }
  cur_concat_buf_len_ = 0;
  group_rows_.reuse();
  aggr_alloc_.reset_remain_one_page();
  //  aggr_udf_buf_.reset_remain_one_page();
  //  if (aggr_unique_sort_op_.size() > 0) {
  //    aggr_unique_sort_op_.reuse();
  //  }
  //  aggr_udf_.reuse();
}

// we need reuse memory here
int ObAggregateProcessor::clone_cell(ObDatum& target_cell, const ObDatum& src_cell, const bool is_number /*false*/)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t curr_size = 0;
  // length + magic num + data
  int64_t need_size = sizeof(int64_t) * 2 + (is_number ? ObNumber::MAX_BYTE_LEN : src_cell.len_);
  // we can't decide reuse memory on target_cell.len_, because for null datum we 
  // also have reserved buffer
  if (OB_NOT_NULL(target_cell.ptr_)) {
    void* data_ptr = const_cast<char*>(target_cell.ptr_);
    const int64_t data_length = target_cell.len_;
    if (OB_ISNULL((char*)data_ptr - sizeof(int64_t)) ||
        OB_ISNULL((char*)data_ptr - sizeof(int64_t) - sizeof(int64_t))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("clone_cell use aggr_alloc, need has meta", KP(data_ptr), K(ret));
    } else if (OB_UNLIKELY(*((int64_t*)(data_ptr)-1) != STORED_ROW_MAGIC_NUM)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("aggr_alloc memory is mismatch, maybe some one make bad things",
          "curr_magic",
          *((int64_t*)(data_ptr)),
          K(ret));
    } else if (OB_UNLIKELY((curr_size = *((int64_t*)(data_ptr)-2)) < data_length)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("target obj size is overflow", K(curr_size), "target_size", data_length, K(ret));
    } else if (is_number) {
      if (OB_UNLIKELY(need_size != curr_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("need_size should equal curr_size", K(curr_size), K(need_size), K(ret));
      } else {
        buf = (char*)(data_ptr);
      }
    } else {
      if (need_size > curr_size) {
        need_size = need_size * 2;
        void* buff_ptr = NULL;
        if (OB_ISNULL(buff_ptr = static_cast<char*>(aggr_alloc_.alloc(need_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fall to alloc buff", K(need_size), K(ret));
        } else {
          ((int64_t*)buff_ptr)[0] = need_size;
          ((int64_t*)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
          buf = (char*)((int64_t*)(buff_ptr) + 2);
          LOG_DEBUG("succ to alloc buff", K(need_size), K(src_cell), K(target_cell));
        }
      } else {
        buf = (char*)(data_ptr);
      }
    }
  } else {
    void* buff_ptr = NULL;
    if (OB_ISNULL(buff_ptr = static_cast<char*>(aggr_alloc_.alloc(need_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fall to alloc buff", K(need_size), K(ret));
    } else {
      ((int64_t*)buff_ptr)[0] = need_size;
      ((int64_t*)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
      buf = (char*)((int64_t*)(buff_ptr) + 2);
      LOG_DEBUG("succ to alloc buff", K(need_size), K(src_cell), K(target_cell));
    }
  }

  if (OB_SUCC(ret)) {
    // To reuse prealloc memory, we must use specialize deep_copy method
    // Otherwise for null value, we won't reserve its orgin ptr in ObDatum::deep_copy
    memcpy(buf, src_cell.ptr_, src_cell.len_);
    target_cell.ptr_ = buf;
    target_cell.pack_ = src_cell.pack_;
  }
  OX(LOG_DEBUG("succ to clone cell", K(src_cell), K(target_cell), K(curr_size), K(need_size)));
  return ret;
}

int ObAggregateProcessor::clone_number_cell(const ObNumber& src_number, ObDatum& target_cell)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  // length + magic num + data
  int64_t need_size = sizeof(int64_t) * 2 + ObNumber::MAX_BYTE_LEN;
  if (target_cell.len_ > 0) {
    void* data_ptr = const_cast<char*>(target_cell.ptr_);
    const int64_t data_length = target_cell.len_;
    int64_t curr_size = 0;
    if (OB_ISNULL((char*)data_ptr - sizeof(int64_t)) ||
        OB_ISNULL((char*)data_ptr - sizeof(int64_t) - sizeof(int64_t))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("clone_cell use aggr_alloc, need has meta", KP(data_ptr), K(ret));
    } else if (OB_UNLIKELY(*((int64_t*)(data_ptr)-1) != STORED_ROW_MAGIC_NUM)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("aggr_alloc memory is mismatch, maybe some one make bad things",
          "curr_magic",
          *((int64_t*)(data_ptr)),
          K(ret));
    } else if (OB_UNLIKELY((curr_size = *((int64_t*)(data_ptr)-2)) < data_length)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("target obj size is overflow", K(curr_size), "target_size", data_length, K(ret));
    } else if (OB_UNLIKELY(need_size != curr_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("need_size should equal curr_size", K(curr_size), K(need_size), K(ret));
    } else {
      buf = (char*)(data_ptr);
    }
  } else {
    void* buff_ptr = NULL;
    if (OB_ISNULL(buff_ptr = static_cast<char*>(aggr_alloc_.alloc(need_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ((int64_t*)buff_ptr)[0] = need_size;
      ((int64_t*)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
      buf = (char*)((int64_t*)(buff_ptr) + 2);
      LOG_DEBUG("succ to alloc buff", K(need_size), K(src_number), K(target_cell));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    target_cell.ptr_ = buf;
    target_cell.set_number(src_number);
    LOG_DEBUG("succ to clone cell", K(src_number), K(target_cell));
  }
  return ret;
}

int ObAggregateProcessor::prepare(GroupRow& group_row)
{
  int ret = OB_SUCCESS;
  // for sort-based group by operator, for performance reason,
  // after producing a group, we will invoke reuse_group() function to clear the group
  // thus, we do not need to allocate the group space again here, simply reuse the space
  // process aggregate columns
  for (int64_t i = 0; OB_SUCC(ret) && i < group_row.n_cells_; ++i) {
    const ObAggrInfo& aggr_info = aggr_infos_.at(i);
    AggrCell& aggr_cell = group_row.aggr_cells_[i];
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr info is null", K(aggr_cell), K(ret));
    } else if (OB_FAIL(aggr_cell.curr_row_results_.init(aggr_alloc_, aggr_info.get_child_output_count()))) {
      LOG_WARN("init failed", K(i), K(ret));
    } else if (aggr_info.is_implicit_first_aggr()) {
      ObDatum* result = NULL;
      if (aggr_info.expr_->eval(eval_ctx_, result)) {
        LOG_WARN("eval failed", K(ret));
      } else if (OB_FAIL(clone_cell(aggr_cell.get_iter_result(), *result))) {
        LOG_WARN("failed to clone non_aggr cell", K(ret));
      } else {
        aggr_cell.curr_row_results_.get_store_row()->cells()[0] = *result;
      }
    } else {
      if (OB_FAIL(aggr_info.eval_aggr(aggr_cell.curr_row_results_, eval_ctx_))) {
        LOG_WARN("fail to eval", K(ret));
      } else {
        if (aggr_info.has_distinct_) {
          ExtraResult* ad_result = static_cast<ExtraResult*>(aggr_cell.get_extra());
          // only last one add distinct
          if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("distinct set is NULL", K(ret));
          } else if (OB_FAIL(ad_result->unique_sort_op_->add_row(aggr_info.param_exprs_))) {
            LOG_WARN("add row to distinct set failed", K(ret));
          }
        } else {
          if (OB_FAIL(prepare_aggr_result(
                  *aggr_cell.curr_row_results_.get_store_row(), &(aggr_info.param_exprs_), aggr_cell, aggr_info))) {
            LOG_WARN("failed to prepare_aggr_result", K(ret));
          }
        }
      }
    }
    OX(LOG_DEBUG("finish prepare", K(aggr_cell), K(aggr_cell.curr_row_results_)));
  }  // end for
  return ret;
}

int ObAggregateProcessor::process(GroupRow& group_row)
{
  int ret = OB_SUCCESS;
  // process aggregate columns
  for (int64_t i = 0; OB_SUCC(ret) && i < group_row.n_cells_; ++i) {
    const ObAggrInfo& aggr_info = aggr_infos_.at(i);
    AggrCell& aggr_cell = group_row.aggr_cells_[i];
    ObDatum* result = NULL;
    if (aggr_info.is_implicit_first_aggr()) {
      // do nothing
      break;  // no need process the follower column
    } else {
      if (OB_FAIL(aggr_info.eval_aggr(aggr_cell.curr_row_results_, eval_ctx_))) {
        LOG_WARN("fail to eval", K(ret));
      } else {
        if (aggr_info.has_distinct_) {
          ExtraResult* ad_result = static_cast<ExtraResult*>(aggr_cell.get_extra());
          if (OB_FAIL(ad_result->unique_sort_op_->add_row(aggr_info.param_exprs_))) {
            LOG_WARN("add row to distinct set failed", K(ret));
          }
        } else {
          // not distinct aggr column
          if (OB_FAIL(process_aggr_result(
                  *aggr_cell.curr_row_results_.get_store_row(), &(aggr_info.param_exprs_), aggr_cell, aggr_info))) {
            LOG_WARN("failed to calculate aggr cell", K(ret));
          }
        }
      }
    }
    OX(LOG_DEBUG("finish process", K(aggr_cell), K(aggr_cell.curr_row_results_)));
  }  // end for
  return ret;
}

int ObAggregateProcessor::collect(const int64_t group_id /*= 0*/, const ObExpr* diff_expr /*= NULL*/)
{
  int ret = OB_SUCCESS;
  GroupRow* group_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    LOG_WARN("failed to get group_row", K(group_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group_row->n_cells_; ++i) {
      const ObAggrInfo& aggr_info = aggr_infos_.at(i);
      AggrCell& aggr_cell = group_row->aggr_cells_[i];
      ObDatum* result = NULL;
      if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr node is null", K(aggr_info), K(ret));
      } else if (aggr_info.is_implicit_first_aggr()) {
        ObDatum& result = aggr_info.expr_->locate_expr_datum(eval_ctx_);
        aggr_info.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
        result.set_datum(aggr_cell.get_iter_result());
      } else {
        if (aggr_info.has_distinct_) {
          ExtraResult* ad_result = static_cast<ExtraResult*>(aggr_cell.get_extra());
          if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("distinct set is NULL", K(ret));
          } else {
            if (group_id > 0) {
              // Group id greater than zero in sort based group by must be rollup,
              // distinct set is sorted and iterated in rollup_process(), rewind here.
              if (OB_FAIL(ad_result->unique_sort_op_->rewind())) {
                LOG_WARN("rewind iterator failed", K(ret));
              }
            } else {
              if (OB_FAIL(ad_result->unique_sort_op_->sort())) {
                LOG_WARN("sort failed", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(process_aggr_result_from_distinct(aggr_cell, aggr_info))) {
              LOG_WARN("aggregate distinct cell failed", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(collect_aggr_result(aggr_cell, diff_expr, aggr_info))) {
            LOG_WARN("collect_aggr_result failed", K(ret));
          }
        }
      }
      OX(LOG_DEBUG("finish collect", K(group_id), K(aggr_cell)));
    }  // end for
  }
  return ret;
}

int ObAggregateProcessor::collect_for_empty_set()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
    ObAggrInfo& aggr_info = aggr_infos_.at(i);
    ObDatum* result = NULL;
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(ret));
    } else if (aggr_info.is_implicit_first_aggr()) {
      ObDatum& datum = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
      aggr_info.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
      datum.set_null();

    } else {
      switch (aggr_info.get_expr_type()) {
        case T_FUN_COUNT:
        case T_FUN_COUNT_SUM:
        case T_FUN_APPROX_COUNT_DISTINCT:
        case T_FUN_KEEP_COUNT:
        case T_FUN_GROUP_PERCENT_RANK: {
          ObDatum& result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
          aggr_info.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
          if (share::is_mysql_mode()) {
            result.set_int(0);
          } else {
            ObNumber result_num;
            result_num.set_zero();
            result.set_number(result_num);
          }
          break;
        }
        case T_FUN_GROUP_RANK:
        case T_FUN_GROUP_DENSE_RANK:
        case T_FUN_GROUP_CUME_DIST: {
          ObDatum& result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
          aggr_info.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
          ObNumber result_num;
          int64_t num = 1;
          if (OB_FAIL(result_num.from(num, aggr_alloc_))) {
            LOG_WARN("failed to create number", K(ret));
          } else {
            result.set_number(result_num);
          }
          break;
        }
        case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
        case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
          ret = llc_init_empty(*aggr_info.expr_, eval_ctx_);
          break;
        }
        default: {
          ObDatum& datum = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
          aggr_info.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
          datum.set_null();
          // do nothing
          break;
        }
      }
    }
  }
  return ret;
}

// int ObAggregateProcessor::calc_distinct_item(const ObDatum &input,
//                                             AggrDistinctItem &distinct_item,
//                                             AggrCellCtx *cell_ctx)
//{
//  int ret = OB_SUCCESS;
//  if (OB_FAIL(fill_distinct_item(input, distinct_item))) {
//    LOG_WARN("failed to fill_distinct_item", K(ret));
//  } else {
//    ret = aggr_unique_sort_op_.exist_refactored(distinct_item);
//    if (OB_HASH_EXIST == ret) {
//      ret = OB_SUCCESS;
//    } else if (OB_HASH_NOT_EXIST == ret) {
//      if (OB_FAIL(process_aggr_result(distinct_item.expr_,
//                                 input,
//                                 const_cast<AggrCell &>(distinct_item.result_),
//                                 cell_ctx))) {
//        LOG_WARN("failed to calculate aggr cell", K(ret));
//      } else if (OB_FAIL(aggr_unique_sort_op_.set_refactored(distinct_item))) {
//        LOG_WARN("fail to set distinct item", K(ret));
//      } else {
//        ret = OB_SUCCESS;
//      }
//    } else {
//      LOG_WARN("check exist from aggr_distinct_set failed", K(ret));
//    }
//  }
//  return ret;
//}
//
int ObAggregateProcessor::process_aggr_result_from_distinct(AggrCell& aggr_cell, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  ExtraResult* ad_result = static_cast<ExtraResult*>(aggr_cell.get_extra());
  if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else {
    bool is_first = true;
    while (OB_SUCC(ret)) {
      const ObChunkDatumStore::StoredRow* stored_row = NULL;
      if (OB_FAIL(ad_result->unique_sort_op_->get_next_stored_row(stored_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get row from distinct set failed", K(ret));
        }
        break;
      } else if (OB_ISNULL(stored_row) || OB_ISNULL(stored_row->cells())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stored_row is NULL", KP(stored_row), K(ret));
      } else {
        if (is_first) {
          is_first = false;
          if (OB_FAIL(prepare_aggr_result(*stored_row, NULL, aggr_cell, aggr_info))) {
            LOG_WARN("prepare_aggr_result failed", K(ret));
          }
        } else {
          if (OB_FAIL(process_aggr_result(*stored_row, NULL, aggr_cell, aggr_info))) {
            LOG_WARN("process_aggr_result failed", K(ret));
          }
        }
        OX(LOG_DEBUG("succ iter prepare/process aggr result", K(stored_row), K(aggr_cell)));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::init_group_rows(const int64_t col_count)
{
  int ret = OB_SUCCESS;
  if (col_count > 1) {
    if (OB_FAIL(group_rows_.reserve(col_count))) {
      LOG_WARN("failed to reserve", "cnt", col_count, K(ret));
    }
    for (int64_t i = group_rows_.count(); OB_SUCC(ret) && i < col_count; ++i) {
      if (OB_FAIL(init_one_group(i))) {
        LOG_WARN("failed to init one group", K(i), K(col_count), K(ret));
      }
    }
  } else {
    if (OB_FAIL(init_one_group())) {
      LOG_WARN("failed to init one group", K(col_count), K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::init_one_group(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  GroupRow* group_row = NULL;
  const int64_t alloc_size = GROUP_ROW_SIZE + GROUP_CELL_SIZE * aggr_infos_.count();
  void* buff = NULL;
  if (OB_ISNULL(buff = aggr_alloc_.alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc stored row failed", K(alloc_size), K(group_id), K(ret));
  } else if (FALSE_IT(group_row = new (buff) GroupRow())) {
  } else if (FALSE_IT(group_row->n_cells_ = aggr_infos_.count())) {
    // do nothing
  } else if (FALSE_IT(group_row->aggr_cells_ =
                          new ((static_cast<char*>(buff)) + GROUP_ROW_SIZE) AggrCell[group_row->n_cells_])) {
    LOG_WARN("prepare_allocate failed", K(ret), K(aggr_infos_.count()));
  } else if (has_extra_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
      const ObAggrInfo& aggr_info = aggr_infos_.at(i);
      AggrCell& aggr_cell = group_row->aggr_cells_[i];
      switch (aggr_info.get_expr_type()) {
        case T_FUN_GROUP_CONCAT:
        case T_FUN_GROUP_RANK:
        case T_FUN_GROUP_DENSE_RANK:
        case T_FUN_GROUP_PERCENT_RANK:
        case T_FUN_GROUP_CUME_DIST:
        case T_FUN_MEDIAN:
        case T_FUN_GROUP_PERCENTILE_CONT:
        case T_FUN_GROUP_PERCENTILE_DISC:
        case T_FUN_KEEP_MAX:
        case T_FUN_KEEP_MIN:
        case T_FUN_KEEP_SUM:
        case T_FUN_KEEP_COUNT:
        case T_FUN_KEEP_WM_CONCAT:
        case T_FUN_WM_CONCAT: 
        case T_FUN_JSON_ARRAYAGG: 
        case T_FUN_JSON_OBJECTAGG: {
          void* tmp_buf = NULL;
          if (OB_ISNULL(tmp_buf = aggr_alloc_.alloc(sizeof(GroupConcatExtraResult)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            GroupConcatExtraResult* result = new (tmp_buf) GroupConcatExtraResult(aggr_alloc_);
            aggr_cell.set_extra(result);
            aggr_cell.set_allocator(&aggr_alloc_);
            const bool need_rewind = (in_window_func_ || group_id > 0);
            if (OB_FAIL(result->init(eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                    aggr_info,
                    eval_ctx_,
                    need_rewind, dir_id_))) {
              LOG_WARN("init GroupConcatExtraResult failed", K(ret));
            } else if (aggr_info.separator_expr_ != NULL) {
              ObDatum* separator_result = NULL;
              if (OB_UNLIKELY(!aggr_info.separator_expr_->obj_meta_.is_string_type())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expr node is null", K(ret), KPC(aggr_info.separator_expr_));
              } else if (OB_FAIL(aggr_info.separator_expr_->eval(eval_ctx_, separator_result))) {
                LOG_WARN("eval failed", K(ret));
              } else {
                // parse separator in prepare
                int64_t pos = sizeof(ObDatum);
                int64_t len = pos + (separator_result->null_ ? 0 : separator_result->len_);
                char* buf = (char*)aggr_alloc_.alloc(len);
                if (OB_ISNULL(buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fall to alloc buff", K(len), K(ret));
                } else {
                  ObDatum** separator_datum = const_cast<ObDatum**>(&result->get_separator_datum());
                  *separator_datum = new (buf) ObDatum;
                  if (OB_FAIL((*separator_datum)->deep_copy(*separator_result, buf, len, pos))) {
                    LOG_WARN("failed to deep copy datum", K(ret), K(pos), K(len));
                  } else {
                    LOG_DEBUG("succ to calc separator", K(ret), KP(*separator_datum));
                  }
                }
              }
            }
          }
          break;
        }
        default:
          break;
      }

      if (OB_SUCC(ret) && aggr_info.has_distinct_) {
        if (NULL == aggr_cell.get_extra()) {
          void* tmp_buf = NULL;
          if (OB_ISNULL(tmp_buf = aggr_alloc_.alloc(sizeof(ExtraResult)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            ExtraResult* result = new (tmp_buf) ExtraResult(aggr_alloc_);
            aggr_cell.set_extra(result);
            aggr_cell.set_allocator(&aggr_alloc_);
          }
        }

        if (OB_SUCC(ret)) {
          // In window function, get result will be called more than once, rewind is needed.
          //
          // The distinct set will iterate twice with rollup, for rollup processing and aggregation,
          // rewind is needed for the second iteration.
          //
          // Rollup is supported and only supported in sort based group by with multi-groups,
          // only groups with group id greater than zero need to rewind.
          const bool need_rewind = (in_window_func_ || group_id > 0);
          if (OB_FAIL(aggr_cell.get_extra()->init_distinct_set(
                  eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                  aggr_info,
                  eval_ctx_,
                  need_rewind))) {
            LOG_WARN("init_distinct_set failed", K(ret));
          }
        }
      }
    }  // end of for
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(group_rows_.push_back(group_row))) {
      LOG_WARN("push_back failed", K(group_id), K(group_row), K(ret));
    } else {
      LOG_DEBUG("succ init group_row", K(group_id), KPC(group_row), K(ret));
    }
  }

  if (OB_FAIL(ret) && NULL != group_row) {
    group_row->destroy();
    aggr_alloc_.free(group_row);
    group_row = NULL;
  }
  return ret;
}

int ObAggregateProcessor::rollup_process(const int64_t group_id, const ObExpr* diff_expr /*= NULL*/)
{
  int ret = OB_SUCCESS;
  GroupRow* group_row = NULL;
  GroupRow* rollup_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    LOG_WARN("get group_row failed", K(group_id), K(ret));
  } else if (OB_FAIL(group_rows_.at(group_id - 1, rollup_row))) {
    LOG_WARN("get group_row failed", "group_id", group_id - 1, K(ret));
  } else if (OB_ISNULL(group_row) || OB_ISNULL(rollup_row)) {
    LOG_WARN("group_row is null", KP(group_row), KP(rollup_row), K(ret));
  }

  // copy aggregation column
  for (int64_t i = 0; OB_SUCC(ret) && i < group_row->n_cells_; ++i) {
    const ObAggrInfo& aggr_info = aggr_infos_.at(i);
    AggrCell& aggr_cell = group_row->aggr_cells_[i];
    AggrCell& rollup_cell = rollup_row->aggr_cells_[i];
    ObDatum* result = NULL;
    if (aggr_info.is_implicit_first_aggr()) {
      if (OB_FAIL(clone_cell(rollup_cell.get_iter_result(), aggr_cell.get_iter_result()))) {
        LOG_WARN("failed to clone cell", K(ret));
      }
    } else {
      if (aggr_info.has_distinct_) {
        if (OB_FAIL(rollup_distinct(aggr_cell, rollup_cell))) {
          LOG_WARN("failed to rollup aggregation results", K(ret));
        }
      } else {
        if (OB_FAIL(rollup_aggregation(aggr_cell, rollup_cell, diff_expr, aggr_info))) {
          LOG_WARN("failed to rollup aggregation results", K(ret));
        }
      }
    }
    OX(LOG_DEBUG("finish rollup_process iter", K(i), K(aggr_cell), K(rollup_cell), K(ret)));
  }
  OX(LOG_DEBUG("finish rollup_process", K(group_id), KPC(group_row), KPC(rollup_row), KPC(diff_expr), K(ret)));
  return ret;
}

int ObAggregateProcessor::rollup_aggregation(
    AggrCell& aggr_cell, AggrCell& rollup_cell, const ObExpr* diff_expr, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  ObDatum& src_result = aggr_cell.get_iter_result();
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  ObDatum& target_result = rollup_cell.get_iter_result();
  switch (aggr_fun) {
    case T_FUN_COUNT: {
      rollup_cell.add_row_count(aggr_cell.get_row_count());
      break;
    }
    case T_FUN_MAX: {
      ret = max_calc(target_result, src_result, aggr_info.expr_->basic_funcs_->null_first_cmp_, aggr_info.is_number());
      break;
    }
    case T_FUN_MIN: {
      ret = min_calc(target_result, src_result, aggr_info.expr_->basic_funcs_->null_first_cmp_, aggr_info.is_number());
      break;
    }
    case T_FUN_AVG:
      rollup_cell.add_row_count(aggr_cell.get_row_count());
    case T_FUN_SUM:
    case T_FUN_COUNT_SUM: {
      ret = rollup_add_calc(aggr_cell, rollup_cell, aggr_info);
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT: {
      if (rollup_cell.get_llc_bitmap().len_ <= 0) {
        ret = clone_cell(rollup_cell.get_llc_bitmap(), aggr_cell.get_llc_bitmap());
      } else {
        ret = llc_add(rollup_cell.get_llc_bitmap(), aggr_cell.get_llc_bitmap());
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      if (rollup_cell.get_iter_result().len_ <= 0) {
        ret = clone_cell(target_result, src_result);
      } else {
        ret = llc_add(target_result, src_result);
      }
      break;
    }
    case T_FUN_GROUPING: {
      if (OB_UNLIKELY(aggr_info.param_exprs_.count() != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("param_exprs_ count is not 1", K(aggr_info));
      } else {
        if (diff_expr != NULL && diff_expr == aggr_info.param_exprs_.at(0)) {
          aggr_cell.set_tiny_num_int(1);
        }
        rollup_cell.set_tiny_num_int(aggr_cell.get_tiny_num_int());
      }
      break;
    }
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_MEDIAN:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT: 
    case T_FUN_JSON_ARRAYAGG: 
    case T_FUN_JSON_OBJECTAGG: {
      GroupConcatExtraResult* aggr_extra = NULL;
      GroupConcatExtraResult* rollup_extra = NULL;
      if (OB_ISNULL(aggr_extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra())) ||
          OB_ISNULL(rollup_extra = static_cast<GroupConcatExtraResult*>(rollup_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is NULL", K(ret), K(aggr_cell), K(rollup_cell));
      } else if (OB_FAIL(aggr_extra->finish_add_row())) {
        LOG_WARN("finish add row failed", K(ret));
      } else {
        const ObChunkDatumStore::StoredRow* stored_row = NULL;
        if (aggr_fun == T_FUN_JSON_ARRAYAGG || aggr_fun == T_FUN_JSON_OBJECTAGG) {
          int64_t len = aggr_extra->get_bool_mark_size();
          if (OB_FAIL(rollup_extra->reserve_bool_mark_count(len))) {
            LOG_WARN("reserve_bool_mark_count failed", K(ret), K(len));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < len; i++) {
            bool is_bool = false;
            if (OB_FAIL(aggr_extra->get_bool_mark(i, is_bool))) {
              LOG_WARN("get_bool_mark failed", K(ret));
            } else if (OB_FAIL(rollup_extra->set_bool_mark(i, is_bool))) {
              LOG_WARN("set_bool_mark failed", K(ret));
            }
          }
        }
        while (OB_SUCC(ret) && OB_SUCC(aggr_extra->get_next_row(stored_row))) {
          if (OB_ISNULL(stored_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("stored_row is NULL", K(ret), K(aggr_cell), K(rollup_cell));
          } else if (OB_FAIL(rollup_extra->add_row(*stored_row))) {
            LOG_WARN("add row failed", K(ret));
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
      }
      break;
    }
      //      case T_FUN_AGG_UDF: {
      //        ret = OB_NOT_SUPPORTED;
      //        LOG_WARN("rollup contain agg udfs still not supported", K(ret));
      //        break;
      //      }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun));
      break;
  }
  return ret;
}

int ObAggregateProcessor::rollup_distinct(AggrCell& aggr_cell, AggrCell& rollup_cell)
{
  int ret = OB_SUCCESS;
  ExtraResult* ad_result = static_cast<ExtraResult*>(aggr_cell.get_extra());
  ExtraResult* rollup_result = static_cast<ExtraResult*>(rollup_cell.get_extra());
  if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_) || OB_ISNULL(rollup_result) ||
      OB_ISNULL(rollup_result->unique_sort_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else if (OB_FAIL(ad_result->unique_sort_op_->sort())) {
    LOG_WARN("sort failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      const ObChunkDatumStore::StoredRow* stored_row = NULL;
      if (OB_FAIL(ad_result->unique_sort_op_->get_next_stored_row(stored_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get row from distinct set failed", K(ret));
        }
        break;
      } else if (OB_ISNULL(stored_row) || OB_ISNULL(stored_row->cells())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stored_row is NULL", KP(stored_row), K(ret));
      } else if (OB_FAIL(rollup_result->unique_sort_op_->add_stored_row(*stored_row))) {
        LOG_WARN("add_row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::reuse_group(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  GroupRow* group_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    LOG_WARN("fail to get stored row", K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored_row is null", K(group_row));
  } else {
    group_row->reuse();
  }
  return ret;
}

int ObAggregateProcessor::prepare_aggr_result(const ObChunkDatumStore::StoredRow& stored_row,
    const ObIArray<ObExpr*>* param_exprs, AggrCell& aggr_cell, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  if (NULL != param_exprs && param_exprs->empty()) {
    if (T_FUN_COUNT == aggr_fun) {
      aggr_cell.inc_row_count();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is null", K(aggr_fun), K(ret));
    }
  } else {
    switch (aggr_fun) {
      case T_FUN_COUNT: {
        bool has_null = false;
        for (int64_t i = 0; !has_null && i < stored_row.cnt_; ++i) {
          has_null = stored_row.cells()[i].is_null();
        }
        if (!has_null) {
          aggr_cell.inc_row_count();
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = clone_cell(aggr_cell.get_iter_result(), stored_row.cells()[0], aggr_info.is_number());
        }
        break;
      }
      case T_FUN_AVG: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          aggr_cell.inc_row_count();
          ret = prepare_add_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        }
        break;
      }
      case T_FUN_COUNT_SUM:
      case T_FUN_SUM: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = prepare_add_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        }
        break;
      }
      case T_FUN_GROUPING: {
        aggr_cell.set_tiny_num_int(0);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
        ObDatum& llc_bitmap =
            (T_FUN_APPROX_COUNT_DISTINCT == aggr_fun ? aggr_cell.get_llc_bitmap() : aggr_cell.get_iter_result());
        if (OB_FAIL(llc_init(llc_bitmap))) {
          LOG_WARN("llc_init failed");
        } else {
          bool has_null_cell = false;
          uint64_t hash_value = llc_calc_hash_value(stored_row, aggr_info.param_exprs_, has_null_cell);
          if (has_null_cell) {
            /*do nothing*/
          } else {
            ret = llc_add_value(hash_value, llc_bitmap.get_string());
          }
        }
        break;
      }
      case T_FUN_GROUP_CONCAT:
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST:
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_COUNT:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_WM_CONCAT: 
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG: {
        GroupConcatExtraResult* extra = NULL;
        if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
        } else {
          extra->reuse_self();
          if (param_exprs == NULL && OB_FAIL(extra->add_row(stored_row))) {
            LOG_WARN("fail to add row", K(ret));
          } else if (param_exprs != NULL && OB_FAIL(extra->add_row(*param_exprs, eval_ctx_))) {
            LOG_WARN("fail to add row", K(ret));
          } else {
            if (aggr_fun == T_FUN_JSON_ARRAYAGG || aggr_fun == T_FUN_JSON_OBJECTAGG) {
              int64_t len = aggr_info.param_exprs_.count();
              if (OB_FAIL(extra->reserve_bool_mark_count(len))) {
                LOG_WARN("reserve_bool_mark_count failed", K(ret), K(len));
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < len; i++) {
                ObExpr *tmp = NULL;
                if (OB_FAIL(aggr_info.param_exprs_.at(i, tmp))){
                  LOG_WARN("fail to get param_exprs[i]", K(ret));
                } else {
                  bool is_bool = (tmp->is_boolean_ == 1);
                  if (OB_FAIL(extra->set_bool_mark(i, is_bool))){
                    LOG_WARN("fail to set_bool_mark", K(ret));
                  }
                }
              }
            }
            LOG_DEBUG("succ to add row", K(stored_row), KPC(extra));
          }
        }
        break;
      }
        //    case T_FUN_AGG_UDF: {
        //      void *func_buf = aggr_udf_buf_.alloc(sizeof(ObAggUdfFunction));
        //      void *ctx_buf = aggr_udf_buf_.alloc(sizeof(ObUdfFunction::ObUdfCtx));
        //      ObAggUdfFunction *aggr_udf_func = nullptr;
        //      ObUdfFunction::ObUdfCtx *aggr_udf_ctx = nullptr;
        //      if (OB_ISNULL(ctx_buf) || OB_ISNULL(func_buf)) {
        //        ret = OB_ALLOCATE_MEMORY_FAILED;
        //        LOG_WARN("allocate memory failed", K(ret), K(ctx_buf), K(func_buf));
        //      } else if (FALSE_IT(aggr_udf_func =  new (func_buf)ObAggUdfFunction())) {
        //      } else if (FALSE_IT(aggr_udf_ctx =  new (ctx_buf)ObUdfFunction::ObUdfCtx())) {
        //      } else if (OB_ISNULL(aggr_udf_meta)) {
        //        ret = OB_ERR_UNEXPECTED;
        //        LOG_WARN("failed to push_back", K(ret), K(aggr_udf_meta));
        //      } else if (OB_FAIL(aggr_udf_func->init(aggr_udf_meta->udf_meta_))) {
        //        LOG_WARN("udf function init failed", K(ret));
        //      } else if (OB_FAIL(ObUdfUtil::init_udf_args(aggr_udf_buf_,
        //                                                  aggr_udf_meta->udf_attributes_,
        //                                                  aggr_udf_meta->udf_attributes_types_,
        //                                                  aggr_udf_ctx->udf_args_))) {
        //        LOG_WARN("failed to set udf args", K(ret));
        //      } else if (OB_FAIL(aggr_udf_func->process_init_func(*aggr_udf_ctx))) {
        //        LOG_WARN("do agg init func failed", K(ret));
        //      } else if (OB_FAIL(aggr_udf_func->process_clear_func(*aggr_udf_ctx))) {
        //        LOG_WARN("the udf clear func process failed", K(ret));
        //      } else if (OB_FAIL(aggr_udf_.set_refactored(aggr_udf_id,
        //                                                 ObAggUdfExeUnit(aggr_udf_func, aggr_udf_ctx)))) {
        //        LOG_WARN("udf failed", K(ret), K(aggr_udf_id));
        //      } else if (OB_FAIL(aggr_udf_func->process_add_func(expr_ctx_, aggr_udf_buf_, input_row, *aggr_udf_ctx)))
        //      {
        //        LOG_WARN("the udf add func process failed", K(ret));
        //      }
        //      LOG_DEBUG("agg init cell", K(input_row), K(aggr_udf_id), K(aggr_udf_ctx), K(ctx_buf),
        //      K(aggr_udf_.size())); break;
        //    }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown aggr function type", K(aggr_fun), K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::process_aggr_result(const ObChunkDatumStore::StoredRow& stored_row,
    const ObIArray<ObExpr*>* param_exprs, AggrCell& aggr_cell, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  if (NULL != param_exprs && param_exprs->empty()) {
    if (T_FUN_COUNT == aggr_fun) {
      aggr_cell.inc_row_count();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is null", K(aggr_fun), K(ret));
    }
  } else {
    switch (aggr_fun) {
      case T_FUN_COUNT: {
        bool has_null = false;
        for (int64_t i = 0; !has_null && i < stored_row.cnt_; ++i) {
          has_null = stored_row.cells()[i].is_null();
        }
        if (!has_null) {
          aggr_cell.inc_row_count();
        }
        break;
      }
      case T_FUN_MAX: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = max_calc(aggr_cell.get_iter_result(),
              stored_row.cells()[0],
              aggr_info.expr_->basic_funcs_->null_first_cmp_,
              aggr_info.is_number());
        }
        break;
      }
      case T_FUN_MIN: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = min_calc(aggr_cell.get_iter_result(),
              stored_row.cells()[0],
              aggr_info.expr_->basic_funcs_->null_first_cmp_,
              aggr_info.is_number());
        }
        break;
      }
      case T_FUN_AVG: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          aggr_cell.inc_row_count();
          ret = add_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        }
        break;
      }
      case T_FUN_SUM:
      case T_FUN_COUNT_SUM: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = add_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        }
        break;
      }
      case T_FUN_GROUPING: {
        // do nothing
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
        bool has_null_cell = false;
        ObDatum* llc_bitmap =
            (T_FUN_APPROX_COUNT_DISTINCT == aggr_fun ? &aggr_cell.get_llc_bitmap() : &aggr_cell.get_iter_result());
        uint64_t hash_value = llc_calc_hash_value(stored_row, aggr_info.param_exprs_, has_null_cell);
        if (has_null_cell) {
          /*do nothing*/
        } else {
          ret = llc_add_value(hash_value, llc_bitmap->get_string());
        }
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else {
          ret = llc_add(aggr_cell.get_iter_result(), stored_row.cells()[0]);
        }
        break;
      }
      case T_FUN_GROUP_CONCAT:
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST:
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_COUNT:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_WM_CONCAT: 
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG: {
        GroupConcatExtraResult* extra = NULL;
        if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
        } else if (param_exprs == NULL && OB_FAIL(extra->add_row(stored_row))) {
          LOG_WARN("fail to add row", K(ret));
        } else if (param_exprs != NULL && OB_FAIL(extra->add_row(*param_exprs, eval_ctx_))) {
          LOG_WARN("fail to add row", K(ret));
        } else {
          LOG_DEBUG("succ to add row", K(stored_row), KPC(extra));
        }
        break;
      }
        //    case T_FUN_AGG_UDF: {
        //      ObAggUdfExeUnit aggr_udf_unit;
        //      if (OB_FAIL(aggr_udf_.get_refactored(aggr_udf_id, aggr_udf_unit))) {
        //        LOG_WARN("failed to get agg func", K(ret));
        //      } else if (OB_ISNULL(aggr_udf_unit.aggr_func_) || OB_ISNULL(aggr_udf_unit.udf_ctx_)) {
        //        ret = OB_ERR_UNEXPECTED;
        //        LOG_WARN("the agg func is null", K(ret));
        //      } else if (OB_FAIL(aggr_udf_unit.aggr_func_->process_add_func(expr_ctx_, aggr_udf_buf_, input_row,
        //      *aggr_udf_unit.udf_ctx_))) {
        //        LOG_WARN("the udf add func process failed", K(ret));
        //      }
        //      break;
        //    }
      default:
        LOG_WARN("unknown aggr function type", K(aggr_fun), K(ret));
        break;
    }
  }
  return ret;
}

int ObAggregateProcessor::extend_concat_str_buf(const ObString& pad_str, const int64_t pos,
    const int64_t group_concat_cur_row_num, int64_t& append_len, bool& buf_is_full)
{
  int ret = OB_SUCCESS;
  int64_t tmp_max_len = pad_str.length() + pos;
  if (0 == cur_concat_buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current concat buf len is 0", K(ret));
  } else if (tmp_max_len > cur_concat_buf_len_) {
    if (cur_concat_buf_len_ < concat_str_max_len_) {
      // start from 1024, extend by double
      cur_concat_buf_len_ = cur_concat_buf_len_ * 2 < tmp_max_len ? next_pow2(tmp_max_len) : cur_concat_buf_len_ * 2;
      if (cur_concat_buf_len_ > concat_str_max_len_) {
        cur_concat_buf_len_ = concat_str_max_len_;
      }
      char* tmp_concat_str_buf = nullptr;
      if (OB_ISNULL(tmp_concat_str_buf = static_cast<char*>(aggr_alloc_.alloc(cur_concat_buf_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fall to alloc buff", K(ret));
      } else {
        if (0 < pos) {
          MEMCPY(tmp_concat_str_buf, concat_str_buf_, pos);
        }
        concat_str_buf_ = tmp_concat_str_buf;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (pad_str.length() + pos > concat_str_max_len_) {
      append_len = concat_str_max_len_ - pos;
      buf_is_full = true;
      if (share::is_oracle_mode()) {
        ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
        LOG_WARN(
            "result of string concatenation is too long", K(ret), K(pad_str.length()), K(pos), K(concat_str_max_len_));
      } else {
        LOG_USER_WARN(OB_ERR_CUT_VALUE_GROUP_CONCAT, group_concat_cur_row_num + 1);
      }
    }
  }
  return ret;
}
int ObAggregateProcessor::collect_aggr_result(AggrCell& aggr_cell, const ObExpr* diff_expr, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  ObDatum& result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
  ObEvalInfo& eval_info = aggr_info.expr_->get_eval_info(eval_ctx_);
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  switch (aggr_fun) {
    case T_FUN_COUNT: {
      if (share::is_mysql_mode()) {
        result.set_int(aggr_cell.get_row_count());
        eval_info.evaluated_ = true;
      } else {
        ObNumber result_num;
        char local_buff[ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
        if (OB_FAIL(result_num.from(aggr_cell.get_row_count(), local_alloc))) {
          LOG_WARN("fail to call from", K(ret));
        } else {
          result.set_number(result_num);
          eval_info.evaluated_ = true;
        }
      }
      break;
    }
    case T_FUN_COUNT_SUM:
    case T_FUN_SUM: {
      const ObObjTypeClass tc = ob_obj_type_class(aggr_info.get_first_child_type());
      if (OB_FAIL(aggr_cell.collect_result(tc, eval_ctx_, aggr_info))) {
        LOG_WARN("fail to collect_result", K(ret));
      } else {
        eval_info.evaluated_ = true;
      }
      break;
    }
    case T_FUN_AVG: {
      const ObObjType first_child_type = aggr_info.get_first_child_type();
      if (OB_FAIL(aggr_cell.collect_result(ob_obj_type_class(first_child_type), eval_ctx_, aggr_info))) {
        LOG_WARN("fail to collect_result", K(ret));
      } else if (!result.is_null() && OB_FAIL(ObExprDiv::calc_for_avg(
                                          result, result, aggr_cell.get_row_count(), eval_ctx_, first_child_type))) {
        LOG_WARN("fail to calc_for_avg", K(ret));
      } else {
        eval_info.evaluated_ = true;
      }
      break;
    }
    case T_FUN_GROUPING: {
      int64_t new_value = aggr_cell.get_tiny_num_int();
      if (diff_expr != NULL && diff_expr == aggr_info.param_exprs_.at(0)) {
        new_value = 1;
        aggr_cell.set_tiny_num_int(new_value);
      }
      if (share::is_mysql_mode()) {
        result.set_int(new_value);
        eval_info.evaluated_ = true;
      } else {
        ObNumber result_num;
        char local_buff[ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
        if (OB_FAIL(result_num.from(new_value, local_alloc))) {
          LOG_WARN("fail to call from", K(ret));
        } else {
          result.set_number(result_num);
          eval_info.evaluated_ = true;
        }
      }
      break;
    }
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      ret = aggr_info.expr_->deep_copy_datum(eval_ctx_, aggr_cell.get_iter_result());
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT: {
      int64_t tmp_result = OB_INVALID_COUNT;
      ObExprEstimateNdv::llc_estimate_ndv(tmp_result, aggr_cell.get_llc_bitmap().get_string());
      eval_info.evaluated_ = true;
      if (tmp_result >= 0) {
        if (share::is_mysql_mode()) {
          result.set_int(tmp_result);
        } else {
          char local_buff[ObNumber::MAX_BYTE_LEN];
          ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
          ObNumber result_num;
          if (OB_FAIL(result_num.from(tmp_result, local_alloc))) {
            LOG_WARN("failed to convert to number", K(ret));
          } else {
            result.set_number(result_num);
          }
        }
      }
      break;
    }
    case T_FUN_JSON_ARRAYAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_json_arrayagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get json_arrayagg result", K(ret));
      } else {
        eval_info.evaluated_ = true;
      }
      break;
    }
    case T_FUN_JSON_OBJECTAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_json_objectagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get json_objectagg result", K(ret));
      } else {
        eval_info.evaluated_ = true;
      }
      break;
    }
    case T_FUN_WM_CONCAT:
    case T_FUN_KEEP_WM_CONCAT: {
      GroupConcatExtraResult* extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra());
      if (OB_FAIL(get_wm_concat_result(aggr_info, extra, T_FUN_KEEP_WM_CONCAT == aggr_fun, result))) {
        LOG_WARN("failed to get wm concat result", K(ret));
      } else {
        eval_info.evaluated_ = true;
      }
      break;
    }

    case T_FUN_GROUP_CONCAT: {
      GroupConcatExtraResult* extra = NULL;
      ObString sep_str;
      if (NULL == concat_str_buf_) {
        uint64_t concat_str_max_len =
            (share::is_oracle_mode() ? OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE : OB_DEFAULT_GROUP_CONCAT_MAX_LEN);
        if (!share::is_oracle_mode() &&
            OB_FAIL(eval_ctx_.exec_ctx_.get_my_session()->get_group_concat_max_len(concat_str_max_len))) {
          LOG_WARN("fail to get group concat max len", K(ret));
        } else if (0 != cur_concat_buf_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first time concat buf len is not 0", K(cur_concat_buf_len_));
        } else {
          cur_concat_buf_len_ = concat_str_max_len > 1024 ? 1024 : concat_str_max_len;
          if (OB_ISNULL(concat_str_buf_ = static_cast<char*>(aggr_alloc_.alloc(cur_concat_buf_len_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fall to alloc buff", K(concat_str_max_len), K(ret));
          } else {
            concat_str_max_len_ = concat_str_max_len;
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(extra->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is empty", K(ret), KPC(extra));
      } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
        // Group concat row may be iterated in rollup_process(), rewind here.
        LOG_WARN("rewind failed", KPC(extra), K(ret));
      } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
        LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
      } else {
        if (aggr_info.separator_expr_ != NULL) {
          if (OB_ISNULL(extra->get_separator_datum())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("seperator is nullptr", K(ret));
          } else {
            sep_str = extra->get_separator_datum()->get_string();
          }
        } else {
          if (share::is_oracle_mode()) {
            sep_str = ObString::make_empty_string();
          } else {
            // default ','
            sep_str = ObCharsetUtils::get_const_str(aggr_info.expr_->datum_meta_.cs_type_, ',');
          }
        }
      }

      if (OB_SUCC(ret)) {
        int64_t pos = 0;
        bool buf_is_full = false;
        const ObChunkDatumStore::StoredRow* storted_row = NULL;
        int64_t group_concat_cur_row_num = 0;
        bool first_item_printed = false;
        const int64_t group_concat_param_count = aggr_info.group_concat_param_count_;
        while (OB_SUCC(ret) && !buf_is_full && OB_SUCC(extra->get_next_row(storted_row))) {
          bool should_skip = false;
          for (int64_t i = 0; !should_skip && i < group_concat_param_count; ++i) {
            should_skip = storted_row->cells()[i].is_null();
          }

          LOG_DEBUG("group concat iter",
              K(group_concat_cur_row_num),
              K(should_skip),
              KPC(extra),
              K(group_concat_param_count),
              KPC(storted_row),
              K(sep_str));

          if (!should_skip) {
            if (group_concat_cur_row_num != 0) {
              int64_t append_len = sep_str.length();
              if (OB_FAIL(extend_concat_str_buf(sep_str, pos, group_concat_cur_row_num, append_len, buf_is_full))) {
                LOG_WARN("failed to extend concat str buf", K(ret));
              } else if (cur_concat_buf_len_ < append_len + pos) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("concat buf len is error", K(ret), K(cur_concat_buf_len_), K(pos), K(append_len));
              } else if (0 < append_len) {
                MEMCPY(concat_str_buf_ + pos, sep_str.ptr(), append_len);
                pos += append_len;
                LOG_DEBUG("group concat iter append", K(sep_str), K(append_len), K(pos));
              }
            }
            for (int64_t i = 0; OB_SUCC(ret) && !buf_is_full && i < group_concat_param_count; ++i) {
              const ObString cell_string = storted_row->cells()[i].get_string();
              int64_t append_len = cell_string.length();
              if (OB_FAIL(extend_concat_str_buf(cell_string, pos, group_concat_cur_row_num, append_len, buf_is_full))) {
                LOG_WARN("failed to extend concat str buf", K(ret));
              } else if (cur_concat_buf_len_ < append_len + pos) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("concat buf len is error",
                    K(ret),
                    K(cur_concat_buf_len_),
                    K(pos),
                    K(append_len),
                    K(cell_string.length()));
              } else if (0 < append_len) {
                MEMCPY(concat_str_buf_ + pos, cell_string.ptr(), append_len);
                pos += append_len;
                LOG_DEBUG("group concat iter append", K(i), K(cell_string), K(append_len), K(pos));
              }
            }  // end of for

            if (OB_SUCC(ret)) {
              ++group_concat_cur_row_num;
              first_item_printed |= (!first_item_printed);
            }
          }
        }  // end of while

        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
          ObDatum concat_result;
          if (first_item_printed) {
            concat_result.set_string(concat_str_buf_, pos);
          } else {
            concat_result.set_null();  // set null if all skiped
          }
          OX(LOG_DEBUG("group concat finish ", K(group_concat_cur_row_num), K(pos), K(concat_result), KPC(extra)));
          ret = aggr_info.expr_->deep_copy_datum(eval_ctx_, concat_result);
          eval_info.evaluated_ = true;
        }
      }
      break;
    }
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST: {  // reuse GroupConcatCtx
      GroupConcatExtraResult* extra = NULL;
      if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(extra->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is empty", K(ret), KPC(extra));
      } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
        // Group concat row may be iterated in rollup_process(), rewind here.
        LOG_WARN("rewind failed", KPC(extra), K(ret));
      } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
        LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
      } else {
        const ObChunkDatumStore::StoredRow* storted_row = NULL;
        int64_t rank_num = 0;
        bool need_check_order_equal = T_FUN_GROUP_DENSE_RANK == aggr_fun;
        ObChunkDatumStore::LastStoredRow prev_row(aggr_alloc_);
        int64_t total_sort_row_cnt = extra->get_row_count();
        bool is_first = true;
        while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
          if (NULL == storted_row) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else {
            ++rank_num;
            bool find_target_rank = true;
            int comp_result = -2;
            bool is_all_equal = T_FUN_GROUP_CUME_DIST == aggr_fun;  // use to cume dist
            bool need_continue = true;
            for (int64_t i = 0; OB_SUCC(ret) && need_continue && i < aggr_info.sort_collations_.count(); ++i) {
              bool is_asc = false;
              uint32_t field_index = aggr_info.sort_collations_.at(i).field_idx_;
              if (OB_UNLIKELY(i >= storted_row->cnt_ || field_index >= storted_row->cnt_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get invalid argument", K(ret), K(i), K(storted_row->cnt_), K(field_index));
              } else if (OB_FAIL(compare_calc(storted_row->cells()[i],
                             storted_row->cells()[field_index],
                             aggr_info,
                             i,
                             comp_result,
                             is_asc))) {
                LOG_WARN("failed to compare calc", K(ret));
              } else if (comp_result < -1 || comp_result > 1) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get invalid argument", K(ret), K(comp_result));
              } else if (0 == comp_result) {  // equal(=)
                /*do nothing*/
              } else if (is_asc && 1 == comp_result) {  // asc great(>)
                find_target_rank = false;
                is_all_equal = false;
                need_continue = false;
              } else if (is_asc && -1 == comp_result) {  // asc less(<)
                find_target_rank = true;
                is_all_equal = false;
                need_continue = false;
              } else if (!is_asc && 1 == comp_result) {  // desc great(>)
                find_target_rank = true;
                is_all_equal = false;
                need_continue = false;
              } else if (!is_asc && -1 == comp_result) {  // desc less(<)
                find_target_rank = false;
                is_all_equal = false;
                need_continue = false;
              } else { /*do nothing*/
              }
            }
            if (OB_SUCC(ret)) {
              bool is_equal = false;
              if (need_check_order_equal) {
                if (is_first) {
                  if (OB_FAIL(prev_row.save_store_row(*storted_row))) {
                    LOG_WARN("failed to deep copy limit last rows", K(ret));
                  } else {
                    is_first = false;
                  }
                } else if (OB_FAIL(check_rows_equal(prev_row, *storted_row, aggr_info, is_equal))) {
                  LOG_WARN("failed to is order by item equal with prev row", K(ret));
                } else if (is_equal) {
                  --rank_num;
                } else {
                  if (OB_FAIL(prev_row.save_store_row(*storted_row))) {
                    LOG_WARN("failed to deep copy limit last rows", K(ret));
                  }
                }
                if (OB_SUCC(ret)) {
                  if (find_target_rank) {
                    break;
                  }
                }
              } else if (find_target_rank && !is_all_equal) {
                break;
              }
            }
          }
        }
        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          rank_num = ret == OB_ITER_END ? rank_num + 1 : rank_num;
          ret = OB_SUCCESS;
          number::ObNumber num_result;
          if (T_FUN_GROUP_RANK == aggr_fun || T_FUN_GROUP_DENSE_RANK == aggr_fun) {
            if (OB_FAIL(num_result.from(rank_num, aggr_alloc_))) {
              LOG_WARN("failed to create number", K(ret));
            }
          } else {
            number::ObNumber num;
            number::ObNumber num_total;
            rank_num = aggr_fun == T_FUN_GROUP_PERCENT_RANK ? rank_num - 1 : rank_num;
            total_sort_row_cnt = aggr_fun == T_FUN_GROUP_CUME_DIST ? total_sort_row_cnt + 1 : total_sort_row_cnt;
            if (OB_FAIL(num.from(rank_num, aggr_alloc_))) {
              LOG_WARN("failed to create number", K(ret));
            } else if (OB_FAIL(num_total.from(total_sort_row_cnt, aggr_alloc_))) {
              LOG_WARN("failed to div number", K(ret));
            } else if (OB_FAIL(num.div(num_total, num_result, aggr_alloc_))) {
              LOG_WARN("failed to div number", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            result.set_number(num_result);
            eval_info.evaluated_ = true;
          }
        }
      }
      break;
    }
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_MEDIAN: {
      GroupConcatExtraResult* extra = NULL;
      if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(extra->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is empty", K(ret), KPC(extra));
      } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
        // Group concat row may be iterated in rollup_process(), rewind here.
        LOG_WARN("rewind failed", KPC(extra), K(ret));
      } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
        LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
      } else if (1 != aggr_info.sort_collations_.count() ||
                 aggr_info.sort_collations_.at(0).field_idx_ >= aggr_info.param_exprs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sort_collations/param_exprs count", K(ret));
      } else {
        const int64_t param_idx = 0;
        const int64_t obj_idx = aggr_info.sort_collations_.at(0).field_idx_;
        const ObChunkDatumStore::StoredRow* storted_row = NULL;
        const int64_t total_row_count = extra->get_row_count();
        char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN);
        ObChunkDatumStore::LastStoredRow prev_row(aggr_alloc_);
        number::ObNumber factor;
        bool need_linear_inter = false;
        int64_t not_null_start_loc = 0;
        int64_t dest_loc = 0;
        int64_t row_cnt = 0;
        while (OB_SUCC(ret) && 0 == not_null_start_loc && OB_SUCC(extra->get_next_row(storted_row))) {
          if (OB_ISNULL(storted_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected group storted_row", K(ret), K(storted_row));
          } else {
            ++row_cnt;
            if (!storted_row->cells()[obj_idx].is_null()) {
              not_null_start_loc = row_cnt;
            }
          }
        }
        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else if (ret == OB_ITER_END) {
          /* do nothing */
        } else if (T_FUN_MEDIAN == aggr_fun) {
          if (1 == (total_row_count - not_null_start_loc) % 2) {
            need_linear_inter = true;
            if (OB_FAIL(factor.from(number::ObNumber::get_positive_zero_dot_five(), allocator))) {
              LOG_WARN("failed to create number", K(ret));
            }
          }
          dest_loc = not_null_start_loc + (total_row_count - not_null_start_loc) / 2;
        } else if (OB_FAIL(get_percentile_param(aggr_info,
                       storted_row->cells()[param_idx],
                       not_null_start_loc,
                       total_row_count,
                       dest_loc,
                       need_linear_inter,
                       factor,
                       allocator))) {
          LOG_WARN("get linear inter factor", K(factor));
        }
        while (OB_SUCC(ret) && row_cnt < dest_loc && OB_SUCC(extra->get_next_row(storted_row))) {
          if (OB_ISNULL(storted_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else {
            ++row_cnt;
          }
        }
        if (ret == OB_ITER_END && 0 == not_null_start_loc) {
          ret = OB_SUCCESS;
          result.set_null();
          eval_info.evaluated_ = true;
          LOG_DEBUG("result is null", K(*storted_row), K(row_cnt));
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to get dest loc row", K(ret), K(dest_loc), K(row_cnt));
        } else if (OB_FAIL(prev_row.save_store_row(*storted_row))) {
          LOG_WARN("fail to deep copy cur row", K(ret));
        } else if (need_linear_inter) {
          if (OB_FAIL(extra->get_next_row(storted_row))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else if (OB_ISNULL(storted_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else if (OB_FAIL(linear_inter_calc(aggr_info,
                         prev_row.store_row_->cells()[obj_idx],
                         storted_row->cells()[obj_idx],
                         factor,
                         result))) {
            LOG_WARN("failed to calc linear inter",
                K(ret),
                K(prev_row.store_row_->cells()[obj_idx]),
                K(storted_row->cells()[obj_idx]),
                K(factor));
          } else {
            eval_info.evaluated_ = true;
            LOG_DEBUG("get median result",
                K(factor),
                K(prev_row.store_row_->cells()[obj_idx]),
                K(storted_row->cells()[obj_idx]),
                K(not_null_start_loc),
                K(dest_loc),
                K(need_linear_inter),
                K(result));
          }
        } else if (OB_FAIL(aggr_info.expr_->deep_copy_datum(eval_ctx_, prev_row.store_row_->cells()[obj_idx]))) {
          LOG_WARN("clone cell failed", K(ret), K(prev_row.store_row_->cells()[obj_idx]));
        } else {
          eval_info.evaluated_ = true;
          LOG_DEBUG("get median result",
              K(prev_row.store_row_->cells()[obj_idx]),
              K(not_null_start_loc),
              K(dest_loc),
              K(need_linear_inter),
              K(result));
        }
      }
      break;
    }
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_COUNT: {
      if (OB_UNLIKELY(aggr_info.group_concat_param_count_ != 1 && aggr_info.group_concat_param_count_ != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(aggr_info.group_concat_param_count_));
      } else {
        GroupConcatExtraResult* extra = NULL;
        if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult*>(aggr_cell.get_extra()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
        } else if (OB_UNLIKELY(extra->empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra is empty", K(ret), KPC(extra));
        } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
          LOG_WARN("rewind failed", KPC(extra), K(ret));
        } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
          LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
        } else {
          const ObChunkDatumStore::StoredRow* storted_row = NULL;
          ObChunkDatumStore::LastStoredRow first_row(aggr_alloc_);
          bool is_first = true;
          while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
            bool is_equal = false;
            if (NULL == storted_row) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
            } else if (is_first && OB_FAIL(first_row.save_store_row(*storted_row))) {
              LOG_WARN("failed to deep copy limit last rows", K(ret));
            } else if (!is_first && OB_FAIL(check_rows_equal(first_row, *storted_row, aggr_info, is_equal))) {
              LOG_WARN("failed to is order by item equal with prev row", K(ret));
            } else if (is_first || is_equal) {
              is_first = false;
              switch (aggr_fun) {
                case T_FUN_KEEP_MAX: {
                  if (!storted_row->cells()[0].is_null()) {
                    ret = max_calc(aggr_cell.get_iter_result(),
                        storted_row->cells()[0],
                        aggr_info.expr_->basic_funcs_->null_first_cmp_,
                        aggr_info.is_number());
                  }
                  break;
                }
                case T_FUN_KEEP_MIN: {
                  if (!storted_row->cells()[0].is_null()) {
                    ret = min_calc(aggr_cell.get_iter_result(),
                        storted_row->cells()[0],
                        aggr_info.expr_->basic_funcs_->null_first_cmp_,
                        aggr_info.is_number());
                  }
                  break;
                }
                case T_FUN_KEEP_SUM: {
                  if (!storted_row->cells()[0].is_null()) {
                    ret = add_calc(storted_row->cells()[0], aggr_cell, aggr_info);
                  }
                  break;
                }
                case T_FUN_KEEP_COUNT: {
                  if (aggr_info.group_concat_param_count_ == 0 || !storted_row->cells()[0].is_null()) {
                    aggr_cell.inc_row_count();
                  }
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("get unexpected aggr type", K(ret), K(aggr_fun));
                  break;
                }
              }
            } else {
              break;
            }
          }
          if (ret != OB_ITER_END && ret != OB_SUCCESS) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            switch (aggr_fun) {
              case T_FUN_KEEP_MAX:
              case T_FUN_KEEP_MIN: {
                if (OB_FAIL(aggr_info.expr_->deep_copy_datum(eval_ctx_, aggr_cell.get_iter_result()))) {
                  LOG_WARN("fail to deep copy datum", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  eval_info.evaluated_ = true;
                }
                break;
              }
              case T_FUN_KEEP_SUM: {
                const ObObjTypeClass tc = ob_obj_type_class(aggr_info.get_first_child_type());
                if (OB_FAIL(aggr_cell.collect_result(tc, eval_ctx_, aggr_info))) {
                  LOG_WARN("fail to collect_result", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  eval_info.evaluated_ = true;
                }
                break;
              }
              case T_FUN_KEEP_COUNT: {
                ObNumber result_num;
                char local_buff[ObNumber::MAX_BYTE_LEN];
                ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
                if (OB_FAIL(result_num.from(aggr_cell.get_row_count(), local_alloc))) {
                  LOG_WARN("fail to call from", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  result.set_number(result_num);
                  eval_info.evaluated_ = true;
                }
                break;
              }
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected aggr type", K(ret), K(aggr_fun));
            }
          }
        }
      }
      break;
    }
      //        case T_FUN_AGG_UDF: {
      //          ObObj &aggr_result = *aggr_cell;
      //          ObAggUdfExeUnit aggr_udf_unit;
      //          LOG_DEBUG("calc aggr cell", K(aggr_udf_id));
      //          if (OB_FAIL(aggr_udf_.get_refactored(aggr_udf_id, aggr_udf_unit))) {
      //            LOG_WARN("failed to get agg func", K(ret));
      //          } else if (OB_ISNULL(aggr_udf_unit.aggr_func_) || OB_ISNULL(aggr_udf_unit.udf_ctx_)) {
      //            ret = OB_ERR_UNEXPECTED;
      //            LOG_WARN("the agg func/ctx is null", K(ret));
      //          } else if (OB_FAIL(aggr_udf_unit.aggr_func_->process_origin_func(aggr_udf_buf_, aggr_result,
      //          *aggr_udf_unit.udf_ctx_))) {
      //            LOG_WARN("the udf origin func process failed", K(ret));
      //          } else if (OB_FAIL(aggr_udf_unit.aggr_func_->process_clear_func(*aggr_udf_unit.udf_ctx_))) {
      //            LOG_WARN("the udf clear func process failed", K(ret));
      //          } else if (OB_FAIL(aggr_udf_.erase_refactored(aggr_udf_id))) {
      //            LOG_WARN("fail to erase hash key", K(ret));
      //          } else {
      //            aggr_udf_unit.aggr_func_->process_deinit_func(*aggr_udf_unit.udf_ctx_);
      //          }
      //          LOG_DEBUG("get udf result", K(aggr_result), K(aggr_udf_id));
      //          break;
      //        }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun));
      break;
  }  // end switch
  return ret;
}

int ObAggregateProcessor::search_op_expr(ObExpr* upper_expr, const ObItemType dst_op, ObExpr*& res_expr)
{
  int ret = OB_SUCCESS;
  ObExpr* tmp_expr = upper_expr;
  res_expr = NULL;
  while (OB_SUCC(ret) && OB_NOT_NULL(tmp_expr) && T_FUN_SYS_CAST == tmp_expr->type_) {
    if (2 != tmp_expr->arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sys cast arg cnt", K(ret), K(*tmp_expr));
    } else {
      LOG_DEBUG("search calc expr", K(dst_op), K(*tmp_expr));
      tmp_expr->get_eval_info(eval_ctx_).clear_evaluated_flag();
      tmp_expr = tmp_expr->args_[0];
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(tmp_expr) && dst_op == tmp_expr->type_) {
    tmp_expr->get_eval_info(eval_ctx_).clear_evaluated_flag();
    res_expr = tmp_expr;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail find dst_op", K(ret), K(dst_op));
  }
  return ret;
}

int ObAggregateProcessor::linear_inter_calc(const ObAggrInfo& aggr_info, const ObDatum& prev_datum,
    const ObDatum& curr_datum, const number::ObNumber& factor, ObDatum& res)
{
  int ret = OB_SUCCESS;
  res.set_null();
  ObExpr* linear_inter_expr = NULL;
  ObExpr* add_expr = NULL;
  ObExpr* mul_expr = NULL;
  ObExpr* minus_expr = NULL;
  ObExpr* var_factor = NULL;
  ObExpr* var_prev_datum = NULL;
  ObExpr* var_curr_datum = NULL;
  const int64_t order_idx = aggr_info.sort_collations_.at(0).field_idx_;
  if (OB_UNLIKELY(T_QUESTIONMARK == aggr_info.param_exprs_.at(order_idx)->type_)) {
    res.set_datum(curr_datum);
  } else if (OB_ISNULL(linear_inter_expr = aggr_info.linear_inter_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("linear inter expr is null", K(ret), K(aggr_info));
  } else if (OB_FAIL(search_op_expr(linear_inter_expr, T_OP_ADD, add_expr)) || OB_UNLIKELY(2 != add_expr->arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected linear inter expr", K(ret), K(*linear_inter_expr), K(*add_expr));
  } else if (OB_FAIL(search_op_expr(add_expr->args_[0], T_OP_MUL, mul_expr)) ||
             OB_ISNULL(var_prev_datum = add_expr->args_[1]) || OB_UNLIKELY(T_EXEC_VAR != var_prev_datum->type_) ||
             OB_UNLIKELY(2 != mul_expr->arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected linear inter expr", K(ret), K(*add_expr), K(*var_prev_datum), K(*mul_expr));
  } else if (OB_FAIL(search_op_expr(mul_expr->args_[0], T_EXEC_VAR, var_factor)) ||
             OB_FAIL(search_op_expr(mul_expr->args_[1], T_OP_MINUS, minus_expr)) ||
             OB_UNLIKELY(2 != minus_expr->arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected linear inter expr", K(ret), K(*mul_expr), K(*var_factor), K(*minus_expr));
  } else if (OB_ISNULL(var_curr_datum = minus_expr->args_[0]) || OB_UNLIKELY(T_EXEC_VAR != var_curr_datum->type_) ||
             OB_UNLIKELY(var_prev_datum != minus_expr->args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected linear inter expr", K(ret), K(*var_curr_datum), K(*minus_expr));
  } else if (OB_FAIL(var_curr_datum->deep_copy_datum(eval_ctx_, curr_datum))) {
    LOG_WARN("fail to deep copy datum ", K(ret));
  } else if (OB_FAIL(var_prev_datum->deep_copy_datum(eval_ctx_, prev_datum))) {
    LOG_WARN("fail to deep copy datum ", K(ret));
  } else {
    var_curr_datum->get_eval_info(eval_ctx_).evaluated_ = true;
    var_prev_datum->get_eval_info(eval_ctx_).evaluated_ = true;
    var_factor->locate_datum_for_write(eval_ctx_).set_number(factor);
    var_factor->get_eval_info(eval_ctx_).evaluated_ = true;
    LOG_DEBUG("get linear inter input",
        K(*add_expr),
        K(*mul_expr),
        K(*minus_expr),
        K(*var_prev_datum),
        K(*var_curr_datum),
        K(*var_factor));
    ObDatum* tmp_res = NULL;
    if (OB_FAIL(linear_inter_expr->eval(eval_ctx_, tmp_res))) {
      LOG_WARN("fail to calc linear inter", K(ret));
    } else {
      res.set_datum(*tmp_res);
      LOG_DEBUG("get linear result", K(*linear_inter_expr), K(res));
    }
  }
  return ret;
}

int ObAggregateProcessor::get_percentile_param(const ObAggrInfo& aggr_info, const ObDatum& param,
    const int64_t not_null_start_loc, const int64_t total_row_count, int64_t& dest_loc, bool& need_linear_inter,
    number::ObNumber& factor, ObDataBuffer& allocator)
{
  int ret = OB_SUCCESS;
  need_linear_inter = false;
  int64_t scale = 0;
  int64_t int64_value = 0;
  char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN * 5];
  ObDataBuffer local_allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN * 5);
  const ObDatumMeta param_datum_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
  number::ObNumber percentile;
  if (!ob_is_number_tc(param_datum_meta.type_)) {  // cast to number in deduce type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("percentile value invalid", K(ret), K(param_datum_meta), K(param));
  } else if (OB_FAIL(percentile.from(number::ObNumber(param.get_number()), local_allocator))) {
    LOG_WARN("failed to create number percentile", K(ret));
  } else if (percentile.is_negative() || 0 < percentile.compare(number::ObNumber::get_positive_one())) {
    ret = OB_ERR_PERCENTILE_VALUE_INVALID;
    LOG_WARN("invalid percentile value", K(ret), K(percentile));
  } else if (T_FUN_GROUP_PERCENTILE_DISC == aggr_info.get_expr_type()) {
    //
    //  PERCENTILE_DISC:
    //  ignore null if rows order by null first
    //  percentile value (P) and the number of rows (N)
    //  dest_loc = ceil(N * P) + not_null_start_loc - 1
    //  exception: when  P is 0, read from not_null_start_loc
    //
    number::ObNumber row_count, rn;
    if (percentile.is_zero()) {
      dest_loc = not_null_start_loc;
    } else if (OB_FAIL(row_count.from(total_row_count - not_null_start_loc + 1, local_allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (OB_FAIL(percentile.mul(row_count, rn, local_allocator, false))) {
      LOG_WARN("failed to calc number mul", K(ret));
    } else if (OB_FAIL(rn.ceil(scale))) {
      LOG_WARN("failed to ceil number", K(ret));
    } else if (!rn.is_int64()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get int64 after ceil", K(ret));
    } else if (OB_FAIL(rn.cast_to_int64(int64_value))) {
      LOG_WARN("failed to cast to int64", K(ret), K(rn));
    } else {
      dest_loc = not_null_start_loc + int64_value - 1;
    }
  } else if (T_FUN_GROUP_PERCENTILE_CONT == aggr_info.get_expr_type()) {
    //
    //  PERCENTILE_CONT:
    //  ignore null if rows order by null first
    //  percentile value (P) and the number of rows (N), row number RN = (1 + (P * (N - 1))
    //  CRN = CEILING(RN) and FRN = FLOOR(RN)
    //  If (CRN = FRN = RN) then the result is
    //    (value of expression from row at RN)
    //  Otherwise the result is
    //    (CRN - RN) * (value of expression for row at FRN) +
    //    (RN - FRN) * (value of expression for row at CRN)
    //    = (RN - FRN) * (row at CRN - row at FRN) + row at FRN
    //
    //  factor = (RN - FRN)
    //  FRN position dest_loc = not_null_start_loc + RN - 1
    //  calc_linear_inter function: factor * (obj2 - obj1) + obj1
    //
    number::ObNumber row_count, rn, frn, res;
    if (OB_FAIL(row_count.from(total_row_count - not_null_start_loc, local_allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (OB_FAIL(percentile.mul(row_count, rn, local_allocator, false))) {
      LOG_WARN("failed to calc number mul", K(ret));
    } else if (OB_FAIL(frn.from(rn, local_allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (OB_FAIL(frn.floor(scale))) {
      LOG_WARN("failed to floor number", K(ret));
    } else if (OB_FAIL(rn.sub(frn, res, local_allocator))) {
      LOG_WARN("failed to calc number sub", K(ret));
    } else if (!frn.is_int64()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get int64 after floor", K(ret));
    } else if (OB_FAIL(frn.cast_to_int64(int64_value))) {
      LOG_WARN("failed to cast to int64", K(ret), K(rn));
    } else if (OB_FAIL(factor.from(res, allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (rn.is_integer()) {
      dest_loc = not_null_start_loc + int64_value;
    } else {
      need_linear_inter = true;
      dest_loc = not_null_start_loc + int64_value;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aggr function type", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::max_calc(
    ObDatum& base, const ObDatum& other, ObDatumCmpFuncType cmp_func, const bool is_number)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmp_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret));
  } else if (!base.is_null() && !other.is_null()) {
    if (cmp_func(base, other) < 0) {
      ret = clone_cell(base, other, is_number);
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_cell(base, other, is_number);
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateProcessor::min_calc(
    ObDatum& base, const ObDatum& other, ObDatumCmpFuncType cmp_func, const bool is_number)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmp_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret));
  } else if (!base.is_null() && !other.is_null()) {
    if (cmp_func(base, other) > 0) {
      ret = clone_cell(base, other, is_number);
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_cell(base, other, is_number);
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateProcessor::prepare_add_calc(const ObDatum& first_value, AggrCell& aggr_cell, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum& result_datum = aggr_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      aggr_cell.set_tiny_num_int(first_value.get_int());
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObUIntTC: {
      aggr_cell.set_tiny_num_uint(first_value.get_uint());
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObFloatTC:
    case ObDoubleTC: {
      ret = clone_cell(result_datum, first_value, false);
      break;
    }
    case ObNumberTC: {
      ret = clone_cell(result_datum, first_value, true);
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("bot support now", K(column_tc));
    }
  }
  return ret;
}

int ObAggregateProcessor::add_calc(const ObDatum& iter_value, AggrCell& aggr_cell, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum& result_datum = aggr_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      int64_t left_int = aggr_cell.get_tiny_num_int();
      int64_t right_int = iter_value.get_int();
      int64_t sum_int = left_int + right_int;
      if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
        LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObNumber result_nmb;
        if (!result_datum.is_null()) {
          ObCompactNumber& cnum = const_cast<ObCompactNumber&>(result_datum.get_number());
          result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
        }
        if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
          LOG_WARN("number add failed", K(ret));
        } else if (OB_FAIL(clone_number_cell(result_nmb, result_datum))) {
          LOG_WARN("clone_number_cell failed", K(ret));
        } else {
          aggr_cell.set_tiny_num_int(0);
        }
      } else {
        LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
        aggr_cell.set_tiny_num_int(sum_int);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObUIntTC: {
      uint64_t left_uint = aggr_cell.get_tiny_num_uint();
      uint64_t right_uint = iter_value.get_uint();
      uint64_t sum_uint = left_uint + right_uint;
      if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
        LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObNumber result_nmb;
        if (!result_datum.is_null()) {
          ObCompactNumber& cnum = const_cast<ObCompactNumber&>(result_datum.get_number());
          result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
        }
        if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, allocator))) {
          LOG_WARN("number add failed", K(ret));
        } else if (OB_FAIL(clone_number_cell(result_nmb, result_datum))) {
          LOG_WARN("clone_number_cell failed", K(ret));
        } else {
          aggr_cell.set_tiny_num_uint(0);
        }
      } else {
        LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
        aggr_cell.set_tiny_num_uint(sum_uint);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObFloatTC: {
      if (result_datum.is_null()) {
        ret = clone_cell(result_datum, iter_value, false);
      } else {
        float left_f = result_datum.get_float();
        float right_f = iter_value.get_float();
        if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))) {
          ret = OB_OPERATE_OVERFLOW;
          char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
          int64_t pos = 0;
          databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_f, right_f);
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
          LOG_WARN("float out of range", K(left_f), K(right_f));
        } else {
          result_datum.set_float(left_f + right_f);
        }
      }
      break;
    }
    case ObDoubleTC: {
      if (result_datum.is_null()) {
        ret = clone_cell(result_datum, iter_value, false);
      } else {
        double left_d = result_datum.get_double();
        double right_d = iter_value.get_double();
        result_datum.set_double(left_d + right_d);
      }
      break;
    }
    case ObNumberTC: {
      if (result_datum.is_null()) {
        ret = clone_cell(result_datum, iter_value, true);
        ObNumber left_nmb(result_datum.get_number());
      } else {
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        const bool strict_mode = false;  // this is tmp allocator, so we can ues non-strinct mode
        ObNumber left_nmb(result_datum.get_number());
        ObNumber right_nmb(iter_value.get_number());
        ObNumber result_nmb;
        if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
          LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
        } else {
          ret = clone_number_cell(result_nmb, result_datum);
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("bot support now", K(column_tc));
    }
  }
  return ret;
}

int ObAggregateProcessor::rollup_add_number_calc(ObDatum& aggr_result, ObDatum& rollup_result)
{
  int ret = OB_SUCCESS;
  if (aggr_result.is_null()) {
    // do nothing
  } else if (rollup_result.is_null()) {
    if (OB_FAIL(clone_cell(rollup_result, aggr_result, true))) {
      LOG_WARN("clone_cell failed", K(ret), K(aggr_result));
    }
  } else {
    char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
    const bool strict_mode = false;  // this is tmp allocator, so we can ues non-strinct mode
    ObNumber left_nmb(aggr_result.get_number());
    ObNumber right_nmb(rollup_result.get_number());
    ObNumber result_nmb;
    if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
      LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
    } else if (OB_FAIL(clone_number_cell(result_nmb, rollup_result))) {
      LOG_WARN("clone_number_cell failed", K(ret), K(result_nmb));
    }
  }
  return ret;
}

int ObAggregateProcessor::rollup_add_calc(AggrCell& aggr_cell, AggrCell& rollup_cell, const ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum& aggr_result = aggr_cell.get_iter_result();
  ObDatum& rollup_result = rollup_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      if (aggr_cell.is_tiny_num_used()) {
        int64_t left_int = aggr_cell.get_tiny_num_int();
        int64_t right_int = rollup_cell.get_tiny_num_int();
        int64_t sum_int = left_int + right_int;
        if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
          LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
          char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
          ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
          ObNumber result_nmb;
          if (!rollup_result.is_null()) {
            ObCompactNumber& cnum = const_cast<ObCompactNumber&>(rollup_result.get_number());
            result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
          }
          if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
            LOG_WARN("number add failed", K(ret));
          } else if (OB_FAIL(clone_number_cell(result_nmb, rollup_result))) {
            LOG_WARN("clone_number_cell failed", K(ret));
          } else {
            rollup_cell.set_tiny_num_int(0);
          }
        } else {
          LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
          rollup_cell.set_tiny_num_int(sum_int);
          rollup_cell.set_tiny_num_used();
        }
      }
      if (OB_SUCC(ret)) {
        ret = rollup_add_number_calc(aggr_result, rollup_result);
      }
      break;
    }
    case ObUIntTC: {
      if (aggr_cell.is_tiny_num_used()) {
        uint64_t left_uint = aggr_cell.get_tiny_num_uint();
        uint64_t right_uint = rollup_cell.get_tiny_num_uint();
        uint64_t sum_uint = left_uint + right_uint;
        if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
          LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
          char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
          ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
          ObNumber result_nmb;
          if (!rollup_result.is_null()) {
            ObCompactNumber& cnum = const_cast<ObCompactNumber&>(rollup_result.get_number());
            result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
          }
          if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, allocator))) {
            LOG_WARN("number add failed", K(ret));
          } else if (OB_FAIL(clone_number_cell(result_nmb, rollup_result))) {
            LOG_WARN("clone_number_cell failed", K(ret));
          } else {
            rollup_cell.set_tiny_num_uint(0);
          }
        } else {
          LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
          rollup_cell.set_tiny_num_uint(sum_uint);
          rollup_cell.set_tiny_num_used();
        }
      }
      if (OB_SUCC(ret)) {
        ret = rollup_add_number_calc(aggr_result, rollup_result);
      }
      break;
    }
    case ObNumberTC: {
      ret = rollup_add_number_calc(aggr_result, rollup_result);
      break;
    }
    case ObFloatTC: {
      if (aggr_result.is_null()) {
        // do nothing
      } else if (rollup_result.is_null()) {
        ret = clone_cell(rollup_result, aggr_result, false);
      } else {
        float left_f = aggr_result.get_float();
        float right_f = rollup_result.get_float();
        if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))) {
          ret = OB_OPERATE_OVERFLOW;
          char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
          int64_t pos = 0;
          databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_f, right_f);
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
          LOG_WARN("float out of range", K(left_f), K(right_f));
        } else {
          rollup_result.set_float(left_f + right_f);
        }
      }
      break;
    }
    case ObDoubleTC: {
      if (aggr_result.is_null()) {
        // do nothing
      } else if (rollup_result.is_null()) {
        ret = clone_cell(rollup_result, aggr_result, false);
      } else {
        double left_d = aggr_result.get_double();
        double right_d = rollup_result.get_double();
        if (OB_UNLIKELY(ObArithExprOperator::is_double_out_of_range(left_d + right_d))) {
          ret = OB_OPERATE_OVERFLOW;
          char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
          int64_t pos = 0;
          databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%le + %le)'", left_d, right_d);
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
          LOG_WARN("double out of range", K(left_d), K(right_d), K(ret));
        } else {
          rollup_result.set_double(left_d + right_d);
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("bot support now", K(column_tc));
    }
  }
  return ret;
}

int ObAggregateProcessor::llc_add(ObDatum& result, const ObDatum& new_value)
{
  int ret = OB_SUCCESS;
  ObString res_buf = result.get_string();
  ObString left_buf = result.get_string();
  ;
  ObString right_buf = new_value.get_string();
  ;
  if (OB_UNLIKELY(left_buf.length() < get_llc_size()) || OB_UNLIKELY(right_buf.length() < get_llc_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size don't match", K(left_buf.length()), K(right_buf.length()), K(ret));
  } else {
    // TODO::here we can use SSE
    for (int64_t i = 0; i < get_llc_size(); ++i) {
      res_buf.ptr()[i] = std::max(static_cast<uint8_t>(left_buf[i]), static_cast<uint8_t>(right_buf[i]));
    }
  }
  return ret;
}

int ObAggregateProcessor::get_llc_size()
{
  return sizeof(char) * LLC_NUM_BUCKETS;
}

int ObAggregateProcessor::llc_add_value(const uint64_t value, const ObString& llc_bitmap_buf)
{
  int ret = OB_SUCCESS;
  uint64_t bucket_index = value >> (64 - LLC_BUCKET_BITS);
  const uint64_t pmax = ObExprEstimateNdv::llc_leading_zeros(value << LLC_BUCKET_BITS, 64 - LLC_BUCKET_BITS) + 1;
  ObString::obstr_size_t llc_num_buckets = llc_bitmap_buf.length();
  if (OB_UNLIKELY(llc_bitmap_buf.length() != get_llc_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size don't match", K(llc_bitmap_buf.length()));
  } else if (OB_UNLIKELY(!ObExprEstimateNdv::llc_is_num_buckets_valid(llc_num_buckets)) ||
             OB_UNLIKELY(llc_num_buckets <= bucket_index)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "llc_add_value failed because number of buckets is not valid", K(llc_num_buckets), K(bucket_index), K(ret));
  } else if (pmax > static_cast<uint8_t>(llc_bitmap_buf[bucket_index])) {
    // pmax never exceed 65
    (const_cast<ObString&>(llc_bitmap_buf)).ptr()[bucket_index] = static_cast<uint8_t>(pmax);
  }
  return ret;
}

int ObAggregateProcessor::llc_init(ObDatum& target_datum)
{
  int ret = OB_SUCCESS;
  char llc_bitmap_buf[sizeof(char) * LLC_NUM_BUCKETS] = {};
  ObDatum src_datum;
  src_datum.set_string(llc_bitmap_buf, sizeof(char) * LLC_NUM_BUCKETS);
  ret = clone_cell(target_datum, src_datum);
  return ret;
}

int ObAggregateProcessor::llc_init_empty(ObExpr& expr, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  char* llc_bitmap_buf = NULL;
  const int64_t llc_bitmap_size = get_llc_size();
  if (OB_ISNULL(llc_bitmap_buf = expr.get_str_res_mem(eval_ctx_, llc_bitmap_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc", K(llc_bitmap_size), K(ret));
  } else {
    MEMSET(llc_bitmap_buf, 0, llc_bitmap_size);
    ObDatum& datum = expr.locate_datum_for_write(eval_ctx);
    expr.get_eval_info(eval_ctx).evaluated_ = true;
    datum.set_string(llc_bitmap_buf, llc_bitmap_size);
  }
  return ret;
}

uint64_t ObAggregateProcessor::llc_calc_hash_value(
    const ObChunkDatumStore::StoredRow& stored_row, const ObIArray<ObExpr*>& param_exprs, bool& has_null_cell)
{
  has_null_cell = false;
  uint64_t hash_value = 0;
  for (int64_t i = 0; !has_null_cell && i < stored_row.cnt_; ++i) {
    const ObExpr& expr = *param_exprs.at(i);
    const ObDatum& datum = stored_row.cells()[i];
    if (datum.is_null()) {
      has_null_cell = true;
    } else {
      OB_ASSERT(NULL != expr.basic_funcs_);
      ObExprHashFuncType hash_func = expr.basic_funcs_->default_hash_;
      hash_value = hash_func(datum, hash_value);
    }
  }
  return hash_value;
}

int ObAggregateProcessor::compare_calc(const ObDatum& left_value, const ObDatum& right_value,
    const ObAggrInfo& aggr_info, int64_t index, int& compare_result, bool& is_asc)
{
  int ret = OB_SUCCESS;
  is_asc = false;
  if (OB_UNLIKELY(index >= aggr_info.sort_collations_.count() || index >= aggr_info.sort_cmp_funcs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument",
        K(index),
        K(aggr_info.sort_collations_.count()),
        K(aggr_info.sort_cmp_funcs_.count()),
        K(ret));
  } else {
    compare_result = aggr_info.sort_cmp_funcs_.at(index).cmp_func_(left_value, right_value);
    is_asc = aggr_info.sort_collations_.at(index).is_ascending_;
  }
  return ret;
}

int ObAggregateProcessor::check_rows_equal(const ObChunkDatumStore::LastStoredRow& prev_row,
    const ObChunkDatumStore::StoredRow& cur_row, const ObAggrInfo& aggr_info, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (aggr_info.sort_collations_.empty()) {
    // %sort_columns_ is empty if order by const value, set is_equal to true directly.
    // pre_sort_columns_.store_row_ is NULL here.
    is_equal = true;
  } else if (OB_ISNULL(prev_row.store_row_) || OB_UNLIKELY(prev_row.store_row_->cnt_ != cur_row.cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(prev_row.store_row_), K(cur_row.cnt_));
  } else {
    is_equal = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < aggr_info.sort_collations_.count(); ++i) {
      uint32_t index = aggr_info.sort_collations_.at(i).field_idx_;
      if (OB_UNLIKELY(index >= prev_row.store_row_->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid argument", K(ret), K(index), K(prev_row.store_row_->cnt_));
      } else {
        is_equal =
            0 == aggr_info.sort_cmp_funcs_.at(i).cmp_func_(prev_row.store_row_->cells()[index], cur_row.cells()[index]);
      }
    }
  }
  return ret;
}

// When there is stored_row_ reserved_cells, use stored_row_'s reserved_cells_ for calc equal.
// Other use row_.
int ObGroupRowHashTable::init(ObIAllocator* allocator, lib::ObMemAttr& mem_attr, ObEvalCtx* eval_ctx,
    const common::ObIArray<ObCmpFunc>* cmp_funcs, ObSqlMemMgrProcessor *sql_mem_processor,
    int64_t initial_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExtendHashTable<ObGroupRowItem>::init(allocator, mem_attr,
                                                     sql_mem_processor, initial_size))) {
    LOG_WARN("failed to init extended hash table", K(ret));
  } else {
    eval_ctx_ = eval_ctx;
    cmp_funcs_ = cmp_funcs;
  }
  return ret;
}

bool ObGroupRowHashTable::compare(const ObGroupRowItem& left, const ObGroupRowItem& right) const
{
  int ret = OB_SUCCESS;
  bool result = true;
  ObDatum* l_cells = left.group_row_->groupby_store_row_->cells();
  // hash group by const if group_col_count lt 0, return true
  for (int64_t i = 0; OB_SUCC(ret) && result && i < right.group_exprs_->count(); ++i) {
    ObDatum& left_datum = l_cells[i];
    ObExpr*& expr = right.group_exprs_->at(i);
    ObDatum* right_datum = NULL;
    if (OB_FAIL(expr->eval(*eval_ctx_, right_datum))) {
      LOG_WARN("eval failed", K(ret));
    } else {
      result = (0 == cmp_funcs_->at(i).cmp_func_(left_datum, *right_datum));
    }
    if (OB_FAIL(ret)) {
      result = false;
    }
  }
  return result;
}

const ObGroupRowItem* ObGroupRowHashTable::get(const ObGroupRowItem& item) const
{
  ObGroupRowItem* res = NULL;
  if (OB_UNLIKELY(NULL == buckets_)) {
    // do nothing
  } else {
    const uint64_t hash_val = item.hash();
    ObGroupRowItem* bucket = buckets_->at(hash_val & (get_bucket_num() - 1));
    while (NULL != bucket) {
      if (hash_val == bucket->hash() && compare(*bucket, item)) {
        res = bucket;
        break;
      }
      bucket = bucket->next();
    }
  }
  return res;
}

int ObAggregateCalcFunc::add_calc(const ObDatum& left_value, const ObDatum& right_value, ObDatum& result_datum,
    const ObObjTypeClass type, ObIAllocator& out_allocator)
{
  int ret = OB_SUCCESS;
  if (left_value.is_null() && right_value.is_null()) {
    result_datum.set_null();
  } else {
    switch (type) {
      case ObIntTC: {
        if (left_value.is_null()) {
          result_datum.set_int(right_value.get_int());
        } else if (right_value.is_null()) {
          result_datum.set_int(left_value.get_int());
        } else {
          int64_t left_int = left_value.get_int();
          int64_t right_int = right_value.get_int();
          int64_t sum_int = left_int + right_int;
          if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
            LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
            char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
            ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
            ObNumber result_nmb;
            if (!result_datum.is_null()) {
              ObCompactNumber& cnum = const_cast<ObCompactNumber&>(result_datum.get_number());
              result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
            }
            if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
              LOG_WARN("number add failed", K(ret));
            } else if (OB_FAIL(clone_number_cell(result_nmb, result_datum, out_allocator))) {
              LOG_WARN("clone_number_cell failed", K(ret));
            }
          } else {
            LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
            result_datum.set_int(sum_int);
          }
        }
        break;
      }
      case ObUIntTC: {
        if (left_value.is_null()) {
          result_datum.set_uint(right_value.get_uint());
        } else if (right_value.is_null()) {
          result_datum.set_uint(left_value.get_uint());
        } else {
          uint64_t left_uint = left_value.get_uint();
          uint64_t right_uint = right_value.get_uint();
          uint64_t sum_uint = left_uint + right_uint;
          if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
            LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
            char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
            ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
            ObNumber result_nmb;
            if (!result_datum.is_null()) {
              ObCompactNumber& cnum = const_cast<ObCompactNumber&>(result_datum.get_number());
              result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
            }
            if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, out_allocator))) {
              LOG_WARN("number add failed", K(ret));
            } else if (OB_FAIL(clone_number_cell(result_nmb, result_datum, out_allocator))) {
              LOG_WARN("clone_number_cell failed", K(ret));
            }
          } else {
            LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
            result_datum.set_uint(sum_uint);
          }
        }
        break;
      }
      case ObFloatTC: {
        if (left_value.is_null()) {
          result_datum.set_float(right_value.get_float());
        } else if (right_value.is_null()) {
          result_datum.set_float(left_value.get_float());
        } else {
          float left_f = left_value.get_float();
          float right_f = right_value.get_float();
          if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))) {
            ret = OB_OPERATE_OVERFLOW;
            char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
            int64_t pos = 0;
            databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_f, right_f);
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
            LOG_WARN("float out of range", K(left_f), K(right_f));
          } else {
            result_datum.set_float(left_f + right_f);
          }
        }
        break;
      }
      case ObDoubleTC: {
        if (left_value.is_null()) {
          result_datum.set_double(right_value.get_double());
        } else if (right_value.is_null()) {
          result_datum.set_double(left_value.get_double());
        } else {
          double left_d = left_value.get_double();
          double right_d = right_value.get_double();
          if (OB_UNLIKELY(ObArithExprOperator::is_double_out_of_range(left_d + right_d))) {
            ret = OB_OPERATE_OVERFLOW;
            char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
            int64_t pos = 0;
            databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%le + %le)'", left_d, right_d);
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
            LOG_WARN("double out of range", K(left_d), K(right_d), K(ret));
          } else {
            result_datum.set_double(left_d + right_d);
          }
        }
        break;
      }
      case ObNumberTC: {
        if (left_value.is_null()) {
          if (OB_FAIL(clone_number_cell(right_value.get_number(),
              result_datum, out_allocator))) {
            LOG_WARN("fail to clone number cell", K(ret));
          }
        } else if (right_value.is_null()) {
          if (OB_FAIL(clone_number_cell(left_value.get_number(),
              result_datum, out_allocator))) {
            LOG_WARN("fail to clone number cell", K(ret));
          }
        } else {
          char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
          ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
          const bool strict_mode = false;  // this is tmp allocator, so we can ues non-strinct mode
          ObNumber left_nmb(left_value.get_number());
          ObNumber right_nmb(right_value.get_number());
          ObNumber result_nmb;
          if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
            LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
          } else {
            ret = clone_number_cell(result_nmb, result_datum, out_allocator);
          }
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("bot support now", K(type));
        break;
      }
    }
  }
  return ret;
}

int ObAggregateCalcFunc::clone_number_cell(
    const number::ObNumber& src_number, ObDatum& target_cell, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  int64_t need_size = ObNumber::MAX_BYTE_LEN;
  char* buff_ptr = NULL;
  if (OB_ISNULL(buff_ptr = static_cast<char*>(allocator.alloc(need_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }

  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    target_cell.ptr_ = buff_ptr;
    target_cell.set_number(src_number);
    LOG_DEBUG("succ to clone cell", K(src_number), K(target_cell));
  }
  return ret;
}

int ObAggregateProcessor::get_wm_concat_result(
    const ObAggrInfo& aggr_info, GroupConcatExtraResult*& extra, bool is_keep_group_concat, ObDatum& concat_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    ObString sep_str = ObCharsetUtils::get_const_str(aggr_info.expr_->datum_meta_.cs_type_, ',');
    ObChunkDatumStore::LastStoredRow first_row(aggr_alloc_);
    const ObChunkDatumStore::StoredRow* storted_row = NULL;
    bool is_first = true;
    bool need_continue = true;
    int64_t str_len = 0;
    char* buf = NULL;
    int64_t buf_len = 0;
    char* tmp_buf = NULL;
    while (OB_SUCC(ret) && need_continue && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else if (is_keep_group_concat) {
        if (is_first && OB_FAIL(first_row.save_store_row(*storted_row))) {
          LOG_WARN("failed to deep copy limit last rows", K(ret));
        } else if (!is_first && OB_FAIL(check_rows_equal(first_row, *storted_row, aggr_info, need_continue))) {
          LOG_WARN("failed to is order by item equal with prev row", K(ret));
        } else {
          is_first = false;
        }
      }
      if (OB_SUCC(ret) && need_continue) {
        if (storted_row->cells()[0].is_null()) {
        } else {
          const ObString cell_string = storted_row->cells()[0].get_string();
          if (cell_string.length() > 0) {
            int64_t append_len = cell_string.length() + sep_str.length() + str_len;
            if (OB_UNLIKELY(append_len > OB_MAX_PACKET_LENGTH)) {
              ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
              LOG_WARN("result of string concatenation is too long", K(ret), K(append_len), K(OB_MAX_PACKET_LENGTH));
            } else if (buf_len < append_len) {
              char* tmp_buf = NULL;
              buf_len = append_len * 2;
              if (OB_ISNULL(tmp_buf = static_cast<char*>(aggr_alloc_.alloc(buf_len)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail alloc memory", K(tmp_buf), K(buf_len), K(ret));
              } else {
                MEMCPY(tmp_buf, buf, str_len);
                buf = tmp_buf;
              }
            }
            if (OB_SUCC(ret)) {
              MEMCPY(buf + str_len, cell_string.ptr(), cell_string.length());
              str_len += cell_string.length();
              MEMCPY(buf + str_len, sep_str.ptr(), sep_str.length());
              str_len += sep_str.length();
            }
          }
        }
      }
    }  // end of while
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      str_len = str_len == 0 ? str_len : str_len - sep_str.length();
      if (str_len > 0) {
        ObString res_str;
        res_str.assign(buf, str_len);
        ObLobLocator* result = nullptr;
        char* total_buf = NULL;
        const int64_t total_buf_len = sizeof(ObLobLocator) + str_len;
        if (OB_ISNULL(total_buf = aggr_info.expr_->get_str_res_mem(eval_ctx_, total_buf_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(buf_len));
        } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator*>(total_buf))) {
        } else if (OB_FAIL(result->init(res_str))) {
          LOG_WARN("Failed to init lob locator", K(ret), K(res_str), K(result));
        } else {
          concat_result.set_lob_locator(*result);
        }
      } else {
        concat_result.set_null();  // set null if all skiped
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::convert_datum_to_obj(const ObAggrInfo &aggr_info,
                                               const ObChunkDatumStore::StoredRow &stored_row,
                                               ObObj *tmp_obj,
                                               int64_t obj_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj_cnt != stored_row.cnt_ || obj_cnt != aggr_info.param_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(obj_cnt), K(stored_row.cnt_),
                                     K(aggr_info.param_exprs_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_row.cnt_; ++i) {
      if (OB_FAIL(stored_row.cells()[i].to_obj(tmp_obj[i],
                                               aggr_info.param_exprs_.at(i)->obj_meta_))) {
        LOG_WARN("failed to obj", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObAggregateProcessor::get_json_arrayagg_result(const ObAggrInfo &aggr_info,
                                                   GroupConcatExtraResult *&extra,
                                                   ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc;
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    ObJsonArray json_array(&tmp_alloc);
    bool is_bool = false;
    if (OB_FAIL(extra->get_bool_mark(0, is_bool))) {
      LOG_WARN("get_bool info failed, may not distinguish between bool and int", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else {
        // get type
        ObObj *tmp_obj = NULL;
        if (OB_ISNULL(tmp_obj = static_cast<ObObj*>(tmp_alloc.alloc(
                                                    sizeof(ObObj) * (storted_row->cnt_))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(tmp_obj));
        } else if (OB_FAIL(convert_datum_to_obj(aggr_info, *storted_row, tmp_obj, storted_row->cnt_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (storted_row->cnt_ < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected column count", K(ret), K(storted_row));
        } else {
          ObObjType val_type = tmp_obj->get_type();
          ObCollationType cs_type = tmp_obj->get_collation_type();
          ObScale scale = tmp_obj->get_scale();
          ObIJsonBase *json_val = NULL;
          ObDatum converted_datum;
          converted_datum.set_datum(storted_row->cells()[0]);
          // convert string charset if needed
          if (ob_is_string_type(val_type) 
              && (ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4)) {
            ObString origin_str = converted_datum.get_string();
            ObString converted_str;
            if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type, converted_str, 
                                                             CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
              LOG_WARN("convert string collation failed", K(ret), K(cs_type), K(origin_str.length()));
            } else {
              converted_datum.set_string(converted_str);
              cs_type = CS_TYPE_UTF8MB4_BIN;
            }
          }

          // get json value
          if (OB_FAIL(ret)) {
          } else if (is_bool) {
            void *json_node_buf = tmp_alloc.alloc(sizeof(ObJsonBoolean));
            if (OB_ISNULL(json_node_buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed: allocate jsonboolean", K(ret));
            } else {
              ObJsonBoolean *bool_node = (ObJsonBoolean*)new(json_node_buf)ObJsonBoolean(converted_datum.get_bool());
              json_val = bool_node;
            }
          } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
            if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(converted_datum, val_type,
                                                                          &tmp_alloc, cs_type,
                                                                          json_val, false, true))) {
              LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
            }
          } else {
            if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(converted_datum, val_type,
                                                                    &tmp_alloc, scale,
                                                                    eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                    json_val, false))) {
              LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(json_array.array_append(json_val))) {
            LOG_WARN("failed: json array append json value", K(ret));
          } else if(json_array.get_serialize_size() > OB_MAX_PACKET_LENGTH) {
            ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
            LOG_WARN("result of json_arrayagg is too long", K(ret), K(json_array.get_serialize_size()),
                                                            K(OB_MAX_PACKET_LENGTH));
          }
        }
      }
    }//end of while
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObString str;
      // output res
      if (OB_FAIL(json_array.get_raw_binary(str, &aggr_alloc_))) {
        LOG_WARN("get result binary failed", K(ret));
      } else {
        concat_result.set_string(str);
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::get_json_objectagg_result(const ObAggrInfo &aggr_info,
                                                    GroupConcatExtraResult *&extra,
                                                    ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  const int col_num = 2;
  common::ObArenaAllocator tmp_alloc;
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    ObJsonObject json_object(&tmp_alloc);
    ObObj tmp_obj[col_num];
    bool is_bool = false;
    if (OB_FAIL(extra->get_bool_mark(1, is_bool))) {
      LOG_WARN("get_bool info failed, may not distinguish between bool and int", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else if(storted_row->cnt_ != col_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected column count", K(ret), K(storted_row->cnt_));
      } else {
        // get obj
        if (OB_FAIL(convert_datum_to_obj(aggr_info, *storted_row, tmp_obj, storted_row->cnt_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (tmp_obj[0].get_type() == ObNullType) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_WARN("null type for json_objectagg key");
        } else if (ob_is_string_type(tmp_obj[0].get_type()) 
                   && tmp_obj[0].get_collation_type() == CS_TYPE_BINARY) {
          // not support binary charset as mysql 
          LOG_WARN("unsuport json string type with binary charset", 
              K(tmp_obj[0].get_type()), K(tmp_obj[0].get_collation_type()));
          ret = OB_ERR_INVALID_JSON_CHARSET;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
        } else if (NULL == tmp_obj[0].get_string_ptr()) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("unexpected null result", K(ret), K(tmp_obj[0]));
        } else {
          ObObjType val_type0 = tmp_obj[0].get_type();
          ObCollationType cs_type0 = tmp_obj[0].get_collation_type();
          ObObjType val_type1 = tmp_obj[1].get_type();
          ObScale scale1 = tmp_obj[1].get_scale();
          ObCollationType cs_type1 = tmp_obj[1].get_collation_type();
          ObString key_string = tmp_obj[0].get_string();
          if (OB_SUCC(ret) && ObCharset::charset_type_by_coll(cs_type0) != CHARSET_UTF8MB4) {
            ObString converted_key_str;
            if (OB_FAIL(ObExprUtil::convert_string_collation(key_string, cs_type0, converted_key_str, 
                                                                  CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
              LOG_WARN("convert key string collation failed", K(ret), K(cs_type0), K(key_string.length()));
            } else {
              key_string = converted_key_str;
            }
          }

          // get key and value, and append to json_object
          ObString key_data;
          ObIJsonBase *json_val = NULL;
          if (OB_SUCC(ret) && OB_FAIL(deep_copy_ob_string(tmp_alloc, key_string, key_data))) {
            LOG_WARN("fail copy string", K(ret), K(key_string.length()));
          } else {
            ObDatum converted_datum;
            converted_datum.set_datum(storted_row->cells()[1]);
            if (ob_is_string_type(val_type1) 
            && (ObCharset::charset_type_by_coll(cs_type1) != CHARSET_UTF8MB4)) {
              ObString origin_str = converted_datum.get_string();
              ObString converted_str;
              if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type1, converted_str, 
                                                              CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
                LOG_WARN("convert string collation failed", K(ret), K(cs_type1), K(origin_str.length()));
              } else {
                converted_datum.set_string(converted_str);
                cs_type1 = CS_TYPE_UTF8MB4_BIN;
              }
            }

            if (OB_FAIL(ret)) {
            } else if (is_bool) {
              void *json_node_buf = tmp_alloc.alloc(sizeof(ObJsonBoolean));
              if (OB_ISNULL(json_node_buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed: allocate jsonboolean", K(ret));
              } else {
                ObJsonBoolean *bool_node = (ObJsonBoolean*)new(json_node_buf)ObJsonBoolean(storted_row->cells()[1].get_bool());
                json_val = bool_node;
              }
            } else if (ObJsonExprHelper::is_convertible_to_json(val_type1)) {
              if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(converted_datum, val_type1,
                                                                            &tmp_alloc, cs_type1,
                                                                            json_val, false, true))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
              }
            } else {
              if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(converted_datum, val_type1,
                                                                      &tmp_alloc, scale1,
                                                                      eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                      json_val, false))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(json_object.object_add(key_data, json_val))) {
              LOG_WARN("failed: json object add json value", K(ret));
            } else if(json_object.get_serialize_size() > OB_MAX_PACKET_LENGTH) {
              ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
              LOG_WARN("result of json_objectagg is too long", K(ret), K(json_object.get_serialize_size()),
                                                                K(OB_MAX_PACKET_LENGTH));
            }
          }
        }
      }
    }//end of while
    if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
    }
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObString str;
      // output res
      if (OB_FAIL(json_object.get_raw_binary(str, &aggr_alloc_))) {
        LOG_WARN("get result binary failed", K(ret));
      } else {
        concat_result.set_string(str);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
