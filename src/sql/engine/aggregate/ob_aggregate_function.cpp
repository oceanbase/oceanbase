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
#include "sql/engine/aggregate/ob_aggregate_function.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_less_than.h"
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_mul.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

namespace oceanbase {
using namespace common;
namespace sql {
uint64_t ObAggregateDistinctItem::hash() const
{
  uint64_t hash_id = 0;
  if (OB_ISNULL(cells_) || OB_ISNULL(cs_type_list_)) {
    LOG_ERROR("cells or cs type list is null");
  } else {
    hash_id = group_id_ + col_idx_;
    for (int64_t i = 0; i < cs_type_list_->count(); ++i) {
      hash_id = (cells_[i].is_string_type() ? cells_[i].varchar_hash(cs_type_list_->at(i), hash_id)
                                            : cells_[i].hash(hash_id));
    }
  }
  return hash_id;
}

bool ObAggregateDistinctItem::operator==(const ObAggregateDistinctItem& other) const
{
  bool bool_ret = true;
  if (OB_ISNULL(cells_) || OB_ISNULL(cs_type_list_)) {
    LOG_ERROR("cells or cs type list is null");
  } else {
    bool_ret = (group_id_ == other.group_id_ && col_idx_ == other.col_idx_);
    if (bool_ret && cs_type_list_->count() != other.cs_type_list_->count()) {
      bool_ret = false;
    }
    for (int64_t i = 0; bool_ret && i < cs_type_list_->count(); ++i) {
      if (cells_[i].compare(other.cells_[i], cs_type_list_->at(i)) != 0) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

ObAggCellCtx::ObAggCellCtx(ObIAllocator& alloc) : distinct_set_(NULL), alloc_(alloc)
{}

void ObAggCellCtx::reuse()
{
  if (NULL != distinct_set_) {
    distinct_set_->reuse();
  }
}

ObAggCellCtx::~ObAggCellCtx()
{
  if (NULL != distinct_set_) {
    distinct_set_->~ObUniqueSort();
    alloc_.free(distinct_set_);
    distinct_set_ = NULL;
  }
  if (NULL != sort_columns_.get_base_address()) {
    for (int64_t i = 0; i < sort_columns_.count(); i++) {
      sort_columns_.get_base_address()[i].~ObSortColumn();
    }
    alloc_.free(sort_columns_.get_base_address());
    sort_columns_.reset();
  }
}

int ObAggCellCtx::init_distinct_set(const uint64_t tenant_id, const common::ObIArray<common::ObCollationType>& cs_types,
    const int64_t sort_col_cnt, const bool need_rewind)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || sort_col_cnt <= 0 || sort_col_cnt > cs_types.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(sort_col_cnt), K(cs_types.count()));
  } else {
    ObSortColumn* cols = static_cast<ObSortColumn*>(alloc_.alloc(sizeof(ObSortColumn) * sort_col_cnt));
    if (NULL == cols) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      for (int64_t i = 0; i < sort_col_cnt; i++) {
        new (&cols[i]) ObSortColumn(i, cs_types.at(i), true /* asc order */);
      }
      sort_columns_.init(sort_col_cnt, cols, sort_col_cnt);
      distinct_set_ = static_cast<ObUniqueSort*>(alloc_.alloc(sizeof(*distinct_set_)));
      if (NULL == distinct_set_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(tenant_id), K(sort_col_cnt), K(cs_types.count()));
      } else {
        new (distinct_set_) ObUniqueSort();
        if (OB_FAIL(distinct_set_->init(tenant_id, sort_columns_, need_rewind))) {
          LOG_WARN("init distinct set failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCCESS != ret) {
    if (NULL != distinct_set_) {
      distinct_set_->~ObUniqueSort();
      alloc_.free(distinct_set_);
      distinct_set_ = NULL;
    }
    if (NULL != sort_columns_.get_base_address()) {
      for (int64_t i = 0; i < sort_columns_.count(); i++) {
        sort_columns_.get_base_address()[i].~ObSortColumn();
      }
      alloc_.free(sort_columns_.get_base_address());
      sort_columns_.reset();
    }
  }
  return ret;
}

ObGroupConcatRowStore::ObGroupConcatRowStore() : need_sort_(false), rows_(0), iter_idx_(0)
{}

ObGroupConcatRowStore::~ObGroupConcatRowStore()
{}

int ObGroupConcatRowStore::init(const uint64_t tenant_id, const ObIArray<ObSortColumn>& sort_columns,
    const ObSortImpl::SortExtraInfos* extra_infos, const bool rewind, int64_t dir_id)
{
  int ret = OB_SUCCESS;
  rows_ = 0;
  iter_idx_ = 0;
  if (sort_columns.empty()) {
    int64_t sort_area_size = 0;
    if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(SORT_WORK_AREA, tenant_id, sort_area_size))) {
      LOG_WARN("failed to get workarea size", K(ret), K(tenant_id));
    } else if (OB_FAIL(rs_.init(sort_area_size,
                   tenant_id,
                   ObCtxIds::WORK_AREA,
                   ObModIds::OB_SQL_AGGR_FUN_GROUP_CONCAT,
                   true /* enable dump */,
                   ObChunkRowStore::FULL))) {
      LOG_WARN("row store failed", K(ret));
    } else {
      rs_.set_dir_id(dir_id);
    }
    need_sort_ = false;
  } else {
    if (OB_FAIL(sort_.init(tenant_id, sort_columns, extra_infos, false /* local order */, rewind))) {
      LOG_WARN("sort columns failed", K(ret));
    }
    need_sort_ = true;
  }
  return ret;
}

void ObGroupConcatRowStore::reuse()
{
  if (need_sort_) {
    sort_.reuse();
  } else {
    rs_it_.reset();
    rs_.reuse();
  }
  rows_ = 0;
  iter_idx_ = 0;
}

int ObGroupConcatRowStore::finish_add_row()
{
  iter_idx_ = 0;
  int ret = OB_SUCCESS;
  if (need_sort_) {
    ret = sort_.sort();
  } else {
    rs_it_.reset();
    ret = rs_it_.init(&rs_, ObChunkRowStore::BLOCK_SIZE);
  }
  return ret;
}

int ObGroupConcatRowStore::rewind()
{
  int ret = OB_SUCCESS;
  if (need_sort_) {
    if (OB_FAIL(sort_.rewind())) {
      LOG_WARN("rewind failed", K(ret));
    }
  } else {
    rs_it_.reset();
    if (OB_FAIL(rs_it_.init(&rs_, ObChunkRowStore::BLOCK_SIZE))) {
      LOG_WARN("row store iterator init failed", K(ret));
    }
  }
  iter_idx_ = 0;
  return ret;
}

void ObGroupConcatCtx::reuse()
{
  if (NULL != gc_rs_) {
    gc_rs_->reuse();
  }

  ObAggCellCtx::reuse();
}

ObGroupConcatCtx::~ObGroupConcatCtx()
{
  if (NULL != gc_rs_) {
    gc_rs_->~ObGroupConcatRowStore();
    alloc_.free(gc_rs_);
    gc_rs_ = NULL;
  }
}

ObAggregateFunction::ObAggregateFunction()
    : has_distinct_(false),
      has_sort_(false),
      is_sort_based_gby_(false),
      in_window_func_(false),
      aggr_fun_need_cell_ctx_(false),
      agg_cell_ctx_cnt_(0),
      output_column_count_(0),
      full_column_count_(0),
      did_int_div_as_double_(false),
      stored_row_buf_(ObModIds::OB_SQL_AGGR_FUNC_ROW, common::OB_MALLOC_MIDDLE_BLOCK_SIZE, OB_SERVER_TENANT_ID,
          ObCtxIds::WORK_AREA),
      expr_ctx_(),
      child_column_count_(0),
      aggr_distinct_set_(),
      row_array_(),
      agg_cell_ctx_alloc_(ObModIds::OB_SQL_AGGR_CELL_CTX, common::OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID,
          ObCtxIds::WORK_AREA),
      group_concat_max_len_(OB_DEFAULT_GROUP_CONCAT_MAX_LEN),
      alloc_(ObModIds::OB_SQL_AGGR_FUNC, common::OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      input_cells_(NULL),
      gconcat_cur_row_num_(0),
      first_rollup_cols_(),
      agg_udf_buf_(ObModIds::OB_SQL_UDF),
      agg_udf_metas_(),
      agg_udf_(),
      dir_id_(-1)
{
  if (share::is_oracle_mode()) {
    group_concat_max_len_ = OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE;
  }
}

ObAggregateFunction::~ObAggregateFunction()
{
  destroy();
}

// select the position where the column appears for the first time as a mark,
// and the first column shall prevail in subsequent processing.
int ObAggregateFunction::init_first_rollup_cols(
    common::ObIAllocator* alloc, const ObIArray<ObColumnInfo>& group_idxs, const ObIArray<ObColumnInfo>& rollup_idxs)
{
  int64_t ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 16> no_dup_group_col_idxs;
  common::ObSEArray<int64_t, 16> no_dup_rollup_col_idxs;
  first_rollup_cols_.set_allocator(alloc);
  if (OB_FAIL(first_rollup_cols_.init(group_idxs.count() + rollup_idxs.count()))) {
    LOG_WARN("failed to init the first rollup cols.", K(ret));
  } else { /*do nothing.*/
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group_idxs.count(); i++) {
    if (OB_FAIL(no_dup_group_col_idxs.push_back(group_idxs.at(i).index_))) {
      LOG_WARN("failed to push back into group col idxs.", K(ret));
    } else if (OB_FAIL(first_rollup_cols_.push_back(false))) {
      LOG_WARN("failed to push back cols.", K(ret));
    } else { /*do nothing.*/
    }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < rollup_idxs.count(); j++) {
    if (is_contain(no_dup_group_col_idxs, rollup_idxs.at(j).index_)) {
      if (OB_FAIL(first_rollup_cols_.push_back(false))) {
        LOG_WARN("failed to push back cols.", K(ret));
      } else { /*do nothing.*/
      }
    } else if (is_contain(no_dup_rollup_col_idxs, rollup_idxs.at(j).index_)) {
      if (OB_FAIL(first_rollup_cols_.push_back(false))) {
        LOG_WARN("failed to push back cols.", K(ret));
      } else { /*do nothing.*/
      }
    } else if (OB_FAIL(no_dup_rollup_col_idxs.push_back(rollup_idxs.at(j).index_))) {
      LOG_WARN("failed to push back cols.", K(ret));
    } else if (OB_FAIL(first_rollup_cols_.push_back(true))) {
      LOG_WARN("failed to push back cols.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObAggregateFunction::init(const int64_t input_column_count, const ObAggrExprList* aggr_columns, ObExprCtx& expr_ctx,
    int32_t prepare_row_num, int64_t distinct_set_bucket_num)
{
  int ret = OB_SUCCESS;
  ObItemType aggr_fun;
  bool is_distinct = false;
  int64_t aggr_count = 0;
  ObCollationType tmp_cs_type = CS_TYPE_INVALID;

  gconcat_cur_row_num_ = 0;
  // copy reference of aggr_column
  full_column_count_ = input_column_count;
  output_column_count_ = input_column_count;
  child_column_count_ = input_column_count;
  row_array_.reserve(prepare_row_num);

  if (OB_ISNULL(expr_ctx.phy_plan_ctx_) || OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr_ctx is valid", K_(expr_ctx.phy_plan_ctx), K_(expr_ctx.my_session), K_(expr_ctx.calc_buf));
  } else {
    expr_ctx_ = expr_ctx;
    if (share::is_oracle_mode()) {
      group_concat_max_len_ = OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE;
    } else if (OB_FAIL(expr_ctx.my_session_->get_group_concat_max_len(group_concat_max_len_))) {
      LOG_WARN("fail to get group concat max len", K(ret));
    } else if (group_concat_max_len_ >= CONCAT_STR_BUF_LEN - 1) {
      group_concat_max_len_ = CONCAT_STR_BUF_LEN - 2;
    }
  }
  // add aggr columns
  agg_cell_ctx_cnt_ = 0;
  agg_columns_.reuse();
  DLIST_FOREACH(node, *aggr_columns)
  {
    ++output_column_count_;
    ++full_column_count_;
    const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node);
    if (OB_ISNULL(cexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(cexpr));
    } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, tmp_cs_type))) {
      LOG_WARN("failed to get aggr column", K(ret));
    } else {
      if (is_distinct) {
        has_distinct_ = true;
      }
      if (!cexpr->get_sort_columns().empty()) {
        has_sort_ = true;
      }
      switch (aggr_fun) {
        case T_FUN_GROUP_CONCAT:
        case T_FUN_GROUP_RANK:
        case T_FUN_GROUP_DENSE_RANK:
        case T_FUN_GROUP_PERCENT_RANK:
        case T_FUN_GROUP_CUME_DIST:
        case T_FUN_GROUP_PERCENTILE_CONT:
        case T_FUN_GROUP_PERCENTILE_DISC:
        case T_FUN_MEDIAN:
        case T_FUN_KEEP_MAX:
        case T_FUN_KEEP_MIN:
        case T_FUN_KEEP_SUM:
        case T_FUN_KEEP_COUNT:
        case T_FUN_KEEP_WM_CONCAT:
        case T_FUN_WM_CONCAT: 
        case T_FUN_JSON_ARRAYAGG:
        case T_FUN_JSON_OBJECTAGG: {
          aggr_fun_need_cell_ctx_ = true;
          break;
        }
        default:
          break;
      }
      if (T_FUN_AVG == aggr_fun || T_FUN_APPROX_COUNT_DISTINCT == aggr_fun) {
        ++full_column_count_;
      }
      if (aggr_count < cexpr->get_all_param_col_count()) {
        aggr_count = cexpr->get_all_param_col_count();
      }
      ExprCtxIdx pair;
      pair.expr_ = cexpr;
      // distinct ,group concat ,rank and so on need agg cell context
      if (is_distinct || aggr_fun_need_cell_ctx_) {
        pair.ctx_idx_ = agg_cell_ctx_cnt_;
        agg_cell_ctx_cnt_ += 1;
      }
      if (OB_FAIL(agg_columns_.push_back(pair))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }  // end for
  if (OB_SUCC(ret)) {
    if (aggr_count > 0) {
      void* tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = alloc_.alloc(aggr_count * sizeof(ObObj))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc input cells", K(ret), K(aggr_count));
      } else {
        input_cells_ = new (tmp_ptr) ObObj[aggr_count];
      }
    }
  }
  if (OB_SUCC(ret) && has_distinct_) {
    if (OB_FAIL(aggr_distinct_set_.create(distinct_set_bucket_num))) {
      LOG_WARN("fail to init aggr distinct set", K(ret));
    }
  }
  return ret;
}

void ObAggregateFunction::destroy()
{
  if (has_distinct_) {
    aggr_distinct_set_.destroy();
  }
  if (agg_cell_ctx_cnt_ > 0) {
    FOREACH_CNT(gr, row_array_)
    {
      if (NULL != gr->ctx_) {
        for (int64_t i = 0; i < agg_cell_ctx_cnt_; i++) {
          if (NULL != gr->ctx_[i]) {
            gr->ctx_[i]->~ObAggCellCtx();
            agg_cell_ctx_alloc_.free(gr->ctx_[i]);
            gr->ctx_[i] = NULL;
          }
        }
      }
    }
  }
  // here can not use reuse(),reuse() reuses memory,memory cannot be released,
  // which does not satisfy the semantics of destroy
  agg_cell_ctx_alloc_.reset();
  row_array_.destroy();
  stored_row_buf_.reset();
  if (NULL != input_cells_) {
    alloc_.free(input_cells_);
    input_cells_ = NULL;
  }
  alloc_.reset();
  if (agg_udf_.created()) {
    common::hash::ObHashMap<int64_t, ObAggUdfExeUnit, common::hash::NoPthreadDefendMode>::iterator iter =
        agg_udf_.begin();
    for (; iter != agg_udf_.end(); iter++) {
      ObAggUdfExeUnit& agg_unit = iter->second;
      if (OB_NOT_NULL(agg_unit.agg_func_) && OB_NOT_NULL(agg_unit.udf_ctx_)) {
        IGNORE_RETURN agg_unit.agg_func_->process_deinit_func(*agg_unit.udf_ctx_);
      }
    }
  }
  first_rollup_cols_.reset();
  agg_udf_.destroy();
  agg_udf_buf_.reset();
}

void ObAggregateFunction::reuse()
{
  if (agg_cell_ctx_cnt_ > 0) {
    FOREACH_CNT(gr, row_array_)
    {
      if (NULL != gr->ctx_) {
        for (int64_t i = 0; i < agg_cell_ctx_cnt_; i++) {
          if (NULL != gr->ctx_[i]) {
            gr->ctx_[i]->~ObAggCellCtx();
            // allocate from arena allocator, no need to free
            gr->ctx_[i] = NULL;
          }
        }
      }
    }
  }
  agg_cell_ctx_alloc_.reset_remain_one_page();
  row_array_.reuse();
  stored_row_buf_.reset_remain_one_page();
  agg_udf_buf_.reset_remain_one_page();
  if (aggr_distinct_set_.size() > 0) {
    aggr_distinct_set_.reuse();
  }
  agg_udf_.reuse();
}

int ObAggregateFunction::clone_cell(const ObObj& src_cell, ObObj& target_cell)
{
  int ret = OB_SUCCESS;
  if (ObNumberTC == src_cell.get_type_class()) {
    ret = clone_number_cell(src_cell, target_cell);
  } else if (OB_UNLIKELY(src_cell.need_deep_copy())) {
    char* buf = NULL;
    // length + magic num + data
    int64_t need_size = sizeof(int64_t) * 2 + src_cell.get_deep_copy_size();
    void* data_ptr = NULL;
    const int64_t data_length = target_cell.get_data_length();
    if (target_cell.get_type() == src_cell.get_type() &&
        NULL != (data_ptr = const_cast<void*>(target_cell.get_data_ptr())) && 0 != data_length) {
      int64_t curr_size = 0;
      if (OB_ISNULL((char*)data_ptr - sizeof(int64_t)) ||
          OB_ISNULL((char*)data_ptr - sizeof(int64_t) - sizeof(int64_t))) {
        ret = OB_ERR_UNEXPECTED;
        ;
        LOG_ERROR("clone_cell use stored_row_buf, need has meta", KP(data_ptr), K(ret));
      } else if (OB_UNLIKELY(*((int64_t*)(data_ptr)-1) != STORED_ROW_MAGIC_NUM)) {
        ret = OB_ERR_UNEXPECTED;
        ;
        LOG_ERROR("stored_row_buf memory is mismatch, maybe some one make bad things",
            "curr_magic",
            *((int64_t*)(data_ptr)),
            K(ret));
      } else if (OB_UNLIKELY((curr_size = *((int64_t*)(data_ptr)-2)) < data_length)) {
        ret = OB_ERR_UNEXPECTED;
        ;
        LOG_ERROR("target obj size is overflow", K(curr_size), "target_size", data_length, K(ret));
      } else {
        if (need_size > curr_size) {
          need_size = need_size * 2;
          void* buff_ptr = NULL;
          if (OB_ISNULL(buff_ptr = static_cast<char*>(stored_row_buf_.alloc(need_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            ((int64_t*)buff_ptr)[0] = need_size;
            ((int64_t*)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
            buf = (char*)((int64_t*)(buff_ptr) + 2);
            LOG_DEBUG("succ to alloc buff", K(need_size), K(src_cell), K(target_cell), K(lbt()));
          }
        } else {
          buf = (char*)(data_ptr);
        }
      }
    } else {
      void* buff_ptr = NULL;
      if (OB_ISNULL(buff_ptr = static_cast<char*>(stored_row_buf_.alloc(need_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ((int64_t*)buff_ptr)[0] = need_size;
        ((int64_t*)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
        buf = (char*)((int64_t*)(buff_ptr) + 2);
        LOG_DEBUG("succ to alloc buff", K(need_size), K(src_cell), K(target_cell), K(lbt()));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      ret = target_cell.deep_copy(src_cell, buf, need_size, pos);
    }
  } else {
    target_cell = src_cell;
  }
  return ret;
}

int ObAggregateFunction::clone_number_cell(const ObObj& src_cell, ObObj& target_cell)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != src_cell.get_type_class()) ||
      OB_UNLIKELY(src_cell.get_number_byte_length() > number::ObNumber::MAX_CALC_BYTE_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid",
        K(src_cell.get_type()),
        K(target_cell.get_type()),
        K(src_cell.get_number_byte_length()),
        K(src_cell),
        K(ret));
  } else {
    const int64_t NEED_ALLOC_SIZE = sizeof(int64_t) * 2 + number::ObNumber::MAX_CALC_BYTE_LEN;
    char* buf = NULL;
    void* data_ptr = NULL;
    bool is_finish = false;
    if (src_cell.is_zero_number()) {
      is_finish = true;
      target_cell.set_number(src_cell.get_type(), src_cell.get_number_desc(), NULL);
    } else if (ObNumberTC == target_cell.get_type_class() &&
               NULL != (data_ptr = (void*)(const_cast<uint32_t*>(target_cell.get_number_digits()))) &&
               0 != target_cell.get_number_digit_length()) {
      int64_t curr_alloc_size = 0;
      if (OB_ISNULL((char*)data_ptr - sizeof(int64_t)) ||
          OB_ISNULL((char*)data_ptr - sizeof(int64_t) - sizeof(int64_t))) {
        ret = OB_ERR_UNEXPECTED;
        ;
        LOG_ERROR("clone_cell use stored_row_buf, need has meta", KP(data_ptr), K(ret));
      } else if (OB_UNLIKELY(*((int64_t*)(data_ptr)-1) != STORED_ROW_MAGIC_NUM) ||
                 OB_UNLIKELY((curr_alloc_size = *((int64_t*)(data_ptr)-2)) != NEED_ALLOC_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        ;
        LOG_ERROR("stored_row_buf memory is mismatch, maybe some one make bad things",
            "curr_magic",
            *((int64_t*)(data_ptr)-1),
            "curr_size",
            *((int64_t*)(data_ptr)-2),
            K(target_cell),
            K(ret));
      } else {
        buf = (char*)(data_ptr);
      }
    } else {
      if (OB_ISNULL(data_ptr = static_cast<char*>(stored_row_buf_.alloc(NEED_ALLOC_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(NEED_ALLOC_SIZE));
      } else {
        ((int64_t*)data_ptr)[0] = NEED_ALLOC_SIZE;
        ((int64_t*)data_ptr)[1] = STORED_ROW_MAGIC_NUM;
        buf = (char*)((int64_t*)(data_ptr) + 2);
        LOG_DEBUG("succ to alloc buff",
            "curr_magic",
            *((int64_t*)(buf)-1),
            "curr_size",
            *((int64_t*)(buf)-2),
            K(src_cell),
            K(target_cell),
            K(lbt()));
      }
    }

    if (OB_SUCC(ret) && !is_finish) {
      MEMCPY(buf, src_cell.get_number_digits(), src_cell.get_number_byte_length());
      target_cell = src_cell;
      target_cell.set_number(src_cell.get_type(), src_cell.get_number_desc(), (uint32_t*)(buf));
    }
  }
  return ret;
}

int ObAggregateFunction::fill_distinct_item_cell_list(
    const ObAggregateExpression* cexpr, const ObNewRow& input_row, ObAggregateDistinctItem& distinct_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cexpr) || OB_UNLIKELY(input_row.is_invalid()) ||
      OB_UNLIKELY(input_row.get_count() < cexpr->get_real_param_col_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cexpr), K_(input_row.count));
  } else if (OB_ISNULL(distinct_item.cells_ = static_cast<ObObj*>(
                           stored_row_buf_.alloc(sizeof(ObObj) * cexpr->get_real_param_col_count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    distinct_item.cs_type_list_ = cexpr->get_aggr_cs_types();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cexpr->get_real_param_col_count(); ++i) {
    new (&distinct_item.cells_[i]) ObObj();
    if (OB_FAIL(clone_cell(input_row.get_cell(i), distinct_item.cells_[i]))) {
      LOG_WARN("failed to clone cell", K(ret), K(i), K(input_row.cells_[i]));
    }
  }
  return ret;
}

int ObAggregateFunction::prepare(const ObNewRow& row, int64_t group_id, ObRowStore::StoredRow** output_row)
{
  int ret = OB_SUCCESS;
  ObRowStore::StoredRow* stored_row = NULL;
  ObItemType aggr_fun = T_INVALID;
  bool is_distinct = false;
  ObObj* aggr_cell = NULL;
  ObObj* aux_cell = NULL;
  int64_t aggr_idx = child_column_count_;
  // AVG, APPROX_COUNT_DISTINCT to use auxiliary columns.
  int64_t aux_col_idx = 0;
  int64_t agg_udf_meta_offset = 0;
  // for sort-based group by operator, for performance reason,
  // after producing a group, we will invoke reuse_group() function to clear the group
  // thus, we do not need to allocate the group space again here, simply reuse the space
  if (group_id >= row_array_.count() && OB_FAIL(init_one_group(group_id))) {
    LOG_WARN("failed to init one group", K(group_id), K(ret));
  }
  // get stored row and group_concat_array
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_stored_row(group_id, stored_row))) {
      LOG_WARN("failed to get stored row and group_concat_array", K(ret), K(group_id));
    } else {
      stored_row->reserved_cells_count_ = static_cast<int32_t>(full_column_count_);
    }
  }
  // process non aggregate cols
  for (int64_t i = 0; OB_SUCC(ret) && i < child_column_count_; i++) {
    // If the row generated by the child operator has a projector,
    // then the cell should be the projected cell
    if (OB_FAIL(clone_cell(row.get_cell(i), stored_row->reserved_cells_[i]))) {
      LOG_WARN("failed to clone cell", K(ret));
    } else { /*do nothing.*/
    }
  }

  // process aggregate columns
  FOREACH_CNT_X(node, agg_columns_, OB_SUCC(ret))
  {
    ObCollationType cs_type = CS_TYPE_INVALID;
    const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node->expr_);
    int64_t agg_udf_id = GET_AGG_UDF_ID(group_id, child_column_count_, agg_columns_.count(), aggr_idx);
    ObAggUdfMeta* agg_udf_meta = nullptr;
    if (OB_ISNULL(cexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr/node is null", K(node->expr_));
    } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, cs_type))) {
      LOG_WARN("failed to get aggr column", K(ret));
    } else {
      ObNewRow input_row;
      input_row.cells_ = input_cells_;
      input_row.count_ = cexpr->get_all_param_col_count();
      if (OB_FAIL(cexpr->calc_result_row(expr_ctx_, row, input_row))) {
        LOG_WARN("fail to get calc cell list", K(ret), K(row), K(*cexpr));
      } else {
        aggr_cell = &(stored_row->reserved_cells_[aggr_idx]);
        if (T_FUN_AVG != aggr_fun && T_FUN_APPROX_COUNT_DISTINCT != aggr_fun) {
          aux_cell = NULL;
        } else {
          aux_cell = &(stored_row->reserved_cells_[output_column_count_ + aux_col_idx++]);
        }
        if (T_FUN_AGG_UDF == aggr_fun) {
          if (agg_udf_meta_offset >= agg_udf_metas_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the agg udf meta is invalid", K(ret), K(agg_udf_meta_offset), K(agg_udf_metas_.count()));
          } else {
            agg_udf_meta = &agg_udf_metas_.at(agg_udf_meta_offset);
            ++agg_udf_meta_offset;
          }
        }
        if (OB_SUCC(ret)) {
          ObAggCellCtx* cell_ctx = get_agg_cell_ctx(group_id, node->ctx_idx_);
          if (!(is_distinct && is_sort_based_gby_)) {
            if (OB_FAIL(init_aggr_cell(
                    aggr_fun, input_row, *aggr_cell, aux_cell, cell_ctx, cs_type, agg_udf_id, agg_udf_meta))) {
              LOG_WARN("failed to init cell", K(ret));
            }
          }
          if (OB_SUCC(ret) && is_distinct) {
            if (!is_sort_based_gby_) {
              ObAggregateDistinctItem distinct_item;
              distinct_item.group_id_ = group_id;
              distinct_item.col_idx_ = aggr_idx;
              distinct_item.cs_type_ = cs_type;
              if (OB_FAIL(fill_distinct_item_cell_list(cexpr, input_row, distinct_item))) {
                LOG_WARN("failed to fill distinct item cell list", K(ret), K(*cexpr), K(input_row));
              } else if (OB_FAIL(aggr_distinct_set_.set_refactored(distinct_item))) {
                // collect distinct cells
                LOG_WARN("failed to set distinct item", K(ret));
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              if (OB_ISNULL(cell_ctx) || OB_ISNULL(cell_ctx->distinct_set_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("distinct set is NULL", K(ret));
              } else if (OB_FAIL(cell_ctx->distinct_set_->add_row(input_row))) {
                LOG_WARN("add row to distinct set failed", K(ret));
              }
            }
          }
        }
      }
      if (input_row.cells_ != NULL) {
        for (int64_t idx = 0; idx < input_row.count_; idx++) {
          input_row.cells_[idx].reset();
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++aggr_idx;
    }
  }  // end for

  if (OB_SUCC(ret) && output_row != NULL) {
    *output_row = stored_row;
  }
  return ret;
}

int ObAggregateFunction::process(const ObNewRow& row, const ObTimeZoneInfo* tz_info, const int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObRowStore::StoredRow* stored_row = NULL;
  ObObj* aggr_cell = NULL;
  ObObj* aux_cell = NULL;
  ObItemType aggr_fun = T_INVALID;
  bool is_distinct = false;
  int64_t aggr_idx = child_column_count_;
  int64_t aux_col_idx = 0;
  // get stored row and group_concat_array
  if (OB_FAIL(get_stored_row(group_id, stored_row))) {
    LOG_WARN("failed to get stored row", K(ret));
  }
  FOREACH_CNT_X(node, agg_columns_, OB_SUCC(ret))
  {
    ObCollationType cs_type = CS_TYPE_INVALID;
    const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node->expr_);
    int64_t agg_udf_id = GET_AGG_UDF_ID(group_id, child_column_count_, agg_columns_.count(), aggr_idx);
    if (OB_ISNULL(cexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cexpr node is null", K(node->expr_), K(ret));
    } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, cs_type))) {
      LOG_WARN("failed to get aggr column", K(ret));
    } else if (OB_UNLIKELY(aggr_idx < 0) || OB_UNLIKELY(aggr_idx >= stored_row->reserved_cells_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid aggr idx", K(aggr_idx), K(ret));
    } else if (T_FUN_COUNT == aggr_fun && cexpr->is_empty()) {
      aggr_cell = &stored_row->reserved_cells_[aggr_idx];
      int64_t aux = 0;
      if (OB_ISNULL(aggr_cell)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr_cell is null", K(ret));
      } else if (OB_FAIL(aggr_cell->get_int(aux))) {
        LOG_WARN("fail to get aux", K(ret));
      } else {
        aggr_cell->set_int(++aux);
      }
    } else {
      aggr_cell = &stored_row->reserved_cells_[aggr_idx];
      if (T_FUN_AVG != aggr_fun && T_FUN_APPROX_COUNT_DISTINCT != aggr_fun) {
        aux_cell = NULL;
      } else {
        aux_cell = &(stored_row->reserved_cells_[output_column_count_ + aux_col_idx++]);
      }
      if (OB_SUCC(ret)) {
        ObAggCellCtx* cell_ctx = get_agg_cell_ctx(group_id, node->ctx_idx_);
        ObNewRow input_row;
        input_row.count_ = cexpr->get_all_param_col_count();
        input_row.cells_ = input_cells_;
        if (OB_FAIL(cexpr->calc_result_row(expr_ctx_, row, input_row))) {
          LOG_WARN("fail to get calc cell list", K(ret), K(row), K(*cexpr));
        } else if (is_distinct) {
          if (!is_sort_based_gby_) {
            ObAggregateDistinctItem distinct_item;
            distinct_item.group_id_ = group_id;
            distinct_item.col_idx_ = aggr_idx;
            distinct_item.cs_type_ = cs_type;
            if (OB_FAIL(calc_distinct_item(
                    aggr_fun, input_row, tz_info, cexpr, distinct_item, *aggr_cell, aux_cell, cell_ctx))) {
              LOG_WARN("failed to calculate distinct item", K(ret));
            }
          } else {
            if (OB_ISNULL(cell_ctx) || OB_ISNULL(cell_ctx->distinct_set_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("distinct set is NULL", K(ret));
            } else if (OB_FAIL(cell_ctx->distinct_set_->add_row(input_row))) {
              LOG_WARN("add row to distinct set failed", K(ret));
            }
          }
        } else {
          // not distinct aggr column
          if (OB_FAIL(calc_aggr_cell(aggr_fun,
                  input_row,
                  *aggr_cell,
                  aux_cell,
                  tz_info,
                  cexpr->get_collation_type(),
                  cell_ctx,
                  agg_udf_id))) {
            LOG_WARN("failed to calculate aggr cell", K(ret));
          }
        }
        if (input_cells_ != NULL) {
          for (int64_t idx = 0; idx < input_row.count_; idx++) {
            input_cells_[idx].reset();
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++aggr_idx;
    }
  }  // end for
  if (OB_SUCC(ret)) {
    if (static_cast<ObArenaAllocator*>(expr_ctx_.calc_buf_)->used() >= AGGR_CALC_BUF_LIMIT) {
      static_cast<ObArenaAllocator*>(expr_ctx_.calc_buf_)->reset_remain_one_page();
    }
  }
  return ret;
}

int ObAggregateFunction::get_stored_row(const int64_t group_id, ObRowStore::StoredRow*& stored_row)
{
  int ret = OB_SUCCESS;
  GroupRow gr;
  // get stored row
  if (OB_FAIL(row_array_.at(group_id, gr))) {
    LOG_WARN("failed to get stored row", K(ret));
  } else if (OB_ISNULL(gr.row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored_row is null", K(stored_row));
  } else {
    stored_row = gr.row_;
    // do nothing
  }
  return ret;
}

int ObAggregateFunction::calc_distinct_item(const ObItemType aggr_fun, const common::ObNewRow& input_row,
    const common::ObTimeZoneInfo* tz_info, const ObAggregateExpression* cexpr, ObAggregateDistinctItem& distinct_item,
    ObObj& res1, ObObj* res2, ObAggCellCtx* cell_ctx, const bool should_fill_item, const bool should_calc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(cexpr) || input_row.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (should_fill_item && OB_FAIL(fill_distinct_item_cell_list(cexpr, input_row, distinct_item))) {
      LOG_WARN("failed to fill distinct item cell list", K(ret), K(*cexpr), K(input_row));
    } else if (OB_HASH_NOT_EXIST == (ret = aggr_distinct_set_.exist_refactored(distinct_item))) {
      if (should_calc &&
          OB_FAIL(calc_aggr_cell(aggr_fun, input_row, res1, res2, tz_info, cexpr->get_collation_type(), cell_ctx))) {
        LOG_WARN("failed to calculate aggr cell", K(ret));
      } else if (OB_FAIL(aggr_distinct_set_.set_refactored(distinct_item))) {
        LOG_WARN("fail to set distinct item", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (ret != OB_HASH_EXIST) {
      LOG_WARN("check exist from aggr_distinct_set failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObAggregateFunction::aggr_distinct_cell(const ObItemType aggr_fun, ObAggCellCtx* cell_ctx, common::ObObj& res1,
    common::ObObj* res2, const common::ObTimeZoneInfo* tz_info, common::ObCollationType cs_type, int64_t agg_udf_id,
    ObAggUdfMeta* agg_udf_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cell_ctx) || OB_ISNULL(cell_ctx->distinct_set_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else {
    const ObNewRow* row = NULL;
    bool first = true;
    while (OB_SUCCESS == ret) {
      if (OB_FAIL(cell_ctx->distinct_set_->get_next_row(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get row from distinct set failed", K(ret));
        }
        break;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("return row is NULL", K(ret));
      } else {
        if (first) {
          if (OB_FAIL(init_aggr_cell(aggr_fun, *row, res1, res2, cell_ctx, cs_type, agg_udf_id, agg_udf_meta))) {
            LOG_WARN("init aggregation cell failed", K(ret));
          }
        } else {
          if (OB_FAIL(calc_aggr_cell(aggr_fun, *row, res1, res2, tz_info, cs_type, cell_ctx, agg_udf_id))) {
            LOG_WARN("calc aggr cell failed", K(ret));
          }
        }
        first = false;
      }
    }
  }
  return ret;
}

int ObAggregateFunction::rollup_init(const int64_t num_group_col)
{
  int ret = OB_SUCCESS;
  for (int64_t group_id = 0; OB_SUCC(ret) && group_id <= num_group_col; ++group_id) {
    if (OB_FAIL(init_one_group(group_id))) {
      LOG_WARN("failed to init one group", K(group_id), K(ret));
    }
  }
  return ret;
}

int ObAggregateFunction::init_one_group(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  GroupRow gr;
  const int64_t stored_row_size = sizeof(ObRowStore::StoredRow) + sizeof(ObObj) * full_column_count_;
  const int64_t ctx_ptr_size = sizeof(ObAggCellCtx*) * agg_cell_ctx_cnt_;
  if (NULL == (gr.row_ = static_cast<ObRowStore::StoredRow*>(stored_row_buf_.alloc(stored_row_size + ctx_ptr_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc stored row failed", K(stored_row_size), K(ctx_ptr_size), K(group_id), K(ret));
  } else {
    MEMSET(gr.row_, 0, stored_row_size + ctx_ptr_size);
    gr.ctx_ = reinterpret_cast<ObAggCellCtx**>(reinterpret_cast<char*>(gr.row_) + stored_row_size);
    if (OB_FAIL(row_array_.push_back(gr))) {
      LOG_WARN("fail to push row to row_array", K(group_id), K(ret));
    }
  }
  // setup aggregate cell context for distinct or (groupconcat,rank,dense rank,percent rank,
  //   cume_dist,keep_clause(max(...)/min(...)/count(...)/sum(...) keep ...)
  if (OB_SUCC(ret) && (has_distinct_ || aggr_fun_need_cell_ctx_)) {
    FOREACH_CNT_X(node, agg_columns_, OB_SUCC(ret))
    {
      ObCollationType cs_type = CS_TYPE_INVALID;
      ObItemType aggr_fun = T_INVALID;
      bool is_distinct = false;
      const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node->expr_);
      if (OB_ISNULL(cexpr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr/node is null", K(node->expr_), K(ret));
      } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, cs_type))) {
        LOG_WARN("failed to get aggr column", K(ret));
        // rank,dense rank,percent rank,cume_dist,keep_clause(max(...)/min(...)/count(...)/sum(...) keep ...)
        // Similar to the group_concat structure, so you can use this class to perform calculations
      } else {
        switch (aggr_fun) {
          case T_FUN_GROUP_CONCAT:
          case T_FUN_GROUP_RANK:
          case T_FUN_GROUP_DENSE_RANK:
          case T_FUN_GROUP_PERCENT_RANK:
          case T_FUN_GROUP_CUME_DIST:
          case T_FUN_GROUP_PERCENTILE_CONT:
          case T_FUN_GROUP_PERCENTILE_DISC:
          case T_FUN_MEDIAN:
          case T_FUN_KEEP_MAX:
          case T_FUN_KEEP_MIN:
          case T_FUN_KEEP_SUM:
          case T_FUN_KEEP_COUNT:
          case T_FUN_KEEP_WM_CONCAT:
          case T_FUN_WM_CONCAT: 
          case T_FUN_JSON_ARRAYAGG:
          case T_FUN_JSON_OBJECTAGG: {
            void* mem1 = NULL;
            void* mem2 = NULL;
            ObGroupConcatCtx* gc_ctx = NULL;
            if (OB_ISNULL(mem1 = agg_cell_ctx_alloc_.alloc(sizeof(ObGroupConcatCtx)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              gc_ctx = new (mem1) ObGroupConcatCtx(agg_cell_ctx_alloc_);
              if (OB_ISNULL(mem2 = agg_cell_ctx_alloc_.alloc(sizeof(ObGroupConcatRowStore)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("allocate memory failed", K(ret));
              } else {
                gc_ctx->gc_rs_ = new (mem2) ObGroupConcatRowStore();
                // In window function, get result will be called more than once, rewind is needed.
                // Iterate twice with rollup, rewind is need for groups which id is greater than zero.
                const bool need_rewind = in_window_func_ || (group_id > 0 && is_sort_based_gby_);
                if (OB_FAIL(gc_ctx->gc_rs_->init(expr_ctx_.my_session_->get_effective_tenant_id(),
                        cexpr->get_sort_columns(),
                        &cexpr->get_sort_extra_infos(),
                        need_rewind, dir_id_))) {
                  LOG_WARN("init group concat store failed", K(ret));
                } else {
                  gr.ctx_[node->ctx_idx_] = gc_ctx;
                }
              }
            }
            if (OB_FAIL(ret) && NULL != mem1) {
              gc_ctx->~ObGroupConcatCtx();
              gc_ctx = NULL;
              agg_cell_ctx_alloc_.free(mem1);
            }
            break;
          }
          default:
            break;
        }
      }
      if (OB_SUCC(ret) && is_distinct) {
        if (NULL == gr.ctx_[node->ctx_idx_]) {
          void* mem = NULL;
          if (OB_ISNULL(mem = agg_cell_ctx_alloc_.alloc(sizeof(ObAggCellCtx)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            gr.ctx_[node->ctx_idx_] = new (mem) ObAggCellCtx(agg_cell_ctx_alloc_);
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
          const bool need_rewind = in_window_func_ || (group_id > 0 && is_sort_based_gby_);
          if (OB_FAIL(gr.ctx_[node->ctx_idx_]->init_distinct_set(expr_ctx_.my_session_->get_effective_tenant_id(),
                  *cexpr->get_aggr_cs_types(),
                  cexpr->get_real_param_col_count(),
                  need_rewind))) {
            LOG_WARN("get init distinct set failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCCESS != ret) {
      FOREACH_CNT(node, agg_columns_)
      {
        if (node->ctx_idx_ >= 0 && NULL != gr.ctx_[node->ctx_idx_]) {
          auto& ctx = gr.ctx_[node->ctx_idx_];
          ctx->~ObAggCellCtx();
          agg_cell_ctx_alloc_.free(ctx);
          ctx = NULL;
        }
      }
    }
  }
  return ret;
}

int ObAggregateFunction::rollup_process(const ObTimeZoneInfo* tz_info, const int64_t group_id1, const int64_t group_id2,
    const int64_t diff_col_idx, bool set_grouping /*default false*/)
{
  int ret = OB_SUCCESS;
  ObRowStore::StoredRow* stored_row1 = NULL;
  ObRowStore::StoredRow* stored_row2 = NULL;
  if (!is_sort_based_gby_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rollup exist in non merge group by", K(ret));
  } else if (OB_FAIL(get_stored_row(group_id1, stored_row1)) || OB_FAIL(get_stored_row(group_id2, stored_row2))) {
    LOG_WARN("failed to get stored row and group_concat_array", K(ret));
  } else {
    // copy group by (non-aggregation) column
    bool is_first_rollup = false;
    if (stored_row1->reserved_cells_count_ == 0) {
      is_first_rollup = true;
      stored_row1->reserved_cells_count_ = stored_row2->reserved_cells_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_column_count_; ++i) {
        if (i == diff_col_idx) {
          if (first_rollup_cols_.at(group_id1)) {
            stored_row1->reserved_cells_[i].set_null();
          } else if (OB_FAIL(clone_cell(stored_row2->reserved_cells_[i], stored_row1->reserved_cells_[i]))) {
            LOG_WARN("failed to clone cell.", K(ret));
          } else { /*do nothing*/
          }
        } else {
          if (OB_FAIL(clone_cell(stored_row2->reserved_cells_[i], stored_row1->reserved_cells_[i]))) {
            LOG_WARN("failed to clone cell", K(ret));
          }
        }
      }
    }
    // copy aggregation column
    int64_t aux_col_idx = output_column_count_;
    int64_t aggr_idx = child_column_count_;
    FOREACH_CNT_X(node, agg_columns_, OB_SUCC(ret))
    {
      ObCollationType cs_type = CS_TYPE_INVALID;
      ObItemType aggr_fun = T_INVALID;
      bool is_distinct = false;
      ObObj* res1 = &stored_row1->reserved_cells_[aggr_idx];
      ObObj* res2 = &stored_row2->reserved_cells_[aggr_idx];
      ObObj* aux_res1 = NULL;
      ObObj* aux_res2 = NULL;
      ObGroupConcatCtx* cell_ctx1 = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id1, node->ctx_idx_));
      ObGroupConcatCtx* cell_ctx2 = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id2, node->ctx_idx_));
      ObGroupConcatRowStore* group_concat_row_store1 = NULL;
      ObGroupConcatRowStore* group_concat_row_store2 = NULL;
      const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node->expr_);
      int64_t grouping_column_idx = -1;
      if (OB_ISNULL(cexpr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr/node is null", K(node->expr_));
      } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, cs_type))) {
        LOG_WARN("failed to get aggr column", K(ret));
      } else {
        if (T_FUN_GROUPING == aggr_fun) {
          const ObSqlFixedArray<ObInfixExprItem>& exprs = cexpr->get_expr_items();
          if (1 != exprs.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the grouping func could not handle more than one params", K(exprs));
          } else {
            grouping_column_idx = exprs.at(0).get_column();
          }
        }
        if (T_FUN_AVG == aggr_fun || T_FUN_APPROX_COUNT_DISTINCT == aggr_fun) {
          aux_res1 = &stored_row1->reserved_cells_[aux_col_idx];
          aux_res2 = &stored_row2->reserved_cells_[aux_col_idx++];
        }
        switch (aggr_fun) {
          case T_FUN_GROUP_CONCAT:
          case T_FUN_GROUP_RANK:
          case T_FUN_GROUP_DENSE_RANK:
          case T_FUN_GROUP_PERCENT_RANK:
          case T_FUN_GROUP_CUME_DIST:
          case T_FUN_KEEP_MAX:
          case T_FUN_KEEP_MIN:
          case T_FUN_KEEP_COUNT:
          case T_FUN_KEEP_SUM:
          case T_FUN_GROUP_PERCENTILE_CONT:
          case T_FUN_GROUP_PERCENTILE_DISC:
          case T_FUN_MEDIAN:
          case T_FUN_KEEP_WM_CONCAT:
          case T_FUN_WM_CONCAT: 
          case T_FUN_JSON_ARRAYAGG:
          case T_FUN_JSON_OBJECTAGG: {
            if (NULL == cell_ctx1 || NULL == cell_ctx2 || NULL == cell_ctx1->gc_rs_ || NULL == cell_ctx2->gc_rs_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL group concat row store", K(ret));
            } else {
              group_concat_row_store1 = cell_ctx1->gc_rs_;
              group_concat_row_store2 = cell_ctx2->gc_rs_;
            }
            break;
          }
          default:
            break;
        }

        if (OB_SUCC(ret) && !is_distinct) {
          if (OB_FAIL(rollup_aggregation(aggr_fun,
                  is_first_rollup,
                  tz_info,
                  cs_type,
                  *res1,
                  *res2,
                  *aux_res1,
                  *aux_res2,
                  group_concat_row_store1,
                  group_concat_row_store2,
                  set_grouping ? grouping_column_idx == diff_col_idx : false))) {
            // ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to rollup aggregation results", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_distinct) {
          // copy all distinct item from group2 to group1
          if (OB_ISNULL(cell_ctx1) || OB_ISNULL(cell_ctx1->distinct_set_) || OB_ISNULL(cell_ctx2) ||
              OB_ISNULL(cell_ctx2->distinct_set_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("distinct set is NULL", K(ret), KP(cell_ctx1), KP(cell_ctx2));
          } else if (OB_FAIL(cell_ctx2->distinct_set_->sort())) {
            LOG_WARN("sort failed", K(ret));
          } else {
            const ObNewRow* row = NULL;
            while (OB_SUCC(ret)) {
              if (OB_FAIL(cell_ctx2->distinct_set_->get_next_row(row))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("get row from distinct set failed", K(ret));
                }
                break;
              } else if (OB_ISNULL(row)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("return row is NULL", K(ret));
              } else if (OB_FAIL(cell_ctx1->distinct_set_->add_row(*row))) {
                LOG_WARN("add row to distinct failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          ++aggr_idx;
        }
      }
    }
  }
  return ret;
}

int ObAggregateFunction::rollup_aggregation(const ObItemType aggr_fun, const bool is_first_rollup,
    const common::ObTimeZoneInfo* tz_info, ObCollationType cs_type, ObObj& res1, ObObj& res2, ObObj& aux_res1,
    ObObj& aux_res2, ObGroupConcatRowStore* group_concat_row_store1, ObGroupConcatRowStore* group_concat_row_store2,
    bool set_grouping)
{
  int ret = OB_SUCCESS;
  switch (aggr_fun) {
    case T_FUN_COUNT: {
      if (is_first_rollup) {
        ret = clone_cell(res2, res1);
      } else {
        ret = add_calc(res1, res1, res2, tz_info);
      }
      break;
    }
    case T_FUN_MAX: {
      if (is_first_rollup) {
        ret = clone_cell(res2, res1);
      } else {
        ret = max_calc(res1, res2, cs_type);
      }
      break;
    }
    case T_FUN_MIN: {
      if (is_first_rollup) {
        ret = clone_cell(res2, res1);
      } else {
        ret = min_calc(res1, res2, cs_type);
      }
      break;
    }
    case T_FUN_SUM:
    case T_FUN_COUNT_SUM: {
      if (is_first_rollup || res1.is_null()) {
        ret = clone_cell(res2, res1);
      } else if (!res2.is_null()) {
        ret = add_calc(res1, res1, res2, tz_info);
      }
      break;
    }
    case T_FUN_AVG: {
      if (is_first_rollup) {
        // copy sum part
        ret = clone_cell(res2, res1);
        if (OB_SUCC(ret)) {
          // copy count part
          ret = clone_cell(aux_res2, aux_res1);
        }
      } else {
        // aggregate sum part
        if (res1.is_null()) {
          ret = clone_cell(res2, res1);
        } else if (!res2.is_null()) {
          ret = add_calc(res1, res1, res2, tz_info);
        }
        // aggregate count part
        if (OB_SUCC(ret)) {
          ret = add_calc(aux_res1, aux_res1, aux_res2, tz_info);
        }
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT: {
      if (is_first_rollup) {
        ret = clone_cell(aux_res2, aux_res1);
      } else {
        ret = llc_add(aux_res1, aux_res1, aux_res2);
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      if (is_first_rollup) {
        ret = clone_cell(res2, res1);
      } else {
        ret = llc_add(res1, res1, res2);
      }
      break;
    }
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_MEDIAN:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT: 
    case T_FUN_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG: {
      if (OB_ISNULL(group_concat_row_store1) || OB_ISNULL(group_concat_row_store2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("group concat row store is null", K(ret));
      } else if (OB_FAIL(group_concat_row_store2->finish_add_row())) {
        LOG_WARN("finish add row failed", K(ret));
      } else {
        // add(rollup) all the tuples
        const ObNewRow* row = NULL;
        while (OB_SUCC(ret) && OB_SUCC(group_concat_row_store2->get_next_row(row))) {
          CK(NULL != row);
          OZ(group_concat_row_store1->add_row(*row));
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
      }
      break;
    }
    case T_FUN_GROUPING: {
      if (is_first_rollup) {
        ret = clone_cell(res2, res1);
      }
      if (set_grouping)
        res1.set_int(1);
      break;
    }
    case T_FUN_AGG_UDF: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rollup contain agg udfs still not supported", K(ret));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun));
      break;
  }
  return ret;
}

int ObAggregateFunction::reuse_group(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  GroupRow gr;
  if (OB_FAIL(row_array_.at(group_id, gr))) {
    LOG_WARN("fail to get stored row", K(ret));
  } else if (OB_ISNULL(gr.row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored_row is null", K(gr.row_));
  } else {
    int64_t stored_row_size = sizeof(ObRowStore::StoredRow) + sizeof(ObObj) * full_column_count_;
    memset(gr.row_, 0, stored_row_size);
  }

  if (OB_SUCC(ret) && agg_cell_ctx_cnt_ > 0 && NULL != gr.ctx_) {
    for (int64_t i = 0; i < agg_cell_ctx_cnt_; i++) {
      if (NULL != gr.ctx_[i]) {
        gr.ctx_[i]->reuse();
      }
    }
  }

  if (OB_SUCC(ret) && has_distinct_) {
    FOREACH_X(entry, aggr_distinct_set_, OB_SUCC(ret))
    {
      ObAggregateDistinctItem* dist_item = &entry->first;
      if (dist_item->group_id_ == group_id) {
        if (OB_FAIL(aggr_distinct_set_.erase_refactored(*dist_item))) {
          LOG_WARN("failed to erase distinct item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAggregateFunction::init_aggr_cell(const ObItemType aggr_fun, const ObNewRow& oprands, ObObj& res1, ObObj* res2,
    ObAggCellCtx* cell_ctx, ObCollationType cs_type, int64_t agg_udf_id, ObAggUdfMeta* agg_udf_meta)
{
  int ret = OB_SUCCESS;
  switch (aggr_fun) {
    case T_FUN_COUNT: {
      bool has_null_cell = false;
      if (OB_UNLIKELY(oprands.is_invalid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("oprands is invalid", K(oprands));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !has_null_cell && i < oprands.count_; ++i) {
        if (oprands.cells_[i].is_null()) {
          has_null_cell = true;
        }
      }
      if (has_null_cell) {
        res1.set_int(0);
      } else {
        res1.set_int(1);
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
      bool has_null_cell = false;
      ObObj* llc_bitmap = NULL;
      if (T_FUN_APPROX_COUNT_DISTINCT == aggr_fun) {
        llc_bitmap = res2;
      } else {
        llc_bitmap = &res1;
      }
      if (OB_FAIL(llc_init(llc_bitmap))) {
        SQL_ENG_LOG(WARN, "llc_init failed in approx_count_distinct(_synopsis) function");
      } else {
        uint64_t hash_value = llc_calc_hash_value(oprands, cs_type, has_null_cell);
        if (has_null_cell) {
          /*do nothing*/
        } else {
          if (OB_FAIL(llc_add_value(hash_value, llc_bitmap))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("llc_add_value failed.");
          }
        }
      }
      break;
    }
    case T_FUN_AVG: {
      if (OB_ISNULL(res2) || OB_UNLIKELY(oprands.is_invalid())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "res2 can't be null in avg function", K(res2), K(oprands));
        break;
      } else if ((!oprands.cells_[0].is_null())) {
        res2->set_int(1);
      } else {
        res2->set_int(0);
      }
    }                // fall through
    case T_FUN_MAX:  // fail through
    case T_FUN_MIN:  // fail through
    case T_FUN_COUNT_SUM:
    case T_FUN_SUM:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      if (OB_UNLIKELY(oprands.count_ != 1) || OB_ISNULL(oprands.cells_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("oprands is invalid", K(oprands));
      } else {
        ret = clone_cell(oprands.cells_[0], res1);
      }
      break;
    }
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_MEDIAN:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT: 
    case T_FUN_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG: {
      if (OB_ISNULL(cell_ctx)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("group concat row store is null", K(ret));
      } else {
        ObGroupConcatCtx* gc_ctx = static_cast<ObGroupConcatCtx*>(cell_ctx);
        if (OB_ISNULL(cell_ctx) || OB_ISNULL(gc_ctx->gc_rs_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group concat row store is NULL", K(ret), KP(cell_ctx));
        } else {
          if (!gc_ctx->gc_rs_->empty()) {
            gc_ctx->gc_rs_->reuse();
          }
          if (OB_FAIL(gc_ctx->gc_rs_->add_row(oprands))) {
            LOG_WARN("fail to add row to row store", K(ret), K(oprands));
          }
        }
      }
      break;
    }
    case T_FUN_GROUPING: {
      res1.set_int(0);
      break;
    }
    case T_FUN_AGG_UDF: {
      void* func_buf = agg_udf_buf_.alloc(sizeof(ObAggUdfFunction));
      void* ctx_buf = agg_udf_buf_.alloc(sizeof(ObUdfFunction::ObUdfCtx));
      ObAggUdfFunction* agg_udf_func = nullptr;
      ObUdfFunction::ObUdfCtx* agg_udf_ctx = nullptr;
      if (OB_ISNULL(ctx_buf) || OB_ISNULL(func_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(ctx_buf), K(func_buf));
      } else if (FALSE_IT(agg_udf_func = new (func_buf) ObAggUdfFunction())) {
      } else if (FALSE_IT(agg_udf_ctx = new (ctx_buf) ObUdfFunction::ObUdfCtx())) {
      } else if (OB_ISNULL(agg_udf_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to push_back", K(ret), K(agg_udf_meta));
      } else if (OB_FAIL(agg_udf_func->init(agg_udf_meta->udf_meta_))) {
        LOG_WARN("udf function init failed", K(ret));
      } else if (OB_FAIL(ObUdfUtil::init_udf_args(agg_udf_buf_,
                     agg_udf_meta->udf_attributes_,
                     agg_udf_meta->udf_attributes_types_,
                     agg_udf_ctx->udf_args_))) {
        LOG_WARN("failed to set udf args", K(ret));
      } else if (OB_FAIL(agg_udf_func->process_init_func(*agg_udf_ctx))) {
        LOG_WARN("do agg init func failed", K(ret));
      } else if (OB_FAIL(agg_udf_func->process_clear_func(*agg_udf_ctx))) {
        LOG_WARN("the udf clear func process failed", K(ret));
      } else if (OB_FAIL(agg_udf_.set_refactored(agg_udf_id, ObAggUdfExeUnit(agg_udf_func, agg_udf_ctx)))) {
        LOG_WARN("udf failed", K(ret), K(agg_udf_id));
      } else if (OB_FAIL(agg_udf_func->process_add_func(expr_ctx_, agg_udf_buf_, oprands, *agg_udf_ctx))) {
        LOG_WARN("the udf add func process failed", K(ret));
      }
      LOG_DEBUG("agg init cell", K(oprands), K(agg_udf_id), K(agg_udf_ctx), K(ctx_buf), K(agg_udf_.size()));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun));
      break;
  }
  return ret;
}

int ObAggregateFunction::calc_aggr_cell(const ObItemType aggr_fun, const ObNewRow& oprands, ObObj& res1, ObObj* res2,
    const ObTimeZoneInfo* tz_info, ObCollationType cs_type, ObAggCellCtx* cell_ctx, int64_t agg_udf_id)
{
  int ret = OB_SUCCESS;
  int64_t aux = 0;
  bool ignore_oprand = false;
  switch (aggr_fun) {
    case T_FUN_GROUP_CONCAT:
    case T_FUN_COUNT:
    case T_FUN_APPROX_COUNT_DISTINCT:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
    case T_FUN_AGG_UDF:
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
      if (OB_UNLIKELY(oprands.is_invalid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("oprands is invalid", K(oprands));
      }
      break;
    }
    default: {
      if (OB_UNLIKELY(oprands.is_invalid()) || OB_UNLIKELY(oprands.count_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("oprands is invalid");
      } else if (oprands.cells_[0].is_null()) {
        ignore_oprand = true;
      }
    }
  }
  if (OB_SUCC(ret) && !ignore_oprand) {
    switch (aggr_fun) {
      case T_FUN_COUNT: {
        bool has_null_cell = false;
        for (int64_t i = 0; !has_null_cell && i < oprands.count_; ++i) {
          if (oprands.cells_[i].is_null()) {
            has_null_cell = true;
          }
        }
        if (has_null_cell) {
          /*do nothing*/
        } else if (OB_FAIL(res1.get_int(aux))) {
          LOG_WARN("fail to get aux", K(ret));
        } else {
          res1.set_int(++aux);
        }
        break;
      }
      case T_FUN_MAX: {
        ret = max_calc(res1, oprands.cells_[0], cs_type);
        break;
      }
      case T_FUN_MIN: {
        ret = min_calc(res1, oprands.cells_[0], cs_type);
        break;
      }
      case T_FUN_SUM:
      case T_FUN_COUNT_SUM: {
        if (res1.is_null()) {
          ret = clone_cell(oprands.cells_[0], res1);
        } else {
          ret = add_calc(res1, res1, oprands.cells_[0], tz_info);
        }
        break;
      }
      case T_FUN_AVG: {
        if (OB_UNLIKELY(NULL == res2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("aux is NULL");
        } else if (res1.is_null()) {
          // the first non-NULL cell
          res2->set_int(1);
          ret = clone_cell(oprands.cells_[0], res1);
        } else {
          if (OB_FAIL(res2->get_int(aux))) {
            LOG_WARN("fail to get aux", K(ret), K(*res2));
          } else {
            res2->set_int(++aux);
            ret = add_calc(res1, res1, oprands.cells_[0], tz_info);
          }
        }
        break;
      }
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST:
      case T_FUN_GROUP_CONCAT:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_MEDIAN:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_COUNT:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_WM_CONCAT: 
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG: {
        if (OB_ISNULL(cell_ctx)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("group concat row store is null", K(ret));
        } else if (OB_FAIL(static_cast<ObGroupConcatCtx*>(cell_ctx)->gc_rs_->add_row(oprands))) {
          LOG_WARN("fail to add row to row store", K(ret), K(oprands));
        } else {
        }
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
        bool has_null_cell = false;
        ObObj* llc_bitmap = NULL;
        if (T_FUN_APPROX_COUNT_DISTINCT == aggr_fun) {
          llc_bitmap = res2;
        } else {
          llc_bitmap = &res1;
        }
        if (OB_ISNULL(llc_bitmap)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("llc_bitmap is NULL");
        } else {
          uint64_t hash_value = llc_calc_hash_value(oprands, cs_type, has_null_cell);
          if (has_null_cell) {
            /*do nothing*/
          } else {
            if (OB_FAIL(llc_add_value(hash_value, llc_bitmap))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("llc_add_value failed.");
            }
          }
        }
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        ret = llc_add(res1, res1, oprands.cells_[0]);
        break;
      }
      case T_FUN_GROUPING: {
        // do nothing
        break;
      }
      case T_FUN_AGG_UDF: {
        ObAggUdfExeUnit agg_udf_unit;
        if (OB_FAIL(agg_udf_.get_refactored(agg_udf_id, agg_udf_unit))) {
          LOG_WARN("failed to get agg func", K(ret));
        } else if (OB_ISNULL(agg_udf_unit.agg_func_) || OB_ISNULL(agg_udf_unit.udf_ctx_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the agg func is null", K(ret));
        } else if (OB_FAIL(agg_udf_unit.agg_func_->process_add_func(
                       expr_ctx_, agg_udf_buf_, oprands, *agg_udf_unit.udf_ctx_))) {
          LOG_WARN("the udf add func process failed", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown aggr function type", K(aggr_fun));
        break;
    }
  }
  return ret;
}

int ObAggregateFunction::max_calc(ObObj& base, const ObObj& other, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (!base.is_null() && !other.is_null()) {
    if (ObObjCmpFuncs::compare_oper_nullsafe(base, other, cs_type, CO_LT)) {
      ret = clone_cell(other, base);
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_cell(other, base);
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateFunction::min_calc(ObObj& base, const ObObj& other, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (!base.is_null() && !other.is_null()) {
    if (ObObjCmpFuncs::compare_oper_nullsafe(base, other, cs_type, CO_GT)) {
      ret = clone_cell(other, base);
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_cell(other, base);
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateFunction::add_calc(ObObj& res, const ObObj& left, const ObObj& right, const ObTimeZoneInfo* tz_info)
{

  UNUSED(tz_info);
  int ret = OB_SUCCESS;
  bool need_expr_calc = true;
  if (OB_ISNULL(expr_ctx_.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr context calc buf is null");
  } else if (ObNumberType == left.get_type() && ObNumberType == right.get_type()) {
    number::ObNumber sum;
    char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN);
    const bool strict_mode = false;  // this is tmp allocator, so we can ues non-strinct mode
    if (OB_FAIL(left.get_number().add_v3(right.get_number(), sum, allocator, strict_mode))) {
      LOG_WARN("number add failed", K(ret), K(left), K(right));
    } else {
      ObObj sum_obj;
      sum_obj.set_number(sum);
      ret = clone_number_cell(sum_obj, res);
      need_expr_calc = false;
    }
  } else if (ob_is_int_tc(left.get_type()) && ob_is_int_tc(right.get_type())) {
    int64_t left_int = left.get_int();
    int64_t right_int = right.get_int();
    int64_t sum_int = left_int + right_int;
    if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
      LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
      need_expr_calc = true;
    } else {
      LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
      res.set_int(sum_int);
      need_expr_calc = false;
    }
  } else if (ob_is_uint_tc(left.get_type()) && ob_is_uint_tc(right.get_type())) {
    uint64_t left_uint = left.get_uint64();
    uint64_t right_uint = right.get_uint64();
    uint64_t sum_uint = left_uint + right_uint;
    if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
      LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
      need_expr_calc = true;
    } else {
      LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
      res.set_uint64(sum_uint);
      need_expr_calc = false;
    }
  }

  if (OB_SUCC(ret) && need_expr_calc) {
    ObObj left_obj;
    ObObj right_obj;
    const ObObj* p_left = &left;
    const ObObj* p_right = &right;
    ObObj res_exp;
    number::ObNumber left_num;
    number::ObNumber right_num;
    if (ob_is_int_tc(left.get_type()) && ob_is_int_tc(right.get_type())) {
      if (OB_FAIL(left_num.from(left.get_int(), *expr_ctx_.calc_buf_))) {
        LOG_WARN("assign left number failed", K(ret));
      } else if (OB_FAIL(right_num.from(right.get_int(), *expr_ctx_.calc_buf_))) {
        LOG_WARN("assign right number failed", K(ret));
      } else {
        left_obj.set_number(left_num);
        right_obj.set_number(right_num);
        p_left = &left_obj;
        p_right = &right_obj;
      }
    } else if (ob_is_uint_tc(left.get_type()) && ob_is_uint_tc(right.get_type())) {
      if (OB_FAIL(left_num.from(left.get_uint64(), *expr_ctx_.calc_buf_))) {
        LOG_WARN("assign left number failed", K(ret));
      } else if (OB_FAIL(right_num.from(right.get_uint64(), *expr_ctx_.calc_buf_))) {
        LOG_WARN("assign right number failed", K(ret));
      } else {
        left_obj.set_number(left_num);
        right_obj.set_number(right_num);
        p_left = &left_obj;
        p_right = &right_obj;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObExprAdd::calc_for_agg(res_exp, *p_left, *p_right, expr_ctx_, -1))) {
        SQL_ENG_LOG(WARN, "add calculate failed", K(*p_left), K(*p_right));
      } else {
        ret = clone_cell(res_exp, res);
      }
    }
  }
  return ret;
}

int ObAggregateFunction::get_result(ObNewRow& row, const common::ObTimeZoneInfo* tz_info, const int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObObj* aggr_cell = NULL;
  ObObj* aux_cell = NULL;
  ObObj result;
  int64_t aggr_idx = child_column_count_;
  int64_t aux_col_idx = 0;
  int64_t agg_udf_meta_offset = 0;
  ObRowStore::StoredRow* stored_row = NULL;
  // get stored row
  if (OB_UNLIKELY(group_id >= row_array_.count()) || OB_UNLIKELY(row.count_ < output_column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(group_id),
        "row_array_count",
        row_array_.count(),
        "row_count",
        row.count_,
        K_(output_column_count),
        K_(child_column_count));
  } else if (OB_FAIL(get_stored_row(group_id, stored_row))) {
    LOG_WARN("failed to get stored row and group_concat_array", K(ret));
  }
  int64_t group_concat_idx = 0;
  FOREACH_CNT_X(node, agg_columns_, OB_SUCC(ret))
  {
    ObItemType aggr_fun;
    bool is_distinct = false;
    const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node->expr_);
    ObCollationType cs_type = CS_TYPE_INVALID;
    int64_t agg_udf_id = GET_AGG_UDF_ID(group_id, child_column_count_, agg_columns_.count(), aggr_idx);
    if (OB_ISNULL(cexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node or expr is null", K(node->expr_));
    } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, cs_type))) {
      LOG_WARN("failed to get aggr column", K(ret));
    } else if (OB_UNLIKELY(aggr_idx >= row.count_) || OB_ISNULL(row.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_idx is invalid", K(aggr_idx), K_(row.count));
    } else {
      aggr_cell = &row.cells_[aggr_idx];
      if (T_FUN_AVG != aggr_fun && T_FUN_APPROX_COUNT_DISTINCT != aggr_fun) {
        aux_cell = NULL;
      } else {
        aux_cell = &(stored_row->reserved_cells_[output_column_count_ + aux_col_idx++]);
      }

      ObAggUdfMeta* agg_udf_meta = nullptr;
      if (T_FUN_AGG_UDF == aggr_fun) {
        if (agg_udf_meta_offset >= agg_udf_metas_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the agg udf meta is invalid", K(ret), K(agg_udf_meta_offset), K(agg_udf_metas_.count()));
        } else {
          agg_udf_meta = &agg_udf_metas_.at(agg_udf_meta_offset);
          ++agg_udf_meta_offset;
        }
      }

      if (OB_SUCC(ret) && is_distinct && is_sort_based_gby_) {
        ObAggCellCtx* cell_ctx = get_agg_cell_ctx(group_id, node->ctx_idx_);
        if (OB_ISNULL(cell_ctx) || OB_ISNULL(cell_ctx->distinct_set_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("distinct set is NULL", K(ret));
        } else {
          if (group_id > 0) {
            // Group id greater than zero in sort based group by must be rollup,
            // distinct set is sorted and iterated in rollup_process(), rewind here.
            if (OB_FAIL(cell_ctx->distinct_set_->rewind())) {
              LOG_WARN("rewind iterator failed", K(ret));
            }
          } else {
            if (OB_FAIL(cell_ctx->distinct_set_->sort())) {
              LOG_WARN("sort failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(aggr_distinct_cell(aggr_fun,
                  cell_ctx,
                  stored_row->reserved_cells_[aggr_idx],
                  aux_cell,
                  tz_info,
                  cs_type,
                  agg_udf_id,
                  agg_udf_meta))) {
            LOG_WARN("aggregate distinct cell failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      switch (aggr_fun) {
        case T_FUN_COUNT_SUM:
        case T_FUN_COUNT: {
          // sum(count(*)) should return same type as count(*), which is bigint in mysql mode
          if (share::is_mysql_mode()) {
            *aggr_cell = stored_row->reserved_cells_[aggr_idx];
          } else {
            ObObj aggr_obj = stored_row->reserved_cells_[aggr_idx];
            ObObjType res_type = common::ObNumberType;
            const ObObj* res_obj = NULL;
            EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_WARN_ON_FAIL);
            EXPR_CAST_OBJ_V2(res_type, aggr_obj, res_obj);
            if (OB_SUCC(ret)) {
              *aggr_cell = *res_obj;
            }
          }
          break;
        }
        case T_FUN_GROUPING:
        case T_FUN_MAX:
        case T_FUN_MIN:
        case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
        case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
          *aggr_cell = stored_row->reserved_cells_[aggr_idx];
          break;
        }
        case T_FUN_APPROX_COUNT_DISTINCT: {
          ObObj aux_obj = *aux_cell;
          ObExprEstimateNdv::llc_estimate_ndv(result, aux_obj, expr_ctx_);
          *aggr_cell = result;
          break;
        }
        case T_FUN_SUM:
          if (stored_row->reserved_cells_[aggr_idx].is_null()) {
            aggr_cell->set_null();
          } else {
            ObObj aggr_obj;
            aggr_obj = stored_row->reserved_cells_[aggr_idx];
            ObObjType res_type;
            if (OB_FAIL(ObExprResultTypeUtil::get_sum_result_type(res_type, aggr_obj.get_type()))) {
              OB_LOG(WARN, "fail to get sum result type", K(res_type), K(aggr_obj.get_type()));
            } else {
              const ObObj* res_obj = NULL;
              EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_WARN_ON_FAIL);
              EXPR_CAST_OBJ_V2(res_type, aggr_obj, res_obj);
              if (OB_SUCC(ret)) {
                *aggr_cell = *res_obj;
              }
            }
          }
          break;
        case T_FUN_AVG:
          if (stored_row->reserved_cells_[aggr_idx].is_null()) {
            ++aux_col_idx;
            aggr_cell->set_null();
          } else {
            ObObj aggr_obj;
            ObObj aux_obj = *aux_cell;
            aggr_obj = stored_row->reserved_cells_[aggr_idx];
            int16_t scale = cexpr->get_accuracy().get_scale();
            ret = ObExprDiv::calc_for_avg(result, aggr_obj, aux_obj, expr_ctx_, scale);
            if (OB_SUCC(ret)) {
              *aggr_cell = result;
            }
          }
          break;
        case T_FUN_GROUP_CONCAT: {
          ObGroupConcatCtx* cell_ctx = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id, node->ctx_idx_));
          ObGroupConcatRowStore* group_concat_row_store = NULL;
          int64_t concat_str_max_len = static_cast<int64_t>(group_concat_max_len_) + 1;
          if (NULL == cell_ctx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no group concat aggregation context", K(ret));
          } else if (OB_ISNULL(group_concat_row_store = cell_ctx->gc_rs_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group concat row store is NULL", K(ret));
          } else if (OB_UNLIKELY(concat_str_max_len >= CONCAT_STR_BUF_LEN)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "group_concat_max_len is too large", K(ret), K(group_concat_max_len_), LITERAL_K(CONCAT_STR_BUF_LEN));
          } else {
            // Group concat row may be iterated in rollup_process(), rewind here.
            if (group_concat_row_store->is_iterated()) {
              if (OB_FAIL(group_concat_row_store->rewind())) {
                LOG_WARN("group concat row store rewind failed", K(ret));
              }
            } else {
              if (OB_FAIL(group_concat_row_store->finish_add_row())) {
                LOG_WARN("finish add row to group concat row store failed", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            const ObNewRow* sort_row = NULL;
            bool concat_str_buf_is_full = false;
            bool first_item_printed = false;
            int64_t pos = 0;
            int64_t append_len = 0;
            // The sep length of the last output in oracle mode, used to erase the sep at the end of the result
            int64_t last_sep_len = 0;
            bool should_skip = true;
            // sep is the last row in oracle mode
            bool sep_in_last_col = (share::is_oracle_mode() && 2 == cexpr->get_real_param_col_count());
            ObString sep_str;
            // Calculation separator
            if (share::is_oracle_mode()) {
              sep_str = ObString::make_empty_string();
            } else {
              // default is ,
              sep_str = ObCharsetUtils::get_const_str(cexpr->get_collation_type(), ',');
              if (sep_str.empty()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get empty separator string", K(ret));
              }
            }
            const ObSqlExpression& sep_expr = cexpr->get_separator_param_expr();
            if (OB_SUCC(ret) && !sep_expr.is_empty()) {
              ObObj sep_obj;
              ObNewRow tmp_row;
              if (OB_FAIL(sep_expr.calc(expr_ctx_, tmp_row, sep_obj))) {  // is const expr
                LOG_WARN("fail to calc separator expression", K(ret), K(sep_expr));
              } else if (OB_FAIL(sep_obj.get_varchar(sep_str))) {
                LOG_WARN("get varchar failed", K(sep_obj));
              } else if (ObCharset::charset_type_by_coll(sep_obj.get_collation_type()) !=
                         ObCharset::charset_type_by_coll(cexpr->get_collation_type())) {
                // Four bytes for one character at most
                int64_t buf_len = sep_str.length() * 4;
                char* buf = static_cast<char*>(expr_ctx_.calc_buf_->alloc(buf_len));
                uint32_t res_len = 0;
                if (OB_ISNULL(buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("allocate memory failed", K(ret));
                } else if (OB_FAIL(ObCharset::charset_convert(sep_obj.get_collation_type(),
                               sep_str.ptr(),
                               sep_str.length(),
                               cexpr->get_collation_type(),
                               buf,
                               buf_len,
                               res_len))) {
                  LOG_WARN("charset convert failed", K(ret));
                } else {
                  sep_str = ObString(res_len, buf);
                }
              }
            }
            while (OB_SUCC(ret) && !concat_str_buf_is_full && OB_SUCC(group_concat_row_store->get_next_row(sort_row))) {
              CK(NULL != sort_row);
              // append cell
              should_skip = false;
              // check whether need skip this
              for (int64_t i = 0; OB_SUCC(ret) && !should_skip && i < cexpr->get_real_param_col_count(); ++i) {
                if (sep_in_last_col && i == cexpr->get_real_param_col_count() - 1) {
                  // In oracle mode, when the separator is null,
                  // it is regarded as an empty string, and the result is still calculated
                } else if (sort_row->cells_[i].is_null()) {
                  should_skip = true;
                }
              }
              LOG_DEBUG("group concat calc",
                  K(sort_row),
                  K(should_skip),
                  K(first_item_printed),
                  K(sep_in_last_col),
                  K(sep_str),
                  K(group_concat_row_store->is_last_row()));
              if (OB_SUCC(ret) && !should_skip && first_item_printed) {
                // append separator
                append_len = sep_str.length();
                if (append_len > concat_str_max_len - 1 - pos) {
                  append_len = concat_str_max_len - 1 - pos;
                  // truncated without error
                  concat_str_buf_is_full = true;
                  if (share::is_oracle_mode()) {
                    ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
                    LOG_WARN("result of string concatenation is too long",
                        K(ret),
                        K(pos),
                        K(append_len),
                        K(concat_str_max_len));
                  } else {
                    LOG_USER_WARN(OB_ERR_CUT_VALUE_GROUP_CONCAT, gconcat_cur_row_num_ + 1);
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_UNLIKELY(append_len < 0)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_ERROR(
                      "sep str append_len < 0", K(ret), K(sep_str), K(sep_str.length()), K(concat_str_max_len), K(pos));
                } else {
                  MEMCPY(concat_str_buf_ + pos, sep_str.ptr(), append_len);
                  pos += append_len;
                }
              }
              if (OB_SUCC(ret) && should_skip && group_concat_row_store->is_last_row() && sep_in_last_col &&
                  pos > last_sep_len) {
                pos -= last_sep_len;
              }
              // if should_skip is true,then the following loop is not executed
              for (int64_t i = 0;
                   OB_SUCC(ret) && !concat_str_buf_is_full && !should_skip && i < cexpr->get_real_param_col_count();
                   ++i) {
                if (sep_in_last_col && cexpr->get_real_param_col_count() - 1 == i &&
                    group_concat_row_store->is_last_row()) {
                  // do nothing since in oracle mode, the last value is seperator which shouldn't output when comes to
                  // the last row
                } else {
                  ObExprCtx& expr_ctx = expr_ctx_;
                  ObString tmp_str;
                  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
                  ObObj& tmp_sort_row_obj = sort_row->cells_[i];
                  cast_ctx.dest_collation_ = cexpr->get_collation_type();
                  EXPR_GET_VARCHAR_V2(tmp_sort_row_obj, tmp_str);
                  if (OB_SUCC(ret)) {
                    append_len = tmp_str.length();
                    if (sep_in_last_col && i == cexpr->get_real_param_col_count() - 1) {
                      last_sep_len = append_len;
                    }
                    if (append_len > concat_str_max_len - 1 - pos) {
                      append_len = concat_str_max_len - 1 - pos;
                      concat_str_buf_is_full = true;
                      if (share::is_oracle_mode()) {
                        ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
                        LOG_WARN("result of string concatenation is too long", K(ret), K(concat_str_max_len));
                      } else {
                        LOG_USER_WARN(OB_ERR_CUT_VALUE_GROUP_CONCAT, gconcat_cur_row_num_ + 1);
                      }
                    }
                    if (OB_FAIL(ret)) {
                    } else if (OB_UNLIKELY(append_len < 0)) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_ERROR(
                          "str append_len < 0", K(ret), K(tmp_str), K(tmp_str.length()), K(concat_str_max_len), K(pos));
                    } else {
                      MEMCPY(concat_str_buf_ + pos, tmp_str.ptr(), append_len);
                      pos += append_len;
                    }
                  }
                }
              }
              if (OB_SUCC(ret) && !should_skip && !first_item_printed) {
                first_item_printed = true;
              }
              if (OB_SUCC(ret) && !should_skip) {
                ++gconcat_cur_row_num_;
              }
            }
            if (ret != OB_ITER_END && ret != OB_SUCCESS) {
              LOG_WARN("fail to get next row", K(ret));
            } else {
              ret = OB_SUCCESS;
              ObObj concat_obj;
              if (first_item_printed) {
                ObString concat_str;
                concat_str.assign_ptr(concat_str_buf_, static_cast<int32_t>(pos));
                concat_obj.set_varchar(concat_str);
                concat_obj.set_collation_type(cexpr->get_collation_type());
              } else {
                concat_obj.set_null();  // skip all and set null
              }
              if (OB_FAIL(clone_cell(concat_obj, stored_row->reserved_cells_[aggr_idx]))) {
                LOG_WARN("fail to clone cell", K(ret), K(concat_obj));
              } else {
                *aggr_cell = stored_row->reserved_cells_[aggr_idx];
              }
            }
          }
          ++group_concat_idx;
          break;
        }
        case T_FUN_WM_CONCAT:
        case T_FUN_KEEP_WM_CONCAT: {
          ObGroupConcatCtx* cell_ctx = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id, node->ctx_idx_));
          ObObj concat_obj;
          if (OB_FAIL(get_wm_concat_result(cexpr, cell_ctx, T_FUN_KEEP_WM_CONCAT == aggr_fun, concat_obj))) {
            LOG_WARN("failed to get wm_concat result", K(ret));
          } else if (OB_FAIL(clone_cell(concat_obj, stored_row->reserved_cells_[aggr_idx]))) {
            LOG_WARN("fail to clone cell", K(ret), K(concat_obj));
          } else {
            LOG_TRACE("concat_obj", K(stored_row->reserved_cells_[aggr_idx]));
            *aggr_cell = stored_row->reserved_cells_[aggr_idx];
          }
          break;
        }
        case T_FUN_AGG_UDF: {
          ObObj& agg_result = *aggr_cell;
          ObAggUdfExeUnit agg_udf_unit;
          LOG_DEBUG("calc aggr cell", K(agg_udf_id));
          if (OB_FAIL(agg_udf_.get_refactored(agg_udf_id, agg_udf_unit))) {
            LOG_WARN("failed to get agg func", K(ret));
          } else if (OB_ISNULL(agg_udf_unit.agg_func_) || OB_ISNULL(agg_udf_unit.udf_ctx_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the agg func/ctx is null", K(ret));
          } else if (OB_FAIL(agg_udf_unit.agg_func_->process_origin_func(
                         agg_udf_buf_, agg_result, *agg_udf_unit.udf_ctx_))) {
            LOG_WARN("the udf origin func process failed", K(ret));
          } else if (OB_FAIL(agg_udf_unit.agg_func_->process_clear_func(*agg_udf_unit.udf_ctx_))) {
            LOG_WARN("the udf clear func process failed", K(ret));
          } else if (OB_FAIL(agg_udf_.erase_refactored(agg_udf_id))) {
            LOG_WARN("fail to erase hash key", K(ret));
          } else {
            agg_udf_unit.agg_func_->process_deinit_func(*agg_udf_unit.udf_ctx_);
          }
          LOG_DEBUG("get udf result", K(agg_result), K(agg_udf_id));
          break;
        }
        case T_FUN_JSON_ARRAYAGG: {
          ObGroupConcatCtx *cell_ctx = static_cast<ObGroupConcatCtx *>(get_agg_cell_ctx(group_id, node->ctx_idx_));
          ObObj concat_obj;
          if (OB_FAIL(get_json_arrayagg_result(cexpr,
                                               cell_ctx,
                                               concat_obj))) {
            LOG_WARN("failed to get json_arrayagg result", K(ret));
          } else if (OB_FAIL(clone_cell(concat_obj, stored_row->reserved_cells_[aggr_idx]))) {
            LOG_WARN("fail to clone cell", K(ret), K(concat_obj));
          } else {
            LOG_TRACE("concat_obj", K(stored_row->reserved_cells_[aggr_idx]));
            *aggr_cell = stored_row->reserved_cells_[aggr_idx];
          }
          break;
        }
        case T_FUN_JSON_OBJECTAGG: {
          ObGroupConcatCtx *cell_ctx = static_cast<ObGroupConcatCtx *>(get_agg_cell_ctx(group_id, node->ctx_idx_));
          ObObj concat_obj;
          if (OB_FAIL(get_json_objectagg_result(cexpr,
                                               cell_ctx,
                                               concat_obj))) {
            LOG_WARN("failed to get json_arrayagg result", K(ret));
          } else if (OB_FAIL(clone_cell(concat_obj, stored_row->reserved_cells_[aggr_idx]))) {
            LOG_WARN("fail to clone cell", K(ret), K(concat_obj));
          } else {
            LOG_TRACE("concat_obj", K(stored_row->reserved_cells_[aggr_idx]));
            *aggr_cell = stored_row->reserved_cells_[aggr_idx];
          }
          break;
        }
        case T_FUN_GROUP_RANK:
        case T_FUN_GROUP_DENSE_RANK:
        case T_FUN_GROUP_PERCENT_RANK:
        case T_FUN_GROUP_CUME_DIST: {
          // GroupConcatCtx is reused, because only the final calculation method is different
          ObGroupConcatCtx* cell_ctx = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id, node->ctx_idx_));
          ObGroupConcatRowStore* group_sort_row_store = NULL;
          ObSEArray<ObSortColumn, 4> sort_columns;
          if (NULL == cell_ctx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no group concat aggregation context", K(ret));
          } else if (OB_ISNULL(group_sort_row_store = cell_ctx->gc_rs_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group concat row store is NULL", K(ret));
          } else if (group_sort_row_store->is_allocate_sort() &&
                     OB_FAIL(group_sort_row_store->get_sort_columns(sort_columns))) {
            LOG_WARN("failed to get sort columns", K(ret));
          } else {
            if (group_sort_row_store->is_iterated()) {
              if (OB_FAIL(group_sort_row_store->rewind())) {
                LOG_WARN("group concat row store rewind failed", K(ret));
              }
            } else {
              if (OB_FAIL(group_sort_row_store->finish_add_row())) {
                LOG_WARN("finish add row to group concat row store failed", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            const ObNewRow* sort_row = NULL;
            int64_t rank_num = 0;
            bool need_check_order_equal = T_FUN_GROUP_DENSE_RANK == aggr_fun;
            ObNewRow* prev_row = NULL;
            int64_t total_sort_row_cnt = group_sort_row_store->get_row_count();
            while (OB_SUCC(ret) && OB_SUCC(group_sort_row_store->get_next_row(sort_row))) {
              if (OB_ISNULL(sort_row)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("group sort row store is NULL", K(ret), K(sort_row));
              } else {
                ++rank_num;
                bool find_target_rank = true;
                int32_t comp_result = -2;
                bool is_all_equal = T_FUN_GROUP_CUME_DIST == aggr_fun;  // use to cume dist(accumulated distribution)
                int64_t compare_count = cexpr->get_all_param_col_count() - cexpr->get_real_param_col_count();
                bool need_continue = true;
                for (int64_t i = 0; OB_SUCC(ret) && need_continue && i < compare_count; ++i) {
                  bool is_asc = false;
                  if (OB_UNLIKELY(i >= sort_row->count_ || i + cexpr->get_real_param_col_count() >= sort_row->count_)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("get invalid argument",
                        K(ret),
                        K(i),
                        K(sort_row->count_),
                        K(i + cexpr->get_real_param_col_count()));
                  } else {
                    ObObj& left_compar_row_obj = sort_row->cells_[i];
                    ObObj& right_compar_row_obj = sort_row->cells_[i + cexpr->get_real_param_col_count()];
                    if (OB_FAIL(compare_calc(left_compar_row_obj,
                            right_compar_row_obj,
                            cexpr->get_sort_extra_infos().at(i),
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
                }
                if (OB_SUCC(ret)) {
                  bool is_equal = false;
                  if (need_check_order_equal) {
                    if (prev_row == NULL) {
                      if (OB_FAIL(deep_copy_cur_row(prev_row, *sort_row))) {
                        LOG_WARN("fail to deep copy cur row", K(ret));
                      }
                    } else if (OB_FAIL(check_rows_equal(cexpr, sort_columns, prev_row, sort_row, is_equal))) {
                      LOG_WARN("failed to is order by item equal with prev row", K(ret));
                    } else if (is_equal) {
                      --rank_num;
                    } else {
                      if (OB_FAIL(deep_copy_cur_row(prev_row, *sort_row))) {
                        LOG_WARN("fail to deep copy cur row", K(ret));
                      }
                    }
                    if (OB_SUCC(ret)) {
                      if (find_target_rank) {
                        break;
                      }
                    }
                    // The cume dist requirement is the total number of <=,
                    // so it is necessary to continue to search for equality
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
              char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN * 3];
              ObDataBuffer allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN * 3);
              if (T_FUN_GROUP_RANK == aggr_fun || T_FUN_GROUP_DENSE_RANK == aggr_fun) {
                if (OB_FAIL(num_result.from(rank_num, allocator))) {
                  LOG_WARN("failed to create number", K(ret));
                }
              } else {
                number::ObNumber num;
                number::ObNumber num_total;
                rank_num = aggr_fun == T_FUN_GROUP_PERCENT_RANK ? rank_num - 1 : rank_num;
                total_sort_row_cnt = aggr_fun == T_FUN_GROUP_CUME_DIST ? total_sort_row_cnt + 1 : total_sort_row_cnt;
                if (OB_FAIL(num.from(rank_num, allocator))) {
                  LOG_WARN("failed to create number", K(ret));
                } else if (OB_FAIL(num_total.from(total_sort_row_cnt, allocator))) {
                  LOG_WARN("failed to div number", K(ret));
                } else if (OB_FAIL(num.div(num_total, num_result, allocator))) {
                  LOG_WARN("failed to div number", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                ObObj sort_obj;
                sort_obj.set_number(num_result);
                if (OB_FAIL(clone_cell(sort_obj, stored_row->reserved_cells_[aggr_idx]))) {
                  LOG_WARN("fail to clone cell", K(ret), K(sort_obj));
                } else {
                  *aggr_cell = stored_row->reserved_cells_[aggr_idx];
                }
              }
            }
          }
          break;
        }
        case T_FUN_GROUP_PERCENTILE_CONT:
        case T_FUN_GROUP_PERCENTILE_DISC:
        case T_FUN_MEDIAN: {
          ObGroupConcatCtx* cell_ctx = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id, node->ctx_idx_));
          ObGroupConcatRowStore* group_sort_row_store = NULL;
          if (NULL == cell_ctx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no group concat aggregation context", K(ret));
          } else if (OB_ISNULL(group_sort_row_store = cell_ctx->gc_rs_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group concat row store is NULL", K(ret));
          } else if (!group_sort_row_store->is_allocate_sort()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group concat row store set no sort", K(ret));
          } else if (2 != cexpr->get_all_param_col_count() || 1 != cexpr->get_real_param_col_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected param count",
                K(ret),
                K(cexpr->get_all_param_col_count()),
                K(cexpr->get_real_param_col_count()));
          } else {
            if (group_sort_row_store->is_iterated()) {
              if (OB_FAIL(group_sort_row_store->rewind())) {
                LOG_WARN("group concat row store rewind failed", K(ret));
              }
            } else {
              if (OB_FAIL(group_sort_row_store->finish_add_row())) {
                LOG_WARN("finish add row to group concat row store failed", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            const int64_t param_idx = 0;
            const int64_t obj_idx = cexpr->get_all_param_col_count() - 1;
            const ObNewRow* sort_row = NULL;
            ObNewRow* prev_row = NULL;
            const int64_t total_row_count = group_sort_row_store->get_row_count();
            char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN];
            ObDataBuffer allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN);
            number::ObNumber factor;
            bool need_linear_inter = false;
            int64_t not_null_start_loc = 0;
            int64_t dest_loc = 0;
            int64_t row_cnt = 0;
            while (OB_SUCC(ret) && 0 == not_null_start_loc && OB_SUCC(group_sort_row_store->get_next_row(sort_row))) {
              if (OB_ISNULL(sort_row)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("group sort row store is NULL", K(ret), K(sort_row));
              } else {
                ++row_cnt;
                if (!sort_row->cells_[obj_idx].is_null()) {
                  not_null_start_loc = row_cnt;
                }
              }
            }
            if (ret != OB_ITER_END && ret != OB_SUCCESS) {
              LOG_WARN("fail to get next row", K(ret));
            } else if (ret == OB_ITER_END) {
              /*do nothing*/
            } else if (T_FUN_MEDIAN == aggr_fun) {
              if (1 == (total_row_count - not_null_start_loc) % 2) {
                need_linear_inter = true;
                if (OB_FAIL(factor.from(number::ObNumber::get_positive_zero_dot_five(), allocator))) {
                  LOG_WARN("failed to create number", K(ret));
                }
              }
              dest_loc = not_null_start_loc + (total_row_count - not_null_start_loc) / 2;
            } else if (OB_FAIL(get_percentile_param(aggr_fun,
                           sort_row->cells_[param_idx],
                           not_null_start_loc,
                           total_row_count,
                           dest_loc,
                           need_linear_inter,
                           factor,
                           allocator))) {
              LOG_WARN("get linear inter factor", K(factor));
            }
            while (OB_SUCC(ret) && row_cnt < dest_loc && OB_SUCC(group_sort_row_store->get_next_row(sort_row))) {
              if (OB_ISNULL(sort_row)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("group sort row store is NULL", K(ret), K(sort_row));
              } else {
                ++row_cnt;
              }
            }
            if (ret == OB_ITER_END && 0 == not_null_start_loc) {
              ret = OB_SUCCESS;
              result.set_null();
            } else if (OB_FAIL(ret)) {
              LOG_WARN("failed to get dest loc row", K(ret), K(dest_loc), K(row_cnt));
            } else if (OB_FAIL(deep_copy_cur_row(prev_row, *sort_row))) {
              LOG_WARN("fail to deep copy cur row", K(ret));
            } else {
              if (need_linear_inter) {
                int16_t scale = cexpr->get_accuracy().get_scale();
                if (OB_FAIL(group_sort_row_store->get_next_row(sort_row))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("group sort row store is NULL", K(ret), K(sort_row));
                } else if (OB_ISNULL(sort_row)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("group sort row store is NULL", K(ret), K(sort_row));
                } else if (OB_FAIL(linear_inter_calc(
                               result, prev_row->cells_[obj_idx], sort_row->cells_[obj_idx], factor, scale))) {
                  LOG_WARN("failed to calc linear inter",
                      K(ret),
                      K(prev_row->cells_[obj_idx]),
                      K(sort_row->cells_[obj_idx]),
                      K(factor));
                } else {
                  LOG_DEBUG("get median result",
                      K(factor),
                      K(not_null_start_loc),
                      K(dest_loc),
                      K(need_linear_inter),
                      K(prev_row->cells_[obj_idx]),
                      K(sort_row->cells_[obj_idx]),
                      K(result));
                }
              } else if (OB_FAIL(clone_cell(prev_row->cells_[obj_idx], result))) {
                LOG_WARN("failed to clone cell", K(ret), K(prev_row->cells_[obj_idx]));
              } else {
                LOG_DEBUG("get median result",
                    K(not_null_start_loc),
                    K(dest_loc),
                    K(need_linear_inter),
                    K(prev_row->cells_[obj_idx]),
                    K(result));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(clone_cell(result, stored_row->reserved_cells_[aggr_idx]))) {
              LOG_WARN("failed to clone cell", K(ret), K(result));
            } else {
              *aggr_cell = stored_row->reserved_cells_[aggr_idx];
            }
          }
          break;
        }
        case T_FUN_KEEP_MAX:
        case T_FUN_KEEP_MIN:
        case T_FUN_KEEP_SUM:
        case T_FUN_KEEP_COUNT: {
          if (OB_UNLIKELY(cexpr->get_real_param_col_count() != 1 && cexpr->get_real_param_col_count() != 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(cexpr->get_real_param_col_count()));
          } else {
            ObGroupConcatCtx* cell_ctx = static_cast<ObGroupConcatCtx*>(get_agg_cell_ctx(group_id, node->ctx_idx_));
            ObGroupConcatRowStore* group_sort_row_store = NULL;
            ObSEArray<ObSortColumn, 4> sort_columns;
            if (NULL == cell_ctx) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("no group concat aggregation context", K(ret));
            } else if (OB_ISNULL(group_sort_row_store = cell_ctx->gc_rs_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("group concat row store is NULL", K(ret));
            } else if (group_sort_row_store->is_allocate_sort() &&
                       OB_FAIL(group_sort_row_store->get_sort_columns(sort_columns))) {
              LOG_WARN("failed to get sort columns", K(ret));
            } else {
              if (group_sort_row_store->is_iterated()) {
                if (OB_FAIL(group_sort_row_store->rewind())) {
                  LOG_WARN("group concat row store rewind failed", K(ret));
                }
              } else {
                if (OB_FAIL(group_sort_row_store->finish_add_row())) {
                  LOG_WARN("finish add row to group concat row store failed", K(ret));
                }
              }
            }
            if (OB_SUCC(ret)) {
              const ObNewRow* sort_row = NULL;
              ObNewRow* first_row = NULL;
              ObObj result;
              while (OB_SUCC(ret) && OB_SUCC(group_sort_row_store->get_next_row(sort_row))) {
                bool is_equal = false;
                if (NULL == sort_row) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("group sort row store is NULL", K(ret), K(sort_row));
                } else if (first_row == NULL) {  // first time init obj value
                  if (OB_FAIL(deep_copy_cur_row(first_row, *sort_row))) {
                    LOG_WARN("fail to deep copy cur row", K(ret));
                  } else if (T_FUN_KEEP_MAX == aggr_fun || T_FUN_KEEP_MIN == aggr_fun || T_FUN_KEEP_SUM == aggr_fun) {
                    if (OB_FAIL(clone_cell(sort_row->cells_[0], result))) {
                      LOG_WARN("failed clone cell", K(ret));
                    }
                  } else if (T_FUN_KEEP_COUNT == aggr_fun) {
                    result.set_int(0);
                    if (cexpr->get_real_param_col_count() == 0 || !sort_row->cells_[0].is_null()) {
                      result.set_int(1);
                    }
                  } else {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("get unexpected aggr type", K(ret), K(aggr_fun));
                  }
                } else if (OB_FAIL(check_rows_equal(cexpr, sort_columns, first_row, sort_row, is_equal))) {
                  LOG_WARN("failed to is order by item equal with prev row", K(ret));
                } else if (is_equal) {
                  switch (aggr_fun) {
                    case T_FUN_KEEP_MAX: {
                      if (OB_FAIL(max_calc(result, sort_row->cells_[0], cexpr->get_collation_type()))) {
                        LOG_WARN("failed clone cell", K(ret));
                      }
                      break;
                    }
                    case T_FUN_KEEP_MIN: {
                      if (OB_FAIL(min_calc(result, sort_row->cells_[0], cexpr->get_collation_type()))) {
                        LOG_WARN("failed clone cell", K(ret));
                      }
                      break;
                    }
                    case T_FUN_KEEP_SUM: {
                      if (!sort_row->cells_[0].is_null()) {
                        if (result.is_null()) {
                          result.set_int(0);
                        }
                        if (add_calc(result, result, sort_row->cells_[0], tz_info)) {
                          LOG_WARN("failed to add calc", K(ret));
                        }
                      }
                      break;
                    }
                    case T_FUN_KEEP_COUNT: {
                      int64_t aux = 0;
                      if (OB_FAIL(result.get_int(aux))) {
                        LOG_WARN("fail to get aux", K(ret));
                      } else if (cexpr->get_real_param_col_count() == 0 || !sort_row->cells_[0].is_null()) {
                        result.set_int(++aux);
                      }
                      break;
                    }
                    default: {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("get unexpected aggr type", K(ret), K(aggr_fun));
                      break;
                    }
                  }
                } else {  // not equal , over
                  break;
                }
              }
              if (ret != OB_ITER_END && ret != OB_SUCCESS) {
                LOG_WARN("fail to get next row", K(ret));
              } else if (OB_FAIL(clone_cell(result, stored_row->reserved_cells_[aggr_idx]))) {
                LOG_WARN("fail to clone cell", K(ret), K(result));
              } else {
                *aggr_cell = stored_row->reserved_cells_[aggr_idx];
              }
            }
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown aggr function type", K(aggr_fun));
          break;
      }  // end switch
    }
    ++aggr_idx;
  }  // end for
  for (int64_t i = 0; OB_SUCC(ret) && i < child_column_count_; ++i) {
    if (OB_UNLIKELY((i >= row.count_ || i >= stored_row->reserved_cells_count_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index is invalid", K(i), K(row));
    } else {
      stored_row->reserved_cells_[i].set_scale(-1);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_write_obj(*expr_ctx_.calc_buf_, stored_row->reserved_cells_[i], row.cells_[i]))) {
        LOG_WARN("write_obj failed", K(ret), "cell", stored_row->reserved_cells_[i]);
      }
    }
  }
  LOG_DEBUG("get result row", K(row));
  return ret;
}

//
//  res = factor * (right - left) + left
//
int ObAggregateFunction::linear_inter_calc(
    ObObj& res, const ObObj& left, const ObObj& right, const number::ObNumber& factor, const int16_t scale)
{
  int ret = OB_SUCCESS;
  ObObj obj_factor, res_sub, res_mul, tmp_res;
  obj_factor.set_number(factor);
  if (OB_ISNULL(expr_ctx_.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr context calc buf is null");
  } else if (left.get_type() != right.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type");
  } else if (!lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not oracle mode");
  } else if (!ob_is_oracle_datetime_tc(left.get_type()) && !ob_is_interval_tc(left.get_type()) &&
             !ob_is_numeric_type(left.get_type())) {
    ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
    LOG_WARN("expected numeric or date/datetime type", K(ret), K(left.get_type()));
  } else if (OB_FAIL(ObExprMinus::calc(res_sub, right, left, expr_ctx_, scale))) {
    LOG_WARN("failed to calc minus", K(ret), K(left), K(right));
  } else if (OB_FAIL(ObExprMul::calc(res_mul, obj_factor, res_sub, expr_ctx_, scale))) {
    LOG_WARN("failed to calc mul", K(ret), K(obj_factor), K(res_sub));
  } else if (OB_FAIL(ObExprAdd::calc(tmp_res, res_mul, left, expr_ctx_, scale))) {
    LOG_WARN("failed to calc add", K(ret), K(res_mul), K(left));
  } else if (OB_FAIL(clone_cell(tmp_res, res))) {
    LOG_WARN("failed to clone cell", K(ret), K(tmp_res));
  } else {
    LOG_DEBUG("get linear result", K(obj_factor), K(res_sub), K(res_mul), K(tmp_res), K(factor), K(left), K(right));
  }
  return ret;
}

int ObAggregateFunction::get_percentile_param(const ObItemType aggr_fun, const ObObj& param_obj,
    const int64_t not_null_start_loc, const int64_t total_row_count, int64_t& dest_loc, bool& need_linear_inter,
    number::ObNumber& factor, ObDataBuffer& allocator)
{
  int ret = OB_SUCCESS;
  need_linear_inter = false;
  int64_t scale = 0;
  int64_t int64_value = 0;
  char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN * 5];
  ObDataBuffer local_allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN * 5);
  number::ObNumber percentile;
  const ObObj* cast_para_obj = &param_obj;
  ObObj tmp_obj;
  if (!ob_is_numeric_type(param_obj.get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("percentile value invalid", K(ret), K(param_obj));
  } else if (!param_obj.is_number()) {
    EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_WARN_ON_FAIL);
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, param_obj, tmp_obj, cast_para_obj))) {
      LOG_WARN("cast failed.", K(ret), K(param_obj), K(ObNumberType));
    }
  }
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (!param_obj.is_number() || OB_FAIL(percentile.from(cast_para_obj->get_number(), local_allocator))) {
    LOG_WARN("failed to create number percentile", K(ret));
  } else if (percentile.is_negative() || 0 < percentile.compare(number::ObNumber::get_positive_one())) {
    ret = OB_ERR_PERCENTILE_VALUE_INVALID;
    LOG_WARN("invalid percentile value", K(ret), K(percentile));
  } else if (T_FUN_GROUP_PERCENTILE_DISC == aggr_fun) {
    //
    // PERCENTILE_DISC calculation process
    // rows are sorted by null first, null is ignored in calculation
    // percentile value (P) and the number of rows (N)
    // dest_loc = ceil(N * P) + not_null_start_loc-1
    // Special case: when P = 0, start reading directly from not_null_start_loc
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
  } else if (T_FUN_GROUP_PERCENTILE_CONT == aggr_fun) {
    //
    // PERCENTILE_CONT calculation process
    // rows are sorted by null first, null is ignored in calculation
    // percentile value (P) and the number of rows (N), row number RN = (1 + (P * (N-1))
    // CRN = CEILING(RN) and FRN = FLOOR(RN)
    // If (CRN = FRN = RN) then the result is
    // (value of expression from row at RN)
    // Otherwise the result is
    // (CRN-RN) * (value of expression for row at FRN) +
    // (RN-FRN) * (value of expression for row at CRN)
    // = (RN-FRN) * (row at CRN-row at FRN) + row at FRN
    //
    // in the program factor = (RN-FRN)
    // FRN location dest_loc = not_null_start_loc + RN-1
    // Calculate factor in the calc_linear_inter function when interpolation is needed * (obj2-obj1) + obj1
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

int ObAggregateFunction::check_rows_equal(const ObAggregateExpression* cexpr,
    common::ObIArray<ObSortColumn>& sort_columns, const ObNewRow* prev_row, const ObNewRow* curr_row, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(cexpr) || OB_ISNULL(prev_row) || OB_ISNULL(curr_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cexpr), K(prev_row), K(curr_row));
  } else {
    is_equal = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < sort_columns.count(); ++i) {
      int64_t index = i + cexpr->get_real_param_col_count();
      if (OB_UNLIKELY(index >= prev_row->count_ || index >= curr_row->count_ || i >= sort_columns.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order by projector is invalid",
            K(ret),
            K(index),
            K(i),
            K(sort_columns.count()),
            K(prev_row->count_),
            K(curr_row->count_));
      } else {
        is_equal = 0 == prev_row->cells_[index].compare(curr_row->cells_[index], sort_columns.at(i).cs_type_);
      }
    }
  }
  return ret;
}

int ObAggregateFunction::deep_copy_cur_row(ObNewRow*& prev_row, const ObNewRow cur_row)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = sizeof(ObNewRow) + cur_row.get_deep_copy_size();
  int64_t pos = sizeof(ObNewRow);
  char* buf = NULL;
  prev_row = NULL;
  if (OB_ISNULL(buf = static_cast<char*>(stored_row_buf_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc new row failed", K(ret), K(buf_len));
  } else if (OB_ISNULL(prev_row = new (buf) ObNewRow())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_row is null", K(ret), K(buf_len));
  } else if (OB_FAIL(prev_row->deep_copy(cur_row, buf, buf_len, pos))) {
    LOG_WARN("deep copy row failed", K(ret), K(buf_len), K(pos));
  } else { /*do nothing*/
  }
  return ret;
}

int ObAggregateFunction::get_result_for_empty_set(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObItemType aggr_fun;
  bool is_distinct = false;
  ObObj zero_cell;
  ObObj one_cell;
  ObObj zero_llc_bitmap;

  if (OB_UNLIKELY(row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret));
  }
  // 1. set all cells as null
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
    row.cells_[i].set_null();
  }  // end for
  // 2. init COUNT cell as 0
  //    init RANK cell as 1
  //    init DENSE_RANK cell as 1
  //    init PERCENT_RANK cell as 0
  //    init CUME_DIST cell as 1
  //    init KEEP_COUNT cell as 0
  if (lib::is_mysql_mode()) {
    // mysql mode return int type
    zero_cell.set_int(0);
    one_cell.set_int(1);
  } else {
    // oracle mode need translate into number type
    zero_cell.set_int(0);
    one_cell.set_int(1);
    ObObjType res_type = common::ObNumberType;
    const ObObj* res_obj = NULL;
    EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_WARN_ON_FAIL);
    EXPR_CAST_OBJ_V2(res_type, zero_cell, res_obj);
    if (OB_SUCC(ret) && OB_NOT_NULL(res_obj)) {
      zero_cell = *res_obj;
      res_obj = NULL;
      EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_WARN_ON_FAIL);
      EXPR_CAST_OBJ_V2(res_type, one_cell, res_obj);
      if (OB_SUCC(ret) && OB_NOT_NULL(res_obj)) {
        one_cell = *res_obj;
      }
    }
  }
  FOREACH_CNT_X(node, agg_columns_, OB_SUCC(ret))
  {
    ObCollationType tmp_cs_type = CS_TYPE_INVALID;
    const ObAggregateExpression* cexpr = static_cast<const ObAggregateExpression*>(node->expr_);
    if (OB_ISNULL(cexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node or expr is null", K(node->expr_));
    } else if (OB_FAIL(cexpr->get_aggr_column(aggr_fun, is_distinct, tmp_cs_type))) {
      LOG_WARN("failed to get aggr column", K(ret));
    } else if (T_FUN_COUNT == aggr_fun || T_FUN_COUNT_SUM == aggr_fun || T_FUN_APPROX_COUNT_DISTINCT == aggr_fun ||
               T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_fun ||
               T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == aggr_fun || T_FUN_GROUP_PERCENT_RANK == aggr_fun ||
               T_FUN_KEEP_COUNT == aggr_fun) {
      if (OB_UNLIKELY(cexpr->get_result_index() >= row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result index is out of range", K(cexpr->get_result_index()), K_(row.count));
      } else {
        if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_fun ||
            T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == aggr_fun) {
          if (OB_FAIL(llc_init(&zero_llc_bitmap))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "init zero llc bitmap in APPROX_COUNT_DISTINCT_SYNOPSIS(_MERGE) for empty set failed.");
          } else {
            row.cells_[cexpr->get_result_index()] = zero_llc_bitmap;
          }
        } else {
          row.cells_[cexpr->get_result_index()] = zero_cell;
        }
      }
    } else if (T_FUN_GROUP_RANK == aggr_fun || T_FUN_GROUP_DENSE_RANK == aggr_fun ||
               T_FUN_GROUP_CUME_DIST == aggr_fun) {
      if (OB_UNLIKELY(cexpr->get_result_index() >= row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result index is out of range", K(cexpr->get_result_index()), K_(row.count));
      } else {
        row.cells_[cexpr->get_result_index()] = one_cell;
      }
    }
  }  // end for

  return ret;
}

int ObAggregateFunction::llc_add(ObObj& res, const ObObj& left, const ObObj& right)
{
  int ret = OB_SUCCESS;
  ObString res_buf;
  ObString left_buf;
  ObString right_buf;
  if (OB_FAIL(res.get_varchar(res_buf)) || OB_FAIL(left.get_varchar(left_buf)) ||
      OB_FAIL(right.get_varchar(right_buf))) {
    LOG_WARN("buffer type is not varchar", K(ret));
  } else if (OB_UNLIKELY(res_buf.length() < LLC_NUM_BUCKETS) || OB_UNLIKELY(left_buf.length() < LLC_NUM_BUCKETS) ||
             OB_UNLIKELY(right_buf.length() < LLC_NUM_BUCKETS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size don't match", K(res_buf.length()), K(left_buf.length()), K(right_buf.length()));
  } else {
    for (int64_t i = 0; i < LLC_NUM_BUCKETS; ++i) {
      res_buf.ptr()[i] = std::max(static_cast<uint8_t>(left_buf[i]), static_cast<uint8_t>(right_buf[i]));
    }
  }
  return ret;
}

int ObAggregateFunction::llc_add_value(const uint64_t value, ObObj* llc_bitmap)
{
  int ret = OB_SUCCESS;
  uint64_t bucket_index = value >> (64 - LLC_BUCKET_BITS);
  const uint64_t pmax = ObExprEstimateNdv::llc_leading_zeros(value << LLC_BUCKET_BITS, 64 - LLC_BUCKET_BITS) + 1;
  ObString llc_bitmap_buf = llc_bitmap->get_varchar();
  ObString::obstr_size_t llc_num_buckets = llc_bitmap_buf.length();
  if (OB_UNLIKELY(!ObExprEstimateNdv::llc_is_num_buckets_valid(llc_num_buckets) || llc_num_buckets <= bucket_index)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("llc_add_value failed because number of buckets is not valid", K(llc_num_buckets));
  } else {
    if (pmax > static_cast<uint8_t>(llc_bitmap_buf[bucket_index])) {
      // pmax is not exceed 65.
      llc_bitmap_buf.ptr()[bucket_index] = static_cast<uint8_t>(pmax);
    }
  }
  return ret;
}

int ObAggregateFunction::llc_init(ObObj* llc_bitmap)
{
  int ret = OB_SUCCESS;
  char* llc_bitmap_buf = NULL;
  const int64_t llc_bitmap_size = sizeof(char) * LLC_NUM_BUCKETS;
  if (OB_ISNULL(llc_bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("llc_init failed because bitmap is null");
  } else if (OB_ISNULL(llc_bitmap_buf = static_cast<char*>(stored_row_buf_.alloc(llc_bitmap_size)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("llc_init failed because bitmap buffer allocation failed");
  } else {
    memset(llc_bitmap_buf, 0, llc_bitmap_size);
    llc_bitmap->set_varchar(llc_bitmap_buf, LLC_NUM_BUCKETS);
    llc_bitmap->set_default_collation_type();
  }
  return ret;
}

uint64_t ObAggregateFunction::llc_calc_hash_value(const ObNewRow& oprands, ObCollationType cs_type, bool& has_null_cell)
{
  has_null_cell = false;
  uint64_t hash_value = 0;
  for (int64_t i = 0; !has_null_cell && i < oprands.count_; ++i) {
    if (oprands.cells_[i].is_null()) {
      has_null_cell = true;
    } else {
      hash_value = oprands.cells_[i].is_string_type() ? oprands.cells_[i].varchar_hash(cs_type, hash_value)
                                                      : oprands.cells_[i].hash(hash_value);
    }
  }
  return hash_value;
}

int ObAggregateFunction::init_agg_udf(const ObIArray<ObAggUdfMeta>& agg_udf_metas)
{
  int ret = OB_SUCCESS;
  int64_t bucket_size = common::hash::cal_next_prime(agg_udf_metas.count() + 1);
  if (OB_FAIL(agg_udf_.create(bucket_size, common::ObModIds::OB_SQL_UDF, common::ObModIds::OB_SQL_UDF))) {
    LOG_WARN("init map failed", K(ret));
  } else if (OB_FAIL(agg_udf_metas_.assign(agg_udf_metas))) {
    LOG_WARN("failed to assign udf metas", K(ret));
  }
  return ret;
}

int ObAggregateFunction::compare_calc(
    ObObj& obj1, ObObj& obj2, const ObOpSchemaObj& extra_info, int32_t& compare_result, bool& is_asc)
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool need_cast = false;
  compare_result = -2;
  is_asc = extra_info.is_ascending_direction();
  common::ObCmpNullPos null_pos =
      extra_info.is_ascending_direction() ^ extra_info.is_null_first() ? NULL_LAST : NULL_FIRST;
  common::ObCompareCtx cmp_ctx(obj2.get_type(), obj2.get_collation_type(), true, expr_ctx_.tz_offset_, null_pos);
  if (OB_FAIL(ObRelationalExprOperator::compare_nocast(result, obj1, obj2, cmp_ctx, CO_CMP, need_cast))) {
    LOG_WARN("failed to compare objects", K(ret), K(obj1), K(obj2));
  } else if (need_cast) {
    EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_NONE);
    if (OB_FAIL(ObRelationalExprOperator::compare_cast(result, obj1, obj2, cmp_ctx, cast_ctx, CO_CMP))) {
      LOG_WARN("failed to compare objects", K(ret), K(obj1), K(obj2));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result.get_int32(compare_result))) {
      LOG_WARN("failed to get int", K(ret));
    }
  }
  return ret;
}

int ObAggregateFunction::get_wm_concat_result(
    const ObAggregateExpression*& cexpr, ObGroupConcatCtx*& cell_ctx, bool is_keep_group_concat, ObObj& concat_obj)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSortColumn, 4> sort_columns;
  ObGroupConcatRowStore* wm_concat_row_store = NULL;
  if (OB_ISNULL(cell_ctx) || OB_ISNULL(expr_ctx_.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(wm_concat_row_store = cell_ctx->gc_rs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group concat row store is NULL", K(ret));
  } else if (wm_concat_row_store->is_allocate_sort() && OB_FAIL(wm_concat_row_store->get_sort_columns(sort_columns))) {
    LOG_WARN("failed to get sort columns", K(ret));
  } else {
    // wm concat row may be iterated in rollup_process(), rewind here.
    if (wm_concat_row_store->is_iterated()) {
      if (OB_FAIL(wm_concat_row_store->rewind())) {
        LOG_WARN("group concat row store rewind failed", K(ret));
      }
    } else {
      if (OB_FAIL(wm_concat_row_store->finish_add_row())) {
        LOG_WARN("finish add row to group concat row store failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObString sep_str = ObCharsetUtils::get_const_str(cexpr->get_collation_type(), ',');
      if (sep_str.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get empty separator string", K(ret));
      } else {
        ObNewRow* first_row = NULL;
        bool need_continue = true;
        const ObNewRow* sort_row = NULL;
        int64_t str_len = 0;
        char* buf = NULL;
        int64_t buf_len = 0;
        char* tmp_buf = NULL;
        while (OB_SUCC(ret) && need_continue && OB_SUCC(wm_concat_row_store->get_next_row(sort_row))) {
          if (OB_ISNULL(sort_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(sort_row));
          } else if (is_keep_group_concat) {
            if (first_row == NULL) {
              if (OB_FAIL(deep_copy_cur_row(first_row, *sort_row))) {
                LOG_WARN("failed to deep copy cur row", K(ret));
              } else { /*do nothing*/
              }
            } else if (OB_FAIL(check_rows_equal(cexpr, sort_columns, first_row, sort_row, need_continue))) {
              LOG_WARN("failed to is order by item equal with prev row", K(ret));
            } else { /*do nothing*/
            }
          }
          if (OB_SUCC(ret) && need_continue) {
            if (sort_row->cells_[0].is_null()) {
            } else {
              ObString tmp_str;
              if (sort_row->cells_[0].is_string_type()) {
                // output without convert
                tmp_str = sort_row->cells_[0].get_string();
              } else {
                ObExprCtx& expr_ctx = expr_ctx_;
                EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
                ObObj& tmp_sort_row_obj = sort_row->cells_[0];
                cast_ctx.dest_collation_ = cexpr->get_collation_type();
                EXPR_GET_VARCHAR_V2(tmp_sort_row_obj, tmp_str);
              }
              if (OB_SUCC(ret) && tmp_str.length() > 0) {
                int64_t append_len = str_len + tmp_str.length() + sep_str.length();
                if (OB_UNLIKELY(append_len > OB_MAX_PACKET_LENGTH)) {
                  ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
                  LOG_WARN("result of string concatenation is too long", K(ret), K(buf_len), K(OB_MAX_PACKET_LENGTH));
                } else if (buf_len < append_len) {
                  char* tmp_buf = NULL;
                  buf_len = append_len * 2;
                  if (OB_ISNULL(tmp_buf = static_cast<char*>(alloc_.alloc(buf_len)))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("fail alloc memory", K(tmp_buf), K(buf_len), K(ret));
                  } else {
                    MEMCPY(tmp_buf, buf, str_len);
                    buf = tmp_buf;
                  }
                }
                if (OB_SUCC(ret)) {
                  MEMCPY(buf + str_len, tmp_str.ptr(), tmp_str.length());
                  str_len += tmp_str.length();
                  MEMCPY(buf + str_len, sep_str.ptr(), sep_str.length());
                  str_len += sep_str.length();
                }
              }
            }
          }
        }
        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
          str_len = str_len == 0 ? str_len : str_len - sep_str.length();
          if (str_len > 0) {
            concat_obj.set_lob_value(ObLongTextType, buf, str_len);
            concat_obj.set_collation_type(expr_ctx_.my_session_->get_nls_collation());
            LOG_TRACE("concat_obj", K(concat_obj), K(expr_ctx_.my_session_->get_nls_collation()));
          } else {
            concat_obj.set_null();
          }
        }
      }
    }
  }
  return ret;
}

int ObAggregateFunction::get_json_arrayagg_result(const ObAggregateExpression *&cexpr,
                                                  ObGroupConcatCtx *&cell_ctx,
                                                  ObObj &concat_obj)
{
  UNUSED(cexpr);
  int ret = OB_SUCCESS;
  ObSEArray<ObSortColumn, 4> sort_columns;
  common::ObArenaAllocator tmp_alloc;
  ObGroupConcatRowStore *json_row_store = NULL;
  if (OB_ISNULL(cell_ctx) || OB_ISNULL(expr_ctx_.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(json_row_store = cell_ctx->gc_rs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group concat row store is NULL", K(ret));
  } else if (json_row_store->is_allocate_sort() &&
             OB_FAIL(json_row_store->get_sort_columns(sort_columns))) {
    LOG_WARN("failed to get sort columns", K(ret));
  } else {
    //concat row may be iterated in rollup_process(), rewind here.
    if (json_row_store->is_iterated()) {
      if (OB_FAIL(json_row_store->rewind())) {
        LOG_WARN("group concat row store rewind failed", K(ret));
      }
    } else {
      if (OB_FAIL(json_row_store->finish_add_row())) {
        LOG_WARN("finish add row to group concat row store failed", K(ret));
      }
    }
    const ObNewRow *sort_row = NULL;
    ObJsonArray json_array(&tmp_alloc);
    while (OB_SUCC(ret) && OB_SUCC(json_row_store->get_next_row(sort_row))) {
      if (OB_ISNULL(sort_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(sort_row));
      } else if (sort_row->get_count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected column count", K(ret), K(sort_row));
      } else {
        ObObjType val_type = sort_row->cells_[0].get_type();
        ObCollationType cs_type = sort_row->cells_[0].get_collation_type();
        ObScale scale = sort_row->cells_[0].get_scale();
        ObObj converted_obj(sort_row->cells_[0]);
        ObString converted_str;
        // get value and add to json_array
        ObIJsonBase *json_val = NULL;
        if (ob_is_string_type(val_type) 
            && (ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4)) {
          ObString origin_str = converted_obj.get_string();
          if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type, converted_str, 
                                                            CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
            LOG_WARN("convert string collation failed", K(ret), K(cs_type), K(origin_str.length()));
          } else {
            converted_obj.set_string(val_type, converted_str);
            cs_type = CS_TYPE_UTF8MB4_BIN;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
          if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(converted_obj, val_type,
                                                                        &tmp_alloc, cs_type,
                                                                        json_val, false, true))) {
            LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
          }
        } else {
          if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(converted_obj, val_type,
                                                                   &tmp_alloc, scale,
                                                                   expr_ctx_.my_session_->get_timezone_info(),
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
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObString str;
      // output res
      if (OB_FAIL(json_array.get_raw_binary(str, &stored_row_buf_))) {
        LOG_WARN("get result binary failed", K(ret));
      } else {
        concat_obj.set_string(ObJsonType, str);
      }
    }
  }
  return ret;
}

int ObAggregateFunction::get_json_objectagg_result(const ObAggregateExpression *&cexpr,
                                                   ObGroupConcatCtx *&cell_ctx,
                                                   ObObj &concat_obj)
{
  UNUSED(cexpr);
  int ret = OB_SUCCESS;
  ObSEArray<ObSortColumn, 4> sort_columns;
  common::ObArenaAllocator tmp_alloc;
  ObGroupConcatRowStore *json_row_store = NULL;
  if (OB_ISNULL(cell_ctx) || OB_ISNULL(expr_ctx_.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(json_row_store = cell_ctx->gc_rs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group concat row store is NULL", K(ret));
  } else if (json_row_store->is_allocate_sort() &&
             OB_FAIL(json_row_store->get_sort_columns(sort_columns))) {
    LOG_WARN("failed to get sort columns", K(ret));
  } else {
    //concat row may be iterated in rollup_process(), rewind here.
    if (json_row_store->is_iterated()) {
      if (OB_FAIL(json_row_store->rewind())) {
        LOG_WARN("group concat row store rewind failed", K(ret));
      }
    } else {
      if (OB_FAIL(json_row_store->finish_add_row())) {
        LOG_WARN("finish add row to group concat row store failed", K(ret));
      }
    }
    const ObNewRow *sort_row = NULL;
    ObJsonObject json_object(&tmp_alloc);
    while (OB_SUCC(ret) && OB_SUCC(json_row_store->get_next_row(sort_row))) {
      if (OB_ISNULL(sort_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(sort_row));
      } else {
        const ObObj &param_obj = sort_row->get_cell(0);
        const ObObj *cast_para_obj = &param_obj;
        ObObj tmp_obj;
        ObObjType val_type0 = sort_row->get_cell(0).get_type();
        ObCollationType cs_type0 = param_obj.get_collation_type();
        if (val_type0 == ObNullType) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_WARN("null type for json_objectagg key");
        } else if (!param_obj.is_string_type()) {
          // if not string convert to string
          EXPR_DEFINE_CAST_CTX(expr_ctx_, CM_WARN_ON_FAIL);
          cast_ctx.dest_collation_ = CS_TYPE_UTF8MB4_BIN;
          if (OB_FAIL(ObObjCaster::to_type(ObLongTextType,
                                           cast_ctx,
                                           param_obj,
                                           tmp_obj,
                                           cast_para_obj))) {
            LOG_WARN("cast failed.", K(ret), K(param_obj), K(ObLongTextType));
          }
        } else if (ob_is_string_type(param_obj.get_type()) && cs_type0 == CS_TYPE_BINARY) {
          // not support binary charset as mysql 
          LOG_WARN("unsuport json string type with binary charset", K(param_obj.get_type()), K(cs_type0));
          ret = OB_ERR_INVALID_JSON_CHARSET;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
        }

        ObString key_str = cast_para_obj->get_string();
        if (OB_SUCC(ret) && ObCharset::charset_type_by_coll(cs_type0) != CHARSET_UTF8MB4) {
          // string, other non-utf8 charsets
          ObString converted_key_str;
          if (OB_FAIL(ObExprUtil::convert_string_collation(key_str, cs_type0, converted_key_str, 
                                                                CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
            LOG_WARN("convert key string collation failed", K(ret), K(cs_type0), K(key_str.length()));
          } else {
            key_str = converted_key_str;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (sort_row->get_count() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected column count", K(ret), K(sort_row->get_count()));
        } else if (OB_SUCC(ret)) {
          ObObjType val_type1 = sort_row->get_cell(1).get_type();
          ObScale scale1 = sort_row->get_cell(1).get_scale();
          ObCollationType cs_type1 = sort_row->get_cell(1).get_collation_type();
          // get key and value, and append to wrapper
          ObString key_data;
          ObIJsonBase *json_val = NULL;
          if (OB_FAIL(deep_copy_ob_string(tmp_alloc, key_str, key_data))) {
            LOG_WARN("fail copy string", K(ret), K(key_str.length()));
          } else {
            ObObj converted_obj(sort_row->get_cell(1));
            ObString converted_str;
            if (ob_is_string_type(val_type1) 
            && (ObCharset::charset_type_by_coll(cs_type1) != CHARSET_UTF8MB4)) {
              ObString origin_str = converted_obj.get_string();
              if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type1, converted_str, 
                                                              CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
                LOG_WARN("convert string collation failed", K(ret), K(cs_type1), K(origin_str.length()));
              } else {
                converted_obj.set_string(val_type1, converted_str);
                cs_type1 = CS_TYPE_UTF8MB4_BIN;
              }
            }

            if (OB_FAIL(ret)) {
            } else if (ObJsonExprHelper::is_convertible_to_json(val_type1)) {
              if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(converted_obj, val_type1,
                                                                            &tmp_alloc, cs_type1,
                                                                            json_val, false, true))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
              }
            } else {
              if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(converted_obj, val_type1,
                                                                      &tmp_alloc, val_type1,
                                                                      expr_ctx_.my_session_->get_timezone_info(),
                                                                      json_val, false))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
              }
            }

            if (OB_FAIL(ret)) {
            }else if (OB_FAIL(json_object.object_add(key_data, json_val))) {
              LOG_WARN("failed: json object add json value", K(ret));
            } else if(json_object.get_serialize_size() > OB_MAX_PACKET_LENGTH) {
              ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
              LOG_WARN("result of json_arrayagg is too long", K(ret), K(json_object.get_serialize_size()),
                                                              K(OB_MAX_PACKET_LENGTH));
            }
          }
        }
      }
    }
    if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
    }
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObString str;
      // output res
      if (OB_FAIL(json_object.get_raw_binary(str, &stored_row_buf_))) {
        LOG_WARN("get result binary failed", K(ret));
      } else {
        concat_obj.set_string(ObJsonType, str);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
