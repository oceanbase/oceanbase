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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_partition_merge.h"
#include "ob_partition_merge_util.h"
#include "storage/ob_partition_merge_task.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase {
using namespace share::schema;
using namespace common;
using namespace memtable;
using namespace storage;

namespace compaction {

/*
 * Misc
 */

static inline bool macro_cmp_ret_valid(const int64_t cmp_ret)
{
  return cmp_ret <= ObMacroRowIterator::CANNOT_COMPARE_BOTH_ARE_RANGE &&
         cmp_ret >= ObMacroRowIterator::CANNOT_COMPARE_LEFT_IS_RANGE;
}

static inline bool macro_need_open_left(const int64_t cmp_ret)
{
  return (ObMacroRowIterator::CANNOT_COMPARE_LEFT_IS_RANGE == cmp_ret ||
          ObMacroRowIterator::CANNOT_COMPARE_BOTH_ARE_RANGE == cmp_ret);
}

static inline bool macro_need_open_right(const int64_t cmp_ret)
{
  return (ObMacroRowIterator::CANNOT_COMPARE_RIGHT_IS_RANGE == cmp_ret ||
          ObMacroRowIterator::CANNOT_COMPARE_BOTH_ARE_RANGE == cmp_ret);
}

static inline bool macro_need_open(const int64_t cmp_ret)
{
  return macro_need_open_left(cmp_ret) || macro_need_open_right(cmp_ret);
}

/*
 *ObIPartitionMergeFuser
 */
ObIPartitionMergeFuser::~ObIPartitionMergeFuser()
{
  reset();
}

void ObIPartitionMergeFuser::reset()
{
  if (OB_NOT_NULL(result_row_)) {
    free_store_row(allocator_, result_row_);
    result_row_ = NULL;
  }
  schema_rowkey_column_cnt_ = 0;
  column_cnt_ = 0;
  checksum_method_ = blocksstable::CCM_UNKOWN;
  has_lob_column_ = false;
  nop_pos_.reset();
  compact_type_ = ObCompactRowType::T_INVALID;
  schema_column_ids_.reset();
  allocator_.reset();
  is_inited_ = false;
}

bool ObIPartitionMergeFuser::is_valid() const
{
  return (is_inited_ && schema_rowkey_column_cnt_ > 0 && column_cnt_ > 0 && schema_column_ids_.count() > 0 &&
          OB_NOT_NULL(result_row_));
}

int ObIPartitionMergeFuser::calc_column_checksum(const bool rewrite)
{
  int ret = OB_SUCCESS;
  UNUSED(rewrite);
  return ret;
}

int ObIPartitionMergeFuser::init(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(check_merge_param(merge_param))) {
    STORAGE_LOG(WARN, "Invalid argument to init ObIPartitionMergeFuser", K(merge_param), K(ret));
  } else if (OB_FAIL(inner_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to inner init", K(ret));
  } else if (OB_FAIL(base_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to base init", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObIPartitionMergeFuser::reset_store_row(ObStoreRow& store_row)
{
  store_row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  store_row.from_base_ = false;
  store_row.reset_dml();
  store_row.trans_id_ptr_ = nullptr;
  store_row.row_type_flag_.reset();
  MEMSET(store_row.row_val_.cells_, 0, store_row.row_val_.count_ * sizeof(ObObj));
}

int ObIPartitionMergeFuser::check_merge_param(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObMajorPartitionMergeFuser", K(merge_param), K(ret));
  } else if (OB_UNLIKELY(!merge_param.table_schema_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Unexpected merge param with major fuser", K(merge_param), K(ret));
  } else if (OB_FAIL(inner_check_merge_param(merge_param))) {
    STORAGE_LOG(WARN, "Unexpected merge param to init merge fuser", K(merge_param), K(ret));
  }

  return ret;
}

int ObIPartitionMergeFuser::base_init(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(column_cnt_ <= 0 || column_cnt_ > OB_MAX_COLUMN_NUMBER)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected column cnt", K_(column_cnt), K(ret));
  } else if (OB_FAIL(malloc_row(column_cnt_, result_row_))) {
    STORAGE_LOG(WARN, "Failed to alloc result row for fuser", K_(column_cnt), K(ret));
  } else if (OB_ISNULL(result_row_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null result row", K(ret));
  } else if (OB_FAIL(nop_pos_.init(allocator_, column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to init nop pos", K_(column_cnt), K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_1470 &&
             OB_FAIL(merge_param.table_schema_->has_lob_column(has_lob_column_, true))) {
    STORAGE_LOG(WARN, "Failed to check lob column in table schema", K(ret));
  } else {
    checksum_method_ = merge_param.checksum_method_;
    schema_rowkey_column_cnt_ = merge_param.table_schema_->get_rowkey_column_num();
    compact_type_ = ObCompactRowType::T_INVALID;
  }

  return ret;
}

int ObIPartitionMergeFuser::fuse_compact_type(const MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid null argument", K(macro_row_iters), K(ret));
  } else if (ObActionFlag::OP_DEL_ROW == result_row_->flag_) {
    compact_type_ = ObCompactRowType::T_DELETE_ROW;
  } else if (ObActionFlag::OP_ROW_EXIST == result_row_->flag_) {
    if (1 == macro_row_iters.count()) {
      if (macro_row_iters.at(0)->is_sstable_iter() && macro_row_iters.at(0)->is_base_iter()) {
        compact_type_ = ObCompactRowType::T_BASE_ROW;
      } else if (macro_row_iters.at(0)->get_curr_row()->get_dml() == T_DML_UPDATE) {
        compact_type_ = ObCompactRowType::T_UPDATE_ROW;
      } else {
        compact_type_ = ObCompactRowType::T_INSERT_ROW;
      }
    } else if (macro_row_iters.at(macro_row_iters.count() - 1)->is_base_iter()) {
      compact_type_ = ObCompactRowType::T_UPDATE_ROW;
    } else {
      compact_type_ = ObCompactRowType::T_INSERT_ROW;
    }
  }

  return ret;
}

int ObIPartitionMergeFuser::compare_row_iters_simple(ObMacroRowIterator* base_iter,  // right
    ObMacroRowIterator* macro_row_iter,                                              // left
    int64_t& cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_ISNULL(macro_row_iter) || OB_ISNULL(base_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to compare row iters", KP(macro_row_iter), KP(base_iter), K(ret));
  } else if (OB_UNLIKELY(macro_row_iter->is_end() || base_iter->is_end())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected end row iters", K(ret));
  } else if (OB_FAIL(macro_row_iter->compare(*base_iter, cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare macro_row_iter", K(ret));
  } else if (!macro_cmp_ret_valid(cmp_ret)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected macro compare result", K(cmp_ret), K(ret));
  }

  return ret;
}

int ObIPartitionMergeFuser::compare_row_iters_range(ObMacroRowIterator* base_iter,  // right
    ObMacroRowIterator* macro_row_iter,                                             // left
    int64_t& cmp_ret)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMajorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_ISNULL(macro_row_iter) || OB_ISNULL(base_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to compare rowiters", KP(macro_row_iter), KP(base_iter), K(ret));
  } else if (!macro_need_open(cmp_ret)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected intial compare result", K(cmp_ret), K(ret));
  } else {
    while (OB_SUCC(ret) && macro_need_open(cmp_ret)) {
      if (macro_need_open_left(cmp_ret) && OB_FAIL(macro_row_iter->open_curr_range())) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          cmp_ret = 1;
        } else {
          STORAGE_LOG(WARN, "Fail to open current range", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (macro_need_open_right(cmp_ret) && OB_FAIL(base_iter->open_curr_range())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            cmp_ret = -1;
          } else {
            STORAGE_LOG(WARN, "Fail to open current range", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && macro_need_open(cmp_ret)) {
        if (OB_FAIL(macro_row_iter->compare(*base_iter, cmp_ret))) {
          STORAGE_LOG(WARN, "Failed to compare merge_row_iter", K(ret));
        } else if (!macro_cmp_ret_valid(cmp_ret)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected macro compare result", K(cmp_ret), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObIPartitionMergeFuser::next_row_iters(MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_row_iters.count(); ++i) {
    ObMacroRowIterator* cur_iter = macro_row_iters.at(i);
    if (OB_ISNULL(cur_iter)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null row iter", K(i), K(ret));
    } else {
      LOG_DEBUG("merge purge, next row iter", "row", *cur_iter->get_curr_row());
      purged_count_++;
      cur_iter->inc_purged_count();
      if (OB_FAIL(cur_iter->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "Failed to next iters", K(i), K(ret));
        }
      }
    }
  }

  return ret;
}

/*
 *ObMajorPartitionMergeFuser
 */
ObMajorPartitionMergeFuser::~ObMajorPartitionMergeFuser()
{
  reset();
}

void ObMajorPartitionMergeFuser::reset()
{
  if (OB_NOT_NULL(default_row_)) {
    free_store_row(allocator_, default_row_);
    default_row_ = NULL;
  }
  column_changed_ = NULL;
  table_schema_ = NULL;
  dependent_exprs_.reset();
  sql::ObSQLUtils::destruct_default_expr_context(expr_ctx_);
  expr_allocator_.reset();
  need_fuse_generate_ = true;
  is_full_merge_ = false;
  ObIPartitionMergeFuser::reset();
}

bool ObMajorPartitionMergeFuser::is_valid() const
{
  return ObIPartitionMergeFuser::is_valid() && OB_NOT_NULL(table_schema_) && OB_NOT_NULL(default_row_);
}

int ObMajorPartitionMergeFuser::malloc_row(int64_t column_count, storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(column_count), K(ret));
  } else if (OB_FAIL(malloc_store_row(allocator_, column_count, row, FLAT_ROW_STORE))) {
    STORAGE_LOG(WARN, "Failed to alloc store row", K(ret));
  }
  return ret;
}

int ObMajorPartitionMergeFuser::inner_check_merge_param(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!merge_param.is_major_merge() || merge_param.checksum_method_ == blocksstable::CCM_TYPE_AND_VALUE)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected merge param with major fuser", K(merge_param), K(ret));
  } else {
    ObITable* first_table = merge_param.tables_handle_->get_table(0);
    if (NULL == first_table) {
      ret = OB_ERR_SYS;
      LOG_ERROR("first table must not null", K(ret), K(merge_param));
    } else if (!first_table->is_major_sstable()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid first table type", K(ret), K(*first_table));
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::inner_init(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(merge_param.table_schema_->get_store_column_ids(schema_column_ids_))) {
    STORAGE_LOG(WARN, "Failed to get column ids", K(ret));
  } else if (OB_FAIL(malloc_store_row(allocator_, schema_column_ids_.count(), default_row_))) {
    STORAGE_LOG(WARN, "Failed to allocate default row", K(ret));
  } else if (OB_ISNULL(default_row_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null default row", K(ret));
  } else if (OB_FAIL(merge_param.table_schema_->get_orig_default_row(schema_column_ids_, default_row_->row_val_))) {
    STORAGE_LOG(WARN, "Failed to get default row from table schema", K(ret));
  } else {
    default_row_->flag_ = ObActionFlag::OP_ROW_EXIST;
    default_row_->from_base_ = false;
    table_schema_ = merge_param.table_schema_;
    column_cnt_ = schema_column_ids_.count();
    is_full_merge_ = merge_param.is_full_merge_;
    need_fuse_generate_ = true;
  }
  if (OB_SUCC(ret) && !merge_param.table_schema_->is_materialized_view()) {
    const ObColumnSchemaV2* column_schema = NULL;
    bool has_generated_column = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_column_ids_.count(); ++i) {
      if (OB_ISNULL(column_schema = merge_param.table_schema_->get_column_schema(schema_column_ids_.at(i).col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The column schema is NULL", K(ret));
      } else if (column_schema->is_generated_column() && !merge_param.table_schema_->is_storage_index_table()) {
        // the generated columns in index are always filled before insert
        // make sql expression for generated column
        ObISqlExpression* expr = NULL;
        if (OB_FAIL(sql::ObSQLUtils::make_generated_expression_from_str(default_row_->row_val_.cells_[i].get_string(),
                *merge_param.table_schema_,
                *column_schema,
                schema_column_ids_,
                allocator_,
                expr))) {
          STORAGE_LOG(WARN,
              "Failed to make sql expression",
              K(default_row_->row_val_.cells_[i].get_string()),
              K(*merge_param.table_schema_),
              K(*column_schema),
              K_(schema_column_ids),
              K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null generated expr", KP(expr), K(ret));
        } else if (OB_FAIL(dependent_exprs_.push_back(expr))) {
          STORAGE_LOG(WARN, "push back error", K(ret));
        } else {
          has_generated_column = true;
        }
      } else {
        if (OB_FAIL(dependent_exprs_.push_back(NULL))) {
          STORAGE_LOG(WARN, "push back error", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && has_generated_column) {
      if (OB_FAIL(sql::ObSQLUtils::make_default_expr_context(allocator_, expr_ctx_))) {
        STORAGE_LOG(WARN, "Failed to make default expr context ", K(ret));
      }
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::fuse_row(MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  int64_t macro_row_iters_cnt = macro_row_iters.count();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMajorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(macro_row_iters), K(ret));
  } else {
    const bool is_join_mv = table_schema_->is_materialized_view();
    nop_pos_.reset();
    reset_store_row(*result_row_);
    result_row_->row_type_flag_.flag_ = 0;

    for (int64_t i = 0; OB_SUCC(ret) && !final_result && i < macro_row_iters_cnt; ++i) {
      if (has_lob_column_ && 0 == i && ObActionFlag::OP_DEL_ROW == macro_row_iters.at(i)->get_curr_row()->flag_) {
        if (OB_FAIL(fuse_delete_row(macro_row_iters.at(i), result_row_, schema_rowkey_column_cnt_))) {
          STORAGE_LOG(WARN, "Failed to fuse delete row", K(ret));
        } else {
          final_result = true;
          STORAGE_LOG(DEBUG, "success to fuse delete row", K(ret), K(*result_row_));
        }
      } else if (OB_FAIL(storage::ObRowFuse::fuse_row(*macro_row_iters.at(i)->get_curr_row(),
                     *result_row_,
                     nop_pos_,
                     final_result,
                     NULL,
                     NULL,
                     column_cnt_))) {
        STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "success to fuse row", K(ret), K(*result_row_));
      }
      if (is_join_mv) {
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_join_mv && nop_pos_.count() > 0 && ObActionFlag::OP_ROW_EXIST == result_row_->flag_) {
      if (need_fuse_generate_ && OB_FAIL(fuse_generate_exprs())) {
        STORAGE_LOG(WARN, "Failed to fuse generage exprs", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fuse_compact_type(macro_row_iters))) {
      STORAGE_LOG(WARN, "Failed to fuse compact type", K(ret));
    } else {
      result_row_->reset_dml();
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::fuse_delete_row(
    ObMacroRowIterator* row_iter, ObStoreRow* row, const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_iter) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid null argument", KP(row), KP(row_iter), K(ret));
  } else if (ObActionFlag::OP_DEL_ROW != row_iter->get_curr_row()->flag_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row flag", K(ret));
  } else {
    const ObStoreRow* del_row = row_iter->get_curr_row();
    for (int64_t i = 0; i < rowkey_column_cnt; ++i) {
      row->row_val_.cells_[i] = del_row->row_val_.cells_[i];
    }
    for (int64_t i = rowkey_column_cnt; i < row->row_val_.count_; ++i) {
      row->row_val_.cells_[i].set_nop_value();
    }
    if (OB_SUCC(ret)) {
      row->flag_ = ObActionFlag::OP_DEL_ROW;
      row->dml_ = T_DML_DELETE;
      row->row_type_flag_ = del_row->row_type_flag_;
      row->from_base_ = del_row->from_base_;
      STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(*del_row), K(*row));
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::fuse_generate_exprs()
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  int64_t idx = -1;
  const ObColumnSchemaV2* col = NULL;
  int64_t left_cnt = 0;

  expr_allocator_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos_.count(); ++i) {
    if (OB_FAIL(nop_pos_.get_nop_pos(i, idx))) {
      STORAGE_LOG(WARN, "Failed to get nop pos", K(i), K(ret));
    } else if (idx < 0 || idx >= schema_column_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Invalid nops pos", K_(schema_column_ids), K(i), K(idx), K(ret));
    } else if (OB_UNLIKELY(NULL == (col = table_schema_->get_column_schema(schema_column_ids_.at(idx).col_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected NULl column schema", K(col), K(idx), K(ret));
    } else if (!col->is_generated_column()) {
      nop_pos_.nops_[left_cnt++] = static_cast<int16_t>(idx);
    } else if (table_schema_->is_storage_index_table()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "nop generated column occurs in a index table", K(i), K(ret));
    } else if (!col->is_column_stored_in_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "virtual column in main table should not be stored", K(*col), K(idx), K(ret));
    } else if (OB_ISNULL(dependent_exprs_.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "generated expr is NULL", K(i), K(ret));
    } else if (OB_FAIL(sql::ObSQLUtils::calc_sql_expression(dependent_exprs_.at(idx),
                   *table_schema_,
                   schema_column_ids_,
                   result_row_->row_val_,
                   expr_allocator_,
                   expr_ctx_,
                   result_row_->row_val_.cells_[idx]))) {
      STORAGE_LOG(WARN,
          "failed to calc expr from str",
          K(result_row_->row_val_),
          K_(schema_column_ids),
          K(default_row_->row_val_.cells_[idx].get_string()),
          K(ret));
    } else if (ob_is_text_tc(col->get_data_type())) {
      if (!result_row_->row_val_.cells_[idx].is_lob_inrow()) {
        const ObObj& lob_obj = result_row_->row_val_.cells_[idx];
        STORAGE_LOG(WARN,
            "[LOB] Unexpected lob obj scale from generated_exprs",
            K(lob_obj),
            "lob_scale",
            result_row_->row_val_.cells_[idx].get_scale(),
            K(ret));
        result_row_->row_val_.cells_[idx].set_lob_inrow();
      }
    } else {
      STORAGE_LOG(DEBUG, "Fuse generate epxr", K(idx), K(col->get_column_id()), K(*result_row_));
    }
  }
  if (OB_SUCC(ret)) {
    nop_pos_.count_ = left_cnt;
    if (OB_FAIL(storage::ObRowFuse::fuse_row(
            *default_row_, *result_row_, nop_pos_, final_result, NULL, column_changed_, column_cnt_))) {
      STORAGE_LOG(WARN, "Failed to fuse default row", K(ret));
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::fuse_old_row(ObMacroRowIterator* row_iter, storage::ObStoreRow* row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_iter) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid null argument", KP(row), KP(row_iter), K(ret));
  } else {
    bool final_result = false;
    nop_pos_.reset();
    if (ObActionFlag::OP_DEL_ROW == row_iter->get_curr_row()->flag_) {
      if (OB_FAIL(fuse_delete_row(row_iter, row, schema_rowkey_column_cnt_))) {
        STORAGE_LOG(WARN, "Failed to fuse delete row", K(ret));
      }
    } else if (OB_FAIL(storage::ObRowFuse::fuse_row(
                   *row_iter->get_curr_row(), *row, nop_pos_, final_result, NULL, NULL, 0 /*fake_column_count*/))) {
      STORAGE_LOG(WARN, "Failed to fuse old row", K(ret));
    } else if (OB_FAIL(storage::ObRowFuse::fuse_row(
                   *default_row_, *row, nop_pos_, final_result, NULL, NULL, 0 /*fake_column_count*/))) {
      STORAGE_LOG(WARN, "Failed to fuse default row for old row", K(ret));
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::check_row_iters_purge(
    ObMacroRowIterator* cur_iter, ObMacroRowIterator* base_row_iter, bool& can_purged)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* curr_row = NULL;
  can_purged = false;

  if (OB_ISNULL(base_row_iter) || OB_ISNULL(cur_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to check rowiters purge", KP(base_row_iter), KP(cur_iter), K(ret));
  } else if (!base_row_iter->is_base_iter()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_full_merge_) {
    // skip purte checking
  } else if (OB_ISNULL(curr_row = cur_iter->get_curr_row())) {
    // skip check not open range
  } else if (curr_row->is_delete()) {
    // we cannot trust the dml flag
    // else if (T_DML_DELETE == curr_row->get_dml() || curr_row->is_delete())
    bool is_exist = false;
    if (OB_FAIL(base_row_iter->exist(curr_row, is_exist))) {
      STORAGE_LOG(WARN, "Failed to check if rowkey exist in base iter", K(ret));
    } else if (!is_exist) {
      can_purged = true;
      LOG_DEBUG("merge check_row_iters_purge", K(can_purged), K(*curr_row));
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::find_minimum_iters(
    const MERGE_ITER_ARRAY& macro_row_iters, MERGE_ITER_ARRAY& minimum_iters)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMajorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "The macro row iters array is empty", K(macro_row_iters.count()), K(ret));
  } else {
    ObMacroRowIterator* macro_row_iter = NULL;
    bool is_purged = true;
    int64_t cmp_ret = 0;
    minimum_iters.reuse();
    while (OB_SUCC(ret) && is_purged) {
      is_purged = false;
      for (int64_t i = macro_row_iters.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_ISNULL(macro_row_iter = macro_row_iters.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null macro iter", K(ret));
        } else if (macro_row_iter->is_end()) {
          // skip end macro row iter
        } else if (minimum_iters.empty()) {
          if (OB_FAIL(minimum_iters.push_back(macro_row_iter))) {
            STORAGE_LOG(WARN, "Fail to push macro iter to minimum iters", K(ret));
          }
        } else if (OB_FAIL(compare_row_iters_simple(minimum_iters.at(0), macro_row_iter, cmp_ret))) {
          STORAGE_LOG(WARN, "Failed to compare row iters", K(ret));
          // the base sstable during major is major sstable
        } else if (macro_need_open(cmp_ret) &&
                   OB_FAIL(check_row_iters_purge(minimum_iters.at(0), macro_row_iter, is_purged))) {
          STORAGE_LOG(WARN, "Failed to check purge row iters", K(ret));
        } else if (is_purged) {
          if (OB_FAIL(next_row_iters(minimum_iters))) {
            STORAGE_LOG(WARN, "Failed to next minium row iters", K(ret));
          } else {
            STORAGE_LOG(INFO, "Macro row iters is purged", K(ret));
            minimum_iters.reuse();
            break;  // end for
          }
        } else if (macro_need_open(cmp_ret) &&
                   OB_FAIL(compare_row_iters_range(minimum_iters.at(0), macro_row_iter, cmp_ret))) {
          STORAGE_LOG(WARN, "Failed to compare row iters", K(ret));
        } else if (OB_UNLIKELY(macro_need_open(cmp_ret))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "Unexpected compare result", K(cmp_ret), K(ret));
        } else {
          if (cmp_ret < 0) {
            minimum_iters.reuse();
          }
          if (cmp_ret <= 0) {
            if (!macro_row_iter->is_end() && OB_FAIL(minimum_iters.push_back(macro_row_iter))) {
              STORAGE_LOG(WARN, "Fail to push macro_row_iter to minimum_iters", K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

/*
 *ObIncrementMajorPartitionMergeFuser
 */
ObIncrementMajorPartitionMergeFuser::~ObIncrementMajorPartitionMergeFuser()
{
  reset();
}

void ObIncrementMajorPartitionMergeFuser::reset()
{
  row_changed_ = false;
  column_changed_ = NULL;
  if (OB_NOT_NULL(old_row_)) {
    free_store_row(allocator_, old_row_);
    old_row_ = NULL;
  }
  checksum_calculator_ = NULL;
  ObMajorPartitionMergeFuser::reset();
}

bool ObIncrementMajorPartitionMergeFuser::is_valid() const
{
  return ObMajorPartitionMergeFuser::is_valid() && OB_NOT_NULL(old_row_) && OB_NOT_NULL(column_changed_);
}

int ObIncrementMajorPartitionMergeFuser::inner_check_merge_param(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!merge_param.is_major_merge() || merge_param.checksum_method_ != blocksstable::CCM_TYPE_AND_VALUE ||
                  OB_ISNULL(merge_param.checksum_calculator_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected merge param with major fuser", K(merge_param), K(ret));
  }

  return ret;
}

int ObIncrementMajorPartitionMergeFuser::inner_init(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  if (OB_FAIL(ObMajorPartitionMergeFuser::inner_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to init ObIncrementMajorPartitionMergeFuser", K(ret));
  } else if (OB_FAIL(malloc_store_row(allocator_, column_cnt_, old_row_))) {
    STORAGE_LOG(WARN, "Failed to alloc old row for fuser", K_(column_cnt), K(ret));
  } else if (OB_ISNULL(old_row_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null old row", K(ret));
  } else if (NULL == (buf = allocator_.alloc(column_cnt_ * sizeof(bool)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Failed to allocate memory for column changed array", K(ret));
  } else {
    row_changed_ = false;
    column_changed_ = new (buf) bool[column_cnt_];
    for (int64_t i = 0; i < column_cnt_; i++) {
      column_changed_[i] = true;
    }
    checksum_calculator_ = merge_param.checksum_calculator_;
  }

  return ret;
}

int ObIncrementMajorPartitionMergeFuser::fuse_row(MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMajorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(macro_row_iters), K(ret));
  } else if (OB_ISNULL(column_changed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null column changed array", K(ret));
  } else {
    row_changed_ = true;
    memset(column_changed_, 0, column_cnt_ * sizeof(bool));
    if (OB_FAIL(ObMajorPartitionMergeFuser::fuse_row(macro_row_iters))) {
      STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
    } else if (OB_FAIL(prepare_old_row(macro_row_iters))) {
      STORAGE_LOG(WARN, "Failed to get old row", K(ret));
    }
  }

  return ret;
}

int ObIncrementMajorPartitionMergeFuser::calc_column_checksum(const bool rewrite)
{
  int ret = OB_SUCCESS;
  UNUSED(rewrite);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMajorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_ISNULL(checksum_calculator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null checksum calculator", K(ret));
  } else if (!row_changed_) {
    // skip not changed row
  } else if (OB_FAIL(checksum_calculator_->calc_column_checksum(
                 checksum_method_, result_row_, old_row_, column_changed_))) {
    STORAGE_LOG(WARN, "Failed to calculate column checksum", K(ret));
  }

  return ret;
}

int ObIncrementMajorPartitionMergeFuser::prepare_old_row(const MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;

  reset_store_row(*old_row_);
  for (int64_t i = macro_row_iters.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (macro_row_iters.at(i)->get_table()->is_major_sstable()) {
      if (OB_FAIL(fuse_old_row(macro_row_iters.at(i), old_row_))) {
        STORAGE_LOG(WARN, "Fail to fuse old row, ", K(ret));
      } else if (macro_row_iters.count() == 1) {
        row_changed_ = false;
      }
    } else {
      break;
    }
  }

  return ret;
}

/*
 *ObMinorPartitionMergeFuser
 */
ObMinorPartitionMergeFuser::~ObMinorPartitionMergeFuser()
{
  reset();
}

void ObMinorPartitionMergeFuser::reset()
{
  out_cols_project_ = NULL;
  multi_version_row_info_ = NULL;
  multi_version_col_desc_gen_.reset();
  column_ids_.reset();
  need_check_curr_row_last_ = true;
  cur_first_dml_ = T_DML_UNKNOWN;
  ObIPartitionMergeFuser::reset();
}

bool ObMinorPartitionMergeFuser::is_valid() const
{
  return ObIPartitionMergeFuser::is_valid() && OB_NOT_NULL(out_cols_project_) && OB_NOT_NULL(multi_version_row_info_) &&
         column_ids_.count() > 0;
}

int ObMinorPartitionMergeFuser::inner_check_merge_param(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(merge_param.is_major_merge())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected merge param with major fuser", K(merge_param), K(ret));
  } else {
    ObITable* first_table = merge_param.tables_handle_->get_table(0);
    if (NULL == first_table) {
      ret = OB_ERR_SYS;
      LOG_ERROR("first table must not null", K(ret), K(merge_param));
    } else if (!first_table->is_multi_version_table() && !first_table->is_trans_sstable()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid first table type", K(ret), K(*first_table));
    }
  }
  return ret;
}

int ObMinorPartitionMergeFuser::inner_init(const ObMergeParameter& merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(merge_param.table_schema_->get_store_column_ids(schema_column_ids_))) {
    STORAGE_LOG(WARN, "Failed to get column ids", K(ret));
  } else if (OB_FAIL(multi_version_col_desc_gen_.init(merge_param.table_schema_))) {
    STORAGE_LOG(WARN, "Failed to init multi version col desc generator", K(ret));
  } else if (OB_FAIL(multi_version_col_desc_gen_.generate_column_ids(column_ids_))) {
    STORAGE_LOG(WARN, "Failed to generate column ids", K(ret));
  } else if (OB_FAIL(multi_version_col_desc_gen_.generate_multi_version_row_info(multi_version_row_info_))) {
    STORAGE_LOG(WARN, "Failed to generate multi version row info", K(ret));
  } else if (OB_ISNULL(multi_version_row_info_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null multi version row info", K(ret));
  } else if (OB_ISNULL(out_cols_project_ = multi_version_col_desc_gen_.get_out_cols_project())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null out cols project", K(ret));
  } else {
    column_cnt_ = multi_version_row_info_->column_cnt_;
    cur_first_dml_ = T_DML_UNKNOWN;
    need_check_curr_row_last_ = true;
  }

  return ret;
}

// fuse sparse delete row
int ObSparseMinorPartitionMergeFuser::fuse_delete_row(
    ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_iter) || OB_ISNULL(row) || !row->is_sparse_row_ || OB_ISNULL(row->column_ids_) ||
      OB_ISNULL(row_iter->get_curr_row()->column_ids_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", KP(row), KP(row_iter), K(ret));
  } else if (ObActionFlag::OP_DEL_ROW != row_iter->get_curr_row()->flag_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row flag", K(ret));
  } else {
    int64_t copy_cnt = rowkey_column_cnt;
    const ObStoreRow* del_row = row_iter->get_curr_row();
    if (!is_committed_row_(*row_iter->get_curr_row())) {
      copy_cnt = del_row->row_val_.count_;
    }
    for (int64_t i = 0; i < copy_cnt; ++i) {
      row->row_val_.cells_[i] = del_row->row_val_.cells_[i];
      row->column_ids_[i] = del_row->column_ids_[i];
    }
    if (OB_SUCC(ret)) {
      row->row_val_.count_ = copy_cnt;
      row->is_sparse_row_ = true;
      row->flag_ = ObActionFlag::OP_DEL_ROW;
      row->dml_ = T_DML_DELETE;
      row->row_type_flag_ = del_row->row_type_flag_;
      row->from_base_ = del_row->from_base_;
      STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(*del_row), K(*row), K(copy_cnt));
    }
  }

  return ret;
}

int ObSparseMinorPartitionMergeFuser::malloc_row(int64_t column_count, storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(column_count), K(ret));
  } else {
    if (OB_FAIL(malloc_store_row(allocator_, column_count, row, SPARSE_ROW_STORE))) {
      STORAGE_LOG(WARN, "Failed to alloc sparse store row", K(ret));
    }
  }
  return ret;
}

int ObSparseMinorPartitionMergeFuser::fuse_row(MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSparseMinorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(macro_row_iters), K(ret));
  } else {
    int64_t macro_row_iters_cnt = macro_row_iters.count();
    int64_t rowkey_column_cnt = schema_rowkey_column_cnt_;
    const ObIArray<int32_t>* out_cols_project = out_cols_project_;
    bool final_result = false;
    storage::ObITable* first_table = macro_row_iters.at(0)->get_table();
    ObFixedBitSet<OB_ALL_MAX_COLUMN_ID> bit_set;

    nop_pos_.reset();
    reset_store_row(*result_row_);
    result_row_->row_val_.count_ = 0;
    result_row_->is_sparse_row_ = true;
    if (first_table->is_multi_version_table()) {
      rowkey_column_cnt = macro_row_iters.at(0)->get_multi_version_rowkey_cnt();
      out_cols_project = NULL;
      if (schema_rowkey_column_cnt_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() != rowkey_column_cnt) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rowkey column cnt not match", K(ret), K(rowkey_column_cnt), K(schema_rowkey_column_cnt_));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !final_result && i < macro_row_iters_cnt; ++i) {
      const ObStoreRow* cur_row = macro_row_iters.at(i)->get_curr_row();
      if (0 == i && OB_NOT_NULL(cur_row->trans_id_ptr_)) {
        if (!cur_row->row_type_flag_.is_uncommitted_row()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("uncommitted row is not valid", K(ret), K(macro_row_iters), "row", *cur_row);
        } else {
          result_row_->trans_id_ptr_ = cur_row->trans_id_ptr_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 == i && ObActionFlag::OP_DEL_ROW == macro_row_iters.at(i)->get_curr_row()->flag_) {
        if (OB_FAIL(fuse_delete_row(macro_row_iters.at(0), result_row_, rowkey_column_cnt))) {
          STORAGE_LOG(WARN, "Failed to fuse delete row", K(ret), K(rowkey_column_cnt));
        } else {
          STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(*result_row_), K(macro_row_iters_cnt));
          final_result = true;
        }
      } else {
        if (OB_FAIL(storage::ObRowFuse::fuse_sparse_row(*macro_row_iters.at(i)->get_curr_row(),
                *result_row_,
                bit_set,
                final_result))) {  // fuse sparse row
          STORAGE_LOG(WARN, "Fail to fuse row, ", K(ret), KPC(macro_row_iters.at(i)->get_curr_row()));
        } else {
          STORAGE_LOG(DEBUG, "Success to fuse row, ", K(ret), KPC(macro_row_iters.at(i)->get_curr_row()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      set_dml(macro_row_iters, *result_row_);
      // set trans version
      const int64_t trans_version_index = macro_row_iters.at(0)->get_trans_version_index();
      if (OB_FAIL(set_multi_version_row_flag(macro_row_iters, *result_row_))) {
        STORAGE_LOG(WARN, "failed to set multi version row flag", K(ret), K_(result_row));
      } else if (!first_table->is_multi_version_table()) {
        const int64_t trans_version = first_table->get_snapshot_version();
        result_row_->row_val_.cells_[trans_version_index].set_int(-trans_version);
        result_row_->row_val_.cells_[trans_version_index + 1].set_int(0);
        result_row_->row_type_flag_.set_last_multi_version_row(true);
        result_row_->set_first_dml(T_DML_UNKNOWN);
      } else if (result_row_->row_val_.cells_[trans_version_index].is_nop_value() ||
                 !result_row_->row_val_.cells_[trans_version_index].is_int()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "multi version trans version column type is invalid",
            K(ret),
            K(trans_version_index),
            K(result_row_->row_val_.cells_[trans_version_index].get_meta()),
            K(macro_row_iters),
            "rowkey",
            first_table->get_key(),
            K(*result_row_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fuse_compact_type(macro_row_iters))) {
      STORAGE_LOG(WARN, "Failed to fuse compact type", K(ret));
    }
  }
  return ret;
}

int ObFlatMinorPartitionMergeFuser::malloc_row(int64_t column_count, storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(column_count), K(ret));
  } else {
    if (OB_FAIL(malloc_store_row(allocator_, column_count, row, FLAT_ROW_STORE))) {
      STORAGE_LOG(WARN, "Failed to alloc sparse store row", K(ret));
    }
  }
  return ret;
}

int ObFlatMinorPartitionMergeFuser::fuse_row(MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSplitPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(macro_row_iters), K(ret));
  } else {
    int64_t macro_row_iters_cnt = macro_row_iters.count();
    int64_t rowkey_column_cnt = schema_rowkey_column_cnt_;
    bool final_result = false;
    storage::ObITable* first_table = macro_row_iters.at(0)->get_table();

    nop_pos_.reset();
    reset_store_row(*result_row_);
    if (first_table->is_multi_version_table()) {
      rowkey_column_cnt = macro_row_iters.at(0)->get_multi_version_rowkey_cnt();
      if (schema_rowkey_column_cnt_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() != rowkey_column_cnt) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rowkey column cnt not match", K(ret), K(rowkey_column_cnt), K(schema_rowkey_column_cnt_));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !final_result && i < macro_row_iters_cnt; ++i) {
      const ObStoreRow* cur_row = macro_row_iters.at(i)->get_curr_row();
      LOG_DEBUG("fuse row", K(ret), K(*cur_row), K(*macro_row_iters.at(i)));
      if (0 == i && OB_NOT_NULL(cur_row->trans_id_ptr_)) {
        if (!cur_row->row_type_flag_.is_uncommitted_row()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR(
              "uncommitted row is not valid", K(ret), K(macro_row_iters), "row", *cur_row, K(cur_row->trans_id_ptr_));
        } else {
          result_row_->trans_id_ptr_ = cur_row->trans_id_ptr_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 == i && ObActionFlag::OP_DEL_ROW == macro_row_iters.at(i)->get_curr_row()->flag_) {
        if (OB_FAIL(fuse_delete_row(macro_row_iters.at(0), result_row_, rowkey_column_cnt))) {
          STORAGE_LOG(WARN, "Failed to fuse delete row", K(ret), K(rowkey_column_cnt));
        } else {
          STORAGE_LOG(DEBUG,
              "success to fuse delete row",
              K(ret),
              K(*macro_row_iters.at(i)->get_curr_row()),
              K(*result_row_),
              K(macro_row_iters_cnt));
          final_result = true;
        }
      } else {
        if (OB_FAIL(storage::ObRowFuse::fuse_row(*macro_row_iters.at(i)->get_curr_row(),
                *result_row_,
                nop_pos_,
                final_result,
                macro_row_iters.at(i)->get_table()->is_multi_version_table() ? NULL : out_cols_project_,
                NULL,
                column_cnt_))) {
          STORAGE_LOG(WARN, "Fail to fuse row, ", K(ret));
        } else if (result_row_->row_val_.count_ != column_cnt_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN,
              "row count is not valid",
              K(ret),
              K(i),
              KPC(cur_row),
              KPC(result_row_),
              KPC(macro_row_iters.at(i)),
              KPC(macro_row_iters.at(i)->get_table()),
              KPC(this));
        } else {
          STORAGE_LOG(DEBUG,
              "Success to fuse row, ",
              K(ret),
              K(*macro_row_iters.at(i)),
              K(*macro_row_iters.at(i)->get_curr_row()),
              K(*result_row_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      set_dml(macro_row_iters, *result_row_);
      // set trans version
      const int64_t trans_version_index = macro_row_iters.at(0)->get_trans_version_index();
      if (OB_FAIL(set_multi_version_row_flag(macro_row_iters, *result_row_))) {
        STORAGE_LOG(WARN, "failed to set multi version row flag", K(ret), K_(result_row));
      } else if (!first_table->is_multi_version_table()) {
        const int64_t trans_version = first_table->get_snapshot_version();
        result_row_->row_val_.cells_[trans_version_index].set_int(-trans_version);
        result_row_->row_val_.cells_[trans_version_index + 1].set_int(0);
        result_row_->row_type_flag_.set_last_multi_version_row(true);
        result_row_->set_first_dml(T_DML_UNKNOWN);
      } else if (result_row_->row_val_.cells_[trans_version_index].is_nop_value() ||
                 !result_row_->row_val_.cells_[trans_version_index].is_int()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "multi version trans version column type is invalid",
            K(ret),
            K(trans_version_index),
            K(result_row_->row_val_.cells_[trans_version_index].get_meta()),
            K(macro_row_iters.count()),
            "rowkey",
            first_table->get_key(),
            K(*result_row_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fuse_compact_type(macro_row_iters))) {
      STORAGE_LOG(WARN, "Failed to fuse compact type", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "fuse row", K(ret), KPC(result_row_), K(macro_row_iters_cnt));
    }
  }
  return ret;
}

// fuse delete flat row
int ObFlatMinorPartitionMergeFuser::fuse_delete_row(
    ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_iter) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", KP(row), KP(row_iter), K(ret));
  } else if (ObActionFlag::OP_DEL_ROW != row_iter->get_curr_row()->flag_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row flag", K(ret));
  } else {
    int64_t copy_cnt = rowkey_column_cnt;
    if (!is_committed_row_(*row_iter->get_curr_row())) {
      copy_cnt = row->row_val_.count_;
    }
    const ObStoreRow* del_row = row_iter->get_curr_row();
    for (int64_t i = 0; i < copy_cnt; ++i) {
      row->row_val_.cells_[i] = del_row->row_val_.cells_[i];
    }
    for (int64_t i = copy_cnt; i < row->row_val_.count_; ++i) {
      row->row_val_.cells_[i].set_nop_value();
    }
    if (OB_SUCC(ret)) {
      row->flag_ = ObActionFlag::OP_DEL_ROW;
      row->dml_ = T_DML_DELETE;
      row->row_type_flag_ = del_row->row_type_flag_;
      row->from_base_ = del_row->from_base_;
      STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(*del_row), K(*row));
    }
  }

  return ret;
}

int ObMinorPartitionMergeFuser::compare_row_iters(ObMacroRowIterator* base_iter,  // right
    ObMacroRowIterator* macro_row_iter,                                           // left
    int64_t& cmp_ret, memtable::ObNopBitMap& nop_pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMinoPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else {
    ObITable::TableType minimum_iter_table_type = base_iter->get_table()->get_key().table_type_;
    ObITable::TableType macro_row_iter_table_type = macro_row_iter->get_table()->get_key().table_type_;
    if (ObITable::is_old_minor_sstable(macro_row_iter_table_type) ||
        ObITable::is_old_minor_sstable(minimum_iter_table_type)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(
          ERROR, "unexpected table type", K(ret), KPC(base_iter->get_table()), KPC(macro_row_iter->get_table()));
    } else if (OB_FAIL(macro_row_iter->multi_version_compare(*base_iter, cmp_ret))) {
      STORAGE_LOG(WARN, "Fail to compare multi version row iter", K(ret));
    } else {
      if ((macro_row_iter->is_multi_version_compacted_row() && base_iter->is_multi_version_compacted_row()) ||
          (base_iter->is_multi_version_compacted_row() && ObITable::is_old_minor_sstable(macro_row_iter_table_type))) {
        STORAGE_LOG(DEBUG,
            "compact two row",
            K(*macro_row_iter->get_curr_row()),
            K(*base_iter->get_curr_row()),
            K(nop_pos.get_nop_cnt()),
            K(cmp_ret));
        if (macro_row_iter->get_curr_row()->is_sparse_row_) {  // compact sparse row
          if (OB_FAIL(base_iter->compact_multi_version_sparse_row(*macro_row_iter))) {
            STORAGE_LOG(WARN, "Fail to compact multi version sparse row", K(ret));
          }
        } else if (nop_pos.get_nop_cnt() > 0) {  // compact flat row
          if (OB_FAIL(base_iter->compact_multi_version_row(*macro_row_iter, nop_pos))) {
            STORAGE_LOG(WARN, "Fail to compact multi version row", K(ret));
          }
        }
        // the first multi version row is always a compacted row,
        // if we have two compacted rows, the compacted row from older table must
        // not be the first multi version row any more
        macro_row_iter->reset_first_multi_version_row_flag();
        // inherit the first dml from oldest multi-version minor sstable
        base_iter->set_curr_row_first_dml(macro_row_iter->get_curr_row()->get_first_dml());
      }
      if (OB_SUCC(ret) && 0 != cmp_ret) {
        // meet row with smaller multi_version_rowkey, curr row can't have Last flag
        need_check_curr_row_last_ = false;
      }
    }
  }

  return ret;
}

int ObMinorPartitionMergeFuser::find_minimum_iters(
    const MERGE_ITER_ARRAY& macro_row_iters, MERGE_ITER_ARRAY& minimum_iters)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMinorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "The macro row iters array is empty", K(macro_row_iters.count()), K(ret));
  } else {
    ObMacroRowIterator* macro_row_iter = NULL;
    int64_t schema_rowkey_cmp_ret = 0;
    int64_t multi_version_rowkey_cmp_ret = 0;
    memtable::ObNopBitMap nop_pos;
    int64_t column_cnt = macro_row_iters.at(macro_row_iters.count() - 1)->get_row_column_cnt();
    int64_t rowkey_cnt = macro_row_iters.at(macro_row_iters.count() - 1)->get_rowkey_column_cnt();
    minimum_iters.reuse();
    need_check_curr_row_last_ = true;
    if (OB_FAIL(nop_pos.init(column_cnt, rowkey_cnt))) {
      STORAGE_LOG(WARN, "Failed to ini noppos", K(ret));
    }
    for (int64_t i = macro_row_iters.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      schema_rowkey_cmp_ret = 0;
      multi_version_rowkey_cmp_ret = 0;
      if (OB_ISNULL(macro_row_iter = macro_row_iters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro iter", K(ret));
      } else if (macro_row_iter->is_end()) {
        // skip end macro row iter
      } else if (minimum_iters.empty()) {
        if (OB_FAIL(minimum_iters.push_back(macro_row_iter))) {
          STORAGE_LOG(WARN, "Fail to push macro iter to minimum iters", K(ret));
        }
      } else if (OB_FAIL(compare_row_iters_simple(minimum_iters.at(0), macro_row_iter, schema_rowkey_cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare row iters", K(ret));
      } else if (macro_need_open(schema_rowkey_cmp_ret) &&
                 OB_FAIL(compare_row_iters_range(minimum_iters.at(0), macro_row_iter, schema_rowkey_cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare row iters", K(ret));
      } else if (OB_UNLIKELY(macro_need_open(schema_rowkey_cmp_ret))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "Unexpected compare result", K(schema_rowkey_cmp_ret), K(ret));
      } else if (0 == schema_rowkey_cmp_ret &&
                 OB_FAIL(
                     compare_row_iters(minimum_iters.at(0), macro_row_iter, multi_version_rowkey_cmp_ret, nop_pos))) {
        STORAGE_LOG(WARN, "Failed to compare row minor iters", K(ret));
      } else {
        STORAGE_LOG(DEBUG,
            "compare row minor iters",
            K(schema_rowkey_cmp_ret),
            K(multi_version_rowkey_cmp_ret),
            KPC(minimum_iters.at(0)->get_curr_row()),
            KPC(macro_row_iter->get_curr_row()),
            K(need_check_curr_row_last_));

        if (schema_rowkey_cmp_ret < 0) {
          need_check_curr_row_last_ = true;
        }
        if (schema_rowkey_cmp_ret < 0 || (0 == schema_rowkey_cmp_ret && multi_version_rowkey_cmp_ret < 0)) {
          nop_pos.reset();
          minimum_iters.reuse();
        }
        if (schema_rowkey_cmp_ret < 0 || (0 == schema_rowkey_cmp_ret && multi_version_rowkey_cmp_ret <= 0)) {
          if (OB_FAIL(minimum_iters.push_back(macro_row_iter))) {
            STORAGE_LOG(WARN, "Fail to push macro_row_iter to minimum_iters", K(ret));
          } else {
            STORAGE_LOG(DEBUG,
                "Success to push macro_row_iter to minimum_iters",
                K(ret),
                K(schema_rowkey_cmp_ret),
                K(multi_version_rowkey_cmp_ret),
                K(macro_row_iter),
                KPC(macro_row_iter->get_curr_row()),
                KPC(macro_row_iter));
          }
        }
      }
    }  // end for
  }
  return ret;
}

int ObMinorPartitionMergeFuser::set_multi_version_row_flag(
    const MERGE_ITER_ARRAY& macro_row_iters, ObStoreRow& store_row)
{
  int ret = OB_SUCCESS;
  store_row.row_type_flag_.set_compacted_multi_version_row(false);
  store_row.row_type_flag_.set_first_multi_version_row(true);
  if (need_check_curr_row_last_) {
    store_row.row_type_flag_.set_last_multi_version_row(true);
  } else {
    store_row.row_type_flag_.set_last_multi_version_row(false);
  }

  for (int64_t i = 0; i < macro_row_iters.count(); ++i) {
    if (macro_row_iters.at(i)->get_curr_row()->row_type_flag_.is_compacted_multi_version_row() ||
        !macro_row_iters.at(i)->get_table()->is_multi_version_table()) {
      store_row.row_type_flag_.set_compacted_multi_version_row(true);
      break;
    }
  }
  for (int64_t i = 0; need_check_curr_row_last_ && i < macro_row_iters.count(); ++i) {
    if (!macro_row_iters.at(i)->get_curr_row()->row_type_flag_.is_last_multi_version_row()) {
      store_row.row_type_flag_.set_last_multi_version_row(false);
      break;
    }
  }
  // set first row flag
  bool is_first_multi_version_row = macro_row_iters.at(0)->get_curr_row()->row_type_flag_.is_first_multi_version_row();
  store_row.row_type_flag_.set_first_multi_version_row(is_first_multi_version_row);
  store_row.row_type_flag_.set_uncommitted_row(
      macro_row_iters.at(0)->get_curr_row()->row_type_flag_.is_uncommitted_row());
  if (macro_row_iters.at(0)->get_curr_row()->row_type_flag_.is_magic_row()) {
    for (int i = 0; OB_SUCC(ret) && i < macro_row_iters.count(); ++i) {
      if (!macro_row_iters.at(i)->get_curr_row()->row_type_flag_.is_magic_row()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "all iters should be magic row",
            K(store_row),
            KPC(macro_row_iters.at(i)->get_curr_row()),
            K(macro_row_iters));
      }
    }
    if (OB_SUCC(ret)) {
      store_row.row_type_flag_.set_magic_row(true);
    }
  }
  STORAGE_LOG(DEBUG,
      "set_multi_version_row_flag",
      K(need_check_curr_row_last_),
      K(store_row),
      KPC(macro_row_iters.at(0)->get_curr_row()));
  return ret;
}

// called when doing multi version minor merge
void ObMinorPartitionMergeFuser::set_dml(const MERGE_ITER_ARRAY& macro_row_iters, storage::ObStoreRow& store_row)
{
  // when doing multi version minor merge, dml is set for each row
  store_row.set_dml(macro_row_iters.at(0)->get_curr_row()->get_dml());
  // 1. if the fused row is the first multi version row, then its first dml is correct and we save it
  // for later multi version rows
  // 2. why not always use cur_first_dml ? Since minor merge use incremental merge as default merge algorithm,
  // we can not guarantee that cur_first_dml is always set correctly. For instance, suppose a logic row span
  // over two macro blocks and we only open the next macro block, then we have no way to know the first dml of
  // this logic row.
  // 3. why check is_multi_version_last_row? This flag indicating whether there is only one macro row iterator
  // has the current logic row. If this is true, then this iterator is from the oldest table among all tables
  // who contains this logic row, so we can safely keep its first dml.
  ObRowDml tmp_dml = macro_row_iters.at(0)->get_curr_row()->get_first_dml();
  if (store_row.row_type_flag_.is_first_multi_version_row()) {
    store_row.set_first_dml(tmp_dml);
    cur_first_dml_ = tmp_dml;
  } else {
    if (need_check_curr_row_last_) {
      store_row.set_first_dml(tmp_dml);
    } else {
      store_row.set_first_dml(cur_first_dml_);
    }
  }
}

bool ObMinorPartitionMergeFuser::is_committed_row_(const ObStoreRow& row) const
{
  return !row.row_type_flag_.is_uncommitted_row();
}

}  // namespace compaction
}  // namespace oceanbase
