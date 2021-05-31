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
#include "sql/engine/dml/ob_duplicated_key_checker.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/executor/ob_task_spliter.h"
#include "storage/ob_value_row_iterator.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
using namespace share::schema;
namespace sql {
// for main table: conflict row = rowkey + local unique index column
// for global unique index: conflict row = unique index rowkey, which saved in index_conflict_exprs_
int ObDuplicatedKeyChecker::ObUniqueIndexRowkeyIter::init(ObChunkDatumStore* datum_store)
{
  int ret = OB_SUCCESS;
  checker_datum_row_iter_.reset();
  if (OB_FAIL(datum_store->begin(checker_datum_row_iter_))) {
    LOG_WARN("fail to get store begin iter", K(ret));
  }
  return ret;
}

int ObDuplicatedKeyChecker::ObUniqueIndexRowkeyIter::get_next_conflict_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_output_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(iter_output_));
  } else if (OB_FAIL(checker_datum_row_iter_.get_next_row(*iter_output_, *eval_ctx_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from checker row iterator failed", K(ret));
    }
  }
  return ret;
}

int ObDuplicatedKeyChecker::ObUniqueIndexRowkeyIter::get_next_conflict_row(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  int64_t conflict_expr_cnt = index_info_.index_conflict_exprs_.get_size();
  int64_t conflict_column_cnt = index_info_.conflict_column_cnt_;
  ObNewRow* tmp_row = NULL;
  ObNewRow* checker_row = NULL;
  if (OB_FAIL(checker_row_iter_.get_next_row(checker_row, NULL))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from checker row iterator failed", K(ret));
    }
  } else if (conflict_expr_cnt <= 0 && conflict_column_cnt > 0) {
    row.cells_ = checker_row->cells_;
    row.count_ = conflict_column_cnt;
  } else if (OB_FAIL(ob_create_row(allocator_, conflict_column_cnt, tmp_row))) {
    // secondary unique idx
    LOG_WARN("create rowkey row failed", K(ret), K(conflict_column_cnt));
  } else if (OB_ISNULL(expr_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(expr_ctx_), K(ret));
  } else {
    int64_t column_idx = 0;
    DLIST_FOREACH(node, index_info_.index_conflict_exprs_)
    {
      CK(OB_NOT_NULL(node));
      CK(column_idx >= 0 && column_idx < conflict_column_cnt);
      OZ(node->calc(*expr_ctx_, *checker_row, tmp_row->cells_[column_idx++]));
    }
    if (OB_SUCC(ret)) {
      row.cells_ = tmp_row->cells_;
      row.count_ = conflict_column_cnt;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("get next unique index rowkey", K(row), K_(index_info));
  }
  return ret;
}

int ObDuplicatedKeyChecker::ObPrimaryRowkeyIter::init(ObChunkDatumStore* datum_store)
{
  int ret = OB_SUCCESS;
  datum_rowkey_iter_.reset();
  if (OB_FAIL(datum_store->begin(datum_rowkey_iter_))) {
    LOG_WARN("fail to get store begin iter", K(ret));
  }
  return ret;
}

int ObDuplicatedKeyChecker::ObPrimaryRowkeyIter::get_next_conflict_row(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* tmp_row = NULL;
  bool is_exists = false;
  do {
    if (OB_FAIL(rowkey_iter_.get_next_row(tmp_row, NULL))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from rowkey iterator failed", K(ret));
      }
    } else if (OB_FAIL(checker_ctx_.check_rowkey_exists(*tmp_row, is_exists))) {
      LOG_WARN("check rowkey exists failed", KPC(tmp_row));
    }
  } while (OB_SUCC(ret) && is_exists);
  if (OB_SUCC(ret)) {
    row = *tmp_row;
    LOG_DEBUG("get primary index rowkey", K(row));
  }
  return ret;
}

// iterator rowkey for lookup task
int ObDuplicatedKeyChecker::ObPrimaryRowkeyIter::get_next_conflict_row()
{
  int ret = OB_SUCCESS;
  bool is_exists = false;
  if (OB_ISNULL(iter_output_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    do {
      if (OB_FAIL(datum_rowkey_iter_.get_next_row(*iter_output_, *eval_ctx_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from rowkey iterator failed", K(ret));
        } else {
          LOG_DEBUG("datum rowkey iter end", K(ret));
        }
        // iter_output_ represent main table primary exprs
      } else if (OB_FAIL(checker_ctx_.check_rowkey_exists(*iter_output_, is_exists))) {
        LOG_WARN("check rowkey exists failed", K(ret));
      } else {
        LOG_DEBUG("primary rowkey", "row", ROWEXPR2STR(*eval_ctx_, *iter_output_), K(is_exists));
      }
    } while (OB_SUCC(ret) && is_exists);
  }
  return ret;
}

int ObConstraintKey::init_constraint_row(ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  const ObExprPtrIArray* exprs = NULL;
  if (is_primary_index_) {
    cnt = (constraint_info_ != NULL) ? constraint_info_->se_constraint_columns_.count()
                                     : constraint_column_depend_exprs_->count();
    exprs = constraint_column_depend_exprs_;
  } else if (constraint_info_ != NULL) {
    exprs = &(constraint_info_->se_constraint_columns_);
    cnt = constraint_info_->se_constraint_columns_.count();
  }
  if (OB_ISNULL(eval_ctx_) || OB_ISNULL(exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(eval_ctx_), KP(exprs), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(datums_ = static_cast<ObDatum*>(alloc.alloc(cnt * sizeof(ObDatum))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new (datums_) ObDatum[cnt];
    }
    ObDatum* datum = NULL;
    for (int i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      if (OB_ISNULL(exprs->at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KP(exprs->at(i)), K(ret));
      } else if (OB_FAIL(exprs->at(i)->eval(*eval_ctx_, datum))) {
        LOG_WARN("fail to eval expr", K(ret), K(i));
      } else {
        datums_[i] = *datum;
      }
    }
    LOG_DEBUG("init constraint row", "output", ROWEXPR2STR(*eval_ctx_, *exprs), K(cnt), K(is_primary_index_));
  }
  return ret;
}

uint64_t ObConstraintKey::hash() const
{
  uint64_t hash_code = 0;
  int ret = OB_SUCCESS;
  bool is_static_engine = (NULL != constraint_column_depend_exprs_);
  if (is_static_engine) {
    ObDatum* datum = NULL;
    CK(OB_NOT_NULL(eval_ctx_));
    if (is_primary_index_) {
      // rowkey at the header of row, so not needs to calc rowkey obj
      int64_t rowkey_cnt = (constraint_info_ != NULL) ? constraint_info_->se_constraint_columns_.count()
                                                      : constraint_column_depend_exprs_->count();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
        CK(OB_NOT_NULL(constraint_column_depend_exprs_->at(i)));
        if (OB_SUCC(ret)) {
          ObExprHashFuncType hash_func = constraint_column_depend_exprs_->at(i)->basic_funcs_->default_hash_;
          if (OB_FAIL(constraint_column_depend_exprs_->at(i)->eval(*eval_ctx_, datum))) {
            LOG_WARN("fail to eval expr", K(ret), K(i), K(*constraint_column_depend_exprs_));
          } else {
            hash_code = hash_func(*datum, hash_code);
          }
        }
      }
    } else if (constraint_info_ != NULL) {
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_info_->se_constraint_columns_.count(); i++) {
        ObExpr* expr = constraint_info_->se_constraint_columns_.at(i);
        ObExprHashFuncType hash_func = expr->basic_funcs_->default_hash_;
        OZ(expr->eval(*eval_ctx_, datum));
        if (OB_SUCC(ret)) {
          hash_code = hash_func(*datum, hash_code);
        }
      }
    }
    LOG_DEBUG("constraint hash value", K(hash_code), K(ret), K(is_primary_index_));
  } else {
    if (is_primary_index_) {
      // rowkey at the header of row, so not needs to calc rowkey obj
      int64_t rowkey_cnt =
          (constraint_info_ != NULL) ? constraint_info_->constraint_columns_.get_size() : duplicated_row_->get_count();
      for (int64_t i = 0; i < rowkey_cnt; ++i) {
        hash_code = duplicated_row_->get_cell(i).hash(hash_code);
      }
    } else if (constraint_info_ != NULL) {
      ObObj result;
      ObExprCtx expr_ctx;
      DLIST_FOREACH(node, constraint_info_->constraint_columns_)
      {
        result.reset();
        OZ(node->calc(expr_ctx, *duplicated_row_, result), node, *duplicated_row_);
        if (OB_SUCC(ret)) {
          hash_code = result.hash(hash_code);
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    right_to_die_or_duty_to_live();
  }
  return hash_code;
}

bool ObConstraintKey::operator==(const ObConstraintKey& other) const
{
  bool bret = true;
  int ret = OB_SUCCESS;
  bret = (is_primary_index_ == other.is_primary_index_);
  if (bret) {
    bool is_static_engine = (NULL != other.constraint_column_depend_exprs_);
    if (is_static_engine) {
      CK(OB_NOT_NULL(other.eval_ctx_));
      if (is_primary_index_) {
        ObDatum* r_datum = NULL;
        // rowkey at the header of row, so not needs to calc rowkey obj
        int64_t rowkey_cnt = (constraint_info_ != NULL) ? constraint_info_->se_constraint_columns_.count()
                                                        : constraint_column_depend_exprs_->count();
        for (int64_t i = 0; bret && OB_SUCC(ret) && i < rowkey_cnt; ++i) {
          CK(OB_NOT_NULL(constraint_column_depend_exprs_->at(i)));
          if (OB_SUCC(ret)) {
            ObExprCmpFuncType cmp_func = constraint_column_depend_exprs_->at(i)->basic_funcs_->null_first_cmp_;
            if (OB_FAIL(constraint_column_depend_exprs_->at(i)->eval(*other.eval_ctx_, r_datum))) {
              LOG_WARN("fail to eval expr", K(ret), K(i), K(*constraint_column_depend_exprs_));
            } else {
              bret = (0 == cmp_func(datums_[i], *r_datum));
              LOG_DEBUG("constraint cmp", K(i), K(*r_datum), K(datums_[i]), K(bret));
            }
          }
        }
      } else if (constraint_info_ != NULL) {
        bret = (constraint_info_ == other.constraint_info_);
        ObDatum* r_datum = NULL;
        for (int64_t i = 0; bret && OB_SUCC(ret) && i < constraint_info_->se_constraint_columns_.count(); i++) {
          ObExpr* expr = constraint_info_->se_constraint_columns_.at(i);
          ObExprCmpFuncType cmp_func = expr->basic_funcs_->null_first_cmp_;
          OZ(expr->eval(*eval_ctx_, r_datum));
          if (OB_SUCC(ret)) {
            bret = (0 == cmp_func(datums_[i], *r_datum));
            LOG_DEBUG("constraint cmp",
                K(i),
                K(*r_datum),
                K(datums_[i]),
                K(bret),
                K(constraint_info_->se_constraint_columns_));
          }
        }
      }
    } else {
      ObObj l_result;
      ObObj r_result;
      if (is_primary_index_) {
        // rowkey at the header of row, so not needs to calc rowkey obj
        int64_t rowkey_cnt = (constraint_info_ != NULL) ? constraint_info_->constraint_columns_.get_size()
                                                        : duplicated_row_->get_count();
        for (int64_t i = 0; bret && i < rowkey_cnt; ++i) {
          const ObObj& l_result = duplicated_row_->get_cell(i);
          const ObObj& r_result = other.duplicated_row_->get_cell(i);
          bret = (l_result == r_result);
        }
      } else {
        bret = (constraint_info_ == other.constraint_info_);
        ObExprCtx expr_ctx;
        DLIST_FOREACH_X(node, constraint_info_->constraint_columns_, (bret && OB_SUCC(ret)))
        {
          l_result.reset();
          r_result.reset();
          OZ(node->calc(expr_ctx, *duplicated_row_, l_result), node, *duplicated_row_);
          OZ(node->calc(expr_ctx, *other.duplicated_row_, r_result), node, *other.duplicated_row_);
          if (OB_SUCC(ret)) {
            bret = (l_result == r_result);
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      right_to_die_or_duty_to_live();
    }
  }
  return bret;
}

bool ObConstraintValue::operator==(const ObConstraintValue& other) const
{
  return current_row_ == other.current_row_ && current_datum_row_ == other.current_datum_row_;
}

ObDupKeyCheckerCtx::ObDupKeyCheckerCtx(ObIAllocator& allocator, ObExprCtx* expr_ctx,
    ObDMLMiniTaskExecutor& index_executor, DupKeyCheckerRowStore* checker_row_store, ObEvalCtx* eval_ctx /*=NULL*/,
    DupKeyCheckerDatumRowStore* checker_datum_row_store /*= NULL*/)
    : allocator_(allocator),
      expr_ctx_(expr_ctx),
      eval_ctx_(eval_ctx),
      checker_row_store_(checker_row_store),
      checker_datum_store_(checker_datum_row_store),
      index_executor_(index_executor),
      mini_job_(),
      unique_rowkey_result_(allocator, false),
      gui_lookup_result_(allocator, false),
      unique_index_count_(0),
      primary_rowkey_count_(0),
      has_global_unique_index_(false),
      update_incremental_row_(false),
      scan_tasks_()
{}

int ObDupKeyCheckerCtx::init(int64_t constraint_cnt, bool has_gui)
{
  int ret = OB_SUCCESS;
  has_global_unique_index_ = has_gui;
  OZ(unique_rowkey_result_.init());
  OZ(gui_lookup_result_.init());
  OZ(constraint_ctxs_.allocate_array(allocator_, constraint_cnt), constraint_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_cnt; ++i) {
    OZ(constraint_ctxs_.at(i).create(ROW_BATCH_SIZE, ObModIds::OB_HASH_BUCKET));
  }
  ObCurTraceId::TraceId execution_id;
  OX(execution_id.init(ObCurTraceId::get_addr()));
  OX(ob_job_id_.set_mini_task_type());
  OX(ob_job_id_.set_server(execution_id.get_addr()));
  OX(ob_job_id_.set_execution_id(execution_id.get_seq()));
  return ret;
}

void ObDupKeyCheckerCtx::destroy()
{
  unique_rowkey_result_.~ObMiniTaskResult();
  gui_lookup_result_.~ObMiniTaskResult();
  reset_task_info();
  // destroy constraint context
  for (int64_t i = 0; i < constraint_ctxs_.count(); ++i) {
    constraint_ctxs_.at(i).destroy();
  }
}

// conflict row iterator
int ObDupKeyCheckerCtx::get_unique_index_part_info(const ObUniqueIndexScanInfo& index_info, ObExecContext& ctx,
    ObIRowkeyIterator& rowkey_iter, ObIArray<PartRowStore>& part_row_store, bool is_static_engine /*=false*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> part_ids;
  int64_t part_idx = OB_INVALID_INDEX;
  PartRowStore* part_store = NULL;
  if (is_static_engine) {
    ObExpr* calc_part_id_expr = index_info.calc_index_part_id_expr_;
    ObDatum* partition_id_datum = NULL;
    CK(OB_NOT_NULL(calc_part_id_expr));
    int64_t conflict_expr_cnt = index_info.se_index_conflict_exprs_.count();
    int64_t conflict_column_cnt = index_info.conflict_column_cnt_;
    ObSEArray<ObExpr*, 8> conflict_exprs;
    if (conflict_expr_cnt <= 0 && conflict_column_cnt > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < conflict_column_cnt; i++) {
        OZ(conflict_exprs.push_back(rowkey_iter.get_output()->at(i)));
      }
    }
    CK(OB_NOT_NULL(ctx.get_eval_ctx()));
    while (OB_SUCC(ret) && OB_SUCC(rowkey_iter.get_next_conflict_row())) {
      if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(index_info.calc_exprs_, *ctx.get_eval_ctx()))) {
        LOG_WARN("fail to clear evaluated flag", K(ret));
      } else if (OB_FAIL(calc_part_id_expr->eval(*ctx.get_eval_ctx(), partition_id_datum))) {
        LOG_WARN("fail to calc part id", K(ret), K(calc_part_id_expr));
      } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == partition_id_datum->get_int()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("no partition matched", K(ret));
      } else {
        if (OB_FAIL(add_var_to_array_no_dup(part_ids, partition_id_datum->get_int(), &part_idx))) {
          LOG_WARN("Failed to add var to array no dup", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(part_idx < 0) || OB_UNLIKELY(part_idx > part_row_store.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part index is invalid", K(ret), K(part_idx), K(part_ids), K(part_row_store));
      } else if (OB_UNLIKELY(part_idx < part_row_store.count())) {
        part_store = &(part_row_store.at(part_idx));
      } else {
        PartRowStore empty_store;
        void* ptr = NULL;
        empty_store.partition_id_ = part_ids.at(part_idx);
        if (OB_FAIL(part_row_store.push_back(empty_store))) {
          LOG_WARN("store part range to part ranges failed", K(ret), K(part_row_store));
        } else {
          part_store = &(part_row_store.at(part_idx));
          // create new store
          if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObChunkDatumStore)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate row store failed", K(sizeof(ObRowStore)));
          } else {
            ObChunkDatumStore* datum_store = new (ptr) ObChunkDatumStore(&allocator_);
            if (OB_FAIL(datum_store->init(UINT64_MAX,
                    OB_SERVER_TENANT_ID,
                    ObCtxIds::DEFAULT_CTX_ID,
                    ObModIds::OB_SQL_CHUNK_ROW_STORE,
                    false /*enable_dump*/))) {
              LOG_WARN("fail to init datum store", K(ret));
            } else {
              part_store->datum_store_ = datum_store;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(part_store->partition_id_ != part_ids.at(part_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part range is invalid", K(ret), K(part_store), K(index_info), K(part_ids.at(part_idx)));
        } else {
          if (conflict_expr_cnt <= 0 && conflict_column_cnt > 0) {
            LOG_DEBUG("primary key conflict exprs",
                K(part_store->partition_id_),
                "output",
                ROWEXPR2STR(*ctx.get_eval_ctx(), conflict_exprs));
            OZ(part_store->datum_store_->add_row(conflict_exprs, ctx.get_eval_ctx()));
          } else {
            LOG_DEBUG("index conflict exprs",
                K(part_store->partition_id_),
                "output",
                ROWEXPR2STR(*ctx.get_eval_ctx(), index_info.se_index_conflict_exprs_));
            OZ(part_store->datum_store_->add_row(index_info.se_index_conflict_exprs_, ctx.get_eval_ctx()));
          }
        }
      }
    }  // while store end
  } else {
    ObTableLocation* index_loc = index_info.index_location_;
    ObSchemaGetterGuard schema_guard;
    ObMultiVersionSchemaService* schema_service = NULL;
    ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
    if (OB_ISNULL(index_loc) || OB_ISNULL(schema_service = task_exec_ctx.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index location is null", K(index_loc), K(schema_service));
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(),
                   schema_guard,
                   task_exec_ctx.get_query_tenant_begin_schema_version(),
                   task_exec_ctx.get_query_sys_begin_schema_version()))) {
      LOG_WARN("get schema guard from schema service failed",
          K(ret),
          K(task_exec_ctx.get_query_tenant_begin_schema_version()),
          K(task_exec_ctx.get_query_sys_begin_schema_version()));
    }
    ObNewRow rowkey_row;
    while (OB_SUCC(ret) && OB_SUCC(rowkey_iter.get_next_conflict_row(rowkey_row))) {
      if (OB_FAIL(index_loc->calculate_partition_ids_by_row(ctx, &schema_guard, rowkey_row, part_ids, part_idx))) {
        LOG_WARN("calculate partition ids by row failed", K(ret), K(rowkey_row), K(part_ids), K(part_idx));
      } else if (OB_UNLIKELY(part_idx < 0) || OB_UNLIKELY(part_idx > part_row_store.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part index is invalid", K(ret), K(part_idx), K(part_ids), K(part_row_store), K(rowkey_row));
      } else if (OB_UNLIKELY(part_idx < part_row_store.count())) {
        part_store = &(part_row_store.at(part_idx));
      } else {
        PartRowStore empty_store;
        void* ptr = NULL;
        empty_store.partition_id_ = part_ids.at(part_idx);
        if (OB_FAIL(part_row_store.push_back(empty_store))) {
          LOG_WARN("store part range to part ranges failed", K(ret), K(part_row_store));
        } else {
          part_store = &(part_row_store.at(part_idx));
          if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRowStore)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate row store failed", K(sizeof(ObRowStore)));
          } else {
            part_store->row_store_ =
                new (ptr) ObRowStore(allocator_, ObModIds::OB_SQL_ROW_STORE, OB_SERVER_TENANT_ID, false);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(part_store->partition_id_ != part_ids.at(part_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part range is invalid", K(ret), K(part_store), K(index_info), K(part_ids.at(part_idx)));
        } else if (OB_FAIL(part_store->row_store_->add_row(rowkey_row))) {
          LOG_WARN("build index range failed", K(ret), K(part_row_store));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  const bool is_weak = false;
  if (OB_FAIL(ret)) {
    LOG_WARN("get index rowkey with row failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::extend_checker_stmt(
                 ctx, index_info.table_id_, index_info.index_tid_, part_ids, is_weak))) {
    LOG_WARN("extend checker stmt failed", K(ret), K(part_ids));
  }
  return ret;
}

int ObDupKeyCheckerCtx::get_or_create_scan_task_info(
    ObAddr& runner_server, ObTaskInfo*& task_info, bool is_static_engine)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; !found && i < scan_tasks_.count(); ++i) {
    if (scan_tasks_.at(i)->get_task_location().get_server() == runner_server) {
      task_info = scan_tasks_.at(i);
      found = true;
    }
  }
  if (!found) {
    if (OB_FAIL(create_mini_task_info(task_info))) {
      LOG_WARN("create mini task info failed", K(ret));
    } else if ((is_static_engine && OB_ISNULL(checker_datum_store_)) ||
               (!is_static_engine && OB_ISNULL(checker_row_store_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(is_static_engine), KP(checker_row_store_), KP(checker_datum_store_));
    } else {
      ObTaskLocation task_loc;
      int64_t location_cnt = 0;
      if (is_static_engine) {
        location_cnt = checker_datum_store_->get_row_cnt() * constraint_ctxs_.count();
      } else {
        location_cnt = checker_row_store_->get_row_count() * constraint_ctxs_.count();
      }
      task_loc.set_ob_job_id(ob_job_id_);
      task_loc.set_task_id(scan_tasks_.count() - 1);
      task_loc.set_server(runner_server);
      task_info->set_task_location(task_loc);
      task_info->set_task_split_type(ObTaskSpliter::DISTRIBUTED_SPLIT);
      task_info->get_range_location().server_ = runner_server;
      task_info->get_range_location().part_locs_.set_allocator(&allocator_);
      if (OB_FAIL(task_info->get_range_location().part_locs_.init(location_cnt))) {
        LOG_WARN("init partition location failed", K(ret), K(location_cnt));
      }
    }
  }
  return ret;
}

int ObDupKeyCheckerCtx::create_mini_task_info(ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTaskInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate task info failed", K(ret), "task_info_size", sizeof(ObTaskInfo));
  } else {
    task_info = new (buf) ObTaskInfo(allocator_);
    if (OB_FAIL(scan_tasks_.push_back(task_info))) {
      LOG_WARN("store mini task info failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && task_info != NULL) {
    task_info->~ObTaskInfo();
    allocator_.free(task_info);
    task_info = NULL;
  }
  return ret;
}

int ObDupKeyCheckerCtx::check_rowkey_exists(const ObNewRow& rowkey, bool& is_exists)
{
  int ret = OB_SUCCESS;
  ObConstraintKey constraint_key;
  constraint_key.constraint_info_ = NULL;
  constraint_key.duplicated_row_ = &rowkey;
  constraint_key.is_primary_index_ = true;
  is_exists = (constraint_ctxs_.at(0).get(constraint_key) != NULL);
  return ret;
}

int ObDupKeyCheckerCtx::check_rowkey_exists(const ObExprPtrIArray& row_exprs, bool& is_exists)
{
  int ret = OB_SUCCESS;
  ObConstraintKey constraint_key;
  constraint_key.constraint_info_ = NULL;
  constraint_key.eval_ctx_ = eval_ctx_;
  constraint_key.constraint_column_depend_exprs_ = &row_exprs;
  constraint_key.is_primary_index_ = true;
  is_exists = (constraint_ctxs_.at(0).get(constraint_key) != NULL);
  return ret;
}

int ObDupKeyCheckerCtx::get_checker_store_begin_iter(DupKeyCheckerRowIter& row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(checker_row_store_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    row_iter = checker_row_store_->begin();
  }

  return ret;
}

int ObDupKeyCheckerCtx::get_checker_store_begin_iter(DupKeyCheckerDatumRowIter& datum_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(checker_datum_store_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(checker_datum_store_->begin(datum_iter))) {
    LOG_WARN("fail to get datum store begin iter", K(ret));
  }

  return ret;
}

int ObDupKeyCheckerCtx::fetch_unique_index_rowkey(ObExecContext& ctx, ObRowStore::Iterator& row_iter)
{
  int ret = OB_SUCCESS;
  if (!scan_tasks_.empty()) {
    if (OB_FAIL(index_executor_.execute(ctx, mini_job_, scan_tasks_, false, unique_rowkey_result_))) {
      LOG_WARN("fetch unique index data failed", K(ret), K_(mini_job), K_(scan_tasks));
    } else {
      if (has_global_unique_index_) {
        // global unique index: pkey info saved in extend result
        row_iter = unique_rowkey_result_.get_extend_result().begin();
      } else {
        row_iter = unique_rowkey_result_.get_task_result().begin();
      }
    }
  }
  return ret;
}

int ObDupKeyCheckerCtx::fetch_unique_index_rowkey(
    ObExecContext& ctx, const ObExprPtrIArray*& iter_output, DupKeyCheckerDatumRowIter& row_iter)
{
  int ret = OB_SUCCESS;
  if (!scan_tasks_.empty()) {
    if (OB_FAIL(index_executor_.execute(ctx, mini_job_, scan_tasks_, false, unique_rowkey_result_))) {
      LOG_WARN("fetch unique index data failed", K(ret), K_(mini_job), K_(scan_tasks));
    } else {
      if (has_global_unique_index_) {
        iter_output = &(mini_job_.get_extend_spec()->output_);
        OZ(unique_rowkey_result_.get_extend_result().get_datum_store().begin(row_iter));
      } else {
        iter_output = &(mini_job_.get_root_spec()->output_);
        OZ(unique_rowkey_result_.get_task_result().get_datum_store().begin(row_iter));
      }
    }
  }
  return ret;
}

int ObDupKeyCheckerCtx::fetch_gui_lookup_data(
    ObExecContext& ctx, const ObExprPtrIArray*& iter_output, ObChunkDatumStore::Iterator& datum_iter)
{
  int ret = OB_SUCCESS;
  if (!scan_tasks_.empty()) {
    if (OB_FAIL(index_executor_.execute(ctx, mini_job_, scan_tasks_, false, gui_lookup_result_))) {
      LOG_WARN("fetch unique index data failed", K(ret));
    } else if (OB_FAIL(gui_lookup_result_.get_task_result().get_datum_store().begin(datum_iter))) {
      LOG_WARN("fail to get store begin", K(ret));
    } else {
      iter_output = &(mini_job_.get_root_spec()->output_);
    }
  }
  return ret;
}

int ObDupKeyCheckerCtx::fetch_gui_lookup_data(ObExecContext& ctx, ObRowStore::Iterator& row_iter)
{
  int ret = OB_SUCCESS;
  if (!scan_tasks_.empty()) {
    if (OB_FAIL(index_executor_.execute(ctx, mini_job_, scan_tasks_, false, gui_lookup_result_))) {
      LOG_WARN("fetch unique index data failed", K(ret));
    } else {
      row_iter = gui_lookup_result_.get_task_result().begin();
    }
  }
  return ret;
}

void ObDupKeyCheckerCtx::reset_task_info()
{
  for (int64_t i = 0; i < scan_tasks_.count(); ++i) {
    if (scan_tasks_.at(i) != NULL) {
      scan_tasks_.at(i)->~ObTaskInfo();
      allocator_.free(scan_tasks_.at(i));
      scan_tasks_.at(i) = NULL;
    }
  }
  scan_tasks_.reset();
}

int ObDuplicatedKeyChecker::init_checker_ctx(ObDupKeyCheckerCtx& ctx) const
{
  return ctx.init(constraint_infos_.count(), gui_scan_infos_.count() > 0);
}

int ObDuplicatedKeyChecker::build_conflict_row_task_info_list(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx) const
{
  int ret = OB_SUCCESS;
  if (is_static_engine_) {
    const ObOpSpec* plan_root_spec = gui_scan_root_spec_;
    const ObOpSpec* extend_root_spec = table_scan_info_.index_fetcher_spec_;
    if (NULL == plan_root_spec) {
      plan_root_spec = table_scan_info_.index_fetcher_spec_;
      extend_root_spec = NULL;
    }
    if ((NULL != plan_root_spec) && OB_FAIL(plan_root_spec->create_op_input(ctx))) {
      LOG_WARN("fail to create op input", K(ret));
    } else if ((NULL != extend_root_spec) && OB_FAIL(extend_root_spec->create_op_input(ctx))) {
      LOG_WARN("fail to create op input", K(ret));
    } else {
      checker_ctx.set_mini_job(phy_plan_, plan_root_spec, extend_root_spec);
      LOG_DEBUG("mini job", KP(plan_root_spec), KP(extend_root_spec));
      ObUniqueIndexRowkeyIter table_rowkey_iter(
          checker_ctx.get_allocator(), table_columns_, table_scan_info_, ctx.get_eval_ctx());
      if (OB_FAIL(table_rowkey_iter.init(checker_ctx.get_checker_datum_store()))) {
        LOG_WARN("failed to init iterator", K(ret));
      } else if (OB_FAIL(build_conflict_row_task_info(table_scan_info_, ctx, table_rowkey_iter, checker_ctx))) {
        LOG_WARN("build table scan task info failed", K(ret), K_(table_scan_info));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < gui_scan_infos_.count(); ++i) {
        LOG_DEBUG("gui scan info", K(ret), K(i));
        const ObUniqueIndexScanInfo& index_info = gui_scan_infos_.at(i);
        ObUniqueIndexRowkeyIter unique_rowkey_iter(
            checker_ctx.get_allocator(), table_columns_, index_info, ctx.get_eval_ctx());
        if (OB_FAIL(unique_rowkey_iter.init(checker_ctx.get_checker_datum_store()))) {
          LOG_WARN("failed to init iterator", K(ret));
        } else if (OB_FAIL(build_conflict_row_task_info(index_info, ctx, unique_rowkey_iter, checker_ctx))) {
          LOG_WARN("build index scan task info failed", K(ret), K(index_info));
        }
      }  // for gui_scan_infos end
    }
  } else {
    const ObPhyOperator* plan_root = gui_scan_root_;
    const ObPhyOperator* extend_root = table_scan_info_.index_fetcher_op_;
    if (NULL == plan_root) {
      plan_root = table_scan_info_.index_fetcher_op_;
      extend_root = NULL;
    }
    checker_ctx.set_mini_job(phy_plan_, plan_root, extend_root);
    DupKeyCheckerRowIter row_iter;
    if (OB_FAIL(checker_ctx.get_checker_store_begin_iter(row_iter))) {
      LOG_WARN("fail to get check store begin iter", K(ret));
    } else {
      ObUniqueIndexRowkeyIter table_rowkey_iter(
          checker_ctx.get_allocator(), checker_ctx.get_expr_ctx(), row_iter, table_scan_info_);
      if (OB_FAIL(build_conflict_row_task_info(table_scan_info_, ctx, table_rowkey_iter, checker_ctx))) {
        LOG_WARN("build table scan task info failed", K(ret), K_(table_scan_info));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < gui_scan_infos_.count(); ++i) {
      const ObUniqueIndexScanInfo& index_info = gui_scan_infos_.at(i);
      if (OB_FAIL(checker_ctx.get_checker_store_begin_iter(row_iter))) {
        LOG_WARN("fail to get check store begin iter", K(ret));
      } else {
        ObUniqueIndexRowkeyIter unique_rowkey_iter(
            checker_ctx.get_allocator(), checker_ctx.get_expr_ctx(), row_iter, index_info);
        if (OB_FAIL(build_conflict_row_task_info(index_info, ctx, unique_rowkey_iter, checker_ctx))) {
          LOG_WARN("build index scan task info failed", K(ret), K(index_info));
        }
      }
    }
  }
  return ret;
}

OB_INLINE bool ObDuplicatedKeyChecker::need_gui_lookup(ObDupKeyCheckerCtx& checker_ctx) const
{
  return ((is_static_engine_ ? gui_scan_root_spec_ != NULL : gui_scan_root_ != NULL) &&
          !checker_ctx.get_primary_rowkey_scanner().is_empty());
}

int ObDuplicatedKeyChecker::rebuild_gui_lookup_task_info_list(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx) const
{
  int ret = OB_SUCCESS;
  checker_ctx.reset_task_info();
  if (is_static_engine_) {
    checker_ctx.set_mini_job(phy_plan_, gui_lookup_info_.index_fetcher_spec_, NULL);
    ObChunkDatumStore::Iterator datum_iter;
    const ObChunkDatumStore& datum_store = checker_ctx.get_primary_rowkey_scanner().get_datum_store();
    if (OB_ISNULL(gui_scan_root_spec_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(gui_lookup_info_.index_fetcher_spec_->create_op_input(ctx))) {
      LOG_WARN("fail to create op input", K(ret));
    } else {
      ObPrimaryRowkeyIter primary_rowkey_iter(&gui_scan_root_spec_->output_, checker_ctx, ctx.get_eval_ctx());
      if (OB_FAIL(primary_rowkey_iter.init(const_cast<ObChunkDatumStore*>(&datum_store)))) {
        LOG_WARN("failed to init primary_rowkey_iter", K(ret));
      } else if (OB_FAIL(build_conflict_row_task_info(gui_lookup_info_, ctx, primary_rowkey_iter, checker_ctx))) {
        LOG_WARN("build gui look up task info failed", K(ret));
      }
    }
  } else {
    checker_ctx.set_mini_job(phy_plan_, gui_lookup_info_.index_fetcher_op_, NULL);
    ObPrimaryRowkeyIter primary_rowkey_iter(checker_ctx.get_primary_rowkey_scanner().begin(), checker_ctx);
    if (OB_FAIL(build_conflict_row_task_info(gui_lookup_info_, ctx, primary_rowkey_iter, checker_ctx))) {
      LOG_WARN("build gui look up task info failed", K(ret));
    }
  }
  return ret;
}

int ObDuplicatedKeyChecker::build_conflict_row_task_info(const ObUniqueIndexScanInfo& index_info, ObExecContext& ctx,
    ObIRowkeyIterator& rowkey_iter, ObDupKeyCheckerCtx& checker_ctx) const
{
  int ret = OB_SUCCESS;
  ObAddr runner_server;
  ObTaskInfo* task_info = NULL;
  ObTaskInfo::ObPartLoc part_loc;
  ObSEArray<ObDupKeyCheckerCtx::PartRowStore, 4> part_row_stores;
  if (OB_FAIL(
          checker_ctx.get_unique_index_part_info(index_info, ctx, rowkey_iter, part_row_stores, is_static_engine_))) {
    LOG_WARN("get unique index part info failed", K(ret), K(index_info), K(part_row_stores));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_row_stores.count(); ++i) {
    part_loc.reset();
    const ObDupKeyCheckerCtx::PartRowStore& part_row_store = part_row_stores.at(i);
    if (is_static_engine_) {
      part_loc.part_key_ref_id_ = index_info.index_fetcher_spec_->id_;
      part_loc.value_ref_id_ = index_info.index_fetcher_spec_->id_;
      part_loc.datum_store_ = part_row_store.datum_store_;
    } else {
      part_loc.part_key_ref_id_ = index_info.index_fetcher_op_->get_id();
      part_loc.value_ref_id_ = index_info.index_fetcher_op_->get_id();
      part_loc.row_store_ = part_row_store.row_store_;
    }
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_part_runner_server(
            ctx, index_info.table_id_, index_info.index_tid_, part_row_store.partition_id_, runner_server))) {
      LOG_WARN("get part runner server failed", K(ret), K(part_row_store));
    } else if (OB_FAIL(checker_ctx.get_or_create_scan_task_info(runner_server, task_info, is_static_engine_))) {
      LOG_WARN("get or create scan task info failed", K(ret), K(index_info), K(runner_server));
    } else if (OB_FAIL(part_loc.partition_key_.init(
                   index_info.index_tid_, part_row_store.partition_id_, index_info.part_cnt_))) {
      LOG_WARN("init part location pkey failed", K(ret), K(part_row_store), K(index_info));
    } else if (OB_FAIL(task_info->get_range_location().part_locs_.push_back(part_loc))) {
      LOG_WARN("set part location to task info failed", K(ret), K(part_loc));
    }
  }
  return ret;
}

int ObDuplicatedKeyChecker::build_duplicate_rowkey_map(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx) const
{
  int ret = OB_SUCCESS;
  if (is_static_engine_) {
    DupKeyCheckerDatumRowIter datum_iter;
    const ObExprPtrIArray* iter_output = NULL;
    // first: build checker task info
    if (OB_ISNULL(checker_ctx.checker_datum_store_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to check datum store", K(ret));
    } else if ((checker_ctx.checker_datum_store_->is_empty())) {
      // do nothing, check row store is empty, skip it
    } else if (OB_FAIL(build_conflict_row_task_info_list(ctx, checker_ctx))) {
      LOG_WARN("build index scan task info list failed", K(ret));
    } else if (OB_FAIL(checker_ctx.fetch_unique_index_rowkey(ctx, iter_output, datum_iter))) {
      LOG_WARN("fetch unique index data failed", K(ret), K_(table_scan_info));
    } else if (OB_FAIL(build_base_line_constraint_infos(checker_ctx, ctx.get_eval_ctx(), iter_output, datum_iter))) {
      LOG_WARN("build base line constraint infos failed", K(ret));
    } else if (!need_gui_lookup(checker_ctx)) {
      // when withoutglobal unique index duplicate key check, only need check main table
      // no need extra lookup
    } else if (OB_FAIL(rebuild_gui_lookup_task_info_list(ctx, checker_ctx))) {
      LOG_WARN("rebuild gui loop up task info list failed", K(ret));
    } else if (OB_FAIL(checker_ctx.fetch_gui_lookup_data(ctx, iter_output, datum_iter))) {
      LOG_WARN("fetch gui look up data failed", K(ret));
    } else if (OB_FAIL(build_base_line_constraint_infos(checker_ctx, ctx.get_eval_ctx(), iter_output, datum_iter))) {
      LOG_WARN("build base line constraint infos failed", K(ret));
    }
  } else {
    DupKeyCheckerRowIter row_iter;
    // first: build checker task info
    if (OB_ISNULL(checker_ctx.checker_row_store_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("checker row store is null", K(ret));
    } else if (checker_ctx.checker_row_store_->is_empty()) {
      // do nothing, check row store is empty, skip it
    } else if (OB_FAIL(build_conflict_row_task_info_list(ctx, checker_ctx))) {
      LOG_WARN("build index scan task info list failed", K(ret));
    } else if (OB_FAIL(checker_ctx.fetch_unique_index_rowkey(ctx, row_iter))) {
      LOG_WARN("fetch unique index data failed", K(ret), K_(table_scan_info));
    } else if (OB_FAIL(build_base_line_constraint_infos(checker_ctx, row_iter))) {
      LOG_WARN("build base line constraint infos failed", K(ret));
    } else if (!need_gui_lookup(checker_ctx)) {
    } else if (OB_FAIL(rebuild_gui_lookup_task_info_list(ctx, checker_ctx))) {
      LOG_WARN("rebuild gui loop up task info list failed", K(ret));
    } else if (OB_FAIL(checker_ctx.fetch_gui_lookup_data(ctx, row_iter))) {
      LOG_WARN("fetch gui look up data failed", K(ret));
    } else if (OB_FAIL(build_base_line_constraint_infos(checker_ctx, row_iter))) {
      LOG_WARN("build base line constraint infos failed", K(ret));
    }
  }
  return ret;
}

int ObDuplicatedKeyChecker::build_base_line_constraint_infos(ObDupKeyCheckerCtx& checker_ctx, ObEvalCtx* eval_ctx,
    const ObExprPtrIArray* iter_output, ObChunkDatumStore::Iterator& datum_iter) const
{
  int ret = OB_SUCCESS;
  CK(constraint_infos_.count() == checker_ctx.constraint_ctxs_.count());
  CK(OB_NOT_NULL(iter_output) && OB_NOT_NULL(eval_ctx));
  while (OB_SUCC(ret)) {
    const ObChunkDatumStore::StoredRow* row = NULL;
    bool is_duplicated = false;
    if (OB_FAIL(datum_iter.get_next_row(*iter_output, *eval_ctx, &row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from row iterator failed", K(ret));
      } else {
        LOG_DEBUG("datum iter end", K(ret));
      }
    } else {
      LOG_DEBUG("check job result", "output", ROWEXPR2STR(*eval_ctx, *iter_output));
    }
    CK(OB_NOT_NULL(row));
    for (int64_t i = 0; OB_SUCC(ret) && !is_duplicated && i < constraint_infos_.count(); ++i) {
      ObConstraintKey constraint_key;
      constraint_key.constraint_info_ = &(constraint_infos_.at(i));
      constraint_key.constraint_column_depend_exprs_ = iter_output;
      constraint_key.eval_ctx_ = checker_ctx.eval_ctx_;
      constraint_key.is_primary_index_ = (0 == i);  // the first index is primary index
      ObConstraintValue constraint_value;
      constraint_value.baseline_datum_row_ = row;
      constraint_value.current_datum_row_ = row;
      if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(constraint_infos_.at(i).calc_exprs_, *eval_ctx))) {
        LOG_WARN("fail to clear evaluated flag", K(ret));
      } else if (OB_FAIL(constraint_key.init_constraint_row(checker_ctx.get_allocator()))) {
        LOG_WARN("fail to init constraint_row", K(ret));
      } else if (OB_FAIL(checker_ctx.constraint_ctxs_.at(i).set_refactored(constraint_key, constraint_value))) {
        if (OB_HASH_EXIST == ret && 0 == i) {
          // primary index duplicate key, should skip duplicated row
          is_duplicated = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("set constraint key failed", K(ret), K(i));
        }
      }
      LOG_DEBUG("build base line constraint infos", K(ret), K(constraint_key), K(constraint_value), K(is_duplicated));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDuplicatedKeyChecker::build_base_line_constraint_infos(
    ObDupKeyCheckerCtx& checker_ctx, ObRowStore::Iterator& row_iter) const
{
  int ret = OB_SUCCESS;
  CK(constraint_infos_.count() == checker_ctx.constraint_ctxs_.count());
  while (OB_SUCC(ret)) {
    ObNewRow* row = NULL;
    bool is_duplicated = false;
    if (OB_FAIL(row_iter.get_next_row(row, NULL))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from row iterator failed", K(ret));
      }
    }
    CK(OB_NOT_NULL(row));
    for (int64_t i = 0; OB_SUCC(ret) && !is_duplicated && i < constraint_infos_.count(); ++i) {
      ObConstraintKey constraint_key;
      constraint_key.constraint_info_ = &(constraint_infos_.at(i));
      constraint_key.duplicated_row_ = row;
      constraint_key.is_primary_index_ = (0 == i);  // the first index is primary index
      ObConstraintValue constraint_value;
      constraint_value.baseline_row_ = row;
      constraint_value.current_row_ = row;
      if (OB_FAIL(checker_ctx.constraint_ctxs_.at(i).set_refactored(constraint_key, constraint_value))) {
        if (OB_HASH_EXIST == ret && 0 == i) {
          // primary index duplicate key, should skip duplicated row
          is_duplicated = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("set constraint key failed", K(ret), K(i));
        }
      }
      LOG_DEBUG("build base line constraint infos", K(ret), K(constraint_key), K(is_duplicated));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDuplicatedKeyChecker::check_duplicate_rowkey(
    ObDupKeyCheckerCtx& checker_ctx, const ObNewRow& row, ObIArray<ObConstraintValue>& constraint_values) const
{
  int ret = OB_SUCCESS;
  bool is_break = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_break && i < constraint_infos_.count(); ++i) {
    ObConstraintKey constraint_key;
    ObConstraintValue constraint_value;
    constraint_key.constraint_info_ = &(constraint_infos_.at(i));
    constraint_key.duplicated_row_ = &row;
    constraint_key.is_primary_index_ = (0 == i);
    if (OB_FAIL(checker_ctx.constraint_ctxs_.at(i).get_refactored(constraint_key, constraint_value))) {
      if (OB_HASH_NOT_EXIST != ret) {

        LOG_WARN("get duplicated row from constraint contexts failed", K(ret), K(constraint_key));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (constraint_value.baseline_row_ != constraint_value.current_row_) {
        checker_ctx.update_incremental_row_ = true;
      }
    }
    if (OB_SUCC(ret) && constraint_value.current_row_ != NULL) {
      if (OB_FAIL(add_var_to_array_no_dup(constraint_values, constraint_value))) {
        LOG_WARN("add constraint value no duplicate failed", K(ret));
      } else if (INSERT_RETURN_ONE_DUP == dml_flag_) {
        // INSERT_RETURN_ONE_DUP: only need one duplicated row(compatible with MySQL insert into on duplicate key
        // update) INSERT_RETURN_ALL_DUP: need all duplicated row(compatible with MySQL replace)
        is_break = true;
      }
    }
    LOG_DEBUG("check duplicate rowkey",
        K(ret),
        K(constraint_key),
        K(constraint_value),
        K(checker_ctx.update_incremental_row_));
  }
  return ret;
}

int ObDuplicatedKeyChecker::check_duplicate_rowkey(ObDupKeyCheckerCtx& checker_ctx, const ObExprPtrIArray& row_exprs,
    ObIArray<ObConstraintValue>& constraint_values) const
{
  int ret = OB_SUCCESS;
  bool is_break = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_break && i < constraint_infos_.count(); ++i) {
    ObConstraintKey constraint_key;
    ObConstraintValue constraint_value;
    constraint_key.constraint_info_ = &(constraint_infos_.at(i));
    constraint_key.eval_ctx_ = checker_ctx.eval_ctx_;
    constraint_key.constraint_column_depend_exprs_ = &row_exprs;
    constraint_key.is_primary_index_ = (0 == i);
    if (OB_ISNULL(checker_ctx.eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(constraint_infos_.at(i).calc_exprs_, *checker_ctx.eval_ctx_))) {
      LOG_WARN("fail to clear evaluated flag", K(ret));
    } else if (OB_FAIL(checker_ctx.constraint_ctxs_.at(i).get_refactored(constraint_key, constraint_value))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get duplicated row from constraint contexts failed", K(ret), K(constraint_key));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && constraint_value.current_datum_row_ != NULL) {
      if (constraint_value.baseline_datum_row_ != constraint_value.current_datum_row_) {
        checker_ctx.update_incremental_row_ = true;
      }
      if (OB_FAIL(add_var_to_array_no_dup(constraint_values, constraint_value))) {
        LOG_WARN("add constraint value no duplicate failed", K(ret));
      } else if (INSERT_RETURN_ONE_DUP == dml_flag_) {
        // INSERT_RETURN_ONE_DUP: only need one duplicated row(compatible with MySQL insert into on duplicate key
        // update) INSERT_RETURN_ALL_DUP: need all duplicated row(compatible with MySQL replace)
        is_break = true;
      }
    }
    LOG_DEBUG("check duplicate rowkey",
        K(ret),
        K(constraint_key),
        K(constraint_value),
        "row",
        ROWEXPR2STR(*checker_ctx.eval_ctx_, row_exprs),
        K(dml_flag_));
  }
  return ret;
}

int ObDuplicatedKeyChecker::delete_old_row(ObDupKeyCheckerCtx& checker_ctx, const ObNewRow& old_row) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_.count(); ++i) {
    ObConstraintKey constraint_key;
    ObConstraintValue* constraint_value = NULL;
    constraint_key.constraint_info_ = &(constraint_infos_.at(i));
    constraint_key.duplicated_row_ = &old_row;
    constraint_key.is_primary_index_ = (0 == i);
    constraint_value = const_cast<ObConstraintValue*>(checker_ctx.constraint_ctxs_.at(i).get(constraint_key));
    CK(OB_NOT_NULL(constraint_value));
    if (OB_SUCC(ret)) {
      constraint_value->current_row_ = NULL;
    }
    LOG_DEBUG("erase old row", K(ret), K(constraint_key), KPC(constraint_value));
  }
  return ret;
}

// row_exprs is table column exprs
int ObDuplicatedKeyChecker::delete_old_row(ObDupKeyCheckerCtx& checker_ctx, const ObExprPtrIArray& row_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_.count(); ++i) {
    ObConstraintKey constraint_key;
    ObConstraintValue* constraint_value = NULL;
    constraint_key.constraint_info_ = &(constraint_infos_.at(i));
    constraint_key.eval_ctx_ = checker_ctx.eval_ctx_;
    constraint_key.constraint_column_depend_exprs_ = &row_exprs;
    constraint_key.is_primary_index_ = (0 == i);
    if (OB_ISNULL(checker_ctx.eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(constraint_infos_.at(i).calc_exprs_, *checker_ctx.eval_ctx_))) {
      LOG_WARN("fail to clear evaluated flag", K(ret));
    } else {
      constraint_value = const_cast<ObConstraintValue*>(checker_ctx.constraint_ctxs_.at(i).get(constraint_key));
      CK(OB_NOT_NULL(constraint_value));
      if (OB_SUCC(ret)) {
        constraint_value->current_row_ = NULL;
        constraint_value->current_datum_row_ = NULL;
      }
    }
    LOG_DEBUG("erase old row", K(ret), K(constraint_key), KPC(constraint_value));
  }
  return ret;
}

int ObDuplicatedKeyChecker::insert_new_row(ObDupKeyCheckerCtx& checker_ctx, const ObNewRow& new_row) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_.count(); ++i) {
    ObConstraintKey constraint_key;
    constraint_key.constraint_info_ = &(constraint_infos_.at(i));
    constraint_key.duplicated_row_ = &new_row;
    constraint_key.is_primary_index_ = (0 == i);
    ObConstraintValue* constraint_value = NULL;
    constraint_value = const_cast<ObConstraintValue*>(checker_ctx.constraint_ctxs_.at(i).get(constraint_key));
    if (constraint_value != NULL) {
      if (OB_NOT_NULL(constraint_value->current_row_)) {
        // current_row not null, indicates conflict, need throw error
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        char rowkey_buffer[OB_TMP_BUF_SIZE_256];
        if (OB_SUCCESS != (extract_rowkey_info(constraint_infos_.at(i),
                              new_row,
                              rowkey_buffer,
                              OB_TMP_BUF_SIZE_256,
                              checker_ctx.expr_ctx_->my_session_->get_timezone_info()))) {
          LOG_WARN("extract rowkey info failed", K(ret), K(constraint_infos_.at(i)), K(new_row));
        } else {
          const ObString& constraint_name = constraint_infos_.at(i).constraint_name_;
          LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, constraint_name.length(), constraint_name.ptr());
        }
      } else {
        constraint_value->current_row_ = &new_row;
      }
    } else {
      ObConstraintValue new_constraint_value;
      new_constraint_value.current_row_ = &new_row;
      OZ(checker_ctx.constraint_ctxs_.at(i).set_refactored(constraint_key, new_constraint_value));
    }
    LOG_DEBUG("insert new row", K(ret), K(constraint_key), KPC(constraint_value));
  }
  return ret;
}

int ObDuplicatedKeyChecker::insert_new_row(ObDupKeyCheckerCtx& checker_ctx, const ObExprPtrIArray& row_exprs,
    const ObChunkDatumStore::StoredRow& new_row) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_.count(); ++i) {
    ObConstraintKey constraint_key;
    constraint_key.constraint_info_ = &(constraint_infos_.at(i));
    constraint_key.eval_ctx_ = checker_ctx.eval_ctx_;
    constraint_key.constraint_column_depend_exprs_ = &row_exprs;
    constraint_key.is_primary_index_ = (0 == i);
    ObConstraintValue* constraint_value = NULL;
    if (OB_ISNULL(checker_ctx.eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(constraint_infos_.at(i).calc_exprs_, *checker_ctx.eval_ctx_))) {
      LOG_WARN("fail to clear evaluated flag", K(ret));
    } else {
      constraint_value = const_cast<ObConstraintValue*>(checker_ctx.constraint_ctxs_.at(i).get(constraint_key));
      if (constraint_value != NULL) {
        if (OB_NOT_NULL(constraint_value->current_row_)) {
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
          char rowkey_buffer[OB_TMP_BUF_SIZE_256];
          if (OB_SUCCESS != (extract_rowkey_info(constraint_infos_.at(i),
                                *checker_ctx.eval_ctx_,
                                rowkey_buffer,
                                OB_TMP_BUF_SIZE_256,
                                checker_ctx.expr_ctx_->my_session_->get_timezone_info()))) {
            LOG_WARN("extract rowkey info failed", K(ret), K(constraint_infos_.at(i)), K(new_row));
          } else {
            const ObString& constraint_name = constraint_infos_.at(i).constraint_name_;
            LOG_USER_ERROR(
                OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, constraint_name.length(), constraint_name.ptr());
          }
        } else {
          constraint_value->current_datum_row_ = &new_row;
        }
      } else {
        ObConstraintValue new_constraint_value;
        new_constraint_value.current_datum_row_ = &new_row;
        OZ(constraint_key.init_constraint_row(checker_ctx.get_allocator()));
        OZ(checker_ctx.constraint_ctxs_.at(i).set_refactored(constraint_key, new_constraint_value));
      }
    }
    LOG_DEBUG("insert new row", K(ret), K(constraint_key), KPC(constraint_value));
  }
  return ret;
}

int ObDuplicatedKeyChecker::extract_rowkey_info(const ObPhyUniqueConstraintInfo& constraint_info,
    const ObNewRow& table_row, char* buf, int64_t buf_len, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObObj result;
  int64_t pos = 0;
  int64_t unique_key_idx = 0;
  int64_t primary_rowkey_cnt = constraint_infos_.at(0).constraint_columns_.get_size();
  int64_t unique_key_cnt = 0;
  if (&constraint_info == &(constraint_infos_.at(0))) {
    unique_key_cnt = primary_rowkey_cnt;
  } else {
    unique_key_cnt = constraint_info.constraint_columns_.get_size() - primary_rowkey_cnt;
  }
  DLIST_FOREACH_X(node, constraint_info.constraint_columns_, (unique_key_idx < unique_key_cnt))
  {
    OZ(node->calc(expr_ctx, table_row, result));
    if (OB_FAIL(result.print_plain_str_literal(buf, buf_len - 1, pos, tz_info))) {
      LOG_WARN("fail to print_plain_str_literal", K(ret));
    } else if (node != constraint_info.constraint_columns_.get_last() && unique_key_idx != unique_key_cnt - 1) {
      if (OB_FAIL(databuff_printf(buf, buf_len - 1, pos, "-"))) {
        LOG_WARN("databuff print failed", K(ret));
      }
    }
    ++unique_key_idx;
  }
  return ret;
}

int ObDuplicatedKeyChecker::extract_rowkey_info(const ObPhyUniqueConstraintInfo& constraint_info, ObEvalCtx& eval_ctx,
    char* buf, int64_t buf_len, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  ObDatum* r_datum = NULL;
  ObObj result;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_info.se_constraint_columns_.count(); i++) {
    ObExpr* expr = constraint_info.se_constraint_columns_.at(i);
    OZ(expr->eval(eval_ctx, r_datum));
    OZ(r_datum->to_obj(result, expr->obj_meta_));
    OZ(result.print_plain_str_literal(buf, buf_len - 1, pos, tz_info));
    if (OB_SUCC(ret) && constraint_info.se_constraint_columns_.count() - 1 == i) {
      OZ(databuff_printf(buf, buf_len - 1, pos, "-"));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
