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

#include "ob_table_insert_up_op.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_query_iterator_factory.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {

OB_SERIALIZE_MEMBER((ObTableInsertUpOpInput, ObTableModifyOpInput));

ObTableInsertUpSpec::ObTableInsertUpSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      scan_column_ids_(alloc),
      update_related_column_ids_(alloc),
      updated_column_ids_(alloc),
      updated_column_infos_(alloc),
      insert_row_(alloc),
      old_row_(alloc),
      new_row_(alloc)

{}

OB_SERIALIZE_MEMBER((ObTableInsertUpSpec, ObTableModifySpec), scan_column_ids_, update_related_column_ids_,
    updated_column_ids_, updated_column_infos_, insert_row_, old_row_, new_row_);

ObTableInsertUpOp::ObTableInsertUpOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObTableModifyOp(exec_ctx, spec, input),
      row2exprs_projector_(exec_ctx.get_allocator()),
      found_rows_(0),
      get_count_(0),
      dml_param_(),
      part_infos_(),
      cur_gi_task_iter_end_(false)
{}

int ObTableInsertUpOp::prepare_next_storage_row(const ObExprPtrIArray*& output)
{
  int ret = OB_SUCCESS;
  if (get_count_ == 0) {
    output = &MY_SPEC.old_row_;
  } else if (get_count_ == 1) {
    if (OB_FAIL(check_row_null(MY_SPEC.new_row_, MY_SPEC.column_infos_))) {
      LOG_WARN("failed to check row null", K(ret));
    } else {
      OZ(ForeignKeyHandle::do_handle(*this, MY_SPEC.fk_args_, MY_SPEC.old_row_, MY_SPEC.new_row_));
      output = &MY_SPEC.new_row_;
    }
  } else {
    ret = OB_ITER_END;
  }
  get_count_++;
  return ret;
}

int ObTableInsertUpOp::inner_open()
{
  int ret = OB_SUCCESS;
  NG_TRACE(insertup_open);
  OZ(ObTableModifyOp::inner_open());
  OZ(ObTableModify::init_dml_param_se(ctx_, MY_SPEC.index_tid_, false, &MY_SPEC.table_param_, dml_param_));
  dml_param_.is_ignore_ = MY_SPEC.is_ignore_;
  if (OB_SUCC(ret) && MY_SPEC.gi_above_) {
    OZ(get_gi_task());
  }
  if (OB_SUCC(ret)) {
    OZ(do_table_insert_up());
  }
  return ret;
}

int ObTableInsertUpOp::init_autoinc_param(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  plan_ctx->record_last_insert_id_cur_stmt();
  // insert success, auto-increment value consumed; clear saved auto-increment value
  ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
    autoinc_params.at(i).pkey_ = pkey;
    if (NULL != autoinc_params.at(i).cache_handle_) {
      autoinc_params.at(i).cache_handle_->last_row_dup_flag_ = false;
      autoinc_params.at(i).cache_handle_->last_value_to_confirm_ = 0;
    }
  }
  return ret;
}

int ObTableInsertUpOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table insert update rescan not supported", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    reset();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_gi_task())) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_insert_up())) {
        LOG_WARN("do table insert update failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableInsertUpOp::do_table_insert_up()
{
  int ret = OB_SUCCESS;
  ObNewRowIterator* duplicated_rows = NULL;
  int64_t affected_rows = 0;
  int64_t cur_affected = 0;
  int64_t n_duplicated_rows = 0;
  int64_t update_count = 0;
  ObPartitionKey* pkey;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;

  if (iter_end_) {
    cur_gi_task_iter_end_ = true;
    LOG_DEBUG("can't get gi task, iter end", K(iter_end_), K(MY_SPEC.id_));
  } else if (OB_FAIL(get_part_location(part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (part_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the part info count is not right", K(ret), K(part_infos_.count()));
  } else if (OB_ISNULL(pkey = &part_infos_.at(0).partition_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition key is null", K(ret), K(pkey));
  } else if (OB_FAIL(set_autoinc_param_pkey(*pkey))) {
    LOG_WARN("set autoinc param pkey failed", K(pkey), K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else {
    NG_TRACE(insertup_start_do);
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = child_->get_next_row())) {
      NG_TRACE_TIMES(2, insertup_start_calc_insert_row);
      clear_evaluated_flag();
      if (OB_FAIL(calc_insert_row())) {
        LOG_WARN("fail to calc insert row", K(ret));
      } else if (OB_FAIL(check_row_null(MY_SPEC.insert_row_, MY_SPEC.column_infos_))) {
        LOG_WARN("fail to check_row_null", K(ret));
      } else if (OB_FAIL(ForeignKeyHandle::do_handle_new_row(*this, MY_SPEC.fk_args_, MY_SPEC.insert_row_))) {
        LOG_WARN("do_handle_new_row failed", K(ret));
      } else if (OB_FAIL(project_row(MY_SPEC.insert_row_, insert_row_))) {
        LOG_WARN("project to old style row failed", K(ret));
      } else {
        found_rows_++;
        duplicated_rows = NULL;
        cur_affected = 0;
        NG_TRACE_TIMES(2, insertup_start_insert_row);
        ret = partition_service->insert_row(my_session->get_trans_desc(),
            dml_param_,
            *pkey,
            MY_SPEC.column_ids_,
            MY_SPEC.primary_key_ids_,
            insert_row_,
            INSERT_RETURN_ONE_DUP,
            cur_affected,
            duplicated_rows);
        affected_rows += cur_affected;
        NG_TRACE_TIMES(2, insertup_end_insert_row);
        SQL_ENG_LOG(DEBUG, "insert row on duplicate key", K(ret), K(affected_rows), K(insert_row_));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(init_autoinc_param(*pkey))) {
            LOG_WARN("fail to init autoinc param", K(ret));
          }
        }
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(process_on_duplicate_update(
                  duplicated_rows, dml_param_, n_duplicated_rows, affected_rows, update_count))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
              LOG_WARN("fail to process on duplicate update", K(ret));
            }
          }
        }
        NG_TRACE_TIMES(2, revert_insert_iter);
        if (OB_UNLIKELY(duplicated_rows != NULL)) {
          ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
          duplicated_rows = NULL;
        }
        NG_TRACE_TIMES(2, revert_insert_iter_end);
      }
    }  // end while

    if (OB_ITER_END != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to deal with insert on duplicate key", K(ret));
      }
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      cur_gi_task_iter_end_ = true;
      ret = OB_SUCCESS;
      SQL_ENG_LOG(DEBUG, "insert on dupliate key finish", K(affected_rows));
      plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                      ? affected_rows + n_duplicated_rows
                                      : affected_rows);
      plan_ctx->add_row_matched_count(found_rows_);
      plan_ctx->add_row_duplicated_count(update_count);
      if (OB_SUCCESS != (ret = plan_ctx->sync_last_value_local())) {
        // sync last user specified value after iter ends(compatible with MySQL)
        LOG_WARN("failed to sync last value", K(ret));
      }
    }
    NG_TRACE_TIMES(2, insertup_end);
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      LOG_WARN("failed to sync value globally", K(sync_ret));
    }
    NG_TRACE(sync_auto_value);
    if (OB_SUCC(ret)) {
      ret = sync_ret;
    }
  }

  return ret;
}

int ObTableInsertUpOp::calc_insert_row()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.insert_row_.count(); i++) {
    ObDatum* datum = NULL;
    if (OB_FAIL(MY_SPEC.insert_row_.at(i)->eval(eval_ctx_, datum))) {
      log_user_error_inner(ret, i, found_rows_ + 1, MY_SPEC.column_infos_);
    }
  }
  return ret;
}

int ObTableInsertUpOp::process_on_duplicate_update(ObNewRowIterator* duplicated_rows,
    storage::ObDMLBaseParam& dml_param, int64_t& n_duplicated_rows, int64_t& affected_rows, int64_t& update_count)
{
  int ret = OB_SUCCESS;
  int64_t cur_affected = 0;
  common::ObNewRowIterator* result = NULL;
  storage::ObTableScanParam scan_param;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPartitionService* partition_service = NULL;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  const ObPhyTableLocation* table_location = NULL;
  ObNewRow* dup_row = NULL;
  bool is_row_changed = false;
  const ObPartitionReplicaLocation* part_replica = NULL;
  NG_TRACE_TIMES(2, insertup_before_scan);
  if (OB_ISNULL(duplicated_rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL((partition_service = executor_ctx->get_partition_service()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 *executor_ctx, MY_SPEC.table_id_, MY_SPEC.index_tid_, table_location))) {
    LOG_WARN("failed to get physical table location", K(ret), K(MY_SPEC.table_id_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (OB_FAIL(get_part_location(*table_location, part_replica))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (OB_FAIL(duplicated_rows->get_next_row(dup_row))) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_FAIL(build_scan_param(*part_replica, dup_row, scan_param))) {
    LOG_WARN("fail to build scan param", K(ret), K(dup_row));
  } else if (OB_FAIL(partition_service->table_scan(scan_param, result))) {
    LOG_WARN("fail to table scan", K(ret));
  } else if (OB_FAIL(result->get_next_row())) {
    LOG_WARN("fail to get next row", K(ret), K(MY_SPEC.update_related_column_ids_));
  } else if (OB_FAIL(calc_update_rows(is_row_changed))) {
    LOG_WARN("fail to calc rows for update", K(ret));
  }
  // update
  if (OB_SUCC(ret)) {
    if (is_row_changed) {
      update_count++;
      DMLRowIterator dml_row_iter(ctx_, *this);
      NG_TRACE_TIMES(2, insertup_start_update_row);
      if (OB_FAIL(dml_row_iter.init())) {
        LOG_WARN("init dml row iterator failed", K(ret));
      } else if (OB_FAIL(partition_service->update_rows(my_session->get_trans_desc(),
                     dml_param,
                     scan_param.pkey_,
                     MY_SPEC.update_related_column_ids_,
                     MY_SPEC.updated_column_ids_,
                     &dml_row_iter,
                     cur_affected))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("update rows to partition storage failed",
              K(ret),
              K(MY_SPEC.update_related_column_ids_),
              K(MY_SPEC.updated_column_ids_));
        }
      } else {
        NG_TRACE_TIMES(2, insertup_end_update_row);
        // The affected-rows value per row is 1 if the row is inserted as a new row,
        // 2 if an existing row is updated, and 0 if an existing row is set to its current values
        affected_rows = affected_rows + cur_affected + 1;
        SQL_ENG_LOG(DEBUG, "update row", K(ret), K(affected_rows));
      }
    } else {
      n_duplicated_rows++;
      // lock row
      if (OB_FAIL(lock_row(MY_SPEC.old_row_, dml_param, scan_param.pkey_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to lock row", K(ret), K(lock_row_), K(scan_param.pkey_));
        }
      } else {
        NG_TRACE_TIMES(2, lock_row);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ITER_END != (ret = duplicated_rows->get_next_row(dup_row))) {
      LOG_WARN("get next row fail. row count should be only one", K(ret));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get next row fail ,expected return code is OB_ITER_END");
      }
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ITER_END != (ret = result->get_next_row())) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("scan result more than one row.", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (result != NULL) {
    NG_TRACE_TIMES(2, revert_scan_iter);
    int revert_return = OB_SUCCESS;
    if (NULL != result && OB_SUCCESS != (revert_return = partition_service->revert_scan_iter(result))) {
      LOG_WARN("fail to revert scan iter", K(revert_return));
      if (OB_SUCC(ret)) {
        ret = revert_return;
      }
    } else {
      result = NULL;
    }
    NG_TRACE_TIMES(2, revert_scan_iter_end);
  }
  return ret;
}

int ObTableInsertUpOp::build_scan_param(
    const ObPartitionReplicaLocation& part_replica, const ObNewRow* dup_row, storage::ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_location = NULL;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_ISNULL(dup_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(dup_row));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 *executor_ctx, MY_SPEC.table_id_, MY_SPEC.index_tid_, table_location))) {
    LOG_WARN("failed to get physical table location", K(ret), K(MY_SPEC.table_id_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else {
    ObRowkey key(dup_row->cells_, dup_row->count_);
    common::ObNewRange range;
    scan_param.key_ranges_.reset();
    if (OB_FAIL(range.build_range(MY_SPEC.index_tid_, key))) {
      LOG_WARN("fail to build key range", K(ret), K_(MY_SPEC.index_tid), K(key));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    } else {
      scan_param.timeout_ = plan_ctx->get_ps_timeout_timestamp();
      ObQueryFlag query_flag(ObQueryFlag::Forward,  // scan_order
          false,                                    // daily_merge
          false,                                    // optimize
          false,                                    // sys scan
          false,                                    // full_row
          false,                                    // index_back
          false,                                    // query_stat
          ObQueryFlag::MysqlMode,                   // sql_mode
          true                                      // read_latest
      );
      scan_param.scan_flag_.flag_ = query_flag.flag_;
      scan_param.reserved_cell_count_ = MY_SPEC.scan_column_ids_.count();
      scan_param.for_update_ = false;
      scan_param.column_ids_.reset();
      if (OB_FAIL(scan_param.column_ids_.assign(MY_SPEC.scan_column_ids_))) {
        LOG_WARN("fail to assign column id", K(ret));
      } else if (OB_FAIL(part_replica.get_partition_key(scan_param.pkey_))) {
        LOG_WARN("get partition key fail", K(ret), K(part_replica));
      } else if (OB_ISNULL(MY_SPEC.plan_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid physical plan", K(ret));
      } else if (OB_FAIL(MY_SPEC.plan_->get_base_table_version(MY_SPEC.index_tid_, scan_param.schema_version_))) {
        LOG_WARN("fail to get table schema version", K(ret), K(MY_SPEC.index_tid_));
      } else {
        SQL_ENG_LOG(DEBUG, "set scan param", K(MY_SPEC.scan_column_ids_));
        // scan_param.expr_ctx_ not used in new engine scan interface.
        scan_param.output_exprs_ = &MY_SPEC.old_row_;
        scan_param.op_ = this;
        scan_param.limit_param_.limit_ = -1;
        scan_param.limit_param_.offset_ = 0;
        scan_param.trans_desc_ = &my_session->get_trans_desc();
        scan_param.index_id_ = MY_SPEC.index_tid_;
        scan_param.sql_mode_ = my_session->get_sql_mode();
        scan_param.row2exprs_projector_ = &row2exprs_projector_;
      }
    }
  }
  return ret;
}

int ObTableInsertUpOp::calc_update_rows(bool& is_row_changed)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  is_row_changed = false;
  NG_TRACE_TIMES(2, insertup_start_calc_update_row);
  if (OB_SUCC(ret)) {
    // before calc new row, to be compatible with MySQL
    // 1. disable operation to sync user specified value for auto-increment column because duplicate
    //    hidden pk will be placed in first place of row
    //    ATTENTION: suppose two auto-increment column at most here
    // 2. set duplicate flag to reuse auto-generated value
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      autoinc_params.at(i).sync_flag_ = false;
      if (NULL != autoinc_params.at(i).cache_handle_) {
        autoinc_params.at(i).cache_handle_->last_row_dup_flag_ = true;
      }
    }
    if (OB_FAIL(calc_new_row(is_row_changed))) {
      LOG_WARN("fail to calc new row", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUpOp::calc_new_row(bool& is_row_changed)
{
  int ret = OB_SUCCESS;
  is_row_changed = false;
  bool is_auto_col_changed = false;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  // if auto-increment value used when duplicate, reset last_insert_id_cur_stmt
  // TODO check if auto-increment value used actually
  if (plan_ctx->get_autoinc_id_tmp() == plan_ctx->get_last_insert_id_cur_stmt()) {
    plan_ctx->set_last_insert_id_cur_stmt(0);
  }

  NG_TRACE_TIMES(2, insertup_calc_new_row);
  if (OB_FAIL(check_row_value(is_row_changed, MY_SPEC.updated_column_infos_, MY_SPEC.old_row_, MY_SPEC.new_row_))) {
    LOG_WARN("check row value failed", K(ret));
  } else {
    FOREACH_CNT_X(col, MY_SPEC.updated_column_infos_, OB_SUCC(ret))
    {
      NG_TRACE_TIMES(2, insertup_auto_increment);
      if (OB_FAIL(update_auto_increment(*MY_SPEC.new_row_.at(col->projector_index_),
              MY_SPEC.update_related_column_ids_.at(col->projector_index_),
              is_auto_col_changed))) {
        LOG_WARN("update auto increment failed", K(ret));
      }
    }

    NG_TRACE_TIMES(2, insertup_end_auto_increment);
    if (OB_SUCC(ret)) {
      get_count_ = 0;
    }
  }
  return ret;
}

int ObTableInsertUpOp::update_auto_increment(const ObExpr& expr, const uint64_t cid, bool& is_auto_col_changed)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
  AutoincParam* autoinc_param = NULL;
  for (int64_t j = 0; OB_SUCC(ret) && j < autoinc_params.count(); ++j) {
    if (cid == autoinc_params.at(j).autoinc_col_id_) {
      autoinc_param = &autoinc_params.at(j);
      break;
    }
  }
  if (NULL != autoinc_param) {
    is_auto_col_changed = true;
    bool is_zero = false;
    uint64_t casted_value = 0;
    ObDatum* datum = NULL;
    if (OB_FAIL(expr.eval(eval_ctx_, datum))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else if (OB_FAIL(ObExprAutoincNextval::get_uint_value(expr, datum, is_zero, casted_value))) {
      LOG_WARN("get casted valued failed", K(ret), K(*datum));
    } else {
      CacheHandle* cache_handle = autoinc_param->cache_handle_;
      if (!OB_ISNULL(cache_handle) && true == cache_handle->last_row_dup_flag_ &&
          0 != cache_handle->last_value_to_confirm_) {
        // auto-increment value has been generated for this row
        if (casted_value == cache_handle->last_value_to_confirm_) {
          // column may be updated, but updated value is the same with old value
          cache_handle->last_row_dup_flag_ = false;
          cache_handle->last_value_to_confirm_ = 0;
        } else if (cache_handle->in_range(casted_value)) {
          // update value in generated range
          ret = OB_ERR_AUTO_INCREMENT_CONFLICT;
          LOG_WARN("update value in auto-generated range", K(ret));
        } else {
          autoinc_param->value_to_sync_ = casted_value;
          autoinc_param->sync_flag_ = true;
        }
      } else {
        // no auto-increment value generated; user specify a value
        // mark sync flag to sync update value
        autoinc_param->value_to_sync_ = casted_value;
        autoinc_param->sync_flag_ = true;
      }
    }
  }
  NG_TRACE_TIMES(2, insertup_end_auto_increment);
  return ret;
}

int ObTableInsertUpOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_close())) {
    LOG_WARN("fail to do inner close", K(ret));
  }
  return ret;
}

int ObTableInsertUpOp::inner_get_next_row()
{
  // row is consumed in inner_open(), return iterate end here.
  return OB_ITER_END;
}

void ObTableInsertUpOp::reset()
{
  found_rows_ = 0;
  get_count_ = 0;
  part_infos_.reset();
  cur_gi_task_iter_end_ = false;
}

int ObTableInsertUpSpec::set_updated_column_info(
    int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp)
{
  int ret = OB_SUCCESS;
  ColumnContent column;
  column.projector_index_ = project_index;
  column.auto_filled_timestamp_ = auto_filled_timestamp;
  CK(array_index >= 0 && array_index < updated_column_ids_.count());
  CK(array_index >= 0 && array_index < updated_column_infos_.count());
  if (OB_SUCC(ret)) {
    updated_column_ids_.at(array_index) = column_id;
    updated_column_infos_.at(array_index) = column;
  }
  return ret;
}

int ObTableInsertUpSpec::init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
{
  UNUSED(allocator);
  int ret = common::OB_SUCCESS;
  OZ(updated_column_infos_.prepare_allocate(count));
  OZ(updated_column_ids_.prepare_allocate(count));

  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
