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
#include "sql/engine/dml/ob_table_merge_op.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_physical_plan.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace sql;
using storage::INSERT_RETURN_ONE_DUP;
using storage::ObPartitionService;

namespace sql {

OB_SERIALIZE_MEMBER((ObTableMergeOpInput, ObTableModifyOpInput));

OB_SERIALIZE_MEMBER((ObTableMergeSpec, ObTableModifySpec), has_insert_clause_, has_update_clause_, delete_conds_,
    update_conds_, insert_conds_, rowkey_exprs_, delete_column_ids_, updated_column_ids_, updated_column_infos_,
    old_row_, new_row_);

ObTableMergeSpec::ObTableMergeSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      has_insert_clause_(false),
      has_update_clause_(false),
      delete_conds_(alloc),
      update_conds_(alloc),
      insert_conds_(alloc),
      rowkey_exprs_(alloc),
      delete_column_ids_(alloc),
      updated_column_ids_(alloc),
      updated_column_infos_(alloc),
      old_row_(alloc),
      new_row_(alloc)
{}

int ObTableMergeSpec::set_updated_column_info(
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

int ObTableMergeSpec::init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
{
  UNUSED(allocator);
  int ret = common::OB_SUCCESS;
  OZ(updated_column_infos_.prepare_allocate(count));
  OZ(updated_column_ids_.prepare_allocate(count));

  return ret;
}

ObTableMergeOp::ObTableMergeOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObTableModifyOp(ctx, spec, input),
      insert_row_(),
      insert_row_store_(ctx_.get_allocator(), ObModIds::OB_SQL_ROW_STORE, OB_SERVER_TENANT_ID, false),
      old_row_(),
      new_row_(),
      affected_rows_(0)
{}

int ObTableMergeOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL((plan_ctx = ctx_.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL((my_session = GET_MY_SESSION(ctx_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(get_spec().plan_->get_base_table_version(MY_SPEC.index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child op", K(ret), K(child_));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    dml_param_.schema_version_ = schema_version;
    dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    dml_param_.sql_mode_ = my_session->get_sql_mode();
    dml_param_.tz_info_ = TZ_INFO(my_session);
    dml_param_.table_param_ = &MY_SPEC.table_param_;
    dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();

    if (MY_SPEC.gi_above_) {
      OZ(get_gi_task());
    }
    if (OB_SUCC(ret)) {
      OZ(do_table_merge());
    }
  }
  return ret;
}

int ObTableMergeOp::do_table_merge()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPartitionKey* pkey;
  ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  if (iter_end_) {
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
    if (OB_FAIL(set_autoinc_param_pkey(*pkey))) {
      LOG_WARN("set autoinc param pkey failed", K(pkey), K(ret));
    }
    // since inner open performes get_next_row, assgin opened_ as true
    opened_ = true;
    while (OB_SUCC(ret)) {
      bool is_match = false;
      clear_evaluated_flag();
      if (OB_FAIL(try_check_status())) {
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(check_is_match(is_match))) {
        LOG_WARN("failed to check is match", K(ret));
      } else if (is_match) {  // execute update
        if (MY_SPEC.has_update_clause_ && OB_FAIL(process_update(partition_service, *pkey, dml_param_))) {
          LOG_WARN("fail to process update", K(ret));
        }
      } else {  // execute insert
        if (MY_SPEC.has_insert_clause_ &&
            OB_FAIL(process_insert(partition_service, my_session->get_trans_desc(), dml_param_, *pkey))) {
          LOG_WARN("fail to insert row", K(ret));
        }
      }
    }  // end while

    if (OB_ITER_END == ret &&
        OB_FAIL(insert_all_rows(partition_service, my_session->get_trans_desc(), dml_param_, *pkey))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("insert all rows failed", K(ret));
      }
    }

    // deal with different ret
    if (OB_ITER_END != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to deal with merge into", K(ret));
      }
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    plan_ctx->add_affected_rows(affected_rows_);
  }
  return ret;
}
int ObTableMergeOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table merge into rescan not supported", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    reset();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_gi_task())) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_merge())) {
        LOG_WARN("do table merge into failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableMergeOp::check_is_match(bool& is_match)
{
  int ret = OB_SUCCESS;
  const ObTableMergeSpec& spec = MY_SPEC;
  is_match = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < spec.rowkey_exprs_.count(); ++i) {
    ObDatum* datum = NULL;
    const ObExpr* e = spec.rowkey_exprs_.at(i);
    if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (e->eval(eval_ctx_, datum)) {
      LOG_WARN("failed to evaluate expression", K(ret));
    } else {
      is_match = !datum->is_null();
    }
  }
  return ret;
}

int ObTableMergeOp::calc_delete_condition(const ObIArray<ObExpr*>& exprs, bool& is_true_cond)
{
  int ret = OB_SUCCESS;
  if (0 == exprs.count()) {
    is_true_cond = false;
  } else if (OB_FAIL(calc_condition(exprs, is_true_cond))) {
    LOG_WARN("fail to calc delete join condition", K(ret), K(exprs));
  }
  return ret;
}

int ObTableMergeOp::calc_condition(const ObIArray<ObExpr*>& exprs, bool& is_true_cond)
{
  int ret = OB_SUCCESS;
  ObDatum* cmp_res = NULL;
  is_true_cond = true;
  ARRAY_FOREACH(exprs, i)
  {
    if (OB_FAIL(exprs.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*exprs.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_true_cond = false;
      break;
    }
  }
  return ret;
}

int ObTableMergeOp::process_insert(ObPartitionService* partition_service, const transaction::ObTransDesc& trans_desc,
    const storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  bool is_true_cond = false;
  int64_t cur_affected = 0;
  ObNewRowIterator* duplicated_rows = NULL;
  if (!MY_SPEC.has_insert_clause_) {  // do nothing
    LOG_DEBUG("trace insert clause", K(ret));
  } else if (OB_FAIL(calc_condition(MY_SPEC.insert_conds_, is_true_cond))) {
    LOG_WARN("fail to calc condition", K(MY_SPEC.insert_conds_), K(ret));
  } else if (is_true_cond) {
    bool is_filtered = false;
    // in resolve phase, insert's check expression will be put in
    // check_constraint_exprs_'s second half
    int64_t cst_beg_idx = MY_SPEC.check_constraint_exprs_.count() / 2;
    int64_t cst_end_idx = MY_SPEC.check_constraint_exprs_.count();
    if (OB_FAIL(check_row_null(MY_SPEC.storage_row_output_, MY_SPEC.column_infos_))) {
      LOG_WARN("failed to check null", K(ret));
    } else if (OB_FAIL(
                   filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered, cst_beg_idx, cst_end_idx))) {
      LOG_WARN("failed to filter row for constraint", K(ret));
    } else if (is_filtered) {
      ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
      LOG_WARN("row is filtered by check", K(ret));
    } else if (OB_FAIL(project_row(MY_SPEC.storage_row_output_, insert_row_))) {
      LOG_WARN("failed to project row", K(ret));
    } else if (OB_FAIL(partition_service->insert_row(trans_desc,
                   dml_param,
                   pkey,
                   MY_SPEC.column_ids_,
                   MY_SPEC.primary_key_ids_,
                   insert_row_,
                   INSERT_RETURN_ONE_DUP,
                   cur_affected,
                   duplicated_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("fail to insert row", K(pkey), K(MY_SPEC.column_ids_), K(ret));
      }
      LOG_DEBUG("trace insert clause", K(ret), K(affected_rows_));
    }

    if (OB_SUCC(ret)) {
      affected_rows_ += cur_affected;
    }

    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      if (OB_FAIL(insert_row_store_.add_row(insert_row_, false))) {
        LOG_WARN("add row to row store failed", K(ret), K(insert_row_));
      }
    }

    if (nullptr != duplicated_rows) {
      (void)partition_service->revert_insert_iter(pkey, duplicated_rows);
      duplicated_rows = nullptr;
    }
  }
  return ret;
}

int ObTableMergeOp::insert_all_rows(ObPartitionService* partition_service, const transaction::ObTransDesc& trans_desc,
    const storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObRowStore::Iterator insert_row_iter;
  ObNewRow* insert_row = nullptr;
  int64_t cur_affected = 0;
  ObNewRowIterator* duplicated_rows = NULL;
  CK(OB_NOT_NULL(partition_service));
  insert_row_iter = insert_row_store_.begin();
  while (OB_SUCC(ret) && (OB_SUCC(insert_row_iter.get_next_row(insert_row, nullptr)))) {
    if (OB_FAIL(partition_service->insert_row(trans_desc,
            dml_param,
            pkey,
            MY_SPEC.column_ids_,
            MY_SPEC.primary_key_ids_,
            *insert_row,
            INSERT_RETURN_ONE_DUP,
            cur_affected,
            duplicated_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("fail to insert row", K(pkey), K(MY_SPEC.column_ids_), K(ret));
      }
    } else {
      affected_rows_ += cur_affected;
    }

    if (nullptr != duplicated_rows) {
      (void)partition_service->revert_insert_iter(pkey, duplicated_rows);
    }
  }
  return ret;
}

int ObTableMergeOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_close())) {
    LOG_WARN("fail to do inner close", K(ret));
  }
  return ret;
}

int ObTableMergeOp::inner_get_next_row()
{
  return OB_ITER_END;
}

int ObTableMergeOp::process_update(
    ObPartitionService* partition_service, const ObPartitionKey& pkey, storage::ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  bool need_delete = false;
  bool is_conflict = false;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  OZ(calc_condition(MY_SPEC.update_conds_, need_update));
  if (need_update) {
    OZ(generate_origin_row(is_conflict));
    OZ(calc_delete_condition(MY_SPEC.delete_conds_, need_delete));
    if (is_conflict) {
      ret = need_delete ? OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS : OB_ERR_UPDATE_TWICE;
    } else {
      if (need_delete) {
        OZ(project_row(MY_SPEC.old_row_, old_row_));
        OZ(partition_service->delete_row(my_session->get_trans_desc(), dml_param, pkey, MY_SPEC.column_ids_, old_row_));
        OX(affected_rows_ += 1);
      } else {
        OZ(update_row(partition_service, pkey, dml_param));
      }
    }
  }
  return ret;
}

int ObTableMergeOp::generate_origin_row(bool& conflict)
{
  int ret = OB_SUCCESS;
  bool is_distinct = true;
  conflict = false;
  if (OB_FAIL(check_rowkey_whether_distinct(
          MY_SPEC.old_row_, MY_SPEC.primary_key_ids_.count(), T_HASH_DISTINCT, rowkey_dist_ctx_, is_distinct))) {
    LOG_WARN("faield to check whether row distinct", K(ret));
  } else if (!is_distinct) {
    conflict = true;
  }
  return ret;
}

int ObTableMergeOp::update_row(
    ObPartitionService* partition_service, const ObPartitionKey& pkey, storage::ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObNewRowIterator* duplicated_rows = NULL;
  int64_t cur_affected = 0;
  OV(OB_NOT_NULL(partition_service));
  OZ(check_row_null(MY_SPEC.new_row_, MY_SPEC.column_infos_));
  bool is_filtered = false;
  // In resolve phase, check constraint expression corresponding to
  // update subquery will be put in first half.
  int64_t cst_beg_idx = 0;
  int64_t cst_end_idx = MY_SPEC.check_constraint_exprs_.count() / 2;
  OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered, cst_beg_idx, cst_end_idx));
  OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
  // do update.
  bool is_row_changed = false;
  OZ(check_updated_value(*this, MY_SPEC.updated_column_infos_, MY_SPEC.old_row_, MY_SPEC.new_row_, is_row_changed));

  OZ(project_row(MY_SPEC.old_row_, old_row_));
  OZ(project_row(MY_SPEC.new_row_, new_row_));
  if (!is_row_changed) {
    OZ(lock_row(partition_service, pkey, dml_param));
  } else {
    OZ(partition_service->delete_row(my_session->get_trans_desc(), dml_param, pkey, MY_SPEC.column_ids_, old_row_));
    OZ(partition_service->insert_row(my_session->get_trans_desc(),
        dml_param,
        pkey,
        MY_SPEC.column_ids_,
        MY_SPEC.primary_key_ids_,
        new_row_,
        INSERT_RETURN_ONE_DUP,
        cur_affected,
        duplicated_rows));
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      OZ(insert_row_store_.add_row(new_row_, false));
    }
    if (nullptr != duplicated_rows) {
      (void)partition_service->revert_insert_iter(pkey, duplicated_rows);
      duplicated_rows = nullptr;
    }
    OX(affected_rows_ += 1);
  }
  return ret;
}

int ObTableMergeOp::lock_row(
    ObPartitionService* partition_service, const ObPartitionKey& pkey, storage::ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL((partition_service))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableModifyOp::lock_row(MY_SPEC.rowkey_exprs_, dml_param, pkey))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("fail to lock row", K(ret));
        }
      } else {
        affected_rows_ += 1;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
