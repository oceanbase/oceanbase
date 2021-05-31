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

#include "ob_table_update_op.h"
#include "share/system_variable/ob_system_variable.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
namespace sql {

OB_SERIALIZE_MEMBER((ObTableUpdateOpInput, ObTableModifyOpInput));

ObTableUpdateSpec::ObTableUpdateSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      updated_column_ids_(alloc),
      updated_column_infos_(alloc),
      old_row_(alloc),
      new_row_(alloc)
{}

ObTableUpdateSpec::~ObTableUpdateSpec()
{}

int ObTableUpdateSpec::set_updated_column_info(
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

int ObTableUpdateSpec::init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
{
  UNUSED(allocator);
  int ret = common::OB_SUCCESS;
  OZ(updated_column_infos_.prepare_allocate(count));
  OZ(updated_column_ids_.prepare_allocate(count));

  return ret;
}

OB_SERIALIZE_MEMBER(
    (ObTableUpdateSpec, ObTableModifySpec), updated_column_ids_, updated_column_infos_, old_row_, new_row_);

ObTableUpdateOp::ObTableUpdateOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObTableModifyOp(exec_ctx, spec, input),
      has_got_old_row_(false),
      found_rows_(0),
      changed_rows_(0),
      affected_rows_(0),
      dml_param_(),
      part_key_(),
      part_row_cnt_(0),
      part_infos_(),
      cur_part_idx_(0),
      need_update_(false)
{}

int ObTableUpdateOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  NG_TRACE(update_open);
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child operator is NULL", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(MY_SPEC.plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan", K(ret), KP(MY_SPEC.plan_));
  } else if (OB_FAIL(MY_SPEC.plan_->get_base_table_version(MY_SPEC.index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    dml_param_.schema_version_ = schema_version;
    dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    dml_param_.tz_info_ = TZ_INFO(my_session);
    dml_param_.sql_mode_ = my_session->get_sql_mode();
    dml_param_.table_param_ = &MY_SPEC.table_param_;
    dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    dml_param_.is_ignore_ = MY_SPEC.is_ignore_;
    if (MY_SPEC.gi_above_) {
      if (OB_FAIL(get_gi_task())) {
        LOG_WARN("get granule iterator task failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_table_update())) {
        LOG_WARN("do table update failed", K(ret));
      }
    }
  }
  NG_TRACE(update_end);
  return ret;
}

int ObTableUpdateOp::do_table_update()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_));
  } else if (OB_FAIL(get_part_location(part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (OB_FAIL(update_rows(affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN(
          "update rows to partition storage failed", K(ret), K(MY_SPEC.column_ids_), K(MY_SPEC.updated_column_ids_));
    }
  } else if (!MY_SPEC.from_multi_table_dml()) {
    // dml meta info will be counted in multiple table dml operator, not here
    plan_ctx->add_row_matched_count(found_rows_);
    plan_ctx->add_row_duplicated_count(changed_rows_);
    plan_ctx->add_affected_rows(
        my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS ? found_rows_ : affected_rows_);
  }
  SQL_ENG_LOG(
      DEBUG, "update rows end", K(ret), K(affected_rows), K(MY_SPEC.column_ids_), K(MY_SPEC.updated_column_ids_));
  return ret;
}

int ObTableUpdateOp::inner_close()
{
  return ObTableModifyOp::inner_close();
}

int ObTableUpdateOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_ || MY_SPEC.from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table update rescan not supported", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    found_rows_ = 0;
    changed_rows_ = 0;
    affected_rows_ = 0;
    has_got_old_row_ = false;
    part_infos_.reset();
    part_key_.reset();
    if (nullptr != rowkey_dist_ctx_) {
      rowkey_dist_ctx_->clear();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_gi_task())) {
      LOG_WARN("get granule task failed", K(ret));
    } else if (OB_FAIL(do_table_update())) {
      LOG_WARN("do table update failed", K(ret));
    }
  }
  return ret;
}

// Child rows will be consumed by prepare_next_storage_row(), when the executor
// call get_next_row() will get OB_ITER_END;
int ObTableUpdateOp::prepare_next_storage_row(const ObExprPtrIArray*& output)
{
  int ret = OB_SUCCESS;
  ObPartitionArray part_keys;
  // update operator must has project operation
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check status failed", K(ret));
  } else if (!has_got_old_row_) {
    NG_TRACE_TIMES(2, update_start_next_row);
    need_update_ = false;
    OZ(restore_and_reset_fk_res_info());
    // filter unchanged rows
    while (OB_SUCC(ret) && !need_update_ && OB_SUCC(inner_get_next_row())) {
      if (OB_SUCC(ret)) {
        if (!MY_SPEC.from_multi_table_dml()) {
          // TODO : process trigger
          // OZ (TriggerHandle::init_param_rows(*this, *update_ctx, old_row, new_row), old_row, new_row);
          // OZ (TriggerHandle::do_handle_before_row(*this, *update_ctx, &new_row), old_row, new_row);
          OZ(check_row_null(MY_SPEC.new_row_, MY_SPEC.column_infos_));
          if (MY_SPEC.need_filter_null_row_) {
            bool is_null = false;
            if (OB_FAIL(check_rowkey_is_null(MY_SPEC.old_row_, MY_SPEC.primary_key_ids_.count(), is_null))) {
              LOG_WARN("check rowkey is null failed", K(ret));
            } else if (is_null) {
              continue;
            }
          }
        }
        if (OB_SUCC(ret) && !MY_SPEC.from_multi_table_dml()) {
          bool is_distinct = false;
          if (OB_FAIL(check_rowkey_whether_distinct(MY_SPEC.old_row_,
                  MY_SPEC.primary_key_ids_.count(),
                  MY_SPEC.distinct_algo_,
                  rowkey_dist_ctx_,
                  is_distinct))) {
            LOG_WARN("check rowkey whether distinct failed", K(ret));
          } else if (!is_distinct) {
            continue;
          }
        }
      }
      if (OB_SUCC(ret)) {
        // check update row whether changed
        if (OB_LIKELY(!MY_SPEC.from_multi_table_dml())) {
          // if update operator from multi table dml,
          // the row value will be check in multiple table dml operator
          // dml meta info will also be counted in multiple table dml operator
          OZ(check_updated_value(
              *this, MY_SPEC.updated_column_infos_, MY_SPEC.old_row_, MY_SPEC.new_row_, need_update_));
        } else if (OB_LIKELY(check_row_whether_changed())) {
          need_update_ = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (need_update_) {
          if (!MY_SPEC.from_multi_table_dml()) {
            bool is_filtered = false;
            OZ(ForeignKeyHandle::do_handle(*this, MY_SPEC.get_fk_args(), MY_SPEC.old_row_, MY_SPEC.new_row_),
                MY_SPEC.old_row_,
                MY_SPEC.new_row_);
            OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered));
            if (OB_SUCC(ret) && is_filtered) {
              ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
              LOG_WARN("row is filtered by check filters, running is stopped", K(ret));
            }
          }
        } else {
          if (OB_FAIL(lock_row(MY_SPEC.old_row_, dml_param_, part_key_))) {
            // lock row if no changes
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              LOG_WARN("fail to lock row", K(ret), K(lock_row_), K(part_key_));
            }
          } else {
            LOG_DEBUG("lock row", K(ret), K(lock_row_), K(part_key_));
          }
        }
        if (!MY_SPEC.from_multi_table_dml()) {
          // TODO : process trigger
          // OZ (TriggerHandle::do_handle_after_row(*this, *update_ctx), old_row, new_row);
        }
      }
      if (MY_SPEC.is_returning_) {
        // return current row no matter need update or not
        break;
      }
    }  // while
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("get next row from child operator failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      output = &MY_SPEC.old_row_;
      has_got_old_row_ = true;
    }
  } else {
    // new row
    // shadow pk is add to new_row_, need do nothing for it in new engine.
    output = &MY_SPEC.new_row_;
    has_got_old_row_ = false;
  }
  NG_TRACE_TIMES(2, update_end_next_row);
  return ret;
}

int ObTableUpdateOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch single child operator iterator failed", K(ret));
    }
  } else {
    has_got_old_row_ = false;
  }
  return ret;
}

bool ObTableUpdateOp::check_row_whether_changed() const
{
  bool bret = false;
  if (MY_SPEC.updated_column_infos_.count() > 0) {
    ObDatum& datum = MY_SPEC.lock_row_flag_expr_->locate_expr_datum(eval_ctx_);
    if (ObActionFlag::OP_LOCK_ROW != datum.get_int()) {
      bret = true;
    }
  }
  return bret;
}

int ObTableUpdateOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_));
    ret = OB_ITER_END;
  } else if (MY_SPEC.from_multi_table_dml()) {
    if (part_row_cnt_ <= 0) {
      if (cur_part_idx_ < part_infos_.count()) {
        part_row_cnt_ = part_infos_.at(cur_part_idx_).part_row_cnt_;
        part_key_ = part_infos_.at(cur_part_idx_).partition_key_;
        ++cur_part_idx_;
      }
    }
    if (OB_SUCC(ret)) {
      --part_row_cnt_;
    }
  }
  if (OB_SUCC(ret)) {
    clear_evaluated_flag();
    ret = child_->get_next_row();
  }
  if (OB_ITER_END == ret) {
    NG_TRACE(update_iter_end);
  }
  return ret;
}

inline int ObTableUpdateOp::update_rows(int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObPartitionService* partition_service = NULL;
  if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_UNLIKELY(part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part infos is empty", K(ret));
  } else if (OB_LIKELY(part_infos_.count() == 1)) {
    part_key_ = part_infos_.at(0).partition_key_;
    DMLRowIterator dml_row_iter(ctx_, *this);
    if (OB_FAIL(dml_row_iter.init())) {
      LOG_WARN("init dml row iterator", K(ret));
    } else if (OB_FAIL(partition_service->update_rows(my_session->get_trans_desc(),
                   dml_param_,
                   part_key_,
                   MY_SPEC.column_ids_,
                   MY_SPEC.updated_column_ids_,
                   &dml_row_iter,
                   affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert row to partition storage failed", K(ret));
      }
    }
  } else {
    // multi partition insert
    while (OB_SUCC(ret)) {
      if (OB_FAIL(do_row_update())) {
        if (OB_ITER_END != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("update row failed", K(ret));
        }
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("process update row failed", K(ret));
    }
  }
  return ret;
}

int ObTableUpdateOp::do_row_update()
{
  // get next now twice to get the old row and new new.
  int ret = OB_SUCCESS;
  const ObExprPtrIArray* output = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObPartitionService* partition_service = NULL;
  if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(prepare_next_storage_row(output)) || OB_FAIL(prepare_next_storage_row(output))) {
  } else if (OB_FAIL(project_row(MY_SPEC.old_row_.get_data(), MY_SPEC.old_row_.count(), old_row_)) ||
             OB_FAIL(project_row(MY_SPEC.new_row_.get_data(), MY_SPEC.new_row_.count(), new_row_))) {
    LOG_WARN("project expr to row failed", K(ret));
  } else if (need_update_ && OB_FAIL(partition_service->update_row(my_session->get_trans_desc(),
                                 dml_param_,
                                 part_key_,
                                 MY_SPEC.column_ids_,
                                 MY_SPEC.updated_column_ids_,
                                 old_row_,
                                 new_row_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("update row to partition storage failed", K(ret));
    }
  } else {
    LOG_DEBUG("update row", K_(part_key), K(old_row_), K(new_row_));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
