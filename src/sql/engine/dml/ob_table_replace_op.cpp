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
#include "sql/engine/dml/ob_table_replace_op.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ob_query_iterator_factory.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;

namespace sql {

OB_SERIALIZE_MEMBER((ObTableReplaceOpInput, ObTableModifyOpInput));
OB_SERIALIZE_MEMBER((ObTableReplaceSpec, ObTableModifySpec), only_one_unique_key_, table_column_exprs_);

int ObTableReplaceOp::inner_open()
{
  int ret = OB_SUCCESS;
  NG_TRACE(replace_open);
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  ObPhysicalPlan* phy_plan = NULL;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(MY_SPEC.plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid physical plan", K(ret));
  } else if (OB_FAIL(MY_SPEC.plan_->get_base_table_version(MY_SPEC.index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
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
      OZ(do_table_replace());
    }
  }  // end else
  return ret;
}

int ObTableReplaceOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table replace rescan not supported", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    part_infos_.reset();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_gi_task())) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_replace())) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableReplaceOp::do_table_replace()
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
    ObNewRowIterator* dup_rows_iter = NULL;
    while (OB_SUCC(ret) && OB_SUCC(inner_get_next_row())) {
      if (OB_FAIL(try_insert(*my_session, *pkey, *partition_service, dml_param_, dup_rows_iter))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          if (OB_ISNULL(dup_rows_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dup row iter is NULL", K(ret), KP(dup_rows_iter));
          } else if (OB_FAIL(do_replace(*my_session, *pkey, *partition_service, *dup_rows_iter, dml_param_))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              LOG_WARN("fail to do replace row", K(ret));
            }
          }
        } else {
          LOG_WARN("failed to insert row", K(ret));
        }
      }
      NG_TRACE_TIMES(2, revert_insert_iter);
      if (OB_UNLIKELY(dup_rows_iter != NULL)) {
        ObQueryIteratorFactory::free_insert_dup_iter(dup_rows_iter);
        dup_rows_iter = NULL;
      }
      NG_TRACE_TIMES(2, revert_insert_iter_end);
      if (OB_SUCC(ret)) {
        ctx_.get_physical_plan_ctx()->record_last_insert_id_cur_stmt();
      }
    }  // end while
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to replace", K(ret));
    } else {
      ret = OB_SUCCESS;
      //: for replace, OB_CLIENT_FOUND_ROWS has no effect to affected_rows_
      plan_ctx->set_affected_rows(affected_rows_);
      if (OB_FAIL(plan_ctx->set_row_matched_count(record_))) {
        LOG_WARN("fail to set row matched count", K(ret), K(record_));
      } else if (OB_FAIL(plan_ctx->set_row_duplicated_count(delete_count_))) {
        LOG_WARN("fail to set row duplicate count", K(delete_count_));
      } else if (OB_FAIL(plan_ctx->sync_last_value_local())) {
        // sync last user specified value after iter ends(compatible with MySQL)
        LOG_WARN("failed to sync last value", K(ret));
      }
    }

    NG_TRACE(sync_auto_value);
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      LOG_WARN("failed to sync value globally", K(sync_ret));
    }
    if (OB_SUCC(ret)) {
      ret = sync_ret;
    }
  }
  return ret;
}

int ObTableReplaceOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  CK(OB_NOT_NULL(child_));
  OZ(try_check_status());
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    ret = OB_ITER_END;
  } else if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    clear_evaluated_flag();
  }
  ObDatum* datum = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_spec().get_output_count(); ++i) {
    if (OB_FAIL(MY_SPEC.output_.at(i)->eval(eval_ctx_, datum))) {
      if (OB_UNLIKELY(i >= MY_SPEC.column_infos_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column idx", K(ret), K(i), K(MY_SPEC.column_infos_.count()));
      } else {
        log_user_error_inner(ret, i, record_ + 1, MY_SPEC.column_infos_);
      }
    }
  }
  return ret;
}

void ObTableReplaceOp::destroy()
{
  dml_param_.~ObDMLBaseParam();
  part_infos_.reset();
  row2exprs_projector_.destroy();
  ObTableModifyOp::destroy();
}

// try insert, will return all dup rows if conflits
// NOTE: datum arr is alloced in inner_open(), and it is freed in destroy().
//       datum.ptr_ is freed in the end of do_replace().
int ObTableReplaceOp::try_insert(ObSQLSessionInfo& my_session, const ObPartitionKey& part_key,
    ObPartitionService& partition_service, const ObDMLBaseParam& dml_param, ObNewRowIterator*& dup_rows_iter)
{
  int ret = OB_SUCCESS;
  int64_t cur_affected = 0;
  ObNewRow new_row;

  OZ(check_row_null(MY_SPEC.output_, MY_SPEC.column_infos_));
  OZ(ForeignKeyHandle::do_handle_new_row(*this, MY_SPEC.get_fk_args(), MY_SPEC.output_));
  OZ(project_row(MY_SPEC.output_.get_data(), MY_SPEC.get_output_count(), new_row));
  if (OB_SUCC(ret)) {
    record_++;
    NG_TRACE_TIMES(2, replace_start_insert);
    if (OB_SUCC(partition_service.insert_row(my_session.get_trans_desc(),
            dml_param,
            part_key,
            MY_SPEC.column_ids_,
            MY_SPEC.primary_key_ids_,
            new_row,
            INSERT_RETURN_ALL_DUP,
            cur_affected,
            dup_rows_iter))) {

      affected_rows_ += cur_affected;
    }
    NG_TRACE_TIMES(2, replace_end_insert);
    LOG_DEBUG("try to insert:",
        K(ret),
        K(MY_SPEC.column_ids_),
        K(MY_SPEC.primary_key_ids_),
        K(MY_SPEC.only_one_unique_key_),
        K(new_row));
  }
  return ret;
}

// scan and delete all duplicated rows
// insert new_row again
int ObTableReplaceOp::do_replace(ObSQLSessionInfo& my_session, const ObPartitionKey& part_key,
    ObPartitionService& partition_service, const ObNewRowIterator& dup_rows_iter, const ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  ObSingleRowIteratorWrapper dup_row_iter;
  ObNewRowIterator* scan_res = NULL;
  ObTableScanParam scan_param;
  ObNewRow new_row;

  OZ(project_row(MY_SPEC.output_.get_data(), MY_SPEC.get_output_count(), new_row));
  OZ(scan_row(my_session, part_key, partition_service, scan_param, dup_rows_iter, &scan_res));
  CK(OB_NOT_NULL(scan_res));

  if (OB_SUCC(ret)) {
    int64_t cur_affected = 0;
    ObNewRow old_del_row;
    while (OB_SUCC(ret) && OB_SUCC(scan_res->get_next_row())) {
      if (OB_FAIL(ForeignKeyHandle::do_handle_old_row(*this, MY_SPEC.get_fk_args(), MY_SPEC.table_column_exprs_))) {
        LOG_WARN("handle foreign key failed", K(ret), K(MY_SPEC.table_column_exprs_));
      } else if (OB_FAIL(project_row(
                     MY_SPEC.table_column_exprs_.get_data(), MY_SPEC.table_column_exprs_.count(), old_del_row))) {
        LOG_WARN("convert table scan output to old_del_row failed", K(ret));
      } else {
        dup_row_iter.reset();
        dup_row_iter.set_row(&old_del_row);
        NG_TRACE_TIMES(2, replace_start_delete);
        if (OB_FAIL(partition_service.delete_rows(
                my_session.get_trans_desc(), dml_param, part_key, MY_SPEC.column_ids_, &dup_row_iter, cur_affected))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to delete row", K(ret), K(old_del_row));
          }
        } else {
          NG_TRACE_TIMES(2, replace_end_delete);
          bool is_equal = false;
          if (true == MY_SPEC.only_one_unique_key_) {
            if (OB_FAIL(check_values(is_equal))) {
              LOG_WARN("fail to check values", K(ret));
            }
          }
          if (!is_equal) {
            delete_count_++;
            affected_rows_ += cur_affected;
          }
        }
        LOG_DEBUG("delete rows done", K(ret), K(old_del_row), K(new_row), K(MY_SPEC.only_one_unique_key_));
      }
    }
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next dup_row", K(ret));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      // 3. insert again
      ObSingleRowIteratorWrapper insert_row_iter;
      insert_row_iter.reset();
      insert_row_iter.set_row(&new_row);
      NG_TRACE_TIMES(2, replace_start_insert);
      ret = partition_service.insert_rows(
          my_session.get_trans_desc(), dml_param, part_key, MY_SPEC.column_ids_, &insert_row_iter, cur_affected);
      if (OB_FAIL(ret)) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to insert", K(ret), K(new_row));
        }
      } else {
        affected_rows_ += cur_affected;
      }
      NG_TRACE_TIMES(2, replace_end_insert);
      LOG_DEBUG("insert again done", K(ret), K(new_row));
    }
    int revert_scan_return = OB_SUCCESS;
    if (scan_res != NULL) {
      NG_TRACE_TIMES(2, revert_scan_iter);
      if (OB_SUCCESS != (revert_scan_return = partition_service.revert_scan_iter(scan_res))) {
        LOG_WARN("fail to to revert scan iter");
        if (OB_SUCC(ret)) {
          ret = revert_scan_return;
        }
      } else {
        scan_res = NULL;
      }
      NG_TRACE_TIMES(2, revert_scan_iter_end);
    }
  }
  return ret;
}

// table scan all dup rows
int ObTableReplaceOp::scan_row(ObSQLSessionInfo& my_session, const ObPartitionKey& part_key,
    ObPartitionService& partition_service, ObTableScanParam& scan_param, const ObNewRowIterator& dup_rows_iter,
    ObNewRowIterator** scan_res)
{
  int ret = OB_SUCCESS;
  ObNewRow* dup_row = NULL;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  NG_TRACE_TIMES(2, replace_start_scan_row);
  CK(OB_NOT_NULL(MY_SPEC.plan_));
  CK(OB_NOT_NULL(plan_ctx));
  CK(OB_NOT_NULL(scan_res));
  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(const_cast<ObNewRowIterator&>(dup_rows_iter).get_next_row(dup_row))) {
        LOG_WARN("get next row from dup row iter failed", K(ret));
      } else if (OB_ISNULL(dup_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dup_row is NULL", K(ret));
      } else {
        ObRowkey tmp_key(dup_row->cells_, dup_row->count_);
        ObRowkey key;
        if (OB_FAIL(tmp_key.deep_copy(key, ctx_.get_allocator()))) {
          LOG_WARN("fail to deep copy rowkey", K(ret));
        } else {
          ObNewRange range;
          if (OB_FAIL(range.build_range(MY_SPEC.index_tid_, key))) {
            LOG_WARN("fail to build key range", K(ret), K(MY_SPEC.table_id_), K(key));
          } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
            LOG_WARN("fail to push back key range", K(ret), K(range));
          }
        }
        LOG_DEBUG("scan dup row itering", K(ret), K(*dup_row));
      }
    }
    if (ret != OB_ITER_END) {
      LOG_WARN("get next row not return ob_iter_end", K(ret));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_SUCCESS;
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
      scan_param.reserved_cell_count_ = MY_SPEC.column_ids_.count();
      scan_param.for_update_ = false;
      scan_param.column_ids_.reset();
      if (OB_FAIL(scan_param.column_ids_.assign(MY_SPEC.column_ids_))) {
        LOG_WARN("fail to assign column id", K(ret));
      }
      if (OB_SUCC(ret)) {
        scan_param.output_exprs_ = &MY_SPEC.table_column_exprs_;
        scan_param.op_ = this;
        scan_param.row2exprs_projector_ = &row2exprs_projector_;
        scan_param.limit_param_.limit_ = -1;
        scan_param.limit_param_.offset_ = 0;
        scan_param.trans_desc_ = &my_session.get_trans_desc();
        scan_param.index_id_ = MY_SPEC.index_tid_;
        scan_param.sql_mode_ = my_session.get_sql_mode();
        scan_param.pkey_ = part_key;
        NG_TRACE_TIMES(2, replace_start_table_scan);
        if (OB_FAIL(MY_SPEC.plan_->get_base_table_version(MY_SPEC.index_tid_, scan_param.schema_version_))) {
          LOG_WARN("fail to get table schema version", K(ret), K(MY_SPEC.index_tid_));
        } else if (OB_FAIL(partition_service.table_scan(scan_param, *scan_res))) {
          LOG_WARN("fail to table scan", K(ret));
        }
        NG_TRACE_TIMES(2, replace_end_table_scan);
      }
    }
  }
  return ret;
}

int ObTableReplaceOp::check_values(bool& is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  ObDatum* insert_datum = NULL;
  ObDatum* del_datum = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.get_output_count(); ++i) {
    CK(OB_NOT_NULL(MY_SPEC.table_column_exprs_.at(i)));
    CK(MY_SPEC.output_.at(i)->basic_funcs_->null_first_cmp_ ==
        MY_SPEC.table_column_exprs_.at(i)->basic_funcs_->null_first_cmp_);

    if (schema::ObColumnSchemaV2::is_hidden_pk_column_id(MY_SPEC.column_ids_[i])) {
    } else if (OB_FAIL(MY_SPEC.output_.at(i)->eval(eval_ctx_, insert_datum) ||
                       OB_FAIL(MY_SPEC.table_column_exprs_.at(i)->eval(eval_ctx_, del_datum)))) {
      LOG_WARN("eval expr failed", K(ret));
    } else if (0 != MY_SPEC.output_.at(i)->basic_funcs_->null_first_cmp_(*insert_datum, *del_datum)) {
      is_equal = false;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
