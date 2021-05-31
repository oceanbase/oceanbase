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
#include "sql/engine/dml/ob_table_delete.h"
#include "lib/profile/ob_perf_event.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_executor_ctx.h"
namespace oceanbase {
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {
ObTableDelete::ObTableDelete(ObIAllocator& alloc) : ObTableModify(alloc)
{}

ObTableDelete::~ObTableDelete()
{}

int ObTableDelete::add_compute(ObColumnExpression* expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObTableDelete::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableDeleteCtx* delete_ctx = NULL;
  if (!gi_above_ || from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table delete rescan not supported", K(ret));
  } else if (OB_ISNULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table delete context is null", K(ret), K(get_id()));
  } else if (OB_FAIL(ObTableModify::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    delete_ctx->part_infos_.reset();
    if (nullptr != delete_ctx->rowkey_dist_ctx_) {
      delete_ctx->rowkey_dist_ctx_->clear();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_delete(ctx))) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableDelete::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableDeleteCtx* delete_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  NG_TRACE(delete_open);
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get delete operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    delete_ctx->dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    delete_ctx->dml_param_.schema_version_ = schema_version;
    delete_ctx->dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    delete_ctx->dml_param_.sql_mode_ = my_session->get_sql_mode();
    delete_ctx->dml_param_.tz_info_ = TZ_INFO(my_session);
    delete_ctx->dml_param_.table_param_ = &table_param_;
    delete_ctx->dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    NG_TRACE(delete_start_delete);
    if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule iterator task failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_table_delete(ctx))) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  NG_TRACE(delete_iter_end);
  return ret;
}

inline int ObTableDelete::do_table_delete(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTableDeleteCtx* delete_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get delete operator context failed", K(ret), K(get_id()));
  } else if (delete_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(delete_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, delete_ctx->part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (is_returning()) {
    // do nothing, delete rows in ObTableDeleteReturning
  } else if (OB_FAIL(delete_rows(ctx, delete_ctx->dml_param_, delete_ctx->part_infos_, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("delete row to partition storage failed", K(ret));
    }
  } else if (!from_multi_table_dml()) {
    plan_ctx->add_affected_rows(affected_rows);
  }
  return ret;
}

inline int ObTableDelete::delete_rows(ObExecContext& ctx, ObDMLBaseParam& dml_param,
    const ObIArray<DMLPartInfo>& part_infos, int64_t& affected_rows) const
{
  int ret = OB_SUCCESS;
  ObTableDeleteCtx* delete_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObPartitionService* partition_service = NULL;
  ObDMLRowIterator dml_row_iter(ctx, *this);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session is null");
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_UNLIKELY(part_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part infos is empty", K(part_infos.empty()));
  } else if (OB_FAIL(dml_row_iter.init())) {
    LOG_WARN("init dml row iterator", K(ret));
  } else if (OB_LIKELY(part_infos.count() == 1)) {
    if (OB_FAIL(partition_service->delete_rows(my_session->get_trans_desc(),
            dml_param,
            part_infos.at(0).partition_key_,
            column_ids_,
            &dml_row_iter,
            affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("delete row to partition storage failed", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_infos.count(); ++i) {
      const ObPartitionKey& part_key = part_infos.at(i).partition_key_;
      delete_ctx->part_row_cnt_ = part_infos.at(i).part_row_cnt_;
      ObNewRow* row = NULL;
      while (OB_SUCC(ret) && OB_SUCC(dml_row_iter.get_next_row(row))) {
        if (OB_FAIL(
                partition_service->delete_row(my_session->get_trans_desc(), dml_param, part_key, column_ids_, *row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("delete row to partition storage failed", K(ret));
          }
        } else {
          affected_rows += 1;
          if (delete_ctx->part_row_cnt_ <= 0) {
            break;
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("process delete row failed", K(ret));
    }
  }
  return ret;
}

int ObTableDelete::inner_close(ObExecContext& ctx) const
{
  return ObTableModify::inner_close(ctx);
}

int64_t ObTableDelete::to_string_kv(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID, table_id_, K_(index_tid), N_CID, column_ids_);
  return pos;
}

int ObTableDelete::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableDeleteCtx* delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id());
  NG_TRACE_TIMES(2, delete_start_next_row);
  if (OB_ISNULL(child_op_) || OB_ISNULL(delete_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op_ is null", K(child_op_), K(delete_ctx));
  } else if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("check status failed", K(ret));
  } else if (delete_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(get_id()), K(delete_ctx->iter_end_));
    ret = OB_ITER_END;
  } else {
    if (from_multi_table_dml()) {
      --delete_ctx->part_row_cnt_;
    }
    if (OB_SUCC(ret)) {
      bool get_next_row = false;
      do {
        bool is_null = false;
        get_next_row = false;
        if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row from child op failed", K(ret));
          }
        } else if (need_filter_null_row_ && !from_multi_table_dml()) {
          if (OB_FAIL(check_rowkey_is_null(*row, primary_key_ids_.count(), is_null))) {
            LOG_WARN("check rowkey is null failed", K(ret));
          } else {
            get_next_row = is_null;
          }
        } else {
#if !defined(NDEBUG)
          if (need_check_pk_is_null()) {
            if (!from_multi_table_dml()) {
              if (OB_FAIL(check_rowkey_is_null(*row, primary_key_ids_.count(), is_null))) {
                LOG_WARN("failed to check rowkey is null", K(ret));
              } else if (OB_UNLIKELY(is_null)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("delete row failed validity check", K(ret));
              }
            }
          }
#endif
        }
        if (OB_SUCC(ret) && !get_next_row && !from_multi_table_dml()) {
          bool is_distinct = false;
          if (OB_FAIL(check_rowkey_whether_distinct(
                  ctx, *row, primary_key_ids_.count(), distinct_algo_, delete_ctx->rowkey_dist_ctx_, is_distinct))) {
            LOG_WARN("check rowkey whether distinct failed", K(ret));
          } else {
            get_next_row = !is_distinct;
          }
        }
      } while (OB_SUCC(ret) && get_next_row);
    }
    NG_TRACE_TIMES(2, delete_end_next_row);
  }
  if (OB_SUCC(ret) && !from_multi_table_dml()) {
    ObTableDeleteCtx* delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id());
    CK(OB_NOT_NULL(delete_ctx));
    CK(OB_NOT_NULL(row));
    OZ(ForeignKeyHandle::do_handle_old_row(*delete_ctx, fk_args_, *row));
  }
  return ret;
}

int ObTableDelete::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableDeleteCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObTableDeleteCtx, ctx, get_id(), get_type(), op_ctx), get_type());
  CK(OB_NOT_NULL(op_ctx));
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
