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
#include "sql/engine/dml/ob_table_lock.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/dml/ob_table_lock.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::sql;
namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {

ObTableLock::ObTableLock(ObIAllocator& alloc)
    : ObTableModify(alloc),
      rowkey_projector_(NULL),
      rowkey_projector_size_(0),
      for_update_wait_us_(-1),
      skip_locked_(false)
{}

ObTableLock::~ObTableLock()
{}

void ObTableLock::reset()
{
  rowkey_projector_ = NULL;
  rowkey_projector_size_ = 0;
  for_update_wait_us_ = -1;
  skip_locked_ = false;
  ObTableModify::reset();
}

void ObTableLock::reuse()
{
  rowkey_projector_ = NULL;
  rowkey_projector_size_ = 0;
  for_update_wait_us_ = -1;
  skip_locked_ = false;
  ObTableModify::reuse();
}

int ObTableLock::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableLockCtx* op_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my session is null", K(ret));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableLockCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create phy operator context", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator context is null", K(ret));
  } else if (from_multi_table_dml()) {
    // do nothing
  } else if (OB_FAIL(op_ctx->alloc_row_cells(rowkey_projector_size_, op_ctx->lock_row_))) {
    LOG_WARN("failed to allocate row cells", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("failed to init current row", K(ret));
  }
  if (OB_SUCC(ret)) {
    op_ctx->for_update_wait_timeout_ =
        for_update_wait_us_ > 0 ? for_update_wait_us_ + my_session->get_query_start_time() : for_update_wait_us_;
  }
  return ret;
}

int ObTableLock::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableLockCtx* lock_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObTableLockCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(my_phy_plan_) || OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operator", K(ret), K(my_phy_plan_), K(child_op_));
  } else if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    lock_ctx->dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    lock_ctx->dml_param_.schema_version_ = schema_version;
    lock_ctx->dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    lock_ctx->dml_param_.tz_info_ = TZ_INFO(my_session);
    lock_ctx->dml_param_.sql_mode_ = my_session->get_sql_mode();
    lock_ctx->dml_param_.table_param_ = &table_param_;
    lock_ctx->dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    if (gi_above_ && OB_FAIL(get_gi_task(ctx))) {
      LOG_WARN("get granule iterator task failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_table_lock(ctx))) {
      LOG_WARN("do table lock failed", K(ret));
    }
  }
  return ret;
}

int ObTableLock::do_table_lock(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableLockCtx* lock_ctx = NULL;
  if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObTableLockCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor context is invalid", K(ret), K(lock_ctx));
  } else if (lock_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(lock_ctx->iter_end_), K(get_id()));
  } else if (OB_FAIL(get_part_location(ctx, lock_ctx->part_infos_))) {
    LOG_WARN("failed to get part locations", K(ret));
  } else if (OB_UNLIKELY(lock_ctx->part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition infos are empty", K(ret));
  } else if (from_multi_table_dml()) {
    if (OB_FAIL(lock_multi_part(ctx))) {
      if (OB_TRANSACTION_SET_VIOLATION != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to lock multi part", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLock::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(from_multi_table_dml())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(lock_single_part(ctx, row))) {
    if (OB_TRANSACTION_SET_VIOLATION != ret && OB_TRY_LOCK_ROW_CONFLICT != ret && OB_ITER_END != ret) {
      LOG_WARN("failed to lock next row", K(ret));
    }
  }
  return ret;
}

int ObTableLock::lock_single_part(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableLockCtx* lock_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  bool is_null = false;
  bool got_row = false;
  if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObTableLockCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor context is invalid", K(ret), K(lock_ctx));
  } else if (lock_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(lock_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)) ||
             OB_ISNULL(partition_service = executor_ctx->get_partition_service()) ||
             OB_ISNULL(my_session = GET_MY_SESSION(ctx)) || OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor context is invalid", K(ret), K(executor_ctx), K(partition_service), K(my_session));
  } else {
    while (OB_SUCC(ret) && !got_row) {
      if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_FAIL(build_lock_row(lock_ctx->lock_row_, *row))) {
        LOG_WARN("failed to build lock row", K(ret));
      } else if (OB_FAIL(copy_cur_row(*lock_ctx, row))) {
        LOG_WARN("failed to copy cur row", K(ret));
      } else if (need_filter_null_row() &&
                 OB_FAIL(check_rowkey_is_null(lock_ctx->lock_row_, rowkey_projector_size_, is_null))) {
        LOG_WARN("failed to check rowkey is null", K(ret));
      } else if (is_null) {
        // no need to lock
        got_row = true;
      } else if (OB_FAIL(partition_service->lock_rows(my_session->get_trans_desc(),
                     lock_ctx->dml_param_,
                     lock_ctx->for_update_wait_timeout_,
                     lock_ctx->part_infos_.at(0).partition_key_,
                     lock_ctx->lock_row_,
                     LF_NONE))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret &&
            OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
          LOG_WARN("failed to lock row", K(ret));
        } else if (is_skip_locked()) {
          ret = OB_SUCCESS;
        } else if (is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
        }
      } else {
        got_row = true;
        plan_ctx->add_affected_rows(1LL);
      }
    }
  }
  return ret;
}

int ObTableLock::lock_multi_part(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  ObTableLockCtx* lock_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObTableLockCtx, ctx, get_id())) ||
      OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)) ||
      OB_ISNULL(partition_service = executor_ctx->get_partition_service()) ||
      OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execution context is invalid", K(ret), K(lock_ctx), K(executor_ctx), K(partition_service), K(my_session));
  } else {
    const ObIArray<DMLPartInfo>& part_infos = lock_ctx->part_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_infos.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < part_infos.at(i).part_row_cnt_; ++j) {
        if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
          LOG_WARN("failed to check next row from child", K(ret));
        } else if (OB_FAIL(partition_service->lock_rows(my_session->get_trans_desc(),
                       lock_ctx->dml_param_,
                       lock_ctx->for_update_wait_timeout_,
                       lock_ctx->part_infos_.at(i).partition_key_,
                       *row,
                       LF_NONE))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret &&
              OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
            LOG_WARN("failed to lock row", K(ret));
          } else if (is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
            ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLock::inner_close(ObExecContext& ctx) const
{
  return ObTableModify::inner_close(ctx);
}

int ObTableLock::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableLockCtx* lock_ctx = NULL;
  if (!gi_above_ || from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table update rescan not supported", K(ret));
  } else if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObTableLockCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan for update context is null", K(ret), K(get_id()));
  } else if (OB_FAIL(ObTableModify::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    lock_ctx->part_infos_.reset();
    lock_ctx->part_key_.reset();
    if (nullptr != lock_ctx->rowkey_dist_ctx_) {
      lock_ctx->rowkey_dist_ctx_->clear();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_gi_task(ctx))) {
      LOG_WARN("get granule iterator task failed", K(ret));
    } else if (OB_FAIL(get_part_location(ctx, lock_ctx->part_infos_))) {
      LOG_WARN("failed to get part locations", K(ret));
    }
  }
  return ret;
}

int ObTableLock::build_lock_row(ObNewRow& lock_row, const ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  ObNewRow tmp = row;
  tmp.projector_ = rowkey_projector_;
  tmp.projector_size_ = rowkey_projector_size_;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp.get_count(); ++i) {
    lock_row.cells_[i] = tmp.get_cell(i);
  }
  return ret;
}

int64_t ObTableLock::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID, table_id_, K(index_tid_), K(for_update_wait_us_), K(skip_locked_));
  return pos;
}

OB_DEF_SERIALIZE(ObTableLock)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObTableLock, ObTableModify));
  OB_UNIS_ENCODE_ARRAY(rowkey_projector_, rowkey_projector_size_);
  OB_UNIS_ENCODE(for_update_wait_us_);
  OB_UNIS_ENCODE(skip_locked_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableLock)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObTableLock, ObTableModify));
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(rowkey_projector_size_);
    if (rowkey_projector_size_ > 0) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL(rowkey_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * rowkey_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K(rowkey_projector_size_));
      } else {
        OB_UNIS_DECODE_ARRAY(rowkey_projector_, rowkey_projector_size_);
      }
    } else {
      rowkey_projector_ = NULL;
    }
  }
  OB_UNIS_DECODE(for_update_wait_us_);
  OB_UNIS_DECODE(skip_locked_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLock)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObTableLock, ObTableModify));
  OB_UNIS_ADD_LEN_ARRAY(rowkey_projector_, rowkey_projector_size_);
  OB_UNIS_ADD_LEN(for_update_wait_us_);
  OB_UNIS_ADD_LEN(skip_locked_);
  return len;
}
}  // namespace sql
}  // namespace oceanbase
