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
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/dml/ob_table_update_returning.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
namespace sql {
class ObTableUpdateReturning::ObTableUpdateReturningCtx : public ObTableUpdateCtx {
public:
  explicit ObTableUpdateReturningCtx(ObExecContext& ctx) : ObTableUpdateCtx(ctx)
  {}
  ~ObTableUpdateReturningCtx()
  {}

  friend class ObTableUpdateReturning;
  ObNewRow returning_row_;
};

void ObTableUpdateReturning::reset()
{
  updated_projector_ = NULL;
  updated_projector_size_ = 0;
  ObTableUpdate::reset();
}

void ObTableUpdateReturning::reuse()
{
  updated_projector_ = NULL;
  updated_projector_size_ = 0;
  ObTableUpdate::reuse();
}

int ObTableUpdateReturning::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250) &&
             (get_phy_plan()->is_remote_plan() || get_phy_plan()->is_distributed_plan())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("returning is not supported before 2260", K(ret));
  } else if (OB_FAIL(ObTableUpdate::inner_open(ctx))) {
    LOG_WARN("failed to open delete", K(ret));
  }
  return ret;
}

int ObTableUpdateReturning::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* old_row = NULL;
  const ObNewRow* new_row = NULL;
  ObTableUpdateReturningCtx* update_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObPartitionService* partition_service = NULL;
  CK(OB_NOT_NULL(my_session));
  CK(OB_NOT_NULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)));
  CK(OB_NOT_NULL(partition_service = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateReturningCtx, ctx, get_id())));
  CK(OB_LIKELY(update_ctx->part_infos_.count() == 1));

  // update here
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ObTableUpdate::get_next_row(ctx, old_row)) && OB_SUCC(ObTableUpdate::get_next_row(ctx, new_row))) {
      update_ctx->part_key_ = update_ctx->part_infos_.at(0).partition_key_;
      if (OB_FAIL(copy_cur_row_by_projector(update_ctx->cur_rows_[0], old_row))) {
        LOG_WARN("copy old row failed", K(ret));
      } else if (OB_FAIL(copy_cur_row_by_projector(update_ctx->cur_rows_[1], new_row))) {
        LOG_WARN("copy new row failed", K(ret));
      } else if (OB_FAIL(partition_service->update_row(my_session->get_trans_desc(),
                     update_ctx->dml_param_,
                     update_ctx->part_key_,
                     column_ids_,
                     updated_column_ids_,
                     *old_row,
                     *new_row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("update row to partition storage failed", K(ret));
        }
      } else if (OB_FAIL(
                     calc_returning_row(update_ctx->expr_ctx_, update_ctx->full_row_, update_ctx->returning_row_))) {
        LOG_WARN("failed to calc returning row", K(ret));
      } else {
        row = &update_ctx->returning_row_;
      }
    }
  }

  // end
  if (OB_SUCCESS != ret) {
    if (OB_ITER_END == ret) {
      ObPhysicalPlanCtx* plan_ctx = NULL;
      if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("get physical plan context failed", K(ret));
      } else {
        plan_ctx->add_row_matched_count(update_ctx->get_found_rows());
        plan_ctx->add_row_duplicated_count(update_ctx->get_changed_rows());
        plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                        ? update_ctx->get_found_rows()
                                        : update_ctx->get_affected_rows());
      }
    } else {
      LOG_WARN("process update row failed", K(ret));
    }
  }
  return ret;
}

int ObTableUpdateReturning::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateReturningCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObTableUpdateReturningCtx, ctx, get_id(), get_type(), op_ctx));
  CK(OB_NOT_NULL(op_ctx));
  if (OB_SUCC(ret)) {
    op_ctx->new_row_projector_ = updated_projector_;
    op_ctx->new_row_projector_size_ = updated_projector_size_;
  }
  OZ(op_ctx->create_cur_rows(2, op_ctx->new_row_projector_size_, NULL, 0));
  OZ(op_ctx->alloc_row_cells(returning_exprs_.get_size(), op_ctx->returning_row_));
  return ret;
}

OB_DEF_SERIALIZE(ObTableUpdateReturning)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObTableUpdateReturning, ObTableUpdate));
  OB_UNIS_ENCODE_ARRAY(updated_projector_, updated_projector_size_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableUpdateReturning)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObTableUpdateReturning, ObTableUpdate));
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(updated_projector_size_);
    if (updated_projector_size_ > 0) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL(
              updated_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * updated_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K(updated_projector_size_));
      } else {
        OB_UNIS_DECODE_ARRAY(updated_projector_, updated_projector_size_);
      }
    } else {
      updated_projector_ = NULL;
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableUpdateReturning)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObTableUpdateReturning, ObTableUpdate));
  OB_UNIS_ADD_LEN_ARRAY(updated_projector_, updated_projector_size_);
  return len;
}

}  // namespace sql
}  // namespace oceanbase
