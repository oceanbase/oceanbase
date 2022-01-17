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
#include "sql/engine/dml/ob_table_insert_returning.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"

namespace oceanbase {
using namespace storage;
using namespace share;
namespace sql {
class ObTableInsertReturning::ObTableInsertReturningCtx : public ObTableInsertCtx {
public:
  explicit ObTableInsertReturningCtx(ObExecContext& ctx) : ObTableInsertCtx(ctx), new_row_()
  {}

  ~ObTableInsertReturningCtx()
  {}

  virtual void destroy()
  {
    ObTableInsertCtx::destroy();
  }

public:
  common::ObNewRow insert_row_;
  common::ObNewRow new_row_;
};

ObTableInsertReturning::ObTableInsertReturning(ObIAllocator& alloc)
    : ObTableInsert(alloc), insert_row_exprs_(), insert_projector_(NULL), insert_projector_size_(0)
{}

ObTableInsertReturning::~ObTableInsertReturning()
{}

void ObTableInsertReturning::reset()
{
  insert_row_exprs_.reset();
  insert_projector_ = NULL;
  insert_projector_size_ = 0;
  ObTableInsert::reset();
}

void ObTableInsertReturning::reuse()
{
  insert_row_exprs_.reset();
  insert_projector_ = NULL;
  insert_projector_size_ = 0;
  ObTableInsert::reuse();
}

int ObTableInsertReturning::set_insert_row_exprs()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_exprs_.move(insert_row_exprs_))) {
    LOG_WARN("failed to set insert row exprs", K(ret));
  } else {
    insert_projector_ = projector_;
    insert_projector_size_ = projector_size_;
    projector_ = NULL;
    projector_size_ = 0;
  }
  return ret;
}

int ObTableInsertReturning::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertReturningCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableInsertReturningCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create insert returning context", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator context is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("init current row failed", K(ret));
  } else if (OB_FAIL(op_ctx->alloc_row_cells(insert_projector_size_, op_ctx->insert_row_))) {
    LOG_WARN("failed to alloc row cells", K(ret));
  } else if (OB_FAIL(op_ctx->alloc_row_cells(projector_size_, op_ctx->new_row_))) {
    LOG_WARN("failed to alloc returning row", K(ret));
  }
  if (OB_SUCC(ret)) {
    op_ctx->new_row_exprs_ = &insert_row_exprs_;
    op_ctx->new_row_projector_ = insert_projector_;
    op_ctx->new_row_projector_size_ = insert_projector_size_;
  }
  return ret;
}

int ObTableInsertReturning::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertReturningCtx* insert_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250) &&
             (get_phy_plan()->is_remote_plan() || get_phy_plan()->is_distributed_plan())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("returning is not supported before 2260", K(ret));
  }
  OZ(ObTableInsert::inner_open(ctx));
  CK(OB_NOT_NULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertReturningCtx, ctx, get_id())));
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)));
  CK(OB_LIKELY(insert_ctx->part_infos_.count() == 1));
  OZ(set_autoinc_param_pkey(ctx, insert_ctx->part_infos_.at(0).partition_key_));
  return ret;
}

int ObTableInsertReturning::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableInsertReturningCtx* insert_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPartitionService* partition_service = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  const ObNewRow* insert_row = NULL;
  CK(OB_NOT_NULL(my_session = ctx.get_my_session()));
  CK(OB_NOT_NULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)));
  CK(OB_NOT_NULL(partition_service = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertReturningCtx, ctx, get_id())));
  CK(OB_LIKELY(insert_ctx->part_infos_.count() == 1));
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableInsert::get_next_row(ctx, insert_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_ISNULL(insert_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert row is null", K(ret));
    } else if (OB_FAIL(calc_returning_row(insert_ctx->expr_ctx_, *insert_row, insert_ctx->new_row_))) {
      LOG_WARN("failed to calc returning row", K(ret));
    } else if (OB_FAIL(copy_cur_row_by_projector(insert_ctx->insert_row_, insert_row))) {
      LOG_WARN("failed to remove projector for insert row", K(ret));
    } else if (OB_FAIL(partition_service->insert_row(my_session->get_trans_desc(),
                   insert_ctx->dml_param_,
                   insert_ctx->part_infos_.at(0).partition_key_,
                   column_ids_,
                   insert_ctx->insert_row_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert row to partition storage failed", K(ret));
      }
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        plan_ctx->set_last_insert_id_cur_stmt(0);
      }
    } else {
      row = &(insert_ctx->new_row_);
      plan_ctx->add_row_matched_count(1L);
      plan_ctx->add_affected_rows(1L);
    }
  }
  return ret;
}

int ObTableInsertReturning::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int sync_ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)));
  OZ(plan_ctx->sync_last_value_local());
  if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
    LOG_WARN("failed to sync value globally", K(sync_ret));
  }
  OZ(ObTableInsert::inner_close(ctx));
  if (OB_SUCC(ret)) {
    ret = sync_ret;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableInsertReturning)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObTableInsertReturning, ObTableInsert));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(insert_row_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize calc_exprs_", K(ret));
    }
  }
  OB_UNIS_ENCODE_ARRAY(insert_projector_, insert_projector_size_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableInsertReturning)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObTableInsertReturning, ObTableInsert));
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, insert_row_exprs_, my_phy_plan_);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(insert_projector_size_);
    if (insert_projector_size_ > 0) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL(insert_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * insert_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K(insert_projector_size_));
      } else {
        OB_UNIS_DECODE_ARRAY(insert_projector_, insert_projector_size_);
      }
    } else {
      insert_projector_ = NULL;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableInsertReturning)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObTableInsertReturning, ObTableInsert));
  len += get_dlist_serialize_size(insert_row_exprs_);
  OB_UNIS_ADD_LEN_ARRAY(insert_projector_, insert_projector_size_);
  return len;
}

}  // namespace sql
}  // namespace oceanbase
