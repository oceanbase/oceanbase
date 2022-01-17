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
#include "sql/engine/dml/ob_table_delete_returning.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_executor_ctx.h"

namespace oceanbase {
using namespace storage;
using namespace share;
namespace sql {
class ObTableDeleteReturning::ObTableDeleteReturningCtx : public ObTableDeleteCtx {
public:
  explicit ObTableDeleteReturningCtx(ObExecContext& ctx) : ObTableDeleteCtx(ctx)
  {}
  ~ObTableDeleteReturningCtx()
  {}
  friend class ObTableDeleteReturning;
  ObNewRow delete_row_;
  ObNewRow returning_row_;
};

ObTableDeleteReturning::ObTableDeleteReturning(ObIAllocator& alloc) : ObTableDelete(alloc)
{}

ObTableDeleteReturning::~ObTableDeleteReturning()
{}

int ObTableDeleteReturning::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableDeleteReturningCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObTableDeleteReturningCtx, ctx, get_id(), get_type(), op_ctx));
  CK(OB_NOT_NULL(op_ctx));
  OZ(op_ctx->alloc_row_cells(projector_size_, op_ctx->returning_row_));
  OZ(op_ctx->alloc_row_cells(get_column_ids().count(), op_ctx->delete_row_));
  return ret;
}

int ObTableDeleteReturning::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250) &&
             (get_phy_plan()->is_remote_plan() || get_phy_plan()->is_distributed_plan())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("returning is not supported before 2260", K(ret));
  } else if (OB_FAIL(ObTableDelete::inner_open(ctx))) {
    LOG_WARN("failed to open delete", K(ret));
  }
  return ret;
}

int ObTableDeleteReturning::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableDeleteReturningCtx* delete_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPartitionService* partition_service = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  const ObNewRow* delete_row = NULL;

  CK(OB_NOT_NULL(my_session = ctx.get_my_session()));
  CK(OB_NOT_NULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)));
  CK(OB_NOT_NULL(partition_service = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObTableDeleteReturningCtx, ctx, get_id())));
  CK(OB_NOT_NULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx)));
  CK(OB_LIKELY(delete_ctx->part_infos_.count() == 1));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableDelete::inner_get_next_row(ctx, delete_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get delete row", K(ret));
      }
    } else if (OB_ISNULL(delete_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete row is null", K(ret));
    } else if (OB_FAIL(calc_returning_row(delete_ctx->expr_ctx_, *delete_row, delete_ctx->returning_row_))) {
      LOG_WARN("failed to calc returning row", K(ret));
    } else if (copy_cur_row_by_projector(delete_ctx->delete_row_, delete_row)) {
      LOG_WARN("failed to remove projector for delete row", K(ret));
    } else if (OB_FAIL(partition_service->delete_row(my_session->get_trans_desc(),
                   delete_ctx->dml_param_,
                   delete_ctx->part_infos_.at(0).partition_key_,
                   column_ids_,
                   *delete_row))) {
      LOG_WARN("failed to delete row", K(ret));
    } else {
      row = &delete_ctx->returning_row_;
      phy_plan_ctx->add_affected_rows(1);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
