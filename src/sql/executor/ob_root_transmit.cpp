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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_root_transmit.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

OB_SERIALIZE_MEMBER((ObRootTransmitInput, ObTransmitInput));

int ObRootTransmitInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return OB_SUCCESS;
}

ObRootTransmit::ObRootTransmit(ObIAllocator& alloc) : ObTransmit(alloc)
{
  job_conf_.set_task_split_type(ObTaskSpliter::LOCAL_IDENTITY_SPLIT);
}

ObRootTransmit::~ObRootTransmit()
{}

int ObRootTransmit::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("check status failed", K(ret));
  } else {
    ret = child_op_->get_next_row(ctx, row);
    if (OB_LIKELY(NULL != row)) {
      // LOG_DEBUG("root transmit current row", K(*row), K_(type));
    }
  }
  return ret;
}

int ObRootTransmit::init_op_ctx(ObExecContext& ctx) const
{
  ObPhyOperatorCtx* op_ctx = NULL;
  UNUSED(op_ctx);
  return CREATE_PHY_OPERATOR_CTX(ObRootTransmitCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObRootTransmit::process_expect_error(ObExecContext& ctx, int errcode) const
{
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (errcode == OB_ERR_PRIMARY_KEY_DUPLICATE) {
    if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_phy_plan_)) {
      // do nothing
    } else if (my_phy_plan_->is_ignore()) {
      errcode = OB_SUCCESS;
      plan_ctx->set_error_ignored(true);
    }
  }
  return errcode;
}
}  // namespace sql
}  // namespace oceanbase
