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

#include "sql/engine/px/exchange/ob_px_repart_transmit.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/executor/ob_range_hash_key_getter.h"
#include "sql/executor/ob_slice_calc.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;

OB_SERIALIZE_MEMBER((ObPxRepartTransmitInput, ObPxTransmitInput));

//////////////////////////////////////////

ObPxRepartTransmit::ObPxRepartTransmitCtx::ObPxRepartTransmitCtx(ObExecContext& ctx)
    : ObPxTransmit::ObPxTransmitCtx(ctx)
{}

ObPxRepartTransmit::ObPxRepartTransmitCtx::~ObPxRepartTransmitCtx()
{}

//////////////////////////////////////////

ObPxRepartTransmit::ObPxRepartTransmit(common::ObIAllocator& alloc) : ObPxTransmit(alloc)
{}

ObPxRepartTransmit::~ObPxRepartTransmit()
{}

int ObPxRepartTransmit::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  LOG_TRACE("Inner open px fifo transmit", "op_id", get_id());
  if (OB_FAIL(ObPxTransmit::inner_open(exec_ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

int ObPxRepartTransmit::do_transmit(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObPxRepartTransmitInput* trans_input = NULL;
  ObPxRepartTransmitCtx* transmit_ctx = NULL;

  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx)) ||
      OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxRepartTransmitInput, exec_ctx, get_id())) ||
      OB_ISNULL(transmit_ctx = GET_PHY_OPERATOR_CTX(ObPxRepartTransmitCtx, exec_ctx, get_id()))) {
    LOG_ERROR("fail to op ctx", "op_id", get_id(), "op_type", get_type(), KP(trans_input), KP(transmit_ctx), K(ret));
  } else if (!is_repart_exchange()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect repart repartition", K(ret));
  } else if (OB_INVALID_ID == repartition_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("repartition table id should be set for repart transmit op", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            exec_ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("faile to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(repartition_table_id_, table_schema))) {
      LOG_WARN("faile to get table schema", K(ret), K_(repartition_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null. repart sharding requires a table in dfo", K_(repartition_table_id), K(ret));
    } else if (OB_FAIL(
                   trans_input->get_part_ch_map(transmit_ctx->part_ch_info_, phy_plan_ctx->get_timeout_timestamp()))) {
      LOG_WARN("fail to get channel affinity map", K(ret));
    } else {
      if (get_dist_method() == ObPQDistributeMethod::PARTITION_RANDOM) {
        // pkey random
        ObRepartRandomSliceIdxCalc repart_slice_calc(exec_ctx,
            *table_schema,
            &repart_func_,
            &repart_sub_func_,
            &repart_columns_,
            &repart_sub_columns_,
            unmatch_row_dist_method_,
            transmit_ctx->part_ch_info_,
            repartition_type_);
        if (OB_FAIL(do_repart_transmit(exec_ctx, repart_slice_calc))) {
          LOG_WARN("failed to do repart transmit for pkey random", K(ret));
        }
      } else if (ObPQDistributeMethod::PARTITION_HASH == get_dist_method()) {
        // pkey hash
        ObSlaveMapPkeyHashIdxCalc repart_slice_calc(exec_ctx,
            *table_schema,
            &repart_func_,
            &repart_sub_func_,
            &repart_columns_,
            &repart_sub_columns_,
            unmatch_row_dist_method_,
            transmit_ctx->task_channels_.count(),
            transmit_ctx->part_ch_info_,
            transmit_ctx->expr_ctx_,
            hash_dist_columns_,
            dist_exprs_,
            repartition_type_);
        if (OB_FAIL(do_repart_transmit(exec_ctx, repart_slice_calc))) {
          LOG_WARN("failed to do repart transmit for pkey random", K(ret));
        }
      } else {
        // pkey
        ObAffinitizedRepartSliceIdxCalc repart_slice_calc(exec_ctx,
            *table_schema,
            &repart_func_,
            &repart_sub_func_,
            &repart_columns_,
            &repart_sub_columns_,
            unmatch_row_dist_method_,
            transmit_ctx->task_channels_.count(),
            transmit_ctx->part_ch_info_,
            repartition_type_);
        if (OB_FAIL(do_repart_transmit(exec_ctx, repart_slice_calc))) {
          LOG_WARN("failed to do repart transmit for pkey", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPxRepartTransmit::do_repart_transmit(ObExecContext& exec_ctx, ObRepartSliceIdxCalc& repart_slice_calc) const
{
  int ret = OB_SUCCESS;
  ObPxRepartTransmitCtx* transmit_ctx = NULL;
  if (OB_ISNULL(transmit_ctx = GET_PHY_OPERATOR_CTX(ObPxRepartTransmitCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit_ctx is null", K(ret));
  }
  if (OB_SUCC(ret)) {
    // init the ObRepartSliceIdxCalc cache map
    if (OB_FAIL(repart_slice_calc.init_partition_cache_map())) {
      LOG_WARN("failed to repart_slice_calc init partiiton cache map", K(ret));
    } else if (OB_FAIL(repart_slice_calc.init())) {
      LOG_WARN("failed to init repart slice calc", K(ret));
    } else if (OB_FAIL(send_rows(exec_ctx, *transmit_ctx, repart_slice_calc))) {
      LOG_WARN("failed to send rows", K(ret));
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = repart_slice_calc.destroy())) {
    LOG_WARN("failed to destroy", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObPxRepartTransmit::inner_close(ObExecContext& exec_ctx) const
{
  return ObPxTransmit::inner_close(exec_ctx);
}

int ObPxRepartTransmit::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxRepartTransmitCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, has_partition_id_column_idx()))) {
    LOG_WARN("fail to int cur row", K(ret));
  }
  return ret;
}

int ObPxRepartTransmit::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxRepartTransmitInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  UNUSED(input);
  return ret;
}
int ObPxRepartTransmit::add_compute(ObColumnExpression* expr)
{
  return ObPhyOperator::add_compute(expr);
}
