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

#include "ob_px_repart_transmit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER((ObPxRepartTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxRepartTransmitSpec, ObPxTransmitSpec), calc_part_id_expr_, dist_exprs_, dist_hash_funcs_);

ObPxRepartTransmitSpec::ObPxRepartTransmitSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObPxTransmitSpec(alloc, type), calc_part_id_expr_(NULL), dist_exprs_(alloc), dist_hash_funcs_(alloc)
{}

ObPxRepartTransmitOp::ObPxRepartTransmitOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObPxTransmitOp(exec_ctx, spec, input)
{}

int ObPxRepartTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("Inner open px fifo transmit", "op_id", MY_SPEC.id_);
  if (OB_FAIL(ObPxTransmitOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

int ObPxRepartTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObPxRepartTransmitOpInput* trans_input = static_cast<ObPxRepartTransmitOpInput*>(input_);

  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_)) || OB_ISNULL(trans_input)) {
    LOG_ERROR("fail to op ctx", "op_id", MY_SPEC.id_, "op_type", MY_SPEC.type_, KP(trans_input), K(ret));
  } else if (!MY_SPEC.is_repart_exchange()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect repart repartition", K(ret));
  } else if (OB_INVALID_ID == MY_SPEC.repartition_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("repartition table id should be set for repart transmit op", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            ctx_.get_my_session()->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("faile to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(MY_SPEC.repartition_table_id_, table_schema))) {
      LOG_WARN("faile to get table schema", K(ret), K(MY_SPEC.repartition_table_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "table schema is null. repart sharding requires a table in dfo", K(MY_SPEC.repartition_table_id_), K(ret));
    } else if (OB_FAIL(trans_input->get_part_ch_map(part_ch_info_, phy_plan_ctx->get_timeout_timestamp()))) {
      LOG_WARN("fail to get channel affinity map", K(ret));
    } else {
      if (ObPQDistributeMethod::PARTITION_RANDOM == MY_SPEC.dist_method_) {
        // pkey random
        ObRepartRandomSliceIdxCalc repart_slice_calc(ctx_,
            *table_schema,
            MY_SPEC.calc_part_id_expr_,
            MY_SPEC.unmatch_row_dist_method_,
            part_ch_info_,
            MY_SPEC.repartition_type_);
        if (OB_FAIL(do_repart_transmit(repart_slice_calc))) {
          LOG_WARN("failed to do repart transmit for pkey random", K(ret));
        }
      } else if (ObPQDistributeMethod::PARTITION_HASH == MY_SPEC.dist_method_) {
        // pkey hash
        ObSlaveMapPkeyHashIdxCalc repart_slice_calc(ctx_,
            *table_schema,
            MY_SPEC.calc_part_id_expr_,
            MY_SPEC.unmatch_row_dist_method_,
            part_ch_info_,
            task_channels_.count(),
            MY_SPEC.dist_exprs_,
            MY_SPEC.dist_hash_funcs_,
            MY_SPEC.repartition_type_);
        if (OB_FAIL(do_repart_transmit(repart_slice_calc))) {
          LOG_WARN("failed to do repart transmit for pkey hash", K(ret));
        }
      } else {
        ObAffinitizedRepartSliceIdxCalc repart_slice_calc(ctx_,
            *table_schema,
            MY_SPEC.calc_part_id_expr_,
            task_channels_.count(),
            part_ch_info_,
            MY_SPEC.unmatch_row_dist_method_,
            MY_SPEC.repartition_type_);
        if (OB_FAIL(do_repart_transmit(repart_slice_calc))) {
          LOG_WARN("failed to do repart transmit for pkey", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPxRepartTransmitOp::do_repart_transmit(ObRepartSliceIdxCalc& repart_slice_calc)
{
  int ret = OB_SUCCESS;
  // init the ObRepartSliceIdxCalc cache map
  if (OB_FAIL(repart_slice_calc.init_partition_cache_map())) {
    LOG_WARN("failed to repart_slice_calc init partiiton cache map", K(ret));
  } else if (OB_FAIL(repart_slice_calc.init())) {
    LOG_WARN("failed to init repart slice calc", K(ret));
  } else if (OB_FAIL(send_rows(repart_slice_calc))) {
    LOG_WARN("failed to send rows", K(ret));
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

int ObPxRepartTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

}  // end namespace sql
}  // end namespace oceanbase
