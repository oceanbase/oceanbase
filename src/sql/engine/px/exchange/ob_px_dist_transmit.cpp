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

#include "ob_px_dist_transmit.h"
#include "sql/executor/ob_slice_calc.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
OB_SERIALIZE_MEMBER((ObPxDistTransmitInput, ObPxTransmitInput));

int ObPxDistTransmit::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxTransmit::inner_open(ctx))) {
    LOG_WARN("PX transmit open failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmit::do_transmit(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObPxTransmitInput* trans_input = NULL;
  ObPxTransmitCtx* op_ctx = NULL;
  int64_t use_shared_bcast_msg = ObBcastOptimization::BC_TO_WORKER;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx)) ||
      OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxTransmitCtx, ctx, get_id())) ||
      OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxTransmitInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get NULL operator context", K(ret));
  } else if (ObPQDistributeMethod::MAX_VALUE == dist_method_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid PX distribution method", K(ret), K(dist_method_));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session info", K(ret));
  } else if (OB_FAIL(ctx.get_my_session()->get_sys_variable(
                 share::SYS_VAR__OB_PX_BCAST_OPTIMIZATION, use_shared_bcast_msg))) {
    LOG_WARN("failed to get system variable", K(ret));
  } else if (ObPQDistributeMethod::BROADCAST == dist_method_) {
    // old engine dont' use dtl broadcast optimization
    op_ctx->use_bcast_opt_ = false;
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("do distribution",
        K(dist_method_),
        "method",
        ObPQDistributeMethod::get_type_string(dist_method_),
        "consumer PX worker count",
        op_ctx->task_channels_.count());
    switch (dist_method_) {
      case ObPQDistributeMethod::HASH: {
        if (OB_FAIL(do_hash_dist(ctx, *op_ctx))) {
          LOG_WARN("do hash distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::BC2HOST: {
        if (OB_FAIL(do_bc2host_dist(ctx, *op_ctx))) {
          LOG_WARN("do BC2HOST distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::RANDOM: {
        if (OB_FAIL(do_random_dist(ctx, *op_ctx))) {
          LOG_WARN("do random distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::BROADCAST: {
        if (OB_FAIL(do_broadcast_dist(ctx, *op_ctx))) {
          LOG_WARN("do broadcast distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::SM_BROADCAST: {
        if (OB_FAIL(do_sm_broadcast_dist(ctx, *op_ctx))) {
          LOG_WARN("do broadcast distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::PARTITION_HASH: {
        if (OB_FAIL(do_sm_pkey_hash_dist(ctx, *op_ctx))) {
          LOG_WARN("do broadcast distribution failed", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("distribution method not supported right now", K(ret), K(dist_method_));
      }
    }
  }
  return ret;
}

int ObPxDistTransmit::do_hash_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObHashSliceIdCalc slice_id_calc(
      ctx.get_allocator(), op_ctx.expr_ctx_, hash_dist_columns_, dist_exprs_, op_ctx.task_channels_.count());
  if (OB_FAIL(send_rows(ctx, op_ctx, slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmit::do_bc2host_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObBc2HostSliceIdCalc::ChannelIdxArray channel_idx;
  ObBc2HostSliceIdCalc::HostIdxArray host_idx;
  auto& channels = op_ctx.task_channels_;
  for (int64_t i = 0; i < channels.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(channel_idx.push_back(i))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(channel_idx.begin(), channel_idx.end(), [&channels](int64_t l, int64_t r) {
      return channels.at(l)->get_peer() < channels.at(r)->get_peer();
    });
  }
  ObBc2HostSliceIdCalc::HostIndex hi;
  uint64_t idx = 0;
  while (OB_SUCC(ret) && idx < channel_idx.count()) {
    hi.begin_ = idx;
    while (idx < channel_idx.count() &&
           channels.at(channel_idx.at(hi.begin_))->get_peer() == channels.at(channel_idx.at(idx))->get_peer()) {
      idx++;
    }
    hi.end_ = idx;
    if (OB_FAIL(host_idx.push_back(hi))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("broadcast to host distribution", "channel_cnt", channel_idx.count(), "host_cnt", host_idx.count());
    ObBc2HostSliceIdCalc slice_id_calc(ctx.get_allocator(), channel_idx, host_idx);
    if (OB_FAIL(send_rows(ctx, op_ctx, slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmit::do_random_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObRandomSliceIdCalc slice_id_calc(ctx.get_allocator(), op_ctx.task_channels_.count());
  if (OB_FAIL(send_rows(ctx, op_ctx, slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmit::do_broadcast_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObBroadcastSliceIdCalc slice_id_calc(ctx.get_allocator(), op_ctx.task_channels_.count());
  ObPxTransmitCtx& transmit_ctx = static_cast<ObPxTransmitCtx&>(op_ctx);
  if (!transmit_ctx.use_bcast_opt_) {
    if (OB_FAIL(send_rows(ctx, op_ctx, slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  } else if (OB_FAIL(broadcast_rows(ctx, op_ctx, slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmit::do_sm_broadcast_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObPxTransmitCtx& transmit_ctx = static_cast<ObPxTransmitCtx&>(op_ctx);
  ObPxTransmitInput* trans_input = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxTransmitInput, ctx, get_id())) ||
      OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to op ctx", "op_id", get_id(), "op_type", get_type(), KP(trans_input), K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(repartition_table_id_, table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K_(repartition_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null. repart sharding requires a table in dfo", K_(repartition_table_id), K(ret));
  } else if (OB_FAIL(trans_input->get_part_ch_map(op_ctx.part_ch_info_, phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("fail to get channel affinity map", K(ret));
  } else {
    ObSlaveMapBcastIdxCalc slice_idx_calc(ctx,
        *table_schema,
        &repart_func_,
        &repart_sub_func_,
        &repart_columns_,
        &repart_sub_columns_,
        unmatch_row_dist_method_,
        transmit_ctx.task_channels_.count(),
        op_ctx.part_ch_info_,
        repartition_type_);
    if (OB_FAIL(send_rows(ctx, op_ctx, slice_idx_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmit::do_sm_pkey_hash_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObPxTransmitCtx& transmit_ctx = static_cast<ObPxTransmitCtx&>(op_ctx);
  ObPxTransmitInput* trans_input = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxTransmitInput, ctx, get_id())) ||
      OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to op ctx", "op_id", get_id(), "op_type", get_type(), KP(trans_input), K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(repartition_table_id_, table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K_(repartition_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null. repart sharding requires a table in dfo", K_(repartition_table_id), K(ret));
  } else if (OB_FAIL(trans_input->get_part_ch_map(op_ctx.part_ch_info_, phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("fail to get channel affinity map", K(ret));
  } else {
    ObSlaveMapPkeyHashIdxCalc slice_idx_calc(ctx,
        *table_schema,
        &repart_func_,
        &repart_sub_func_,
        &repart_columns_,
        &repart_sub_columns_,
        unmatch_row_dist_method_,
        transmit_ctx.task_channels_.count(),
        op_ctx.part_ch_info_,
        op_ctx.expr_ctx_,
        hash_dist_columns_,
        dist_exprs_,
        repartition_type_);
    if (OB_FAIL(slice_idx_calc.init())) {
      LOG_WARN("failed to init slice idx calc", K(ret));
    } else if (OB_FAIL(send_rows(ctx, op_ctx, slice_idx_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = slice_idx_calc.destroy())) {
      LOG_WARN("failed to destroy", K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObPxDistTransmit::inner_close(ObExecContext& ctx) const
{
  return ObPxTransmit::inner_close(ctx);
}

int ObPxDistTransmit::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxTransmitCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxTransmitCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("init current row failed");
  }
  return ret;
}

int ObPxDistTransmit::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxDistTransmitInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxDistTransmitInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("create phy operator input failed", K(ret));
  }
  UNUSED(input);
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
