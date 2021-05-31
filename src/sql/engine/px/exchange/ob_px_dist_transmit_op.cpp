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

#include "ob_px_dist_transmit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER((ObPxDistTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxDistTransmitSpec, ObPxTransmitSpec), dist_exprs_, dist_hash_funcs_);

int ObPxDistTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxTransmitOp::inner_open())) {
    LOG_WARN("PX transmit open failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int64_t use_shared_bcast_msg = ObBcastOptimization::BC_TO_WORKER;
  if (ObPQDistributeMethod::MAX_VALUE == MY_SPEC.dist_method_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid PX distribution method", K(ret), K(MY_SPEC.dist_method_));
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(
                 share::SYS_VAR__OB_PX_BCAST_OPTIMIZATION, use_shared_bcast_msg))) {
    LOG_WARN("failed to get system variable", K(ret));
  } else if (ObPQDistributeMethod::BROADCAST == MY_SPEC.dist_method_) {
    use_bcast_opt_ = (ObBcastOptimization::BC_TO_SERVER == use_shared_bcast_msg);
    if (use_shared_bcast_msg && OB_FAIL(chs_agent_.init(dfc_,
                                    task_ch_set_,
                                    task_channels_,
                                    ctx_.get_my_session()->get_effective_tenant_id(),
                                    phy_plan_ctx->get_timeout_timestamp()))) {
      LOG_WARN("failed to init chs agent", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("do distribution",
        K(MY_SPEC.dist_method_),
        "method",
        ObPQDistributeMethod::get_type_string(MY_SPEC.dist_method_),
        "consumer PX worker count",
        task_channels_.count());
    switch (MY_SPEC.dist_method_) {
      case ObPQDistributeMethod::HASH: {
        if (OB_FAIL(do_hash_dist())) {
          LOG_WARN("do hash distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::BC2HOST: {
        if (OB_FAIL(do_bc2host_dist())) {
          LOG_WARN("do BC2HOST distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::RANDOM: {
        if (OB_FAIL(do_random_dist())) {
          LOG_WARN("do random distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::BROADCAST: {
        if (OB_FAIL(do_broadcast_dist())) {
          LOG_WARN("do broadcast distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::SM_BROADCAST: {
        if (OB_FAIL(do_sm_broadcast_dist())) {
          LOG_WARN("do broadcast distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::PARTITION_HASH: {
        if (OB_FAIL(do_sm_pkey_hash_dist())) {
          LOG_WARN("do broadcast distribution failed", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("distribution method not supported right now", K(ret), K(MY_SPEC.dist_method_));
      }
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_hash_dist()
{
  int ret = OB_SUCCESS;
  ObHashSliceIdCalc slice_id_calc(
      ctx_.get_allocator(), task_channels_.count(), &MY_SPEC.dist_exprs_, &MY_SPEC.dist_hash_funcs_);
  if (OB_FAIL(send_rows(slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_bc2host_dist()
{
  int ret = OB_SUCCESS;
  ObBc2HostSliceIdCalc::ChannelIdxArray channel_idx;
  ObBc2HostSliceIdCalc::HostIdxArray host_idx;
  auto& channels = task_channels_;
  for (int64_t i = 0; i < task_channels_.count() && OB_SUCC(ret); i++) {
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
    while (idx < channel_idx.count() && task_channels_.at(channel_idx.at(hi.begin_))->get_peer() ==
                                            task_channels_.at(channel_idx.at(idx))->get_peer()) {
      idx++;
    }
    hi.end_ = idx;
    if (OB_FAIL(host_idx.push_back(hi))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("broadcast to host distribution", "channel_cnt", channel_idx.count(), "host_cnt", host_idx.count());
    ObBc2HostSliceIdCalc slice_id_calc(ctx_.get_allocator(), channel_idx, host_idx);
    if (OB_FAIL(send_rows(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_random_dist()
{
  int ret = OB_SUCCESS;
  ObRandomSliceIdCalc slice_id_calc(ctx_.get_allocator(), task_channels_.count());
  if (OB_FAIL(send_rows(slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_broadcast_dist()
{
  int ret = OB_SUCCESS;
  ObBroadcastSliceIdCalc slice_id_calc(ctx_.get_allocator(), task_channels_.count());
  if (!use_bcast_opt_) {
    if (OB_FAIL(send_rows(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  } else if (OB_FAIL(broadcast_rows(slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_sm_broadcast_dist()
{
  int ret = OB_SUCCESS;
  // TODO : to be implement
  /*
  ObPxTransmitCtx &transmit_ctx = static_cast<ObPxTransmitCtx&>(op_ctx);
  ObPxTransmitInput *trans_input = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxTransmitInput, ctx, get_id()))
      || OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to op ctx", "op_id", get_id(), "op_type", get_type(),
              KP(trans_input), K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
      ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(repartition_table_id_, table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K_(repartition_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null. repart sharding requires a table in dfo",
             K_(repartition_table_id), K(ret));
  } else if (OB_FAIL(trans_input->get_part_ch_map(op_ctx.part_ch_info_,
                                                  phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("fail to get channel affinity map", K(ret));
  } else {
    ObSlaveMapBcastIdxCalc slice_idx_calc(ctx,
                                          *table_schema,
                                          repart_func_,
                                          repart_sub_func_,
                                          repart_columns_,
                                          repart_sub_columns_,
                                          unmatch_row_dist_method_,
                                          transmit_ctx.task_channels_.count(),
                                          op_ctx.part_ch_info_);
    if (OB_FAIL(send_rows(ctx, op_ctx, slice_idx_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  */
  return ret;
}

int ObPxDistTransmitOp::do_sm_pkey_hash_dist()
{
  int ret = OB_SUCCESS;
  // TODO : to be implement
  /*
  ObPxTransmitCtx &transmit_ctx = static_cast<ObPxTransmitCtx&>(op_ctx);
  ObPxTransmitInput *trans_input = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxTransmitInput, ctx, get_id()))
      || OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to op ctx", "op_id", get_id(), "op_type", get_type(),
              KP(trans_input), K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
      ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(repartition_table_id_, table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K_(repartition_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null. repart sharding requires a table in dfo",
             K_(repartition_table_id), K(ret));
  } else if (OB_FAIL(trans_input->get_part_ch_map(op_ctx.part_ch_info_,
                                                  phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("fail to get channel affinity map", K(ret));
  } else {
    ObSlaveMapPkeyHashIdxCalc slice_idx_calc(ctx,
                                             *table_schema,
                                             repart_func_,
                                             repart_sub_func_,
                                             repart_columns_,
                                             repart_sub_columns_,
                                             unmatch_row_dist_method_,
                                             transmit_ctx.task_channels_.count(),
                                             op_ctx.part_ch_info_,
                                             op_ctx.expr_ctx_,
                                             hash_dist_columns_,
                                             dist_exprs_);
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
  */
  return ret;
}

int ObPxDistTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

}  // end namespace sql
}  // end namespace oceanbase
