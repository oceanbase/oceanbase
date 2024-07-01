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
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxRepartTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxRepartTransmitSpec, ObPxTransmitSpec),
                    calc_tablet_id_expr_,
                    dist_exprs_,
                    dist_hash_funcs_,
                    sort_cmp_funs_,
                    sort_collations_,
                    ds_tablet_ids_,
                    repartition_exprs_);

ObPxRepartTransmitSpec::ObPxRepartTransmitSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxTransmitSpec(alloc, type),
    calc_tablet_id_expr_(NULL),
    dist_exprs_(alloc),
    dist_hash_funcs_(alloc),
    sort_cmp_funs_(alloc),
    sort_collations_(alloc),
    ds_tablet_ids_(alloc),
    repartition_exprs_(alloc)
{
}

int ObPxRepartTransmitSpec::register_to_datahub(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxTransmitSpec::register_to_datahub(exec_ctx))) {
    LOG_WARN("failed to register init channel msg", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    typedef ObDynamicSampleWholeMsg::WholeMsgProvider MsgProvider;
    void *buf = nullptr;
    if (OB_ISNULL(buf = exec_ctx.get_allocator().alloc(sizeof(MsgProvider)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MsgProvider *provider = new (buf) MsgProvider();
      ObSqcCtx &sqc_ctx = exec_ctx.get_sqc_handler()->get_sqc_ctx();
      if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), dtl::DH_DYNAMIC_SAMPLE_WHOLE_MSG, *provider))) {
        LOG_WARN("fail add whole msg provider", K(ret));
      } else {
        buf = nullptr;
      }
      if (nullptr != buf) {
        provider->~MsgProvider();
        provider = nullptr;
        exec_ctx.get_allocator().free(buf);
      }
    }
  }
  return ret;
}

ObPxRepartTransmitOp::ObPxRepartTransmitOp(
  ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
: ObPxTransmitOp(exec_ctx, spec, input)
{
}

int ObPxRepartTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("Inner open px fifo transmit", "op_id", MY_SPEC.id_);
  if (OB_FAIL(ObPxTransmitOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

void ObPxRepartTransmitOp::destroy()
{
  ObPxTransmitOp::destroy();
}

int ObPxRepartTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObPxRepartTransmitOpInput *trans_input = static_cast<ObPxRepartTransmitOpInput*>(input_);

  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_)) || OB_ISNULL(trans_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to op ctx", "op_id", MY_SPEC.id_, "op_type", MY_SPEC.type_,
              KP(trans_input), K(ret));
  } else if (!MY_SPEC.is_repart_exchange()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect repart repartition", K(ret));
  } else if (OB_INVALID_ID == MY_SPEC.repartition_ref_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("repartition table id should be set for repart transmit op", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                ctx_.get_my_session()->get_effective_tenant_id(),
                schema_guard))) {
      LOG_WARN("faile to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(
               ctx_.get_my_session()->get_effective_tenant_id(),
               MY_SPEC.repartition_ref_table_id_, table_schema))) {
      LOG_WARN("faile to get table schema", K(ret), K(MY_SPEC.repartition_ref_table_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("table schema is null. repart sharding requires a table in dfo",
               K(MY_SPEC.repartition_ref_table_id_), K(ret));
    } else if (OB_FAIL(trans_input->get_part_ch_map(part_ch_info_,
                                                    phy_plan_ctx->get_timeout_timestamp()))) {
      LOG_WARN("fail to get channel affinity map", K(ret));
    } else {
      switch (MY_SPEC.dist_method_) {
        case ObPQDistributeMethod::PARTITION_RANDOM: {
          // pkey random
          ObSlaveMapPkeyRandomIdxCalc repart_slice_calc(ctx_,
                                                       *table_schema,
                                                       MY_SPEC.calc_tablet_id_expr_,
                                                       MY_SPEC.unmatch_row_dist_method_,
                                                       MY_SPEC.null_row_dist_method_,
                                                       part_ch_info_,
                                                       MY_SPEC.repartition_type_);
          if (OB_FAIL(do_repart_transmit<ObSliceIdxCalc::SM_REPART_RANDOM>(repart_slice_calc))) {
            LOG_WARN("failed to do repart transmit for pkey random", K(ret));
          }
          break;
        }
        case ObPQDistributeMethod::PARTITION_HASH: {
          // pkey hash
          ObSlaveMapPkeyHashIdxCalc repart_slice_calc(ctx_,
                                                      *table_schema,
                                                      MY_SPEC.calc_tablet_id_expr_,
                                                      MY_SPEC.unmatch_row_dist_method_,
                                                      MY_SPEC.null_row_dist_method_,
                                                      part_ch_info_,
                                                      task_channels_.count(),
                                                      MY_SPEC.dist_exprs_,
                                                      MY_SPEC.dist_hash_funcs_,
                                                      MY_SPEC.repartition_type_);
          if (OB_FAIL(do_repart_transmit<ObSliceIdxCalc::SM_REPART_HASH>(repart_slice_calc))) {
            LOG_WARN("failed to do repart transmit for pkey random", K(ret));
          }
          break;
        }
        case ObPQDistributeMethod::PARTITION_RANGE: {
          ObSlaveMapPkeyRangeIdxCalc range_slice_calc(ctx_,
                                                      *table_schema,
                                                      MY_SPEC.calc_tablet_id_expr_,
                                                      MY_SPEC.unmatch_row_dist_method_,
                                                      MY_SPEC.null_row_dist_method_,
                                                      part_ch_info_,
                                                      MY_SPEC.dist_exprs_,
                                                      &MY_SPEC.sort_cmp_funs_,
                                                      &MY_SPEC.sort_collations_,
                                                      MY_SPEC.repartition_type_);
          if (OB_FAIL(dynamic_sample())) {
            LOG_WARN("fail to do dynamic sample", K(ret));
          } else if (OB_FAIL(do_repart_transmit<ObSliceIdxCalc::SM_REPART_RANGE>(range_slice_calc))) {
            LOG_WARN("failed to do repart transmit for pkey range", K(ret));
          }
          break;
        }
        default: {
          if (MY_SPEC.need_null_aware_shuffle_) {
            ObNullAwareAffinitizedRepartSliceIdxCalc repart_slice_calc(ctx_,
                                                                  *table_schema,
                                                                  MY_SPEC.calc_tablet_id_expr_,
                                                                  task_channels_.count(),
                                                                  part_ch_info_,
                                                                  MY_SPEC.unmatch_row_dist_method_,
                                                                  MY_SPEC.repartition_type_,
                                                                  &MY_SPEC.dist_exprs_,
                                                                  &MY_SPEC.dist_hash_funcs_,
                                                                  &MY_SPEC.repartition_exprs_);
            if (OB_FAIL(do_repart_transmit<ObSliceIdxCalc::NULL_AWARE_AFFINITY_REPART>(repart_slice_calc))) {
              LOG_WARN("failed to do repart transmit for pkey", K(ret));
            }
          } else {
            ObAffinitizedRepartSliceIdxCalc repart_slice_calc(ctx_,
                                                              *table_schema,
                                                              MY_SPEC.calc_tablet_id_expr_,
                                                              task_channels_.count(),
                                                              part_ch_info_,
                                                              MY_SPEC.unmatch_row_dist_method_,
                                                              MY_SPEC.null_row_dist_method_,
                                                              MY_SPEC.repartition_type_,
                                                              &MY_SPEC.dist_exprs_,
                                                              &MY_SPEC.dist_hash_funcs_);
            if (OB_FAIL(do_repart_transmit<ObSliceIdxCalc::AFFINITY_REPART>(repart_slice_calc))) {
              LOG_WARN("failed to do repart transmit for pkey", K(ret));
            }
          }
          break;
        }
      }
    }
  }
  return ret;
}

template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
int ObPxRepartTransmitOp::do_repart_transmit(ObRepartSliceIdxCalc &repart_slice_calc)
{
  int ret = OB_SUCCESS;
  // init the ObRepartSliceIdxCalc cache map
  if (OB_FAIL(repart_slice_calc.init(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("failed to init repart slice calc", K(ret));
  } else if (OB_FAIL(send_rows<CALC_TYPE>(repart_slice_calc))) {
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

int ObPxRepartTransmitOp::build_ds_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg)
{
  int ret = OB_SUCCESS;
  piece_msg.reset();
  CK(!MY_SPEC.ds_tablet_ids_.empty());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(piece_msg.tablet_ids_.assign(MY_SPEC.ds_tablet_ids_))) {
    LOG_WARN("fail to assign partition ids", K(ret));
  } else if (FALSE_IT(piece_msg.sample_type_ = MY_SPEC.sample_type_)) {
  } else if (is_object_sample()) {
    OZ(build_object_sample_piece_msg(expected_range_count, piece_msg));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sample type", K(ret), K(MY_SPEC.sample_type_));
  }
  return ret;
}

int ObPxRepartTransmitOp::dynamic_sample()
{
  int ret = OB_SUCCESS;
  if (!sample_done_) {
    ObDynamicSamplePieceMsg piece_msg;
    if (OB_FAIL(build_ds_piece_msg(task_channels_.count(), piece_msg))) {
      LOG_WARN("fail to buil ds piece msg", K(ret));
    } else if (OB_FAIL(do_datahub_dynamic_sample(MY_SPEC.id_, piece_msg))) {
      LOG_WARN("fail to do dynamic sample");
    } else if (OB_FAIL(child_->rescan())) {
      LOG_WARN("fail to rescan child", K(ret));
    } else if (OB_FAIL(ObOperator::inner_rescan())) {
      LOG_WARN("fail to inner rescan", K(ret));
    } else {
      iter_end_ = false;
      consume_first_row_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sample status", K(ret));
  }
  return ret;
}

int ObPxRepartTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

} // end namespace sql
} // end namespace oceanbase
