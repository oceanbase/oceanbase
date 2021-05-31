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

#include "ob_px_transmit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_utils.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql {

OB_SERIALIZE_MEMBER(ObPxTransmitOpInput, ch_provider_ptr_);

int ObPxTransmitOpInput::get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else if (OB_FAIL(ch_provider->get_part_ch_map(map, timeout_ts))) {
    LOG_WARN("fail get affinity map from provider", K(ret));
  }
  return ret;
}

int ObPxTransmitOpInput::get_parent_dfo_key(ObDtlDfoKey& key)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else {
    ch_provider->get_parent_dfo_key(key);
    if (!ObDfo::is_valid_dfo_id(key.get_dfo_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(key.get_dfo_id()));
    }
  }
  return ret;
}

int ObPxTransmitOpInput::get_data_ch(ObPxTaskChSet& task_ch_set, int64_t timeout_ts, ObDtlChTotalInfo*& ch_info)
{
  int ret = OB_SUCCESS;
  int64_t task_id = OB_INVALID_ID;
  ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else if (OB_FAIL(
                 ch_provider->get_transmit_data_ch(get_sqc_id(), get_task_id(), timeout_ts, task_ch_set, &ch_info))) {
    LOG_WARN("fail get data ch sets from provider", K(ret));
  }
  return ret;
}
//------------- end ObPxTransmitOpInput -------
OB_SERIALIZE_MEMBER((ObPxTransmitSpec, ObTransmitSpec), partition_id_idx_);

ObPxTransmitSpec::ObPxTransmitSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObTransmitSpec(alloc, type), partition_id_idx_(OB_INVALID_INDEX)
{}

ObPxTransmitOp::ObPxTransmitOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObTransmitOp(exec_ctx, spec, input),
      px_row_allocator_(common::ObModIds::OB_SQL_PX),
      transmited_(false),
      // first_row_(),
      iter_end_(false),
      consume_first_row_(false),
      dfc_unblock_msg_proc_(dfc_),
      chs_agent_(),
      use_bcast_opt_(false),
      part_ch_info_(),
      ch_info_(nullptr)

{}

void ObPxTransmitOp::destroy()
{
  task_ch_set_.reset();
  px_row_allocator_.reset();
  task_channels_.reset();
  dfc_.destroy();
  loop_.reset();
  chs_agent_.~ObDtlChanAgent();
  part_ch_info_.~ObPxPartChInfo();
  ObTransmitOp::destroy();
}

int ObPxTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  opened_ = true;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObPxTransmitOpInput* trans_input = static_cast<ObPxTransmitOpInput*>(input_);
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is NULL", K(ret));
  } else if (OB_UNLIKELY(get_spec().filters_.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(get_spec().filters_.count()));
  } else if (OB_FAIL(ObTransmitOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (child_->get_spec().is_dml_operator() && !child_->get_spec().is_pdml_operator()) {
      iter_end_ = true;
      LOG_TRACE("transmit iter end", K(ret), K(iter_end_));
    } else if ((ret = ObOperator::get_next_row()) != OB_SUCCESS && (ret != OB_ITER_END)) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      iter_end_ = ret == OB_SUCCESS ? false : true;
      LOG_TRACE("transmit iter end", K(ret), K(iter_end_));
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("TIMERECORD ",
        "reserve:=1 name:=TASK dfoid:",
        trans_input->get_dfo_id(),
        "sqcid:",
        trans_input->get_sqc_id(),
        "taskid:",
        trans_input->get_task_id(),
        "start:",
        ObTimeUtility::current_time());
    if (OB_FAIL(init_channel(*trans_input))) {
      LOG_WARN("failed to init channel", K(ret));
    } else {
      metric_.set_id(get_spec().id_);
    }
    LOG_TRACE("TIMERECORD ",
        "reserve:=1 name:=TASK dfoid:",
        trans_input->get_dfo_id(),
        "sqcid:",
        trans_input->get_sqc_id(),
        "taskid:",
        trans_input->get_task_id(),
        "end:",
        ObTimeUtility::current_time());
  }

  if (OB_SUCC(ret)) {
    // NOTE: following methods has do_transmit
    //   - ObPxDistTransmit
    //   - ObPxReduceTransmit
    //   - ObPxRepartTransmit
    if (OB_FAIL(do_transmit())) {
      LOG_WARN("do transmit failed", K(ret));
    }
  }

  return ret;
}

int ObPxTransmitOp::init_dfc(ObDtlDfoKey& key)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(dfc_.init(ctx_.get_my_session()->get_effective_tenant_id(), task_ch_set_.count()))) {
    LOG_WARN("Fail to init dfc", K(ret));
  } else {
    dfc_.set_timeout_ts(phy_plan_ctx->get_timeout_timestamp());
    dfc_.set_transmit();
    dfc_.set_dfo_key(key);
    dfc_.set_op_metric(&metric_);
    dfc_.set_dtl_channel_watcher(&loop_);
    DTL.get_dfc_server().register_dfc(dfc_);
    LOG_TRACE("Worker init dfc", K(key), K(dfc_.is_receive()));
  }
  return ret;
}

int ObPxTransmitOp::init_channel(ObPxTransmitOpInput& trans_input)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObDtlDfoKey key;
  LOG_TRACE("Try to get channel information from SQC", K(lbt()));
  if (OB_FAIL(trans_input.get_data_ch(task_ch_set_, phy_plan_ctx->get_timeout_timestamp(), ch_info_))) {
    LOG_WARN("Fail to get data dtl channel", K(ret));
  } else if (OB_FAIL(trans_input.get_parent_dfo_key(key))) {
    LOG_WARN("Failed to get parent dfo key", K(ret));
  } else if (OB_FAIL(init_dfc(key))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(ObPxTransmitOp::link_ch_sets(task_ch_set_, task_channels_, &dfc_))) {
    LOG_WARN("Fail to link data channel", K(ret));
  } else {
    bool enable_audit = GCONF.enable_sql_audit && ctx_.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    common::ObIArray<dtl::ObDtlChannel*>& channels = task_channels_;
    loop_.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    loop_.register_processor(dfc_unblock_msg_proc_).register_interrupt_processor(interrupt_proc_);
    bool use_interm_result = false;
    ObPxSQCProxy* sqc_proxy = NULL;
    if (OB_ISNULL(sqc_proxy = reinterpret_cast<ObPxSQCProxy*>(trans_input.get_ch_provider_ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get ch provider ptr", K(ret));
    } else {
      use_interm_result = sqc_proxy->get_transmit_use_interm_result();
    }
    loop_.set_interm_result(use_interm_result);
    ARRAY_FOREACH_X(channels, idx, cnt, OB_SUCC(ret))
    {
      dtl::ObDtlChannel* ch = channels.at(idx);
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ch), K(ret));
      } else {
        ch->set_audit(enable_audit);
        ch->set_interm_result(use_interm_result);
        ch->set_compression_type(dfc_.get_compressor_type());
      }
      LOG_TRACE("Transmit channel", K(ch), KP(ch->get_id()), K(ch->get_peer()));
    }
    LOG_TRACE("Get transmit channel ok", "task_id", trans_input.get_task_id(), "ch_cnt", channels.count(), K(ret));
  }
  return ret;
}

int ObPxTransmitOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("get next row from child failed", K(ret));
  }
  return ret;
}

int ObPxTransmitOp::inner_close()
{
  int ret = OB_SUCCESS;
  /* we must release channel even if there is some error happen before */
  chs_agent_.destroy();
  int release_channel_ret = loop_.unregister_all_channel();
  if (release_channel_ret != common::OB_SUCCESS) {
    // the following unlink actions is not safe is any unregister failure happened
    LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
  }

  release_channel_ret = ObPxChannelUtil::unlink_ch_set(task_ch_set_, &dfc_);
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }
  if (OB_FAIL(ObTransmitOp::inner_close())) {
    LOG_WARN("fail close op", K(ret));
  }
  return ret;
}

int ObPxTransmitOp::send_rows(ObSliceIdxCalc& slice_calc)
{
  int ret = OB_SUCCESS;
  int64_t send_row_time_recorder = 0;
  int64_t top_level_get_next_time_used = 0;
  int64_t slice_time = 0;
  int64_t row_count = 0;
  ObObj partition_id;

  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from child op", K(ret), K(child_->get_spec().get_type()));
      } else {
        // iter end
        if (OB_FAIL(send_eof_row())) {  // overwrite err code
          LOG_WARN("fail send eof rows to channels", K(ret));
        }
        break;
      }
    }
    row_count++;
    const ObPxTransmitSpec& spec = static_cast<const ObPxTransmitSpec&>(get_spec());
    metric_.count();
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_FAIL(slice_calc.get_slice_indexes(get_spec().output_, eval_ctx_, slice_idx_array))) {
      LOG_WARN("fail get slice idx", K(ret));
    } else if (dfc_.all_ch_drained()) {
      ret = OB_ITER_END;
      LOG_DEBUG("all channel has been drained");
    } else if (spec.has_partition_id_column_idx() && OB_FAIL(slice_calc.get_previous_row_partition_id(partition_id))) {
      LOG_WARN("failed to get previous row partition_id", K(ret));
    }
    FOREACH_CNT_X(slice_idx, slice_idx_array, OB_SUCC(ret))
    {
      if (OB_FAIL(send_row(*slice_idx, send_row_time_recorder, partition_id.get_int()))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx_array));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    LOG_TRACE("transmit meet a iter end");
  }
  LOG_TRACE("Transmit time record", K(top_level_get_next_time_used), K(slice_time), K(row_count), K(ret));
  return ret;
}

int ObPxTransmitOp::send_eof_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  LOG_TRACE("Send eof row", "op_id", get_spec().id_, "ch_cnt", task_channels_.count(), K(ret));
  int64_t max_loop = 0;
  if (OB_ISNULL(ch_info_) || ch_info_->receive_exec_server_.total_task_cnt_ != task_channels_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ch info is null", K(ret), KP(ch_info_), K(task_channels_.count()));
  } else {
    ObTransmitEofAsynSender eof_asyn_sender(
        task_channels_, ch_info_, true, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_);
    if (OB_FAIL(eof_asyn_sender.asyn_send())) {
      LOG_WARN("failed to asyn send drain", K(ret), K(lbt()));
    }
  }
  return ret;
}

int ObPxTransmitOp::broadcast_rows(ObSliceIdxCalc& slice_calc)
{
  int ret = OB_SUCCESS;
  UNUSED(slice_calc);
  int64_t send_row_time_recorder = 0;
  int64_t top_level_get_next_time_used = 0;
  int64_t slice_time = 0;
  int64_t row_count = 0;

  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from child op", K(ret), K(child_->get_spec().get_type()));
      } else {
        // iter end
        if (OB_FAIL(broadcast_eof_row())) {  // overwrite err code
          LOG_WARN("fail send eof rows to channels", K(ret));
        }
        break;
      }
    }
    row_count++;
    metric_.count();
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (dfc_.all_ch_drained()) {
      ret = OB_ITER_END;
      LOG_DEBUG("all channel has been drained");
    } else {
      ObPxNewRow px_row(get_spec().output_);
      ret = chs_agent_.broadcast_row(px_row, &eval_ctx_);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    LOG_TRACE("transmit meet a iter end");
  }
  LOG_TRACE("Transmit time record", K(top_level_get_next_time_used), K(slice_time), K(row_count), K(ret));
  return ret;
}

int ObPxTransmitOp::update_row(int partition_id_column_idx, int64_t partition_id)
{
  int ret = OB_SUCCESS;
  if (get_spec().output_.count() <= partition_id_column_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of output expr is error", K(ret), K(get_spec().output_.count()), K(partition_id_column_idx));
  } else {
    ObExpr* expr = get_spec().output_.at(partition_id_column_idx);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the expr is null", K(ret));
    } else if (expr->type_ != T_PDML_PARTITION_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the type of expr is not pdml partition id", K(ret));
    } else {
      expr->locate_datum_for_write(eval_ctx_).set_int(partition_id);
      expr->get_eval_info(eval_ctx_).evaluated_ = true;
    }
  }
  return ret;
}

// TODO : add update row here.
int ObPxTransmitOp::send_row(int64_t slice_idx, int64_t& time_recorder, int64_t partition_id)
{
  UNUSED(time_recorder);
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel* ch = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  LOG_DEBUG("Send row begin", K(ret), K(ObToStringExprRow(eval_ctx_, get_spec().output_)));
  const ObPxTransmitSpec& spec = static_cast<const ObPxTransmitSpec&>(get_spec());
  common::ObIArray<dtl::ObDtlChannel*>& channels = task_channels_;
  if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == slice_idx) {
    op_monitor_info_.otherstat_1_value_++;
    op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
    // do nothing, just drop this row
  } else if (slice_idx < 0 || slice_idx >= channels.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slice idx", K(ret), K(lbt()), K(slice_idx), "channels", channels.count());
  } else if (NULL == (ch = channels.at(slice_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (ch->is_drain()) {
    // if drain, don't send again
    LOG_TRACE("drain channel", KP(ch->get_id()));
  } else if (spec.has_partition_id_column_idx() &&
             OB_FAIL(update_row(spec.get_partition_id_column_idx(), partition_id))) {
    LOG_WARN("failed to update cur row expr", K(ret));
  } else {
    ObPxNewRow px_row(get_spec().output_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ch->send(px_row, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail send row to slice channel", K(px_row), K(slice_idx), K(ret));
      }
    } else {
      LOG_DEBUG("send row to slice channel", K(px_row), K(slice_idx));
    }
  }
  LOG_DEBUG("Send row", K(slice_idx), K(ret));
  return ret;
}

int ObPxTransmitOp::broadcast_eof_row()
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel* ch = NULL;
  LOG_TRACE("broadcast eof row", "op_id", get_spec().id_, "ch_cnt", task_channels_.count(), K(ret));
  ObPxNewRow px_eof_row;
  px_eof_row.set_eof_row();
  px_eof_row.set_data_type(ObDtlMsgType::PX_DATUM_ROW);
  if (OB_FAIL(chs_agent_.broadcast_row(px_eof_row, &eval_ctx_, true))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (OB_FAIL(chs_agent_.flush())) {
    LOG_WARN("fail flush row to slice channel", K(ret));
  }
  return ret;
}

int ObPxTransmitOp::next_row()
{
  int ret = OB_SUCCESS;
  if ((child_->get_spec().is_dml_operator() && !child_->get_spec().is_pdml_operator()) || iter_end_) {
    ret = OB_ITER_END;
    consume_first_row_ = true;
    LOG_TRACE("transmit iter end", K(ret), K(iter_end_));
  } else if (!consume_first_row_) {
    consume_first_row_ = true;
  } else {
    ret = ObOperator::get_next_row();
  }
  return ret;
}

int ObPxTransmitOp::link_ch_sets(
    ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  for (int64_t idx = 0; idx < ch_set.count() && OB_SUCC(ret); ++idx) {
    dtl::ObDtlChannel* ch = NULL;
    if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
      LOG_WARN("fail get channel info", K(idx), K(ret));
    } else if (OB_FAIL(ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      LOG_WARN("fail add qc channel", K(ret));
    } else if (OB_FAIL(channels.push_back(ch))) {
      LOG_WARN("fail push back channel ptr", K(ci), K(ret));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
