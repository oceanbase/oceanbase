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

#include "sql/engine/px/exchange/ob_px_transmit.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/executor/ob_range_hash_key_getter.h"
#include "sql/executor/ob_slice_calc.h"
#include "sql/dtl/ob_dtl.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::share::schema;

// ObPxExchangeInput do NOT need serialize, filled by TaskProcessor
OB_SERIALIZE_MEMBER(ObPxTransmitInput, ch_provider_ptr_);

int ObPxTransmitInput::get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts)
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

int ObPxTransmitInput::get_parent_dfo_key(ObDtlDfoKey& key)
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

int ObPxTransmitInput::get_data_ch(ObPxTaskChSet& task_ch_set, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int64_t task_id = OB_INVALID_ID;
  ObPxTaskChSets ch_sets;
  ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else if (OB_FAIL(
                 ch_provider->get_transmit_data_ch(get_sqc_id(), get_task_id(), timeout_ts, task_ch_set, nullptr))) {
    LOG_WARN("fail get data ch sets from provider", K(ret));
  }
  return ret;
}

//////////////////////////////////////////

ObPxTransmit::ObPxTransmit(common::ObIAllocator& alloc) : ObTransmit(alloc)
{}

ObPxTransmit::~ObPxTransmit()
{}

ObPxTransmit::ObPxTransmitCtx::ObPxTransmitCtx(ObExecContext& ctx)
    : ObTransmit::ObTransmitCtx(ctx),
      px_row_allocator_(common::ObModIds::OB_SQL_PX),
      transmited_(false),
      first_row_(nullptr),
      iter_end_(false),
      consume_first_row_(false),
      dfc_unblock_msg_proc_(dfc_),
      chs_agent_(),
      use_bcast_opt_(false),
      part_ch_info_()
{}

ObPxTransmit::ObPxTransmitCtx::~ObPxTransmitCtx()
{
  destroy();
}

void ObPxTransmit::ObPxTransmitCtx::destroy()
{
  ObTransmitCtx::destroy();
  task_ch_set_.reset();
  px_row_allocator_.reset();
  task_channels_.reset();
  dfc_.destroy();
  loop_.reset();
  chs_agent_.~ObDtlChanAgent();
  part_ch_info_.~ObPxPartChInfo();
}

int ObPxTransmit::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObPxTransmitInput* trans_input = NULL;
  ObPxTransmitCtx* transmit_ctx = NULL;
  const ObNewRow* row = nullptr;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is NULL", K(ret));
  } else if (OB_UNLIKELY(filter_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(filter_exprs_.get_size()));
  } else if (OB_FAIL(ObTransmit::inner_open(exec_ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx)) ||
             OB_ISNULL(transmit_ctx = GET_PHY_OPERATOR_CTX(ObPxTransmitCtx, exec_ctx, get_id())) ||
             OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObPxTransmitInput, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (child_op_->is_dml_operator() && !child_op_->is_pdml_operator()) {
      transmit_ctx->iter_end_ = true;
      LOG_TRACE("transmit iter end", K(ret), K(transmit_ctx->iter_end_));
    } else if ((ret = get_next_row(exec_ctx, row)) != OB_SUCCESS && (ret != OB_ITER_END)) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      transmit_ctx->iter_end_ = ret == OB_SUCCESS ? false : true;
      LOG_TRACE("transmit iter end", K(ret), K(transmit_ctx->iter_end_));
      ret = OB_SUCCESS;
      if (OB_NOT_NULL(row)) {
        transmit_ctx->first_row_ = row;
      }
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
    if (OB_FAIL(transmit_ctx->init_channel(exec_ctx, *trans_input))) {
      LOG_WARN("failed to init channel", K(ret));
    } else {
      transmit_ctx->metric_.set_id(get_id());
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
    // NOTE: do_transmit by
    //   - ObPxDistTransmit
    //   - ObPxReduceTransmit
    //   - ObPxRepartTransmit
    if (OB_FAIL(do_transmit(exec_ctx))) {
      LOG_WARN("do transmit failed", K(ret));
    }
  }

  return ret;
}

int ObPxTransmit::ObPxTransmitCtx::init_dfc(ObExecContext& ctx, ObDtlDfoKey& key)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The phy plan ctx is null", K(ret));
  } else if (OB_FAIL(dfc_.init(ctx.get_my_session()->get_effective_tenant_id(), task_ch_set_.count()))) {
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

int ObPxTransmit::ObPxTransmitCtx::init_channel(ObExecContext& ctx, ObPxTransmitInput& trans_input)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObDtlDfoKey key;
  LOG_TRACE("Try to get channel information from SQC");
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The phy plan ctx is null", K(ret));
  } else if (OB_FAIL(trans_input.get_data_ch(task_ch_set_, phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("Fail to get data dtl channel", K(ret));
  } else if (OB_FAIL(trans_input.get_parent_dfo_key(key))) {
    LOG_WARN("Failed to get parent dfo key", K(ret));
  } else if (OB_FAIL(init_dfc(ctx, key))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(ObPxTransmit::link_ch_sets(task_ch_set_, task_channels_, &dfc_))) {
    LOG_WARN("Fail to link data channel", K(ret));
  } else {
    bool enable_audit = GCONF.enable_sql_audit && ctx.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    common::ObIArray<dtl::ObDtlChannel*>& channels = task_channels_;
    loop_.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    bool use_interm_result = false;
    loop_.register_processor(dfc_unblock_msg_proc_).register_interrupt_processor(interrupt_proc_);
    if (OB_SUCC(ret)) {
      ObPxSQCProxy* sqc_proxy = NULL;
      if (OB_ISNULL(sqc_proxy = reinterpret_cast<ObPxSQCProxy*>(trans_input.get_ch_provider_ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ch provider ptr", K(ret));
      } else {
        use_interm_result = sqc_proxy->get_transmit_use_interm_result();
      }
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
      }
      LOG_TRACE("Transmit channel", K(ch), KP(ch->get_id()), K(ch->get_peer()));
    }
    LOG_TRACE("Get transmit channel ok", "task_id", trans_input.get_task_id(), "ch_cnt", channels.count(), K(ret));
  }
  return ret;
}

int ObPxTransmit::get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ret = ObPhyOperator::get_next_row(exec_ctx, row);
  return ret;
}

int ObPxTransmit::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is NULL", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(exec_ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  }
  return ret;
}

int ObPxTransmit::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPxTransmitCtx* transmit_ctx = NULL;
  if (OB_ISNULL(transmit_ctx = GET_PHY_OPERATOR_CTX(ObPxTransmitCtx, exec_ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else if (OB_FAIL(ObTransmit::inner_close(exec_ctx))) {
    LOG_WARN("fail close op", K(ret));
  }
  /* we must release channel even if there is some error happen before */
  if (OB_NOT_NULL(transmit_ctx)) {
    transmit_ctx->chs_agent_.destroy();
    dtl::ObDtlChannelLoop& loop = transmit_ctx->loop_;
    int release_channel_ret = loop.unregister_all_channel();
    if (release_channel_ret != common::OB_SUCCESS) {
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
    }

    release_channel_ret = ObPxChannelUtil::unlink_ch_set(transmit_ctx->task_ch_set_, &transmit_ctx->dfc_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
  }
  return ret;
}

int ObPxTransmit::send_rows(ObExecContext& exec_ctx, ObPxTransmitCtx& transmit_ctx, ObSliceIdxCalc& slice_calc) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  int64_t send_row_time_recorder = 0;
  int64_t top_level_get_next_time_used = 0;
  int64_t slice_time = 0;
  int64_t row_count = 0;
  ObObj partition_id;

  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(next_row(exec_ctx, row, transmit_ctx))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from child op", K(ret), K(child_op_->get_type()));
      } else {
        // iter end
        if (OB_FAIL(send_eof_row(transmit_ctx, exec_ctx))) {  // overwrite err code
          LOG_WARN("fail send eof rows to channels", K(ret));
        }
        break;
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is NULL", K(ret));
    }
    row_count++;
    transmit_ctx.metric_.count();
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_FAIL(slice_calc.get_slice_indexes(*row, slice_idx_array))) {
      LOG_WARN("fail get slice idx", K(ret));
    } else if (transmit_ctx.dfc_.all_ch_drained()) {
      ret = OB_ITER_END;
      LOG_DEBUG("all channel has been drained");
    } else if (has_partition_id_column_idx() && OB_FAIL(slice_calc.get_previous_row_partition_id(partition_id))) {
      LOG_WARN("failed to get previous row partition_id", K(ret));
    }
    FOREACH_CNT_X(slice_idx, slice_idx_array, OB_SUCC(ret))
    {
      if (OB_FAIL(send_row(exec_ctx,
              transmit_ctx,
              *slice_idx,
              partition_id,
              *row,
              send_row_time_recorder))) {  // via dtl
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

int ObPxTransmit::broadcast_rows(
    ObExecContext& exec_ctx, ObPxTransmitCtx& transmit_ctx, ObSliceIdxCalc& slice_calc) const
{
  int ret = OB_SUCCESS;
  UNUSED(slice_calc);
  const ObNewRow* row = NULL;
  int64_t send_row_time_recorder = 0;
  int64_t top_level_get_next_time_used = 0;
  int64_t slice_time = 0;
  int64_t row_count = 0;

  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(next_row(exec_ctx, row, transmit_ctx))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from child op", K(ret), K(child_op_->get_type()));
      } else {
        // iter end
        if (OB_FAIL(broadcast_eof_row(transmit_ctx))) {  // overwrite err code
          LOG_WARN("fail send eof rows to channels", K(ret));
        }
        break;
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is NULL", K(ret));
    }
    row_count++;
    transmit_ctx.metric_.count();
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (transmit_ctx.dfc_.all_ch_drained()) {
      ret = OB_ITER_END;
      LOG_DEBUG("all channel has been drained");
    } else {
      ObPxNewRow px_row(*row);
      ret = transmit_ctx.chs_agent_.broadcast_row(px_row);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    LOG_TRACE("transmit meet a iter end");
  }
  LOG_TRACE("Transmit time record", K(top_level_get_next_time_used), K(slice_time), K(row_count), K(ret));
  return ret;
}

int ObPxTransmit::send_row(ObExecContext& ctx, ObPxTransmitCtx& transmit_ctx, int64_t slice_idx, ObObj& partition_id,
    const ObNewRow& row, int64_t& time_recorder) const
{
  UNUSED(time_recorder);
  int ret = OB_SUCCESS;
  const ObNewRow* sending_row = &row;
  dtl::ObDtlChannel* ch = NULL;
  LOG_DEBUG("Send row begin", K(row), K(ret));
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  common::ObIArray<dtl::ObDtlChannel*>& channels = transmit_ctx.task_channels_;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is null", K(ret));
  } else if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == slice_idx) {
    // do nothing, just drop this row
    transmit_ctx.op_monitor_info_.otherstat_1_value_++;
    transmit_ctx.op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
  } else if (OB_UNLIKELY(slice_idx < 0 || slice_idx >= channels.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slice idx", K(ret), K(lbt()), K(slice_idx), "channels", channels.count(), K(row));
  } else if (OB_ISNULL(ch = channels.at(slice_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (has_partition_id_column_idx() && OB_FAIL(copy_cur_row(transmit_ctx.get_cur_row(), sending_row, true))) {
    LOG_WARN("failed to copy cur row", K(ret));
  } else if (has_partition_id_column_idx() &&
             OB_FAIL(update_row(transmit_ctx.get_cur_row(), get_partition_id_column_idx(), partition_id))) {
    LOG_WARN("failed to update row", K(ret), K(get_partition_id_column_idx()), K(partition_id));
  } else {
    LOG_DEBUG("send row",
        K(get_name()),
        K(has_partition_id_column_idx()),
        K(get_partition_id_column_idx()),
        K(partition_id),
        K(*sending_row));
    ObPxNewRow px_row(*sending_row);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ch->send(px_row, phy_plan_ctx->get_timeout_timestamp()))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail send row to slice channel", K(px_row), K(slice_idx), K(ret));
      }
    } else {
      LOG_DEBUG("send row to slice channel", K(px_row), K(slice_idx));
    }
  }
  LOG_DEBUG("Send row", K(slice_idx), K(row), K(ret));
  return ret;
}

int ObPxTransmit::send_eof_row(ObPxTransmitCtx& transmit_ctx, ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel* ch = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  common::ObIArray<dtl::ObDtlChannel*>& channels = transmit_ctx.task_channels_;
  LOG_TRACE("Send eof row", "op_id", get_id(), "ch_cnt", channels.count(), K(ret));
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is null", K(ret));
  } else {
    for (int64_t slice_idx = 0; (OB_SUCCESS == ret) && slice_idx < channels.count(); ++slice_idx) {
      if (NULL == (ch = channels.at(slice_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL ptr", K(ret));
      } else {
        ObPxNewRow px_eof_row;
        px_eof_row.set_eof_row();
        px_eof_row.set_data_type(ObDtlMsgType::PX_CHUNK_ROW);
        if (OB_FAIL(ch->send(px_eof_row, phy_plan_ctx->get_timeout_timestamp(), nullptr, true))) {
          LOG_WARN("fail send eof row to slice channel", K(px_eof_row), K(slice_idx), K(ret));
        } else if (OB_FAIL(ch->flush())) {
          LOG_WARN("failed to flush send msg", K(px_eof_row), K(slice_idx), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPxTransmit::broadcast_eof_row(ObPxTransmitCtx& transmit_ctx) const
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel* ch = NULL;
  common::ObIArray<dtl::ObDtlChannel*>& channels = transmit_ctx.task_channels_;
  LOG_TRACE("broadcast eof row", "op_id", get_id(), "ch_cnt", channels.count(), K(ret));
  ObPxNewRow px_eof_row;
  px_eof_row.set_eof_row();
  px_eof_row.set_data_type(ObDtlMsgType::PX_CHUNK_ROW);
  if (OB_FAIL(transmit_ctx.chs_agent_.broadcast_row(px_eof_row, nullptr, true))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (OB_FAIL(transmit_ctx.chs_agent_.flush())) {
    LOG_WARN("fail flush row to slice channel", K(ret));
  }
  return ret;
}

int ObPxTransmit::update_row(ObNewRow& row, int partition_id_column_idx, ObObj& partition_id) const
{
  int ret = OB_SUCCESS;
  if (projector_size_ > partition_id_column_idx && partition_id_column_idx >= 0) {
    row.get_cell(partition_id_column_idx) = partition_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition id column idx", K(partition_id_column_idx), K(projector_size_), K(*projector_));
  }
  return ret;
}

int ObPxTransmit::next_row(ObExecContext& exec_ctx, const ObNewRow*& row, ObPxTransmitCtx& transmit_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if ((child_op_->is_dml_operator() && !child_op_->is_pdml_operator()) || transmit_ctx.iter_end_) {
    ret = OB_ITER_END;
    transmit_ctx.consume_first_row_ = true;
    LOG_TRACE("transmit iter end", K(ret), K(transmit_ctx.iter_end_));
  } else if (!transmit_ctx.consume_first_row_) {
    row = transmit_ctx.first_row_;
    transmit_ctx.consume_first_row_ = true;
  } else {
    ret = get_next_row(exec_ctx, row);
  }
  return ret;
}

int ObPxTransmit::link_ch_sets(
    ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  for (int64_t idx = 0; idx < ch_set.count() && OB_SUCC(ret); ++idx) {
    dtl::ObDtlChannel* ch = NULL;
    if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
      LOG_WARN("fail get channel info", K(idx), K(ret));
    } else if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      LOG_WARN("fail add qc channel", K(ret));
    } else if (OB_FAIL(channels.push_back(ch))) {
      LOG_WARN("fail push back channel ptr", K(ci), K(ret));
    }
  }
  return ret;
}
