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

#include "ob_px_receive.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/px/ob_px_scheduler.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER(ObPxReceiveInput, ch_provider_ptr_, child_dfo_id_);
OB_SERIALIZE_MEMBER((ObPxFifoReceiveInput, ObPxReceiveInput));

int ObPxReceiveInput::get_dfo_key(ObDtlDfoKey& key)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else {
    ch_provider->get_self_dfo_key(key);
    if (!ObDfo::is_valid_dfo_id(key.get_dfo_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(key.get_dfo_id()));
    }
  }
  return ret;
}

int ObPxReceiveInput::get_first_buffer_cache(dtl::ObDtlLocalFirstBufferCache*& first_buffer_cache)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy* sqc_proxy = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  if (OB_ISNULL(sqc_proxy)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else {
    first_buffer_cache = sqc_proxy->get_first_buffer_cache();
    LOG_TRACE("trace first buffer cache", K(first_buffer_cache));
  }
  return ret;
}

int ObPxReceiveInput::get_data_ch(ObPxTaskChSet& task_ch_set, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(ch_provider_ptr_);
  int64_t task_id = OB_INVALID_ID;
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else if (OB_INVALID_ID == child_dfo_id_) {
    ret = OB_NOT_INIT;
    LOG_WARN("child dfo id not init", K(ret));
  } else if (child_dfo_id_ < 0 || child_dfo_id_ >= ObDfo::MAX_DFO_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("child dfo id bad init value", K_(child_dfo_id), K(ret));
  } else if (OB_FAIL(ch_provider->get_receive_data_ch(
                 child_dfo_id_, get_sqc_id(), get_task_id(), timeout_ts, task_ch_set, nullptr))) {
    LOG_WARN("fail get data ch sets from provider", K_(child_dfo_id), K(ret));
  } else if (OB_INVALID_ID == (task_id = get_task_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task id invalid", K(ret));
  }
  return ret;
}

ObPxReceive::ObPxReceive(common::ObIAllocator& alloc) : ObReceive(alloc)
{}

ObPxReceive::~ObPxReceive()
{}

int ObPxReceive::ObPxReceiveCtx::init_dfc(ObExecContext& ctx, ObDtlDfoKey& key)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObDtlLocalFirstBufferCache* buf_cache = nullptr;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The phy plan ctx is null", K(ret));
  } else if (OB_FAIL(dfc_.init(ctx.get_my_session()->get_effective_tenant_id(), task_ch_set_.count()))) {
    LOG_WARN("Fail to init dfc", K(ret));
  } else if (OB_FAIL(DTL.get_dfc_server().get_buffer_cache(
                 ctx.get_my_session()->get_effective_tenant_id(), key, buf_cache))) {
    LOG_WARN("failed to get buffer cache", K(key));
  } else {
    dfc_.set_timeout_ts(phy_plan_ctx->get_timeout_timestamp());
    dfc_.set_receive();
    dfc_.set_dfo_key(key);
    dfc_.set_op_metric(&metric_);
    dfc_.set_first_buffer_cache(buf_cache);
    dfc_.set_dtl_channel_watcher(&msg_loop_);
    DTL.get_dfc_server().register_dfc(dfc_);
    bool force_block = false;
#ifdef ERRSIM
    int ret = OB_SUCCESS;
    ret = E(EventTable::EN_FORCE_DFC_BLOCK) ret;
    force_block = (OB_HASH_NOT_EXIST == ret);
    LOG_TRACE("Worker init dfc", K(key), K(dfc_.is_receive()), K(force_block), K(ret));
    ret = OB_SUCCESS;
#endif
    LOG_TRACE("Worker init dfc", K(key), K(dfc_.is_receive()), K(force_block));
  }
  return ret;
}

int ObPxReceive::ObPxReceiveCtx::init_channel(ObExecContext& ctx, ObPxReceiveInput& recv_input,
    ObPxTaskChSet& task_ch_set, common::ObIArray<dtl::ObDtlChannel*>& task_channels, dtl::ObDtlChannelLoop& loop,
    ObPxReceiveRowP& px_row_msg_proc, ObPxInterruptP& interrupt_proc)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObDtlDfoKey key;

  LOG_TRACE("Try to get channel infomation from SQC");

  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The phy plan ctx is null", K(ret));
  } else if (OB_FAIL(recv_input.get_data_ch(task_ch_set, phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("Fail to get data dtl channel", K(ret));
  } else if (OB_FAIL(recv_input.get_dfo_key(key))) {
    LOG_WARN("Failed to get dfo key", K(ret));
  } else if (OB_FAIL(recv_input.get_first_buffer_cache(proxy_first_buffer_cache_))) {
    LOG_WARN("Failed to get first buffer cache", K(ret));
  } else if (OB_FAIL(init_dfc(ctx, key))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(ObPxReceive::link_ch_sets(task_ch_set, task_channels, &dfc_))) {
    LOG_WARN("Fail to link data channel", K(ret));
  } else {
    bool enable_audit = GCONF.enable_sql_audit && ctx.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    bool use_interm_result = false;
    common::ObIArray<dtl::ObDtlChannel*>& channels = task_channels;
    loop.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    loop.set_first_buffer_cache(proxy_first_buffer_cache_);
    loop.register_processor(px_row_msg_proc).register_interrupt_processor(interrupt_proc);
    loop.set_monitor_info(&op_monitor_info_);
    if (OB_SUCC(ret)) {
      ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(recv_input.get_ch_provider());
      use_interm_result = ch_provider->get_recieve_use_interm_result();
    }
    loop.set_interm_result(use_interm_result);
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
      LOG_TRACE("Receive channel", KP(ch->get_id()), K(ch->get_peer()));
    }
    LOG_TRACE("Get receive channel ok", "task_id", recv_input.get_task_id(), "ch_cnt", task_channels.count(), K(ret));
  }
  // No matter success or not
  channel_linked_ = true;
  return ret;
}

int ObPxReceive::link_ch_sets(
    ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, dtl::ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  // do link ch_set
  dtl::ObDtlChannelInfo ci;

  for (int64_t idx = 0; idx < ch_set.count() && OB_SUCC(ret); ++idx) {
    dtl::ObDtlChannel* ch = NULL;
    if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
      LOG_WARN("fail get channel info", K(idx), K(ret));
    } else if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail add qc channel", K(ci), K(ret));
    } else if (OB_FAIL(channels.push_back(ch))) {
      LOG_WARN("fail push back channel ptr", K(ci), K(ret));
    } else {
      LOG_TRACE("link receive-transmit ch", K(*ch), K(idx), K(ci));
    }
  }
  LOG_TRACE("Data ch set all linked and ready to add to msg loop", "count", ch_set.count(), K(ret));
  return ret;
}

int ObPxReceive::rescan(ObExecContext& ctx) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  LOG_WARN("the receive operator is not supported now. the plan may be wrong", K(ret), K(lbt()));
  return ret;
}

int ObPxReceive::get_sqc_id(ObExecContext& ctx, int64_t& sqc_id) const
{
  int ret = OB_SUCCESS;
  ObPxReceiveInput* recv_input = NULL;
  sqc_id = OB_INVALID_ID;
  if (OB_ISNULL(recv_input = GET_PHY_OP_INPUT(ObPxReceiveInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Fail to get op input", K(ret), "op_id", get_id(), "op_type", get_type());
  } else {
    sqc_id = recv_input->get_sqc_id();
  }
  return ret;
}

int ObPxReceive::drain_exch(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObPxReceiveCtx* recv_ctx = NULL;
  ObPxReceiveInput* recv_input = NULL;
  uint64_t version = -1;
  if (OB_FAIL(try_open_and_get_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("get operator ctx failed", K(ret));
  } else if (OB_ISNULL(recv_ctx = static_cast<ObPxReceiveCtx*>(op_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get recv ctx", K(ret));
  } else if (op_ctx->exch_drained_) {
    // has been drained, do noting.
  } else if (recv_ctx->iter_end_) {
    recv_ctx->exch_drained_ = true;
  } else if (!op_ctx->exch_drained_) {
    if ((version = GET_MIN_CLUSTER_VERSION()) < CLUSTER_VERSION_2200) {
      int64_t row_cnt = 0;
      while (OB_SUCCESS == (tmp_ret = get_next_row(ctx, row))) {
        row_cnt++;
      }
      LOG_TRACE("drain px receive", K(tmp_ret), K(row_cnt));
      if (OB_ITER_END != tmp_ret) {
        LOG_WARN("get next row failed while draining data, ignore", K(tmp_ret));
      }
    } else {
      if (IS_PX_COORD(get_type())) {
        LOG_TRACE("drain QC");
      } else if (OB_FAIL(try_link_channel(ctx))) {
        LOG_WARN("failed to link channel", K(ret));
      } else if (OB_FAIL(active_all_receive_channel(*recv_ctx, ctx))) {
        LOG_WARN("failed to active all receive channel", K(ret));
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("drain px receive", K(get_id()), K(ret));
        recv_ctx->dfc_.drain_all_channels();
        op_ctx->exch_drained_ = true;
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPxReceive::active_all_receive_channel(ObPxReceiveCtx& recv_ctx, ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const int batch = 10;
  while (!recv_ctx.dfc_.is_all_channel_act() && OB_SUCC(ret)) {
    const ObNewRow* row = nullptr;
    if (OB_FAIL(inner_get_next_row(ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
  }
  return ret;
}

//////////////////////////////////////////

ObPxFifoReceive::ObPxFifoReceive(common::ObIAllocator& alloc) : ObPxReceive(alloc)
{}

ObPxFifoReceive::~ObPxFifoReceive()
{}

ObPxFifoReceive::ObPxFifoReceiveCtx::ObPxFifoReceiveCtx(ObExecContext& ctx)
    : ObPxReceive::ObPxReceiveCtx(ctx), interrupt_proc_()
{}

ObPxFifoReceive::ObPxFifoReceiveCtx::~ObPxFifoReceiveCtx()
{}

int ObPxFifoReceive::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxFifoReceiveCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail create op ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("fail init px fifo receive cur row", K(ret));
  }
  return ret;
}

int ObPxFifoReceive::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;

  LOG_TRACE("Inner open px fifo receive", "op_id", get_id());

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_FAIL(ObPxReceive::inner_open(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else {
  }

  LOG_TRACE("Inner open px fifo receive ok", "op_id", get_id());

  return ret;
}

int ObPxFifoReceive::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  LOG_TRACE("Inner_close px fifo receive");
  ObPxFifoReceiveCtx* recv_ctx = NULL;

  if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxFifoReceiveCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  }

  /* we must release channel even if there is some error happen before */
  if (OB_NOT_NULL(recv_ctx)) {

    int release_channel_ret = ObPxChannelUtil::flush_rows(recv_ctx->task_channels_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
    ObDTLIntermResultKey key;
    ObDtlBasicChannel* channel = NULL;
    ;
    for (int i = 0; i < recv_ctx->task_channels_.count(); ++i) {
      channel = static_cast<ObDtlBasicChannel*>(recv_ctx->task_channels_.at(i));
      key.channel_id_ = channel->get_id();
      if (channel->use_interm_result()) {
        release_channel_ret = ObDTLIntermResultManager::getInstance().erase_interm_result_info(key);
        if (release_channel_ret != common::OB_SUCCESS) {
          LOG_WARN("fail to release recieve internal result", KR(release_channel_ret), K(ret));
        }
      }
    }

    dtl::ObDtlChannelLoop& loop = recv_ctx->msg_loop_;
    release_channel_ret = loop.unregister_all_channel();
    if (release_channel_ret != common::OB_SUCCESS) {
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
    }
    release_channel_ret = ObPxChannelUtil::unlink_ch_set(recv_ctx->get_ch_set(), &recv_ctx->dfc_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", KR(release_channel_ret));
    }
  }
  return ret;
}

int ObPxFifoReceive::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPxFifoReceiveCtx* recv_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx);
  if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxFifoReceiveCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the phy plan ctx is null", K(ret));
  } else if (OB_FAIL(try_link_channel(ctx))) {
    LOG_WARN("failed to init channel", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (recv_ctx->finish_ch_cnt_ == recv_ctx->task_channels_.count()) {
      ret = OB_ITER_END;
      LOG_TRACE("All data received",
          "finish_cnt",
          recv_ctx->finish_ch_cnt_,
          "chan_cnt",
          recv_ctx->task_channels_.count(),
          K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    dtl::ObDtlChannelLoop& loop = recv_ctx->msg_loop_;
    int64_t timeout_ts = phy_plan_ctx->get_timeout_timestamp();
    int64_t retry_cnt = 0;
    do {
      ret = get_one_row_from_channels(*recv_ctx, loop, timeout_ts - recv_ctx->get_timestamp(), row);
      if (OB_SUCCESS == ret) {
        recv_ctx->metric_.mark_first_out();
        LOG_DEBUG("Got one row from channel", K(ret));
        break;  // got one row
      } else if (OB_ITER_END == ret) {
        recv_ctx->metric_.mark_last_out();
        LOG_TRACE("Got eof row from channel", K(ret));
        break;
      } else if (OB_EAGAIN == ret) {
        // no data for now, wait and try again
        if (ObTimeUtility::current_time() >= timeout_ts) {
          ret = OB_TIMEOUT;
          LOG_WARN("get row from channel timeout", K(ret));
        } else {
          usleep(1 * 1000);
          int tmp_ret = THIS_WORKER.check_status();
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("wait to receive row interrupted", K(tmp_ret), K(ret));
            ret = tmp_ret;
            break;
          }
          if (0 == retry_cnt % 100) {
            LOG_DEBUG("Wait for next row", K(tmp_ret), K(ret));
          }
          retry_cnt++;
        }
      } else {
        LOG_WARN("fail get row from channels", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  if (OB_NOT_NULL(row)) {
    LOG_DEBUG("receive row", K(get_id()), K(ret), K(*row));
  } else if (OB_ITER_END == ret) {
    recv_ctx->iter_end_ = true;
    LOG_TRACE("receive eof row", K(get_id()), K(ret));
  }
  return ret;
}

int ObPxFifoReceive::try_link_channel(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxFifoReceiveCtx* recv_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx);
  if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxFifoReceiveCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the phy plan ctx is null", K(ret));
  } else if (!recv_ctx->channel_linked()) {
    ObPxReceiveInput* recv_input = NULL;
    if (OB_ISNULL(recv_input = GET_PHY_OP_INPUT(ObPxReceiveInput, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Fail to get op input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      ret = recv_ctx->init_channel(ctx,
          *recv_input,
          recv_ctx->task_ch_set_,
          recv_ctx->task_channels_,
          recv_ctx->msg_loop_,
          recv_ctx->px_row_msg_proc_,
          recv_ctx->interrupt_proc_);
      if (OB_FAIL(ret)) {
        LOG_WARN("Fail to init channel", K(ret));
      } else {
        recv_ctx->metric_.set_id(get_id());
      }
    }
  }
  return ret;
}

int ObPxFifoReceive::get_one_row_from_channels(
    ObPxFifoReceiveCtx& recv_ctx, dtl::ObDtlChannelLoop& loop, int64_t timeout_us, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // receive one row from  N channels, save it to px_row_
  bool got_row = false;
  while (!got_row && OB_SUCC(ret)) {
    if (OB_FAIL(loop.process_one(timeout_us))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail pop sqc execution result from channel", K(ret));
      }
    } else {
      ret = recv_ctx.px_row_.get_row(recv_ctx.get_cur_row());
      if (OB_ITER_END == ret) {
        recv_ctx.finish_ch_cnt_++;
        if (recv_ctx.finish_ch_cnt_ < recv_ctx.task_channels_.count()) {
          ret = OB_SUCCESS;  // still has more channels to receive data
        } else {
          LOG_TRACE("All channel finish", "finish_ch_cnt", recv_ctx.finish_ch_cnt_, K(ret));
        }
      } else if (OB_SUCCESS == ret) {
        got_row = true;
        row = &recv_ctx.get_cur_row();
        recv_ctx.metric_.count();
      } else {
        LOG_WARN("fail get row from row store", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObPxFifoReceive::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxFifoReceiveInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  UNUSED(input);
  return ret;
}
