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

#include "ob_px_receive_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql {

OB_SERIALIZE_MEMBER(ObPxReceiveOpInput, ch_provider_ptr_, child_dfo_id_);
OB_SERIALIZE_MEMBER((ObPxFifoReceiveOpInput, ObPxReceiveOpInput));

int ObPxReceiveOpInput::get_dfo_key(ObDtlDfoKey& key)
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

int ObPxReceiveOpInput::get_first_buffer_cache(dtl::ObDtlLocalFirstBufferCache*& first_buffer_cache)
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

int ObPxReceiveOpInput::get_data_ch(ObPxTaskChSet& task_ch_set, int64_t timeout_ts, ObDtlChTotalInfo& ch_info)
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
                 child_dfo_id_, get_sqc_id(), get_task_id(), timeout_ts, task_ch_set, &ch_info))) {
    LOG_WARN("fail get data ch sets from provider", K_(child_dfo_id), K(ret));
  } else if (OB_INVALID_ID == (task_id = get_task_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task id invalid", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObPxFifoReceiveSpec, ObPxReceiveSpec));
OB_SERIALIZE_MEMBER((ObPxReceiveSpec, ObReceiveSpec), child_exprs_);

ObPxReceiveSpec::ObPxReceiveSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObReceiveSpec(alloc, type), child_exprs_(alloc)
{}

//------------- start ObPxReceiveOp-----------------
ObPxReceiveOp::ObPxReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObReceiveOp(exec_ctx, spec, input),
      task_ch_set_(),
      iter_end_(false),
      channel_linked_(false),
      task_channels_(),
      px_row_(),
      px_row_msg_proc_(px_row_),
      msg_loop_(),
      finish_ch_cnt_(0),
      ts_cnt_(0),
      ts_(0),
      proxy_first_buffer_cache_(nullptr),
      ch_info_()
{}

void ObPxReceiveOp::destroy()
{
  ObReceiveOp::destroy();
  task_ch_set_.reset();
  msg_loop_.destroy();
  px_row_msg_proc_.destroy();
  // no need to reset px_row_
  task_channels_.reset();
  ts_cnt_ = 0;
  ts_ = 0;
  dfc_.destroy();
  ch_info_.reset();
}

int ObPxReceiveOp::init_dfc(ObDtlDfoKey& key)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObDtlLocalFirstBufferCache* buf_cache = nullptr;
  if (OB_FAIL(dfc_.init(ctx_.get_my_session()->get_effective_tenant_id(), task_ch_set_.count()))) {
    LOG_WARN("Fail to init dfc", K(ret));
  } else if (OB_FAIL(DTL.get_dfc_server().get_buffer_cache(
                 ctx_.get_my_session()->get_effective_tenant_id(), key, buf_cache))) {
    LOG_WARN("failed to get buffer cache", K(key));
  } else {
    dfc_.set_timeout_ts(plan_ctx->get_timeout_timestamp());
    dfc_.set_receive();
    dfc_.set_dfo_key(key);
    dfc_.set_op_metric(&metric_);
    dfc_.set_first_buffer_cache(buf_cache);
    dfc_.set_dtl_channel_watcher(&msg_loop_);
    if (0 == ch_info_.start_channel_id_) {
      dfc_.set_total_ch_info(nullptr);
    } else {
      dfc_.set_total_ch_info(&ch_info_);
    }
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

int64_t ObPxReceiveOp::get_sqc_id()
{
  ObPxExchangeOpInput* recv_input = reinterpret_cast<ObPxExchangeOpInput*>(input_);
  return recv_input->get_sqc_id();
}

int ObPxReceiveOp::init_channel(ObPxReceiveOpInput& recv_input, ObPxTaskChSet& task_ch_set,
    common::ObIArray<dtl::ObDtlChannel*>& task_channels, dtl::ObDtlChannelLoop& loop, ObPxReceiveRowP& px_row_msg_proc,
    ObPxInterruptP& interrupt_proc)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObDtlDfoKey key;
  LOG_TRACE("Try to get channel infomation from SQC");
  if (OB_FAIL(recv_input.get_data_ch(task_ch_set, phy_plan_ctx->get_timeout_timestamp(), ch_info_))) {
    LOG_WARN("Fail to get data dtl channel", K(ret));
  } else if (OB_FAIL(recv_input.get_dfo_key(key))) {
    LOG_WARN("Failed to get dfo key", K(ret));
  } else if (OB_FAIL(recv_input.get_first_buffer_cache(proxy_first_buffer_cache_))) {
    LOG_WARN("Failed to get first buffer cache", K(ret));
  } else if (OB_FAIL(init_dfc(key))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(link_ch_sets(task_ch_set, task_channels, &dfc_))) {
    LOG_WARN("Fail to link data channel", K(ret));
  } else {
    bool enable_audit = GCONF.enable_sql_audit && ctx_.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    common::ObIArray<dtl::ObDtlChannel*>& channels = task_channels;
    loop.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    loop.set_first_buffer_cache(proxy_first_buffer_cache_);
    loop.register_processor(px_row_msg_proc).register_interrupt_processor(interrupt_proc);
    loop.set_monitor_info(&op_monitor_info_);
    ObPxSQCProxy* ch_provider = reinterpret_cast<ObPxSQCProxy*>(recv_input.get_ch_provider());
    const bool use_interm_result = ch_provider->get_recieve_use_interm_result();
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

int ObPxReceiveOp::link_ch_sets(
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

int ObPxReceiveOp::rescan()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("the receive operator is not supported now. the plan may be wrong", K(ret), K(lbt()));
  return ret;
}

int ObPxReceiveOp::drain_exch()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  ObPxReceiveOpInput* recv_input = NULL;
  uint64_t version = -1;
  if (OB_FAIL(try_open())) {
    LOG_WARN("get operator ctx failed", K(ret));
  } else if (exch_drained_) {
    // has been drained, do noting.
  } else if (iter_end_) {
    exch_drained_ = true;
  } else if (!exch_drained_) {
    if ((version = GET_MIN_CLUSTER_VERSION()) < CLUSTER_VERSION_2200) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: cluster version", K(ret), K(GET_MIN_CLUSTER_VERSION()));
    } else {
      if (IS_PX_COORD(get_spec().get_type())) {
        /** for plan like this
         *               merge join
         *          QC1              QC2
         * if QC1 iter end, QC2 will end soon, wo do not need to process since all data is return
         */
        LOG_TRACE("drain QC");
      } else if (OB_FAIL(try_link_channel())) {
        LOG_WARN("failed to link channel", K(ret));
      } else if (OB_FAIL(active_all_receive_channel())) {
        LOG_WARN("failed to active all receive channel", K(ret));
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("drain px receive", K(get_spec().id_), K(ret), K(lbt()));
        dfc_.drain_all_channels();
        exch_drained_ = true;
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPxReceiveOp::active_all_receive_channel()
{
  int ret = OB_SUCCESS;
  const int batch = 10;
  while (!dfc_.is_all_channel_act() && OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
  }
  return ret;
}
//------------- end ObPxReceiveOp-----------------

ObPxFifoReceiveOp::ObPxFifoReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObPxReceiveOp(exec_ctx, spec, input), interrupt_proc_()
{}

int ObPxFifoReceiveOp::inner_open()
{
  return ObPxReceiveOp::inner_open();
}

int ObPxFifoReceiveOp::inner_close()
{
  int ret = OB_SUCCESS;
  /* we must release channel even if there is some error happen before */
  if (channel_linked_) {
    int release_channel_ret = ObPxChannelUtil::flush_rows(task_channels_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
    ObDTLIntermResultKey key;
    ObDtlBasicChannel* channel = NULL;
    for (int i = 0; i < task_channels_.count(); ++i) {
      channel = static_cast<ObDtlBasicChannel*>(task_channels_.at(i));
      key.channel_id_ = channel->get_id();
      if (channel->use_interm_result()) {
        release_channel_ret = ObDTLIntermResultManager::getInstance().erase_interm_result_info(key);
        if (release_channel_ret != common::OB_SUCCESS) {
          LOG_WARN("fail to release recieve internal result", K(ret));
        }
      }
    }

    release_channel_ret = msg_loop_.unregister_all_channel();
    if (release_channel_ret != common::OB_SUCCESS) {
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
    }
    release_channel_ret = ObPxChannelUtil::unlink_ch_set(get_ch_set(), &dfc_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
  }
  return ret;
}

int ObPxFifoReceiveOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(try_link_channel())) {
    LOG_WARN("failed to init channel", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (finish_ch_cnt_ == task_channels_.count()) {
      ret = OB_ITER_END;
      LOG_TRACE("All data received", "finish_cnt", finish_ch_cnt_, "chan_cnt", task_channels_.count(), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t timeout_ts = phy_plan_ctx->get_timeout_timestamp();
    int64_t retry_cnt = 0;
    do {
      clear_evaluated_flag();
      ret = get_one_row_from_channels(timeout_ts - get_timestamp());
      if (OB_SUCCESS == ret) {
        metric_.mark_first_out();
        LOG_DEBUG("Got one row from channel", K(ret));
        break;  // got one row
      } else if (OB_ITER_END == ret) {
        metric_.mark_last_out();
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
  if (OB_ITER_END == ret) {
    iter_end_ = true;
    LOG_TRACE("receive eof row", K(get_spec().id_), K(ret));
  }
  return ret;
}

int ObPxFifoReceiveOp::try_link_channel()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (!channel_linked_) {
    ObPxReceiveOpInput* recv_input = reinterpret_cast<ObPxReceiveOpInput*>(input_);
    ret = init_channel(*recv_input, task_ch_set_, task_channels_, msg_loop_, px_row_msg_proc_, interrupt_proc_);
    if (OB_FAIL(ret)) {
      LOG_WARN("Fail to init channel", K(ret));
    } else {
      metric_.set_id(get_spec().id_);
    }
  }
  return ret;
}

int ObPxFifoReceiveOp::get_one_row_from_channels(int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (!got_row && OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(msg_loop_.process_one(timeout_us))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail pop sqc execution result from channel", K(ret));
      }
    } else {
      ret = px_row_.get_next_row((static_cast<const ObPxReceiveSpec*>(&get_spec()))->child_exprs_, eval_ctx_);
      if (OB_ITER_END == ret) {
        finish_ch_cnt_++;
        if (finish_ch_cnt_ < task_channels_.count()) {
          ret = OB_SUCCESS;  // still has more channels to receive data
        } else {
          LOG_TRACE("All channel finish", "finish_ch_cnt", finish_ch_cnt_, K(ret));
        }
      } else if (OB_SUCCESS == ret) {
        got_row = true;
        metric_.count();
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

}  // end namespace sql
}  // end namespace oceanbase
