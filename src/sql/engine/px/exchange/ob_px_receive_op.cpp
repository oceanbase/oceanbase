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
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_share.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/px/exchange/ob_px_ms_receive_op.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace sql;
using namespace sql::dtl;
namespace sql
{

OB_SERIALIZE_MEMBER(ObPxReceiveOpInput, ch_provider_ptr_, child_dfo_id_, ignore_vtable_error_);
OB_SERIALIZE_MEMBER((ObPxFifoReceiveOpInput, ObPxReceiveOpInput));

int ObPxReceiveOpInput::get_dfo_key(ObDtlDfoKey &key)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(ch_provider_ptr_);
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

int ObPxReceiveOpInput::get_data_ch(ObPxTaskChSet &task_ch_set, int64_t timeout_ts, ObDtlChTotalInfo &ch_info)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(ch_provider_ptr_);
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
OB_SERIALIZE_MEMBER((ObPxReceiveSpec, ObReceiveSpec),
                    child_exprs_,
                    repartition_table_id_,
                    dynamic_const_exprs_,
                    bloom_filter_id_array_);

ObPxReceiveSpec::ObPxReceiveSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObReceiveSpec(alloc, type),
    child_exprs_(alloc),
    repartition_table_id_(OB_INVALID_ID),
    dynamic_const_exprs_(alloc),
    bloom_filter_id_array_(alloc)
{
  repartition_table_id_ = 0;
}

//------------- start ObPxReceiveOp-----------------
ObPxReceiveOp::ObPxReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObReceiveOp(exec_ctx, spec, input),
    task_ch_set_(),
    iter_end_(false),
    channel_linked_(false),
    task_channels_(),
    row_reader_(get_spec().id_),
    px_row_msg_proc_(&row_reader_),
    msg_loop_(op_monitor_info_),
    ts_cnt_(0),
    ts_(0),
    ch_info_(),
    stored_rows_(NULL),
    vector_rows_(nullptr),
    bf_rpc_proxy_(),
    bf_ctx_idx_(0),
    bf_send_idx_(0),
    each_group_size_(0)
{}

int ObPxReceiveOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("operator inner open failed", K(ret));
  } else {
    if (is_vectorized()) {
      stored_rows_ = static_cast<const ObChunkDatumStore::StoredRow **>(
          ctx_.get_allocator().alloc(spec_.max_batch_size_ * sizeof(*stored_rows_)));
      if (NULL == stored_rows_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc stored rows pointer failed", K(ret));
      }
      if (OB_SUCC(ret) && get_spec().use_rich_format_) {
        vector_rows_ = static_cast<const ObCompactRow **>(
          ctx_.get_allocator().alloc(spec_.max_batch_size_ * sizeof(*vector_rows_)));
        if (NULL == vector_rows_) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc vector rows pointer failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_obrpc_proxy(bf_rpc_proxy_))) {
        LOG_WARN("fail init rpc proxy", K(ret));
      }
    }
  }
  return ret;
}


void ObPxReceiveOp::destroy()
{
  ObReceiveOp::destroy();
  row_reader_.reset();
  task_ch_set_.reset();
  msg_loop_.destroy();
  px_row_msg_proc_.destroy();
  //no need to reset px_row_
  task_channels_.reset();
  ts_cnt_ = 0;
  ts_ = 0;
  dfc_.destroy();
  ch_info_.reset();
}

// 必须在channel set_dfc之前init好dfc信息，否则channel使用的dfc信息是不完整的
int ObPxReceiveOp::init_dfc(ObDtlDfoKey &key)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(dfc_.init(ctx_.get_my_session()->get_effective_tenant_id(),
                        task_ch_set_.count()))) {
    LOG_WARN("Fail to init dfc", K(ret));
  } else {
    dfc_.set_timeout_ts(plan_ctx->get_timeout_timestamp());
    dfc_.set_receive();
    dfc_.set_dfo_key(key);
    dfc_.set_op_metric(&metric_);
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
    ret = OB_E(EventTable::EN_FORCE_DFC_BLOCK) ret;
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
  ObPxExchangeOpInput *recv_input = reinterpret_cast<ObPxExchangeOpInput*>(input_);
  return recv_input->get_sqc_id();
}

int ObPxReceiveOp::init_channel(
  ObPxReceiveOpInput &recv_input,
  ObPxTaskChSet &task_ch_set,
  common::ObIArray<dtl::ObDtlChannel *> &task_channels,
  dtl::ObDtlChannelLoop &loop,
  ObPxReceiveRowP &px_row_msg_proc,
  ObPxInterruptP &interrupt_proc)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObDtlDfoKey key;
  LOG_TRACE("Try to get channel infomation from SQC");
  CK (OB_NOT_NULL(ctx_.get_physical_plan_ctx()) && OB_NOT_NULL(ctx_.get_physical_plan_ctx()->get_phy_plan()));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(recv_input.get_data_ch(task_ch_set, phy_plan_ctx->get_timeout_timestamp(), ch_info_))) {
    LOG_WARN("Fail to get data dtl channel", K(ret));
  } else if (OB_FAIL(recv_input.get_dfo_key(key))) {
    LOG_WARN("Failed to get dfo key", K(ret));
  } else if (OB_FAIL(init_dfc(key))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(link_ch_sets(task_ch_set, task_channels, &dfc_))) {
    LOG_WARN("Fail to link data channel", K(ret));
  } else {
    bool enable_audit = GCONF.enable_sql_audit
                      && ctx_.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    common::ObIArray<dtl::ObDtlChannel*> &channels = task_channels;
    loop.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    loop.register_processor(px_row_msg_proc)
        .register_interrupt_processor(interrupt_proc);
    loop.set_process_query_time(ctx_.get_my_session()->get_process_query_time());
    loop.set_query_timeout_ts(ctx_.get_physical_plan_ctx()->get_timeout_timestamp());
    ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(recv_input.get_ch_provider());
    int64_t batch_id = ctx_.get_px_batch_id();
    const bool use_interm_result = ch_provider->get_recieve_use_interm_result();
    loop.set_interm_result(use_interm_result);
    int64_t thread_id = GETTID();
    ARRAY_FOREACH_X(channels, idx, cnt, OB_SUCC(ret)) {
      dtl::ObDtlChannel *ch = channels.at(idx);
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ch), K(ret));
      } else {
        ch->set_audit(enable_audit);
        ch->set_interm_result(use_interm_result);
        ch->set_enable_channel_sync(true);
        ch->set_ignore_error(recv_input.is_ignore_vtable_error());
        ch->set_batch_id(batch_id);
        ch->set_operator_owner();
        ch->set_thread_id(thread_id);
      }
      LOG_TRACE("Receive channel",KP(ch->get_id()), K(ch->get_peer()));
    }
    LOG_TRACE("Get receive channel ok",
              "task_id", recv_input.get_task_id(),
              "ch_cnt", task_channels.count(),
              K(ret));
  }
  // No matter success or not
  channel_linked_ = true;
  return ret;
}

int ObPxReceiveOp::link_ch_sets(ObPxTaskChSet &ch_set,
                              common::ObIArray<dtl::ObDtlChannel*> &channels,
                              dtl::ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  // do link ch_set
  dtl::ObDtlChannelInfo ci;
  int64_t hash_val = 0;
  int64_t offset = 0;
  const int64_t DTL_CHANNEL_SIZE = sizeof(ObDtlRpcChannel) > sizeof(ObDtlLocalChannel) ? sizeof(ObDtlRpcChannel) : sizeof(ObDtlLocalChannel);
  CK (OB_NOT_NULL(ctx_.get_physical_plan_ctx()) && OB_NOT_NULL(ctx_.get_physical_plan_ctx()->get_phy_plan()));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(channels.reserve(ch_set.count()))) {
    LOG_WARN("fail reserve channels", K(ret), K(ch_set.count()));
  } else if (OB_FAIL(dfc->reserve(ch_set.count()))) {
    LOG_WARN("fail reserve dfc channels", K(ret), K(ch_set.count()));
  } else if (ch_set.count() > 0) {
    ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(), "SqlDtlRecvChan");
    void *buf = oceanbase::common::ob_malloc(DTL_CHANNEL_SIZE * ch_set.count(), attr);
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("malloc channel buf failed", K(ret));
    } else {
      uint16_t seed[3] = {0, 0, 0};
      int64_t time = ObTimeUtility::current_time();
      if (0 == seed[0] && 0 == seed[1] && 0 == seed[2]) {
        seed[0] = static_cast<uint16_t>(GETTID());
        seed[1] = static_cast<uint16_t>(time & 0x0000FFFF);
        seed[2] = static_cast<uint16_t>((time & 0xFFFF0000) >> 16);
        seed48(seed);
      }
      bool failed_in_push_back_to_channels = false;
      for (int64_t idx = 0; idx < ch_set.count() && OB_SUCC(ret); ++idx) {
        dtl::ObDtlChannel *ch = NULL;
        hash_val = jrand48(seed);
        if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
          LOG_WARN("fail get channel info", K(idx), K(ret));
        } else if (nullptr != dfc && ci.type_ == DTL_CT_LOCAL) {
          ch = new((char*)buf + offset) ObDtlLocalChannel(ci.tenant_id_, ci.chid_, ci.peer_, hash_val, dtl::ObDtlChannel::DtlChannelType::LOCAL_CHANNEL);
        } else {
          ch = new((char*)buf + offset) ObDtlRpcChannel(ci.tenant_id_, ci.chid_, ci.peer_, hash_val, dtl::ObDtlChannel::DtlChannelType::RPC_CHANNEL);
        }
        if (OB_FAIL(ret)) {
        } else if (nullptr == ch) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("create channel fail", K(ret), K(ci.tenant_id_), K(ci.chid_));
        } else if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
          LOG_WARN("fail link channel", K(ci), K(ret));
        } else if (OB_ISNULL(ch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail add qc channel", K(ci), K(ret));
        } else if (OB_FAIL(channels.push_back(ch))) {
          failed_in_push_back_to_channels = true;
          LOG_WARN("fail push back channel ptr", K(ci), K(ret));
        } else {
          offset += DTL_CHANNEL_SIZE;
          LOG_TRACE("link receive-transmit ch", K(*ch), K(idx), K(ci));
        }
      }
      if (0 == channels.count() && !failed_in_push_back_to_channels) {
        ob_free(buf);
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(send_channel_ready_msg(reinterpret_cast<ObPxReceiveOpInput*>(input_)->get_child_dfo_id()))) {
    LOG_WARN("failed to send channel ready msg", K(ret));
  }
  LOG_TRACE("Data ch set all linked and ready to add to msg loop",
            "count", ch_set.count(), K(ret));
  return ret;
}

int ObPxReceiveOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to do rescan", K(ret));
  } else {
    ObDtlChannel *channel = NULL;
    iter_end_ = false;
    row_reader_.reset();
    msg_loop_.reset_eof_cnt();
    ts_cnt_ = 0;
    ObDTLIntermResultKey key;
    int release_channel_ret = 0;
    for (int i = 0; i < task_channels_.count(); ++i) {
      channel = task_channels_.at(i);
      key.channel_id_ = channel->get_id();
      key.batch_id_ = channel->get_batch_id();
      channel->set_channel_is_eof(false);
      channel->reset_state();
      channel->set_batch_id(ctx_.get_px_batch_id());
      channel->reset_px_row_iterator();
      release_channel_ret = MTL(ObDTLIntermResultManager*)->erase_interm_result_info(key);
      if (release_channel_ret != common::OB_SUCCESS) {
        LOG_WARN("fail to release recieve internal result", KR(release_channel_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObPxReceiveOp::inner_drain_exch()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    exch_drained_ = true;
  } else if (!exch_drained_) {
    if (OB_FAIL(try_link_channel())) {
      LOG_WARN("failed to link channel", K(ret));
    } else if (OB_FAIL(active_all_receive_channel())) {
      LOG_WARN("failed to active all receive channel", K(ret));
    }
    LOG_TRACE("drain px receive", K(get_spec().id_), K(ret), K(lbt()));
    dfc_.drain_all_channels();
    exch_drained_ = true;
    if (OB_ITER_END == ret) {
      /**
       * active_all_receive_channel有拿行的操作，可能会产生OB_ITER_END，
       * 所以这里错误码是OB_ITER_END，我们已经达到了drain的目的，将错误码
       * 设置为成功即可。
       */
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPxReceiveOp::active_all_receive_channel()
{
  int ret = OB_SUCCESS;
  while (!dfc_.is_all_channel_act() && OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObPxReceiveOp::get_bf_ctx(int64_t idx, ObJoinFilterDataCtx &bf_ctx)
{
  int ret = OB_SUCCESS;
  const ObPxReceiveSpec &spec = static_cast<const ObPxReceiveSpec &>(spec_);
  if (idx < 0 || idx >= spec.bloom_filter_id_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    int64_t filter_id = spec.bloom_filter_id_array_.at(idx); // get filter id by idx
    int64_t i = 0;
    oceanbase::common::ObIArray<oceanbase::sql::ObJoinFilterDataCtx> &bf_ctx_array = ctx_.get_bloom_filter_ctx_array();
    const int64_t bf_ctx_count = bf_ctx_array.count();
    while (i < bf_ctx_count) {
      if (filter_id == bf_ctx_array.at(i).filter_data_->filter_id_) { // find bloom filter ctx in exec context by filter id
        bf_ctx = bf_ctx_array.at(i);
        break;
      }
      ++i;
    }
    if (i == bf_ctx_count) {
      LOG_DEBUG("can not find bloom filter ctx in exec context", K(ret), K(idx), K(filter_id));
    }
  }
  return ret;
}

int ObPxReceiveOp::wrap_get_next_batch(const int64_t max_row_cnt)
{
  const int64_t max_cnt = std::min(max_row_cnt, spec_.max_batch_size_);
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(max_cnt);
  const ObIArray<ObExpr *> *all_exprs = nullptr;
  for (; idx < max_cnt && OB_SUCC(ret); idx++) {
    batch_info_guard.set_batch_idx(idx);
    if (OB_FAIL(inner_get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get one row failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      break;
    } else {
      // deep copy
      all_exprs = (static_cast<const ObPxReceiveSpec &>(get_spec())).get_all_exprs();
      if (NULL != all_exprs) {
        for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs->count(); i++) {
          ObExpr *e = all_exprs->at(i);
          if (OB_LIKELY(e->is_batch_result())) {
            ObDatum *datum = NULL;
            if (OB_FAIL(e->eval(eval_ctx_, datum))) {
              LOG_WARN("expr eval failed", K(ret));
            } else if (!datum->is_null()) {
              if (datum->len_ <= e->res_buf_len_) {
                char *ptr = eval_ctx_.frames_[e->frame_idx_] + e->res_buf_off_
                    + idx * e->res_buf_len_;
                MEMMOVE(ptr, datum->ptr_, datum->len_);
                datum->ptr_ = ptr;
              } else {
                char *ptr = e->get_str_res_mem(eval_ctx_, datum->len_, idx);
                if (OB_UNLIKELY(NULL == ptr)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("allocate memory failed", K(ret), K(datum->len_));
                } else {
                  MEMMOVE(ptr, datum->ptr_, datum->len_);
                  datum->ptr_ = ptr;
                }
              }
            } else {
              // do nothing
            }
          }
        } // for end
      }
    }
  }
  if (OB_SUCC(ret)) {
    brs_.size_ = idx;
    brs_.end_ = idx < max_cnt;
    // set project flag to prevent duplcated expression calculation
    if (NULL != all_exprs) {
      FOREACH_CNT(e, *(all_exprs)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = 1;
      }
    }
  }
  return ret;
}

int ObPxReceiveOp::erase_dtl_interm_result()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int ecode = EventTable::EN_PX_SINGLE_DFO_NOT_ERASE_DTL_INTERM_RESULT;
  if (OB_SUCCESS != ecode && OB_SUCC(ret)) {
    LOG_WARN("ObPxCoordOp not erase_dtl_interm_result by design", K(ret));
    return OB_SUCCESS;
  }
#endif

  dtl::ObDtlChannelInfo ci;
  ObDTLIntermResultKey key;
  for (int i = 0; i < get_ch_set().count(); ++i) {
    if (OB_FAIL(get_ch_set().get_channel_info(i, ci))) {
      LOG_WARN("fail get channel info", K(ret));
    } else {
      key.channel_id_ = ci.chid_;
      for (int64_t batch_id = ctx_.get_px_batch_id();
           batch_id < PX_RESCAN_BATCH_ROW_COUNT && OB_SUCC(ret); batch_id++) {
        key.batch_id_ = batch_id;
        if (OB_FAIL(MTL(ObDTLIntermResultManager*)->erase_interm_result_info(key))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to release recieve internal result", K(ret), K(key));
          }
        }
      }
      LOG_TRACE("receive erase dtl interm res", K(i), K(get_spec().get_id()), K(ci), K(ctx_.get_px_batch_id()));
    }
  }
  return ret;
}

int ObPxReceiveSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    void *buf = ctx.get_allocator().alloc(sizeof(ObInitChannelWholeMsg::WholeMsgProvider));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObInitChannelWholeMsg::WholeMsgProvider *provider =
        new (buf)ObInitChannelWholeMsg::WholeMsgProvider();
      ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
      if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), DH_INIT_CHANNEL_PIECE_MSG, *provider))) {
        LOG_WARN("fail add whole msg provider", K(ret));
      }
    }
  }
  return ret;
}

int ObPxReceiveSpec::register_init_channel_msg(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
    OZ (sqc_ctx.init_channel_msg_cnts_.push_back({id_, 0}));
  }
  return ret;
}

int ObPxReceiveOp::send_channel_ready_msg(int64_t child_dfo_id)
{
  int ret = OB_SUCCESS;
  bool send_piece = true;
  bool need_wait_whole_msg = false;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (IS_PX_COORD(get_spec().get_type())) {
    // do nothing
  } else if (OB_ISNULL(handler)) {
    if (!IS_PX_COORD(get_spec().get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("px receive do not have sqc handler", K(get_spec().get_type()), K(ret));
    }
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    if (!proxy.get_recieve_use_interm_result()) {
      bool merge_finish = false;
      int64_t *curr_piece_cnt_ptr = nullptr;
      if (OB_FAIL(proxy.sqc_ctx_.get_init_channel_msg_cnt(get_spec().id_, curr_piece_cnt_ptr))) {
        LOG_WARN("failed to get curr piece cnt", K(ret));
      } else if (OB_ISNULL(curr_piece_cnt_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null piece cnt ptr got", K(ret));
      } else if (proxy.get_task_count() == ATOMIC_AAF(curr_piece_cnt_ptr, 1)) {
        merge_finish = true;
      } else if (proxy.get_task_count() < *curr_piece_cnt_ptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece count exceeded task count", K(proxy.get_task_count()), K(*curr_piece_cnt_ptr), K(ret));
      }
      LOG_TRACE("1 receive is ready", K(merge_finish), K(ATOMIC_LOAD(curr_piece_cnt_ptr)), K(get_spec().id_), K(lbt()));
      if (OB_SUCC(ret) && merge_finish) {
        ObInitChannelPieceMsg piece;
        piece.piece_count_ = ATOMIC_LOAD(curr_piece_cnt_ptr);
        piece.op_id_ = get_spec().id_;
        piece.thread_id_ = GETTID();
        piece.source_dfo_id_ = proxy.get_dfo_id();
        piece.target_dfo_id_ = child_dfo_id;
        const ObInitChannelWholeMsg *whole_msg = nullptr;
        if (OB_FAIL(proxy.get_dh_msg(get_spec().id_,
                                    dtl::DH_INIT_CHANNEL_PIECE_MSG,
                                    piece,
                                    whole_msg,
                                    ctx_.get_physical_plan_ctx()->get_timeout_timestamp(),
                                    send_piece,
                                    need_wait_whole_msg))) {
          LOG_WARN("failed to send piece msg", K(ret));
        }
      }
    }
  }
  return ret;
}
//------------- end ObPxReceiveOp-----------------


ObPxFifoReceiveOp::ObPxFifoReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
: ObPxReceiveOp(exec_ctx, spec, input),
  interrupt_proc_()
{
}

int ObPxFifoReceiveOp::inner_open()
{
  return ObPxReceiveOp::inner_open();
}

int ObPxFifoReceiveOp::inner_close()
{
  int ret = OB_SUCCESS;
  row_reader_.reset();
  int release_channel_ret = common::OB_SUCCESS;
  /* we must release channel even if there is some error happen before */
  if (channel_linked_) {
    release_channel_ret = ObPxChannelUtil::flush_rows(task_channels_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
    int64_t recv_cnt = 0;
    op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::DTL_SEND_RECV_COUNT;
    op_monitor_info_.otherstat_3_value_ = recv_cnt;
    release_channel_ret = msg_loop_.unregister_all_channel();
    if (release_channel_ret != common::OB_SUCCESS) {
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
    }
    release_channel_ret = ObPxChannelUtil::unlink_ch_set(get_ch_set(), &dfc_, true);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
  }
  // must erase after unlink channel
  release_channel_ret = erase_dtl_interm_result();
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_TRACE("release interm result failed", KR(release_channel_ret));
  }
  return ret;
}

int ObPxFifoReceiveOp::inner_get_next_row()
{
  const int64_t row_cnt = 1;
  return fetch_rows(row_cnt);
}

int ObPxFifoReceiveOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = fetch_rows(std::min(max_row_cnt, MY_SPEC.max_batch_size_));
  if (OB_ITER_END == ret) {
    brs_.size_ = 0;
    brs_.end_ = true;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPxReceiveOp::try_send_bloom_filter()
{
  int ret = OB_SUCCESS;
  /*ObPxReceiveOpInput *recv_input = reinterpret_cast<ObPxReceiveOpInput*>(input_);
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(recv_input->get_ch_provider());
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  common::ObIArray<ObBloomFilterSendCtx> &bf_send_ctx_array = sqc_proxy->get_bf_send_ctx_array();
  int64_t bfsendctxcount = bf_send_ctx_array.count();
  while (OB_SUCC(ret) && bf_send_idx_ < bf_send_ctx_array.count()) {
    if (bf_send_ctx_array.at(bf_send_idx_).bloom_filter_ready()) {
      int64_t start_time = ObTimeUtility::current_time();
      ObBloomFilterSendCtx &bf_send_ctx = bf_send_ctx_array.at(bf_send_idx_);
      ObPxBFSendBloomFilterArgs args;
      OZ(args.bloom_filter_.init(&bf_send_ctx.get_filter_data()->filter_));
      if (OB_SUCC(ret)) {
        args.bf_key_.init(bf_send_ctx.get_filter_data()->tenant_id_,
            bf_send_ctx.get_filter_data()->filter_id_,
            bf_send_ctx.get_filter_data()->server_id_,
            bf_send_ctx.get_filter_data()->px_sequence_id_);
        args.expect_bloom_filter_count_ = bf_send_ctx.get_filter_data()->bloom_filter_count_;
        args.current_bloom_filter_count_ = 1;
        args.phase_ = ObSendBFPhase::FIRST_LEVEL;
        args.timeout_timestamp_ = phy_plan_ctx->get_timeout_timestamp();
      }
      if (OB_SUCC(ret)) {
        common::ObIArray<BloomFilterIndex> &filter_index_array = bf_send_ctx.get_filter_indexes();
        ObPxBloomFilterChSet &bf_chset = bf_send_ctx.get_bf_ch_set();
        int64_t ch_info_count = bf_chset.count();
        args.expect_phase_count_ = ch_info_count;
        int64_t filter_idx = 0;
        while (bf_send_ctx.get_filter_channel_idx() < filter_index_array.count() && OB_SUCC(ret)) {
          filter_idx = ATOMIC_FAA(&bf_send_ctx.get_filter_channel_idx(), 1);
          if (filter_idx < filter_index_array.count()) {
            args.next_peer_addrs_.reuse();
            auto channel_filter_idx = filter_index_array.at(filter_idx);
            args.bloom_filter_.set_begin_idx(channel_filter_idx.begin_idx_);
            args.bloom_filter_.set_end_idx(channel_filter_idx.end_idx_);
            ObDtlChannelInfo chan_info; // just for declare a reference, don't use chan_info to do anything else
            ObDtlChannelInfo &ch_info = chan_info;
            for (int i = 0; OB_SUCC(ret) && i < channel_filter_idx.channel_ids_.count(); ++i) {
              if (OB_FAIL(bf_chset.get_channel_info(channel_filter_idx.channel_ids_.at(i), ch_info))) {
                LOG_WARN("failed get ObDtlChannelInfo", K(channel_filter_idx.channel_ids_.at(i)), K(i), K(ret));
              } else if (OB_FAIL(args.next_peer_addrs_.push_back(ch_info.peer_))) {
                LOG_WARN("failed push back peer addr", K(i), K(ret));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (channel_filter_idx.channel_id_ >= ch_info_count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected channel id", K(channel_filter_idx.channel_id_), K(ch_info_count));
            } else if (OB_FAIL(bf_chset.get_channel_info(channel_filter_idx.channel_id_, ch_info))) {
                LOG_WARN("failed get ObDtlChannelInfo", K(channel_filter_idx.channel_id_), K(ret));
            } else if (OB_FAIL(bf_rpc_proxy_.to(ch_info.peer_)
                        .by(bf_send_ctx.get_filter_data()->tenant_id_)
                        .timeout(phy_plan_ctx->get_timeout_timestamp())
                        .compressed(bf_send_ctx.get_bf_compress_type())
                        .send_bloom_filter(args, NULL))) {
              LOG_WARN("fail to send bloom filter", K(ret));
            }
          }
        }
        op_monitor_info_.otherstat_1_value_ = ObTimeUtility::current_time() - start_time;
        bf_send_ctx.set_bloom_filter_ready(false); // all piece of bloom filter was sent, this thread sent the last one piece
      }
    }
    ++bf_send_idx_;
  }*/
  return ret;
}

// Routine "do_clear_datum_eval_flag" behave almost the same as
// "ObOperator::do_clear_datum_eval_flag" except explicitly clear
// projected flag under batch_result mode(vectorization)
void ObPxReceiveOp::do_clear_datum_eval_flag()
{
  FOREACH_CNT(e, spec_.calc_exprs_) {
    if ((*e)->is_batch_result()) {
      (*e)->get_evaluated_flags(eval_ctx_).unset(eval_ctx_.get_batch_idx());
      (*e)->get_eval_info(eval_ctx_).projected_ = 0;
    } else {
      (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
    }
  }
}

int ObPxFifoReceiveOp::fetch_rows(const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  // 从channel sets 读取数据，并向上迭代
  const ObPxReceiveSpec &spec = static_cast<const ObPxReceiveSpec &>(get_spec());
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(try_link_channel())) {
    LOG_WARN("failed to init channel", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t timeout_ts = phy_plan_ctx->get_timeout_timestamp();
    int64_t retry_cnt = 0;
    do {
      ret = get_spec().use_rich_format_
              ? get_rows_from_channels_vec(row_cnt, timeout_ts - get_timestamp())
              : get_rows_from_channels(row_cnt, timeout_ts - get_timestamp());
      if (OB_SUCCESS == ret) {
        metric_.mark_first_out();
        metric_.set_last_out_ts(::oceanbase::common::ObTimeUtility::current_time());
        LOG_DEBUG("Got one row from channel", K(ret));
        break; // got one row
      } else if (OB_ITER_END == ret) {
        if (GCONF.enable_sql_audit) {
          op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::EXCHANGE_EOF_TIMESTAMP;
          op_monitor_info_.otherstat_2_value_ = oceanbase::common::ObClockGenerator::getClock();
        }
        metric_.mark_eof();
        LOG_TRACE("Got eof row from channel", K(ret));
        break;
      } else if (OB_EAGAIN == ret) {
        // no data for now, wait and try again
        if (ObTimeUtility::current_time() >= timeout_ts) {
          ret = OB_TIMEOUT;
          LOG_WARN("get row from channel timeout", K(ret));
        } else {
          ob_usleep<ObWaitEventIds::DTL_PROCESS_CHANNEL_SLEEP>(1 * 1000);
          int tmp_ret = ctx_.fast_check_status();
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
        break;
      }
    } while (OB_EAGAIN == ret);
  }
  if (OB_ITER_END == ret) {
    iter_end_ = true;
    LOG_TRACE("receive eof row", K(get_spec().id_),  K(ret));
  }
  return ret;
}

int ObPxFifoReceiveOp::try_link_channel()
{
  int ret = OB_SUCCESS;
  // 从channel sets 读取数据，并向上迭代
  if (!channel_linked_) {
    ObPxReceiveOpInput *recv_input = reinterpret_cast<ObPxReceiveOpInput*>(input_);
    ret = init_channel(*recv_input, task_ch_set_, task_channels_,
        msg_loop_, px_row_msg_proc_, interrupt_proc_);
    if (OB_FAIL(ret)) {
      LOG_WARN("Fail to init channel", K(ret));
    } else {
      metric_.set_id(get_spec().id_);
    }
  }
  return ret;
}

int ObPxFifoReceiveOp::get_rows_from_channels(const int64_t row_cnt, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  UNUSED(timeout_us);
  while (!got_row && OB_SUCC(ret)) {
    // Check uniterated rows first, then check all channels are EOF.
    // Because channel may mark to EOF after transfer buffer to reader, rows still in reader.
    int64_t left = row_reader_.left_rows();
    if (left >= row_cnt || (left > 0 && msg_loop_.all_eof(task_channels_.count()))) {
      clear_evaluated_flag();
      clear_dynamic_const_parent_flag();
      if (!is_vectorized()) {
        if (OB_FAIL(row_reader_.get_next_row(MY_SPEC.child_exprs_,
                                             MY_SPEC.dynamic_const_exprs_,
                                             eval_ctx_))) {
          LOG_WARN("get next row from row reader failed", K(ret));
        } else {
          got_row = true;
          metric_.count();
        }
      } else {
        int64_t read_rows = 0;
        if (OB_FAIL(row_reader_.get_next_batch(MY_SPEC.child_exprs_,
                                               MY_SPEC.dynamic_const_exprs_,
                                               eval_ctx_,
                                               row_cnt,
                                               read_rows,
                                               stored_rows_))) {
          LOG_WARN("get next batch failed", K(ret));
        } else {
          got_row = true;
          brs_.size_ = read_rows;
        }
      }
      break;
    }
    if (msg_loop_.all_eof(task_channels_.count())) {
      ret = OB_ITER_END;
      LOG_TRACE("no more date in all channels", K(ret));
      break;
    }
    if (OB_FAIL(msg_loop_.process_any())) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail pop sqc execution result from channel", K(ret));
      } else {
        ret = OB_EAGAIN;
      }
    }
  }
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObPxFifoReceiveOp::get_rows_from_channels_vec(const int64_t row_cnt, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  UNUSED(timeout_us);
  while (!got_row && OB_SUCC(ret)) {
    // Check uniterated rows first, then check all channels are EOF.
    // Because channel may mark to EOF after transfer buffer to reader, rows still in reader.
    int64_t left = row_reader_.left_rows();
    if (left >= row_cnt || (left > 0 && msg_loop_.all_eof(task_channels_.count()))) {
      clear_evaluated_flag();
      clear_dynamic_const_parent_flag();
      int64_t read_rows = 0;
      if (OB_FAIL(row_reader_.get_next_batch_vec(MY_SPEC.child_exprs_,
                                              MY_SPEC.dynamic_const_exprs_,
                                              eval_ctx_,
                                              row_cnt,
                                              read_rows,
                                              vector_rows_))) {
        LOG_WARN("get next batch failed", K(ret));
      } else {
        got_row = true;
        brs_.size_ = read_rows;
      }
      break;
    }
    if (msg_loop_.all_eof(task_channels_.count())) {
      ret = OB_ITER_END;
      LOG_TRACE("no more date in all channels", K(ret));
      break;
    }
    if (OB_FAIL(msg_loop_.process_any())) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail pop sqc execution result from channel", K(ret));
      } else {
        ret = OB_EAGAIN;
      }
    }
  }
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_EAGAIN;
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
