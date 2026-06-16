/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DTL_UTILS_H
#define OB_DTL_UTILS_H

#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/dtl/ob_dtl_flow_control.h"

namespace oceanbase {
namespace sql {
namespace dtl {

struct ObDtlServerChannelGroup;

class ObPxControlChannelProc : public ObIDltChannelLoopPred
{
public:
  ObPxControlChannelProc()
  {}
  virtual bool pred_process(int64_t idx, ObDtlChannel *chan) override
  {
    UNUSED(idx);
    return nullptr == chan->get_dfc();
  }
};

class ObDtlAsynSender
{
public:
  ObDtlAsynSender(ObIArray<ObDtlChannel *> &channels, ObDtlChTotalInfo *ch_info,
                  bool is_transmit, int64_t timeout_ts,
                  const ObIArray<ObDtlServerChannelGroup *> *server_groups)
      : channels_(channels), ch_info_(ch_info), is_transmit_(is_transmit),
        timeout_ts_(timeout_ts), server_groups_(server_groups) {}

  int asyn_send();
  virtual int async_send_batch() = 0;
  int syn_send();
  virtual int action(ObDtlChannel* ch) = 0;

private:
  int calc_batch_buffer_cnt(int64_t &max_batch_size, int64_t &max_loop_cnt);
protected:
  ObIArray<ObDtlChannel*> &channels_;
  ObDtlChTotalInfo *ch_info_;
  bool is_transmit_;
  int64_t timeout_ts_;
  const ObIArray<ObDtlServerChannelGroup *> *server_groups_;
};

struct ObDtlPeerCtlBatch {

  bool all_done() const { return next_send_ch_idx_ >= chs_.count(); }
  void advance_next_send_ch_idx_prefix()
  {
    const int64_t n = chs_.count();
    while (next_send_ch_idx_ < n && posted_flags_.at(next_send_ch_idx_)) {
      ++next_send_ch_idx_;
    }
  }
  ObDtlServerChannelGroup *server_group_;
  ObTMArray<ObDtlChannel *> chs_; //channels of self side
  ObTMArray<int64_t> ch_ids_; //channel ids of peers side
  // First slot index not in the fully-posted prefix: for all i in [0, next_send_ch_idx_), posted_flags_[i].
  // Scan for new EOF aggregation starts here to avoid rescanning the completed prefix.
  int64_t next_send_ch_idx_{0};
  // Slot indices (into chs_) aggregated in the current fill_batch_msg, same order as batch_msg payloads.
  ObTMArray<int64_t> cur_batch_ch_slot_indices_;
  ObTMArray<bool> posted_flags_;
  // Cumulative payload bytes posted via batch RPC for this peer (EOF path updates on each post).
  int64_t total_sent_payload_bytes_{0};
  int64_t wait_count_{0};
  int64_t max_wait_time_{0};
  int64_t total_wait_time_{0};
  TO_STRING_KV(K(chs_.count()), K(next_send_ch_idx_),
               K(total_sent_payload_bytes_), K(wait_count_), K(max_wait_time_),
               K(total_wait_time_));
};

struct ObWaitChannelInfo {
  ObDtlChannel *ch;
  ObDtlPeerCtlBatch *slot;
  TO_STRING_KV(KP(ch), KP(slot));
};

class ObDtlBatchAsyncSender : public ObDtlAsynSender {
public:
  ObDtlBatchAsyncSender(
      ObIArray<ObDtlChannel *> &channels, ObDtlChTotalInfo *ch_info,
      bool is_transmit, int64_t timeout_ts,
      const ObIArray<ObDtlServerChannelGroup *> *server_groups)
      : ObDtlAsynSender(channels, ch_info, is_transmit, timeout_ts, server_groups),
        batch_msg_() {}

  int async_send_batch() override final;
  int post_batch_dtl_msg(ObDtlPeerCtlBatch &slot, ObDtlChannel *&out_carrier_ch);
  int after_send_batch(ObDtlPeerCtlBatch &slot);
  int wait_batch_response(ObIArray<ObWaitChannelInfo> &wait_channels);
  virtual int check_need_send(ObDtlChannel *ch, bool &need) {
    need = true;
    return OB_SUCCESS;
  }
  virtual int fill_batch_msg(ObDtlPeerCtlBatch &slot) {
    int ret = OB_SUCCESS;
    slot.cur_batch_ch_slot_indices_.reset();
    if (OB_FAIL(batch_msg_.ch_ids_.assign(slot.ch_ids_))) {
      SQL_DTL_LOG(WARN, "failed to assign batch ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < slot.chs_.count(); ++i) {
        if (OB_FAIL(slot.cur_batch_ch_slot_indices_.push_back(i))) {
          SQL_DTL_LOG(WARN, "failed to push batch ch index", K(ret));
        }
      }
    }
    return ret;
  }

  virtual int process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch) = 0;

private:
  // Wait for one pending channel (in-process RPC or flow-control block) so the next
  // batch round can make progress. Only one channel per call to avoid serial block
  // waits on sibling channels in the same server group (see ob_dtl_rpc_channel batch collect).
  int wait_one_pending_channel(ObDtlPeerCtlBatch &slot, bool &waited);

protected:
  ObDtlBatchMsg batch_msg_;
};

class ObTransmitEofAsynSender : public ObDtlBatchAsyncSender
{
public:
  ObTransmitEofAsynSender(ObIArray<ObDtlChannel*> &channels,
                          ObDtlChTotalInfo *ch_info,
                          bool is_transmit,
                          int64_t timeout_ts,
                          sql::ObEvalCtx *eval_ctx,
                          ObDtlMsgType type,
                          const ObIArray<ObDtlServerChannelGroup *> *server_groups) :
    ObDtlBatchAsyncSender(channels, ch_info, is_transmit, timeout_ts, server_groups),
    eval_ctx_(eval_ctx),
    type_(type)
  {
    batch_msg_.contained_msg_type_ = type_;
  }

  int action(ObDtlChannel *ch) override;
  int check_need_send(ObDtlChannel *ch, bool &need) override;
  int fill_batch_msg(ObDtlPeerCtlBatch &slot) override;
  int process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch) override;
private:
  sql::ObEvalCtx *eval_ctx_;
  ObDtlMsgType type_;
};

class ObDfcDrainAsynSender : public ObDtlBatchAsyncSender
{
public:
  ObDfcDrainAsynSender(ObIArray<ObDtlChannel*> &channels,
                      ObDtlChTotalInfo *ch_info,
                      bool is_transmit,
                      int64_t timeout_ts,
                      const ObIArray<ObDtlServerChannelGroup *> *server_groups) :
    ObDtlBatchAsyncSender(channels, ch_info, is_transmit, timeout_ts, server_groups)
  {
    batch_msg_.contained_msg_type_ = ObDtlMsgType::DRAIN_DATA_FLOW;
  }

  int action(ObDtlChannel *ch) override;
  int process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch) override;
};

class ObDfcUnblockAsynSender : public ObDtlBatchAsyncSender
{
public:
  ObDfcUnblockAsynSender(ObIArray<ObDtlChannel*> &channels,
                      ObDtlChTotalInfo *ch_info,
                      bool is_transmit,
                      int64_t timeout_ts,
                      ObDtlFlowControl &dfc,
                      const ObIArray<ObDtlServerChannelGroup *> *server_groups) :
    ObDtlBatchAsyncSender(channels, ch_info, is_transmit, timeout_ts, server_groups),
    dfc_(dfc),
    unblock_cnt_(0)
  {
    batch_msg_.contained_msg_type_ = ObDtlMsgType::UNBLOCKING_DATA_FLOW;
  }

  int action(ObDtlChannel *ch) override;

  int64_t get_unblocked_cnt() { return unblock_cnt_; }

  int check_need_send(ObDtlChannel *ch, bool &need) override;
  int process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch) override;
private:
  ObDtlFlowControl &dfc_;
  int64_t unblock_cnt_;
};

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_UTILS_H */
