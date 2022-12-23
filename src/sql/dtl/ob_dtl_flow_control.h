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

#ifndef OB_DTL_FLOW_CONTROL_H
#define OB_DTL_FLOW_CONTROL_H

#include "lib/utility/ob_unify_serialize.h"
#include "sql/dtl/ob_dtl_msg.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "lib/atomic/ob_atomic.h"
#include "sql/dtl/ob_dtl_processor.h"
#include "lib/lock/ob_thread_cond.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_task.h"
#include "lib/compress/ob_compress_util.h"
#include "lib/utility/ob_tracepoint.h"
namespace oceanbase {
namespace sql {

class ObSqcBcastService;

namespace dtl {

class ObDtlChannel;
class ObDtlChannelLoop;
class ObDtlFlowControl;
class ObDtlBasicChannel;
class ObDtlLocalFirstBufferCache;

// Receive op send unblocking msg to transimit op, and transmit op will going
class ObDtlUnblockingMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::UNBLOCKING_DATA_FLOW>
{
  OB_UNIS_VERSION_V(1);
public:
  ObDtlUnblockingMsg() {}

  void reset() {}
};

class ObDtlUnblockingMsgP : public dtl::ObDtlPacketProc<ObDtlUnblockingMsg>
{
public:
  explicit ObDtlUnblockingMsgP(ObDtlFlowControl &dfc) : dfc_(dfc)
  {}
  virtual ~ObDtlUnblockingMsgP() = default;
  int process(const ObDtlUnblockingMsg &pkt);
  ObDtlFlowControl &dfc_;
};

// Receive op send drain msg to transimit op, and transmit op will going
class ObDtlDrainMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::DRAIN_DATA_FLOW>
{
  OB_UNIS_VERSION_V(1);
public:
  ObDtlDrainMsg() {}

  void reset() {}
};

class ObDtlDrainMsgP : public dtl::ObDtlPacketProc<ObDtlDrainMsg>
{
public:
  explicit ObDtlDrainMsgP(ObDtlFlowControl &dfc) : dfc_(dfc)
  {}
  virtual ~ObDtlDrainMsgP() = default;
  int process(const ObDtlDrainMsg &pkt);
  ObDtlFlowControl &dfc_;
};

class ObDtlFlowControl
{
public:
  ObDtlFlowControl() :
  tenant_id_(OB_INVALID_ID), timeout_ts_(0), communicate_flag_(0),
  compressor_type_(common::ObCompressorType::NONE_COMPRESSOR), is_init_(false), block_ch_cnt_(0),
  total_memory_size_(0), total_buffer_cnt_(0), accumulated_blocked_cnt_(0), blocks_(), chans_(), drain_ch_cnt_(0),
  dfo_key_(), op_metric_(nullptr), first_buf_cache_(nullptr),
  chan_loop_(nullptr), ch_info_(nullptr)
  { }

  virtual ~ObDtlFlowControl() { destroy(); }

  bool is_all_channel_act();
  bool is_init() { return is_init_; }

  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE int64_t get_timeout_ts() const { return timeout_ts_; }
  OB_INLINE void set_timeout_ts(int64_t timeout_ts) { timeout_ts_ = timeout_ts; }

  OB_INLINE void increase(int64_t size) { (ATOMIC_AAF(&total_memory_size_, size)); ATOMIC_INC(&total_buffer_cnt_); }
  OB_INLINE void decrease(int64_t size) { (ATOMIC_SAF(&total_memory_size_, size)); ATOMIC_DEC(&total_buffer_cnt_); }
  OB_INLINE int64_t get_used() { return (ATOMIC_LOAD(&total_memory_size_)); }

  OB_INLINE bool need_block()
  {
    bool need_block = (ATOMIC_LOAD(&total_memory_size_)) >= THRESHOLD_SIZE
      || MAX_BUFFER_CNT <= (ATOMIC_LOAD(&total_buffer_cnt_));
  #ifdef ERRSIM
    int ret = common::OB_SUCCESS;
    ret = OB_E(EventTable::EN_FORCE_DFC_BLOCK) ret;
    need_block = (common::OB_SUCCESS != ret) ? true : need_block;
    SQL_DTL_LOG(TRACE, "trace block", K(need_block), K(total_buffer_cnt_), K(ret));
    ret = common::OB_SUCCESS;
  #endif
    return need_block;
  }
  OB_INLINE bool can_unblock()
  {
    bool can_unblock = (ATOMIC_LOAD(&total_memory_size_)) <= THRESHOLD_SIZE / 2
        || MAX_BUFFER_CNT / 2 >= (ATOMIC_LOAD(&total_buffer_cnt_));
#ifdef ERRSIM
  int ret = common::OB_SUCCESS;
  ret = OB_E(EventTable::EN_DTL_ONE_ROW_ONE_BUFFER) ret;
  can_unblock = (common::OB_SUCCESS != ret) ? 0 == (ATOMIC_LOAD(&total_buffer_cnt_)) : can_unblock;
  SQL_DTL_LOG(TRACE, "trace unblock", K(can_unblock), K(total_buffer_cnt_), K(ret));
  ret = common::OB_SUCCESS;
#endif
    return can_unblock;
  }

  OB_INLINE bool is_block() { return 1 <= (ATOMIC_LOAD(&block_ch_cnt_)); }
  OB_INLINE bool is_block(int64_t idx);

  bool is_drain(ObDtlBasicChannel *channel);
  int set_drain(ObDtlBasicChannel *channel);
  bool all_ch_drained() {
    return (ATOMIC_LOAD(&drain_ch_cnt_) == chans_.count());
  }

  OB_INLINE void increase_blocked_cnt(int64_t cnt) {
    ATOMIC_AAF(&block_ch_cnt_, cnt);
    ATOMIC_AAF(&accumulated_blocked_cnt_, cnt);
  }
  OB_INLINE void decrease_blocked_cnt(int64_t cnt) { ATOMIC_SAF(&block_ch_cnt_, cnt); }


  // 支持多个channel公用一起block，如果block_ch_cnt_大于1，表示block为true
  OB_INLINE void set_block(int64_t idx);
  OB_INLINE void unblock(int64_t idx);
  OB_INLINE int64_t get_blocked_cnt() { return (ATOMIC_LOAD(&block_ch_cnt_)); }
  OB_INLINE int64_t get_total_buffer_cnt() { return (ATOMIC_LOAD(&total_buffer_cnt_)); }
  OB_INLINE bool get_nth_block(int64_t idx) { return (ATOMIC_LOAD(&blocks_.at(idx))); }
  OB_INLINE int64_t get_accumulated_blocked_cnt() { return (ATOMIC_LOAD(&accumulated_blocked_cnt_)); }

  int register_channel(ObDtlChannel* ch);
  int unregister_channel(ObDtlChannel* ch);
  int unregister_all_channel();

  int reserve(int64_t size) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(blocks_.reserve(size))) {
      // nop
    } else if (OB_FAIL(chans_.reserve(size))) {
      // nop
    }
    return ret;
  }
  virtual int init(uint64_t tenant_id, int64_t chan_cnt);
  virtual void destroy() {
    chans_.reset();
    blocks_.reset();
    ch_info_ = nullptr;
    is_init_ = false;
  }

  virtual int final_check();

  int get_channel(int64_t idx, ObDtlChannel *&ch);
  int find(ObDtlChannel* ch, int64_t &out_idx);
  bool is_block(ObDtlChannel* ch);
  int block_channel(ObDtlChannel* ch);
  int unblock_channel(ObDtlChannel* ch);
  int sync_send_drain(int64_t &unblock_cnt);
  int notify_all_blocked_channels_unblocking(int64_t &unblock_cnt);
  int notify_channel_unblocking(ObDtlChannel *ch, int64_t &block_cnt, bool asyn_send = true);
  int64_t get_channel_count() const {
    return chans_.count();
  }
  int drain_all_channels();

  OB_INLINE void set_receive() { communicate_flag_ |= 0x01; }
  OB_INLINE bool is_receive() { return communicate_flag_ & 0x01; }
  OB_INLINE void set_transmit() { communicate_flag_ |= 0x10; }
  OB_INLINE bool is_transmit() { return communicate_flag_ & 0x10; }
  OB_INLINE void set_qc_coord() { communicate_flag_ |= 0x100; }
  OB_INLINE bool is_qc_coord() { return communicate_flag_ & 0x100; }

  OB_INLINE void set_op_metric(sql::ObOpMetric *op_metric) { op_metric_ = op_metric;}
  OB_INLINE sql::ObOpMetric *get_op_metric() { return op_metric_; }
  OB_INLINE int64_t get_op_id() { return nullptr != op_metric_ ? op_metric_->get_id() : -1; }

  bool has_dfo_key() { return dfo_key_.is_valid(); }
  ObDtlDfoKey &get_dfo_key() { return dfo_key_; }
  void set_dfo_key(ObDtlDfoKey &key) { dfo_key_ = key; }
  ObDtlSqcInfo &get_sender_sqc_info() { return sender_sqc_info_; }
  void set_sender_sqc_info(ObDtlSqcInfo &sqc_info) { sender_sqc_info_ = sqc_info; }

  ObDtlLocalFirstBufferCache *get_first_buffer_cache() { return first_buf_cache_; }
  void set_first_buffer_cache(ObDtlLocalFirstBufferCache *first_buf_cache)
  {
    first_buf_cache_ = first_buf_cache;
  }
  void set_dtl_channel_watcher(ObDtlChannelLoop *dtl_watcher)
  {
    chan_loop_ = dtl_watcher;
  }
  ObDtlChannelLoop *get_dtl_channel_watcher()
  {
    return chan_loop_;
  }
  void set_total_ch_info(ObDtlChTotalInfo *ch_info)
  { ch_info_ = ch_info; }

  common::ObCompressorType get_compressor_type() { return compressor_type_; }

private:
  static const int64_t THRESHOLD_SIZE = 2097152;
  static const int64_t MAX_BUFFER_CNT = 3;
  static const int64_t MAX_BUFFER_FACTOR = 2;
  uint64_t tenant_id_;
  int64_t timeout_ts_;
  // 标识是否是transmit、receive、qc等
  int communicate_flag_;
  common::ObCompressorType compressor_type_;
  bool is_init_;
  int64_t block_ch_cnt_;
  int64_t total_memory_size_;
  int64_t total_buffer_cnt_;

  int64_t accumulated_blocked_cnt_;

  // dfc control to block how many object
  // for receive, only 1
  // for transmit, it's equal the count of channels
  common::ObSEArray<bool, 16> blocks_;
  common::ObSEArray<ObDtlChannel*, 16> chans_;

  uint64_t drain_ch_cnt_;
  ObDtlDfoKey dfo_key_;
  ObDtlSqcInfo sender_sqc_info_;
  sql::ObOpMetric *op_metric_;

  ObDtlLocalFirstBufferCache *first_buf_cache_;
  // for dtl channel notify
  ObDtlChannelLoop *chan_loop_;

  ObDtlChTotalInfo *ch_info_;

private:
  // Todo: In DFC, it can monitor data size and so on
};

OB_INLINE void ObDtlFlowControl::set_block(int64_t idx)
{
  increase_blocked_cnt(1);
  bool &blocked = blocks_.at(idx);
  ATOMIC_SET(&blocked, true);
}

OB_INLINE void ObDtlFlowControl::unblock(int64_t idx)
{
  decrease_blocked_cnt(1);
  bool &blocked = blocks_.at(idx);
  ATOMIC_SET(&blocked, false);
}

OB_INLINE bool ObDtlFlowControl::is_block(int64_t idx)
{
  bool blocked = blocks_.at(idx);
  return (ATOMIC_LOAD(&blocked)) && 1 <= (ATOMIC_LOAD(&block_ch_cnt_));
}

} // dtl
} // sql
} // oceanbase

#endif /* OB_DTL_FLOW_CONTROL_H */
