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

#ifndef OB_DTL_CHANNEL_H
#define OB_DTL_CHANNEL_H

#include <stdint.h>
#include <functional>
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/ob_print_utils.h"
#include <lib/lock/ob_thread_cond.h>
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "sql/dtl/ob_dtl_msg.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_processor.h"
#include "sql/dtl/ob_op_metric.h"
#include "observer/ob_server_struct.h"
#include "lib/compress/ob_compress_util.h"
#include "share/detect/ob_detectable_id.h"

namespace oceanbase {

// forward declarations
namespace common {
class ObNewRow;
}  // common

namespace sql {
namespace dtl {

// Channel is a abstract way to exchange data between two tasks. Each
// side of task needn't care about what it in deed do when they
// communication. Channel will choose the right way to let it done.
//
// Channel is used as a queue when one task need to talk with the
// other side. You can call "standard queue interfaces" just like
// push&pop functions when using it.
//
// Currently channel only have implemented one direction data
// communication so decide the right position of each task is a must,
// which is the producer and which is the consumer.
//

// define dtl process api
class ObIDtlChannelProc
{
public:
  // Buffer may transfer to processor, the processor should release the buffer
  // to DTL buffer manager if transferred.
  virtual int process(const ObDtlLinkedBuffer &, bool &transferred) = 0;
};

// define interface for process_one/process_one_if
class ObIDltChannelLoopPred
{
public:
  virtual bool pred_process(int64_t, ObDtlChannel *) = 0;
};

class ObDtlChannelLoop;
class ObDtlChannelWatcher;
class ObDtlFlowControl;

#define DTL_CHAN_RUN     (1ULL)
#define DTL_CHAN_BLOCKED (1ULL << 1)
#define DTL_CHAN_DARIN   (1ULL << 2)
#define DTL_CHAN_EOF     (1ULL << 3)
#define DTL_CHAN_FIRST_BUF     (1ULL << 4)
#define IS_LOCAL_CHANNEL(chan) (ObDtlChannel::DtlChannelType::LOCAL_CHANNEL == data_ch->get_channel_type())
#define IS_RECEIVE_CHANNEL(chid) (!!(chid & 0x1))

enum DTLChannelOwner
{
  INVALID_OWNER,
  OPERATOR_OWNER,
  JOIN_FILTER_OWNER,
  QC_OWNER,
  SQC_OWNER,
  DAS_OWNER,
};

class ObDtlChannel : public common::ObDLinkBase<ObDtlChannel>
{
public:
  enum class DtlChannelType {
    LOCAL_CHANNEL = 0,
    RPC_CHANNEL = 1,
    BASIC_CHANNEL = 2
  };
public:
  explicit ObDtlChannel(uint64_t id, const common::ObAddr &peer, DtlChannelType type);
  virtual ~ObDtlChannel() {}

  DtlChannelType get_channel_type() const { return channel_type_; };
  virtual int init() = 0;

  // typical queue interfaces
  virtual int send(const ObDtlMsg &msg, int64_t timeout_ts,
      ObEvalCtx *eval_ctx = nullptr, bool is_eof = false) = 0;
  virtual int feedup(ObDtlLinkedBuffer *&buffer) = 0;
  virtual int attach(ObDtlLinkedBuffer *&linked_buffer, bool inc_recv_buf_cnt = true) = 0;
  virtual int flush(bool wait=true, bool wait_response = true) = 0;

  virtual bool is_empty() const = 0;

  virtual int process1(
      ObIDtlChannelProc *proc,
      int64_t timeout, bool &last_row_in_buffer) = 0;
  virtual int send1(
      std::function<int(const ObDtlLinkedBuffer &buffer)> &proc,
      int64_t timeout) = 0;

  virtual void set_dfc_idx(int64_t idx) = 0;

  void set_msg_watcher(ObDtlChannelWatcher &watcher);

  void unset_msg_watcher();

  void set_send_buffer_size(int64_t send_buffer_size);

  uint64_t get_id() const;
  const common::ObAddr &get_peer() const { return peer_; }

  virtual int clear_response_block() = 0;
  virtual int wait_response() = 0;
  void done();
  bool is_done() const;

  int64_t pin();
  int64_t unpin();
  int64_t get_pins() const;

  TO_STRING_KV(KP_(id), K_(peer));

  static uint64_t generate_id(uint64_t ch_cnt = 1);

  void set_channel_loop(ObDtlChannelLoop &channel_loop) {
    channel_loop_ = &channel_loop;
  }
  ObDtlChannelLoop *get_channel_loop() const { return channel_loop_; }
  void set_dfc(ObDtlFlowControl *dfc) { dfc_ = dfc; }
  ObDtlFlowControl *get_dfc() { return dfc_; }
  bool is_data_channel() { return nullptr != dfc_; }
  virtual int clean_recv_list () = 0;

  bool first_recv_msg_processed() { return !first_recv_msg_; }
  bool can_unblock();

  void set_audit(bool enable_audit) { metric_.set_audit(enable_audit); }

  void set_owner_mod(DTLChannelOwner owner_mod)
  {
    owner_mod_ = owner_mod;
  }
  DTLChannelOwner get_owner_mod() const { return owner_mod_; }
  void set_operator_owner() { set_owner_mod(DTLChannelOwner::OPERATOR_OWNER); }
  void set_join_filter_owner() { set_owner_mod(DTLChannelOwner::JOIN_FILTER_OWNER); }
  void set_qc_owner() { set_owner_mod(DTLChannelOwner::QC_OWNER); }
  void set_sqc_owner() { set_owner_mod(DTLChannelOwner::SQC_OWNER); }
  void set_das_owner() { set_owner_mod(DTLChannelOwner::DAS_OWNER); }

  void set_thread_id(int64_t thread_id) { thread_id_ = thread_id; }
  int64_t get_thread_id() const { return thread_id_; }

  ObOpMetric &get_op_metric() { return metric_; }
  int64_t get_op_id();

  int64_t alloc_buffer_count();
  int64_t free_buffer_count();

  int64_t get_alloc_buffer_cnt() { return alloc_buffer_cnt_; }
  int64_t get_free_buffer_cnt() { return free_buffer_cnt_; }
  int64_t get_send_buffer_size() { return send_buffer_size_; }

  uint64_t get_state() { return ATOMIC_LOAD(&state_); }

  bool is_drain() { return (ATOMIC_LOAD(&state_) & DTL_CHAN_DARIN); }
  bool is_blocked() { return (ATOMIC_LOAD(&state_) & DTL_CHAN_BLOCKED); }
  bool is_run() { return (ATOMIC_LOAD(&state_) & DTL_CHAN_RUN); }
  bool is_eof() { return (ATOMIC_LOAD(&state_) & DTL_CHAN_EOF); }

  void reset_state() { state_ = DTL_CHAN_RUN; }

  void set_status(int64_t status)
  {
    do {
      uint64_t state = state_;
      uint64_t new_state = state | status;
      bool succ = ATOMIC_BCAS(&state_, state, new_state);
      if (succ) {
        break;
      }
    } while(1);
  }
  void set_drain() { set_status(DTL_CHAN_DARIN); }
  void set_eof() { set_status(DTL_CHAN_EOF); }
  void set_blocked() { set_status(DTL_CHAN_BLOCKED); }

  void unset_status(int64_t status)
  {
    do {
      uint64_t state = state_;
      uint64_t new_state = state & (~status);
      bool succ = ATOMIC_BCAS(&state_, state, new_state);
      if (succ) {
        break;
      }
    } while(1);
  }
  void unset_blocked() { unset_status(DTL_CHAN_BLOCKED); }

  int set_bcast_chan_ids(common::ObIArray<uint64_t> &chans); 
  int get_bcast_chan_ids(common::ObIArray<uint64_t> &chans);

  void set_interm_result(bool flag) { use_interm_result_ = flag; }
  bool use_interm_result() { return use_interm_result_; }

  void set_enable_channel_sync(bool enable_channel_sync) { enable_channel_sync_ = enable_channel_sync; }
  uint64_t enable_channel_sync() const { return enable_channel_sync_; }

  OB_INLINE void set_loop_index(int64_t loop_idx) { loop_idx_ = loop_idx; }
  OB_INLINE int64_t get_loop_index() { return loop_idx_; }

  OB_INLINE ObDtlChannelWatcher *get_msg_watcher() { return msg_watcher_; }

  void set_compression_type(const common::ObCompressorType &type) { compressor_type_ = type; }

  void set_batch_id(int64_t batch_id) { batch_id_ = batch_id; }
  int64_t get_batch_id() { return batch_id_; }

  bool is_px_channel() { return is_px_channel_; }
  void set_is_px_channel(bool flag) { is_px_channel_ = flag; }

  bool channel_is_eof() { return channel_is_eof_; }
  void set_channel_is_eof(bool flag) { 
    channel_is_eof_ = flag;
  }
  virtual void reset_px_row_iterator() { /*do nothing*/ }

  bool ignore_error() { return ignore_error_; }
  void set_ignore_error(bool flag) { ignore_error_ = flag; }

  void set_register_dm_info(common::ObRegisterDmInfo &register_dm_info) { register_dm_info_ = register_dm_info; }
  const common::ObRegisterDmInfo &get_register_dm_info() { return register_dm_info_; }

  virtual int push_buffer_batch_info() = 0;
protected:
  common::ObThreadCond cond_;
  int64_t pins_;
  uint64_t id_;
  bool done_;
  int64_t send_buffer_size_;
  ObDtlChannelWatcher *msg_watcher_;
  const common::ObAddr peer_;
  ObDtlChannelLoop *channel_loop_;
  ObDtlFlowControl *dfc_;
  bool first_recv_msg_;
  bool channel_is_eof_;
  //为了验证每个query信息一致，添加申请和释放需要一致
  int64_t alloc_buffer_cnt_;
  int64_t free_buffer_cnt_;

  ObOpMetric metric_;

  uint64_t state_;
  // 标记此channel是否从中间结果管理器获取数据.
  // 目前在px单层dfo调度会使用.
  // 两层调度dfo时实时收发时, 不会使用此变量.
  bool use_interm_result_;
  int64_t batch_id_;
  bool is_px_channel_;
  bool ignore_error_;
  // for single dfo dispatch scene, the process of using intermediate results is at the rpc processor end
  // the add the ObRegisterDmInfo in dtl channel and send to processor for register check item into dm
  common::ObRegisterDmInfo register_dm_info_;

  int64_t loop_idx_;

  common::ObCompressorType compressor_type_;

  DTLChannelOwner owner_mod_;
  int64_t thread_id_;
  // choose new dtl channel sync or first buffer cache
  bool enable_channel_sync_;
  DtlChannelType channel_type_;

public:
  // ObDtlChannel is link base, so it add extra link
  // link data list
  void remove_self();
  void add_last(ObDtlChannel *node);
  virtual bool has_msg() { return false; }
  ObDtlChannel *prev_link_;
  ObDtlChannel *next_link_;
};

OB_INLINE void ObDtlChannel::remove_self()
{
  if (nullptr == prev_link_ || nullptr == next_link_) {
  } else {
    prev_link_->next_link_ = next_link_;
    next_link_->prev_link_ = prev_link_;
    prev_link_ = nullptr;
    next_link_ = nullptr;
  }
}

OB_INLINE uint64_t ObDtlChannel::get_id() const
{
  return id_;
}

OB_INLINE void ObDtlChannel::done()
{
  done_ = true;
}

OB_INLINE bool ObDtlChannel::is_done() const
{
  return done_;
}

OB_INLINE uint64_t ObDtlChannel::generate_id(uint64_t ch_cnt)
{
  //       channel ID
  // |    <16>     |      <28>     |     20
  //    server_id       timestamp     sequence
  // id重复的情况：
  // 单秒TPS直接超过2^20，可能出现重复.
  // 相同server id的机器在2^28时间以后才重启起来.
  //
  int64_t start_id = (common::ObTimeUtility::current_time() / 1000000) << 20;
  static volatile uint64_t sequence = start_id;
  const uint64_t svr_id = GCTX.server_id_;
  uint64_t ch_id = -1;
  if (1 < ch_cnt) {
    uint64_t org_ch_id = 0;
    do {
      org_ch_id = (sequence & 0x0000FFFFFFFFFFFF) | (svr_id << 48);
      ch_id = ((ATOMIC_AAF(&sequence, ch_cnt) & 0x0000FFFFFFFFFFFF) | (svr_id << 48));
    } while (ch_id < org_ch_id);
  } else {
    ch_id = ((ATOMIC_AAF(&sequence, 1) & 0x0000FFFFFFFFFFFF) | (svr_id << 48));
  }
  return ch_id;
}

OB_INLINE int64_t ObDtlChannel::pin()
{
  return ATOMIC_AAF(&pins_, 1);
}

OB_INLINE int64_t ObDtlChannel::unpin()
{
  return ATOMIC_SAF(&pins_, 1);
}

OB_INLINE int64_t ObDtlChannel::get_pins() const
{
  return ATOMIC_LOAD(&pins_);
}

OB_INLINE void ObDtlChannel::set_msg_watcher(ObDtlChannelWatcher &watcher)
{
  msg_watcher_ = &watcher;
}

OB_INLINE void ObDtlChannel::set_send_buffer_size(int64_t send_buffer_size)
{
  send_buffer_size_ = send_buffer_size;
}

OB_INLINE int64_t ObDtlChannel::alloc_buffer_count()
{
  return ATOMIC_AAF(&alloc_buffer_cnt_, 1);
}

OB_INLINE int64_t ObDtlChannel::free_buffer_count()
{
  return ATOMIC_AAF(&free_buffer_cnt_, 1);
}

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_CHANNEL_H */
