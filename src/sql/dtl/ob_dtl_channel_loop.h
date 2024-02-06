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

#ifndef OB_DTL_CHANNEL_LOOP_H
#define OB_DTL_CHANNEL_LOOP_H

#include <cstdint>
#include <functional>
#include "lib/lock/ob_scond.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl_processor.h"
#include "sql/dtl/ob_dtl_channel_watcher.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_local_channel.h"
#include "sql/dtl/ob_dtl_local_first_buffer_manager.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlChannelLoop
    : public ObDtlChannelWatcher {
private:
  static constexpr auto MAX_PROCS = (int64_t)(ObDtlMsgType::MAX);
  using Proc = ObDtlPacketProcBase;
  using InterruptProc = ObDtlInterruptProc;
  using PredFunc = std::function<bool(int64_t, ObDtlChannel *)>;

  class ObDtlChannelLoopProc : public ObIDtlChannelProc
  {
  public:
    ObDtlChannelLoopProc(uint16_t &last_msg_type,  Proc **proc_map)
      : last_msg_type_(last_msg_type), proc_map_(proc_map)
    {}
    virtual int process(const ObDtlLinkedBuffer &, bool &transferred) override;

    uint16_t &last_msg_type_;
    Proc **proc_map_;
  };

  typedef ObDtlLocalChannel ObDtlMockChannel;
public:
  ObDtlChannelLoop();
  ObDtlChannelLoop(ObMonitorNode &op_monitor_info);
  virtual ~ObDtlChannelLoop() = default;

  void notify(ObDtlChannel &chan) override;
  virtual int has_first_buffer(uint64_t chan_id, bool &has_first_buffer) override;
  virtual int set_first_buffer(uint64_t chan_id) override;

  void reset();
  OB_INLINE ObDtlChannelLoop &register_interrupt_processor(InterruptProc &proc);
  OB_INLINE ObDtlChannelLoop &register_processor(Proc &proc);
  OB_INLINE ObDtlChannelLoop &register_channel(ObDtlChannel &chan);
  int unregister_channel(ObDtlChannel &chan);
  int unregister_all_channel();
  int process_any(int64_t timeout = 0);
  virtual int process_one(int64_t &nth_channel, int64_t timeout = 0);
  virtual int process_one_if(ObIDltChannelLoopPred *proc, int64_t &nth_channel, int64_t timeout = 0);
  ObDtlMsgType get_last_msg_type() const { return static_cast<ObDtlMsgType>(last_msg_type_); }
  int64_t get_channel_count() const { return chans_.count(); }
  ObDtlChannel *get_channel(const int64_t idx)
  {
    return idx >= 0 && idx <= chans_.count() ? chans_.at(idx) : NULL;
  }
  void destroy() { reset(); }
  int find(ObDtlChannel* ch, int64_t &out_idx);
  void ignore_interrupt() { ignore_interrupt_ = true; }
  void clear_all_proc() {
    for (int64_t i = 0; i < MAX_PROCS; ++i) {
      proc_map_[i] = nullptr;
    }
    interrupt_proc_ = nullptr;
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id;}
  void set_first_buffer_cache(dtl::ObDtlLocalFirstBufferCache *proxy_first_buffer_cache)
  { proxy_first_buffer_cache_ = proxy_first_buffer_cache; }

  void reset_eof_cnt() { eof_channel_cnt_ = 0; }
  void inc_eof_cnt() { eof_channel_cnt_ += 1; }
  int64_t get_eof_cnt() { return eof_channel_cnt_; }
  bool all_eof(const int64_t data_channel_cnt) const
  {
    return eof_channel_cnt_ >= data_channel_cnt;
  }
  int64_t get_process_query_time() const { return process_query_time_; }
  void set_process_query_time(int64_t process_query_time) { process_query_time_ = process_query_time; }
  int64_t get_query_timeout_ts() const { return query_timeout_ts_; }
  void set_query_timeout_ts(int64_t time) { query_timeout_ts_ = time; }
private:
  int process_channels(ObIDltChannelLoopPred *pred, int64_t &nth_channel);
  int process_channel(int64_t &nth_channel);
  int process_base(ObIDltChannelLoopPred *pred, int64_t &hinted_channel, int64_t timeout);
  inline void begin_wait_time_counting()
  {
    begin_wait_time_ = rdtsc();
  }
  inline void end_wait_time_counting()
  {
    op_monitor_info_.block_time_ += rdtsc() - begin_wait_time_;
  }
public:
  int unblock_channels(int64_t data_channel_idx);
  int unblock_channel(int64_t start_data_channel_idx, int64_t dfc_channel_idx);
  void add_last_data_list(ObDtlChannel *ch);
  virtual void remove_data_list(ObDtlChannel *ch, bool force) override;
  void add(ObDtlChannel *prev, ObDtlChannel *node, ObDtlChannel *next);
  virtual void set_first_no_data(ObDtlChannel *ch) override
  {
    UNUSED(ch);
    ++n_first_no_data_;
  }
  void set_interm_result(bool flag) { use_interm_result_ = flag; }
private:
  static const int64_t INTERRUPT_CHECK_TIMES = 16;
  static const int64_t SERVER_ALIVE_CHECK_TIMES = 4096;
  Proc *proc_map_[MAX_PROCS];
  InterruptProc *interrupt_proc_;
  common::ObSEArray<ObDtlChannel*, 128> chans_;
  int64_t next_idx_;
  uint16_t last_msg_type_;
  common::SimpleCond cond_; // 1对1的唤醒模式，用SimpleCond即可
  bool ignore_interrupt_;
  uint64_t tenant_id_;
  int64_t timeout_;
  dtl::ObDtlLocalFirstBufferCache *proxy_first_buffer_cache_;

  // list hold channels that has msg
  ObSpinLock spin_lock_;
  ObAddr mock_addr_;
  ObDtlMockChannel sentinel_node_;
  int64_t n_first_no_data_;
  ObMonitorNode default_op_monitor_info_; // used by sqc, das module
  ObMonitorNode &op_monitor_info_;
  bool first_data_get_;
  ObDtlChannelLoopProc process_func_;
  bool use_interm_result_;
  int64_t eof_channel_cnt_;
  int64_t loop_times_;
  int64_t begin_wait_time_; // use rdtsc to record begin dtl wait time
  int64_t process_query_time_;
  int64_t last_dump_channel_time_; // Used to track long-term active channels
  int64_t query_timeout_ts_;
};

OB_INLINE void ObDtlChannelLoop::add_last_data_list(ObDtlChannel *ch)
{
  ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
  if (nullptr == ch->next_link_ && nullptr == ch->prev_link_) {
    add(sentinel_node_.prev_link_, ch, &sentinel_node_);
  }
}

OB_INLINE void ObDtlChannelLoop::remove_data_list(ObDtlChannel *ch, bool force = false)
{
  if (OB_UNLIKELY(force)) {
    // 当unlink channel时去掉ch的prev_link和next_link，这样其他channel就避免了依赖
    ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
    ch->remove_self();
  } else {
    if (!ch->has_msg()) {
      ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
      if (!ch->has_msg()) {
        ch->remove_self();
      }
    }
  }
}

OB_INLINE void ObDtlChannelLoop::add(ObDtlChannel *prev, ObDtlChannel *node, ObDtlChannel *next)
{
  prev->next_link_ = node;
  node->prev_link_ = prev;
  next->prev_link_ = node;
  node->next_link_ = next;
}

OB_INLINE void ObDtlChannelLoop::reset()
{
  clear_all_proc();
  chans_.reset();
  next_idx_ = 0;
  last_msg_type_ = static_cast<uint16_t>(ObDtlMsgType::MAX);
  ignore_interrupt_ = false;
  n_first_no_data_ = 0;
  first_data_get_ = false;
  sentinel_node_.prev_link_ = &sentinel_node_;
  sentinel_node_.next_link_ = &sentinel_node_;
  eof_channel_cnt_ = 0;
}

OB_INLINE ObDtlChannelLoop &ObDtlChannelLoop::register_processor(Proc &proc)
{
  proc_map_[(int64_t)(proc.get_proc_type())] = &proc;
  return *this;
}

OB_INLINE ObDtlChannelLoop &ObDtlChannelLoop::register_interrupt_processor(InterruptProc &proc)
{
  interrupt_proc_ = &proc;
  return *this;
}

OB_INLINE ObDtlChannelLoop &ObDtlChannelLoop::register_channel(ObDtlChannel &chan)
{
  chans_.push_back(&chan);
  chan.set_loop_index(chans_.count() - 1);
  chan.set_msg_watcher(*this);
  chan.set_channel_loop(*this);
  return *this;
}

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_CHANNEL_LOOP_H */
