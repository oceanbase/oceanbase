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

class ObDtlChannelLoop : public ObDtlChannelWatcher {
private:
  static constexpr auto MAX_PROCS = (int64_t)(ObDtlMsgType::MAX);
  using Proc = ObDtlPacketProcBase;
  using InterruptProc = ObDtlInterruptProc;
  using PredFunc = std::function<bool(int64_t, ObDtlChannel*)>;

  class ObDtlChannelLoopProc : public ObIDtlChannelProc {
  public:
    ObDtlChannelLoopProc(uint16_t& last_msg_type, Proc** proc_map) : last_msg_type_(last_msg_type), proc_map_(proc_map)
    {}
    virtual int process(const ObDtlLinkedBuffer&, dtl::ObDtlMsgIterator*);

    uint16_t& last_msg_type_;
    Proc** proc_map_;
  };

  typedef ObDtlLocalChannel ObDtlMockChannel;

public:
  ObDtlChannelLoop();
  virtual ~ObDtlChannelLoop() = default;

  void notify(ObDtlChannel& chan) override;
  virtual int has_first_buffer(uint64_t chan_id, bool& has_first_buffer) override;
  virtual int set_first_buffer(uint64_t chan_id) override;

  void reset();
  OB_INLINE ObDtlChannelLoop& register_interrupt_processor(InterruptProc& proc);
  OB_INLINE ObDtlChannelLoop& register_processor(Proc& proc);
  OB_INLINE ObDtlChannelLoop& register_channel(ObDtlChannel& chan);
  int unregister_channel(ObDtlChannel& chan);
  int unregister_all_channel();
  int process_one(int64_t timeout);
  virtual int process_one(int64_t& nth_channel, int64_t timeout);
  virtual int process_one_if(ObIDltChannelLoopPred* proc, int64_t timeout, int64_t& nth_channel);
  ObDtlMsgType get_last_msg_type() const
  {
    return static_cast<ObDtlMsgType>(last_msg_type_);
  }
  int64_t get_channel_count() const
  {
    return chans_.count();
  }
  void destroy()
  {
    reset();
  }
  int find(ObDtlChannel* ch, int64_t& out_idx);
  void ignore_interrupt()
  {
    ignore_interrupt_ = true;
  }
  void clear_all_proc()
  {
    for (int64_t i = 0; i < MAX_PROCS; ++i) {
      proc_map_[i] = nullptr;
    }
    interrupt_proc_ = nullptr;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }

  void set_monitor_info(ObMonitorNode* op_monitor_info)
  {
    op_monitor_info_ = op_monitor_info;
  }
  void set_first_buffer_cache(dtl::ObDtlLocalFirstBufferCache* proxy_first_buffer_cache)
  {
    proxy_first_buffer_cache_ = proxy_first_buffer_cache;
  }

private:
  int process_channels(ObIDltChannelLoopPred* pred, int64_t& nth_channel);
  int process_channel(int64_t& nth_channel);
  int process_base(ObIDltChannelLoopPred* pred, int64_t& hinted_channel, int64_t timeout);

public:
  int unblock_channels(int64_t data_channel_idx);
  void add_last_data_list(ObDtlChannel* ch);
  virtual void remove_data_list(ObDtlChannel* ch, bool force) override;
  void add(ObDtlChannel* prev, ObDtlChannel* node, ObDtlChannel* next);
  virtual void set_first_no_data(ObDtlChannel* ch) override
  {
    UNUSED(ch);
    ++n_first_no_data_;
  }
  void set_interm_result(bool flag)
  {
    use_interm_result_ = flag;
  }

private:
  static const int64_t INTERRUPT_CHECK_TIMES = 16;
  Proc* proc_map_[MAX_PROCS];
  InterruptProc* interrupt_proc_;
  common::ObSEArray<ObDtlChannel*, 128> chans_;
  int64_t next_idx_;
  uint16_t last_msg_type_;
  common::SimpleCond cond_;  // one to one, SiimpleCond is good enough
  bool ignore_interrupt_;
  uint64_t tenant_id_;
  int64_t timeout_;
  dtl::ObDtlLocalFirstBufferCache* proxy_first_buffer_cache_;

  // list hold channels that has msg
  ObSpinLock spin_lock_;
  ObAddr mock_addr_;
  ObDtlMockChannel sentinel_node_;
  int64_t n_first_no_data_;
  ObMonitorNode* op_monitor_info_;
  bool first_data_get_;
  ObDtlChannelLoopProc process_func_;

  int64_t loop_times_;
  bool use_interm_result_;

public:
  int64_t time_recorder_;
};

OB_INLINE void ObDtlChannelLoop::add_last_data_list(ObDtlChannel* ch)
{
  ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
  if (nullptr == ch->next_link_ && nullptr == ch->prev_link_) {
    add(sentinel_node_.prev_link_, ch, &sentinel_node_);
  }
}

OB_INLINE void ObDtlChannelLoop::remove_data_list(ObDtlChannel* ch, bool force = false)
{
  if (OB_UNLIKELY(force)) {
    // when unlink channel, get rid of the prev_link and next_link
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

OB_INLINE void ObDtlChannelLoop::add(ObDtlChannel* prev, ObDtlChannel* node, ObDtlChannel* next)
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
  time_recorder_ = 0;
  n_first_no_data_ = 0;
  first_data_get_ = false;
  sentinel_node_.prev_link_ = &sentinel_node_;
  sentinel_node_.next_link_ = &sentinel_node_;
}

OB_INLINE ObDtlChannelLoop& ObDtlChannelLoop::register_processor(Proc& proc)
{
  proc_map_[(int64_t)(proc.get_proc_type())] = &proc;
  return *this;
}

OB_INLINE ObDtlChannelLoop& ObDtlChannelLoop::register_interrupt_processor(InterruptProc& proc)
{
  interrupt_proc_ = &proc;
  return *this;
}

OB_INLINE ObDtlChannelLoop& ObDtlChannelLoop::register_channel(ObDtlChannel& chan)
{
  chans_.push_back(&chan);
  chan.set_loop_index(chans_.count() - 1);
  chan.set_msg_watcher(*this);
  chan.set_channel_loop(*this);
  return *this;
}

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase

#endif /* OB_DTL_CHANNEL_LOOP_H */
