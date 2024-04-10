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

#ifndef OB_DTL_TASK_H
#define OB_DTL_TASK_H

#include <stdint.h>
#include <lib/ob_define.h>
#include <lib/net/ob_addr.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_array_serialization.h"

namespace oceanbase {

// forward declarations
namespace sql { namespace dtl {
class ObDtlChannel;
class ObDtlBasicChannel;
}}  // sql


namespace sql {
namespace dtl {

enum DTL_CHAN_TYPE { DTL_CT_LOCAL, DTL_CT_RPC };
enum DTL_CHAN_ROLE { DTL_CR_PUSHER, DTL_CR_PULLER };
enum DTL_CHAN_STATE { DTL_CS_RUN, DTL_CS_DRAINED, DTL_CS_UNREGISTER };

struct ObDtlChannelInfo {
  OB_UNIS_VERSION(1);
public:
  uint64_t chid_;
  // Local or RPC channel is used.
  DTL_CHAN_TYPE type_;
  // Peer address if use RPC channel.
  common::ObAddr peer_;
  // Describe role of this channel of this task. A typical task may
  // have some pusher(producer) channels and some puller(consumer)
  // channels whereas task at top only has puller and bottom task only
  // has pusher.
  DTL_CHAN_ROLE role_;
  uint64_t tenant_id_;
  // no need to serialize
  DTL_CHAN_STATE state_;

  TO_STRING_KV(K_(chid), K_(type), K_(peer), K_(role), K_(tenant_id), K(state_));
};

class ObDtlChSet
{
  OB_UNIS_VERSION(1);
public:
  static constexpr int64_t MAX_CHANS = 65536; // nearly unlimited
public:
  ObDtlChSet() : exec_addr_() {}
  ~ObDtlChSet() = default;
  int reserve(int64_t size) { return ch_info_set_.reserve(size); }
  void set_exec_addr(const common::ObAddr &addr) { exec_addr_ = addr; }
  const common::ObAddr &get_exec_addr() const { return exec_addr_; }
  int add_channel_info(const dtl::ObDtlChannelInfo &info);
  int get_channel_info(int64_t chan_idx, ObDtlChannelInfo &ci) const;
  int64_t count() const { return ch_info_set_.count(); }
  int assign(const ObDtlChSet &other);
  void reset() { ch_info_set_.reset(); }
  common::ObIArray<dtl::ObDtlChannelInfo> &get_ch_info_set() { return ch_info_set_; }
  TO_STRING_KV(K_(exec_addr), K_(ch_info_set));
protected:
  common::ObAddr exec_addr_;
  common::ObSEArray<dtl::ObDtlChannelInfo, 12> ch_info_set_;
};


class ObDtlExecServer
{
  OB_UNIS_VERSION(1);
public:
  ObDtlExecServer()
    : total_task_cnt_(0), exec_addrs_(), prefix_task_counts_()
  {}

  void reset()
  {
    total_task_cnt_ = 0;
    exec_addrs_.reset();
    prefix_task_counts_.reset();
  }
  int add_exec_addr(const common::ObAddr &exec_addr);
  int assign(const ObDtlExecServer &other);

  common::ObIArray<common::ObAddr> &get_all_exec_addr() { return exec_addrs_; }
  common::ObIArray<int64_t> &get_prefix_task_counts() { return prefix_task_counts_; }
  TO_STRING_KV(K_(total_task_cnt), K_(exec_addrs), K_(prefix_task_counts));
public:
  int64_t total_task_cnt_;
  common::ObSEArray<common::ObAddr, 8> exec_addrs_;
  // 表示每个sqc前置的task总数
  // eg:sqc workers:      [0-2], [3-5], [6,7]
  //    prefix taskcount: [0], [3], [6]
  // 即prefix_task_counts_[idx] + task_id就是这个sqc的某个task对应的全局task_id
  common::ObSEArray<int64_t, 8> prefix_task_counts_;
};

class ObDtlChTotalInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDtlChTotalInfo()
    : start_channel_id_(0), transmit_exec_server_(), receive_exec_server_(),
      channel_count_(0), tenant_id_(common::OB_INVALID_ID), is_local_shuffle_(false)
  {}
  int assign(const ObDtlChTotalInfo &other);
  void reset()
  {
    start_channel_id_ = 0;
    transmit_exec_server_.reset();
    receive_exec_server_.reset();
    channel_count_ = 0;
    tenant_id_ = 0;
  }
  bool is_valid() const
  {
    return transmit_exec_server_.exec_addrs_.count() <= transmit_exec_server_.total_task_cnt_ &&
    channel_count_ == transmit_exec_server_.total_task_cnt_ * receive_exec_server_.total_task_cnt_;
  }
  TO_STRING_KV(K_(start_channel_id),
              K_(transmit_exec_server),
              K_(receive_exec_server),
              K_(channel_count),
              K_(tenant_id),
              K_(is_local_shuffle));
public:
  int64_t start_channel_id_;
  ObDtlExecServer transmit_exec_server_;
  ObDtlExecServer receive_exec_server_;
  int64_t channel_count_;   // 理论上要等于 tranmit_total_task_cnt_ * receive_total_task_cnt_
  uint64_t tenant_id_;
  bool is_local_shuffle_;
};

class ObDtlTask
{
  OB_UNIS_VERSION(1);
  static constexpr int64_t MAX_CHANS = 128;
public:
  ObDtlTask();
  virtual ~ObDtlTask();

  void set_jobid(uint64_t jobid);
  void set_taskid(uint64_t taskid);
  int add_channel_info(const ObDtlChannelInfo &ci);

  ObDtlChannel *get_channel(int64_t chan_idx);
  ObDtlChannel *wait_any(int64_t timeout);

  void set_exec_addr(const common::ObAddr &addr);
  void set_submit_addr(const common::ObAddr &addr);
  const common::ObAddr &get_exec_addr() const;
  const common::ObAddr &get_submit_addr() const;

  TO_STRING_KV(K_(jobid));

protected:
  // Link channels with channel ID.
  // int link_chans();
  // Unlink channels linked before.
  // void unlink_chans();

protected:
  DISALLOW_COPY_AND_ASSIGN(ObDtlTask);

  uint64_t jobid_;
  uint64_t taskid_;
  common::ObAddr exec_addr_;
  common::ObAddr submit_addr_;
  ObDtlChannel *chans_[MAX_CHANS];
  ObDtlChannelInfo cis_[MAX_CHANS];
  int64_t chans_cnt_;
};

OB_INLINE ObDtlTask::ObDtlTask()
    : jobid_(0),
      taskid_(0),
      exec_addr_(),
      submit_addr_(),
      chans_(),
      chans_cnt_(0)
{
}

OB_INLINE ObDtlTask::~ObDtlTask()
{}

OB_INLINE void ObDtlTask::set_jobid(uint64_t jobid)
{
  jobid_ = jobid;
}

OB_INLINE void ObDtlTask::set_taskid(uint64_t taskid)
{
  taskid_ = taskid;
}

OB_INLINE int ObDtlTask::add_channel_info(const ObDtlChannelInfo &ci)
{
  int ret = common::OB_SUCCESS;
  if (chans_cnt_ < MAX_CHANS) {
    cis_[chans_cnt_++] = ci;
  } else {
    ret = common::OB_SIZE_OVERFLOW;
  }
  return ret;
}

OB_INLINE ObDtlChannel *ObDtlTask::get_channel(int64_t chan_idx)
{
  ObDtlChannel *chan = nullptr;
  if (chan_idx < MAX_CHANS) {
    chan = chans_[chan_idx];
  }
  return chan;
}

OB_INLINE void ObDtlTask::set_exec_addr(const common::ObAddr &addr)
{
  exec_addr_ = addr;
}

OB_INLINE const common::ObAddr &ObDtlTask::get_exec_addr() const
{
  return exec_addr_;
}

OB_INLINE void ObDtlTask::set_submit_addr(const common::ObAddr &addr)
{
  submit_addr_ = addr;
}

OB_INLINE const common::ObAddr &ObDtlTask::get_submit_addr() const
{
  return submit_addr_;
}

}  // dtl
}  // sql
}  // oceanbase


#endif /* OB_DTL_TASK_H */
