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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_BATCH_SENDER_H
#define OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_BATCH_SENDER_H

#include "share/ob_thread_pool.h"
#include "lib/net/ob_addr.h"
#include "ob_lcl_message.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "share/deadlock/ob_deadlock_detector_common_define.h"

namespace oceanbase
{
namespace share
{
namespace detector
{
class ObDeadLockDetectorMgr;

class ObLCLBatchSenderThread : public share::ObThreadPool
{
public:
  ObLCLBatchSenderThread(ObDeadLockDetectorMgr *mgr) :
  is_inited_(false),
  is_running_(false),
  total_record_time_(0),
  total_busy_time_(0),
  over_night_times_(0),
  mgr_(mgr) {}
  ~ObLCLBatchSenderThread() { destroy(); }
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();
public:
  int cache_msg(const ObDependencyResource &key,
                const ObLCLMessage &lcl_msg);
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_running), K_(total_record_time), K_(over_night_times));
private:
  class RemoveIfOp
  {
  public:
    RemoveIfOp(common::ObArray<ObLCLMessage> &list) : lcl_message_list_(list) {}
    bool operator()(const ObDependencyResource &, ObLCLMessage &);
  private:
    common::ObArray<ObLCLMessage> &lcl_message_list_;
  };
  class MergeOp
  {
  public:
    MergeOp(const ObLCLMessage &lcl_message) : lcl_message_(lcl_message) {}
    bool operator()(const ObDependencyResource &, ObLCLMessage &);
  private:
    const ObLCLMessage &lcl_message_;
  };
private:
  int64_t update_and_get_lcl_op_interval_();
  void record_summary_info_and_logout_when_necessary_(int64_t, int64_t, int64_t);
private:
  bool is_inited_;
  bool is_running_;
  int64_t total_record_time_;
  int64_t total_busy_time_;
  int64_t over_night_times_;
  ObDeadLockDetectorMgr* mgr_;
  common::ObLinearHashMap<ObDependencyResource, ObLCLMessage> lcl_msg_map_;
};

}
}
}
#endif