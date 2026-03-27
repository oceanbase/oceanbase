/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_GTS_TASK_QUEUE_
#define OCEANBASE_TRANSACTION_OB_GTS_TASK_QUEUE_

#include "ob_gts_define.h"
#include "share/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/hash/ob_link_hashmap.h"

namespace oceanbase
{
namespace transaction
{
class ObTsCbTask;

class ObGTSTaskQueue
{
public:
  ObGTSTaskQueue() : is_inited_(false), task_type_(INVALID_GTS_TASK_TYPE) {}
  ~ObGTSTaskQueue() { destroy(); }
  int init(const ObGTSCacheTaskType &type);
  void destroy();
  void reset();
  int foreach_task(const MonotonicTs srr,
                   const int64_t gts,
                   const MonotonicTs receive_gts_ts);
  int push(ObTsCbTask *task);
  int64_t get_task_count() const { return queue_.size(); }
  int gts_callback_interrupted(const int errcode, const share::ObLSID ls_id);
private:
  static const int64_t TOTAL_WAIT_TASK_NUM = 500 * 1000;
private:
  bool is_inited_;
  ObGTSCacheTaskType task_type_;
  common::ObLinkQueue queue_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_GTS_TASK_QUEUE_
