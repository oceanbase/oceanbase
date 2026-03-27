/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_TENANT_TASK_QUEUE_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_TENANT_TASK_QUEUE_H__

#include "lib/container/ob_ext_ring_buffer.h"       // ObExtendibleRingBuffer
#include "lib/thread/ob_thread_lease.h"             // ObThreadLease
#include "ob_log_part_trans_task.h"                 // ObLogEntryTask

namespace oceanbase
{
namespace libobcdc
{
//////////////////////////// ObLogTenantTaskQueue /////////////////////////

typedef common::ObExtendibleRingBuffer<ObLogEntryTask> LogEntryTaskQueue;

class ObLogTenant;
class ObLogTenantTaskQueue final
{
public:
  explicit ObLogTenantTaskQueue(ObLogTenant &host);
  ~ObLogTenantTaskQueue();

public:
  int init(const int64_t start_seq);
  void reset();

  ObLogTenant &get_host() { return host_; }

  bool acquire_lease() { return lease_.acquire(); }
  bool revoke_lease() {return lease_.revoke();}

  int push_log_entry_task(ObLogEntryTask *task);

  LogEntryTaskQueue &get_log_entry_task_queue() { return log_entry_task_queue_; }

  int64_t get_next_task_seq() const { return log_entry_task_queue_.begin_sn(); }
  int64_t get_log_entry_task_count() const
  {
    return log_entry_task_queue_.end_sn() - log_entry_task_queue_.begin_sn();
  }

  TO_STRING_KV("log_entry_task_count", get_log_entry_task_count(),
      "next_task_seq", get_next_task_seq());

private:
  bool inited_;
  ObLogTenant &host_;

  common::ObThreadLease lease_;    // Responsible for the state transition of the queue
  LogEntryTaskQueue log_entry_task_queue_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTenantTaskQueue);
};

}
}

#endif
