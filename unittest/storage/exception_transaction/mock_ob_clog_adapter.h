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

#ifndef OB_TEST_OB_TEST_MOCK_CLOG_ADAPTER_H_
#define OB_TEST_OB_TEST_MOCK_CLOG_ADAPTER_H_

#include "common/ob_queue_thread.h"
#include "clog/ob_partition_log_service.h"
#include "storage/transaction/ob_clog_adapter.h"
#include "../mockcontainer/mock_ob_partition.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "../../clog/mock_ob_partition_log_service.h"

namespace oceanbase {
using namespace clog;
namespace transaction {
class ObITransSubmitLogCb;
}
namespace unittest {
class LogServiceSubmitTask {
public:
  LogServiceSubmitTask() : cb_(NULL)
  {}
  ~LogServiceSubmitTask()
  {}
  void set_submit_cb(transaction::ObITransSubmitLogCb* cb)
  {
    cb_ = cb;
  }
  transaction::ObITransSubmitLogCb* get_submit_cb()
  {
    return cb_;
  }
  void set_partition_key(const ObPartitionKey& partition_key)
  {
    partition_key_ = partition_key;
  }
  ObPartitionKey& get_partition_key()
  {
    return partition_key_;
  }

private:
  transaction::ObITransSubmitLogCb* cb_;
  ObPartitionKey partition_key_;
};

class MockObClogAdapter : public transaction::ObIClogAdapter, public ObSimpleThreadPool {
public:
  MockObClogAdapter()
  {
    ObSimpleThreadPool::init(1, 10000);
  }
  ~MockObClogAdapter()
  {
    destroy();
  }
  int start()
  {
    return OB_SUCCESS;
  }
  int stop()
  {
    return OB_SUCCESS;
  }
  int wait()
  {
    return OB_SUCCESS;
  }
  void destroy()
  {
    ObSimpleThreadPool::destroy();
  }
  int init(storage::ObPartitionService* partition_service)
  {
    UNUSED(partition_service);
    return OB_SUCCESS;
  }
  int get_status(const common::ObPartitionKey& partition, const int64_t ctx_ts, const bool check_election,
      int& clog_status, bool& in_changing_leader_windows)
  {
    UNUSED(partition);
    UNUSED(ctx_ts);
    UNUSED(check_election);
    UNUSED(in_changing_leader_windows);
    clog_status = OB_SUCCESS;
    return OB_SUCCESS;
  }
  int get_status_unsafe(
      const common::ObPartitionKey& partition, const int64_t ctx_ts, int& clog_status, bool& in_changing_leader_windows)
  {
    UNUSED(partition);
    UNUSED(ctx_ts);
    UNUSED(clog_status);
    UNUSED(in_changing_leader_windows);
    return OB_SUCCESS;
  }
  int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buf,
      const int64_t size, transaction::ObITransSubmitLogCb* cb)
  {
    UNUSED(version);
    UNUSED(buf);
    UNUSED(size);
    int ret = OB_SUCCESS;

    LogServiceSubmitTask* submit_task = new LogServiceSubmitTask();
    if (NULL == submit_task) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      submit_task->set_submit_cb(cb);
      submit_task->set_partition_key(partition);
      push(submit_task);
    }

    return ret;
  }
  int submit_log_task(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, transaction::ObITransSubmitLogCb* cb)
  {
    UNUSED(partition);
    UNUSED(version);
    UNUSED(buff);
    UNUSED(size);
    UNUSED(cb);
    return OB_SUCCESS;
  }
  void handle(void* task)
  {
    UNUSED(task);

    TRANS_LOG(INFO, "MockObClogAdapter handle");
    LogServiceSubmitTask* submit_task = static_cast<LogServiceSubmitTask*>(task);
    const uint64_t log_id = 1;
    const uint64_t trans_version = 2;
    const ObPartitionKey& partition_key = submit_task->get_partition_key();
    submit_task->get_submit_cb()->on_success(partition_key, log_id, trans_version);
    delete submit_task;
  }
  bool can_start_trans()
  {
    return true;
  }
};

}  // namespace unittest
}  // namespace oceanbase

#endif
