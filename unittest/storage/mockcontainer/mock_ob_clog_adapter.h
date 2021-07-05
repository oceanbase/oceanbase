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

#ifndef MOCK_OB_CLOG_ADAPTER_H_
#define MOCK_OB_CLOG_ADAPTER_H_

#include "storage/transaction/ob_clog_adapter.h"
#include "common/ob_clock_generator.h"
#include "common/ob_partition_key.h"
#include "common/ob_queue_thread.h"
#include "clog/ob_partition_log_service.h"

namespace oceanbase {
namespace transaction {

using namespace common;
using namespace clog;
using namespace storage;

struct MySubmitLogTask {
  MySubmitLogTask() : cb(NULL)
  {}
  ~MySubmitLogTask()
  {}
  ObITransSubmitLogCb* cb;
  ObPartitionKey partition;
};

class MockObClogAdapter : public ObIClogAdapter, public ObSimpleThreadPool {
public:
  MockObClogAdapter()
  {}
  ~MockObClogAdapter()
  {
    destroy();
  }
  int init(ObPartitionService* partition_service, const ObAddr& self)
  {
    int ret = OB_SUCCESS;

    UNUSED(partition_service);
    UNUSED(self);

    if (OB_SUCCESS != (ret = ObSimpleThreadPool::init(1, 100000))) {
      TRANS_LOG(WARN, "ObSimpleThreadPool init error", K(ret));
    }

    return ret;
  }

  virtual int init(storage::ObPartitionService* partition_service)
  {
    UNUSED(partition_service);
    return OB_SUCCESS;
  }

  virtual int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, ObITransSubmitLogCb* cb)
  {
    UNUSED(version);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = submit_log(partition, buff, size, cb))) {
      TRANS_LOG(WARN, "MockObClogAdapter submit log error", K(ret));
    }

    return ret;
  }

  int submit_log_task(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, ObITransSubmitLogCb* cb)
  {
    UNUSED(partition);
    UNUSED(version);
    UNUSED(buff);
    UNUSED(size);
    UNUSED(cb);
    return OB_SUCCESS;
  }

  int start()
  {
    return OB_SUCCESS;
  }
  int stop()
  {
    destroy();
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

  int get_status(const ObPartitionKey& partition, const int64_t ctx_ts, const bool check_election, int& clog_status,
      bool& in_changing_leader_windows, int64_t& leader_epoch)
  {
    UNUSED(partition);
    UNUSED(ctx_ts);
    UNUSED(check_election);
    UNUSED(clog_status);
    UNUSED(in_changing_leader_windows);
    UNUSED(leader_epoch);
    return OB_SUCCESS;
  }

  int submit_rollback_trans_task(ObTransCtx* ctx)
  {
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  int get_status_unsafe(const ObPartitionKey& partition, const int64_t ctx_ts, int& clog_status,
      bool& in_changing_leader_windows, int64_t& leader_epoch)
  {
    UNUSED(partition);
    UNUSED(ctx_ts);
    UNUSED(clog_status);
    UNUSED(in_changing_leader_windows);
    UNUSED(leader_epoch);
    return OB_SUCCESS;
  }

  int submit_log(const ObPartitionKey& partition, const char* buff, const int64_t size, ObITransSubmitLogCb* cb)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    int64_t log_type = storage::OB_LOG_UNKNOWN;
    int64_t tmp_idx = 0;
    MySubmitLogTask* task = NULL;

    if (OB_SUCCESS != (ret = serialization::decode_i64(buff, size, pos, &log_type))) {
      TRANS_LOG(WARN, "decode log type error", K(ret));
    } else if (OB_SUCCESS != (ret = serialization::decode_i64(buff, size, pos, &tmp_idx))) {
      TRANS_LOG(WARN, "decode index error", K(ret));
    } else {
      switch (log_type) {
        case OB_LOG_TRANS_REDO: {
          break;
        }
        case OB_LOG_TRANS_PREPARE: {
          ObTransPrepareLogHelper helper;
          ObTransPrepareLog log(helper);
          if (OB_SUCCESS != (ret = log.deserialize(buff, size, pos))) {
            CLOG_LOG(WARN, "decode prepare log error", K(ret));
          } else {
            CLOG_LOG(DEBUG, "log success", K(log_type), K(log));
          }
          break;
        }
        case OB_LOG_TRANS_COMMIT: {
          PartitionLogInfoArray partition_log_info_arr;
          ObTransCommitLog log(partition_log_info_arr);
          if (OB_SUCCESS != (ret = log.deserialize(buff, size, pos))) {
            CLOG_LOG(WARN, "decode commit log error", K(ret));
          } else {
            CLOG_LOG(DEBUG, "log success", K(log_type), K(log));
          }
          break;
        }
        case OB_LOG_TRANS_ABORT: {
          ObTransAbortLog log;
          if (OB_SUCCESS != (ret = log.deserialize(buff, size, pos))) {
            CLOG_LOG(WARN, "decode abort log error", K(ret));
          } else {
            CLOG_LOG(DEBUG, "log success", K(log_type), K(log));
          }
          break;
        }
        case OB_LOG_TRANS_CLEAR: {
          ObTransClearLog log;
          if (OB_SUCCESS != (ret = log.deserialize(buff, size, pos))) {
            CLOG_LOG(WARN, "decode clear log error", K(ret));
          } else {
            CLOG_LOG(DEBUG, "log success", K(log_type), K(log));
          }
          break;
        }
        default: {
          break;
        }
      }

      if (NULL == (task = new MySubmitLogTask)) {
        TRANS_LOG(WARN, "new MySubmitLogTask error");
        ret = OB_ERR_UNEXPECTED;
      } else {
        task->cb = cb;
        task->partition = partition;
        if (OB_SUCCESS != (ret = push(task))) {
          TRANS_LOG(WARN, "push task error", K(ret));
        } else {
          TRANS_LOG(INFO, "push task success");
        }
      }
    }

    return ret;
  }

  void handle(void* task)
  {
    MySubmitLogTask* log_task = static_cast<MySubmitLogTask*>(task);
    log_task->cb->on_success(log_task->partition, 1, ObClockGenerator::getClock());
    TRANS_LOG(INFO, "handle log task sucess");
    delete log_task;
  }
  bool can_start_trans()
  {
    return true;
  }

  int batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const clog::ObLogInfoArray& log_info_array, const clog::ObISubmitLogCbArray& cb_array)
  {
    UNUSED(trans_id);
    UNUSED(partition_array);
    UNUSED(log_info_array);
    UNUSED(cb_array);
    return common::OB_SUCCESS;
  }
};

}  // namespace transaction
}  // namespace oceanbase

#endif
