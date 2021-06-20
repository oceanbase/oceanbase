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

#ifndef OCEANBASE_STORAGE_CLOG_CB_ASYNC_WORKER
#define OCEANBASE_STORAGE_CLOG_CB_ASYNC_WORKER

#include "common/ob_queue_thread.h"
#include "lib/ob_define.h"
#include "common/ob_partition_key.h"
#include "storage/ob_storage_log_type.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

struct ObCLogCallbackAsyncTask {
  ObCLogCallbackAsyncTask()
      : pg_key_(),
        partition_key_(),
        log_type_(ObStorageLogType::OB_LOG_UNKNOWN),
        log_id_(common::OB_INVALID_ID),
        is_physical_drop_(false)
  {}

  OB_INLINE bool is_valid() const
  {
    return pg_key_.is_valid() && partition_key_.is_valid() && ObStorageLogType::OB_LOG_UNKNOWN != log_type_ &&
           common::OB_INVALID_ID != log_id_;
  }

  OB_INLINE void deep_copy(const ObCLogCallbackAsyncTask& that)
  {
    if (&that != this) {
      pg_key_ = that.pg_key_;
      partition_key_ = that.partition_key_;
      log_type_ = that.log_type_;
      log_id_ = that.log_id_;
      is_physical_drop_ = that.is_physical_drop_;
    }
  }

  TO_STRING_KV(K(pg_key_), K(partition_key_), K(log_type_), K(log_id_), K(is_physical_drop_));

public:
  common::ObPGKey pg_key_;
  common::ObPartitionKey partition_key_;
  int64_t log_type_;
  uint64_t log_id_;
  bool is_physical_drop_;  // used by offline log
};

class ObCLogCallbackAsyncWorker : public common::M2SQueueThread {
public:
  ObCLogCallbackAsyncWorker() : is_inited_(false), ptt_svr_(nullptr), free_queue_(), tasks_(nullptr){};
  ~ObCLogCallbackAsyncWorker()
  {
    destroy();
  };

  int init(ObPartitionService* ptt_svr);
  void destroy();
  virtual void handle(void* task, void* pdata);
  int push_task(const ObCLogCallbackAsyncTask& task);

private:
  int get_task(ObCLogCallbackAsyncTask*& task);
  void free_task(ObCLogCallbackAsyncTask* task);
  DISALLOW_COPY_AND_ASSIGN(ObCLogCallbackAsyncWorker);

private:
  static const int64_t LONG_RETRY_INTERVAL = 100 * 1000;  // 100ms
  static const int64_t RETRY_INTERVAL = 2 * 1000;         // 2ms
  static const int64_t MAX_TASK_NUM = common::OB_MAX_PARTITION_NUM_PER_SERVER;
  static const int64_t IDLE_INTERVAL = INT64_MAX;

  bool is_inited_;
  ObPartitionService* ptt_svr_;
  common::ObFixedQueue<ObCLogCallbackAsyncTask> free_queue_;
  ObCLogCallbackAsyncTask* tasks_;
};

}  // namespace storage
}  // namespace oceanbase
#endif
