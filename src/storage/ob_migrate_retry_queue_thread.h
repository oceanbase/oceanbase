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

#ifndef OCEANBASE_STORAGE_MIGRATE_RETRY_QUEUE_THREAD_
#define OCEANBASE_STORAGE_MIGRATE_RETRY_QUEUE_THREAD_

#include "common/ob_partition_key.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

enum ObMigRetryTaskType {
  INVALID_RETRY_TYPE = 0,
  RETRY_REBUILD,
  RETRY_STANDBY_RESTORE,
};

struct ObMigrateRetryTask {
  common::ObPartitionKey pkey_;
  int32_t task_type_;

  ObMigrateRetryTask() : pkey_(), task_type_(INVALID_RETRY_TYPE)
  {}
  ~ObMigrateRetryTask()
  {}

  bool is_valid() const
  {
    return (pkey_.is_valid() && INVALID_RETRY_TYPE != task_type_);
  }

  TO_STRING_KV(N_KEY, pkey_, "task_type", task_type_);
};

class ObMigrateRetryQueueThread : public lib::TGTaskHandler {
public:
  static const int64_t QUEUE_THREAD_NUM = 4;
  static const int64_t MINI_MODE_QUEUE_THREAD_NUM = 2;
  ObMigrateRetryQueueThread();
  virtual ~ObMigrateRetryQueueThread();

public:
  virtual int init(ObPartitionService* partition_service, int tg_id);
  virtual int push(const ObMigrateRetryTask* task);
  virtual void handle(void* task);
  virtual void destroy();
  int get_tg_id() const
  {
    return tg_id_;
  }

private:
  int get_task(ObMigrateRetryTask*& task);
  void free_task(ObMigrateRetryTask* task);

private:
  bool inited_;
  ObPartitionService* partition_service_;
  common::ObFixedQueue<ObMigrateRetryTask> free_queue_;
  ObMigrateRetryTask* tasks_;
  int tg_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateRetryQueueThread);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MIGRATE_RETRY_QUEUE_THREAD_
