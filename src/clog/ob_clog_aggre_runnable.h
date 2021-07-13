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

#ifndef OCEANBASE_CLOG_OB_CLOG_AGGRE_RUNNABLE_
#define OCEANBASE_CLOG_OB_CLOG_AGGRE_RUNNABLE_

#include "ob_log_define.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {

class ObClogAggreRunnable;

class ObClogAggreTask {
public:
  ObClogAggreTask(ObClogAggreRunnable* host, storage::ObPartitionService* ps) : host_(host), ps_(ps), run_ts_(0)
  {}
  ~ObClogAggreTask()
  {}
  int set_partition_key(const common::ObPartitionKey& pkey);
  int set_run_ts(const int64_t run_ts);

public:
  void handle();
  uint64_t hash() const
  {
    return pkey_.hash();
  }
  TO_STRING_KV(K_(pkey));

private:
  common::ObPartitionKey pkey_;
  ObClogAggreRunnable* host_;
  storage::ObPartitionService* ps_;
  int64_t run_ts_;
};

class ObClogAggreRunnable : public lib::TGTaskHandler {
  friend class ObClogAggreTask;

public:
  ObClogAggreRunnable();
  virtual ~ObClogAggreRunnable();

public:
  int init(storage::ObPartitionService* partition_service);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  void handle(void* task);
  int add_task(const common::ObPartitionKey& pkey, const int64_t delay_us);

public:
  static const int64_t THREAD_COUNT = 4;
  static const int64_t MINI_THREAD_COUNT = 1;
  static const int64_t TOTAL_TASK = 256;

private:
  int push_back(ObClogAggreTask* task);

private:
  bool is_inited_;
  ObClogAggreTask* task_array_[TOTAL_TASK];
  int64_t available_index_;
  common::ObSpinLock lock_;
  int tg_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObClogAggreRunnable);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_CLOG_AGGRE_RUNNABLE_
