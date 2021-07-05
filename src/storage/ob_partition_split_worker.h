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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_SPLIT_WORKER_H_
#define OCEANBASE_STORAGE_OB_PARTITION_SPLIT_WORKER_H_

#include "share/ob_errno.h"
#include "common/ob_partition_key.h"
#include "storage/ob_partition_split.h"
#include "share/ob_partition_modify.h"
#include "lib/queue/ob_lighty_queue.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

class ObPartitionSplitTask {
public:
  ObPartitionSplitTask() : schema_version_(0), partition_pair_(), next_run_ts_(0)
  {}
  ~ObPartitionSplitTask()
  {}
  int init(const int64_t schema_version, const share::ObSplitPartitionPair& partition_pair);
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  const share::ObSplitPartitionPair& get_partition_pair() const
  {
    return partition_pair_;
  }
  void delay(const int64_t ts)
  {
    next_run_ts_ = common::ObTimeUtility::current_time() + ts;
  }
  bool need_run() const
  {
    return common::ObTimeUtility::current_time() >= next_run_ts_;
  }
  bool is_valid() const;
  void reset();

public:
  TO_STRING_KV(K_(schema_version), K_(partition_pair), K_(next_run_ts));

private:
  int64_t schema_version_;
  share::ObSplitPartitionPair partition_pair_;
  int64_t next_run_ts_;
};

class ObPartitionSplitTaskFactory {
public:
  static ObPartitionSplitTask* alloc();
  static void release(ObPartitionSplitTask* task);
};

class ObPartitionSplitWorker : public share::ObThreadPool {
public:
  ObPartitionSplitWorker() : is_inited_(false), partition_service_(NULL), queue_()
  {}
  ~ObPartitionSplitWorker()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();
  int push(ObPartitionSplitTask* task);

private:
  static const int64_t CHECK_PHYSICAL_SPLIT_PROGRESS_INTERVAL = 10 * 1000 * 1000;

private:
  int handle_(ObPartitionSplitTask* task);
  int push_(ObPartitionSplitTask* task);

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  common::ObLightyQueue queue_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionSplitWorker);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_SPLIT_WORKER_H_
