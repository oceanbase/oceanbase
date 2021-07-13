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

#ifndef OCEANBASE_STORAGE_OB_TRANS_CHECKPOINT_WORKER_
#define OCEANBASE_STORAGE_OB_TRANS_CHECKPOINT_WORKER_

#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;

class ObTransCheckpointAdapter : public share::ObThreadPool {
public:
  ObTransCheckpointAdapter() : inited_(false), partition_service_(nullptr)
  {}
  ~ObTransCheckpointAdapter()
  {
    destroy();
  }
  int init(storage::ObPartitionService* ps);
  void destroy();
  int start();
  void stop();
  void wait();
  virtual void run1() override;

private:
  int scan_all_partitions_(int64_t& valid_user_part_count, int64_t& valid_inner_part_count);

private:
  bool inited_;
  storage::ObPartitionService* partition_service_;
};

class ObTransCheckpointWorker {
public:
  ObTransCheckpointWorker() : inited_(false)
  {}
  ~ObTransCheckpointWorker()
  {
    destroy();
  }
  int init(storage::ObPartitionService* ps);
  void destroy();
  int start();
  void stop();
  void wait();

private:
  static const int64_t MAX_CHECKPOINT_NUM = 2;

private:
  bool inited_;
  ObTransCheckpointAdapter checkpoint_adapters_[MAX_CHECKPOINT_NUM];
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TRANS_CHECKPOINT_WORKER_
