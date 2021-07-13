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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_WORKER_
#define OCEANBASE_STORAGE_OB_PARTITION_WORKER_

#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;

class ObPartitionWorker : public share::ObThreadPool {
  const int64_t DIVISION_PARTITION_CNT = 100;

public:
  ObPartitionWorker() : inited_(false), partition_service_(NULL)
  {}
  ~ObPartitionWorker()
  {
    destroy();
  }
  int init(storage::ObPartitionService* ps);
  void destroy();
  int start();
  void stop();
  void wait();

public:
  virtual void run1() override;

private:
  int scan_all_partitions_(int64_t& valid_user_part_count, int64_t& valid_inner_part_count);

private:
  bool inited_;
  storage::ObPartitionService* partition_service_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_WORKER_
