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

#ifndef OCEANBASE_OBSERVER_FAKE_PARTITION_MGR_H_
#define OCEANBASE_OBSERVER_FAKE_PARTITION_MGR_H_

#include "lib/container/ob_array.h"
#include "lib/thread/ob_async_task_queue.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_i_partition_storage.h"
#include "mockcontainer/mock_ob_partition_service.h"
#define private public
#include "storage/ob_replay_status.h"

namespace oceanbase {
namespace storage {

struct FakeMajorFreezePartition {
  common::ObPartitionKey pk_;
  int64_t frozen_version_;
  int64_t frozen_timestamp_;
  int64_t status_;
  TO_STRING_KV(K_(pk), K_(frozen_version), K_(frozen_timestamp), K_(status));
};

class FakeMajorFreezePartitionService;
class ObMajorFreezeTask : public share::ObAsyncTask {
public:
  ObMajorFreezeTask(FakeMajorFreezePartitionService* pt_service, ObIPSFreezeCb* cb,
      const common::ObPartitionKey& partition_key, const int64_t cmd, const int64_t frozen_version,
      const int64_t frozen_timestamp, const int err);
  virtual ~ObMajorFreezeTask()
  {}

  virtual int process();
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(ObMajorFreezeTask);
  }
  virtual share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

private:
  FakeMajorFreezePartitionService* pt_service_;
  ObIPSFreezeCb* cb_;
  common::ObPartitionKey partition_key_;
  int64_t cmd_;
  int64_t frozen_version_;
  int64_t frozen_timestamp_;
  int err_;  // if OB_SUCCESS, means do major_freeze op succeed
};

class FakeMajorFreezePartitionService : public MockObIPartitionService {
public:
  FakeMajorFreezePartitionService();
  int init();
  // use be test major freeze
  enum MajorFreezeTestMode {
    ALL_SUCCEED = 0,
    SOME_NOT_LEADER = 1,
    SOME_ERROR = 2,
    MIX = 3,
  };

  int add_partition(const common::ObPartitionKey& partition);
  int prepare_freeze(const obrpc::ObPartitionList& partitions, const int64_t frozen_version,
      const common::ObIArray<int64_t>& timestamps, ObIPSFreezeCb& freeze_cb);
  int commit_freeze(const obrpc::ObPartitionList& partitions, const int64_t frozen_version,
      const common::ObIArray<int64_t>& timestamps);
  int abort_freeze(const obrpc::ObPartitionList& partitions, const int64_t frozen_version,
      const common::ObIArray<int64_t>& timestamps);
  int set_freeze_status(
      const common::ObPartitionKey& pkey, const int64_t frozen_version, const int64_t major_freeze_status);
  int get_freeze_status(const common::ObPartitionKey& pkey, int64_t& frozen_version, int64_t& frozen_timestamp,
      int64_t& major_freeze_status);
  void set_mf_test_mode(MajorFreezeTestMode mode)
  {
    mf_test_mode_ = mode;
  }
  int gen_err(const int64_t i);
  FakeMajorFreezePartition* get_partition(const common::ObPartitionKey& pk);

private:
  bool inited_;
  ObIPSFreezeCb* major_freeze_cb_;
  common::ObArray<FakeMajorFreezePartition> partitions_;
  share::ObAsyncTaskQueue queue_;
  MajorFreezeTestMode mf_test_mode_;
};
}  // end namespace storage
}  // end namespace oceanbase
#endif
