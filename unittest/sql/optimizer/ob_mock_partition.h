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

#ifndef _OB_MOCK_PARTITION_H_
#define _OB_MOCK_PARTITION_H_

#undef private
#undef protected
#include "ob_mock_partition_storage.h"
#define private public
#define protected public
#include "storage/ob_partition_group.h"
#include "storage/ob_pg_partition.h"

namespace test {

class MockObPGPartition : public oceanbase::storage::ObPGPartition {
public:
  MockObPGPartition()
  {}
  ~MockObPGPartition()
  {}
  virtual oceanbase::storage::ObIPartitionStorage* get_storage()
  {
    return &storage_;
  }

private:
  MockPartitionStorage storage_;
};

class MockPartition : public oceanbase::storage::ObPartitionGroup {
public:
  virtual oceanbase::storage::ObPartitionStorage* get_storage()
  {
    return NULL;
  }
  virtual int get_replica_state(oceanbase::storage::ObPartitionReplicaState& state)
  {
    state = oceanbase::storage::OB_NORMAL_REPLICA;
    return oceanbase::common::OB_SUCCESS;
  }
  virtual int get_pg_partition(
      const oceanbase::common::ObPartitionKey& pkey, oceanbase::storage::ObPGPartitionGuard& guard)
  {
    guard.set_pg_partition(pkey, &static_cast<oceanbase::storage::ObPGPartition&>(pg_partition_));
    return oceanbase::common::OB_SUCCESS;
  }
  virtual MockPartitionStorage& get_storage(const int64_t flag)
  {
    UNUSED(flag);
    return pg_partition_.storage_;
  }

private:
  MockObPGPartition pg_partition_;
};
}  // namespace test

#endif
