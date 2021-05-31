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

#define USING_LOG_PREFIX SHARE_PT

#include <gtest/gtest.h>
#include "share/partition_table/ob_inmemory_partition_table.h"
#include "fake_part_property_getter.h"
#include "../../rootserver/fake_rs_list_change_cb.h"
#include "share/config/ob_server_config.h"
namespace oceanbase {
namespace share {
using namespace common;
using namespace host;

static uint64_t TID = FakePartPropertyGetter::TID();
const static int64_t PID = FakePartPropertyGetter::PID();

#define FIND(p, s)                                                   \
  ({                                                                 \
    const ObPartitionReplica* __r = NULL;                            \
    int __ret = p.find(s, __r);                                      \
    ASSERT_TRUE(OB_SUCCESS == __ret || OB_ENTRY_NOT_EXIST == __ret); \
    __r;                                                             \
  })

TEST(TestInMemoryPartitionTable, common)
{
  FakeRsListChangeCb cb;
  TID = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  FakePartPropertyGetter property;
  ObInMemoryPartitionTable pt(property);
  ASSERT_EQ(OB_SUCCESS, pt.init(cb));
  property.clear().add(A, LEADER).add(B, FOLLOWER);

  ASSERT_EQ(OB_SUCCESS, property.update_all(pt));

  ObPartitionInfo partition;
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);

  // check is_original_leader
  ASSERT_FALSE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);
  ASSERT_EQ(OB_SUCCESS, pt.set_original_leader(TID, PID, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_TRUE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);

  // check set_unit_id
  ASSERT_EQ(OB_SUCCESS, pt.set_unit_id(TID, PID, A, 10));
  ASSERT_EQ(OB_SUCCESS, pt.set_unit_id(TID, PID, B, 11));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(10UL, partition.get_replicas_v2().at(0).unit_id_);
  ASSERT_EQ(11UL, partition.get_replicas_v2().at(1).unit_id_);

  // update rebuild flag
  ASSERT_EQ(OB_SUCCESS, pt.update_rebuild_flag(TID, PID, A, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_TRUE(FIND(partition, A)->rebuild_);
  ASSERT_FALSE(FIND(partition, B)->rebuild_);
  ASSERT_EQ(OB_SUCCESS, pt.update_rebuild_flag(TID, PID, B, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_TRUE(FIND(partition, B)->rebuild_);
  ASSERT_EQ(OB_SUCCESS, pt.update_rebuild_flag(TID, PID, A, false));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_FALSE(FIND(partition, A)->rebuild_);
  // non exist replica
  ASSERT_EQ(OB_SUCCESS, pt.update_rebuild_flag(TID, PID, C, true));
  ASSERT_EQ(OB_SUCCESS, pt.update_rebuild_flag(TID, PID, C, false));

  ASSERT_EQ(OB_SUCCESS, pt.set_unit_id(TID, PID, B, 11));

  // remove exist leader
  ASSERT_EQ(OB_SUCCESS, pt.remove(TID, PID, A));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);

  // remove non exist replica
  ASSERT_EQ(OB_SUCCESS, pt.remove(TID, PID, C));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());

  // remove exist follower
  ASSERT_EQ(OB_SUCCESS, pt.remove(TID, PID, B));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(0, partition.replica_count());
}

TEST(TestInMemoryPartitionTable, to_leader_time)
{
  FakeRsListChangeCb cb;
  TID = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  FakePartPropertyGetter property;
  ObInMemoryPartitionTable pt(property);
  ;
  ASSERT_EQ(OB_SUCCESS, pt.init(cb));
  property.clear().add(A, LEADER).add(B, FOLLOWER);

  ASSERT_EQ(OB_SUCCESS, property.update_all(pt));

  property.clear().add(A, FOLLOWER).add(B, FOLLOWER);

  ASSERT_EQ(OB_SUCCESS, property.update_all(pt));

  ObPartitionInfo partition;
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_GT(FIND(partition, A)->to_leader_time_, 0);
  ASSERT_EQ(0, FIND(partition, B)->to_leader_time_);

  property.clear().add(A, FOLLOWER).add(B, LEADER);

  ASSERT_EQ(OB_SUCCESS, property.update_all(pt));

  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_GT(FIND(partition, A)->to_leader_time_, 0);
  ASSERT_GT(FIND(partition, B)->to_leader_time_, 0);
  ASSERT_GE(FIND(partition, B)->to_leader_time_, FIND(partition, A)->to_leader_time_);
}

TEST(TestInMemoryPartitionTable, leader_update)
{
  FakeRsListChangeCb cb;

  TID = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);

  FakePartPropertyGetter property;
  ObInMemoryPartitionTable pt(property);
  ASSERT_EQ(OB_SUCCESS, pt.init(cb));

  ObPartitionInfo partition;

  property.clear().add(A, LEADER);
  ObPartitionReplica r;
  r.assign(property.get_replicas().at(0));
  property.clear();

  // add partition not exist on storage
  GCONF.self_addr_ = r.server_;
  ASSERT_EQ(OB_SUCCESS, pt.update(r));
  ASSERT_EQ(OB_SUCCESS, pt.remove(TID, PID, A));

  // add leader as follower
  property.clear().add(A, FOLLOWER);
  r.assign(property.get_replicas().at(0));
  r.role_ = LEADER;
  GCONF.self_addr_ = r.server_;
  ASSERT_EQ(OB_SUCCESS, pt.update(r));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(FOLLOWER, partition.get_replicas_v2().at(0).role_);
  GCONF.self_addr_ = r.server_;

  ASSERT_EQ(OB_SUCCESS, pt.update(r));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(FOLLOWER, partition.get_replicas_v2().at(0).role_);

  // leader's data_version must greater than 0
  property.clear().add(A, LEADER);
  r.assign(property.get_replicas().at(0));
  r.role_ = LEADER;
  r.data_version_ = 0;
  GCONF.self_addr_ = r.server_;
  ASSERT_NE(OB_SUCCESS, pt.update(r));

  // make sure only one leader
  property.clear().add(A, FOLLOWER).add(B, FOLLOWER).add(C, LEADER).add(D, FOLLOWER);
  for (int64_t i = 0; i < property.get_replicas().count(); i++) {
    GCONF.self_addr_ = property.get_replicas().at(i).server_;
    ASSERT_EQ(OB_SUCCESS, pt.update(property.get_replicas().at(i)));
  }
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(4, partition.replica_count());
  ASSERT_TRUE(NULL != FIND(partition, C));
  ASSERT_TRUE(LEADER == FIND(partition, C)->role_);

  property.clear().add(A, FOLLOWER).add(B, LEADER).add(C, FOLLOWER);

  // update leader B:
  //   change C to follower
  //   change B to leader
  GCONF.self_addr_ = property.get_replicas().at(1).server_;
  ASSERT_EQ(OB_SUCCESS, pt.update(property.get_replicas().at(1)));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(4, partition.replica_count());
  ASSERT_TRUE(NULL != FIND(partition, A));
  ASSERT_TRUE(NULL != FIND(partition, B) && LEADER == FIND(partition, B)->role_);
  ASSERT_TRUE(NULL != FIND(partition, C) && LEADER != FIND(partition, C)->role_);
  ASSERT_TRUE(NULL != FIND(partition, D));
  ASSERT_EQ(REPLICA_STATUS_OFFLINE, FIND(partition, D)->replica_status_);
}

TEST(TestInMemoryPartitionTable, log_unit_id)
{
  FakeRsListChangeCb cb;
  TID = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);

  FakePartPropertyGetter property;
  ObInMemoryPartitionTable pt(property);
  ASSERT_EQ(OB_SUCCESS, pt.init(cb));

  ObPartitionInfo partition;
  property.clear().add_flag_replica(A, "zone1", 1);
  ASSERT_EQ(OB_SUCCESS, pt.update(property.get_replicas().at(0)));
  property.clear().add(A, FOLLOWER, "zone1");
  ASSERT_EQ(OB_SUCCESS, pt.update(property.get_replicas().at(0)));
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(1, partition.get_replicas_v2().at(0).unit_id_);
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
