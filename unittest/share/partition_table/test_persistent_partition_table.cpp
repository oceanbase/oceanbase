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
#define private public
#include "lib/stat/ob_session_stat.h"
#include "share/schema/db_initializer.h"
#include "share/partition_table/ob_persistent_partition_table.h"
#undef private
#include "lib/allocator/page_arena.h"
#include "fake_part_property_getter.h"
#include "share/partition_table/ob_partition_table_proxy.h"
#include "share/ob_rpc_struct.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_server_struct.h"
#include "../../rootserver/fake_rs_list_change_cb.h"
namespace oceanbase {

using namespace obrpc;

namespace share {

using namespace common;
using namespace observer;
using namespace storage;
using namespace schema;
using namespace host;
static uint64_t& TEN = FakePartPropertyGetter::TEN();
static uint64_t& TID = FakePartPropertyGetter::TID();
const static int64_t PID = FakePartPropertyGetter::PID();

#define FIND(p, s)                                                   \
  ({                                                                 \
    const ObPartitionReplica* __r = NULL;                            \
    int __ret = p.find(s, __r);                                      \
    ASSERT_TRUE(OB_SUCCESS == __ret || OB_ENTRY_NOT_EXIST == __ret); \
    __r;                                                             \
  })

class FakePartitionService : public ObPartitionService {
public:
  FakePartitionService()
  {}
  virtual ~FakePartitionService()
  {}
  int get_role(const ObPartitionKey& pkey, ObRole& role) const
  {
    UNUSED(pkey);
    role = LEADER;
    return OB_SUCCESS;
  }
};

class TestPersistentPartitionTable : public ::testing::Test {
public:
  TestPersistentPartitionTable() : pt_(prop_getter_)
  {}

  virtual void SetUp();
  virtual void TearDown()
  {}

  void common_test(uint64_t table_id);
  void update_leader_test(uint64_t table_id);
  void to_leader_time_test(uint64_t table_id);

protected:
  DBInitializer db_initer_;
  FakePartPropertyGetter prop_getter_;
  ObPersistentPartitionTable pt_;
  FakePartitionService par_ser_;
};

void TestPersistentPartitionTable::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  GCTX.par_ser_ = &par_ser_;

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool use_sys_tenant = true;
  ret = pt_.init(db_initer_.get_sql_proxy(), NULL, use_sys_tenant);
  ASSERT_EQ(OB_SUCCESS, ret);

  TEN = 1;
  TID = 2;
}

void TestPersistentPartitionTable::common_test(uint64_t table_id)
{
  TID = table_id;
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add_flag_replica(D);

  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter_.get_replicas().at(i)));
  }

  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);

  ASSERT_LT(0, partition.get_replicas_v2().at(0).member_time_us_);
  ASSERT_LT(0, partition.get_replicas_v2().at(1).member_time_us_);

  // set unit id
  int ret = pt_.set_unit_id(TID, PID, A, 1024);
  ASSERT_EQ(OB_SUCCESS, ret);
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(1024, partition.get_replicas_v2().at(0).unit_id_);

  // update rebuild flag
  ASSERT_EQ(OB_SUCCESS, pt_.update_rebuild_flag(TID, PID, A, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_TRUE(FIND(partition, A)->rebuild_);
  ASSERT_FALSE(FIND(partition, B)->rebuild_);
  ASSERT_EQ(OB_SUCCESS, pt_.update_rebuild_flag(TID, PID, A, false));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_FALSE(FIND(partition, A)->rebuild_);

  // check is_original_leader
  ASSERT_FALSE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);
  ASSERT_EQ(OB_SUCCESS, pt_.set_original_leader(TID, PID, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_TRUE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);

  // remove exist leader
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, A));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);

  // remove non exist replica
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, C));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());

  // remove exist follower
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, B));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(0, partition.replica_count());

  // remove flag replica
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get_partition_info(TID, PID, false, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, D));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get_partition_info(TID, PID, false, partition));
  ASSERT_EQ(0, partition.replica_count());
}

TEST_F(TestPersistentPartitionTable, core_kv_batch_remove_replica)
{
  ObPartitionReplica replica;
  replica.partition_id_ = 0;
  replica.table_id_ = combine_id(1, 2);
  replica.partition_cnt_ = 1;
  replica.server_ = A;
  replica.role_ = FOLLOWER;
  replica.data_version_ = 1;
  replica.zone_ = "rongxuan";
  replica.unit_id_ = 1;
  replica.member_list_.push_back(ObPartitionReplica::Member(A, ::oceanbase::common::ObTimeUtility::current_time()));
  replica.member_list_.push_back(ObPartitionReplica::Member(B, ::oceanbase::common::ObTimeUtility::current_time() + 1));
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));
  replica.role_ = LEADER;
  replica.server_ = B;
  replica.to_leader_time_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_EQ(OB_SUCCESS, pt_.get(combine_id(1, 2), 0, partition));
  ASSERT_EQ(2, partition.replica_count());
  ObArray<ObPartitionReplica> array;
  ObPartitionReplica r;
  r.table_id_ = combine_id(1, 2);
  r.partition_id_ = 0;
  r.partition_cnt_ = 1;
  r.zone_ = "rongxuan";
  r.server_ = A;
  r.data_version_ = 1;
  r.is_remove_ = true;
  array.push_back(r);
  GCTX.self_addr_ = A;
  ASSERT_EQ(OB_SUCCESS, pt_.batch_execute(array));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get_partition_info(combine_id(1, 2), 0, true, partition));
  LOG_INFO("output replica", K(partition));
  ASSERT_EQ(1, partition.replica_count());
}

TEST_F(TestPersistentPartitionTable, core_kv_remove_replica)
{
  ObPartitionReplica replica;
  replica.partition_id_ = 0;
  replica.table_id_ = combine_id(1, 2);
  replica.partition_cnt_ = 1;
  replica.server_ = A;
  replica.role_ = FOLLOWER;
  replica.data_version_ = 1;
  replica.zone_ = "rongxuan";
  replica.unit_id_ = 1;
  replica.member_list_.push_back(ObPartitionReplica::Member(A, ::oceanbase::common::ObTimeUtility::current_time()));
  replica.member_list_.push_back(ObPartitionReplica::Member(B, ::oceanbase::common::ObTimeUtility::current_time() + 1));
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));
  replica.role_ = LEADER;
  replica.server_ = B;
  replica.to_leader_time_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_EQ(OB_SUCCESS, pt_.get(combine_id(1, 2), 0, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(OB_SUCCESS, pt_.remove(combine_id(1, 2), 0, A));
  replica.member_list_.reset();
  replica.member_list_.push_back(ObPartitionReplica::Member(B, ::oceanbase::common::ObTimeUtility::current_time() + 1));
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get_partition_info(combine_id(1, 2), 0, true, partition));
  LOG_INFO("output replica", K(partition));
  ASSERT_EQ(1, partition.replica_count());
}

TEST_F(TestPersistentPartitionTable, core_kv_update_flag_replica)
{
  ObPartitionReplica replica;
  replica.partition_id_ = 0;
  replica.table_id_ = combine_id(1, 2);
  replica.partition_cnt_ = 1;
  replica.server_ = A;
  replica.role_ = FOLLOWER;
  replica.data_version_ = -1;
  replica.zone_ = "z1";
  replica.unit_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));

  replica.unit_id_ = -1;
  replica.data_version_ = 1;
  replica.member_list_.push_back(ObPartitionReplica::Member(A, ::oceanbase::common::ObTimeUtility::current_time()));
  ASSERT_EQ(OB_SUCCESS, pt_.update(replica));

  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(combine_id(1, 2), 0, partition));
  ASSERT_EQ(1, partition.replica_count());
}

TEST_F(TestPersistentPartitionTable, common)
{
  common_test(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID));
  ASSERT_FALSE(HasFatalFailure());

  common_test(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_TID));
  ASSERT_FALSE(HasFatalFailure());

  common_test(combine_id(OB_SYS_TENANT_ID, OB_MIN_USER_TABLE_ID + 1));
  ASSERT_FALSE(HasFatalFailure());
}

void TestPersistentPartitionTable::to_leader_time_test(uint64_t table_id)
{
  TID = table_id;
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER);

  ASSERT_EQ(OB_SUCCESS, prop_getter_.update_all(pt_));

  prop_getter_.clear().add(A, FOLLOWER).add(B, FOLLOWER);

  ASSERT_EQ(OB_SUCCESS, prop_getter_.update_all(pt_));

  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));

  ASSERT_GT(FIND(partition, A)->to_leader_time_, 0);
  ASSERT_EQ(0, FIND(partition, B)->to_leader_time_);

  prop_getter_.clear().add(A, FOLLOWER).add(B, LEADER);

  ASSERT_EQ(OB_SUCCESS, prop_getter_.update_all(pt_));

  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));

  ASSERT_GT(FIND(partition, A)->to_leader_time_, 0);
  ASSERT_GT(FIND(partition, B)->to_leader_time_, 0);
  ASSERT_GE(FIND(partition, B)->to_leader_time_, FIND(partition, A)->to_leader_time_);
}

TEST_F(TestPersistentPartitionTable, to_leader_time)
{
  to_leader_time_test(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID));
  ASSERT_FALSE(HasFatalFailure());

  to_leader_time_test(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_TID));
  ASSERT_FALSE(HasFatalFailure());

  to_leader_time_test(combine_id(OB_SYS_TENANT_ID, OB_MIN_USER_TABLE_ID + 1));
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(TestPersistentPartitionTable, all_meta_table)
{
  TEN = 2;
  TID = OB_MIN_USER_TABLE_ID + 1;
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER);

  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter_.get_replicas().at(i)));
  }

  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);

  // remove exist leader
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, A));
  partition.get_replicas_v2().reuse();
  ASSERT_EQ(OB_SUCCESS, pt_.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);
}

void TestPersistentPartitionTable::update_leader_test(uint64_t table_id)
{
  TID = table_id;
  FakePartPropertyGetter& property = prop_getter_;
  ObPersistentPartitionTable& pt = pt_;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  property.clear().add(A, LEADER);
  ObPartitionReplica r;
  r.assign(property.get_replicas().at(0));
  property.clear();

  // add partition not exist on storage
  ASSERT_EQ(OB_SUCCESS, pt.update(r));
  ASSERT_EQ(OB_SUCCESS, pt.remove(TID, PID, A));

  // add leader as follower
  property.clear().add(A, FOLLOWER);
  r.assign(property.get_replicas().at(0));
  r.role_ = LEADER;
  r.to_leader_time_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, pt.update(r));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(FOLLOWER, partition.get_replicas_v2().at(0).role_);

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
  ASSERT_NE(OB_SUCCESS, pt.update(r));

  // remove non exist member && make sure only one leader
  property.clear().add(A, FOLLOWER).add(B, FOLLOWER).add(C, LEADER).add(D, FOLLOWER);
  for (int64_t i = 0; i < property.get_replicas().count(); i++) {
    ASSERT_EQ(OB_SUCCESS, pt.update(property.get_replicas().at(i)));
  }
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(4, partition.replica_count());
  ASSERT_TRUE(NULL != FIND(partition, C) && LEADER == FIND(partition, C)->role_);

  property.clear().add(A, FOLLOWER).add(B, LEADER).add(C, FOLLOWER);

  // update leader B:
  //   remove D
  //   change C to follower
  //   change B to leader
  ASSERT_EQ(OB_SUCCESS, pt.update(property.get_replicas().at(1)));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, pt.get(TID, PID, partition));
  ASSERT_EQ(4, partition.replica_count());
  ASSERT_TRUE(NULL != FIND(partition, A));
  ASSERT_TRUE(NULL != FIND(partition, B) && LEADER == FIND(partition, B)->role_);
  ASSERT_TRUE(NULL != FIND(partition, C) && LEADER != FIND(partition, C)->role_);
  ASSERT_TRUE(NULL != FIND(partition, D) && REPLICA_STATUS_OFFLINE == FIND(partition, D)->replica_status_);
}

TEST_F(TestPersistentPartitionTable, update_leader)
{
  update_leader_test(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID));
  ASSERT_FALSE(HasFatalFailure());
  update_leader_test(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_TID));
  ASSERT_FALSE(HasFatalFailure());
  update_leader_test(combine_id(OB_SYS_TENANT_ID, OB_MIN_USER_TABLE_ID + 1));
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(TestPersistentPartitionTable, invalid_argument)
{
  FakeMergeErrorCb cb;
  bool use_sys_tenant = true;
  ObPartitionTableProxyFactory factory(db_initer_.get_sql_proxy(), &cb, NULL, use_sys_tenant);
  ObPartitionTableProxy* p1 = NULL;
  ObPartitionTableProxy* p2 = NULL;
  ASSERT_EQ(OB_SUCCESS, factory.get_proxy(OB_ALL_ROOT_TABLE_TID, p1));
  ASSERT_EQ(OB_SUCCESS, factory.get_proxy(OB_ALL_META_TABLE_TID, p2));
  ObPartitionTableProxy* proxys[] = {p1, p2};

  for (int64_t i = 0; i < ARRAYSIZEOF(proxys); ++i) {
    ObPartitionTableProxy& proxy = *proxys[i];

    ObPartitionInfo info;
    ASSERT_NE(OB_SUCCESS, proxy.fetch_partition_info(true, 1, -1, false, info));

    ObPartitionReplica replica;
    ASSERT_NE(OB_SUCCESS, proxy.update_replica(replica, true));

    ASSERT_NE(OB_SUCCESS, proxy.set_to_follower_role(1, -1, ObAddr()));

    ASSERT_NE(OB_SUCCESS, proxy.remove(1, -1, ObAddr()));

    ASSERT_NE(OB_SUCCESS, proxy.set_unit_id(1, -1, ObAddr(), 1024));

    ASSERT_NE(OB_SUCCESS, proxy.set_original_leader(1, -1, true));
  }

  ObPersistentPartitionTable pt(prop_getter_);
  for (int64_t i = 0; i < 2; i++) {
    ObPartitionInfo info;
    ObPartitionReplica replica;

    ASSERT_NE(OB_SUCCESS, pt.get(1, -1, info));

    ASSERT_NE(OB_SUCCESS, pt.update(replica));

    ASSERT_NE(OB_SUCCESS, pt.remove(1, -1, ObAddr()));

    ASSERT_NE(OB_SUCCESS, pt.set_unit_id(1, -1, ObAddr(), 1024));

    ASSERT_NE(OB_SUCCESS, pt.set_original_leader(1, -1, true));

    if (0 == i) {
      ASSERT_EQ(OB_SUCCESS, pt.init(db_initer_.get_sql_proxy(), NULL, use_sys_tenant));
    } else {
      ASSERT_EQ(OB_INIT_TWICE, pt.init(db_initer_.get_sql_proxy(), NULL, use_sys_tenant));
    }
  }
}

TEST_F(TestPersistentPartitionTable, update_fresh_replica_version)
{
  ObNormalPartitionTableProxy pt_proxy(db_initer_.get_sql_proxy());
  pt_proxy.set_use_sys_tenant(true);
  TEN = 2;
  TID = 500001;
  int part_cnt = 20;
  ObArray<ObCreatePartitionArg> keys;
  ObPartitionReplica r;
  for (int i = 0; i < part_cnt; ++i) {
    ObPartitionKey key(combine_id(TEN, TID), i, part_cnt);
    ObCreatePartitionArg arg;
    arg.partition_key_ = key;
    ASSERT_EQ(OB_SUCCESS, keys.push_back(arg));
    ASSERT_EQ(OB_SUCCESS,
        ObIPartitionTable::gen_flag_replica(
            combine_id(TEN, TID), i, part_cnt, A, "z1", 1, REPLICA_TYPE_FULL, 100, -1, r));
    ASSERT_EQ(OB_SUCCESS, pt_proxy.update_replica(r, false));
  }
  r.partition_id_ = 10;
  r.data_version_ = 3;
  ASSERT_EQ(OB_SUCCESS, pt_proxy.update_replica(r, true));
  ASSERT_EQ(OB_SUCCESS, pt_proxy.update_fresh_replica_version(A, 88, keys, 2));
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  for (int i = 0; i < part_cnt; ++i) {
    ObPartitionInfo info;
    info.set_allocator(&allocator);

    ASSERT_EQ(OB_SUCCESS, pt_proxy.fetch_partition_info(false, combine_id(TEN, TID), i, false, info));
    ASSERT_EQ(i == 10 ? 3 : 2, info.replicas_.at(0).data_version_);
  }

  // partition belong to different tables
  ASSERT_EQ(OB_SUCCESS, keys.at(10).partition_key_.init(combine_id(TEN, TID + 1), 10, part_cnt));
  ASSERT_EQ(OB_INVALID_ARGUMENT, pt_proxy.update_fresh_replica_version(A, 88, keys, 2));

  // all root table partitions
  for (int i = 0; i < part_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, keys.at(i).partition_key_.init(combine_id(1, 10), i, part_cnt));
  }
  ASSERT_EQ(OB_INVALID_ARGUMENT, pt_proxy.update_fresh_replica_version(A, 88, keys, 2));
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
