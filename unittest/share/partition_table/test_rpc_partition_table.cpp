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
#include <gmock/gmock.h>
#include "share/config/ob_server_config.h"
#include "share/schema/db_initializer.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_rpc_partition_table.h"
#include "lib/allocator/page_arena.h"
#include "fake_part_property_getter.h"
#include "../mock_ob_rs_mgr.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "../../rootserver/fake_rs_list_change_cb.h"
#include "lib/container/ob_array_iterator.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgReferee;
using ::testing::Unused;
namespace oceanbase {
namespace share {

using namespace common;
using namespace schema;
using namespace host;
using namespace obrpc;

uint64_t& TEN = FakePartPropertyGetter::TEN();
uint64_t& TID = FakePartPropertyGetter::TID();
int64_t& PID = FakePartPropertyGetter::PID();

ObServerConfig& config = ObServerConfig::get_instance();
class TestRpcPartitionTable : public ::testing::Test {
public:
  TestRpcPartitionTable();
  virtual ~TestRpcPartitionTable()
  {}

  virtual void SetUp();
  virtual void TearDown()
  {}

protected:
  int get_wrapper(ObPartitionInfo& part_info, const ObRpcOpts& opts);
  int report_wrapper(const ObPartitionReplica& replica, const ObRpcOpts& opts);
  int remove_wrapper(const ObAddr& server, const ObRpcOpts& opts);
  FakePartPropertyGetter prop_getter_;
  MockObRsMgr rs_mgr_;
  MockObCommonRpcProxy rpc_proxy_;
  ObMySQLProxy mysql_proxy_;
  ObPartitionTableOperator pt_;
  ObRpcPartitionTable rpc_table_;
  FakeRsListChangeCb cb_;
  FakeMergeErrorCb merge_error_cb_;
};

TestRpcPartitionTable::TestRpcPartitionTable()
    : prop_getter_(), rs_mgr_(), rpc_proxy_(), mysql_proxy_(), pt_(prop_getter_), rpc_table_(prop_getter_)
{}

void TestRpcPartitionTable::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, pt_.init(mysql_proxy_, NULL));
  ASSERT_EQ(OB_SUCCESS, pt_.set_callback_for_rs(cb_, merge_error_cb_));
  ASSERT_EQ(OB_SUCCESS, rpc_table_.init(rpc_proxy_, rs_mgr_, config));
  rs_mgr_.global_rs().set_ip_addr("127.0.0.1", 5555);
  ON_CALL(rs_mgr_, get_master_root_server(_))
      .WillByDefault(Invoke(&rs_mgr_, &MockObRsMgr::get_master_root_server_wrapper));
  ON_CALL(rpc_proxy_, get_root_partition(_, _)).WillByDefault(Invoke(this, &TestRpcPartitionTable::get_wrapper));
  ON_CALL(rpc_proxy_, report_root_partition(_, _)).WillByDefault(Invoke(this, &TestRpcPartitionTable::report_wrapper));
  ON_CALL(rpc_proxy_, remove_root_partition(_, _)).WillByDefault(Invoke(this, &TestRpcPartitionTable::remove_wrapper));
}

int TestRpcPartitionTable::get_wrapper(ObPartitionInfo& part_info, const ObRpcOpts& opts)
{
  UNUSED(opts);
  return pt_.get(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID), ObRpcPartitionTable::ALL_CORE_TABLE_PARTITION_ID, part_info);
}

int TestRpcPartitionTable::report_wrapper(const ObPartitionReplica& replica, const ObRpcOpts& opts)
{
  UNUSED(opts);
  return pt_.update(replica);
}

int TestRpcPartitionTable::remove_wrapper(const ObAddr& server, const ObRpcOpts& opts)
{
  UNUSED(opts);
  return pt_.remove(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID), ObRpcPartitionTable::ALL_CORE_TABLE_PARTITION_ID, server);
}

TEST_F(TestRpcPartitionTable, not_init)
{
  TID = 5;
  PID = 5;
  ObRpcPartitionTable rpc_table(prop_getter_);
  ObPartitionInfo part_info;
  ObPartitionReplica replica;
  ObAddr server;
  ASSERT_EQ(OB_NOT_INIT, rpc_table.get(TID, PID, part_info));
  ASSERT_EQ(OB_NOT_INIT, rpc_table.update(replica));
  ASSERT_EQ(OB_NOT_INIT, rpc_table.remove(TID, PID, server));
  ASSERT_EQ(OB_SUCCESS, rpc_table.init(rpc_proxy_, rs_mgr_, config));
  ASSERT_EQ(OB_INIT_TWICE, rpc_table.init(rpc_proxy_, rs_mgr_, config));
  ASSERT_EQ(OB_INVALID_ARGUMENT, rpc_table.get(TID, PID, part_info));
  ASSERT_EQ(OB_INVALID_ARGUMENT, rpc_table.update(replica));
  ASSERT_EQ(OB_INVALID_ARGUMENT, rpc_table.remove(TID, PID, server));
}

TEST_F(TestRpcPartitionTable, common)
{
  // to make get member list works
  TID = combine_id(TEN, OB_ALL_CORE_TABLE_TID);
  PID = ObRpcPartitionTable::ALL_CORE_TABLE_PARTITION_ID;
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
  ObPartitionInfo part_info;
  ASSERT_EQ(OB_SUCCESS, rpc_table_.get(TID, PID, part_info));
  ASSERT_EQ(0, part_info.replica_count());
  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); ++i) {
    GCONF.self_addr_ = prop_getter_.get_replicas().at(i).server_;
    ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter_.get_replicas().at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, rpc_table_.get(TID, PID, part_info));
  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); ++i) {
    ASSERT_EQ(part_info.get_replicas_v2().at(i).server_, prop_getter_.get_replicas().at(i).server_);
    ASSERT_EQ(part_info.get_replicas_v2().at(i).role_, prop_getter_.get_replicas().at(i).role_);
  }
  ASSERT_EQ(OB_SUCCESS, rpc_table_.remove(TID, PID, C));
  part_info.reuse();
  ASSERT_EQ(OB_SUCCESS, rpc_table_.get(TID, PID, part_info));
  const ObPartitionReplica* replica = NULL;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, part_info.find(C, replica));

  ASSERT_EQ(OB_NOT_SUPPORTED, rpc_table_.set_original_leader(TID, PID, true));
}

TEST_F(TestRpcPartitionTable, rpc_error)
{
  TEN = OB_SYS_TENANT_ID;
  TID = combine_id(TEN, OB_ALL_CORE_TABLE_TID);
  PID = ObRpcPartitionTable::ALL_CORE_TABLE_PARTITION_ID;
  ObPartitionInfo part_info;
  ObPartitionReplica replica;
  replica.table_id_ = TID;
  replica.partition_id_ = PID;
  replica.server_ = A;
  replica.zone_ = "1";
  replica.partition_cnt_ = 1;
  ObAddr server = A;
  ON_CALL(rpc_proxy_, get_root_partition(_, _)).WillByDefault(Return(OB_ERROR));
  ON_CALL(rpc_proxy_, report_root_partition(_, _)).WillByDefault(Return(OB_ERROR));
  ON_CALL(rpc_proxy_, remove_root_partition(_, _)).WillByDefault(Return(OB_ERROR));
  ASSERT_EQ(OB_ERR_UNEXPECTED, rpc_table_.get(TID, PID, part_info));
  ASSERT_EQ(OB_ERROR, rpc_table_.update(replica));
  ASSERT_EQ(OB_ERROR, rpc_table_.remove(TID, PID, server));
}

TEST_F(TestRpcPartitionTable, update_rebuild_flag)
{
  TID = combine_id(TEN, OB_ALL_CORE_TABLE_TID);
  PID = ObRpcPartitionTable::ALL_CORE_TABLE_PARTITION_ID;

  {
    EXPECT_CALL(rpc_proxy_, rebuild_root_partition(_, _)).WillOnce(Return(OB_SUCCESS));
    ASSERT_EQ(OB_SUCCESS, rpc_table_.update_rebuild_flag(TID, PID, A, true));
  }
  ASSERT_EQ(OB_SUCCESS, rpc_table_.update_rebuild_flag(TID, PID, A, false));
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
