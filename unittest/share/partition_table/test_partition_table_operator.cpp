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
#define private public
#include "lib/stat/ob_session_stat.h"
#include "share/config/ob_server_config.h"
#include "share/schema/db_initializer.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "lib/allocator/page_arena.h"
#include "fake_part_property_getter.h"
#include "../mock_ob_rs_mgr.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "../../rootserver/fake_rs_list_change_cb.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
namespace share {

using namespace common;
using namespace schema;
using namespace host;
using namespace obrpc;

static uint64_t& TEN = FakePartPropertyGetter::TEN();
static uint64_t& TID = FakePartPropertyGetter::TID();
static int64_t& PID = FakePartPropertyGetter::PID();

ObServerConfig& config = ObServerConfig::get_instance();
class TestPartitionTableOperator : public ::testing::Test {
public:
  TestPartitionTableOperator() : operator_(prop_getter_)
  {}

  virtual void SetUp();
  virtual void TearDown()
  {}

protected:
  DBInitializer db_initer_;
  FakePartPropertyGetter prop_getter_;
  MockObCommonRpcProxy rpc_proxy_;
  MockObRsMgr rs_mgr_;
  ObPartitionTableOperator operator_;
  FakeRsListChangeCb rs_list_cb_;
  FakeMergeErrorCb merge_error_cb_;
};

void TestPartitionTableOperator::SetUp()
{
  TID = combine_id(TEN, OB_ALL_ROOT_TABLE_TID);
  int ret = db_initer_.init();

  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = operator_.init(db_initer_.get_sql_proxy(), NULL);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, operator_.set_callback_for_rs(rs_list_cb_, merge_error_cb_));
}

TEST_F(TestPartitionTableOperator, common)
{
  // inmemory table
  TID = combine_id(TEN, OB_ALL_CORE_TABLE_TID);
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER);

  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    GCONF.self_addr_ = prop_getter_.get_replicas().at(i).server_;
    ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(i)));
  }

  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);

  // check is_original_leader
  ASSERT_FALSE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);
  ASSERT_EQ(OB_SUCCESS, operator_.set_original_leader(TID, PID, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_TRUE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);

  ASSERT_EQ(OB_SUCCESS, operator_.remove(TID, PID, A));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);

  // persistent table
  TID = combine_id(TEN, OB_ALL_ROOT_TABLE_TID);
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER);

  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(i)));
  }

  partition.reuse();
  partition.set_allocator(&allocator);
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);

  // check is_original_leader
  ASSERT_FALSE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);
  ASSERT_EQ(OB_SUCCESS, operator_.set_original_leader(TID, PID, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_TRUE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);

  ASSERT_EQ(OB_SUCCESS, operator_.remove(TID, PID, A));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);

  TID = combine_id(TEN, OB_ALL_TABLE_TID);
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER);

  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(i)));
  }

  partition.reuse();
  partition.set_allocator(&allocator);
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_EQ(2, partition.replica_count());
  ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
  ASSERT_EQ(B, partition.get_replicas_v2().at(1).server_);

  // check is_original_leader
  ASSERT_FALSE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);
  ASSERT_EQ(OB_SUCCESS, operator_.set_original_leader(TID, PID, true));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_TRUE(partition.get_replicas_v2().at(0).is_original_leader_);
  ASSERT_FALSE(partition.get_replicas_v2().at(1).is_original_leader_);

  ASSERT_EQ(OB_SUCCESS, operator_.remove(TID, PID, A));
  partition.reuse();
  ASSERT_EQ(OB_SUCCESS, operator_.get(TID, PID, partition));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);
}

TEST_F(TestPartitionTableOperator, invalid_argument)
{
  int ret = operator_.init(db_initer_.get_sql_proxy(), NULL);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);

  ASSERT_NE(OB_SUCCESS, operator_.get(OB_INVALID_ID, 0, partition));
  ASSERT_NE(OB_SUCCESS, operator_.get(TID, -1, partition));

  ObPartitionReplica replica;
  ASSERT_NE(OB_SUCCESS, operator_.update(replica));

  ObAddr addr;
  ASSERT_NE(OB_SUCCESS, operator_.remove(OB_INVALID_ID, 0, addr));
  ASSERT_NE(OB_SUCCESS, operator_.remove(TID, -1, addr));
  ASSERT_NE(OB_SUCCESS, operator_.remove(TID, 0, addr));

  // not inited
  ObPartitionTableOperator operator2(prop_getter_);
  ASSERT_EQ(OB_NOT_INIT, operator2.get(TID, 0, partition));

  replica.table_id_ = combine_id(TEN, TID);
  replica.partition_id_ = 0;
  replica.data_version_ = 1;
  replica.zone_ = "1";
  replica.server_ = A;
  replica.partition_cnt_ = 1;
  ASSERT_EQ(OB_NOT_INIT, operator2.update(replica));

  ASSERT_EQ(OB_NOT_INIT, operator2.remove(TID, 0, A));
}

TEST_F(TestPartitionTableOperator, rpc_table)
{
  int ret = operator_.set_callback_for_obs(rpc_proxy_, rs_mgr_, db_initer_.get_config());
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(operator_.root_meta_table_ == &operator_.rpc_table_);

  operator_.set_callback_for_rs(rs_list_cb_, merge_error_cb_);
  ASSERT_TRUE(operator_.root_meta_table_ == &operator_.get_inmemory_table());
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
