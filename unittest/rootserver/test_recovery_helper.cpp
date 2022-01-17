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

#define USING_LOG_PREFIX RS
#include "gtest/gtest.h"
#include "gmock/gmock.h"
//#include "../share/schema/db_initializer.h"
#include "rootserver/ob_recovery_helper.h"
//#include "share/ob_remote_sql_proxy.h"
#include "../share/partition_table/fake_part_property_getter.h"  // for A,B,C,D,E
//#include "../share/partition_table/fake_partition_table_operator.h"
//#include "../share/partition_table/fake_schema_service.h"
//#include "share/partition_table/ob_partition_table_operator.h"
//#include "share/schema/ob_schema_getter_guard.h"
//#include "share/schema/ob_multi_version_schema_service.h"
//#include "rootserver/ob_server_manager.h"
//#include "rootserver/ob_zone_manager.h"
#include "share/schema/ob_schema_test_utils.cpp"
//#include "lib/hash/ob_hashset.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace share::host;
using namespace common::hash;
namespace rootserver {
// class FakeSchemaService : public MockSchemaService
//{
// public:
//  const static int64_t SCHEMA_VERSION = 1;
//  const static int64_t USR_SCHEMA_VERSION = 2;
//
//  int init() { return MockSchemaService::init(); }
//  virtual int gen_tenant_schema(const uint64_t tenant_id, ObTenantSchema &tenant_schema);
//  virtual int gen_sys_variable(const uint64_t tenant_id, ObSysVariableSchema &sys_variable);
//  virtual int gen_table_schema(const uint64_t tenant_id, const uint64_t pure_id,
//                               const uint64_t pure_db_id, const uint64_t pure_tg_id,
//                               ObTableSchema &table_schema);
//  virtual int get_schema_guard(ObSchemaGetterGuard &guard,
//                               int64_t schema_version = common::OB_INVALID_VERSION,
//                                const bool force_fallback = false)
//  {
//    UNUSED(force_fallback);
//    UNUSED(schema_version);
//    return MockSchemaService::get_schema_guard(guard, schema_version);
//  }
//  int add_tenant(ObTenantSchema &tenant_schema)
//  {
//    tenant_schema.set_schema_version(SCHEMA_VERSION);
//    return MockSchemaService::add_tenant_schema(tenant_schema, SCHEMA_VERSION);
//  }
//  int add_table(ObTableSchema &table_schema)
//  {
//    table_schema.set_schema_version(SCHEMA_VERSION);
//    return MockSchemaService::add_table_schema(table_schema, SCHEMA_VERSION);
//  }
//};
// int FakeSchemaService::gen_tenant_schema(const uint64_t tenant_id, ObTenantSchema &tenant_schema)
//{
//  int ret = OB_SUCCESS;
//  char tenant_name[64];
//  if (snprintf(tenant_name, 64, "tenant_%lu", tenant_id) >= 64) {
//    ret = OB_BUF_NOT_ENOUGH;
//    SHARE_SCHEMA_LOG(WARN, "buf not enough", K(ret));
//  } else {
//    tenant_schema.set_tenant_id(tenant_id);
//    tenant_schema.set_tenant_name(tenant_name);
//    tenant_schema.set_comment("this is a test tenant");
//    tenant_schema.add_zone("zone");
//    tenant_schema.set_primary_zone("zone");
//    tenant_schema.set_locality("auto_locality_strategy");
//  }
//  return ret;
//}
//
// int FakeSchemaService::gen_sys_variable(const uint64_t tenant_id, ObSysVariableSchema &sys_variable)
//{
//  int ret = OB_SUCCESS;
//  sys_variable.set_tenant_id(tenant_id);
//  if (OB_SYS_TENANT_ID == tenant_id) {
//    sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
//    sys_variable.load_default_system_variable(true);
//  } else {
//    sys_variable.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
//    sys_variable.load_default_system_variable(false);
//  }
//  return ret;
//}
//
// int FakeSchemaService::gen_table_schema(const uint64_t tenant_id,
//                                        const uint64_t pure_id,
//                                        const uint64_t pure_db_id,
//                                        const uint64_t pure_tg_id,
//                                        ObTableSchema &table_schema)
//{
//  int ret = OB_SUCCESS;
//  table_schema.reset();
//  ObInnerTableSchema::all_core_table_schema(table_schema);
//  char table_name[64];
//  if (snprintf(table_name, 64, "table_%lu", combine_id(tenant_id, pure_id)) >= 64) {
//    ret = OB_BUF_NOT_ENOUGH;
//    SHARE_SCHEMA_LOG(WARN, "buf not enough", K(ret));
//  } else {
//    table_schema.set_table_name(table_name);
//    table_schema.set_tenant_id(tenant_id);
//    table_schema.set_table_id(combine_id(tenant_id, pure_id));
//    table_schema.set_database_id(combine_id(tenant_id, pure_db_id));
//    table_schema.set_tablegroup_id(combine_id(tenant_id, pure_tg_id));
//    table_schema.set_locality("R@z1,R@z2,R@z3");
//    ObArray<share::ObZoneReplicaNumSet> zone_replica_set;
//    for (int64_t i = 0; i < 3; i++) {
//      share::ObZoneReplicaNumSet set;
//      set.zone_ = ObZone("z1");
//      //set.set_replica_num(0, 0, 0, 1, 0);
//      zone_replica_set.push_back(set);
//    }
//    //table_schema.set_zone_replica_num_array(zone_replica_set);
//  }
//  return ret;
//}

class TestRecoveryHelper : public ::testing::Test {
public:
  TestRecoveryHelper()
  {}
  virtual ~TestRecoveryHelper()
  {}
  virtual void SetUp();
  virtual void TearDown();
  // int add_table_schema();
  // int add_partition();
  int build_partition(int64_t table_id, ObPartitionInfo& partition, int64_t replica_cnt, int64_t pos = 6);
  int add_server();

public:
  // DBInitializer db_initer_;
  // FakePartPropertyGetter prop_getter_;
  // FakePartitionTableOperator pt_;
  // FakeSchemaService schema_service_;
  // schema::ObSchemaGetterGuard schema_guard_;
  ObArray<ObAddr> server_list_;
};

int TestRecoveryHelper::add_server()
{
  int ret = OB_SUCCESS;
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(A));
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(B));
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(C));
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(D));
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(E));
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(F));
  EXPECT_EQ(OB_SUCCESS, server_list_.push_back(G));
  return ret;
}
// int TestRecoveryHelper::add_table_schema()
//{
//  int ret = OB_SUCCESS;
//  ObTableSchema table_schema;
//  ObTenantSchema tenant_schema;
//  ObSysVariableSchema sys_variable;
//  int64_t pure_db_id = 10001;
//  int64_t pure_tg_id = 10001;
//  int64_t TENANT_CNT = 2;
//  int64_t TABLE_CNT = 5;
//  {
//    int64_t MIN_TENANT_ID = 1001;
//    int64_t MIN_TABLE_ID = 50001;
//    for (int64_t i = 0; i < TENANT_CNT; i++) {
//      int64_t tenant_id = MIN_TENANT_ID + i;
//      EXPECT_EQ(OB_SUCCESS, schema_service_.gen_tenant_schema(tenant_id, tenant_schema));
//      EXPECT_EQ(OB_SUCCESS, schema_service_.add_tenant(tenant_schema));
//      EXPECT_EQ(OB_SUCCESS, schema_service_.gen_sys_variable(tenant_id, sys_variable));
//      EXPECT_EQ(OB_SUCCESS, schema_service_.add_sys_variable_schema(sys_variable,
//      tenant_schema.get_schema_version())); for (int64_t j = 0; j < TABLE_CNT; j++) {
//        int64_t table_id = MIN_TABLE_ID + j;
//        EXPECT_EQ(OB_SUCCESS, schema_service_.gen_table_schema(tenant_id, table_id,
//                                                               pure_db_id, pure_tg_id,
//                                                               table_schema));
//        EXPECT_EQ(OB_SUCCESS, schema_service_.add_table(table_schema));
//      }
//    }
//  }
//  return ret;
//}
//
int TestRecoveryHelper::build_partition(int64_t table_id, ObPartitionInfo& partition, int64_t replica_cnt, int64_t pos)
{
  int ret = OB_SUCCESS;
  partition.set_table_id(table_id);
  partition.set_partition_id(0);
  ObPartitionReplica::MemberList ml;

  for (int64_t i = 0; i < replica_cnt; i++) {
    ObPartitionReplica replica;
    replica.table_id_ = table_id;
    replica.partition_id_ = 0;
    replica.partition_cnt_ = 1;
    replica.server_ = server_list_.at(pos - i);
    replica.zone_ = ZONE1;
    replica.data_version_ = 2;
    replica.member_list_ = ml;
    replica.replica_type_ = REPLICA_TYPE_FULL;
    if (i == 0) {
      replica.role_ = LEADER;
      replica.to_leader_time_ = ObTimeUtility::current_time();
    }
    partition.get_replicas_v2().push_back(replica);
  }
  return ret;
}

// int TestRecoveryHelper::add_partition()
//{
//  int ret = OB_SUCCESS;
//  int64_t TENANT_CNT = 2;
//  int64_t TABLE_CNT = 5;
//  {
//    int64_t MIN_TENANT_ID = 1001;
//    int64_t MIN_TABLE_ID = 50001;
//    for (int64_t i = 0; i < TENANT_CNT; i ++) {
//      int64_t tenant_id = MIN_TENANT_ID + i;
//      for (int64_t j = 0; j < TABLE_CNT; j++) {
//        int64_t table_id = combine_id(tenant_id, MIN_TABLE_ID + j);
//        ObPartitionInfo info;
//        info.set_table_id(table_id);
//        info.set_partition_id(0);
//        EXPECT_EQ(OB_SUCCESS, build_partition(table_id, info, 3));
//        EXPECT_EQ(OB_SUCCESS, pt_.partitions_.push_back(info));
//      }
//    }
//  }
//  return ret;
//}

void TestRecoveryHelper::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestRecoveryHelper::SetUp()
{
  ObKVGlobalCache::get_instance().init();
  add_server();
  // ASSERT_EQ(OB_SUCCESS, db_initer_.init());
  // schema_service_.init();
  // const bool only_core_tables = false;
  // ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(only_core_tables));
  // ASSERT_EQ(OB_SUCCESS, db_initer_.fill_sys_stat_table());
  // ASSERT_EQ(OB_SUCCESS, add_table_schema());
  // ASSERT_EQ(OB_SUCCESS, add_partition());
}

#define CHECK_RESULT(addr, array_index, member_index)                                                             \
  ASSERT_EQ(                                                                                                      \
      OB_SUCCESS, list.ml_pk_array_.at(array_index).member_list_.get_server_by_index(member_index, result_addr)); \
  ASSERT_EQ(addr, result_addr);

TEST_F(TestRecoveryHelper, test_member_list)
{
  ObRecoveryHelper::ObMemberListPkeyList list;
  ObAddr result_addr;
  // Add partition has the same member list
  int64_t tenant_id = 1001;
  for (int64_t i = 0; i < 4; i++) {
    int64_t pure_id = 50001 + i;
    int64_t table_id = combine_id(tenant_id, pure_id);
    int64_t replica_cnt = 3;
    int64_t pos = 2;
    ObPartitionInfo partition;
    ASSERT_EQ(OB_SUCCESS, build_partition(table_id, partition, replica_cnt, pos));
    ASSERT_EQ(OB_SUCCESS, list.add_partition(partition));
  }
  ASSERT_EQ(4, list.pkey_array_.count());
  ASSERT_EQ(1, list.ml_pk_array_.count());
  ASSERT_EQ(3, list.ml_pk_array_.at(0).member_list_.get_member_number());
  CHECK_RESULT(A, 0, 0);
  CHECK_RESULT(B, 0, 1);
  CHECK_RESULT(C, 0, 2);
  // ASSERT_EQ(A, list.ml_pk_array_.at(0).member_list_.at(0));
  // ASSERT_EQ(B, list.ml_pk_array_.at(0).member_list_.at(1));
  // ASSERT_EQ(C, list.ml_pk_array_.at(0).member_list_.at(2));
  // Add partition has different member list
  for (int64_t i = 0; i < 4; i++) {
    int64_t pure_id = 50004 + i;
    int64_t table_id = combine_id(tenant_id, pure_id);
    int64_t replica_cnt = 2;
    int64_t pos = 5;
    ObPartitionInfo partition;
    ASSERT_EQ(OB_SUCCESS, build_partition(table_id, partition, replica_cnt, pos));
    ASSERT_EQ(OB_SUCCESS, list.add_partition(partition));
  }
  ASSERT_EQ(8, list.pkey_array_.count());
  ASSERT_EQ(2, list.ml_pk_array_.count());
  ASSERT_EQ(3, list.ml_pk_array_.at(0).member_list_.get_member_number());
  // ASSERT_EQ(A, list.ml_pk_array_.at(0).member_list_.at(0));
  // ASSERT_EQ(B, list.ml_pk_array_.at(0).member_list_.at(1));
  // ASSERT_EQ(C, list.ml_pk_array_.at(0).member_list_.at(2));
  CHECK_RESULT(A, 0, 0);
  CHECK_RESULT(B, 0, 1);
  CHECK_RESULT(C, 0, 2);

  ASSERT_EQ(2, list.ml_pk_array_.at(1).member_list_.get_member_number());
  // ASSERT_EQ(E, list.ml_pk_array_.at(1).member_list_.at(0));
  // ASSERT_EQ(F, list.ml_pk_array_.at(1).member_list_.at(1));
  CHECK_RESULT(E, 1, 0);
  CHECK_RESULT(F, 1, 1);
  for (int64_t i = 0; i < 4; i++) {
    int64_t pure_id = 50008 + i;
    int64_t table_id = combine_id(tenant_id, pure_id);
    int64_t replica_cnt = 4;
    int64_t pos = 3;
    ObPartitionInfo partition;
    ASSERT_EQ(OB_SUCCESS, build_partition(table_id, partition, replica_cnt, pos));
    ASSERT_EQ(OB_SUCCESS, list.add_partition(partition));
  }
  ASSERT_EQ(12, list.pkey_array_.count());
  ASSERT_EQ(3, list.ml_pk_array_.count());
  ASSERT_EQ(3, list.ml_pk_array_.at(0).member_list_.get_member_number());
  // ASSERT_EQ(A, list.ml_pk_array_.at(0).member_list_.at(0));
  // ASSERT_EQ(B, list.ml_pk_array_.at(0).member_list_.at(1));
  // ASSERT_EQ(C, list.ml_pk_array_.at(0).member_list_.at(2));
  CHECK_RESULT(A, 0, 0);
  CHECK_RESULT(B, 0, 1);
  CHECK_RESULT(C, 0, 2);

  ASSERT_EQ(2, list.ml_pk_array_.at(1).member_list_.get_member_number());
  // ASSERT_EQ(E, list.ml_pk_array_.at(1).member_list_.at(0));
  // ASSERT_EQ(F, list.ml_pk_array_.at(1).member_list_.at(1));
  CHECK_RESULT(E, 1, 0);
  CHECK_RESULT(F, 1, 1);
  ASSERT_EQ(4, list.ml_pk_array_.at(2).member_list_.get_member_number());
  CHECK_RESULT(A, 2, 0);
  CHECK_RESULT(B, 2, 1);
  CHECK_RESULT(C, 2, 2);
  CHECK_RESULT(D, 2, 3);
  // Add the partition of the same member list again
  for (int64_t i = 0; i < 4; i++) {
    int64_t pure_id = 50010 + i;
    int64_t table_id = combine_id(tenant_id, pure_id);
    int64_t replica_cnt = 3;
    int64_t pos = 2;
    ObPartitionInfo partition;
    ASSERT_EQ(OB_SUCCESS, build_partition(table_id, partition, replica_cnt, pos));
    ASSERT_EQ(OB_SUCCESS, list.add_partition(partition));
  }
  ASSERT_EQ(16, list.pkey_array_.count());
  ASSERT_EQ(3, list.ml_pk_array_.count());
  ASSERT_EQ(3, list.ml_pk_array_.at(0).member_list_.get_member_number());
  // ASSERT_EQ(A, list.ml_pk_array_.at(0).member_list_.at(0));
  // ASSERT_EQ(B, list.ml_pk_array_.at(0).member_list_.at(1));
  // ASSERT_EQ(C, list.ml_pk_array_.at(0).member_list_.at(2));
  CHECK_RESULT(A, 0, 0);
  CHECK_RESULT(B, 0, 1);
  CHECK_RESULT(C, 0, 2);
  ASSERT_EQ(8, list.ml_pk_array_.at(0).pkey_info_.count());
  ASSERT_EQ(0, list.ml_pk_array_.at(0).pkey_info_.at(0).pkey_index_);
  ASSERT_EQ(1, list.ml_pk_array_.at(0).pkey_info_.at(1).pkey_index_);
  ASSERT_EQ(2, list.ml_pk_array_.at(0).pkey_info_.at(2).pkey_index_);
  ASSERT_EQ(3, list.ml_pk_array_.at(0).pkey_info_.at(3).pkey_index_);
  ASSERT_EQ(12, list.ml_pk_array_.at(0).pkey_info_.at(4).pkey_index_);
  ASSERT_EQ(13, list.ml_pk_array_.at(0).pkey_info_.at(5).pkey_index_);
  ASSERT_EQ(14, list.ml_pk_array_.at(0).pkey_info_.at(6).pkey_index_);
  ASSERT_EQ(15, list.ml_pk_array_.at(0).pkey_info_.at(7).pkey_index_);
  ASSERT_EQ(2, list.ml_pk_array_.at(1).member_list_.get_member_number());
  CHECK_RESULT(E, 1, 0);
  CHECK_RESULT(F, 1, 1);
  ASSERT_EQ(4, list.ml_pk_array_.at(1).pkey_info_.count());
  ASSERT_EQ(4, list.ml_pk_array_.at(2).member_list_.get_member_number());
  // ASSERT_EQ(A, list.ml_pk_array_.at(2).member_list_.at(0));
  // ASSERT_EQ(B, list.ml_pk_array_.at(2).member_list_.at(1));
  // ASSERT_EQ(C, list.ml_pk_array_.at(2).member_list_.at(2));
  // ASSERT_EQ(D, list.ml_pk_array_.at(2).member_list_.at(3));
  CHECK_RESULT(A, 2, 0);
  CHECK_RESULT(B, 2, 1);
  CHECK_RESULT(C, 2, 2);
  CHECK_RESULT(D, 2, 3);

  ASSERT_EQ(4, list.ml_pk_array_.at(2).pkey_info_.count());
  ASSERT_EQ(8, list.ml_pk_array_.at(2).pkey_info_.at(0).pkey_index_);
  ASSERT_EQ(9, list.ml_pk_array_.at(2).pkey_info_.at(1).pkey_index_);
  ASSERT_EQ(10, list.ml_pk_array_.at(2).pkey_info_.at(2).pkey_index_);
  ASSERT_EQ(11, list.ml_pk_array_.at(2).pkey_info_.at(3).pkey_index_);
}
}  // namespace rootserver
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
