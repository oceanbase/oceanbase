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
#define private public
#define protected public
#include "lib/stat/ob_session_stat.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_alive_server_tracer.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_bootstrap.h"
#include "rootserver/ob_server_manager.h"
#include "../share/schema/db_initializer.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "../share/partition_table/fake_part_property_getter.h"
#include "../share/mock_ob_rs_mgr.h"
#include "mock_ob_bootstrap.h"
#include "server_status_builder.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_leader_coordinator.h"
#include "rootserver/ob_partition_creator.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "rootserver/ob_rs_gts_manager.h"
#include "fake_rs_list_change_cb.h"
#include "lib/container/ob_array_iterator.h"
#include "mock_leader_coordinate.h"
#include "mock_freeze_info_manager.h"
#include "share/ob_tenant_mgr.h"
#include "ob_rs_test_utils.h"
#include "rootserver/ob_single_partition_balance.h"

static void* null_ptr = (void*)0x8888;  // instead of NULL to avoid coverity complain.

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share::schema;
using namespace share;
using namespace share::host;

static uint64_t& TID = FakePartPropertyGetter::TID();
static int64_t& PID = FakePartPropertyGetter::PID();

namespace rootserver {
class MockLocalityManager : public ObILocalityManager {
public:
  struct ServerInfo {
    ServerInfo() : server_(), is_local_(false)
    {}
    common::ObAddr server_;
    bool is_local_;
    TO_STRING_KV(K_(server), K_(is_local));
  };
  MockLocalityManager() : is_readonly_(false), server_info_()
  {}
  virtual ~MockLocalityManager()
  {}
  virtual int is_local_zone_read_only(bool& is_readonly)
  {
    is_readonly = is_readonly_;
    return common::OB_SUCCESS;
  }
  virtual int is_local_server(const common::ObAddr& server, bool& is_local)
  {
    int ret = OB_SUCCESS;
    is_local = false;
    for (int64_t i = 0; i < server_info_.count(); i++) {
      if (server == server_info_.at(i).server_) {
        is_local = server_info_.at(i).is_local_;
        break;
      }
    }
    return ret;
  }
  bool is_readonly_;
  common::ObArray<ServerInfo> server_info_;
};

class TestBootstrap : public ::testing::Test {
public:
  TestBootstrap();
  virtual ~TestBootstrap()
  {}
  virtual void SetUp();
  virtual void TearDown();
  int init_partition_table();
  void call_execute_bootstrap();

protected:
  DBInitializer db_initer_;
  ObMultiVersionSchemaService schema_service_;
  MockObSrvRpcProxy rpc_proxy_;
  MockObCommonRpcProxy rs_rpc_proxy_;
  ObDDLOperator ddl_operator_;
  ObZoneManager zone_mgr_;
  ObDDLService ddl_service_;
  ObServerInfoList rs_list_;
  FakePartPropertyGetter prop_getter_;
  ObPartitionTableOperator pt_;
  ObLocationFetcher fetcher_;
  ObPartitionLocationCache loc_cache_;
  MockObRsMgr rs_mgr_;
  MockFreezeInfoManager freeze_info_manager_;
  ObServerManager server_mgr_;
  ObUnitManager unit_mgr_;
  FakeRsListChangeCb rs_list_cb_;
  FakeMergeErrorCb merge_error_cb_;
  ObLeaderCoordinator leader_coordinator_;
  ObAliveServerMap alive_server_;
  MockLocalityManager locality_manager_;
  ObSnapshotInfoManager snapshot_manager_;
  ObRebalanceTaskMgr task_mgr_;
  ObRootBalancer root_balance_;
  ObSinglePartBalance single_part_balance_;
};

TestBootstrap::TestBootstrap()
    : db_initer_(),
      schema_service_(),
      rpc_proxy_(),
      ddl_operator_(schema_service_, db_initer_.get_sql_proxy()),
      ddl_service_(),
      rs_list_(),
      prop_getter_(),
      pt_(prop_getter_),
      fetcher_(),
      loc_cache_(fetcher_),
      unit_mgr_(server_mgr_, zone_mgr_)
{}

void TestBootstrap::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestBootstrap::SetUp()
{
  ObPartitionCreator::flag_replica_batch_update_cnt = 1;
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObKVGlobalCache::get_instance().init();

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, alive_server_.init());

  //  ret = db_initer_.fill_sys_stat_table();
  //  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service_.init(&db_initer_.get_sql_proxy(),
      NULL,
      &db_initer_.get_config(),
      OB_MAX_VERSION_COUNT,
      OB_MAX_VERSION_COUNT_FOR_MERGE,
      false);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRegion default_region("default_region");

  ObServerInfo info;
  info.zone_ = "1";
  info.server_ = A;
  info.region_ = default_region;
  ASSERT_EQ(OB_SUCCESS, rs_list_.push_back(info));
  info.zone_ = "2";
  info.server_ = B;
  info.region_ = default_region;
  ASSERT_EQ(OB_SUCCESS, rs_list_.push_back(info));
  info.zone_ = "3";
  info.server_ = C;
  info.region_ = default_region;
  ASSERT_EQ(OB_SUCCESS, rs_list_.push_back(info));

  ASSERT_EQ(OB_SUCCESS,
      ddl_service_.init(rpc_proxy_,
          rs_rpc_proxy_,
          db_initer_.get_sql_proxy(),
          schema_service_,
          pt_,
          server_mgr_,
          zone_mgr_,
          unit_mgr_,
          *(ObRootBalancer*)null_ptr,
          freeze_info_manager_,
          snapshot_manager_,
          task_mgr_));
  ASSERT_EQ(OB_SUCCESS,
      unit_mgr_.init(
          db_initer_.get_sql_proxy(), db_initer_.get_config(), leader_coordinator_, schema_service_, root_balance_));

  db_initer_.get_config().enable_rootservice_standalone = false;
  ON_CALL(rpc_proxy_, create_partition_batch(_, _, _))
      .WillByDefault(Invoke(&rpc_proxy_, &MockObSrvRpcProxy::create_partition_batch_wrapper));
}

int TestBootstrap::init_partition_table()
{
  int ret = OB_SUCCESS;
  pt_.set_callback_for_rs(rs_list_cb_, merge_error_cb_);
  // insert user table partition info

  ObArray<std::pair<uint64_t, int64_t> > sys_tables;
  const int64_t part_num = 1;
  const schema_create_func* creator_ptr_array[] = {core_table_schema_creators, sys_table_schema_creators, NULL};

  if (OB_FAIL(sys_tables.push_back(std::make_pair(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID), part_num)))) {
    LOG_INFO("failed to add sys table id", K(ret));
  }

  for (const schema_create_func** creator_ptr_ptr = creator_ptr_array; OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
       ++creator_ptr_ptr) {
    for (const schema_create_func* creator_ptr = *creator_ptr_ptr; OB_SUCCESS == ret && NULL != *creator_ptr;
         ++creator_ptr) {
      ObTableSchema table_schema;
      if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_INFO("failed to build table schema", K(ret));
      } else if (table_schema.has_partition()) {
        if (OB_FAIL(sys_tables.push_back(std::make_pair(
                combine_id(OB_SYS_TENANT_ID, table_schema.get_table_id()), table_schema.get_all_part_num())))) {
          LOG_INFO("failed to add sys table id", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; i < sys_tables.count(); ++i) {
    TID = sys_tables.at(i).first;
    for (int64_t j = 0; j < sys_tables.at(i).second; ++j) {
      PID = j;
      prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
      for (int64_t k = 0; k < prop_getter_.get_replicas().count(); ++k) {
        // ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter_.get_replicas().at(k)));
        GCONF.self_addr_ = prop_getter_.get_replicas().at(k).server_;
        pt_.update(prop_getter_.get_replicas().at(k));
      }
    }
  }

  return ret;
}

// TEST_F(TestBootstrap, pre_bootstrap)
//{
//  ObPreBootstrap pre_bootstrap(rpc_proxy_, rs_list_, pt_);
//  ObAddr master_rs;
//  ASSERT_EQ(OB_SUCCESS, pt_.init(db_initer_.get_sql_proxy()));
//  ASSERT_EQ(OB_SUCCESS, init_partition_table());
//  EXPECT_CALL(rpc_proxy_, is_empty_server(_,_))
//      .Times(3)
//      .WillRepeatedly(DoAll(SetArgReferee<0>(true), Return(OB_SUCCESS)));
//  EXPECT_CALL(rpc_proxy_, create_partition_batch(_,_,_))
//      .Times(3)
//      .WillRepeatedly(Invoke(&rpc_proxy_, &MockObSrvRpcProxy::create_partition_batch_wrapper));
//
//  ASSERT_EQ(OB_SUCCESS, pre_bootstrap.prepare_bootstrap(master_rs));
//}
//
// TEST_F(TestBootstrap, pre_bootstrap_invalid_rs_list)
//{
//  ObServerInfoList rs_list;
//  ObPreBootstrap pre_bootstrap(rpc_proxy_, rs_list, pt_);
//  ObAddr master_rs;
//  // empty rs list
//  ASSERT_EQ(OB_INNER_STAT_ERROR, pre_bootstrap.prepare_bootstrap(master_rs));
//
//  // rs in same zone
//  ObServerInfo info;
//  info.zone_ = "1";
//  info.server_.set_ip_addr("10.10.1.1", 2500);
//  ASSERT_EQ(OB_SUCCESS, rs_list.push_back(info));
//  info.zone_ = "1";
//  info.server_.set_ip_addr("10.10.1.2", 2500);
//  ASSERT_EQ(OB_SUCCESS, rs_list.push_back(info));
//  info.zone_ = "3";
//  info.server_.set_ip_addr("10.10.1.3", 2500);
//  ASSERT_EQ(OB_SUCCESS, rs_list.push_back(info));
//  ObPreBootstrap pre_bootstrap1(rpc_proxy_, rs_list, pt_);
//  ASSERT_EQ(OB_PARTITION_ZONE_DUPLICATED, pre_bootstrap1.prepare_bootstrap(master_rs));
//}
//
// TEST_F(TestBootstrap, pre_bootstrap_failed_during_create_partition)
//{
//  ObAddr master_rs;
//  ObPreBootstrap pre_bootstrap(rpc_proxy_, rs_list_, pt_);
//
//  EXPECT_CALL(rpc_proxy_, is_empty_server(_, _))
//      .Times(3)
//      .WillRepeatedly(DoAll(SetArgReferee<0>(true), Return(OB_SUCCESS)));
//  EXPECT_CALL(rpc_proxy_, create_partition_batch(_, _, _))
//      .Times(1)
//      .WillRepeatedly(Return(OB_TIMEOUT));
//
//  ASSERT_EQ(OB_TIMEOUT, pre_bootstrap.prepare_bootstrap(master_rs));
//}
//
////TEST_F(TestBootstrap, pre_bootstrap_failed_during_wait_elect_master_partition)
////{
////  ObAddr master_rs;
////  MockObPreBootstrap pre_bootstrap(rpc_proxy_, rs_list_);
////
////  EXPECT_CALL(rpc_proxy_, is_empty_server(_,_,_))
////      .Times(3)
////      .WillRepeatedly(DoAll(SetArgReferee<2>(true), Return(OB_SUCCESS)));
////  EXPECT_CALL(rpc_proxy_, create_partition(_,_,_,_,_,_))
////      .Times(3)
////      .WillRepeatedly(Return(OB_SUCCESS));
////  EXPECT_CALL(pre_bootstrap, prepare_bootstrap(_))
////      .Times(1);
////  EXPECT_CALL(pre_bootstrap, check_is_all_server_empty(_))
////      .Times(1);
////  EXPECT_CALL(pre_bootstrap, wait_elect_master_partition(_))
////      .Times(1)
////      .WillRepeatedly(Return(OB_TIMEOUT));
////
////  ASSERT_EQ(OB_TIMEOUT, pre_bootstrap.prepare_bootstrap(master_rs));
////}
////
//
// TEST_F(TestBootstrap, pre_bootstrap_failed_during_check_empty)
//{
//  ObAddr master_rs;
//  ObPreBootstrap pre_bootstrap(rpc_proxy_, rs_list_, pt_);
//
//  // rpc failed
//  EXPECT_CALL(rpc_proxy_, is_empty_server(_, _))
//      .Times(1)
//      .WillRepeatedly(DoAll(SetArgReferee<0>(false), Return(OB_TIMEOUT)));
//  EXPECT_CALL(rpc_proxy_, create_partition_batch(_, _, _))
//      .Times(0)
//      .WillRepeatedly(Return(OB_SUCCESS));
//
//  ASSERT_EQ(OB_TIMEOUT, pre_bootstrap.prepare_bootstrap(master_rs));
//
//  // not empty
//  EXPECT_CALL(rpc_proxy_, is_empty_server(_, _))
//      .Times(1)
//      .WillRepeatedly(DoAll(SetArgReferee<0>(false), Return(OB_SUCCESS)));
//  EXPECT_CALL(rpc_proxy_, create_partition_batch(_, _, _))
//      .Times(0)
//      .WillRepeatedly(Return(OB_SUCCESS));
//
//  ASSERT_EQ(OB_INIT_TWICE, pre_bootstrap.prepare_bootstrap(master_rs));
//}
//
// void TestBootstrap::call_execute_bootstrap()
//{
//  // first set to (2, 2, 2), so than in execute_bootstrap, set_init_value will succeed
//  ObGlobalStatProxy stat_proxy(db_initer_.get_sql_proxy());
//  ASSERT_EQ(OB_SUCCESS, stat_proxy.set_init_value(2, 2, 2, 2));
//  ASSERT_EQ(OB_SUCCESS, pt_.init(db_initer_.get_sql_proxy()));
//  const int64_t bucket_num = 1024;
//  const int64_t max_cache_size = 1024 * 1024 * 512;
//  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
//  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
//  const char *cache_name = "location_cache";
//  int64_t priority = 1L;
//  ASSERT_EQ(OB_SUCCESS, fetcher_.init(db_initer_.get_config(), pt_, rs_mgr_, rs_rpc_proxy_, rpc_proxy_,
//  &locality_manager_)); ASSERT_EQ(OB_SUCCESS, loc_cache_.init(schema_service_, db_initer_.get_config(), alive_server_,
//  cache_name, priority, true, &locality_manager_));
//
//  ASSERT_EQ(OB_SUCCESS, init_partition_table());
//
//  // init server manager
//  ObFakeCB cb;
//  ObFakeServerChangeCB cb2;
//  ASSERT_EQ(OB_SUCCESS, server_mgr_.init(cb, cb2, db_initer_.get_sql_proxy(),
//                                         unit_mgr_, zone_mgr_, leader_coordinator_,
//                                         db_initer_.get_config(), A,
//                                         rpc_proxy_));
//  const int64_t now = ObTimeUtility::current_time();
//  const ObZone zone = "1";
//  ObServerStatusBuilder server_builder;
//  ASSERT_EQ(OB_SUCCESS, server_builder.init(db_initer_.get_config()));
//  server_builder.add(ObServerStatus::OB_SERVER_ACTIVE, now, A, zone)
//                .add(ObServerStatus::OB_SERVER_ACTIVE, now, B, zone)
//                .add(ObServerStatus::OB_SERVER_ACTIVE, now, C, zone);
//  server_builder.build(server_mgr_);
//  ObServerStatus status;
//  ASSERT_EQ(OB_SUCCESS, server_mgr_.get_server_status(A, status));
//  status.resource_info_.disk_total_ = OB_UNIT_MIN_DISK_SIZE;
//  ASSERT_EQ(OB_SUCCESS, server_mgr_.update_server_status(status));
//
//  MockLeaderCoordinator leader_coordinator;
//  ObBootstrap bootstrap(rpc_proxy_, rs_list_, ddl_service_, unit_mgr_, leader_coordinator,
//      db_initer_.get_config());
//  EXPECT_CALL(rpc_proxy_, create_partition_batch(_, _, _))
//      .WillRepeatedly(Invoke(&rpc_proxy_, &MockObSrvRpcProxy::create_partition_batch_wrapper));
//  //ON_CALL(leader_coordinator, coordinate(_))
//  //    .WillByDefault(Return(OB_SUCCESS));
//  ASSERT_EQ(OB_SUCCESS, bootstrap.execute_bootstrap());
//  ASSERT_NE(OB_SUCCESS, bootstrap.execute_bootstrap());
//}
//
// TEST_F(TestBootstrap, execute_bootstrap)
//{
//  call_execute_bootstrap();
//}
//
// TEST_F(TestBootstrap, standalone_root_service)
//{
//  db_initer_.get_config().enable_rootservice_standalone= true;
//  call_execute_bootstrap();
//}
//
// TEST_F(TestBootstrap, execute_bootstrap_invalid_rs_list)
//{
//  ObServerInfoList rs_list;
//  MockLeaderCoordinator leader_coordinator;
//  ObBootstrap bootstrap(rpc_proxy_, rs_list, ddl_service_, unit_mgr_, leader_coordinator,
//      db_initer_.get_config());
//  ASSERT_EQ(OB_INNER_STAT_ERROR, bootstrap.execute_bootstrap());
//}
//
// TEST_F(TestBootstrap, gen_sys_unit_ids)
//{
//  ObArray<ObZone> zones;
//  ASSERT_EQ(OB_SUCCESS, zones.push_back("zone2"));
//  ASSERT_EQ(OB_SUCCESS, zones.push_back("zone1"));
//  ASSERT_EQ(OB_SUCCESS, zones.push_back("zone3"));
//
//  ObArray<uint64_t> uids;
//  ASSERT_EQ(OB_SUCCESS, ObBaseBootstrap::gen_sys_unit_ids(zones, uids));
//  ASSERT_EQ(3, uids.count());
//  ASSERT_EQ(OB_SYS_UNIT_ID + 1, uids.at(0));
//  ASSERT_EQ(OB_SYS_UNIT_ID + 0, uids.at(1));
//  ASSERT_EQ(OB_SYS_UNIT_ID + 2, uids.at(2));
//}
TEST_F(TestBootstrap, construct_all_schema)
{
  MockLeaderCoordinator leader_coordinator;
  ObRsGtsManager gts;
  ObBootstrapArg arg;
  arg.server_list_.assign(rs_list_);
  arg.cluster_type_ = PRIMARY_CLUSTER;
  ObBootstrap bootstrap(
      rpc_proxy_, ddl_service_, unit_mgr_, leader_coordinator, db_initer_.get_config(), arg, gts, rs_rpc_proxy_);

  ObArray<ObTableSchema> table_schema;
  ASSERT_EQ(OB_SUCCESS, bootstrap.construct_all_schema(table_schema));
  int64_t size = 0;
  for (int64_t i = 0; i < table_schema.count(); i++) {
    size += table_schema.at(i).get_serialize_size();
    LOG_INFO("calc size", K(size));
  }
}
}  // end namespace rootserver
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  oceanbase::common::ObTenantManager::get_instance().init(10);
  oceanbase::common::ObTenantManager::get_instance().add_tenant(OB_SYS_TENANT_ID);
  oceanbase::common::ObTenantManager::get_instance().set_tenant_mem_limit(
      OB_SYS_TENANT_ID, 1024L * 1024L * 1024L, 1024L * 1024L * 1024L);
  return RUN_ALL_TESTS();
}
