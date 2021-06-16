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
#include "common/ob_partition_key.h"
#include "share/ob_tenant_mgr.h"
#include "common/ob_timeout_ctx.h"
#include "share/config/ob_server_config.h"
#include "share/schema/db_initializer.h"
#include "../../share/schema/mock_schema_service.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/partition_table/ob_location_update_task.h"
#include "share/ob_alive_server_tracer.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_worker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "fake_part_property_getter.h"
#include "../mock_ob_rs_mgr.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgReferee;

namespace oceanbase {
namespace share {

using namespace common;
using namespace schema;
using namespace host;
using namespace obrpc;

static uint64_t& TEN = FakePartPropertyGetter::TEN();
static uint64_t& TID = FakePartPropertyGetter::TID();
static int64_t& PID = FakePartPropertyGetter::PID();
static const int64_t PART_NUM = 4;
static int64_t sys_table_id = OB_MAX_SYS_TABLE_ID - 1;
static int64_t user_table_id = OB_MIN_USER_TABLE_ID + 1;

ObServerConfig& config = ObServerConfig::get_instance();
DBInitializer db_initer;

class ObPartitionTableEnvironment : public ::testing::Environment {
public:
  ObPartitionTableEnvironment();
  virtual ~ObPartitionTableEnvironment()
  {}
  // Override this to define how to set up the environment.
  virtual void SetUp();
  // Override this to define how to tear down the environment.
  virtual void TearDown();
};

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

class MockPartitionTableOperator : public ObPartitionTableOperator {
public:
  MockPartitionTableOperator(ObIPartPropertyGetter& prop_getter) : ObPartitionTableOperator(prop_getter)
  {}

  MOCK_METHOD3(get, int(const uint64_t, const int64_t, ObPartitionInfo&));
};

class TestPartitionLocationCache : public ::testing::Test {
public:
  TestPartitionLocationCache();

  virtual void SetUp();
  virtual void TearDown();

protected:
  uint64_t TT(const uint64_t tid)
  {
    return is_sys_table(tid) ? OB_SYS_TENANT_ID : 2;
  }
  void basic_test(const uint64_t tid);
  void whole_table_test(const uint64_t tid);
  void check_location_with_readonly(
      const uint64_t tid, const int64_t pid, const ObPartitionLocation& location, const ObAddr& leader);

  void check_location(const uint64_t tid, const int64_t pid, const ObPartitionLocation& location, const ObAddr& leader);
  void check_location(const uint64_t tid, const int64_t pid, const ObPartitionLocation& location, const ObAddr& leader,
      const ObIArray<ObAddr>& member_list);
  void check_table_locations(const uint64_t tid, const ObIArray<ObPartitionLocation>& locations);
  void check_vtable_locations(const ObIArray<ObPartitionLocation>& l, const ObIArray<ObPartitionLocation>& r);
  void update_meta_table_with_prop_getter(FakePartPropertyGetter& prop_getter, const int64_t part_num);
  void prepare_for_cache_rpc_renew_location_case(obrpc::ObMemberListAndLeaderArg& prev_leader_member_info,
      obrpc::ObMemberListAndLeaderArg& rpc_member_info_a, obrpc::ObMemberListAndLeaderArg& rpc_member_info_b,
      obrpc::ObMemberListAndLeaderArg& rpc_member_info_c, int64_t& renew_time);

  void prepare_for_cache_rpc_renew_location_case_with_readonly(obrpc::ObMemberListAndLeaderArg& prev_leader_member_info,
      obrpc::ObMemberListAndLeaderArg& rpc_member_info_a, obrpc::ObMemberListAndLeaderArg& rpc_member_info_b,
      obrpc::ObMemberListAndLeaderArg& rpc_member_info_c, int64_t& renew_time);
  void clear_partition_table();
  void add_server_to_locality(const common::ObAddr& server, bool is_local = false);
  DBInitializer& db_initer_;
  FakePartPropertyGetter prop_getter_;
  ObPartitionTableOperator pt_;
  MockObRsMgr rs_mgr_;
  MockObCommonRpcProxy rpc_proxy_;
  ObLocationFetcher fetcher_;
  MockSchemaService schema_service_;
  ObPartitionLocationCache cache_;
  MockObSrvRpcProxy svr_rpc_proxy_;
  ObAliveServerMap alive_server_;
  MockLocalityManager locality_manager_;
};

void TestPartitionLocationCache::add_server_to_locality(const common::ObAddr& server, bool is_local)
{
  MockLocalityManager::ServerInfo info;
  info.server_ = server;
  info.is_local_ = is_local;
  locality_manager_.server_info_.push_back(info);
}

ObPartitionTableEnvironment::ObPartitionTableEnvironment()
{}

void ObPartitionTableEnvironment::SetUp()
{
  const bool only_core_tables = false;

  ASSERT_EQ(OB_SUCCESS, db_initer.init());
  ASSERT_EQ(OB_SUCCESS, db_initer.create_system_table(only_core_tables));
}

void ObPartitionTableEnvironment::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

TestPartitionLocationCache::TestPartitionLocationCache()
    : db_initer_(db_initer), prop_getter_(), pt_(prop_getter_), rs_mgr_(), rpc_proxy_(), cache_(fetcher_)
{}

void TestPartitionLocationCache::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestPartitionLocationCache::SetUp()
{
  int ret = OB_SUCCESS;
  ret = ObTenantManager::get_instance().init(1000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().add_tenant(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pt_.init(db_initer_.get_sql_proxy(), NULL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, alive_server_.init());

  ObTenantManager::get_instance().set_tenant_mem_limit(1, 102400000, 1024000000);

  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);

  const char* cache_name = "location_cache";
  int64_t priority = 1L;
  ASSERT_EQ(OB_SUCCESS, schema_service_.init());
  ASSERT_EQ(OB_SUCCESS, fetcher_.init(config, pt_, rs_mgr_, rpc_proxy_, svr_rpc_proxy_, &locality_manager_));
  ret = cache_.init(schema_service_, config, alive_server_, cache_name, priority, true, &locality_manager_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert sys table parititon info
  TEN = 1;
  TID = combine_id(TEN, sys_table_id);
  for (int64_t i = 0; i < PART_NUM; ++i) {
    PID = i;
    clear_partition_table();
    prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
    update_meta_table_with_prop_getter(prop_getter_, PART_NUM);
  }
#define GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, schema_version) \
  tenant_schema.set_tenant_id(tenant_id);                                        \
  tenant_schema.set_tenant_name(tenant_name);                                    \
  tenant_schema.set_schema_version(schema_version);                              \
  tenant_schema.set_locality("");                                                \
  tenant_schema.add_zone("zone");

  ObTenantSchema sys_tenant;
  ObSysVariableSchema sys_variable;
  GEN_TENANT_SCHEMA(sys_tenant, TEN, "sys", 0);
  ret = schema_service_.add_tenant_schema(sys_tenant, 0);
  sys_variable.set_tenant_id(TEN);
  sys_variable.set_schema_version(0);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  EXPECT_EQ(OB_SUCCESS, schema_service_.add_sys_variable_schema(sys_variable, 0));

  ObTableSchema sys_table_schema;
  sys_table_schema.set_tenant_id(TEN);
  sys_table_schema.set_table_id(TID);
  sys_table_schema.set_database_id(1);
  sys_table_schema.set_table_name("sys");
  sys_table_schema.set_part_level(PARTITION_LEVEL_ONE);
  sys_table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  sys_table_schema.get_part_option().set_part_num(PART_NUM);
  ret = schema_service_.add_table_schema(sys_table_schema, 0);

  // insert user table partition info
  TEN = 2;
  TID = combine_id(TEN, user_table_id);
  for (int64_t i = 0; i < PART_NUM; ++i) {
    PID = i;
    clear_partition_table();
    prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
    update_meta_table_with_prop_getter(prop_getter_, PART_NUM);
    ;
  }

  ObTenantSchema user_tenant;
  GEN_TENANT_SCHEMA(user_tenant, TEN, "user", 0);
  ret = schema_service_.add_tenant_schema(user_tenant, 0);
  sys_variable.set_tenant_id(TEN);
  sys_variable.set_schema_version(0);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  EXPECT_EQ(OB_SUCCESS, schema_service_.add_sys_variable_schema(sys_variable, 0));

  ObTableSchema user_table_schema;
  user_table_schema.set_tenant_id(TEN);
  user_table_schema.set_table_id(TID);
  user_table_schema.set_database_id(1);
  user_table_schema.set_table_name("user");
  user_table_schema.set_part_level(PARTITION_LEVEL_ONE);
  user_table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  user_table_schema.get_part_option().set_part_num(PART_NUM);
  ret = schema_service_.add_table_schema(user_table_schema, 0);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).WillRepeatedly(Return(OB_TIMEOUT));
}

void TestPartitionLocationCache::TearDown()
{
  ASSERT_EQ(OB_SUCCESS, cache_.destroy());
  ObKVGlobalCache::get_instance().destroy();
  ObTenantManager::get_instance().destroy();
}

void TestPartitionLocationCache::update_meta_table_with_prop_getter(
    FakePartPropertyGetter& prop_getter, const int64_t part_num)
{
  for (int64_t i = 0; i < prop_getter.get_replicas().count(); ++i) {
    prop_getter.get_replicas().at(i).partition_cnt_ = part_num;
    ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter.get_replicas().at(i)));
  }
}

void TestPartitionLocationCache::prepare_for_cache_rpc_renew_location_case(
    obrpc::ObMemberListAndLeaderArg& prev_leader_member_info, obrpc::ObMemberListAndLeaderArg& rpc_member_info_a,
    obrpc::ObMemberListAndLeaderArg& rpc_member_info_b, obrpc::ObMemberListAndLeaderArg& rpc_member_info_c,
    int64_t& renew_time)
{
  ObPartitionLocation location;
  bool is_cache_hit = false;
  TEN = 2;
  PID = 1;
  TID = combine_id(TEN, user_table_id);

  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(C));
  prev_leader_member_info.leader_ = A;
  prev_leader_member_info.self_ = A;
  prev_leader_member_info.replica_type_ = common::REPLICA_TYPE_FULL;
  rpc_member_info_a = prev_leader_member_info;
  rpc_member_info_b = prev_leader_member_info;
  rpc_member_info_b.self_ = B;
  rpc_member_info_c = prev_leader_member_info;
  rpc_member_info_c.self_ = C;

  // just fresh cache
  const int64_t expire_renew_time = 0;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_FALSE(is_cache_hit);
  check_location(TID, PID, location, A);
  renew_time = location.renew_time_;
}

void TestPartitionLocationCache::prepare_for_cache_rpc_renew_location_case_with_readonly(
    obrpc::ObMemberListAndLeaderArg& prev_leader_member_info, obrpc::ObMemberListAndLeaderArg& rpc_member_info_a,
    obrpc::ObMemberListAndLeaderArg& rpc_member_info_b, obrpc::ObMemberListAndLeaderArg& rpc_member_info_c,
    int64_t& renew_time)
{
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  ObReplicaMember member1(D, 0, REPLICA_TYPE_READONLY);
  ObReplicaMember member2(E, 0, REPLICA_TYPE_READONLY);
  ObReplicaMember member3(F, 0, REPLICA_TYPE_READONLY);
  prev_leader_member_info.lower_list_.push_back(member1);
  prev_leader_member_info.lower_list_.push_back(member2);
  prev_leader_member_info.lower_list_.push_back(member3);
  rpc_member_info_a.lower_list_.push_back(member1);
  rpc_member_info_b.lower_list_.push_back(member2);
  rpc_member_info_c.lower_list_.push_back(member3);
}
void TestPartitionLocationCache::clear_partition_table()
{
  // only use A,B,C,D,E server, no need check ret
  pt_.remove(TID, PID, A);
  pt_.remove(TID, PID, B);
  pt_.remove(TID, PID, C);
  pt_.remove(TID, PID, D);
  pt_.remove(TID, PID, E);
}

void TestPartitionLocationCache::basic_test(const uint64_t tid)
{
  TID = tid;
  PID = 0;
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  bool is_cache_hit = false;

  // invalid argument
  ASSERT_EQ(OB_INVALID_ARGUMENT, cache_.get(TID, OB_INVALID_INDEX, location, expire_renew_time, is_cache_hit));
  ASSERT_EQ(OB_INVALID_ARGUMENT, cache_.get(OB_INVALID_ID, PID, location, expire_renew_time, is_cache_hit));

  // force_renew false, not hit in cache
  location.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_FALSE(is_cache_hit);
  // force_renew false, hit in cache
  location.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  LOG_INFO("xx", K(location));
  check_location(TID, PID, location, A);
  ASSERT_TRUE(is_cache_hit);

  // force_renew true
  expire_renew_time = INT64_MAX;
  PID = 1;
  location.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  LOG_INFO("xx", K(location));
  LOG_INFO("xx", K(location.get_replica_locations().at(0)));
  check_location(TID, PID, location, A);
}

void TestPartitionLocationCache::whole_table_test(const uint64_t tid)
{
  TID = tid;
  ObArray<ObPartitionLocation> locations;
  obrpc::ObMemberListAndLeaderArg empty_arg;
  const int64_t expire_renew_time = 0;
  bool is_cache_hit = false;

  // get whole table partition locations
  locations.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, locations, expire_renew_time, is_cache_hit));
  ASSERT_FALSE(is_cache_hit);
  check_table_locations(TID, locations);
  locations.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, locations, expire_renew_time, is_cache_hit));
  ASSERT_TRUE(is_cache_hit);
  check_table_locations(TID, locations);
}

void TestPartitionLocationCache::check_location_with_readonly(
    const uint64_t tid, const int64_t pid, const ObPartitionLocation& location, const ObAddr& leader)
{
  check_location(tid, pid, location, leader);
  ObArray<ObReplicaMember> children_list;
  ObReplicaMember member1(D, 0, REPLICA_TYPE_READONLY);
  ObReplicaMember member2(E, 0, REPLICA_TYPE_READONLY);
  ObReplicaMember member3(F, 0, REPLICA_TYPE_READONLY);
  ASSERT_EQ(OB_SUCCESS, children_list.push_back((member1)));
  ASSERT_EQ(OB_SUCCESS, children_list.push_back((member2)));
  ASSERT_EQ(OB_SUCCESS, children_list.push_back((member3)));
  int64_t count = 0;
  for (int64_t i = 0; i < location.size(); i++) {
    const ObReplicaLocation& replica = location.get_replica_locations().at(i);
    bool found = false;
    bool is_local = false;
    locality_manager_.is_local_server(replica.server_, is_local);
    if (replica.replica_type_ != REPLICA_TYPE_READONLY) {
    } else if (!is_local) {
    } else {
      for (int64_t j = 0; j < children_list.count(); j++) {
        if (children_list.at(j).get_server() == replica.server_) {
          found = true;
          count++;
          break;
        }
      }
      ASSERT_EQ(true, found);
    }
  }
  ASSERT_EQ(3, count);
}

void TestPartitionLocationCache::check_location(
    const uint64_t tid, const int64_t pid, const ObPartitionLocation& location, const ObAddr& leader)
{
  ObArray<ObAddr> member_list;
  ASSERT_EQ(OB_SUCCESS, member_list.push_back((A)));
  ASSERT_EQ(OB_SUCCESS, member_list.push_back((B)));
  ASSERT_EQ(OB_SUCCESS, member_list.push_back((C)));
  check_location(tid, pid, location, leader, member_list);
}

void TestPartitionLocationCache::check_location(const uint64_t tid, const int64_t pid,
    const ObPartitionLocation& location, const ObAddr& leader, const ObIArray<ObAddr>& member_list)
{
  bool found = false;
  ObReplicaLocation replica_loc;

  ASSERT_EQ(location.get_table_id(), tid);
  ASSERT_EQ(location.get_partition_id(), pid);
  int64_t member = 0;
  for (int64_t i = 0; i < location.size(); ++i) {
    const ObReplicaLocation& replica = location.get_replica_locations().at(i);
    if (replica.replica_type_ == REPLICA_TYPE_FULL) {
      member++;
    }
  }
  ASSERT_EQ(member_list.count(), member);

  for (int64_t i = 0; i < location.size(); ++i) {
    const ObReplicaLocation& replica = location.get_replica_locations().at(i);
    found = false;
    if (replica.replica_type_ == REPLICA_TYPE_FULL) {
      for (int64_t j = 0; !found && j < member_list.count(); ++j) {
        if (replica.server_ == member_list.at(j)) {
          ASSERT_EQ(replica.server_ == leader ? LEADER : FOLLOWER, replica.role_);
          found = true;
        }
      }
      ASSERT_TRUE(found);
    }
  }
  if (leader.is_valid()) {
    ASSERT_EQ(OB_SUCCESS, location.get_leader(replica_loc));
    ASSERT_EQ(replica_loc.role_, LEADER);
    ASSERT_EQ(replica_loc.server_, leader);
  } else {
    ASSERT_EQ(OB_LOCATION_LEADER_NOT_EXIST, location.get_leader(replica_loc));
  }
}

void TestPartitionLocationCache::check_table_locations(
    const uint64_t tid, const ObIArray<ObPartitionLocation>& locations)
{
  ASSERT_EQ(PART_NUM, locations.count());
  for (int64_t i = 0; i < PART_NUM; ++i) {
    check_location(tid, i, locations.at(i), A);
  }
}

void TestPartitionLocationCache::check_vtable_locations(
    const ObIArray<ObPartitionLocation>& l, const ObIArray<ObPartitionLocation>& r)
{
  ASSERT_EQ(l.count(), r.count());
  for (int64_t i = 0; i < l.count(); ++i) {
    ASSERT_EQ(l.at(i).get_table_id(), r.at(i).get_table_id());
    ASSERT_EQ(l.at(i).get_partition_id(), r.at(i).get_partition_id());
    ASSERT_EQ(l.at(i).get_partition_cnt(), r.at(i).get_partition_cnt());
    ASSERT_EQ(l.at(i).size(), r.at(i).size());
    for (int64_t j = 0; j < l.at(i).size(); ++j) {
      ASSERT_EQ(l.at(i).get_replica_locations().at(j).server_, r.at(i).get_replica_locations().at(j).server_);
      ASSERT_EQ(l.at(i).get_replica_locations().at(j).role_, r.at(i).get_replica_locations().at(j).role_);
    }
  }
}

void delay_get_member_list_and_leader(
    const common::ObPartitionKey& partition_key, obrpc::ObMemberListAndLeaderArg& result, const ObRpcOpts& opts)
{
  UNUSED(partition_key);
  UNUSED(result);
  UNUSED(opts);
  LOG_INFO("start delay_get_member_list_and_leader");
  usleep(1000);  // 1ms
  LOG_INFO("finish delay_get_member_list_and_leader");
}

void timeout_get_member_list_and_leader(
    const common::ObPartitionKey& partition_key, obrpc::ObMemberListAndLeaderArg& result, const ObRpcOpts& opts)
{
  UNUSED(partition_key);
  UNUSED(result);
  UNUSED(opts);
  LOG_INFO("start timeout_get_member_list_and_leader");
  usleep(1100000);  // 1.1s
  LOG_INFO("finish timeout_get_member_list_and_leader");
}

void quick_timeout_get_member_list_and_leader(
    const common::ObPartitionKey& partition_key, obrpc::ObMemberListAndLeaderArg& result, const ObRpcOpts& opts)
{
  UNUSED(partition_key);
  UNUSED(result);
  UNUSED(opts);
  LOG_INFO("start quick_timeout_get_member_list_and_leader");
  usleep(50000);  // 50ms
  LOG_INFO("finish quick_timeout_get_member_list_and_leader");
}

TEST_F(TestPartitionLocationCache, init)
{
  ObPartitionLocationCache cache(fetcher_);
  const int64_t expire_renew_time = INT64_MAX;  // force renew
  ObPartitionLocation location;
  ObArray<ObPartitionLocation> locations;
  bool is_cache_hit = false;
  ASSERT_EQ(OB_NOT_INIT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  // ASSERT_EQ(OB_NOT_INIT, cache.get(TID, PART_NUM, PART_NUM, part_level, locations, expire_renew_time, is_cache_hit));
  ASSERT_EQ(OB_NOT_INIT, cache.nonblock_get(TID, PID, location));
  const char* cache_name = "location_cache_temp";
  int64_t priority = 1L;
  ASSERT_EQ(
      OB_SUCCESS, cache.init(schema_service_, config, alive_server_, cache_name, priority, true, &locality_manager_));
  ASSERT_EQ(OB_INIT_TWICE,
      cache.init(schema_service_, config, alive_server_, cache_name, priority, true, &locality_manager_));
  ASSERT_EQ(OB_SUCCESS, cache.destroy());
}

TEST_F(TestPartitionLocationCache, get_sys_table_location)
{
  TID = combine_id(1, sys_table_id);
  basic_test(TID);
}

TEST_F(TestPartitionLocationCache, get_user_table_location)
{
  TID = combine_id(2, user_table_id);
  basic_test(TID);
}

TEST_F(TestPartitionLocationCache, get_locations_of_whole_table)
{
  TID = combine_id(1, sys_table_id);
  whole_table_test(TID);
  LOG_INFO("sys table succeed");
  TID = combine_id(2, user_table_id);
  whole_table_test(TID);
  LOG_INFO("user table succeed");
}

TEST_F(TestPartitionLocationCache, nonblock_get)
{
  TID = combine_id(2, user_table_id);
  ObPartitionLocation location;
  ObArray<ObPartitionLocation> locations;
  const int64_t expire_renew_time = 0;
  ObPartitionKey pkey;
  ObAddr leader;
  bool is_cache_hit = false;

  // nonblock_get a single partition location
  ASSERT_EQ(OB_LOCATION_NOT_EXIST, cache_.nonblock_get(TID, PID, location));
  location.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  location.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, A);

  pkey.init(TID, PID, PART_NUM);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get_leader(pkey, leader));
  ASSERT_EQ(A, leader);
  pkey.part_id_ = PART_NUM;
  ASSERT_EQ(OB_LOCATION_NOT_EXIST, cache_.nonblock_get_leader(pkey, leader));

  // nonblock_get parititons of a table
  locations.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, locations, expire_renew_time, is_cache_hit));
  check_table_locations(TID, locations);
}

TEST_F(TestPartitionLocationCache, nonblock_renew)
{
  TID = combine_id(2, user_table_id);
  ObPartitionLocation location;
  const int64_t expire_renew_time = 0;

  // nonblock_get a single partition location
  ASSERT_EQ(OB_LOCATION_NOT_EXIST, cache_.nonblock_get(TID, PID, location));
  location.reset();
  ObPartitionKey partition(TID, PID, PID + 1);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(partition, expire_renew_time));
  // trigger again, queue will filter duplicated
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(partition, expire_renew_time));
  }
  sleep(1);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, A);
}

ObAddr global_rs;
// test fetch through rpc
TEST_F(TestPartitionLocationCache, vtable_fetch_location)
{
  int64_t expire_renew_time = 0;
  bool is_cache_hit = false;

  // virtual table id
  TID = combine_id(OB_SYS_TENANT_ID, 15000);
  // create fake virtual table location
  ObSArray<ObPartitionLocation> locations;
  for (int64_t i = 0; i < 2; ++i) {
    ObPartitionLocation location;
    location.set_table_id(TID);
    location.set_partition_id(i);
    location.set_partition_cnt(2);
    ObReplicaLocation replica_loc;
    replica_loc.role_ = LEADER;
    replica_loc.server_ = (i % 2 == 0 ? A : B);
    ASSERT_EQ(OB_SUCCESS, location.add(replica_loc));
    ASSERT_EQ(OB_SUCCESS, locations.push_back(location));
  }
  ON_CALL(rpc_proxy_, fetch_location(_, _, _)).WillByDefault(DoAll(SetArgReferee<1>(locations), Return(OB_SUCCESS)));
  rs_mgr_.global_rs().set_ip_addr("127.0.0.1", 5555);
  ON_CALL(rs_mgr_, get_master_root_server(_))
      .WillByDefault(Invoke(&rs_mgr_, &MockObRsMgr::get_master_root_server_wrapper));

  // not hit in cache, will get through rpc
  ObSArray<ObPartitionLocation> vtable_locations;
  vtable_locations.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, vtable_locations, expire_renew_time, is_cache_hit));
  check_vtable_locations(locations, vtable_locations);
  // hit in cache
  vtable_locations.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, vtable_locations, expire_renew_time, is_cache_hit));

  check_vtable_locations(locations, vtable_locations);
  // force renew
  const_cast<ObReplicaLocation&>(locations.at(0).get_replica_locations().at(0)).server_ = C;
  ON_CALL(rpc_proxy_, fetch_location(_, _, _)).WillByDefault(DoAll(SetArgReferee<1>(locations), Return(OB_SUCCESS)));
  vtable_locations.reset();
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, vtable_locations, expire_renew_time, is_cache_hit));
  check_vtable_locations(locations, vtable_locations);

  // clear location and noblock renew
  vtable_locations.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.clear_vtable_location(TID, expire_renew_time));
  ASSERT_EQ(OB_SUCCESS, cache_.get_from_vtable_cache(TID, vtable_locations));
  ASSERT_TRUE(vtable_locations.at(0).is_mark_fail());
  ObPartitionKey pkey(TID, 0, 1);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(pkey, expire_renew_time));
  usleep(1 * 1000 * 1000);
  vtable_locations.reset();
  expire_renew_time = 0;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, vtable_locations, expire_renew_time, is_cache_hit));
  ASSERT_TRUE(is_cache_hit);
  check_vtable_locations(locations, vtable_locations);

  // invalid argument
  // vtable_locations.reset();
  // part_num = 2;
  // ASSERT_EQ(OB_INVALID_ARGUMENT, cache_.get(TID, part_num, part_num, part_level, vtable_locations, expire_renew_time,
  // is_cache_hit));

  // mock rs rpc failed
  TID = combine_id(OB_SYS_TENANT_ID, 15000);
  ON_CALL(rpc_proxy_, fetch_location(_, _, _)).WillByDefault(Return(OB_ERROR));
  vtable_locations.reset();
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_ERROR, cache_.get(TID, vtable_locations, expire_renew_time, is_cache_hit));
}

TEST_F(TestPartitionLocationCache, deserialize)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  location.set_table_id(combine_id(OB_SYS_TENANT_ID, 1));
  location.set_partition_id(0);
  location.set_partition_cnt(1);
  location.set_renew_time(ObTimeUtility::current_time());
  ObReplicaLocation replica_loc;
  for (int64_t i = 0; i < 10; ++i) {
    replica_loc.server_.set_ip_addr("127.0.0.1", static_cast<int32_t>(5000 + i));
    replica_loc.role_ = (0 == i) ? LEADER : FOLLOWER;
    if (OB_FAIL(location.add(replica_loc))) {
      LOG_WARN("add replica location failed", K(replica_loc), K(ret));
    }
  }

  ObLocationCacheValue cache_value;
  char buffer[ObPartitionLocationCache::OB_MAX_LOCATION_SERIALIZATION_SIZE];
  ASSERT_EQ(OB_SUCCESS,
      cache_.location2cache_value(
          location, buffer, ObPartitionLocationCache::OB_MAX_LOCATION_SERIALIZATION_SIZE, cache_value));
  ObPartitionLocation got_location;
  ASSERT_EQ(OB_SUCCESS, cache_.cache_value2location(cache_value, got_location));
  ASSERT_EQ(location, got_location);

  location.replica_locations_.reuse();
  got_location.reset();
  ASSERT_EQ(OB_SUCCESS,
      cache_.location2cache_value(
          location, buffer, ObPartitionLocationCache::OB_MAX_LOCATION_SERIALIZATION_SIZE, cache_value));
  ASSERT_EQ(OB_SUCCESS, cache_.cache_value2location(cache_value, got_location));
  ASSERT_EQ(location, got_location);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case1)
{
  TEN = 2;
  PID = 1;
  TID = combine_id(TEN, user_table_id);
  const int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  bool is_cache_hit = false;

  LOG_INFO("test_cache_rpc_renew_location_case: fetch from meta table");
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  ASSERT_EQ(OB_SUCCESS, cache_.clear_location(TID, PID, expire_renew_time));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case31)
{
  int64_t expire_renew_time = INT64_MAX;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  PID = 1;
  prop_getter_.clear()
      .add(A, LEADER)
      .add(B, FOLLOWER)
      .add(C, FOLLOWER)
      .add(D, REPLICA_TYPE_READONLY)
      .add(E, REPLICA_TYPE_READONLY)
      .add(F, REPLICA_TYPE_READONLY);
  update_meta_table_with_prop_getter(prop_getter_, PART_NUM);
  ;

  prepare_for_cache_rpc_renew_location_case_with_readonly(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);
  add_server_to_locality(D, true);
  add_server_to_locality(E, true);
  add_server_to_locality(F, true);
  LOG_INFO("test_cache_rpc_renew_location_case:"
           "add new readonly replica, refresh meta_table");
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_FALSE(is_cache_hit);
  check_location_with_readonly(TID, PID, location, A);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "leader and member_list are same with cache; readonly same, RPC renew");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location_with_readonly(TID, PID, location, B);
  ASSERT_LT(renew_time, location.renew_time_);
}
TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case32)
{
  int64_t expire_renew_time = INT64_MAX;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case_with_readonly(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);
  add_server_to_locality(D, true);
  add_server_to_locality(E, true);
  add_server_to_locality(F, true);
  add_server_to_locality(G, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "leader and member_list are same with cache; readonly not same"
           "rpc three times; and get from meta-table");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  ObReplicaMember member3(G, 0, REPLICA_TYPE_READONLY);
  rpc_member_info_b.lower_list_.push_back(member3);
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case3)
{
  int64_t expire_renew_time = INT64_MAX;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           " member not change, but leader is changed: only need to send rpc to three members. "
           "B is leader now");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, B);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case4)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  bool is_cache_hit = false;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "member not change, old leader is changed, but not all member has same leader: "
           "need to send three rpc to all member and refresh from meta_table");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = C;
  rpc_member_info_c.leader_ = C;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, C);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case5)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  bool is_cache_hit = false;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           " member not change, but no leader exist: only need to send rpc to three members");
  ObAddr empty_server;
  rpc_member_info_a.leader_ = empty_server;
  rpc_member_info_b.leader_ = empty_server;
  rpc_member_info_c.leader_ = empty_server;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, empty_server);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case6)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  bool is_cache_hit = false;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "only two member is fetch succ, but leader is changed: "
           "need to send rpc to three members and fetch from meta_table");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.reset();
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_TIMEOUT)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case7)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);

  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);
  LOG_INFO("test_cache_rpc_renew_location_case: "
           "member is changed, but leader is not changed: "
           "need to send rpc to old leader and fetch from meta_table");
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, A));
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, B));
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, C));
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(D, FOLLOWER);
  update_meta_table_with_prop_getter(prop_getter_, PART_NUM);
  ;
  rpc_member_info_a.leader_ = A;
  rpc_member_info_a.member_list_.reset();
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_a.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_a.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_a.member_list_.push_back(D));
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case8)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "member is changed, and leader is changed too: "
           "need to send rpc to all server and fetch from meta_table");
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, A));
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, B));
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, C));
  prop_getter_.clear().add(A, FOLLOWER).add(B, FOLLOWER).add(E, LEADER);
  update_meta_table_with_prop_getter(prop_getter_, PART_NUM);
  ;
  rpc_member_info_a.leader_ = E;
  rpc_member_info_a.member_list_.reset();
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_a.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_a.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_a.member_list_.push_back(E));
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, E, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case9)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  ObAddr empty_server;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "member is not change, and no leader in cache and member: "
           "need to send rpc to old leader and fetch from meta_table");
  rpc_member_info_a.leader_.reset();
  rpc_member_info_b.leader_.reset();
  rpc_member_info_c.leader_.reset();
  // test when cache has leader
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, empty_server, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);

  // test when cache don't has leader
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, empty_server, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case10)
{
  int64_t expire_renew_time = 0;
  ;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  ObAddr empty_server;
  bool is_cache_hit = false;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           "member is not change, and no leader in cache, but has same leader in member: "
           "need to send rpc to old leader and fetch from meta_table");
  rpc_member_info_a.leader_.reset();
  rpc_member_info_b.leader_.reset();
  rpc_member_info_c.leader_.reset();
  // test when cache has leader
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_FALSE(is_cache_hit);
  check_location(TID, PID, location, empty_server, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);

  // test when cache don't has leader
  rpc_member_info_a.leader_ = C;
  rpc_member_info_b.leader_ = C;
  rpc_member_info_c.leader_ = C;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, C);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_cache_rpc_renew_location_case11)
{
  int64_t expire_renew_time = INT64_MAX;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test_cache_rpc_renew_location_case: "
           " member count is changed");
  rpc_member_info_a.leader_ = A;
  rpc_member_info_a.member_list_.reset();
  rpc_member_info_a.member_list_.push_back(A);
  rpc_member_info_a.member_list_.push_back(B);
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_mark_user_location_fail)
{
  const int64_t expire_renew_time = 0;
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  ObAddr empty_server;
  bool is_cache_hit = false;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);

  // leader changed
  cache_.clear_location(TID, PID, renew_time);
  LOG_INFO("start check, leader changed,");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, B, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_mark_user_location_fail_with_noblock_get)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  ObAddr empty_server;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);

  // leader changed
  cache_.clear_location(TID, PID, renew_time);
  LOG_INFO("start check");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_LOCATION_NOT_EXIST, cache_.nonblock_get(TID, PID, location));
  const int64_t sleep_us = 1000;
  usleep(sleep_us);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, B, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_mark_user_location_fail_with_noblock_renew)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  ObAddr empty_server;
  int64_t renew_time = 0;
  ObPartitionKey pkey;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  pkey.init(TID, PID, PART_NUM);

  // leader changed
  cache_.clear_location(TID, PID, renew_time);
  LOG_INFO("start check");
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));

  const int64_t expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(pkey, expire_renew_time));
  const int64_t sleep_us = 1000;
  usleep(sleep_us);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, B, rpc_member_info_a.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_replica_not_in_member_list)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  ObAddr empty_server;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  ObPartitionKey pkey;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);

  ObPartitionTableOperator pt(prop_getter_);
  ASSERT_EQ(OB_SUCCESS, pt.init(db_initer_.get_sql_proxy(), NULL));
  TEN = 2;
  PID = 1;
  TID = combine_id(TEN, user_table_id);
  prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
  update_meta_table_with_prop_getter(prop_getter_, PART_NUM);
  obrpc::ObMemberListAndLeaderArg rpc_member_info_e = prev_leader_member_info;
  prev_leader_member_info.member_list_.reset();
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(D));
  prev_leader_member_info.leader_ = A;
  prev_leader_member_info.self_ = A;
  rpc_member_info_a = prev_leader_member_info;
  prev_leader_member_info.member_list_.reset();
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(D));
  prev_leader_member_info.leader_ = A;
  prev_leader_member_info.self_ = A;
  rpc_member_info_b = prev_leader_member_info;
  rpc_member_info_b.self_ = B;
  rpc_member_info_d = prev_leader_member_info;
  rpc_member_info_d.self_ = D;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)));

  const int64_t expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A, rpc_member_info_e.member_list_);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_renew)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  ObAddr empty_server;
  int64_t renew_time = 0;
  bool is_cache_hit = false;
  ObPartitionKey pkey;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  int64_t expire_renew_time = 0;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("test renew 1");
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  ASSERT_EQ(OB_SUCCESS, cache_.clear_location(TID, PID, expire_renew_time));

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  renew_time = location.get_renew_time();
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);

  LOG_INFO("test renew 2");
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  renew_time = location.get_renew_time();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, location.get_renew_time(), is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  renew_time = location.get_renew_time();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, location.get_renew_time() - 1, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_EQ(renew_time, location.renew_time_);

  expire_renew_time = ObTimeUtility::current_time() - GCONF.location_cache_refresh_min_interval;
  renew_time = location.get_renew_time();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_EQ(renew_time, location.renew_time_);

  LOG_INFO("test renew 3");
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  renew_time = location.get_renew_time();
  const uint32_t sleep_us = static_cast<uint32_t>(GCONF.location_cache_refresh_min_interval);
  usleep(sleep_us);
  expire_renew_time = ObTimeUtility::current_time() - GCONF.location_cache_refresh_min_interval;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_timeout)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  ObAddr empty_server;
  int64_t renew_time = 0;
  ObPartitionKey pkey;
  bool is_cache_hit = false;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  int64_t expire_renew_time = 0;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  // update location cache with leader B
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, B);
  ASSERT_LT(renew_time, location.renew_time_);

  // default timeout
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(
          DoAll(Invoke(&timeout_get_member_list_and_leader), SetArgReferee<1>(rpc_member_info_a), Return(OB_TIMEOUT)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);

  // update location cache with leader B
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, B);
  ASSERT_LT(renew_time, location.renew_time_);

  // set small timeout
  common::ObTimeoutCtx ctx;
  ctx.set_abs_timeout(ObTimeUtility::current_time() + 100000 /*100ms*/);
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(DoAll(
          Invoke(&quick_timeout_get_member_list_and_leader), SetArgReferee<1>(rpc_member_info_a), Return(OB_TIMEOUT)));
  expire_renew_time = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_get_leader)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  ObAddr empty_server;
  int64_t renew_time = 0;
  ObPartitionKey pkey;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  bool force_renew = false;
  ObAddr leader;
  common::ObPartitionKey partition;
  partition.init(TID, PID, PART_NUM);
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("start check", "abs_timeout", ObTimeoutCtx::get_ctx().get_abs_timeout());
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  ASSERT_EQ(OB_SUCCESS, cache_.get_leader(partition, leader, force_renew));

  force_renew = true;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.get_leader(partition, leader, force_renew));
}

TEST_F(TestPartitionLocationCache, test_nonblock_renew)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  ObAddr empty_server;
  int64_t renew_time = 0;
  ObPartitionKey pkey;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);
  int64_t expire_renew_time = 0;
  common::ObPartitionKey partition;
  bool is_cache_hit = false;
  partition.init(TID, PID, PART_NUM);

  LOG_INFO("start check");

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(partition, expire_renew_time));
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_FALSE(location.is_mark_fail());
  ASSERT_EQ(renew_time, location.get_renew_time());

  const uint32_t sleep_us = static_cast<uint32_t>(GCONF.location_cache_refresh_min_interval);
  usleep(sleep_us);
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;

  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(
          DoAll(Invoke(&delay_get_member_list_and_leader), SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(partition, expire_renew_time));
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  ASSERT_FALSE(location.is_mark_fail());
  ASSERT_EQ(renew_time, location.get_renew_time());
  usleep(10000);  // 10ms
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, B);
  ASSERT_LT(renew_time, location.renew_time_);

  rpc_member_info_a.leader_ = C;
  rpc_member_info_b.leader_ = C;
  rpc_member_info_c.leader_ = C;
  renew_time = location.renew_time_;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(
          DoAll(Invoke(&delay_get_member_list_and_leader), SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));

  LOG_INFO("step 1");
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_renew(partition, location.get_renew_time()));
  LOG_INFO("step 2");
  ASSERT_EQ(OB_LOCATION_NOT_EXIST, cache_.nonblock_get(TID, PID, location));
  LOG_INFO("step 3");
  LOG_INFO("step 4");
  usleep(10000);  // 10ms
  LOG_INFO("step 5");
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  LOG_INFO("step 6");
  check_location(TID, PID, location, C);
  ASSERT_LT(renew_time, location.renew_time_);
  renew_time = location.renew_time_;

  partition.init(TID, PID, PART_NUM);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(partition, location));
  check_location(TID, PID, location, C);
  ASSERT_EQ(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_sys_table)
{
  ObPartitionLocation location;
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  ObAddr empty_server;
  int64_t renew_time = 0;
  ObPartitionKey pkey;
  bool is_cache_hit = false;
  TEN = 1;
  PID = 1;
  TID = combine_id(TEN, sys_table_id);

  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, prev_leader_member_info.member_list_.push_back(C));
  prev_leader_member_info.leader_ = A;
  prev_leader_member_info.self_ = A;
  prev_leader_member_info.replica_type_ = common::REPLICA_TYPE_FULL;
  rpc_member_info_a = prev_leader_member_info;
  rpc_member_info_b = prev_leader_member_info;
  rpc_member_info_b.self_ = B;
  rpc_member_info_c = prev_leader_member_info;
  rpc_member_info_c.self_ = C;

  LOG_INFO("step 1");
  const int64_t expire_renew_time = 0;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  check_location(TID, PID, location, A);
  renew_time = location.renew_time_;

  LOG_INFO("step 2");
  ASSERT_EQ(OB_SUCCESS, cache_.clear_location(TID, PID, location.renew_time_ - 1));
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, A);
  ASSERT_EQ(renew_time, location.renew_time_);

  LOG_INFO("step 3");
  ASSERT_EQ(OB_SUCCESS, cache_.clear_location(TID, PID, location.renew_time_));
  rpc_member_info_a.leader_ = B;
  rpc_member_info_b.leader_ = B;
  rpc_member_info_c.leader_ = B;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);

  LOG_INFO("print member info", K(rpc_member_info_a), K(rpc_member_info_b), K(rpc_member_info_c));
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  ASSERT_EQ(OB_LOCATION_NOT_EXIST, cache_.nonblock_get(TID, PID, location));
  LOG_INFO("step 4");
  usleep(1000);  // 1ms
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  check_location(TID, PID, location, B);
  ASSERT_LT(renew_time, location.renew_time_);
}

TEST_F(TestPartitionLocationCache, test_other)
{
  ASSERT_EQ(OB_SUCCESS, cache_.reload_config());
}

TEST_F(TestPartitionLocationCache, test_location_sem)
{
  ObPartitionLocationCache::LocationSem sem;
  const int64_t no_timeout_us = 0;
  const int64_t max_count = 20;
  int64_t abs_timeout_us = ObTimeUtility::current_time() + 100;

  ASSERT_EQ(OB_ERR_UNEXPECTED, sem.acquire(no_timeout_us));
  sem.set_max_count(max_count);

  for (int64_t i = 0; i < max_count; ++i) {
    LOG_INFO("step 1");
    ASSERT_EQ(OB_SUCCESS, sem.acquire(no_timeout_us));
  }

  LOG_INFO("step 2");
  abs_timeout_us = ObTimeUtility::current_time() - 100;
  ASSERT_EQ(OB_TIMEOUT, sem.acquire(abs_timeout_us));

  for (int64_t i = 0; i < max_count; ++i) {
    LOG_INFO("step 3");
    abs_timeout_us = ObTimeUtility::current_time() + 100;
    ASSERT_EQ(OB_TIMEOUT, sem.acquire(abs_timeout_us));
  }

  LOG_INFO("step 4");
  ASSERT_EQ(OB_SUCCESS, sem.release());
  abs_timeout_us = ObTimeUtility::current_time() + 100;
  ASSERT_EQ(OB_SUCCESS, sem.acquire(abs_timeout_us));
  LOG_INFO("step 5");
  abs_timeout_us = ObTimeUtility::current_time() + 100;
  ASSERT_EQ(OB_TIMEOUT, sem.acquire(abs_timeout_us));

  for (int64_t i = 0; i < max_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, sem.release());
  }
  for (int64_t i = 0; i < max_count; ++i) {
    LOG_INFO("step 6");
    abs_timeout_us = ObTimeUtility::current_time() + 100;
    ASSERT_EQ(OB_SUCCESS, sem.acquire(abs_timeout_us));
  }
  for (int64_t i = 0; i < max_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, sem.release());
  }
  ASSERT_EQ(OB_ERR_UNEXPECTED, sem.release());
  for (int64_t i = 0; i < max_count; ++i) {
    LOG_INFO("step 7");
    abs_timeout_us = ObTimeUtility::current_time() + 100;
    ASSERT_EQ(OB_SUCCESS, sem.acquire(abs_timeout_us));
  }
  for (int64_t i = 0; i < max_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, sem.release());
  }
}

TEST_F(TestPartitionLocationCache, test_sql_error)
{
  ObPartitionLocation location;
  bool is_cache_hit = false;
  const char* cache_name = "location_cache_sql_error";
  int64_t priority = 1L;
  int64_t expire_renew_time = 0;
  ObLocationFetcher fetcher;
  ObPartitionLocationCache cache(fetcher);
  MockPartitionTableOperator pt(prop_getter_);
  ASSERT_EQ(OB_SUCCESS, fetcher.init(config, pt, rs_mgr_, rpc_proxy_, svr_rpc_proxy_, &locality_manager_));
  ASSERT_EQ(
      OB_SUCCESS, cache.init(schema_service_, config, alive_server_, cache_name, priority, true, &locality_manager_));

  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_CONNECT_ERROR));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_TIMEOUT));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_WAITQUEUE_TIMEOUT));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_SESSION_NOT_FOUND));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_TRANS_TIMEOUT));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_TRANS_STMT_TIMEOUT));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_TRANS_UNKNOWN));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
  EXPECT_CALL(pt, get(_, _, _)).Times(1).WillOnce(Return(OB_GET_LOCATION_TIME_OUT));
  ASSERT_EQ(OB_GET_LOCATION_TIME_OUT, cache.get(TID, PID, location, expire_renew_time, is_cache_hit));
}

TEST_F(TestPartitionLocationCache, leader_on_non_alive_server)
{
  TID = combine_id(2, user_table_id);
  PID = 0;
  ObPartitionLocation location;
  const int64_t expire_renew_time = 0;
  bool is_cache_hit = false;

  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).Times(0);
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  int64_t old_renew_time = location.renew_time_;
  usleep(static_cast<int32_t>(GCONF.location_cache_refresh_min_interval * 2));
  ObArray<ObAddr> server_list;
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(B));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(C));
  ASSERT_EQ(OB_SUCCESS, alive_server_.refresh(server_list));
  // renew for get
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_GT(location.renew_time_, old_renew_time);
  old_renew_time = location.renew_time_;

  // renew time new than server trace time, no renew
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_EQ(location.renew_time_, old_renew_time);

  usleep(static_cast<int32_t>(GCONF.location_cache_refresh_min_interval * 2));
  ASSERT_EQ(OB_SUCCESS, alive_server_.refresh(server_list));
  // trigger async update task for nonblock_get
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  ObLocationUpdateTask task(cache_, cache_.is_stopped_, TID, PID, ObTimeUtility::current_time());
  // wait async update task finish
  usleep(1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, cache_.nonblock_get(TID, PID, location));
  ASSERT_GT(location.renew_time_, old_renew_time);
}
TEST_F(TestPartitionLocationCache, renew_location_with_rpc_v2)
{
  ObLocationFetcher fetcher;
  MockPartitionTableOperator pt(prop_getter_);
  ASSERT_EQ(OB_SUCCESS, fetcher.init(config, pt, rs_mgr_, rpc_proxy_, svr_rpc_proxy_, &locality_manager_));
  obrpc::ObMemberListAndLeaderArg prev_leader_member_info;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_a;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_b;
  obrpc::ObMemberListAndLeaderArg rpc_member_info_c;
  int64_t renew_time = 0;
  prepare_for_cache_rpc_renew_location_case(
      prev_leader_member_info, rpc_member_info_a, rpc_member_info_b, rpc_member_info_c, renew_time);

  ObPartitionLocation location;
  location.set_table_id(TID);
  location.set_partition_id(PID);
  location.set_partition_cnt(1);

  ObReplicaLocation a_replica;
  a_replica.server_ = A;
  a_replica.replica_type_ = REPLICA_TYPE_FULL;
  ObReplicaLocation b_replica;
  b_replica.server_ = B;
  b_replica.replica_type_ = REPLICA_TYPE_FULL;
  ObReplicaLocation c_replica;
  c_replica.server_ = C;
  c_replica.replica_type_ = REPLICA_TYPE_FULL;
  add_server_to_locality(A, true);
  add_server_to_locality(B, true);
  add_server_to_locality(C, true);
  ASSERT_EQ(OB_SUCCESS, location.add(a_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(b_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(c_replica));
  // B is leader
  rpc_member_info_a.leader_ = C;
  rpc_member_info_b.leader_ = B;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  bool is_valid = false;
  ObPartitionLocation new_location;
  ASSERT_EQ(OB_SUCCESS, fetcher.renew_location_with_rpc_v2(location, new_location, is_valid));
  ASSERT_EQ(true, is_valid);
  check_location(TID, PID, new_location, B);
  // C's leader is A
  rpc_member_info_c.leader_ = A;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  is_valid = false;
  ASSERT_EQ(OB_SUCCESS, fetcher.renew_location_with_rpc_v2(location, new_location, is_valid));
  ASSERT_EQ(true, is_valid);
  check_location(TID, PID, new_location, B);
  // B's leader is A, and no one says it is the leader; RPC renew is considered to be success.
  rpc_member_info_b.leader_ = A;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  is_valid = false;
  ASSERT_EQ(OB_SUCCESS, fetcher.renew_location_with_rpc_v2(location, new_location, is_valid));
  ASSERT_EQ(true, is_valid);
  ObAddr empty;
  check_location(TID, PID, new_location, empty);

  obrpc::ObMemberListAndLeaderArg rpc_member_info_d;
  rpc_member_info_d.replica_type_ = common::REPLICA_TYPE_FULL;
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_d.member_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_d.member_list_.push_back(B));
  ASSERT_EQ(OB_SUCCESS, rpc_member_info_d.member_list_.push_back(D));
  // A says it's the leader and its member_list is changed.
  rpc_member_info_a = rpc_member_info_d;
  rpc_member_info_a.self_ = A;
  rpc_member_info_a.leader_ = A;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)));

  ASSERT_EQ(OB_SUCCESS, fetcher.renew_location_with_rpc_v2(location, new_location, is_valid));
  ASSERT_EQ(false, is_valid);

  // no one says it is the leader, but someone's member_list is changed.
  rpc_member_info_a.leader_ = B;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(3)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_b), Return(OB_SUCCESS)))
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_c), Return(OB_SUCCESS)));
  is_valid = false;
  ASSERT_EQ(OB_SUCCESS, fetcher.renew_location_with_rpc_v2(location, new_location, is_valid));
  ASSERT_EQ(false, is_valid);

  // member_list is changed
  add_server_to_locality(D, true);
  rpc_member_info_d.self_ = D;
  ObReplicaLocation d_replica;
  d_replica.server_ = D;
  d_replica.replica_type_ = REPLICA_TYPE_FULL;
  ASSERT_EQ(OB_SUCCESS, location.add(d_replica));
  rpc_member_info_a.leader_ = A;
  rpc_member_info_b.leader_ = A;
  rpc_member_info_c.leader_ = A;
  rpc_member_info_d.leader_ = A;
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _))
      .Times(1)
      .WillOnce(DoAll(SetArgReferee<1>(rpc_member_info_a), Return(OB_SUCCESS)));
  is_valid = false;
  ASSERT_EQ(OB_SUCCESS, fetcher.renew_location_with_rpc_v2(location, new_location, is_valid));
  ASSERT_EQ(false, is_valid);
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm test_partition_location_cache.log -rf");
  oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  OB_LOGGER.set_file_name("test_partition_location_cache.log", true);

  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new oceanbase::share::ObPartitionTableEnvironment());
  return RUN_ALL_TESTS();
}
