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
static int64_t user_table_id = OB_MIN_USER_TABLE_ID + 1;

ObServerConfig& config = ObServerConfig::get_instance();
DBInitializer db_initer;

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

TestPartitionLocationCache::TestPartitionLocationCache()
    : db_initer_(db_initer), prop_getter_(), pt_(prop_getter_), rs_mgr_(), rpc_proxy_(), cache_(fetcher_)
{}

void TestPartitionLocationCache::SetUp()
{
  int ret = OB_SUCCESS;
  ret = ObTenantManager::get_instance().init(1000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().add_tenant(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, db_initer.init());
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

TEST_F(TestPartitionLocationCache, check_non_paxos_replica)
{
  ObLocationFetcher fetcher;
  MockPartitionTableOperator pt(prop_getter_);
  ASSERT_EQ(OB_SUCCESS, fetcher.init(config, pt, rs_mgr_, rpc_proxy_, svr_rpc_proxy_, &locality_manager_));

  ObPartitionLocation location;
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
  ObReplicaLocation d_replica;
  d_replica.server_ = D;
  d_replica.replica_type_ = REPLICA_TYPE_READONLY;
  ObReplicaLocation e_replica;
  e_replica.server_ = E;
  e_replica.replica_type_ = REPLICA_TYPE_READONLY;
  ObReplicaLocation f_replica;
  f_replica.server_ = F;
  f_replica.replica_type_ = REPLICA_TYPE_READONLY;
  ASSERT_EQ(OB_SUCCESS, location.add(a_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(b_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(c_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(d_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(e_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(f_replica));
  add_server_to_locality(E, true);
  add_server_to_locality(D, true);
  // readonly count is missing one
  ObArray<ObReplicaMember> replica_member;
  ObReplicaMember d_replica_member(D, 0, common::REPLICA_TYPE_READONLY);
  ASSERT_EQ(OB_SUCCESS, replica_member.push_back(d_replica_member));
  bool is_same = false;
  ASSERT_EQ(OB_SUCCESS, fetcher.check_non_paxos_replica(location.get_replica_locations(), replica_member, is_same));
  ASSERT_FALSE(is_same);
  // readonly count is just right
  ObReplicaMember e_replica_member(E, 0, common::REPLICA_TYPE_READONLY);
  ASSERT_EQ(OB_SUCCESS, replica_member.push_back(e_replica_member));
  ASSERT_EQ(OB_SUCCESS, fetcher.check_non_paxos_replica(location.get_replica_locations(), replica_member, is_same));
  ASSERT_TRUE(is_same);
  // readonly count is one more
  ObReplicaMember f_replica_member(F, 0, common::REPLICA_TYPE_READONLY);
  ASSERT_EQ(OB_SUCCESS, replica_member.push_back(f_replica_member));
  ASSERT_EQ(OB_SUCCESS, fetcher.check_non_paxos_replica(location.get_replica_locations(), replica_member, is_same));
  ASSERT_FALSE(is_same);
}

TEST_F(TestPartitionLocationCache, skip_rpc_renew_v2)
{
  ObPartitionLocationCache cache(fetcher_);
  const char* cache_name = "location_cache_temp";
  int64_t priority = 1L;
  ASSERT_EQ(
      OB_SUCCESS, cache.init(schema_service_, config, alive_server_, cache_name, priority, true, &locality_manager_));

  ObPartitionLocation location;
  // build location;
  // two local replicas are online and one remote replica is offline
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
  ObArray<ObAddr> server_list;
  bool skip = true;
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(A));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(B));
  ASSERT_EQ(OB_SUCCESS, alive_server_.refresh(server_list));
  ASSERT_EQ(OB_SUCCESS, location.add(a_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(b_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(c_replica));
  ASSERT_EQ(OB_SUCCESS, cache.check_skip_rpc_renew_v2(location, skip));
  ASSERT_FALSE(skip);
  // without F replicas
  location.reset();
  ASSERT_EQ(OB_SUCCESS, location.add(c_replica));
  ASSERT_EQ(OB_SUCCESS, cache.check_skip_rpc_renew_v2(location, skip));
  ASSERT_TRUE(skip);
  location.reset();
  // one local F replica is online and one remote F replica is offline
  ASSERT_EQ(OB_SUCCESS, location.add(a_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(b_replica));
  ASSERT_EQ(OB_SUCCESS, location.add(c_replica));
  server_list.reset();
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(A));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(C));
  ASSERT_EQ(OB_SUCCESS, alive_server_.refresh(server_list));
  ASSERT_EQ(OB_SUCCESS, cache.check_skip_rpc_renew_v2(location, skip));
  ASSERT_TRUE(skip);
  // two F replicas are online and one local R replica is offline
  ObReplicaLocation d_replica;
  add_server_to_locality(D, true);
  d_replica.server_ = D;
  d_replica.replica_type_ = REPLICA_TYPE_READONLY;
  server_list.reset();
  ASSERT_EQ(OB_SUCCESS, location.add(d_replica));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(A));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(B));
  ASSERT_EQ(OB_SUCCESS, alive_server_.refresh(server_list));
  ASSERT_EQ(OB_SUCCESS, cache.check_skip_rpc_renew_v2(location, skip));
  ASSERT_FALSE(skip);
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm test_partition_location_cache_2.log -rf");
  oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  OB_LOGGER.set_file_name("test_partition_location_cache_2.log", true);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
