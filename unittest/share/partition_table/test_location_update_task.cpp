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
#include "lib/stat/ob_session_stat.h"
#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "share/schema/db_initializer.h"
#include "../../share/schema/mock_schema_service.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/partition_table/ob_location_update_task.h"
#include "share/ob_alive_server_tracer.h"
#include "fake_part_property_getter.h"
#include "../mock_ob_rs_mgr.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"

using ::testing::_;
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

ObServerConfig& config = ObServerConfig::get_instance();
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
class TestLocationUpdateTask : public ::testing::Test {
public:
  TestLocationUpdateTask();

  virtual void SetUp();
  virtual void TearDown();

protected:
  void basic_test(const uint64_t tid);
  void whole_table_test(const uint64_t tid);
  void check_location(const uint64_t tid, const int64_t pid, const ObPartitionLocation& location);
  void check_table_locations(const uint64_t tid, const ObIArray<ObPartitionLocation>& locations);
  DBInitializer db_initer_;
  FakePartPropertyGetter prop_getter_;
  ObPartitionTableOperator pt_;
  MockObRsMgr rs_mgr_;
  MockObCommonRpcProxy rpc_proxy_;
  ObLocationFetcher fetcher_;
  ObPartitionLocationCache cache_;
  ObAliveServerMap alive_server_;
  MockObSrvRpcProxy svr_rpc_proxy_;
  MockSchemaService schema_service_;
  obrpc::ObMemberListAndLeaderArg empty_member_info_;
  MockLocalityManager locality_manager_;
};

TestLocationUpdateTask::TestLocationUpdateTask()
    : db_initer_(), prop_getter_(), pt_(prop_getter_), rs_mgr_(), rpc_proxy_(), fetcher_(), cache_(fetcher_)
{}

void TestLocationUpdateTask::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pt_.init(db_initer_.get_sql_proxy(), NULL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObKVGlobalCache::get_instance().init(1000, 128 * 1024 * 1024);
  const char* cache_name = "location_cache";
  int64_t priority = 1L;
  ASSERT_EQ(OB_SUCCESS, fetcher_.init(config, pt_, rs_mgr_, rpc_proxy_, svr_rpc_proxy_, &locality_manager_));
  ASSERT_EQ(OB_SUCCESS, alive_server_.init());
  ASSERT_EQ(OB_SUCCESS, schema_service_.init());
  ret = cache_.init(schema_service_, config, alive_server_, cache_name, priority, true, &locality_manager_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert user table partition info
  TEN = 2;
  TID = combine_id(TEN, 3003);
  for (int64_t i = 0; i < PART_NUM; ++i) {
    PID = i;
    prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
    for (int64_t j = 0; j < prop_getter_.get_replicas().count(); ++j) {
      ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter_.get_replicas().at(j)));
    }
  }
  EXPECT_CALL(svr_rpc_proxy_, get_member_list_and_leader(_, _, _)).WillRepeatedly(Return(OB_TIMEOUT));
}

void TestLocationUpdateTask::TearDown()
{
  ASSERT_EQ(OB_SUCCESS, cache_.destroy());
  ObKVGlobalCache::get_instance().destroy();
}

TEST_F(TestLocationUpdateTask, basic)
{
  uint64_t tid = OB_INVALID_ID;
  int64_t pid = OB_INVALID_INDEX;
  const bool is_stopped = false;
  const int64_t now = ObTimeUtility::current_time();
  ObLocationUpdateTask task0(cache_, is_stopped, tid, pid, now);
  ASSERT_FALSE(task0.is_valid());
  tid = combine_id(1, 1);
  ObLocationUpdateTask task1(cache_, is_stopped, tid, pid, now);
  ASSERT_FALSE(task1.is_valid());
  pid = 0;
  ObLocationUpdateTask task2(cache_, is_stopped, tid, pid, now);
  ASSERT_TRUE(task2.is_valid());

  ASSERT_FALSE(task0 == task1);
  ASSERT_FALSE(task1 == task2);
  ASSERT_TRUE(task2 == task2);
  ObLocationUpdateTask task3(cache_, is_stopped, tid, pid, now);
  ASSERT_TRUE(task2 == task3);

  char buf[128];
  int64_t buf_size = 0;
  ASSERT_EQ(NULL, task3.deep_copy(NULL, buf_size));
  ASSERT_EQ(NULL, task3.deep_copy(buf, buf_size));
  buf_size = sizeof(task3);
  IObDedupTask* task = task3.deep_copy(buf, buf_size);
  ASSERT_FALSE(NULL == task);
  ASSERT_TRUE(task3 == *task);

  ASSERT_EQ(OB_INVALID_ARGUMENT, task0.process());
}

TEST_F(TestLocationUpdateTask, process)
{
  const int64_t expire_renew_time = 0;
  ObPartitionLocation location;
  bool is_cache_hit = false;
  TEN = 2;
  TID = combine_id(TEN, 3003);
  PID = 0;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_EQ(3, location.size());
  ASSERT_EQ(OB_SUCCESS, pt_.remove(TID, PID, C));
  const bool is_stopped = false;
  const int64_t now = ObTimeUtility::current_time();
  ObLocationUpdateTask task(cache_, is_stopped, TID, PID, now);
  ASSERT_EQ(OB_SUCCESS, task.process());
  location.reset();
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, location, expire_renew_time, is_cache_hit));
  ASSERT_EQ(3, location.size());
  GCONF.internal_sql_execute_timeout = 1 * 1000 * 1000;  // 1s
  sleep(2);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ObPartitionLocation new_location;
  ASSERT_EQ(OB_SUCCESS, cache_.get(TID, PID, new_location, expire_renew_time, is_cache_hit));
  ASSERT_EQ(2, new_location.size());

  ObLocationUpdateTask wait_long_task(cache_, is_stopped, TID, PID, now - ObLocationUpdateTask::WAIT_PROCESS_WARN_TIME);
  ASSERT_EQ(OB_SUCCESS, wait_long_task.process());
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
