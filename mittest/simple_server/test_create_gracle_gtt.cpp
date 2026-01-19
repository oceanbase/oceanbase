// owner: yilan.zyn
// owner group: storage

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

#include <gmock/gmock.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "storage/tablet/ob_session_tablet_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/tablet/ob_session_tablet_info_map.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"

using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
using namespace share::schema;
using namespace common;
using namespace share;

static uint64_t g_tenant_id = OB_INVALID_ID;
static uint64_t g_gtt_table_id = OB_INVALID_ID;
static uint64_t g_gtt_table_id2 = OB_INVALID_ID;

common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool;
common::ObMySQLProxy sql_proxy; // keep session alive for oracle gtt

class TestCreateOracleGTT : public unittest::ObSimpleClusterTestBase
{
public:
  TestCreateOracleGTT() : unittest::ObSimpleClusterTestBase("test_create_oracle_gtt") {}
  int create_gtt_table(const char *table_name, uint64_t &table_id, uint64_t tenant_id);
  int get_table_id_from_db(const char *table_name, uint64_t &table_id, uint64_t tenant_id);
  int check_tablet_exists_in_tablet_to_ls(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id);
  int check_tablet_exists_in_gtt_operator(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id);
};

int TestCreateOracleGTT::create_gtt_table(const char *table_name, uint64_t &table_id, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  // Create global temporary table
  if (OB_FAIL(sql.assign_fmt(
    "CREATE GLOBAL TEMPORARY TABLE %s (id NUMBER, name VARCHAR2(100)) ON COMMIT PRESERVE ROWS",
    table_name))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret));
  } else {
    // Get table id
    sleep(2); // wait for schema refresh
    if (OB_FAIL(get_table_id_from_db(table_name, table_id, tenant_id))) {
      LOG_WARN("failed to get table id", K(ret));
    }
  }

  return ret;
}

int TestCreateOracleGTT::get_table_id_from_db(const char *table_name, uint64_t &table_id, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  table_id = OB_INVALID_ID;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();

  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table WHERE table_name = '%s' AND tenant_id = %lu",
      table_name, tenant_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(inner_sql_proxy.read(result, sql.ptr()))) {
      LOG_WARN("failed to read sql", K(ret));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      if (OB_FAIL(res.next())) {
        LOG_WARN("failed to get next result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
      }
    }
  }

  return ret;
}

int TestCreateOracleGTT::check_tablet_exists_in_tablet_to_ls(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  exists = false;
  ObSqlString sql;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();

  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt(
      "SELECT tablet_id FROM oceanbase.__all_virtual_tablet_to_ls WHERE tablet_id = %lu AND tenant_id = %lu",
      tablet_id.id(), tenant_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(inner_sql_proxy.read(result, sql.ptr()))) {
      LOG_WARN("failed to read sql", K(ret));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      if (OB_SUCCESS == res.next()) {
        exists = true;
      }
    }
  }

  return ret;
}

int TestCreateOracleGTT::check_tablet_exists_in_gtt_operator(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  exists = false;
  ObSqlString sql;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();

  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt(
      "SELECT tablet_id FROM oceanbase.__all_virtual_tablet_to_global_temporary_table WHERE tablet_id = %lu AND tenant_id = %lu",
      tablet_id.id(), tenant_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(inner_sql_proxy.read(result, sql.ptr()))) {
      LOG_WARN("failed to read sql", K(ret));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      if (OB_SUCCESS == res.next()) {
        exists = true;
      }
    }
  }

  return ret;
}

TEST_F(TestCreateOracleGTT, prepare_data)
{
  int ret = OB_SUCCESS;

  // Step 1: Create oracle tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant("oracle_tenant", "4G", "4G", true /* oracle_mode */));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id, "oracle_tenant"));
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));

  // Initialize sql connection pool
  sql_conn_pool.set_db_param("sys@oracle_tenant", "", "SYS");
  ObConnPoolConfigParam param;
  param.sqlclient_wait_timeout_ = 1000;
  param.long_query_timeout_ = 300*1000*1000;
  param.connection_refresh_interval_ = 200*1000;
  param.connection_pool_warn_time_ = 10*1000*1000;
  param.sqlclient_per_observer_conn_limit_ = 1000;
  common::ObAddr db_addr;
  db_addr.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(), get_curr_simple_server().get_mysql_port());
  ret = sql_conn_pool.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy.init(&sql_conn_pool);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  usleep(1 * 1000 * 1000); // wait for connection to be ready

  LOG_INFO("Created oracle tenant", K(g_tenant_id));

  // Create GTT table
  ASSERT_EQ(OB_SUCCESS, create_gtt_table("test_gtt_table", g_gtt_table_id, g_tenant_id));
  LOG_INFO("Created GTT table", K(g_gtt_table_id));

  ASSERT_EQ(OB_SUCCESS, create_gtt_table("test_gtt_table2", g_gtt_table_id2, g_tenant_id));
  LOG_INFO("Created GTT table2", K(g_gtt_table_id2));

  LOG_INFO("Prepare data completed");
}


TEST_F(TestCreateOracleGTT, create_and_delete_tablet)
{
  int ret = OB_SUCCESS;
  int64_t sequence = 1;
  uint32_t session_id = 12345;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10000000);

  // 从 sql_proxy 获取连接
  common::sqlclient::ObISQLConnection *conn = sql_proxy.get_connection();
  if (OB_NOT_NULL(conn)) {
    session_id = conn->get_sessid();
    // 使用 session_id
  }

  ObSessionTabletInfoMap session_tablet_map;
  ObSessionTabletCreateHelper create_helper(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);
  ASSERT_EQ(OB_SUCCESS, create_helper.do_work());
  const ObTabletID &tablet_id = create_helper.get_tablet_ids().at(0);
  const ObLSID &ls_id = create_helper.get_ls_id();

  LOG_INFO("Created session tablet", K(tablet_id), K(ls_id));

  // Check tablet exists in __all_tablet_to_ls
  bool exists = false;
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_id, exists, g_tenant_id));
  ASSERT_TRUE(exists);

  // Check tablet exists in __all_tablet_to_global_temporary_table
  exists = false;
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_id, exists, g_tenant_id));
  ASSERT_TRUE(exists);

  // Step 3: Delete session tablet using ObSessionTabletDeleteHelper
  ObSessionTabletInfo tablet_info;
  ASSERT_EQ(OB_SUCCESS, tablet_info.init(tablet_id, ls_id, g_gtt_table_id, sequence, session_id, 0));
  tablet_info.is_creator_ = true;

  ObSessionTabletDeleteHelper delete_helper(g_tenant_id, tablet_info);
  ASSERT_EQ(OB_SUCCESS, delete_helper.do_work());

  LOG_INFO("Deleted session tablet", K(tablet_id));

  // Verify tablet was deleted
  sleep(2); // wait for deletion to complete

  exists = true;
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_id, exists, g_tenant_id));
  ASSERT_FALSE(exists);

  exists = true;
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_id, exists, g_tenant_id));
  ASSERT_FALSE(exists);

  LOG_INFO("Test basic create and delete completed successfully");
}

// Test case 2: Create duplicate tablet
TEST_F(TestCreateOracleGTT, test_create_duplicate_tablet)
{
  int ret = OB_SUCCESS;

  // Create first session tablet
  int64_t sequence = 1;
  uint32_t session_id = 23456;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10000000);

  // 从 sql_proxy 获取连接
  common::sqlclient::ObISQLConnection *conn = sql_proxy.get_connection();
  if (OB_NOT_NULL(conn)) {
    session_id = conn->get_sessid();
    // 使用 session_id
  }

  ObSessionTabletInfoMap session_tablet_map;
  ObSessionTabletCreateHelper create_helper1(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);
  ASSERT_EQ(OB_SUCCESS, create_helper1.do_work());

  const ObTabletID &tablet_id1 = create_helper1.get_tablet_ids().at(0);
  const ObLSID &ls_id1 = create_helper1.get_ls_id();

  LOG_INFO("Created first session tablet", K(create_helper1));

  // Step 3: Try to create duplicate tablet with same table_id, sequence, session_id
  // This should fail with OB_ERR_PRIMARY_KEY_DUPLICATE
  ObArray<uint64_t> table_ids;
  table_ids.push_back(g_gtt_table_id2);
  ObSessionTabletCreateHelper create_helper2(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);
  create_helper2.set_table_ids(table_ids);
  ASSERT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, create_helper2.do_work());

  const ObTabletID &tablet_id2 = create_helper2.get_tablet_ids().at(0);

  // The second creation is expected to fail with duplicate entry error
  LOG_INFO("Second create result", K(ret), K(create_helper2));


  bool exists = true;
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_id1, exists, g_tenant_id));
  ASSERT_TRUE(exists);
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_id1, exists, g_tenant_id));
  ASSERT_TRUE(exists);

  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_id2, exists, g_tenant_id));
  ASSERT_FALSE(exists);
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_id2, exists, g_tenant_id));
  ASSERT_FALSE(exists);

  // Step 4: Cleanup - delete the tablet
  ObSessionTabletInfo tablet_info;
  ASSERT_EQ(OB_SUCCESS, tablet_info.init(tablet_id1, ls_id1, g_gtt_table_id, sequence, session_id, 0));
  tablet_info.is_creator_ = true;

  ObSessionTabletDeleteHelper delete_helper(g_tenant_id, tablet_info);
  ASSERT_EQ(OB_SUCCESS, delete_helper.do_work());

  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_id1, exists, g_tenant_id));
  ASSERT_FALSE(exists);
  ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_id1, exists, g_tenant_id));
  ASSERT_FALSE(exists);

  LOG_INFO("Test create duplicate tablet completed");
}

// Test case 3: Create multiple tablets for different sessions
TEST_F(TestCreateOracleGTT, test_create_multiple_sessions)
{
  int ret = OB_SUCCESS;

  // Create tablets for different sessions
  const int64_t sequence = 1;
  ObSessionTabletInfoMap session_tablet_map;
  ObSEArray<ObTabletID, 3> tablet_ids;
  ObSEArray<uint32_t, 3> session_ids;
  ObLSID ls_id;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10000000);

  for (int i = 0; i < 3; i++) {
    uint32_t session_id = 30000 + i;
    session_ids.push_back(session_id);

    ObSessionTabletCreateHelper create_helper(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);
    ASSERT_EQ(OB_SUCCESS, create_helper.do_work());

    const ObTabletID &tablet_id = create_helper.get_tablet_ids().at(0);
    tablet_ids.push_back(tablet_id);

    if (i == 0) {
      ls_id = create_helper.get_ls_id();
    }

    LOG_INFO("Created session tablet", K(i), K(tablet_id), K(session_id));

    // Verify each tablet exists
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_id, exists, g_tenant_id));
    ASSERT_TRUE(exists);
  }

  // Delete all tablets
  for (int i = 0; i < 3; i++) {
    ObSessionTabletInfo tablet_info;
    ASSERT_EQ(OB_SUCCESS, tablet_info.init(tablet_ids[i], ls_id, g_gtt_table_id, sequence, session_ids[i], 0));
    tablet_info.is_creator_ = true;

    ObSessionTabletDeleteHelper delete_helper(g_tenant_id, tablet_info);
    ASSERT_EQ(OB_SUCCESS, delete_helper.do_work());

    LOG_INFO("Deleted session tablet", K(i), K(tablet_ids[i]));
  }

  LOG_INFO("Test create multiple sessions completed");
}

// 测试并行的创建和删除tablet
TEST_F(TestCreateOracleGTT, test_parallel_create_and_delete_tablet)
{
  int ret = OB_SUCCESS;
  const int64_t sequence = 1;
  const int thread_count = 10;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30000000);

  ObSessionTabletInfoMap session_tablet_map;
  ObSEArray<ObTabletID, 10> tablet_ids;
  ObSEArray<ObLSID, 10> ls_ids;
  ObSEArray<uint32_t, 10> session_ids;
  std::mutex tablet_ids_mutex;
  std::atomic<int> create_success_count(0);
  std::atomic<int> create_fail_count(0);

  // Phase 1: Parallel creation of tablets
  LOG_INFO("Starting parallel tablet creation");
  std::vector<std::thread> create_threads;

  for (int i = 0; i < thread_count; i++) {
    create_threads.emplace_back([&, i]() {
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30000000);
      int local_ret = OB_SUCCESS;
      uint32_t session_id = 40000 + i;

      ObSessionTabletCreateHelper create_helper(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);

      if (OB_SUCCESS == (local_ret = create_helper.do_work())) {
        const ObTabletID &tablet_id = create_helper.get_tablet_ids().at(0);
        const ObLSID &ls_id = create_helper.get_ls_id();

        {
          std::lock_guard<std::mutex> lock(tablet_ids_mutex);
          tablet_ids.push_back(tablet_id);
          ls_ids.push_back(ls_id);
          session_ids.push_back(session_id);
        }

        create_success_count++;
        LOG_INFO("Thread created tablet successfully", K(i), K(tablet_id), K(session_id));
      } else {
        create_fail_count++;
        LOG_WARN("Thread failed to create tablet", K(local_ret), K(i), K(session_id));
      }
    });
  }

  // Wait for all creation threads to complete
  for (auto &thread : create_threads) {
    thread.join();
  }

  LOG_INFO("Parallel creation completed", K(create_success_count.load()), K(create_fail_count.load()));
  ASSERT_EQ(thread_count, create_success_count.load());
  ASSERT_EQ(0, create_fail_count.load());
  ASSERT_EQ(thread_count, tablet_ids.count());

  // Verify all tablets exist
  for (int i = 0; i < tablet_ids.count(); i++) {
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_ids[i], exists, g_tenant_id));
    ASSERT_TRUE(exists);

    exists = false;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_ids[i], exists, g_tenant_id));
    ASSERT_TRUE(exists);

    LOG_INFO("Verified tablet exists", K(i), K(tablet_ids[i]));
  }

  // Phase 2: Parallel deletion of tablets
  LOG_INFO("Starting parallel tablet deletion");
  std::atomic<int> delete_success_count(0);
  std::atomic<int> delete_fail_count(0);
  std::vector<std::thread> delete_threads;

  for (int i = 0; i < tablet_ids.count(); i++) {
    delete_threads.emplace_back([&, i]() {
      int local_ret = OB_SUCCESS;

      ObSessionTabletInfo tablet_info;
      if (OB_FAIL(tablet_info.init(tablet_ids[i], ls_ids[i], g_gtt_table_id, sequence, session_ids[i], 0))) {
        LOG_WARN("Failed to init tablet_info", K(local_ret), K(i));
        delete_fail_count++;
        return;
      }
      tablet_info.is_creator_ = true;

      ObSessionTabletDeleteHelper delete_helper(g_tenant_id, tablet_info);
      if (OB_SUCCESS == (local_ret = delete_helper.do_work())) {
        delete_success_count++;
        LOG_INFO("Thread deleted tablet successfully", K(i), K(tablet_ids[i]));
      } else {
        delete_fail_count++;
        LOG_WARN("Thread failed to delete tablet", K(local_ret), K(i), K(tablet_ids[i]));
      }
    });
  }

  // Wait for all deletion threads to complete
  for (auto &thread : delete_threads) {
    thread.join();
  }

  LOG_INFO("Parallel deletion completed", K(delete_success_count.load()), K(delete_fail_count.load()));
  ASSERT_EQ(thread_count, delete_success_count.load());
  ASSERT_EQ(0, delete_fail_count.load());

  // Verify all tablets are deleted
  for (int i = 0; i < tablet_ids.count(); i++) {
    bool exists = true;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_tablet_to_ls(tablet_ids[i], exists, g_tenant_id));
    ASSERT_FALSE(exists);

    exists = true;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablet_ids[i], exists, g_tenant_id));
    ASSERT_FALSE(exists);

    LOG_INFO("Verified tablet deleted", K(i), K(tablet_ids[i]));
  }

  LOG_INFO("Test parallel create and delete completed successfully");
}

// 测试混合并发场景：同时进行创建和删除
TEST_F(TestCreateOracleGTT, test_mixed_parallel_create_and_delete)
{
  int ret = OB_SUCCESS;
  const int64_t sequence = 1;
  const int create_count = 5;
  const int delete_count = 3;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30000000);

  // First, prepare some tablets to delete
  ObSessionTabletInfoMap session_tablet_map;
  ObSEArray<ObTabletID, 10> tablets_to_delete;
  ObSEArray<ObLSID, 10> ls_ids_to_delete;
  ObSEArray<uint32_t, 10> session_ids_to_delete;

  LOG_INFO("Preparing tablets for deletion test");
  for (int i = 0; i < delete_count; i++) {
    uint32_t session_id = 50000 + i;
    ObSessionTabletCreateHelper create_helper(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);
    ASSERT_EQ(OB_SUCCESS, create_helper.do_work());

    tablets_to_delete.push_back(create_helper.get_tablet_ids().at(0));
    ls_ids_to_delete.push_back(create_helper.get_ls_id());
    session_ids_to_delete.push_back(session_id);

    LOG_INFO("Prepared tablet for deletion", K(i), K(tablets_to_delete[i]));
  }

  // Now start mixed parallel operations
  std::mutex result_mutex;
  ObSEArray<ObTabletID, 10> newly_created_tablets;
  ObSEArray<ObLSID, 10> newly_created_ls_ids;
  ObSEArray<uint32_t, 10> newly_created_session_ids;
  std::atomic<int> create_success(0);
  std::atomic<int> delete_success(0);
  std::vector<std::thread> threads;

  LOG_INFO("Starting mixed parallel create and delete operations");

  // Launch create threads
  for (int i = 0; i < create_count; i++) {
    threads.emplace_back([&, i]() {
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30000000);
      uint32_t session_id = 60000 + i;
      ObSessionTabletCreateHelper create_helper(g_tenant_id, g_gtt_table_id, sequence, session_id, session_tablet_map);

      if (OB_SUCCESS == create_helper.do_work()) {
        const ObTabletID &tablet_id = create_helper.get_tablet_ids().at(0);
        const ObLSID &ls_id = create_helper.get_ls_id();

        {
          std::lock_guard<std::mutex> lock(result_mutex);
          newly_created_tablets.push_back(tablet_id);
          newly_created_ls_ids.push_back(ls_id);
          newly_created_session_ids.push_back(session_id);
        }

        create_success++;
        LOG_INFO("Create thread succeeded", K(i), K(tablet_id));
      }
    });
  }

  // Launch delete threads
  for (int i = 0; i < delete_count; i++) {
    threads.emplace_back([&, i]() {
      ObSessionTabletInfo tablet_info;
      if (OB_SUCCESS == tablet_info.init(tablets_to_delete[i], ls_ids_to_delete[i],
                                          g_gtt_table_id, sequence, session_ids_to_delete[i], 0)) {
        tablet_info.is_creator_ = true;
        ObSessionTabletDeleteHelper delete_helper(g_tenant_id, tablet_info);

        if (OB_SUCCESS == delete_helper.do_work()) {
          delete_success++;
          LOG_INFO("Delete thread succeeded", K(i), K(tablets_to_delete[i]));
        }
      }
    });
  }

  // Wait for all operations to complete
  for (auto &thread : threads) {
    thread.join();
  }

  LOG_INFO("Mixed operations completed", K(create_success.load()), K(delete_success.load()));
  ASSERT_EQ(create_count, create_success.load());
  ASSERT_EQ(delete_count, delete_success.load());

  // Verify newly created tablets exist
  for (int i = 0; i < newly_created_tablets.count(); i++) {
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(newly_created_tablets[i], exists, g_tenant_id));
    ASSERT_TRUE(exists);
    LOG_INFO("Verified newly created tablet exists", K(i), K(newly_created_tablets[i]));
  }

  // Verify deleted tablets don't exist
  for (int i = 0; i < tablets_to_delete.count(); i++) {
    bool exists = true;
    ASSERT_EQ(OB_SUCCESS, check_tablet_exists_in_gtt_operator(tablets_to_delete[i], exists, g_tenant_id));
    ASSERT_FALSE(exists);
    LOG_INFO("Verified deleted tablet doesn't exist", K(i), K(tablets_to_delete[i]));
  }

  // Cleanup newly created tablets
  for (int i = 0; i < newly_created_tablets.count(); i++) {
    ObSessionTabletInfo tablet_info;
    ASSERT_EQ(OB_SUCCESS, tablet_info.init(newly_created_tablets[i], newly_created_ls_ids[i],
                                            g_gtt_table_id, sequence, newly_created_session_ids[i], 0));
    tablet_info.is_creator_ = true;
    ObSessionTabletDeleteHelper delete_helper(g_tenant_id, tablet_info);
    ASSERT_EQ(OB_SUCCESS, delete_helper.do_work());
  }

  LOG_INFO("Test mixed parallel create and delete completed successfully");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
