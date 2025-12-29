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

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
