#include "share/ob_thread_mgr.h"
#include "test_testbench_config_base.h"
#include "testbench/ob_testbench_server_provider.h"
#include <chrono>

namespace oceanbase {
namespace unittest {
class TestMySQLConnectionPool : public TestConfig {
public:
  TestMySQLConnectionPool() : tg_id(-1), sql_conn_pool() {}
  ~TestMySQLConnectionPool() {}

  virtual void SetUp();
  virtual void Tear();
  void TestBasicRead(common::sqlclient::ObMySQLConnection *conn);

public:
  int tg_id;
  const char *sql =
      "select column_value from oceanbase.__all_core_table where table_name = "
      "\"__all_global_stat\" and column_name = \"baseline_schema_version\";";
  static const int64_t CONCURRENT_LINKS = 50;
  testbench::ObTestbenchSystableHelper systable_helper;
  testbench::ObTestbenchServerProvider server_provider;
  common::sqlclient::ObMySQLConnectionPool sql_conn_pool;
};

void TestMySQLConnectionPool::SetUp() {
  TestConfig::SetUp();
  ASSERT_EQ(OB_SUCCESS, systable_helper.init_conn(mysql_config));
  ASSERT_EQ(OB_SUCCESS, server_provider.init(systable_helper));
  const char *user = "root";
  const char *pass = "";
  const char *db = "oceanbase";
  ASSERT_EQ(OB_SUCCESS, sql_conn_pool.set_db_param(user, pass, db));
  conn_pool_config.sqlclient_per_observer_conn_limit_ = CONCURRENT_LINKS;
  sql_conn_pool.update_config(conn_pool_config);
  sql_conn_pool.set_server_provider(&server_provider);
  ASSERT_EQ(OB_SUCCESS, TG_CREATE(lib::TGDefIDs::MysqlProxyPool, tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, sql_conn_pool.start(tg_id));
}

void TestMySQLConnectionPool::Tear() {
  sql_conn_pool.close_all_connection();
  TG_STOP(tg_id);
  TG_WAIT(tg_id);
  server_provider.destroy();
  systable_helper.destroy();
}

void TestMySQLConnectionPool::TestBasicRead(
    common::sqlclient::ObMySQLConnection *conn) {
  common::ObISQLClient::ReadResult res;
  res.reset();
  conn->execute_read(sql, res);
  common::sqlclient::ObMySQLResult *result = res.mysql_result();
  EXPECT_EQ(OB_SUCCESS, result->next());
  int64_t col_val = 0;
  EXPECT_EQ(OB_SUCCESS, result->get_int("column_value", col_val));
  EXPECT_GE(col_val, 0);
}

TEST_F(TestMySQLConnectionPool, acquire_mysql_connection) {
  EXPECT_EQ(3, sql_conn_pool.get_server_count());
  common::ObSEArray<uint64_t, 16> tenant_list;
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenant_ids(tenant_list));
  int64_t tenant_count = tenant_list.count();
  for (int64_t tenant_idx = 0; tenant_idx < tenant_count; ++tenant_idx) {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    common::sqlclient::ObMySQLConnection *conn = nullptr;
    EXPECT_EQ(OB_SUCCESS, tenant_list.at(tenant_idx, tenant_id));
    EXPECT_EQ(OB_SUCCESS, sql_conn_pool.acquire(tenant_id, conn));
    TestBasicRead(conn);
    EXPECT_EQ(OB_SUCCESS, sql_conn_pool.release(conn, true));
  }
  sql_conn_pool.stop();
}

TEST_F(TestMySQLConnectionPool, create_dblink_pool) {
  EXPECT_EQ(OB_SUCCESS, sql_conn_pool.create_all_dblink_pool());
  ObSEArray<ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1>, 16>
      tenant_name_array("OBMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSEArray<uint64_t, 16> tenant_array("OBMySQLConnPool",
                                       OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenants(tenant_name_array));
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenant_ids(tenant_array));
  for (int64_t tenant_idx = 0; tenant_idx < tenant_name_array.count();
       ++tenant_idx) {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name;
    EXPECT_EQ(OB_SUCCESS, tenant_name_array.at(tenant_idx, tenant_name));
    EXPECT_EQ(OB_SUCCESS, tenant_array.at(tenant_idx, tenant_id));
    ObString tenant_name_str = tenant_name.str();
    ObSEArray<ObAddr, 16> server_array("OBMySQLConnPool",
                                       OB_MALLOC_NORMAL_BLOCK_SIZE);
    EXPECT_EQ(OB_SUCCESS,
              server_provider.get_tenant_servers(tenant_id, server_array));
    for (int64_t server_idx = 0; server_idx < server_array.count();
         ++server_idx) {
      ObArray<void *> conn_array;
      ObAddr server;
      EXPECT_EQ(OB_SUCCESS, server_array.at(server_idx, server));
      uint64_t dblink_id =
          common::sqlclient::DblinkKey(tenant_name_str, server).hash();
      for (int64_t conn_idx = 0; conn_idx < CONCURRENT_LINKS; ++conn_idx) {
        common::sqlclient::ObMySQLConnection *conn = nullptr;
        EXPECT_EQ(OB_SUCCESS,
                  sql_conn_pool.acquire_dblink(
                      dblink_id, common::sqlclient::dblink_param_ctx(), conn));
        EXPECT_EQ(OB_SUCCESS, conn_array.push_back(conn));
      }
      for (int64_t conn_idx = 0; conn_idx < CONCURRENT_LINKS; ++conn_idx) {
        TestBasicRead(
            (common::sqlclient::ObMySQLConnection *)conn_array.at(conn_idx));
      }
      common::sqlclient::ObMySQLConnection *conn = nullptr;
      EXPECT_NE(OB_SUCCESS,
                sql_conn_pool.acquire_dblink(
                    dblink_id, common::sqlclient::dblink_param_ctx(), conn));
      for (int64_t conn_idx = 0; conn_idx < CONCURRENT_LINKS; ++conn_idx) {
        EXPECT_EQ(
            OB_SUCCESS,
            sql_conn_pool.release(
                (common::sqlclient::ObMySQLConnection *)conn_array.at(conn_idx),
                true));
      }
    }
  }
  sql_conn_pool.stop();
}

TEST_F(TestMySQLConnectionPool, dblink_pool_performance) {
  int64_t get_dblink_times = 10000;
  ::testing::Test::RecordProperty("get dblink times", get_dblink_times);
  ::testing::Test::RecordProperty("current links", CONCURRENT_LINKS);
  ASSERT_EQ(OB_SUCCESS, sql_conn_pool.create_all_dblink_pool());
  ObSEArray<ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1>, 16>
      tenant_name_array("OBMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSEArray<uint64_t, 16> tenant_array("OBMySQLConnPool",
                                       OB_MALLOC_NORMAL_BLOCK_SIZE);
  ASSERT_EQ(OB_SUCCESS, server_provider.get_tenants(tenant_name_array));
  ASSERT_EQ(OB_SUCCESS, server_provider.get_tenant_ids(tenant_array));
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name;
  ASSERT_EQ(OB_SUCCESS, tenant_name_array.at(0, tenant_name));
  ASSERT_EQ(OB_SUCCESS, tenant_array.at(0, tenant_id));
  ObString tenant_name_str = tenant_name.str();
  ObSEArray<ObAddr, 16> server_array("OBMySQLConnPool",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObAddr server;
  ASSERT_EQ(OB_SUCCESS,
            server_provider.get_tenant_servers(tenant_id, server_array));
  ASSERT_EQ(OB_SUCCESS, server_array.at(0, server));
  uint64_t dblink_id =
      common::sqlclient::DblinkKey(tenant_name_str, server).hash();

  ObArray<void *> conn_array;
  int64_t conn_idx = 0;
  for (int64_t conn_idx = 0; conn_idx < CONCURRENT_LINKS; ++conn_idx) {
    common::sqlclient::ObMySQLConnection *conn = nullptr;
    ASSERT_EQ(OB_SUCCESS,
              sql_conn_pool.acquire_dblink(
                  dblink_id, common::sqlclient::dblink_param_ctx(), conn));
    ASSERT_EQ(OB_SUCCESS, conn_array.push_back(conn));
  }
  auto start_time = std::chrono::high_resolution_clock::now();
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
  for (int64_t time = 0; time < get_dblink_times; ++time) {
    start_time = std::chrono::high_resolution_clock::now();
    conn_idx = (conn_idx + 1) % CONCURRENT_LINKS;
    common::sqlclient::ObMySQLConnection *conn = nullptr;
    ASSERT_EQ(
        OB_SUCCESS,
        sql_conn_pool.release(
            (common::sqlclient::ObMySQLConnection *)conn_array.at(conn_idx),
            true));
    ASSERT_EQ(OB_SUCCESS,
              sql_conn_pool.acquire_dblink(
                  dblink_id, common::sqlclient::dblink_param_ctx(), conn));
    conn_array[conn_idx] = conn;
    end_time = std::chrono::high_resolution_clock::now();
    duration += std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
  }
  for (int64_t conn_idx = 0; conn_idx < CONCURRENT_LINKS; ++conn_idx) {
    ASSERT_EQ(
        OB_SUCCESS,
        sql_conn_pool.release(
            (common::sqlclient::ObMySQLConnection *)conn_array.at(conn_idx),
            true));
  }
  ::testing::Test::RecordProperty("average acquire release latency",
                                  static_cast<double>(duration.count()) /
                                      get_dblink_times);
  sql_conn_pool.stop();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}