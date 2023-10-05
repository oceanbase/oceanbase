#include "share/ob_thread_mgr.h"
#include "test_testbench_config_base.h"
#include "testbench/ob_testbench_server_provider.h"
#include "gtest/gtest.h"

namespace oceanbase {
namespace unittest {
class TestAsyncOperation : public TestConfig {
public:
  TestAsyncOperation() : tg_id(-1), sql_conn_pool() {}
  ~TestAsyncOperation() {}

  virtual void SetUp();
  virtual void Tear();

public:
  int tg_id;
  static const int64_t CONCURRENT_LINKS = 50;
  testbench::ObTestbenchSystableHelper systable_helper;
  testbench::ObTestbenchServerProvider server_provider;
  common::sqlclient::ObMySQLConnectionPool sql_conn_pool;
};

void TestAsyncOperation::SetUp() {
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
  ASSERT_EQ(OB_SUCCESS, sql_conn_pool.create_all_dblink_pool());
}

void TestAsyncOperation::Tear() {
  sql_conn_pool.close_all_connection();
  TG_STOP(tg_id);
  TG_WAIT(tg_id);
  server_provider.destroy();
  systable_helper.destroy();
}

TEST_F(TestAsyncOperation, basic_async_api) {
  ObSEArray<ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1>, 16>
      tenant_name_array("OBMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSEArray<uint64_t, 16> tenant_array("OBMySQLConnPool",
                                       OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenants(tenant_name_array));
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenant_ids(tenant_array));
  ASSERT_GE(tenant_array.count(), 0);
  ObString tenant_name_str = tenant_name_array[0].str();
  uint64_t tenant_id = tenant_array[0];
  ObSEArray<ObAddr, 16> server_array("OBMySQLConnPool",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS,
            server_provider.get_tenant_servers(tenant_id, server_array));
  ObAddr server = server_array[0];
  uint64_t dblink_id =
      common::sqlclient::DblinkKey(tenant_name_str, server).hash();
  common::sqlclient::ObISQLConnection *conn = nullptr;
  EXPECT_EQ(OB_SUCCESS,
            sql_conn_pool.acquire_dblink(
                dblink_id, common::sqlclient::dblink_param_ctx(), conn));
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
} 