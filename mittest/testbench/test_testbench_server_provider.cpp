#include "testbench/ob_testbench_server_provider.h"
#include "gtest/gtest.h"

namespace oceanbase {
namespace unittest {

class TestServerProvider : public ::testing::Test {
public:
  TestServerProvider() {}
  ~TestServerProvider() {}
  virtual void SetUp();
  virtual void Tear();

public:
  libobcdc::MySQLConnConfig mysql_config;
  testbench::ObTestbenchSystableHelper systable_helper;
  testbench::ObTestbenchServerProvider server_provider;
};

void TestServerProvider::SetUp() {
  common::ObAddr addr;
  const char *host = "127.0.0.1";
  const int32_t port = 2881;
  addr.set_ip_addr(host, port);
  const int64_t sql_conn_timeout_us = 10L * 1000 * 1000;
  const int64_t sql_query_timeout_us = 10L * 1000 * 1000;
  const char *user = "root@sys";
  const char *pass = "";
  const char *db = "oceanbase";
  mysql_config.reset(addr, user, pass, db, sql_conn_timeout_us / 1000000L,
                     sql_query_timeout_us / 1000000L);
  systable_helper.init_conn(mysql_config);
  server_provider.init(systable_helper);
}

void TestServerProvider::Tear() {
  server_provider.destroy();
  systable_helper.destroy();
}

TEST_F(TestServerProvider, refresh) {
  EXPECT_EQ(OB_SUCCESS, server_provider.prepare_refresh());
  EXPECT_EQ(OB_SUCCESS, server_provider.end_refresh());
  common::ObSEArray<uint64_t, 16> tenant_list;
  common::ObSEArray<testbench::ObTenantName, 16> tenant_name_list;
  int64_t count = server_provider.get_server_count();
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenant_ids(tenant_list));
  EXPECT_EQ(OB_SUCCESS, server_provider.get_tenants(tenant_name_list));
  TESTBENCH_LOG(INFO, "get tenant ids", K(tenant_list));
  TESTBENCH_LOG(INFO, "get tenant names", K(tenant_name_list));
  EXPECT_EQ(count, tenant_list.count());
  EXPECT_EQ(count, tenant_name_list.count());
  ObSEArray<ObAddr, 16> server_list;
  for (int64_t tenant_idx = 0; tenant_idx < count; ++tenant_idx) {
  }
}
} // namespace unittest
} // namespace oceanbase