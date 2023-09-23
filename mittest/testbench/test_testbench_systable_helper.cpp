#include "test_testbench_config_base.h"
#include "testbench/ob_testbench_systable_helper.h"
#include "gtest/gtest.h"

namespace oceanbase {
namespace unittest {

class TestSystableHelper : public TestConfig {
public:
  TestSystableHelper() : systable_helper() {}
  ~TestSystableHelper() {}

  virtual void SetUp();
  virtual void Tear();

public:
  testbench::ObTestbenchSystableHelper systable_helper;
};

void TestSystableHelper::SetUp() {
  TestConfig::SetUp();
  ASSERT_EQ(OB_SUCCESS, systable_helper.init_conn(mysql_config));
}

void TestSystableHelper::Tear() { systable_helper.destroy(); }

TEST_F(TestSystableHelper, query_cluster_info) {
  testbench::ClusterInfo cluster_info;
  cluster_info.reset();
  EXPECT_EQ(OB_SUCCESS, systable_helper.query_cluster_info(cluster_info));
  TESTBENCH_LOG(INFO, "get cluster information", K(cluster_info));
}

TEST_F(TestSystableHelper, query_tenant_ls_info) {
  const uint64_t tenant_user_id = 1002;
  common::ObSEArray<share::ObLSID, 16> tenant_ls_ids;
  tenant_ls_ids.reset();
  EXPECT_EQ(OB_SUCCESS, systable_helper.query_tenant_ls_info(tenant_user_id,
                                                             tenant_ls_ids));
  TESTBENCH_LOG(INFO, "get tenant list information", K(tenant_user_id),
                K(tenant_ls_ids));
}

TEST_F(TestSystableHelper, query_sql_server_list) {
  common::ObSEArray<common::ObAddr, 16> sql_server_list;
  sql_server_list.reset();
  EXPECT_EQ(OB_SUCCESS, systable_helper.query_sql_server_list(sql_server_list));
  TESTBENCH_LOG(INFO, "get query sql server list", K(sql_server_list));
}

TEST_F(TestSystableHelper, query_tenant_info_list) {
  common::ObSEArray<testbench::TenantInfo, 16> tenant_info_list;
  tenant_info_list.reset();
  EXPECT_EQ(OB_SUCCESS,
            systable_helper.query_tenant_info_list(tenant_info_list));
  TESTBENCH_LOG(INFO, "get query tenant information list", K(tenant_info_list));
}

TEST_F(TestSystableHelper, query_tenant_list) {
  common::ObSEArray<uint64_t, 16> tenant_id_list;
  common::ObSEArray<testbench::ObTenantName, 16> tenant_name_list;
  tenant_id_list.reset();
  tenant_name_list.reset();
  EXPECT_EQ(OB_SUCCESS, systable_helper.query_tenant_list(tenant_id_list,
                                                          tenant_name_list));
  TESTBENCH_LOG(INFO, "get query tenant list", K(tenant_name_list));
}

TEST_F(TestSystableHelper, query_tenant_sql_server_list) {
  const uint64_t tenant_sys_id = 1;
  common::ObSEArray<common::ObAddr, 16> tenant_server_list;
  tenant_server_list.reset();
  EXPECT_EQ(OB_SUCCESS, systable_helper.query_tenant_sql_server_list(
                            tenant_sys_id, tenant_server_list));
  TESTBENCH_LOG(INFO, "get tenant sql_server_list", K(tenant_sys_id),
                K(tenant_server_list));
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}