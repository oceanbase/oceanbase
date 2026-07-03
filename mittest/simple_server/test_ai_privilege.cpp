/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_priv_type.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

class TestAIPrivilege : public ObSimpleClusterTestBase
{
public:
  TestAIPrivilege() : ObSimpleClusterTestBase("test_ai_privilege_") {}
};

TEST_F(TestAIPrivilege, test_ai_privilege)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "test_ai_privilege start");

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  // ======== Part A: AI PROVIDER privileges ========
  {
    // create user
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql.assign_fmt("CREATE USER test_ai_prov_user@'%%' IDENTIFIED BY '123456'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    // grant all provider privs (comma-separated with ACCESS)
    sql.reset();
    sql.assign_fmt("GRANT REGISTER, ALTER, UNREGISTER, ACCESS ON AI PROVIDER * TO test_ai_prov_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    // SHOW GRANTS: obj-level on 2nd row after USAGE
    sql.reset();
    sql.assign_fmt("SHOW GRANTS FOR test_ai_prov_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, result->get_varchar("Grants for test_ai_prov_user@%", grants));
      ASSERT_TRUE(ObString(grants).prefix_match(
          "GRANT ALTER, REGISTER, UNREGISTER, ACCESS ON AI PROVIDER * TO 'test_ai_prov_user'"));
    }

    // privilege check via schema guard
    observer::ObServer& observer = get_curr_observer();
    share::schema::ObSchemaGetterGuard schema_guard;
    ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(
        RunCtx.tenant_id_, schema_guard));
    const share::schema::ObUserInfo *user_info = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_user_info(
        RunCtx.tenant_id_, "test_ai_prov_user", "%", user_info));
    ASSERT_NE(nullptr, user_info);
    {
      share::schema::ObSessionPrivInfo sp;
      sp.tenant_id_ = RunCtx.tenant_id_;
      sp.user_id_ = user_info->get_user_id();
      sp.user_name_ = user_info->get_user_name_str();
      sp.host_name_ = user_info->get_host_name_str();
      sp.user_priv_set_ = user_info->get_priv_set();
      common::ObSEArray<uint64_t, 16> roles;
      ASSERT_EQ(OB_SUCCESS, schema_guard.check_ai_provider_access(sp, roles, OB_PRIV_REGISTER));
      ASSERT_EQ(OB_SUCCESS, schema_guard.check_ai_provider_access(sp, roles, OB_PRIV_ALTER));
      ASSERT_EQ(OB_SUCCESS, schema_guard.check_ai_provider_access(sp, roles, OB_PRIV_UNREGISTER));
      ASSERT_EQ(OB_SUCCESS, schema_guard.check_ai_provider_access(sp, roles, OB_PRIV_ACCESS));
    }

    // revoke ALTER and UNREGISTER (comma-separated)
    sql.reset();
    sql.assign_fmt("REVOKE ALTER, UNREGISTER ON AI PROVIDER * FROM test_ai_prov_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    // verify remaining
    sql.reset();
    sql.assign_fmt("SHOW GRANTS FOR test_ai_prov_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
      sqlclient::ObMySQLResult *r = res2.get_result();
      ASSERT_NE(nullptr, r);
      ASSERT_EQ(OB_SUCCESS, r->next());
      ASSERT_EQ(OB_SUCCESS, r->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, r->get_varchar("Grants for test_ai_prov_user@%", grants));
      ASSERT_TRUE(ObString(grants).prefix_match(
          "GRANT REGISTER, ACCESS ON AI PROVIDER * TO 'test_ai_prov_user'"));
    }

    // verify after revoke
    {
      share::schema::ObSchemaGetterGuard sg;
      ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(
          RunCtx.tenant_id_, sg));
      const share::schema::ObUserInfo *ui = nullptr;
      ASSERT_EQ(OB_SUCCESS, sg.get_user_info(RunCtx.tenant_id_, "test_ai_prov_user", "%", ui));
      share::schema::ObSessionPrivInfo sp;
      sp.tenant_id_ = RunCtx.tenant_id_;
      sp.user_id_ = ui->get_user_id();
      sp.user_name_ = ui->get_user_name_str();
      sp.host_name_ = ui->get_host_name_str();
      sp.user_priv_set_ = ui->get_priv_set();
      common::ObSEArray<uint64_t, 16> roles;
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_provider_access(sp, roles, OB_PRIV_REGISTER));
      ASSERT_EQ(OB_ERR_NO_PRIVILEGE, sg.check_ai_provider_access(sp, roles, OB_PRIV_ALTER));
      ASSERT_EQ(OB_ERR_NO_PRIVILEGE, sg.check_ai_provider_access(sp, roles, OB_PRIV_UNREGISTER));
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_provider_access(sp, roles, OB_PRIV_ACCESS));
    }

    // cleanup
    sql.reset();
    sql.assign_fmt("DROP USER test_ai_prov_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  // ======== Part B: AI GATEWAY privileges ========
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql.assign_fmt("CREATE USER test_ai_gw_user@'%%' IDENTIFIED BY '123456'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    // grant all gateway privs (comma-separated with ACCESS)
    sql.reset();
    sql.assign_fmt("GRANT CREATE, ALTER, DROP, ACCESS ON AI GATEWAY * TO test_ai_gw_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    // SHOW GRANTS
    sql.reset();
    sql.assign_fmt("SHOW GRANTS FOR test_ai_gw_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *r = res.get_result();
      ASSERT_NE(nullptr, r);
      ASSERT_EQ(OB_SUCCESS, r->next());
      ASSERT_EQ(OB_SUCCESS, r->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, r->get_varchar("Grants for test_ai_gw_user@%", grants));
      ASSERT_TRUE(ObString(grants).prefix_match(
          "GRANT ALTER, CREATE, DROP, ACCESS ON AI GATEWAY * TO 'test_ai_gw_user'"));
    }

    // privilege check
    observer::ObServer& observer = get_curr_observer();
    {
      share::schema::ObSchemaGetterGuard sg;
      ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(
          RunCtx.tenant_id_, sg));
      const share::schema::ObUserInfo *ui = nullptr;
      ASSERT_EQ(OB_SUCCESS, sg.get_user_info(RunCtx.tenant_id_, "test_ai_gw_user", "%", ui));
      share::schema::ObSessionPrivInfo sp;
      sp.tenant_id_ = RunCtx.tenant_id_;
      sp.user_id_ = ui->get_user_id();
      sp.user_name_ = ui->get_user_name_str();
      sp.host_name_ = ui->get_host_name_str();
      sp.user_priv_set_ = ui->get_priv_set();
      common::ObSEArray<uint64_t, 16> roles;
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_gateway_access(sp, roles, OB_PRIV_CREATE));
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_gateway_access(sp, roles, OB_PRIV_ALTER));
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_gateway_access(sp, roles, OB_PRIV_DROP));
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_gateway_access(sp, roles, OB_PRIV_ACCESS));
    }

    // revoke ALTER and DROP (comma-separated)
    sql.reset();
    sql.assign_fmt("REVOKE ALTER, DROP ON AI GATEWAY * FROM test_ai_gw_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    // verify remaining
    sql.reset();
    sql.assign_fmt("SHOW GRANTS FOR test_ai_gw_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
      sqlclient::ObMySQLResult *r = res2.get_result();
      ASSERT_NE(nullptr, r);
      ASSERT_EQ(OB_SUCCESS, r->next());
      ASSERT_EQ(OB_SUCCESS, r->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, r->get_varchar("Grants for test_ai_gw_user@%", grants));
      ASSERT_TRUE(ObString(grants).prefix_match(
          "GRANT CREATE, ACCESS ON AI GATEWAY * TO 'test_ai_gw_user'"));
    }

    // verify after revoke
    {
      share::schema::ObSchemaGetterGuard sg;
      ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(
          RunCtx.tenant_id_, sg));
      const share::schema::ObUserInfo *ui = nullptr;
      ASSERT_EQ(OB_SUCCESS, sg.get_user_info(RunCtx.tenant_id_, "test_ai_gw_user", "%", ui));
      share::schema::ObSessionPrivInfo sp;
      sp.tenant_id_ = RunCtx.tenant_id_;
      sp.user_id_ = ui->get_user_id();
      sp.user_name_ = ui->get_user_name_str();
      sp.host_name_ = ui->get_host_name_str();
      sp.user_priv_set_ = ui->get_priv_set();
      common::ObSEArray<uint64_t, 16> roles;
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_gateway_access(sp, roles, OB_PRIV_CREATE));
      ASSERT_EQ(OB_ERR_NO_PRIVILEGE, sg.check_ai_gateway_access(sp, roles, OB_PRIV_ALTER));
      ASSERT_EQ(OB_ERR_NO_PRIVILEGE, sg.check_ai_gateway_access(sp, roles, OB_PRIV_DROP));
      ASSERT_EQ(OB_SUCCESS, sg.check_ai_gateway_access(sp, roles, OB_PRIV_ACCESS));
    }

    // cleanup
    sql.reset();
    sql.assign_fmt("DROP USER test_ai_gw_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  // ======== Part C: isolation — provider ACCESS ≠ gateway ACCESS ========
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql.assign_fmt("CREATE USER test_iso_user@'%%' IDENTIFIED BY '123456'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    sql.reset();
    sql.assign_fmt("GRANT ACCESS ON AI PROVIDER * TO test_iso_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    observer::ObServer& observer = get_curr_observer();
    share::schema::ObSchemaGetterGuard sg;
    ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(
        RunCtx.tenant_id_, sg));
    const share::schema::ObUserInfo *ui = nullptr;
    ASSERT_EQ(OB_SUCCESS, sg.get_user_info(RunCtx.tenant_id_, "test_iso_user", "%", ui));
    share::schema::ObSessionPrivInfo sp;
    sp.tenant_id_ = RunCtx.tenant_id_;
    sp.user_id_ = ui->get_user_id();
    sp.user_name_ = ui->get_user_name_str();
    sp.host_name_ = ui->get_host_name_str();
    sp.user_priv_set_ = ui->get_priv_set();
    common::ObSEArray<uint64_t, 16> roles;
    ASSERT_EQ(OB_SUCCESS, sg.check_ai_provider_access(sp, roles, OB_PRIV_ACCESS));
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, sg.check_ai_gateway_access(sp, roles, OB_PRIV_CREATE));
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, sg.check_ai_gateway_access(sp, roles, OB_PRIV_ACCESS));

    // cleanup
    sql.reset();
    sql.assign_fmt("DROP USER test_iso_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  SERVER_LOG(INFO, "test_ai_privilege end");
}

TEST_F(TestAIPrivilege, end)
{
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
