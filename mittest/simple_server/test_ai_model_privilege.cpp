/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"
#include "sql/resolver/ob_schema_checker.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::sql;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

class TestAIServiceEndpointPrivilege : public ObSimpleClusterTestBase
{
public:
  TestAIServiceEndpointPrivilege() : ObSimpleClusterTestBase("test_ai_model_privilege_") {}
};

TEST_F(TestAIServiceEndpointPrivilege, test_ai_model_privilege)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "test_ai_model_privilege start");

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  {
    OB_LOG(INFO, "create test user start");
    ObSqlString sql;
    sql.assign_fmt("CREATE USER test_ai_user@'%%' IDENTIFIED BY '123456'");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "create test user succ");
  }

  {
    OB_LOG(INFO, "check initial privileges start");
    ObSqlString sql;
    sql.assign_fmt("SHOW GRANTS FOR test_ai_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, result->get_varchar("Grants for test_ai_user@%", grants));
      OB_LOG(INFO, "initial grants", K(grants));
    }
    OB_LOG(INFO, "check initial privileges succ");
  }

  {
    OB_LOG(INFO, "grant AI MODEL privileges start");
    ObSqlString sql;
    sql.assign_fmt("GRANT CREATE AI MODEL ON *.* TO test_ai_user@'%%'");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.reset();
    sql.assign_fmt("GRANT ALTER AI MODEL ON *.* TO test_ai_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.reset();
    sql.assign_fmt("GRANT DROP AI MODEL ON *.* TO test_ai_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.reset();
    sql.assign_fmt("GRANT ACCESS AI MODEL ON *.* TO test_ai_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    OB_LOG(INFO, "grant AI MODEL privileges succ");
  }

  {
    OB_LOG(INFO, "verify granted privileges start");
    ObSqlString sql;
    sql.assign_fmt("SHOW GRANTS FOR test_ai_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, result->get_varchar("Grants for test_ai_user@%", grants));
      OB_LOG(INFO, "granted privileges", K(grants));

      ObString grants_str(grants);
      ASSERT_TRUE(grants_str.prefix_match("GRANT CREATE AI MODEL, ALTER AI MODEL, DROP AI MODEL, ACCESS AI MODEL ON *.* TO 'test_ai_user'"));
    }
    OB_LOG(INFO, "verify granted privileges succ");
  }

  {
    OB_LOG(INFO, "test privilege check functions start");

    observer::ObServer& observer = get_curr_observer();

    share::schema::ObSchemaGetterGuard schema_guard;
    ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(RunCtx.tenant_id_, schema_guard));

    const share::schema::ObUserInfo *user_info = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_user_info(RunCtx.tenant_id_, "test_ai_user", "%", user_info));
    ASSERT_NE(nullptr, user_info);

    share::schema::ObSessionPrivInfo session_priv;
    session_priv.tenant_id_ = RunCtx.tenant_id_;
    session_priv.user_id_ = user_info->get_user_id();
    session_priv.user_name_ = user_info->get_user_name_str();
    session_priv.host_name_ = user_info->get_host_name_str();
    session_priv.user_priv_set_ = user_info->get_priv_set();
    common::ObArenaAllocator allocator;

    ObAIServiceEndpointPrivUtil priv_util(schema_guard);

    bool has_priv = false;

    ASSERT_EQ(OB_SUCCESS, priv_util.check_create_ai_model_priv(
        allocator, session_priv, has_priv));
    ASSERT_TRUE(has_priv);
    OB_LOG(INFO, "CREATE AI MODEL privilege check passed");

    ASSERT_EQ(OB_SUCCESS, priv_util.check_alter_ai_model_priv(
        allocator, session_priv, has_priv));
    ASSERT_TRUE(has_priv);
    OB_LOG(INFO, "ALTER AI MODEL privilege check passed");

    ASSERT_EQ(OB_SUCCESS, priv_util.check_drop_ai_model_priv(
        allocator, session_priv, has_priv));
    ASSERT_TRUE(has_priv);
    OB_LOG(INFO, "DROP AI MODEL privilege check passed");

    ASSERT_EQ(OB_SUCCESS, priv_util.check_access_ai_model_priv(
        allocator, session_priv, has_priv));
    ASSERT_TRUE(has_priv);
    OB_LOG(INFO, "ACCESS AI MODEL privilege check passed");

    OB_LOG(INFO, "test privilege check functions succ");
  }

  {
    OB_LOG(INFO, "revoke partial privileges start");
    ObSqlString sql;
    sql.assign_fmt("REVOKE ALTER AI MODEL ON *.* FROM test_ai_user@'%%'");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.reset();
    sql.assign_fmt("REVOKE DROP AI MODEL ON *.* FROM test_ai_user@'%%'");
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    OB_LOG(INFO, "revoke partial privileges succ");
  }

  {
    OB_LOG(INFO, "verify revoked privileges start");
    ObSqlString sql;
    sql.assign_fmt("SHOW GRANTS FOR test_ai_user@'%%'");
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ObString grants;
      ASSERT_EQ(OB_SUCCESS, result->get_varchar("Grants for test_ai_user@%", grants));
      OB_LOG(INFO, "remaining privileges after revoke", K(grants));

      ObString grants_str(grants);
      ASSERT_TRUE(grants_str.prefix_match("GRANT CREATE AI MODEL, ACCESS AI MODEL ON *.* TO 'test_ai_user'"));
    }
    OB_LOG(INFO, "verify revoked privileges succ");
  }

  {
    OB_LOG(INFO, "test privilege check functions after revoke start");
    observer::ObServer& observer = get_curr_observer();

    share::schema::ObSchemaGetterGuard schema_guard;
    ASSERT_EQ(OB_SUCCESS, observer.get_schema_service().get_tenant_schema_guard(RunCtx.tenant_id_, schema_guard));

    const share::schema::ObUserInfo *user_info = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_user_info(RunCtx.tenant_id_, "test_ai_user", "%", user_info));
    ASSERT_NE(nullptr, user_info);

    share::schema::ObSessionPrivInfo session_priv;
    session_priv.tenant_id_ = RunCtx.tenant_id_;
    session_priv.user_id_ = user_info->get_user_id();
    session_priv.user_name_ = user_info->get_user_name_str();
    session_priv.host_name_ = user_info->get_host_name_str();
    session_priv.user_priv_set_ = user_info->get_priv_set();

    common::ObArenaAllocator allocator;

    ObAIServiceEndpointPrivUtil priv_util(schema_guard);

    bool has_priv = false;

    ASSERT_EQ(OB_SUCCESS, priv_util.check_create_ai_model_priv(
        allocator, session_priv, has_priv));
    ASSERT_TRUE(has_priv);
    OB_LOG(INFO, "CREATE AI MODEL privilege check passed after revoke");

    ASSERT_EQ(OB_SUCCESS, priv_util.check_alter_ai_model_priv(
        allocator, session_priv, has_priv));
    OB_LOG(INFO, "ALTER AI MODEL privilege check result", K(has_priv), K(session_priv.user_priv_set_));
    ASSERT_FALSE(has_priv);
    OB_LOG(INFO, "ALTER AI MODEL privilege check failed as expected after revoke");

    ASSERT_EQ(OB_SUCCESS, priv_util.check_drop_ai_model_priv(
        allocator, session_priv, has_priv));
    OB_LOG(INFO, "DROP AI MODEL privilege check result", K(has_priv), K(session_priv.user_priv_set_));
    ASSERT_FALSE(has_priv);
    OB_LOG(INFO, "DROP AI MODEL privilege check failed as expected after revoke");

    ASSERT_EQ(OB_SUCCESS, priv_util.check_access_ai_model_priv(
        allocator, session_priv, has_priv));
    ASSERT_TRUE(has_priv);
    OB_LOG(INFO, "ACCESS AI MODEL privilege check passed after revoke");

    OB_LOG(INFO, "test privilege check functions after revoke succ");
  }

  {
    OB_LOG(INFO, "cleanup test user start");
    ObSqlString sql;
    sql.assign_fmt("DROP USER test_ai_user@'%%'");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "cleanup test user succ");
  }

  SERVER_LOG(INFO, "test_ai_model_privilege end");
}

TEST_F(TestAIServiceEndpointPrivilege, end)
{
  // if (RunCtx.time_sec_ > 0) {
  //   ::sleep(RunCtx.time_sec_);
  // }
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