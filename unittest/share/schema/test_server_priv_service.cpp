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

#define private public
#define protected public
#include <gtest/gtest.h>
#include "db_initializer.h"
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_manager.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_server_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "ob_schema_test_utils.cpp"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;
class TestServerSchemaService : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  DBInitializer db_initer_;
  ObSchemaServiceSQLImpl schema_service_;
  ObMultiVersionSchemaService multi_schema_;
};

void TestServerSchemaService::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = db_initer_.fill_sys_stat_table();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service_.init(&db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = multi_schema_.init(&db_initer_.get_sql_proxy(), &db_initer_.get_config(), OB_MAX_VERSION_COUNT, OB_MAX_VERSION_COUNT_FOR_MERGE, false/*with timestamp*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestServerSchemaService, refresh_priv)
{
  int ret = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space(2));
  // insert system tenant, make refresh tenant succeed,
  // ob_schema_test_utils.cpp is ugly, it need schema_service_ of type schema_serivce_sql_impl_ be
  // defined
  ObTableSchema table_schema;
  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::sys_table_schema_creators[i]; ++i)
  {
    table_schema.reset();
    table_schema.set_expire_info(ObString::make_string("a > b"));
    ASSERT_EQ(OB_SUCCESS, (*share::sys_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::core_table_schema_creators[i]; ++i)
  {
    table_schema.reset();
    table_schema.set_expire_info(ObString::make_string("a > b"));
    ASSERT_EQ(OB_SUCCESS, (*share::core_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::virtual_table_schema_creators[i]; ++i)
  {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::virtual_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::information_schema_table_schema_creators[i]; ++i)
  {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::information_schema_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::mysql_table_schema_creators[i]; ++i)
  {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::mysql_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  // create sys_tenant default tablespace and database

  ObTablegroupSchema sys_ts_schema;
  ObDatabaseSchema sys_db_schema;
  FILL_TABLEGROUP_SCHEMA(sys_ts_schema, OB_SYS_TENANT_ID,
                         OB_SYS_TABLEGROUP_ID, "default ts", "default ts");
  FILL_DATABASE_SCHEMA(sys_db_schema, OB_SYS_TENANT_ID,
                       OB_SYS_DATABASE_ID, "default db", "default db");
  CREATE_TABLEGROUP_SCHEMA(ret, sys_ts_schema);
  CREATE_DATABASE_SCHEMA(ret, sys_db_schema);
  ret = multi_schema_.refresh_and_add_schema();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantSchema tenant;
  ObArray<ObString> zone_list;
  zone_list.push_back("127.0.0.1");
  FILL_TENANT_INFO(tenant, 1, "sys", 3, zone_list, "127.0.0.1", 0, "");
  tenant.set_locality("");
  CREATE_TENANT(ret, tenant);
  tenant.reset();
  FILL_TENANT_INFO(tenant, 2, "ob", 3, zone_list, "127.0.0.1", 0, "");
  tenant.set_locality("");
  CREATE_TENANT(ret, tenant);

  ObUserInfo user;
  //user: tenant_id, user_id, user_name, passwd, info, is_locked, priv_set
  FILL_USER_INFO(user, OB_SYS_TENANT_ID, 1, "lilei", "lilei", "student in class one", false,
                 OB_PRIV_ALL);

  //create user
  CREATE_USER_PRIV(ret, user);

  //first refresh_and_add_schema
  ret = multi_schema_.refresh_and_add_schema();
  ASSERT_TRUE(NULL != multi_schema_.schema_manager_for_cache_.get_const_priv_mgr().get_user_info(ObTenantUserId(OB_SYS_TENANT_ID, 1)));
  uint64_t tenant_id = OB_INVALID_ID;
  multi_schema_.schema_manager_for_cache_.get_const_priv_mgr().get_tenant_id("sys", tenant_id);
  ASSERT_TRUE(1 == tenant_id);
  ASSERT_TRUE(NULL != multi_schema_.schema_manager_for_cache_.get_const_priv_mgr().get_tenant_info(1));
  ASSERT_TRUE(NULL != multi_schema_.schema_manager_for_cache_.get_const_priv_mgr().get_tenant_info(2));

  FILL_USER_INFO(user, 2, 2, "hh", "hh", "student in class one", false,
                 OB_PRIV_SELECT | OB_PRIV_CREATE);
  //create user
  CREATE_USER_PRIV(ret, user);

  ObOriginalDBKey db_priv_key;
  //db_priv_org_key:tenant_id, user_id, database_name
  FILL_DB_PRIV_ORG_KEY(db_priv_key, 2, 1, "test");

  //grant database privilege
  GRANT_DATABASE_PRIV(ret, db_priv_key, OB_PRIV_SELECT | OB_PRIV_CREATE);

  //grant table privilege
  ObTablePrivSortKey table_priv_key;
  //table_priv_key: tenant_id, user_id, user_name, table_name
  FILL_TABLE_PRIV_KEY(table_priv_key, 2, 1, "test", "table_priv");
  //grant table priv
  GRANT_TABLE_PRIV(ret, table_priv_key, OB_PRIV_SELECT | OB_PRIV_INSERT);

  //second refresh_and_add_schema(get increment)
  ret = multi_schema_.refresh_and_add_schema();

  const ObSchemaManager *schema_manager;
  schema_manager = multi_schema_.get_schema_manager_by_version(0);
  ASSERT_TRUE(NULL != schema_manager);
  const ObPrivManager &priv_mgr_first = schema_manager->get_const_priv_mgr();
  ASSERT_TRUE(NULL != priv_mgr_first.get_user_info(ObTenantUserId(OB_SYS_TENANT_ID, 1)));

  ASSERT_TRUE(NULL != priv_mgr_first.get_user_info(ObTenantUserId(2, 2)));
  ASSERT_TRUE(NULL != priv_mgr_first.get_user_info(ObTenantUserId(OB_SYS_TENANT_ID, 1)));
  ASSERT_TRUE(NULL != priv_mgr_first.get_db_priv(db_priv_key));
  ASSERT_TRUE(NULL != priv_mgr_first.get_table_priv(table_priv_key));

  ret = multi_schema_.rewrite_switch();
  ASSERT_EQ(OB_SUCCESS, ret);

  FILL_TABLE_PRIV_KEY(table_priv_key, 2, 2, "test", "table_priv");
  //grant table priv
  GRANT_TABLE_PRIV(ret, table_priv_key, OB_PRIV_SELECT | OB_PRIV_INSERT);
  schema_manager = multi_schema_.get_schema_manager_by_version(0);
  ASSERT_TRUE(NULL != schema_manager);
  const ObPrivManager &priv_mgr_second = schema_manager->get_const_priv_mgr();
  ASSERT_TRUE(NULL != priv_mgr_second.get_tenant_info(1));
  priv_mgr_second.get_tenant_id("sys", tenant_id);
  ASSERT_TRUE(1 == tenant_id);
  ASSERT_TRUE(NULL != priv_mgr_second.get_user_info(ObTenantUserId(2, 2)));
  ASSERT_TRUE(NULL != priv_mgr_second.get_db_priv(db_priv_key));
  FILL_TABLE_PRIV_KEY(table_priv_key, 2, 1, "test", "table_priv");
  ASSERT_TRUE(NULL != priv_mgr_second.get_table_priv(table_priv_key));
}

}
}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
