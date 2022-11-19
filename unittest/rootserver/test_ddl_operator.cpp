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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#define  private public
#define  protected public

#include "lib/stat/ob_session_stat.h"
#include "../share/schema/db_initializer.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rootserver/ob_ddl_operator.h"
#include "../share/schema/ob_schema_test_utils.cpp"


namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;

namespace rootserver
{

class TestDDLOperator : public ::testing::Test
{
public:
  TestDDLOperator();
  virtual ~TestDDLOperator() {}
  virtual void SetUp();
  virtual void TearDown();

protected:
  int create_tenant(const char *tenant_name, uint64_t &tenant_id);
  int create_database(const uint64_t tenant_id, const char *database_name,
                      uint64_t &database_id);
  int create_tablegroup(const uint64_t tenant_id, const char *tablegroup_name,
                        uint64_t &tablegroup_id);
  int create_table(const uint64_t tenant_id, const uint64_t database_id,
                   const uint64_t tablegroup_id, const char *table_name,
                   uint64_t &table_id);
  DBInitializer db_initer_;
  ObMultiVersionSchemaService multi_schema_service_;
  ObSrvRpcProxy srv_rpc_proxy_;
  ObDDLOperator ddl_operator_;
  uint64_t pre_tenant_id_;
  uint64_t pre_database_id_;
  uint64_t pre_tablegroup_id_;
  uint64_t pre_table_id_;
};

TestDDLOperator::TestDDLOperator()
  : db_initer_(),
    multi_schema_service_(),
    srv_rpc_proxy_(),
    ddl_operator_(multi_schema_service_, db_initer_.get_sql_proxy())
{
  ObCompatModeGetter::instance().init(&db_initer_.get_sql_proxy());
}

void TestDDLOperator::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestDDLOperator::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObKVGlobalCache::get_instance().init();

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = multi_schema_service_.init(&db_initer_.get_sql_proxy(), &db_initer_.get_config(),
                                   OB_MAX_VERSION_COUNT, OB_MAX_VERSION_COUNT_FOR_MERGE, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSchemaService *schema_service = multi_schema_service_.get_schema_service();
  ASSERT_TRUE(NULL != schema_service);
  ret = db_initer_.fill_sys_stat_table();
  ASSERT_EQ(OB_SUCCESS, ret);
  //ret = multi_schema_service_.refresh_and_add_schema();
  //ASSERT_EQ(OB_SUCCESS, ret);

  // insert system tenant, make refresh tenant succeed,
  // ob_schema_test_utils.cpp is ugly, it need schema_service_ of type schema_serivce_sql_impl_ be
  // defined
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(OB_SYS_TENANT_ID);
  tenant_schema.set_tenant_name(OB_SYS_TENANT_NAME);
  tenant_schema.set_locality("F@test");
  tenant_schema.add_zone("test");
  CREATE_TENANT(ret, tenant_schema);

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

  // create sys_tenant default tablegroup and database
  ObTablegroupSchema sys_tg_schema;
  ObDatabaseSchema sys_db_schema;
  FILL_TABLEGROUP_SCHEMA(sys_tg_schema, OB_SYS_TENANT_ID,
                         OB_SYS_TABLEGROUP_ID, "default tg", "default tg");
  FILL_DATABASE_SCHEMA(sys_db_schema, OB_SYS_TENANT_ID,
                       OB_SYS_DATABASE_ID, "default db", "default db");
  CREATE_TABLEGROUP_SCHEMA(ret, sys_tg_schema);
  CREATE_DATABASE_SCHEMA(ret, sys_db_schema);
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());

  pre_tenant_id_ = OB_INVALID_ID;
  pre_database_id_ = OB_INVALID_ID;
  pre_tablegroup_id_ = OB_INVALID_ID;
  pre_table_id_ = OB_INVALID_ID;
  // pre create schema to make drop ddl operation easy to test
  ASSERT_EQ(OB_SUCCESS, create_tenant("11", pre_tenant_id_));
  ASSERT_EQ(OB_SUCCESS, create_database(pre_tenant_id_, "pre_create_db", pre_database_id_));
  ASSERT_EQ(OB_SUCCESS, create_tablegroup(pre_tenant_id_, "pre_create_tg", pre_tablegroup_id_));
  ASSERT_EQ(OB_SUCCESS, create_table(pre_tenant_id_, pre_database_id_, pre_tablegroup_id_,
                                     "pre_create_table", pre_table_id_));
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
}

int TestDDLOperator::create_tenant(const char *tenant_name, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  ObSysVariableSchema sys_variable_schema;
  if (OB_FAIL(multi_schema_service_.get_schema_service()->fetch_new_tenant_id(tenant_id))) {
    LOG_WARN("fetch_new_tenant_id failed", K(ret));
  } else {
    tenant_schema.set_tenant_id(tenant_id);
    tenant_schema.set_tenant_name(tenant_name);
    tenant_schema.set_comment("this is a test tenant");
    tenant_schema.set_primary_zone(ObString::make_string("test"));
    tenant_schema.add_zone("test");
    tenant_schema.set_locality("");
    tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(db_initer_.create_tenant_space_tables(tenant_id))) {
      LOG_WARN("create_tenant_space tables failed", K(tenant_id), K(ret));
    }
  }

  ObMySQLTransaction trans;
  ObCreateTenantArg fake_arg;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(&db_initer_.get_sql_proxy()))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ddl_operator_.create_tenant(fake_arg, tenant_schema, trans))) {
    LOG_WARN("ddl_operator_ create_tenant failed", K(tenant_schema), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObSysVariableSchema sys_variable_schema;
    INIT_SYS_VARIABLE(trans, tenant_schema, sys_variable_schema);
  }

  if (trans.is_started()) {
    const bool commit = OB_SUCCESS == ret;
    int commit_ret = trans.end(commit);
    if (OB_SUCCESS != commit_ret) {
      LOG_WARN("trans end failed", K(commit), K(commit_ret));
      ret = (OB_SUCCESS == ret) ? commit_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(db_initer_.create_tenant_space(tenant_id))) {
      LOG_WARN("create_tenant_space failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int TestDDLOperator::create_database(const uint64_t tenant_id, const char *database_name,
                                     uint64_t &database_id)
{
  int ret = OB_SUCCESS;
  ObDatabaseSchema database_schema;
  database_schema.set_tenant_id(tenant_id);
  database_schema.set_database_id(OB_INVALID_ID);
  database_schema.set_database_name(database_name);
  database_schema.set_comment("this is a test db");
  database_schema.set_primary_zone("test");
  database_schema.add_zone("test");
  ObArray<ObString> zone_list;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(&db_initer_.get_sql_proxy()))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ddl_operator_.create_database(database_schema, trans))) {
    LOG_WARN("ddl_operator_ create_database failed", K(database_schema), K(ret));
  } else if (OB_FAIL(zone_list.push_back(ObString::make_string("test")))) {
    LOG_WARN("construct zone list failed", K(ret));
  } else {
    database_schema.set_zone_list(zone_list);
  }

  database_id = database_schema.get_database_id();
  if (trans.is_started()) {
    const bool commit = OB_SUCCESS == ret;
    int commit_ret = trans.end(commit);
    if (OB_SUCCESS != commit_ret) {
      LOG_WARN("trans end failed", K(commit), K(commit_ret));
      ret = (OB_SUCCESS == ret) ? commit_ret : ret;
    }
  }
  return ret;
}

int TestDDLOperator::create_tablegroup(const uint64_t tenant_id, const char *tablegroup_name,
                                       uint64_t &tablegroup_id)
{
  int ret = OB_SUCCESS;
  ObTablegroupSchema tablegroup_schema;
  tablegroup_schema.set_tenant_id(tenant_id);
  tablegroup_schema.set_tablegroup_id(OB_INVALID_ID);
  tablegroup_schema.set_tablegroup_name(tablegroup_name);
  tablegroup_schema.set_comment("this is a test tg");

  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(&db_initer_.get_sql_proxy()))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ddl_operator_.create_tablegroup(tablegroup_schema, trans))) {
    LOG_WARN("ddl_operator_ create_tablegroup failed", K(tablegroup_schema), K(ret));
  }

  tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (trans.is_started()) {
    const bool commit = OB_SUCCESS == ret;
    int commit_ret = trans.end(commit);
    if (OB_SUCCESS != commit_ret) {
      LOG_WARN("trans end failed", K(commit), K(commit_ret));
      ret = (OB_SUCCESS == ret) ? commit_ret : ret;
    }
  }
  return ret;
}

int TestDDLOperator::create_table(const uint64_t tenant_id, const uint64_t database_id,
                                  const uint64_t tablegroup_id, const char *table_name,
                                  uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  if (OB_FAIL(multi_schema_service_.get_schema_service()->fetch_new_table_id(
      pre_tenant_id_, table_id))) {
    LOG_WARN("fetch_new_table_id failed", K(ret));
  } else {
    LOG_INFO("table id get", K(table_id));
    share::ObInnerTableSchema::all_server_schema(table_schema);
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_database_id(database_id);
    table_schema.set_tablegroup_id(tablegroup_id);
    table_schema.set_table_id(table_id);
    table_schema.set_table_type(USER_TABLE);
    table_schema.set_table_name(table_name);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
    table_schema.get_part_option().set_part_num(3);
    table_schema.get_part_option().set_max_used_part_id(2);
    table_schema.set_primary_zone("test");
    table_schema.add_zone("test");
  }

  ObMySQLTransaction trans;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(&db_initer_.get_sql_proxy()))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ddl_operator_.create_table(table_schema, trans))) {
    LOG_WARN("ddl_operator_ create_table failed", K(table_schema), K(ret));
  }

  if (trans.is_started()) {
    const bool commit = OB_SUCCESS == ret;
    int commit_ret = trans.end(commit);
    if (OB_SUCCESS != commit_ret) {
      LOG_WARN("trans end failed", K(commit), K(commit_ret));
      ret = (OB_SUCCESS == ret) ? commit_ret : ret;
    }
  }
  return ret;
}

TEST_F(TestDDLOperator, create_tenant)
{
  uint64_t tenant_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant("hai", tenant_id));
  ASSERT_NE(OB_INVALID_ID, tenant_id);
  //const ObSchemaManager *schema_manager = NULL;
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  //schema_manager = multi_schema_service_.get_schema_manager_by_version();
  //ASSERT_TRUE(NULL != schema_manager);
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
  //const ObTenantSchema *p_tenant = schema_manager->get_const_priv_mgr().get_tenant_info("hai");
  const ObTenantSchema *p_tenant = NULL;
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_tenant_info("hai", p_tenant));
  ASSERT_TRUE(NULL != p_tenant);
}

TEST_F(TestDDLOperator, create_database)
{
  uint64_t database_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, create_database(pre_tenant_id_, "haijing_database", database_id));
  ASSERT_NE(OB_INVALID_ID, database_id);
}

TEST_F(TestDDLOperator, create_tablegroup)
{
  uint64_t tablegroup_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, create_tablegroup(pre_tenant_id_, "haijing_tablegroup", tablegroup_id));
  ASSERT_NE(OB_INVALID_ID, tablegroup_id);
}

TEST_F(TestDDLOperator, drop_tenant)
{
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  //const ObSchemaManager *schema_manager = NULL;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  //schema_manager = multi_schema_service_.get_schema_manager_by_version();
  //ASSERT_TRUE(NULL != schema_manager);
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
  ObDatabaseSchema db_schema;
  db_schema.set_tenant_id(pre_tenant_id_);
  db_schema.set_database_id(pre_database_id_);
  ASSERT_EQ(OB_SUCCESS, ddl_operator_.drop_database(db_schema, trans, schema_guard));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());

  uint64_t tablegroup_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard2;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard2));
  ASSERT_EQ(OB_SUCCESS, schema_guard2.get_database_id(pre_tenant_id_,
      "pre_create_db", database_id));
  ASSERT_EQ(OB_INVALID_ID, database_id);
  ASSERT_EQ(OB_SUCCESS, schema_guard2.get_tablegroup_id(pre_tenant_id_,
       "pre_create_tablegroup", tablegroup_id));
  ASSERT_EQ(OB_INVALID_ID, tablegroup_id);
  ASSERT_EQ(OB_SUCCESS, schema_guard2.get_table_id(pre_tenant_id_,
      pre_database_id_, "pre_create_table", false, ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id));
  ASSERT_EQ(OB_INVALID_ID, table_id);
  //bool is_exist = false;
  //const bool is_index = false;
  //ASSERT_EQ(OB_SUCCESS, schema_manager->check_database_exist(
  //    pre_tenant_id_, "pre_create_db", database_id, is_exist));
  //ASSERT_FALSE(is_exist);
  //ASSERT_EQ(OB_SUCCESS, schema_manager->check_tablegroup_exist(
  //    pre_tenant_id_, "pre_create_tablegroup", tablegroup_id, is_exist));
  //ASSERT_FALSE(is_exist);
  //ASSERT_EQ(OB_SUCCESS, schema_manager->check_table_exist(pre_tenant_id_, pre_database_id_,
  //    "pre_create_table", is_index, is_exist));
  //ASSERT_FALSE(is_exist);
}

TEST_F(TestDDLOperator, drop_database)
{
  ObMySQLTransaction trans;
  ObDatabaseSchema db_schema;
  db_schema.set_tenant_id(pre_tenant_id_);
  db_schema.set_database_id(pre_database_id_);
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, ddl_operator_.drop_database(db_schema, trans, schema_guard));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  //const ObSchemaManager *schema_manager = NULL;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  //schema_manager = multi_schema_service_.get_schema_manager_by_version();
  //ASSERT_TRUE(NULL != schema_manager);

  // check whether database and table exist
  uint64_t database_id = OB_INVALID_ID;
  //bool is_exist = false;
  //const bool is_index = false;
  ObSchemaGetterGuard schema_guard2;
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard2));
  ASSERT_EQ(OB_SUCCESS, schema_guard2.get_database_id(pre_tenant_id_, "pre_create_db", database_id));
  ASSERT_EQ(OB_INVALID_ID, database_id);
  //ASSERT_EQ(OB_SUCCESS, schema_manager->check_database_exist(
  //    pre_tenant_id_, "pre_create_db", database_id, is_exist));
  //ASSERT_FALSE(is_exist);
  //ASSERT_EQ(OB_SUCCESS, schema_manager->check_table_exist(pre_tenant_id_, pre_database_id_,
  //    "pre_create_table", is_index, is_exist));
  //ASSERT_FALSE(is_exist);
}

TEST_F(TestDDLOperator, drop_tablegroup)
{
  //ObMySQLTransaction trans;
  //// drop tablegroup
  //ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  //ASSERT_EQ(OB_TABLEGROUP_NOT_EMPTY, ddl_operator_.drop_tablegroup(
  //    pre_tenant_id_, pre_tablegroup_id_, trans));
  //ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ////const ObSchemaManager *schema_manager = NULL;
  //ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  ////schema_manager = multi_schema_service_.get_schema_manager_by_version();

  //ObSchemaGetterGuard schema_guard;
  //ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
  //// drop table first
  ////const ObTableSchema *table_schema = schema_manager->get_table_schema(
  ////    pre_tenant_id_, pre_database_id_, "pre_create_table", false);
  //const ObTableSchema *table_schema = NULL;
  //ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
  //    pre_tenant_id_, pre_database_id_, "pre_create_table", table_schema));
  //ASSERT_TRUE(NULL != table_schema);
  //ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  //ASSERT_EQ(OB_SUCCESS, ddl_operator_.drop_table(*table_schema, trans));
  //ASSERT_EQ(OB_SUCCESS, trans.end(true));
  //ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  ////schema_manager = multi_schema_service_.get_schema_manager_by_version();

  //// drop tablegroup
  //ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  //ASSERT_EQ(OB_SUCCESS, ddl_operator_.drop_tablegroup(pre_tenant_id_, pre_tablegroup_id_, trans));
  //ASSERT_EQ(OB_SUCCESS, trans.end(true));
  //ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  ////schema_manager = multi_schema_service_.get_schema_manager_by_version();

  //// check whether table and table exist
  //uint64_t tablegroup_id = OB_INVALID_ID;
  //uint64_t table_id = OB_INVALID_ID;
  ////bool is_exist = false;
  ////ASSERT_EQ(OB_SUCCESS, schema_manager->check_tablegroup_exist(
  ////    pre_tenant_id_, "pre_create_tablegroup", tablegroup_id, is_exist));
  ////ASSERT_FALSE(is_exist);
  ////const bool is_index = false;
  ////ASSERT_EQ(OB_SUCCESS, schema_manager->check_table_exist(
  ////    pre_tenant_id_, pre_database_id_, "test_user_table", is_index, is_exist));
  ////ASSERT_FALSE(is_exist);
  //ASSERT_EQ(OB_SUCCESS, schema_guard.get_tablegroup_id(
  //    pre_tenant_id_, "pre_create_tablegroup", tablegroup_id));
  //ASSERT_EQ(OB_INVALID_ID, tablegroup_id);
  //ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_id(
  //    pre_tenant_id_, pre_database_id_, "test_user_table",table_id));
  //ASSERT_EQ(OB_INVALID_ID, table_id);

}

}// end namespace rootserver
}// end namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_ddl_operator.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ddl_operator.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
