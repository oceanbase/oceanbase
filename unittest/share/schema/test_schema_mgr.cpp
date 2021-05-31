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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#define private public
#include "share/schema/ob_schema_mgr.h"

namespace oceanbase {
using namespace share::schema;

namespace common {

class TestSchemaMgr : public ::testing::Test {};

#define GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, schema_version) \
  tenant_schema.set_tenant_id(tenant_id);                                        \
  tenant_schema.set_tenant_name(tenant_name);                                    \
  tenant_schema.set_schema_version(schema_version);

#define GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, schema_version) \
  sys_variable.set_tenant_id(tenant_id);                                 \
  sys_variable.set_schema_version(schema_version);                       \
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_USER_SCHEMA(user_schema, tenant_id, user_id, user_name, schema_version) \
  user_schema.set_tenant_id(tenant_id);                                             \
  user_schema.set_user_id(user_id);                                                 \
  user_schema.set_user_name(user_name);                                             \
  user_schema.set_schema_version(schema_version);

#define GEN_DATABASE_SCHEMA(database_schema, tenant_id, database_id, database_name, schema_version) \
  database_schema.set_tenant_id(tenant_id);                                                         \
  database_schema.set_database_id(database_id);                                                     \
  database_schema.set_database_name(database_name);                                                 \
  database_schema.set_schema_version(schema_version);                                               \
  database_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, tablegroup_id, tablegroup_name, schema_version) \
  tablegroup_schema.set_tenant_id(tenant_id);                                                               \
  tablegroup_schema.set_tablegroup_id(tablegroup_id);                                                       \
  tablegroup_schema.set_tablegroup_name(tablegroup_name);                                                   \
  tablegroup_schema.set_schema_version(schema_version);

#define GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, table_id, table_name, table_type, schema_version) \
  table_schema.reset();                                                                                          \
  table_schema.set_tenant_id(tenant_id);                                                                         \
  table_schema.set_database_id(database_id);                                                                     \
  table_schema.set_table_id(table_id);                                                                           \
  table_schema.set_table_name(table_name);                                                                       \
  table_schema.set_table_type(table_type);                                                                       \
  table_schema.set_schema_version(schema_version);                                                               \
  table_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

TEST_F(TestSchemaMgr, init)
{
  int ret = OB_SUCCESS;
  ObSchemaMgr mgr;
  ret = mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSchemaMgr, basic_interface)
{
  ObSchemaMgr schema_mgr;
  uint64_t tenant_id = 1;
  uint64_t user_id = combine_id(1, 1);
  uint64_t database_id = combine_id(1, 1);
  uint64_t tablegroup_id = combine_id(1, 1);
  uint64_t table_id = combine_id(1, 1);
  uint64_t index_table_id = combine_id(1, 2);
  ObString tenant_name;
  ObString user_name;
  ObString database_name;
  ObString tablegroup_name;
  ObString table_name;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleUserSchema user_schema;
  ObSimpleDatabaseSchema database_schema;
  ObSimpleTablegroupSchema tablegroup_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;

  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  // add_tenant
  tenant_id = 2;
  tenant_name = "tenant1";
  const ObSimpleTenantSchema* tenant = NULL;
  GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  // get tenant by id
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_schema(tenant_id, tenant));
  ASSERT_EQ(tenant_schema, *tenant);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_schema(3, tenant));
  ASSERT_EQ(NULL, tenant);
  // get tenant by name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_schema(tenant_name, tenant));
  ASSERT_EQ(tenant_schema, *tenant);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_schema("tenant_not_exist", tenant));
  ASSERT_EQ(NULL, tenant);
  // del_tenant
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_tenant(tenant_id));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_schema(tenant_id, tenant));
  ASSERT_EQ(NULL, tenant);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_schema(tenant_name, tenant));
  ASSERT_EQ(NULL, tenant);
  // add tenants
  ObArray<ObSimpleTenantSchema> tenant_schemas;
  ObArray<ObSimpleSysVariableSchema> sys_variables;
  schema_mgr.reset();
  tenant_schemas.reset();
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  tenant_schemas.push_back(tenant_schema);
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  sys_variables.push_back(sys_variable);
  GEN_TENANT_SCHEMA(tenant_schema, 2, "tenant2", 0);
  tenant_schemas.push_back(tenant_schema);
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 2, 0);
  sys_variables.push_back(sys_variable);
  GEN_TENANT_SCHEMA(tenant_schema, 3, "tenant3", 0);
  tenant_schemas.push_back(tenant_schema);
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 3, 0);
  sys_variables.push_back(sys_variable);
  GEN_TENANT_SCHEMA(tenant_schema, 4, "tenant4", 0);
  tenant_schemas.push_back(tenant_schema);
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 4, 0);
  sys_variables.push_back(sys_variable);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenants(tenant_schemas));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variables(sys_variables));
  ObArray<uint64_t> tenant_ids;
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_ids(tenant_ids));
  ASSERT_EQ(4, tenant_ids.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(tenant_ids.at(i), tenant_schemas.at(i).get_tenant_id());
  }
  // del tenants
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_tenants(tenant_ids));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tenant_ids(tenant_ids));
  ASSERT_EQ(0, tenant_ids.count());

  // add_user
  schema_mgr.reset();
  tenant_id = 1;
  tenant_name = "tenant1";
  user_id = combine_id(1, 1);
  user_name = "user1";
  const ObSimpleUserSchema* user = NULL;
  GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_USER_SCHEMA(user_schema, tenant_id, user_id, user_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_user(user_schema));
  // get user by id
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schema(user_id, user));
  ASSERT_EQ(user_schema, *user);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schema(combine_id(1, 2), user));
  ASSERT_EQ(NULL, user);
  // get user by name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schema(tenant_id, user_name, OB_DEFAULT_HOST_NAME, user));
  ASSERT_EQ(user_schema, *user);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schema(tenant_id, "user_not_exist", OB_DEFAULT_HOST_NAME, user));
  ASSERT_EQ(NULL, user);
  // del_user
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_user(ObTenantUserId(tenant_id, user_id)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schema(user_id, user));
  ASSERT_EQ(NULL, user);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schema(tenant_id, user_name, OB_DEFAULT_HOST_NAME, user));
  ASSERT_EQ(NULL, user);
  // get users in tenant
  schema_mgr.reset();
  ObArray<ObSimpleUserSchema> user_schemas1;
  ObArray<ObSimpleUserSchema> user_schemas2;
  ObArray<const ObSimpleUserSchema*> users;
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_USER_SCHEMA(user_schema, 1, combine_id(1, 1), "db1", 0);
  user_schemas1.push_back(user_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_user(user_schema));
  GEN_USER_SCHEMA(user_schema, 1, combine_id(1, 2), "db2", 0);
  user_schemas1.push_back(user_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_user(user_schema));
  GEN_TENANT_SCHEMA(tenant_schema, 2, "tenant2", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 2, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_USER_SCHEMA(user_schema, 2, combine_id(2, 1), "db3", 0);
  user_schemas2.push_back(user_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_user(user_schema));
  GEN_USER_SCHEMA(user_schema, 2, combine_id(2, 2), "db4", 0);
  user_schemas2.push_back(user_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_user(user_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schemas_in_tenant(1, users));
  ASSERT_EQ(2, users.count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(user_schemas1.at(i), *users.at(i));
  }
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schemas_in_tenant(2, users));
  ASSERT_EQ(2, users.count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(user_schemas2.at(i), *users.at(i));
  }
  // add users
  schema_mgr.reset();
  GEN_TENANT_SCHEMA(tenant_schema, tenant_id, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  ObArray<ObSimpleUserSchema> user_schemas;
  GEN_USER_SCHEMA(user_schema, tenant_id, combine_id(1, 1), "user1", 0);
  user_schemas.push_back(user_schema);
  GEN_USER_SCHEMA(user_schema, tenant_id, combine_id(1, 2), "user2", 0);
  user_schemas.push_back(user_schema);
  GEN_USER_SCHEMA(user_schema, tenant_id, combine_id(1, 3), "user3", 0);
  user_schemas.push_back(user_schema);
  GEN_USER_SCHEMA(user_schema, tenant_id, combine_id(1, 4), "user4", 0);
  user_schemas.push_back(user_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_users(user_schemas));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schemas_in_tenant(tenant_id, users));
  ASSERT_EQ(4, users.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(*users.at(i), user_schemas.at(i));
  }
  // del users
  ObArray<ObTenantUserId> tenant_user_ids;
  tenant_user_ids.push_back(ObTenantUserId(1, combine_id(1, 1)));
  tenant_user_ids.push_back(ObTenantUserId(1, combine_id(1, 2)));
  tenant_user_ids.push_back(ObTenantUserId(1, combine_id(1, 3)));
  tenant_user_ids.push_back(ObTenantUserId(1, combine_id(1, 4)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_users(tenant_user_ids));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_user_schemas_in_tenant(tenant_id, users));
  ASSERT_EQ(0, users.count());

  // add_database
  schema_mgr.reset();
  tenant_id = 1;
  tenant_name = "tenant1";
  database_id = combine_id(1, 1);
  database_name = "db1";
  const ObSimpleDatabaseSchema* db = NULL;
  GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_DATABASE_SCHEMA(database_schema, tenant_id, database_id, database_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  // get database by id
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schema(database_id, db));
  ASSERT_EQ(database_schema, *db);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schema(combine_id(1, 2), db));
  ASSERT_EQ(NULL, db);
  // get database by name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schema(tenant_id, database_name, db));
  ASSERT_EQ(database_schema, *db);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schema(tenant_id, "db_not_exist", db));
  ASSERT_EQ(NULL, db);
  // del_database
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_database(ObTenantDatabaseId(tenant_id, database_id)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schema(database_id, db));
  ASSERT_EQ(NULL, db);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schema(tenant_id, "db1", db));
  ASSERT_EQ(NULL, db);
  // get databases in tenant
  schema_mgr.reset();
  ObArray<ObSimpleDatabaseSchema> database_schemas1;
  ObArray<ObSimpleDatabaseSchema> database_schemas2;
  ObArray<const ObSimpleDatabaseSchema*> databases;
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 1), "db1", 0);
  database_schemas1.push_back(database_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 2), "db2", 0);
  database_schemas1.push_back(database_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  GEN_TENANT_SCHEMA(tenant_schema, 2, "tenant2", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 2, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_DATABASE_SCHEMA(database_schema, 2, combine_id(2, 1), "db3", 0);
  database_schemas2.push_back(database_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  GEN_DATABASE_SCHEMA(database_schema, 2, combine_id(2, 2), "db4", 0);
  database_schemas2.push_back(database_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schemas_in_tenant(1, databases));
  ASSERT_EQ(2, databases.count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(database_schemas1.at(i), *databases.at(i));
  }
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schemas_in_tenant(2, databases));
  ASSERT_EQ(2, databases.count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(database_schemas2.at(i), *databases.at(i));
  }
  // add databases
  schema_mgr.reset();
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  ObArray<ObSimpleDatabaseSchema> database_schemas;
  GEN_DATABASE_SCHEMA(database_schema, tenant_id, combine_id(1, 1), "tg1", 0);
  database_schemas.push_back(database_schema);
  GEN_DATABASE_SCHEMA(database_schema, tenant_id, combine_id(1, 2), "tg2", 0);
  database_schemas.push_back(database_schema);
  GEN_DATABASE_SCHEMA(database_schema, tenant_id, combine_id(1, 3), "tg3", 0);
  database_schemas.push_back(database_schema);
  GEN_DATABASE_SCHEMA(database_schema, tenant_id, combine_id(1, 4), "tg4", 0);
  database_schemas.push_back(database_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_databases(database_schemas));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schemas_in_tenant(tenant_id, databases));
  ASSERT_EQ(4, databases.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(*databases.at(i), database_schemas.at(i));
  }
  // del databases
  ObArray<ObTenantDatabaseId> tenant_database_ids;
  tenant_database_ids.push_back(ObTenantDatabaseId(1, combine_id(1, 1)));
  tenant_database_ids.push_back(ObTenantDatabaseId(1, combine_id(1, 2)));
  tenant_database_ids.push_back(ObTenantDatabaseId(1, combine_id(1, 3)));
  tenant_database_ids.push_back(ObTenantDatabaseId(1, combine_id(1, 4)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_databases(tenant_database_ids));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schemas_in_tenant(tenant_id, databases));
  ASSERT_EQ(0, databases.count());

  // add_tablegroup
  schema_mgr.reset();
  tenant_id = 1;
  tenant_name = "tenant1";
  tablegroup_id = combine_id(1, 1);
  tablegroup_name = "tg1";
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  const ObSimpleTablegroupSchema* tg = NULL;
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, tablegroup_id, tablegroup_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroup(tablegroup_schema));
  // get tablegroup by id
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schema(tablegroup_id, tg));
  ASSERT_EQ(tablegroup_schema, *tg);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schema(combine_id(1, 2), tg));
  ASSERT_EQ(NULL, tg);
  // get tablegroup by name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schema(tenant_id, tablegroup_name, tg));
  ASSERT_EQ(tablegroup_schema, *tg);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schema(tenant_id, "tg_not_exist", tg));
  ASSERT_EQ(NULL, tg);
  // del_tablegroup
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_tablegroup(ObTenantTablegroupId(tenant_id, tablegroup_id)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schema(tablegroup_id, tg));
  ASSERT_EQ(NULL, tg);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schema(tenant_id, "tg1", tg));
  ASSERT_EQ(NULL, tg);
  // get tablegroup schemas in tenant
  schema_mgr.reset();
  ObArray<ObSimpleTablegroupSchema> tablegroup_schemas1;
  ObArray<ObSimpleTablegroupSchema> tablegroup_schemas2;
  ObArray<const ObSimpleTablegroupSchema*> tablegroups;
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 1, combine_id(1, 1), "tg1", 0);
  tablegroup_schemas1.push_back(tablegroup_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroup(tablegroup_schema));
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 1, combine_id(1, 2), "tg2", 0);
  tablegroup_schemas1.push_back(tablegroup_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroup(tablegroup_schema));
  GEN_TENANT_SCHEMA(tenant_schema, 2, "tenant2", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 2, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 2, combine_id(2, 1), "tg3", 0);
  tablegroup_schemas2.push_back(tablegroup_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroup(tablegroup_schema));
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 2, combine_id(2, 2), "tg4", 0);
  tablegroup_schemas2.push_back(tablegroup_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroup(tablegroup_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schemas_in_tenant(1, tablegroups));
  ASSERT_EQ(2, tablegroups.count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(tablegroup_schemas1.at(i), *tablegroups.at(i));
  }
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schemas_in_tenant(2, tablegroups));
  ASSERT_EQ(2, tablegroups.count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(tablegroup_schemas2.at(i), *tablegroups.at(i));
  }
  // add tablegroups
  schema_mgr.reset();
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  ObArray<ObSimpleTablegroupSchema> tablegroup_schemas;
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, combine_id(1, 1), "tg1", 0);
  tablegroup_schemas.push_back(tablegroup_schema);
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, combine_id(1, 2), "tg2", 0);
  tablegroup_schemas.push_back(tablegroup_schema);
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, combine_id(1, 3), "tg3", 0);
  tablegroup_schemas.push_back(tablegroup_schema);
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, combine_id(1, 4), "tg4", 0);
  tablegroup_schemas.push_back(tablegroup_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroups(tablegroup_schemas));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups));
  ASSERT_EQ(4, tablegroups.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(*tablegroups.at(i), tablegroup_schemas.at(i));
  }
  // del tablegroups
  ObArray<ObTenantTablegroupId> tenant_tablegroup_ids;
  tenant_tablegroup_ids.push_back(ObTenantTablegroupId(1, combine_id(1, 1)));
  tenant_tablegroup_ids.push_back(ObTenantTablegroupId(1, combine_id(1, 2)));
  tenant_tablegroup_ids.push_back(ObTenantTablegroupId(1, combine_id(1, 3)));
  tenant_tablegroup_ids.push_back(ObTenantTablegroupId(1, combine_id(1, 4)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_tablegroups(tenant_tablegroup_ids));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups));
  ASSERT_EQ(0, tablegroups.count());

  // add table
  tenant_id = 1;
  database_id = combine_id(1, 1);
  table_id = combine_id(1, 1);
  table_name = "table1";
  GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, table_id, table_name, USER_TABLE, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  const ObSimpleTableSchemaV2* table = NULL;
  // get table by id
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(table_id, table));
  ASSERT_EQ(table_schema, *table);
  // get table by name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, table_name, false, table));
  ASSERT_EQ(table_schema, *table);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, table_name, true, table));
  ASSERT_EQ(NULL, table);
  // rename table
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, table_id, "table1_renamed", USER_TABLE, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(table_id, table));
  ASSERT_EQ(table_schema, *table);
  // get table by old_name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, table_name, false, table));
  ASSERT_EQ(NULL, table);
  // get table by new_name
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, "table1_renamed", false, table));
  ASSERT_EQ(table_schema, *table);
  // get index
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, index_table_id, "index1", USER_INDEX, 0);
  table_schema.set_data_table_id(table_id);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(index_table_id, table));
  ASSERT_EQ(table_schema, *table);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, "index1", false, table));
  ASSERT_EQ(NULL, table);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, "index1", true, table));
  ASSERT_EQ(table_schema, *table);
  // del table
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_table(ObTenantTableId(tenant_id, index_table_id)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(index_table_id, table));
  ASSERT_EQ(NULL, table);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, "index1", true, table));
  ASSERT_EQ(NULL, table);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_table(ObTenantTableId(tenant_id, table_id)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(table_id, table));
  ASSERT_EQ(NULL, table);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(tenant_id, database_id, 0, table_name, false, table));
  ASSERT_EQ(NULL, table);
  // get_table_schemas_in_tenant
  ObArray<ObSimpleTableSchemaV2> table_schemas;
  ObArray<const ObSimpleTableSchemaV2*> tables;
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 1), "table1", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 2), "table2", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 3), "table3", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 4), "index1", USER_INDEX, 0);
  table_schema.set_data_table_id(combine_id(1, 1));
  table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tenant(tenant_id, tables));
  ASSERT_EQ(4, tables.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(*tables.at(i), table_schemas.at(i));
  }
  // add tables
  schema_mgr.reset();
  GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  table_schemas.reset();
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 1), "table1", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 2), "table2", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 3), "table3", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, combine_id(1, 4), "table4", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tables(table_schemas));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tenant(tenant_id, tables));
  ASSERT_EQ(4, tables.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(*tables.at(i), table_schemas.at(i));
  }
  // del tables
  ObArray<ObTenantTableId> tenant_table_ids;
  tenant_table_ids.push_back(ObTenantTableId(1, combine_id(1, 1)));
  tenant_table_ids.push_back(ObTenantTableId(1, combine_id(1, 2)));
  tenant_table_ids.push_back(ObTenantTableId(1, combine_id(1, 3)));
  tenant_table_ids.push_back(ObTenantTableId(1, combine_id(1, 4)));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_tables(tenant_table_ids));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tenant(tenant_id, tables));
  ASSERT_EQ(0, tables.count());

  // del schemas in tenant
  schema_mgr.reset();
  GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 1), "db1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 2), "db2", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_database(database_schema));
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 1, combine_id(1, 1), "tg1", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tablegroup(tablegroup_schema));
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, 1), "db1_table1", USER_TABLE, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, 2), "db1_table2", USER_TABLE, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 2), combine_id(1, 3), "db2_table1", USER_TABLE, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 2), combine_id(1, 4), "db2_table2", USER_TABLE, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 2), combine_id(1, 5), "index", USER_INDEX, 0);
  table_schema.set_data_table_id(combine_id(1, 4));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(table_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schemas_in_tenant(1, databases));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schemas_in_tenant(1, tablegroups));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tenant(1, tables));
  ASSERT_EQ(2, databases.count());
  ASSERT_EQ(1, tablegroups.count());
  ASSERT_EQ(5, tables.count());
  ASSERT_EQ(OB_SUCCESS, schema_mgr.del_schemas_in_tenant(1));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_database_schemas_in_tenant(1, databases));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_tablegroup_schemas_in_tenant(1, tablegroups));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tenant(1, tables));
  ASSERT_EQ(0, databases.count());
  ASSERT_EQ(0, tablegroups.count());
  ASSERT_EQ(0, tables.count());
}

TEST_F(TestSchemaMgr, index_relation)
{
  ObSchemaMgr schema_mgr;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;

  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  ObArray<ObSimpleTableSchemaV2> table_schemas;
  const uint64_t table_id = combine_id(1, 1);
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), table_id, "table", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, 2), "index1", USER_INDEX, 0);
  table_schema.set_data_table_id(combine_id(1, 1));
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, 3), "index2", USER_INDEX, 0);
  table_schema.set_data_table_id(combine_id(1, 1));
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, 4), "index3", USER_INDEX, 0);
  table_schema.set_data_table_id(combine_id(1, 1));
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, 5), "index4", USER_INDEX, 0);
  table_schema.set_data_table_id(combine_id(1, 1));
  table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tables(table_schemas));
  ObArray<const ObSimpleTableSchemaV2*> index_schemas;
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_index_schemas(table_id, index_schemas));
  ASSERT_EQ(table_schemas.count() - 1, index_schemas.count());
  for (int64_t i = 1; i < table_schemas.count(); ++i) {
    ASSERT_EQ(table_schemas.at(i), *index_schemas.at(i - 1));
  }
}

TEST_F(TestSchemaMgr, check_database_exists_in_tablegroup)
{
  ObSchemaMgr schema_mgr;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleDatabaseSchema database_schema;
  ObSimpleSysVariableSchema sys_variable;

  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  ObArray<ObSimpleDatabaseSchema> database_schemas;
  const uint64_t tg_id = combine_id(1, 1);
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 1), "database1", 0);
  database_schemas.push_back(database_schema);
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 2), "database2", 0);
  database_schemas.push_back(database_schema);
  GEN_DATABASE_SCHEMA(database_schema, 1, combine_id(1, 3), "database3", 0);
  database_schema.set_default_tablegroup_id(tg_id);
  database_schemas.push_back(database_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_databases(database_schemas));

  bool not_empty = false;
  ASSERT_EQ(OB_SUCCESS, schema_mgr.check_database_exists_in_tablegroup(1, combine_id(1, 1), not_empty));
  ASSERT_TRUE(not_empty);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.check_database_exists_in_tablegroup(1, combine_id(1, 2), not_empty));
  ASSERT_FALSE(not_empty);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.check_database_exists_in_tablegroup(1, combine_id(1, 3), not_empty));
  ASSERT_FALSE(not_empty);
}

TEST_F(TestSchemaMgr, iteration_interface)
{
  ObSchemaMgr schema_mgr;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;

  const uint64_t db_id1 = combine_id(1, 1);
  const uint64_t db_id2 = combine_id(1, 2);
  const uint64_t tg_id1 = combine_id(1, 1);
  const uint64_t tg_id2 = combine_id(1, 2);
  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  ObArray<ObSimpleTableSchemaV2> table_schemas;
  ObArray<ObSimpleTableSchemaV2> db1_table_schemas;
  ObArray<ObSimpleTableSchemaV2> db2_table_schemas;
  ObArray<ObSimpleTableSchemaV2> tg1_table_schemas;
  ObArray<ObSimpleTableSchemaV2> tg2_table_schemas;
  GEN_TABLE_SCHEMA(table_schema, 1, db_id1, combine_id(1, 1), "table1", USER_TABLE, 0);
  table_schema.set_tablegroup_id(tg_id1);
  table_schemas.push_back(table_schema);
  db1_table_schemas.push_back(table_schema);
  tg1_table_schemas.push_back(table_schema);
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, db_id1, combine_id(1, 2), "table2", USER_TABLE, 0);
  table_schemas.push_back(table_schema);
  db1_table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, db_id1, combine_id(1, 3), "table3", USER_TABLE, 0);
  table_schema.set_tablegroup_id(tg_id2);
  table_schemas.push_back(table_schema);
  db1_table_schemas.push_back(table_schema);
  tg2_table_schemas.push_back(table_schema);
  table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, db_id2, combine_id(1, 4), "table4", USER_TABLE, 0);
  table_schema.set_tablegroup_id(tg_id2);
  table_schemas.push_back(table_schema);
  db2_table_schemas.push_back(table_schema);
  tg2_table_schemas.push_back(table_schema);
  GEN_TABLE_SCHEMA(table_schema, 1, db_id2, combine_id(1, 5), "table5", USER_TABLE, 0);
  table_schema.set_tablegroup_id(tg_id2);
  table_schemas.push_back(table_schema);
  db2_table_schemas.push_back(table_schema);
  tg2_table_schemas.push_back(table_schema);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tables(table_schemas));

  ObArray<const ObSimpleTableSchemaV2*> simple_tables;
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_database(1, db_id1, simple_tables));
  ASSERT_EQ(db1_table_schemas.count(), simple_tables.count());
  for (int64_t i = 1; i < simple_tables.count(); ++i) {
    const ObSimpleTableSchemaV2* simple_table = simple_tables.at(i);
    for (int64_t j = 1; j < db1_table_schemas.count(); ++j) {
      const ObSimpleTableSchemaV2& table = db1_table_schemas.at(j);
      if (simple_table->get_table_id() == table.get_table_id()) {
        ASSERT_EQ(table, *simple_table);
      }
    }
  }

  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_database(1, db_id2, simple_tables));
  ASSERT_EQ(db2_table_schemas.count(), simple_tables.count());
  for (int64_t i = 1; i < simple_tables.count(); ++i) {
    const ObSimpleTableSchemaV2* simple_table = simple_tables.at(i);
    for (int64_t j = 1; j < db2_table_schemas.count(); ++j) {
      const ObSimpleTableSchemaV2& table = db2_table_schemas.at(j);
      if (simple_table->get_table_id() == table.get_table_id()) {
        ASSERT_EQ(table, *simple_table);
      }
    }
  }

  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tablegroup(1, tg_id1, simple_tables));
  ASSERT_EQ(tg1_table_schemas.count(), simple_tables.count());
  for (int64_t i = 1; i < simple_tables.count(); ++i) {
    const ObSimpleTableSchemaV2* simple_table = simple_tables.at(i);
    for (int64_t j = 1; j < tg1_table_schemas.count(); ++j) {
      const ObSimpleTableSchemaV2& table = tg1_table_schemas.at(j);
      if (simple_table->get_table_id() == table.get_table_id()) {
        ASSERT_EQ(table, *simple_table);
      }
    }
  }

  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tablegroup(1, tg_id2, simple_tables));
  ASSERT_EQ(tg2_table_schemas.count(), simple_tables.count());
  for (int64_t i = 1; i < simple_tables.count(); ++i) {
    const ObSimpleTableSchemaV2* simple_table = simple_tables.at(i);
    for (int64_t j = 1; j < tg2_table_schemas.count(); ++j) {
      const ObSimpleTableSchemaV2& table = tg2_table_schemas.at(j);
      if (simple_table->get_table_id() == table.get_table_id()) {
        ASSERT_EQ(table, *simple_table);
      }
    }
  }
}

TEST_F(TestSchemaMgr, integrate_with_outline_mgr)
{
  // reset
  {
    ObSchemaMgr mgr;
    ObSimpleOutlineSchema outline;
    outline.set_tenant_id(1);
    outline.set_outline_id(combine_id(1, 1));
    outline.set_database_id(combine_id(1, 1));
    outline.set_name("outline");
    outline.set_signature("sig");
    outline.set_schema_version(0);
    ASSERT_EQ(OB_SUCCESS, mgr.outline_mgr_.add_outline(outline));
    const ObSimpleOutlineSchema* outline_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr.outline_mgr_.get_outline_schema(outline.get_outline_id(), outline_ptr));
    ASSERT_TRUE(NULL != outline_ptr);
    mgr.reset();
    ASSERT_EQ(OB_SUCCESS, mgr.outline_mgr_.get_outline_schema(outline.get_outline_id(), outline_ptr));
    ASSERT_TRUE(NULL == outline_ptr);
  }

  // assign or deep_copy
  {
    ObSchemaMgr mgr;
    ObSimpleOutlineSchema outline;
    outline.set_tenant_id(1);
    outline.set_outline_id(combine_id(1, 1));
    outline.set_database_id(combine_id(1, 1));
    outline.set_name("outline");
    outline.set_signature("sig");
    outline.set_schema_version(0);
    ASSERT_EQ(OB_SUCCESS, mgr.outline_mgr_.add_outline(outline));
    ObSchemaMgr mgr2;
    mgr2.init();
    ASSERT_EQ(OB_SUCCESS, mgr2.assign(mgr));
    const ObSimpleOutlineSchema* outline_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr2.outline_mgr_.get_outline_schema(outline.get_outline_id(), outline_ptr));
    ASSERT_TRUE(NULL != outline_ptr);
    ObSchemaMgr mgr3;
    mgr3.init();
    ASSERT_EQ(OB_SUCCESS, mgr3.deep_copy(mgr));
    ASSERT_EQ(OB_SUCCESS, mgr3.outline_mgr_.get_outline_schema(outline.get_outline_id(), outline_ptr));
    ASSERT_TRUE(NULL != outline_ptr);
  }
}

TEST_F(TestSchemaMgr, integrate_with_priv_mgr)
{
  // reset
  {
    ObSchemaMgr mgr;
    ObDBPriv db_priv;
    db_priv.set_tenant_id(1);
    db_priv.set_user_id(combine_id(1, 1));
    db_priv.set_database_name("test");
    db_priv.set_priv_set(OB_PRIV_SET_EMPTY);
    ASSERT_EQ(OB_SUCCESS, mgr.priv_mgr_.add_db_priv(db_priv));
    const ObDBPriv* db_priv_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr.priv_mgr_.get_db_priv(db_priv.get_original_key(), db_priv_ptr));
    ASSERT_TRUE(NULL != db_priv_ptr);
    mgr.reset();
    ASSERT_EQ(OB_SUCCESS, mgr.priv_mgr_.get_db_priv(db_priv.get_original_key(), db_priv_ptr));
    ASSERT_TRUE(NULL == db_priv_ptr);
  }

  // assign or deep_copy
  {
    ObSchemaMgr mgr;
    ObDBPriv db_priv;
    db_priv.set_tenant_id(1);
    db_priv.set_user_id(combine_id(1, 1));
    db_priv.set_database_name("test");
    db_priv.set_priv_set(OB_PRIV_SET_EMPTY);
    ASSERT_EQ(OB_SUCCESS, mgr.priv_mgr_.add_db_priv(db_priv));
    ObSchemaMgr mgr2;
    mgr2.init();
    ASSERT_EQ(OB_SUCCESS, mgr2.assign(mgr));
    const ObDBPriv* db_priv_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr2.priv_mgr_.get_db_priv(db_priv.get_original_key(), db_priv_ptr));
    ASSERT_TRUE(NULL != db_priv_ptr);
    ObSchemaMgr mgr3;
    mgr3.init();
    ASSERT_EQ(OB_SUCCESS, mgr3.deep_copy(mgr));
    ASSERT_EQ(OB_SUCCESS, mgr3.priv_mgr_.get_db_priv(db_priv.get_original_key(), db_priv_ptr));
    ASSERT_TRUE(NULL != db_priv_ptr);
  }
}

TEST_F(TestSchemaMgr, assign_and_deep_copy)
{
  ObMalloc global_allocator(ObModIds::OB_TEMP_VARIABLES);
  ObSchemaMgr mgr1(global_allocator);
  ObSchemaMgr mgr2;
  ObSchemaMgr mgr3;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;
  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, mgr1.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, mgr1.sys_variable_mgr_.add_sys_variable(sys_variable));

  mgr2.init();
  mgr3.init();
  ASSERT_EQ(OB_SUCCESS, mgr2.assign(mgr1));
  ASSERT_EQ(0, mgr2.local_allocator_.used());
  ASSERT_EQ(OB_SUCCESS, mgr3.deep_copy(mgr1));
  ASSERT_NE(0, mgr3.local_allocator_.used());

  const ObSimpleTenantSchema* tenant = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_schema(1, tenant));
  ASSERT_TRUE(NULL != tenant);
  ASSERT_EQ(tenant_schema, *tenant);
  ASSERT_EQ(OB_SUCCESS, mgr3.get_tenant_schema(1, tenant));
  ASSERT_TRUE(NULL != tenant);
  ASSERT_EQ(tenant_schema, *tenant);
}

TEST_F(TestSchemaMgr, perf)
{
  ObSchemaMgr schema_mgr;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;

  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  const int64_t table_num = 10000;
  ObArray<ObSimpleTableSchemaV2> table_schemas;
  char table_name[50];
  for (int64_t i = 1; i < table_num; ++i) {
    snprintf(table_name, 50, "table%ld", i);
    GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, i), table_name, USER_TABLE, 0);
    table_schemas.push_back(table_schema);
  }
  int64_t time_1 = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tables(table_schemas));
  int64_t time_2 = ObTimeUtility::current_time();
  LOG_INFO("batch add tables cost", K(time_2 - time_1));
  ObSchemaMgr new_mgr;
  new_mgr.init();
  ASSERT_EQ(OB_SUCCESS, new_mgr.assign(schema_mgr));
  int64_t time_3 = ObTimeUtility::current_time();
  LOG_INFO("schema_mgr assign cost", K(time_3 - time_2));
  ObArray<const ObSimpleTableSchemaV2*> tables;
  ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schemas_in_tenant(1, tables));
  int64_t time_4 = ObTimeUtility::current_time();
  LOG_INFO("schema_mgr assign cost", K(time_4 - time_3));
}

TEST_F(TestSchemaMgr, deal_with_rename)
{
  ObSchemaMgr schema_mgr;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;

  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  // user table
  {
    ObSimpleTableSchemaV2 x;
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "x", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "x_renamed", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    const ObSimpleTableSchemaV2* table = NULL;
    ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(combine_id(1, 1), table));
    ASSERT_TRUE(NULL != table);
    ASSERT_EQ(ObString("x_renamed"), table->get_table_name_str());
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 2), combine_id(1, 1), "x_renamed", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(combine_id(1, 1), table));
    ASSERT_TRUE(NULL != table);
    ASSERT_EQ(combine_id(1, 2), table->get_database_id());
  }

  // index
  {
    const uint64_t data_table_id = combine_id(1, 2);
    ObSimpleTableSchemaV2 x;
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "__idx", USER_INDEX, 0);
    x.set_data_table_id(data_table_id);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 2), combine_id(1, 1), "__idx", USER_INDEX, 0);
    x.set_data_table_id(data_table_id);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    const ObSimpleTableSchemaV2* table = NULL;
    ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(combine_id(1, 1), table));
    ASSERT_TRUE(NULL != table);
    ASSERT_EQ(combine_id(1, 2), table->get_database_id());
  }
}

TEST_F(TestSchemaMgr, rename_swap_table_name)
{
  ObSchemaMgr schema_mgr;
  ObSimpleTenantSchema tenant_schema;
  ObSimpleTableSchemaV2 table_schema;
  ObSimpleSysVariableSchema sys_variable;

  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.add_tenant(tenant_schema));
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  ASSERT_EQ(OB_SUCCESS, schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable));

  // rename case, rename y to z, x to y ,z to x
  {
    ObSimpleTableSchemaV2 x;
    ObSimpleTableSchemaV2 y;
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "x", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    GEN_TABLE_SCHEMA(y, 1, combine_id(1, 1), combine_id(1, 2), "y", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(y));
    ASSERT_EQ(schema_mgr.table_infos_.count(),
        schema_mgr.table_name_map_.item_count() + schema_mgr.index_name_map_.item_count());
    GEN_TABLE_SCHEMA(y, 1, combine_id(1, 1), combine_id(1, 2), "x", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(y));
    ASSERT_NE(schema_mgr.table_infos_.count(),
        schema_mgr.table_name_map_.item_count() + schema_mgr.index_name_map_.item_count());
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "y", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    ASSERT_EQ(OB_SUCCESS, schema_mgr.rebuild_schema_meta_if_not_consistent());
    ASSERT_EQ(schema_mgr.table_infos_.count(),
        schema_mgr.table_name_map_.item_count() + schema_mgr.index_name_map_.item_count());
  }
  // rename x to z, y to x
  {
    ObSimpleTableSchemaV2 x;
    ObSimpleTableSchemaV2 y;
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "x", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    GEN_TABLE_SCHEMA(y, 1, combine_id(1, 1), combine_id(1, 2), "y", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(y));
    ASSERT_EQ(OB_SUCCESS, schema_mgr.rebuild_schema_meta_if_not_consistent());
    ASSERT_EQ(schema_mgr.table_infos_.count(),
        schema_mgr.table_name_map_.item_count() + schema_mgr.index_name_map_.item_count());
    GEN_TABLE_SCHEMA(y, 1, combine_id(1, 1), combine_id(1, 2), "x", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(y));
    GEN_TABLE_SCHEMA(x, 1, combine_id(1, 1), combine_id(1, 1), "z", USER_TABLE, 0);
    ASSERT_EQ(OB_SUCCESS, schema_mgr.add_table(x));
    ASSERT_EQ(OB_SUCCESS, schema_mgr.rebuild_schema_meta_if_not_consistent());
    ASSERT_EQ(schema_mgr.table_infos_.count(),
        schema_mgr.table_name_map_.item_count() + schema_mgr.index_name_map_.item_count());
    const ObSimpleTableSchemaV2* table = NULL;
    ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(combine_id(1, 1), table));
    ASSERT_TRUE(NULL != table);
    ASSERT_EQ(ObString("z"), table->get_table_name_str());
    ASSERT_EQ(OB_SUCCESS, schema_mgr.get_table_schema(combine_id(1, 2), table));
    ASSERT_TRUE(NULL != table);
    ASSERT_EQ(ObString("x"), table->get_table_name_str());
  }
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
