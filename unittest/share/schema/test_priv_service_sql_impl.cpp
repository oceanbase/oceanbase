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

#include <gtest/gtest.h>
#include "db_initializer.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_service.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "ob_schema_test_utils.cpp"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

class TestPrivServiceSQLImpl : public ::testing::Test
{
public:

  virtual void SetUp();
  virtual void TearDown() {}

protected:
  DBInitializer db_initer_;
  ObSchemaServiceSQLImpl schema_service_;
};


void TestPrivServiceSQLImpl::SetUp()
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
}

TEST_F(TestPrivServiceSQLImpl, user_privilege_operations)
{
  int ret = OB_SUCCESS;
  const int64_t schema_version = INT64_MAX;
  ObUserInfo user;
  user.set_tenant_id(2);
  user.set_user_id(1);
  user.set_user_name("lilei");
  user_info.set_host(OB_DEFAULT_HOST_NAME);
  user.set_passwd("lilei");
  user.set_info("student int class 1");
  user.set_is_locked(false);
  user.set_priv_set(OB_PRIV_SELECT | OB_PRIV_CREATE);

  //create user
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space(2));
  ObISQLClient &sql_client = db_initer_.get_sql_proxy();
  ret = schema_service_.create_user(user, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  //get batch of users
  ObArray<ObTenantUserId> tenant_user_ids;
  ObArray<ObUserInfo> user_info_array;
  tenant_user_ids.push_back(ObTenantUserId(2, 1));
  ret = schema_service_.get_batch_users(schema_version, tenant_user_ids, &sql_client, user_info_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, user_info_array.count());
  //Equal test
  EXPECT_EQ(user.get_tenant_id(), user_info_array[0].get_tenant_id());
  EXPECT_EQ(user.get_user_id(), user_info_array[0].get_user_id());
  EXPECT_EQ(user.get_user_name_str(), user_info_array[0].get_user_name_str());
  EXPECT_EQ(user.get_passwd_str(), user_info_array[0].get_passwd_str());
  EXPECT_EQ(user.get_info_str(), user_info_array[0].get_info_str());
  EXPECT_EQ(user.get_is_locked(), user_info_array[0].get_is_locked());
  EXPECT_EQ(user.get_priv_set(), user_info_array[0].get_priv_set());
  user_info_array.reset();

  //set passwd
  ObString new_passwd("sql");
  user.set_passwd("sql");
  ret = schema_service_.set_passwd(user, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = schema_service_.get_batch_users(schema_version, tenant_user_ids, &sql_client, user_info_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, user_info_array.count());
  EXPECT_EQ(new_passwd, user_info_array[0].get_passwd_str());
  user_info_array.reset();

  //drop user
  ret = schema_service_.drop_user(2, 1, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = schema_service_.get_batch_users(schema_version, tenant_user_ids, &sql_client, user_info_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, user_info_array.count());
}

TEST_F(TestPrivServiceSQLImpl, database_privilege_operations)
{
  int ret = OB_SUCCESS;
  const int64_t schema_version = INT64_MAX;
  ObOriginalDBKey db_priv_key;
  db_priv_key.tenant_id_ = 2;
  db_priv_key.user_id_ = 1;
  db_priv_key.db_ = "test";

  //grant database privilege
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space(2));
  ObISQLClient &sql_client = db_initer_.get_sql_proxy();
  ret = schema_service_.grant_database(db_priv_key, OB_PRIV_SELECT | OB_PRIV_CREATE, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  //get batch of database privilege
  ObArray<ObOriginalDBKey> db_priv_keys;
  ObArray<ObDBPriv> db_priv_array;
  db_priv_keys.push_back(db_priv_key);
  ret = schema_service_.get_batch_db_privs(schema_version, db_priv_keys, &sql_client, db_priv_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, db_priv_array.count());
  //Equal test
  EXPECT_EQ(db_priv_key.tenant_id_, db_priv_array[0].get_tenant_id());
  EXPECT_EQ(db_priv_key.user_id_, db_priv_array[0].get_user_id());
  EXPECT_EQ(db_priv_key.db_, db_priv_array[0].get_database_name_str());
  EXPECT_EQ(OB_PRIV_SELECT | OB_PRIV_CREATE, db_priv_array[0].get_priv_set());
  db_priv_array.reset();

  //Revoke database privilege
  ret = schema_service_.revoke_database(db_priv_key, 0, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  db_priv_array.reset();
  ret = schema_service_.get_batch_db_privs(schema_version, db_priv_keys, &sql_client, db_priv_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, db_priv_array.count());

  //grant database privileges
  ret = schema_service_.grant_database(db_priv_key, OB_PRIV_SELECT | OB_PRIV_CREATE, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  db_priv_key.user_id_ = 2;
  ret = schema_service_.grant_database(db_priv_key, OB_PRIV_SELECT | OB_PRIV_CREATE, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  db_priv_array.reset();
  db_priv_keys.reset();

  ret = schema_service_.delete_db_priv(db_priv_key, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestPrivServiceSQLImpl, table_privilege_operations)
{
  const int64_t schema_version = INT64_MAX;
  int ret = OB_SUCCESS;
  ObTablePrivSortKey table_priv_key;
  table_priv_key.tenant_id_ = 2;
  table_priv_key.user_id_ = 1;
  table_priv_key.db_ = "test";
  table_priv_key.table_ = "table_priv";

  //grant table priv
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space(2));
  ObISQLClient &sql_client = db_initer_.get_sql_proxy();
  ret = schema_service_.grant_table(table_priv_key, OB_PRIV_SELECT | OB_PRIV_INSERT, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  //get batch of table privs
  ObArray<ObTablePrivSortKey> table_priv_keys;
  ObArray<ObTablePriv> table_priv_array;
  table_priv_keys.push_back(table_priv_key);
  ret = schema_service_.get_batch_table_privs(schema_version, table_priv_keys, &sql_client, table_priv_array);
  ASSERT_EQ(1, table_priv_array.count());
  //Equal test
  EXPECT_EQ(table_priv_key.tenant_id_, table_priv_array[0].get_tenant_id());
  EXPECT_EQ(table_priv_key.user_id_, table_priv_array[0].get_user_id());
  EXPECT_EQ(table_priv_key.db_, table_priv_array[0].get_database_name_str());
  EXPECT_EQ(table_priv_key.table_, table_priv_array[0].get_table_name_str());
  EXPECT_EQ(OB_PRIV_SELECT | OB_PRIV_INSERT, table_priv_array[0].get_priv_set());
  table_priv_array.reset();

  //Revoke table privilege
  ret = schema_service_.revoke_table(table_priv_key, 0, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = schema_service_.get_batch_table_privs(schema_version, table_priv_keys, &sql_client, table_priv_array);
  ASSERT_EQ(0, table_priv_array.count());//No table privileges now.

  //grant table priv
  table_priv_key.user_id_ = 1;
  ret = schema_service_.grant_table(table_priv_key, OB_PRIV_SELECT | OB_PRIV_INSERT, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  table_priv_key.user_id_ = 2;
  ret = schema_service_.grant_table(table_priv_key, OB_PRIV_SELECT | OB_PRIV_INSERT, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);
  //delete table privilege
  ret = schema_service_.delete_table_priv(table_priv_key, &sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  table_priv_keys.reset();
  table_priv_keys.push_back(table_priv_key);
  table_priv_array.reset();
  ret = schema_service_.get_batch_table_privs(schema_version, table_priv_keys, &sql_client, table_priv_array);
  ASSERT_EQ(0, table_priv_array.count());

  table_priv_key.user_id_ = 1;
  table_priv_keys.reset();
  table_priv_keys.push_back(table_priv_key);
  table_priv_array.reset();
  ret = schema_service_.get_batch_table_privs(schema_version, table_priv_keys, &sql_client, table_priv_array);
  ASSERT_EQ(1, table_priv_array.count());

}

}
}
}


int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
