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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include <unistd.h>
#include <gtest/gtest.h>
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#define private public
#define protected public
#include "share/schema/ob_priv_manager.h"
#include "ob_schema_test_utils.cpp"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
class TestPrivManager : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  ObPrivManager priv_mgr_;
};

void TestPrivManager::SetUp()
{
  //obsys::ObSysConfig c1;
  priv_mgr_.init();
}


TEST_F(TestPrivManager, privs_test)
{
  int ret = OB_SUCCESS;

//----------------privileges constructed as follows---------------
//---__all_tenant
//--tenant_id--tenant_name--replica_num--zone_list--resource_pool_list--primary_zone--locked--status--info
//    1          ob            3         127.0.0.1   127.0.0.1            127.0.0.1    0       0
//    2          yz            3         127.0.0.1   127.0.0.1            127.0.0.1    0       0

//----__all_user
//--tenant_id--user_id--user_name--host--passwd--privs----locked
//  1          1        yz1        %     empty   SELECT   false
//  1          2        yz2        %     empty   empty    false
//  2          1        t2_u1      %     empty   empty    false

//----__all_database_privilige
//--tenant_id--user_id--database_name--privs
//  1          2        db             SELECT
//  1          2        ali%           ALTER
//  1          2        alipay%        CREATE
//  2          1        ali            CREATE

//----__all_table_privilege
//--tenant_id--user_id--database_name--table_name--privs
//  1          2        ali            sale        SELECT, INSERT
//  1          1        ali            customer    CREATE, UPDATE
//  2          1        db2            sale        CREATE, UPDATE

  //-----------------add priv test--------------------
  //----add tenant info
  ObArray<ObTenantSchema> tenant_info_array;
  ObTenantSchema tenant;
  ObArray<ObString> zone_list;
  zone_list.push_back("127.0.0.1");
  FILL_TENANT_INFO(tenant, 1, "ob", 3, zone_list, "127.0.0.1", 0, "");
  tenant_info_array.push_back(tenant);

  tenant.reset();
  FILL_TENANT_INFO(tenant, 2, "yz", 3, zone_list, "127.0.0.1", 0, "");
  tenant_info_array.push_back(tenant);

  ret = priv_mgr_.add_new_tenant_info_array(tenant_info_array);
  ASSERT_TRUE(OB_SUCC(ret));
  ret = priv_mgr_.add_tenant_info(NULL, true);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = priv_mgr_.add_user_info(NULL, true);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  //----add user priv
  ObArray<ObUserInfo> user_info_array;
  ObUserInfo user;
  //user: tenant_id, user_id, user_name, passwd, info, is_locked, priv_set
  FILL_USER_INFO(user, 1, 1, "yz1", "", "student in class one", false, OB_PRIV_SELECT);
  user_info_array.push_back(user);

  user.reset();
  FILL_USER_INFO(user, 1, 2, "yz2", "", "student in class one", false, 0);
  user_info_array.push_back(user);

  user.reset();
  FILL_USER_INFO(user, 1, 3, "user", "passwd", "test", false, 0);
  user_info_array.push_back(user);

  user.reset();
  FILL_USER_INFO(user, 2, 1, "t2_u1", "", "", false, 0);
  user_info_array.push_back(user);

  ret = priv_mgr_.add_new_user_info_array(user_info_array);
  ASSERT_TRUE(OB_SUCC(ret));
  ObUserInfo new_info;
  new_info.tenant_id_ = OB_INVALID_ID;
  ret = priv_mgr_.add_new_user_info(new_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ObArray<const ObUserInfo *> user_infos;
  ret = priv_mgr_.get_user_info_with_tenant_id(2, user_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, user_infos.count());

  //----add user db priv
  ObArray<ObDBPriv> db_priv_array;
  ObDBPriv db_priv;
  //db_priv: tenant_id, user_id, db_name, priv_set, sort_value
  FILL_DB_PRIV(db_priv, 1, 2, "db", OB_PRIV_SELECT);
  db_priv_array.push_back(db_priv);

  db_priv.reset();
  FILL_DB_PRIV(db_priv, 1, 2, "ali%", OB_PRIV_ALTER);
  db_priv_array.push_back(db_priv);

  db_priv.reset();
  FILL_DB_PRIV(db_priv, 1, 2, "alipay%", OB_PRIV_CREATE);
  db_priv_array.push_back(db_priv);

  db_priv.reset();
  FILL_DB_PRIV(db_priv, 2, 1, "ali", OB_PRIV_CREATE);
  db_priv_array.push_back(db_priv);

  ret = priv_mgr_.add_new_db_priv_array(db_priv_array);
  ASSERT_TRUE(OB_SUCC(ret));

  //----add user table priv
  //user(1, "yz2") db(ali) table(sale) priv(SELECT,INSERT)
  ObArray<ObTablePriv> table_priv_array;
  ObTablePriv table_priv;
  FILL_TABLE_PRIV(table_priv, 1, 2, "ali", "sale", OB_PRIV_SELECT | OB_PRIV_INSERT);
  table_priv_array.push_back(table_priv);

  //user(1, "yz1") db(ali) table(customer) priv(CREATE,UPDATE)
  table_priv.reset();
  FILL_TABLE_PRIV(table_priv, 1, 1, "ali", "customer", OB_PRIV_CREATE | OB_PRIV_UPDATE);
  table_priv_array.push_back(table_priv);

  //user(1, "yz1") db(ali) table(sale) priv(CREATE,UPDATE)
  table_priv.reset();
  FILL_TABLE_PRIV(table_priv, 1, 1, "taobao", "sale", OB_PRIV_CREATE | OB_PRIV_UPDATE);
  table_priv_array.push_back(table_priv);

  //user(2, "t2_u1") db(db2) table(sale) priv(CREATE,UPDATE)
  table_priv.reset();
  FILL_TABLE_PRIV(table_priv, 2, 1, "db2", "sale", OB_PRIV_CREATE | OB_PRIV_UPDATE);
  table_priv_array.push_back(table_priv);

  ret = priv_mgr_.add_new_table_priv_array(table_priv_array);
  ASSERT_TRUE(OB_SUCC(ret));
  ret = priv_mgr_.add_table_priv(NULL, true);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ObTablePriv new_table_priv;
  new_table_priv.set_tenant_id(OB_INVALID_ID);
  ret = priv_mgr_.add_new_table_priv(new_table_priv);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  db_priv_array.reset();
  ret = priv_mgr_.get_db_priv_with_user_id(1, 2, db_priv_array);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_EQ(3, db_priv_array.count());
  db_priv_array.reset();
  ret = priv_mgr_.get_db_priv_with_user_id(OB_INVALID_ID, 2, db_priv_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, db_priv_array.count());
  db_priv_array.reset();

  ObArray<const ObDBPriv *>db_priv_pointers;
  ret = priv_mgr_.get_db_priv_with_tenant_id(OB_INVALID_ID, db_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, db_priv_pointers.count());
  ret = priv_mgr_.get_db_priv_with_tenant_id(2, db_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, db_priv_pointers.count());
  db_priv_pointers.reset();
  ret = priv_mgr_.get_db_priv_with_user_id(1, 2, db_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, db_priv_pointers.count());
  db_priv_pointers.reset();
  ret = priv_mgr_.get_db_priv_with_user_id(OB_INVALID_ID, OB_INVALID_ID, db_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, db_priv_pointers.count());

  table_priv_array.reset();
  ret = priv_mgr_.get_table_priv_with_user_id(1, 2, table_priv_array);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_EQ(1, table_priv_array.count());

  ObArray<const ObTablePriv *>table_priv_pointers;
  ret = priv_mgr_.get_table_priv_with_user_id(OB_INVALID, OB_INVALID_ID, table_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, table_priv_pointers.count());
  ret = priv_mgr_.get_table_priv_with_user_id(1, 2, table_priv_pointers);
  ASSERT_EQ(1, table_priv_pointers.count());
  table_priv_pointers.reset();
  ret = priv_mgr_.get_table_priv_with_tenant_id(OB_INVALID_ID, table_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  table_priv_pointers.reset();
  ret = priv_mgr_.get_table_priv_with_tenant_id(1, table_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, table_priv_pointers.count());


  //-----------------get priv test--------------------
  //get tenant info
  const ObTenantSchema *p_tenant = priv_mgr_.get_tenant_info("ob");
  ASSERT_TRUE(NULL != p_tenant);
  p_tenant = priv_mgr_.get_tenant_info(OB_INVALID_ID);
  ASSERT_TRUE(NULL == p_tenant);
  p_tenant = priv_mgr_.get_tenant_info(1);
  ASSERT_TRUE(NULL != p_tenant);
  p_tenant = priv_mgr_.get_tenant_info(111);
  ASSERT_TRUE(NULL == p_tenant);
  p_tenant = priv_mgr_.get_tenant_info("yz");
  ASSERT_TRUE(NULL != p_tenant);
  p_tenant = priv_mgr_.get_tenant_info("not exist");
  ASSERT_TRUE(NULL == p_tenant);
  uint64_t tenant_id = OB_INVALID_ID;
  priv_mgr_.get_tenant_id("ob", tenant_id);
  ASSERT_TRUE(1 == tenant_id);
  priv_mgr_.get_tenant_id("yz", tenant_id);
  ASSERT_TRUE(2 == tenant_id);
  //get user priv
  const ObUserInfo *p_user_info = priv_mgr_.get_user_info(1, "yz1");
  ASSERT_TRUE(NULL != p_user_info);
  ASSERT_TRUE(OB_PRIV_SELECT == p_user_info->get_priv_set());
  p_user_info = priv_mgr_.get_user_info(ObTenantUserId(1, 0));
  ASSERT_TRUE(NULL == p_user_info);
  p_user_info = priv_mgr_.get_user_info(ObTenantUserId(OB_INVALID_ID, 1));
  ASSERT_TRUE(NULL == p_user_info);
  p_user_info = priv_mgr_.get_user_info(ObTenantUserId(1, 1));
  ASSERT_TRUE(NULL != p_user_info);
  ObArray<const ObUserInfo *> user_infos_array;
  ret = priv_mgr_.get_user_infos_with_tenant_id(OB_INVALID_ID, user_infos_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, user_infos_array.count());
  user_infos_array.reset();
  ret = priv_mgr_.get_user_infos_with_tenant_id(2, user_infos_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, user_infos_array.count());
  user_infos_array.reset();
  ret = priv_mgr_.get_user_infos_with_tenant_id(1, user_infos_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < user_infos_array.count(); ++i) {
    LOG_WARN("user_info", K(*user_infos_array.at(i)));
  }
  ASSERT_EQ(4, user_infos_array.count()); //including root user in system tenant

  //get user id
  uint64_t user_id = priv_mgr_.get_user_id(1, "yz1");
  ASSERT_EQ(1, user_id);
  user_id = priv_mgr_.get_user_id(1, "testtest");
  ASSERT_EQ(OB_INVALID_ID, user_id);

  //get db priv
  const ObDBPriv *p_db_priv = priv_mgr_.get_db_priv(1, 2, "db");
  ASSERT_TRUE(NULL != p_db_priv);
  ASSERT_TRUE(1 == p_db_priv->get_tenant_id());
  ASSERT_TRUE(OB_PRIV_SELECT == p_db_priv->get_priv_set());
  p_db_priv = priv_mgr_.get_db_priv(ObOriginalDBKey(1, 2, "db"));
  ASSERT_TRUE(NULL != p_db_priv);

  //add db priv
  ret = priv_mgr_.add_db_priv(NULL, true);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ObDBPriv new_db_priv;
  new_db_priv.set_tenant_id(OB_INVALID_ID);
  ret = priv_mgr_.add_new_db_priv(new_db_priv);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //'alipay_inc' match 'alipay%', because 'alipay' is more specific than 'ali%'
  p_db_priv = priv_mgr_.get_db_priv(1, 2, "alipay_inc");
  ASSERT_TRUE(NULL != p_db_priv);
  ASSERT_TRUE(1 == p_db_priv->get_tenant_id());
  ASSERT_TRUE(OB_PRIV_CREATE == p_db_priv->get_priv_set());

  //get table priv
  const ObTablePriv *p_table_priv = priv_mgr_.get_table_priv(1, 2, "ali", "sale");
  ASSERT_TRUE(NULL != p_table_priv);
  ASSERT_TRUE(1 == p_table_priv->get_tenant_id());
  ASSERT_TRUE(2 == p_table_priv->get_user_id());
  ASSERT_TRUE(p_table_priv->get_database_name_str() == "ali");
  ASSERT_TRUE(p_table_priv->get_table_name_str() == "sale");
  ASSERT_TRUE((OB_PRIV_SELECT | OB_PRIV_INSERT) == p_table_priv->get_priv_set());
  p_table_priv = priv_mgr_.get_table_priv_from_vector(ObTablePrivSortKey(1, 2, "ali", "sale"));
  ASSERT_TRUE(NULL != p_table_priv);
  p_table_priv = priv_mgr_.get_table_priv_from_vector(ObTablePrivSortKey(1, 2, "ali", "fsdfs"));
  ASSERT_TRUE(NULL == p_table_priv);


  p_table_priv = priv_mgr_.get_table_priv(1, 1, "ali", "customer");
  ASSERT_TRUE(NULL != p_table_priv);
  ASSERT_TRUE(1 == p_table_priv->get_tenant_id());
  ASSERT_TRUE(1 == p_table_priv->get_user_id());
  ASSERT_TRUE(p_table_priv->get_database_name_str() == "ali");
  ASSERT_TRUE(p_table_priv->get_table_name_str() == "customer");
  ASSERT_TRUE((OB_PRIV_CREATE | OB_PRIV_UPDATE) == p_table_priv->get_priv_set());

  p_table_priv = priv_mgr_.get_table_priv(2, 1, "db2", "sale");
  ASSERT_TRUE(NULL != p_table_priv);
  ASSERT_TRUE(2 == p_table_priv->get_tenant_id());
  ASSERT_TRUE(1 == p_table_priv->get_user_id());
  ASSERT_TRUE(p_table_priv->get_database_name_str() == "db2");
  ASSERT_TRUE(p_table_priv->get_table_name_str() == "sale");
  ASSERT_TRUE((OB_PRIV_CREATE | OB_PRIV_UPDATE) == p_table_priv->get_priv_set());

  //-----------------check priv test------------------
  //----check user access
  ObSessionPrivInfo session_priv1;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz1", OB_DEFAULT_HOST_NAME, "", ""), session_priv1);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_EQ(OB_SUCCESS, priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz1", "192.168.1.1", "", ""), session_priv1));
  ASSERT_EQ(OB_SUCCESS, priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz1", "192.168.1.%", "", ""), session_priv1));
  ASSERT_EQ(OB_SUCCESS, priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz1", "hello", "", ""), session_priv1));
  ASSERT_EQ(OB_SUCCESS, priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz1", "__", "", ""), session_priv1));
  ASSERT_EQ(OB_SUCCESS, priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz1", "%%%%", "", ""), session_priv1));
  //check user access and db access
  ObSessionPrivInfo session_priv2;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz2", OB_DEFAULT_HOST_NAME, "", "db"), session_priv2);
  ASSERT_TRUE(OB_SUCC(ret));
  //check user access, db access and table grant in db
  //user yz2 has no privilege of database 'ali',
  //but has privilege of table 'sale' in db 'ali'
  ObSessionPrivInfo session_priv3;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz2", OB_DEFAULT_HOST_NAME, "", "ali"), session_priv3);
  ASSERT_TRUE(OB_SUCC(ret));

  ObSessionPrivInfo session_priv4;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("not_exist", "yz2", OB_DEFAULT_HOST_NAME, "", "ali"), session_priv4);
  ASSERT_EQ(OB_ERR_INVALID_TENANT_NAME, ret);

  ObSessionPrivInfo session_priv5;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "test", OB_DEFAULT_HOST_NAME, "", "ali"), session_priv5);
  ASSERT_EQ(OB_PASSWORD_WRONG, ret);

  ObSessionPrivInfo session_priv6;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz2", OB_DEFAULT_HOST_NAME, "xxx", "ali"), session_priv6);
  ASSERT_NE(OB_SUCCESS, ret);

  ObSessionPrivInfo session_priv7;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz2", OB_DEFAULT_HOST_NAME, "", "xxx"), session_priv6);
  ASSERT_NE(OB_SUCCESS, ret);

  //check user exist
  bool exist = false;
  ret = priv_mgr_.check_user_exist(1, "yz1", OB_DEFAULT_HOST_NAME, exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = priv_mgr_.check_user_exist(1, "testtest", OB_DEFAULT_HOST_NAME, user_id, exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = priv_mgr_.check_user_exist(1, "yz1", OB_DEFAULT_HOST_NAME, user_id, exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ASSERT_EQ(1, user_id);
  ret = priv_mgr_.check_user_exist(OB_INVALID_ID, "yz1", OB_DEFAULT_HOST_NAME, user_id, exist);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ret = priv_mgr_.check_user_exist(OB_INVALID_ID, 1, exist);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ret = priv_mgr_.check_user_exist(1, 1, exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = priv_mgr_.check_user_exist(1, 1111, exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);

  //----check db priv
  ObPrivSet user_db_priv_set = 0;
  ret = priv_mgr_.check_db_priv(session_priv3, "db", OB_PRIV_CREATE, user_db_priv_set);
  ASSERT_TRUE(OB_FAIL(ret));

  user_db_priv_set = 0;
  ret = priv_mgr_.check_db_priv(session_priv3, "ali", OB_PRIV_ALTER, user_db_priv_set);
  ASSERT_TRUE(OB_SUCC(ret));

  ObSessionPrivInfo priv_invalid;
  priv_invalid.tenant_id_ = OB_INVALID_ID;
  ret = priv_mgr_.check_db_priv(priv_invalid, "ali", OB_PRIV_ALTER, user_db_priv_set);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  priv_invalid.tenant_id_ = 1;
  priv_invalid.user_id_ = 1;
  priv_invalid.db_ = OB_INFORMATION_SCHEMA_NAME;
  ret = priv_mgr_.check_db_priv(priv_invalid, "ali", OB_PRIV_SELECT, user_db_priv_set);
  ASSERT_EQ(OB_ERR_NO_DB_PRIVILEGE, ret);

  ret = priv_mgr_.check_db_priv(session_priv3, "db", OB_PRIV_CREATE);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = priv_mgr_.check_db_priv(session_priv3, "ali", OB_PRIV_ALTER);
  ASSERT_EQ(OB_SUCCESS, ret);

  //check db access
  ObPrivSet db_privs;
  ObSessionPrivInfo s_priv;
  s_priv.tenant_id_ = OB_INVALID_ID;
  s_priv.user_id_ = 1;
  s_priv.user_priv_set_ = 0;
  ret = priv_mgr_.check_db_access(s_priv, "db", db_privs, true);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  s_priv.tenant_id_ = 1;
  s_priv.user_id_ = 3;
  s_priv.user_priv_set_ = OB_PRIV_CREATE;
  ret = priv_mgr_.check_db_access(s_priv, OB_INFORMATION_SCHEMA_NAME, db_privs, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  s_priv.tenant_id_ = 1;
  s_priv.user_id_ = 2;
  s_priv.user_priv_set_ = OB_PRIV_CREATE;
  ret = priv_mgr_.check_db_access(s_priv, "ali", db_privs, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  s_priv.tenant_id_ = 1;
  s_priv.user_id_ = 2;
  s_priv.user_priv_set_ = OB_PRIV_ALTER;
  ret = priv_mgr_.check_db_access(s_priv, "ali", db_privs, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  s_priv.tenant_id_ = 1;
  s_priv.user_id_ = 3;
  s_priv.user_priv_set_ = 0;
  ret = priv_mgr_.check_db_access(s_priv, "db", db_privs, true);
  ASSERT_EQ(OB_ERR_NO_DB_PRIVILEGE, ret);

  //----check table priv
  ObArray<ObNeedPriv> table_need_priv_array;
  ObNeedPriv table_need_priv;
  table_need_priv.db_ = "db";
  table_need_priv.table_ = "haha";
  table_need_priv.priv_set_ = OB_PRIV_CREATE;
  table_need_priv_array.push_back(table_need_priv);

  ret = priv_mgr_.check_table_priv(session_priv1, table_need_priv_array);
  ASSERT_EQ(OB_ERR_NO_TABLE_PRIVILEGE, ret);
  ret = priv_mgr_.check_single_table_priv(session_priv1, table_need_priv);
  ASSERT_EQ(OB_ERR_NO_TABLE_PRIVILEGE, ret);
  ObSessionPrivInfo invalid_priv_info;
  invalid_priv_info.tenant_id_ = OB_INVALID_ID;
  ret = priv_mgr_.check_single_table_priv(invalid_priv_info, table_need_priv);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = priv_mgr_.check_table_priv(invalid_priv_info, table_need_priv_array);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  table_need_priv.db_ = "db";
  table_need_priv.table_ = "sale";
  table_need_priv.priv_set_ = OB_PRIV_CREATE;
  ret = priv_mgr_.check_single_table_priv(session_priv1, table_need_priv);
  ASSERT_EQ(OB_ERR_NO_TABLE_PRIVILEGE, ret);

  //check table grant in db
  ASSERT_TRUE(priv_mgr_.table_grant_in_db(1, 2, "ali"));
  ASSERT_FALSE(priv_mgr_.table_grant_in_db(1, 3, "db"));
  ASSERT_FALSE(priv_mgr_.table_grant_in_db(OB_INVALID_ID, OB_INVALID_ID, "db"));

  //check table show
  bool allow_show = false;
  ret = priv_mgr_.check_table_show(session_priv3, "ali", "sale", allow_show);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(allow_show);
  ret = priv_mgr_.check_table_show(session_priv1, "db", "sale", allow_show);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(allow_show);
  ret = priv_mgr_.check_table_show(session_priv2, "no_exist", "table", allow_show);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(allow_show);
  ret = priv_mgr_.check_table_show(session_priv5, "db", "sale", allow_show);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(allow_show);
  s_priv.tenant_id_ = 1;
  s_priv.user_id_ = 1;
  s_priv.user_priv_set_ = OB_PRIV_SELECT;
  ret = priv_mgr_.check_db_show(s_priv, "db", allow_show);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(allow_show);
  s_priv.tenant_id_ = 1;
  s_priv.user_id_ = 3;
  s_priv.user_priv_set_ = 0;
  ret = priv_mgr_.check_db_show(s_priv, "db", allow_show);
  ASSERT_FALSE(allow_show);

  table_need_priv_array.reset();
  table_need_priv.db_ = "ali";
  table_need_priv.table_ = "customer";
  table_need_priv.priv_set_ = OB_PRIV_SELECT | OB_PRIV_UPDATE;
  table_need_priv_array.push_back(table_need_priv);

  ret = priv_mgr_.check_table_priv(session_priv1, table_need_priv_array);
  ASSERT_TRUE(OB_SUCC(ret));

  //----check_priv test
  common::ObArenaAllocator allocator;
  ObStmtNeedPrivs stmt_need_privs(allocator);
  stmt_need_privs.need_privs_.init(10);
  ObNeedPriv need_priv;
  need_priv.priv_set_ = OB_PRIV_SELECT;
  need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.priv_set_ = OB_PRIV_CREATE;
  need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_NE(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.priv_set_ = OB_PRIV_SELECT;
  need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.priv_set_ = OB_PRIV_CREATE;
  need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_NE(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.db_ = "";
  need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
  need_priv.priv_set_ = OB_PRIV_SELECT;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.db_ = "db";
  need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
  need_priv.priv_set_ = OB_PRIV_CREATE;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_NE(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.db_ = "";
  need_priv.priv_level_ = OB_PRIV_DB_ACCESS_LEVEL;
  need_priv.priv_set_ = OB_PRIV_SELECT;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_NE(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.table_ = "";
  need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
  need_priv.priv_set_ = OB_PRIV_SELECT;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_need_privs.need_privs_.clear();
  need_priv.table_ = "";
  need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
  need_priv.priv_set_ = OB_PRIV_CREATE;
  stmt_need_privs.need_privs_.push_back(need_priv);
  ret = priv_mgr_.check_priv(session_priv1, stmt_need_privs);
  ASSERT_NE(OB_SUCCESS, ret);

  ObSessionPrivInfo empty_priv;
  ret = priv_mgr_.check_priv(empty_priv, stmt_need_privs);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //----------------delete priv test------------------
  const int64_t BUKET_NUM = 128;
  //----delete tenant
  hash::ObHashSet<uint64_t> tenant_id_set;
  ret = tenant_id_set.create(BUKET_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tenant_id_set.set_refactored(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = priv_mgr_.tenant_name_map_.set_refactored("tenant", NULL, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = priv_mgr_.del_tenant(2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = priv_mgr_.del_tenants(tenant_id_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_tenant = priv_mgr_.get_tenant_info("ob");
  ASSERT_TRUE(NULL == p_tenant);
  p_tenant = priv_mgr_.get_tenant_info("yz");
  ASSERT_TRUE(NULL == p_tenant);
  ret = priv_mgr_.del_tenants(tenant_id_set);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = priv_mgr_.del_tenant(111);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = priv_mgr_.del_db_priv(ObOriginalDBKey(1, 100, "test"));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  //----delete user
  hash::ObHashSet<ObTenantUserId> user_id_set;
  ret = user_id_set.create(BUKET_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantUserId tenant_user_id;
  tenant_user_id.tenant_id_ = 1;
  tenant_user_id.user_id_ = 1;
  ret = user_id_set.set_refactored(tenant_user_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = priv_mgr_.del_users(user_id_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  //user yz1 deleted
  p_user_info = priv_mgr_.get_user_info(1, "yz1");
  ASSERT_TRUE(NULL == p_user_info);
  //user yz2 has not been deleted
  p_user_info = priv_mgr_.get_user_info(1, "yz2");
  ASSERT_TRUE(NULL != p_user_info);
  //user t2_u1 has not been deleted
  p_user_info = priv_mgr_.get_user_info(2, "t2_u1");
  ASSERT_TRUE(NULL != p_user_info);
  ret = priv_mgr_.del_users(user_id_set);
  ASSERT_NE(OB_SUCCESS, ret);

  //----delete db priv
  hash::ObHashSet<ObOriginalDBKey> db_key_set;
  ret = db_key_set.create(BUKET_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObOriginalDBKey db_key;
  db_key.tenant_id_ = 1;
  db_key.user_id_ = 2;
  db_key.db_ = "db";
  ret = db_key_set.set_refactored(db_key);
  ASSERT_EQ(OB_SUCCESS, ret);

  db_key.tenant_id_ = 1;
  db_key.user_id_ = 2;
  db_key.db_ = "ali%";
  ret = db_key_set.set_refactored(db_key);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = priv_mgr_.del_db_privs(db_key_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  //db priv with key (1, 2, db) deleted
  p_db_priv = priv_mgr_.get_db_priv(1, 2, "db");
  ASSERT_TRUE(NULL == p_db_priv);

  //db priv with key (1, 2, ali%) deleted
  p_db_priv = priv_mgr_.get_db_priv(1, 2, "ali%");
  ASSERT_TRUE(NULL == p_db_priv);

  //db priv with key (1, 2, alipay%) has not been deleted
  p_db_priv = priv_mgr_.get_db_priv(1, 2, "alipay%");
  ASSERT_TRUE(NULL != p_db_priv);
  ret = priv_mgr_.del_db_privs(db_key_set);
  ASSERT_NE(OB_SUCCESS, ret);

  //----delete table priv
  hash::ObHashSet<ObTablePrivSortKey> table_key_set;
  ret = table_key_set.create(BUKET_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablePrivSortKey table_key;
  table_key.tenant_id_ = 1;
  table_key.user_id_ = 2;
  table_key.db_ = "ali";
  table_key.table_ = "sale";
  ret = table_key_set.set_refactored(table_key);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablePrivSortKey table_key2;
  table_key2.tenant_id_ = 1;
  table_key2.user_id_ = 2;
  table_key2.db_ = "ali";
  table_key2.table_ = "table11";
  ret = priv_mgr_.table_priv_map_.set_refactored(table_key2, NULL, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = priv_mgr_.del_table_privs(table_key_set);
  ASSERT_EQ(OB_SUCCESS, ret);

  //table priv with key(1, 2, ali, sale) deleted
  p_table_priv = priv_mgr_.get_table_priv(1, 2, "ali", "sale");
  ASSERT_TRUE(NULL == p_table_priv);

  //table priv with key(2, 1, db2, sale) has not been deleted
  p_table_priv = priv_mgr_.get_table_priv(2, 1, "db2", "sale");
  ASSERT_TRUE(NULL != p_table_priv);
  ret = priv_mgr_.del_table_privs(table_key_set);
  ASSERT_NE(OB_SUCCESS, ret);

  ObPrivManager deep_copy_mgr;
  deep_copy_mgr.init();
  ASSERT_EQ(OB_SUCCESS, deep_copy_mgr.deep_copy(priv_mgr_));

  //print priv infos
  priv_mgr_.print_priv_infos();
  deep_copy_mgr.print_priv_infos();

  //get tenant id
  priv_mgr_.get_tenant_id(ObString("test"), tenant_id);
  ASSERT_EQ(OB_INVALID_ID, tenant_id);

  ret = priv_mgr_.build_table_priv_hashmap();
  ASSERT_EQ(OB_SUCCESS, ret);

}

TEST_F(TestPrivManager, test_construct_and_operator)
{
  ObMemfragRecycleAllocator global_allocator;
  ObPrivManager priv_mgr_global(global_allocator);
  ASSERT_EQ(OB_SUCCESS, priv_mgr_global.assign(priv_mgr_));
  ASSERT_EQ(priv_mgr_global.tenant_infos_.count(), priv_mgr_.tenant_infos_.count());
  ASSERT_EQ(priv_mgr_global.user_infos_.count(), priv_mgr_.user_infos_.count());
  ASSERT_EQ(priv_mgr_global.db_privs_.count(), priv_mgr_.db_privs_.count());
  ASSERT_EQ(priv_mgr_global.table_privs_.count(), priv_mgr_.table_privs_.count());
  priv_mgr_global.reset();
  ASSERT_EQ(0, priv_mgr_global.tenant_infos_.count());
  ASSERT_EQ(0, priv_mgr_global.user_infos_.count());
  ASSERT_EQ(0, priv_mgr_global.db_privs_.count());
  ASSERT_EQ(0, priv_mgr_global.table_privs_.count());
  ObMemfragRecycleAllocator &allocator = priv_mgr_global.get_allocator();
  ASSERT_EQ(&global_allocator, &allocator);
  priv_mgr_global.copy_priv_infos(priv_mgr_);
  ASSERT_EQ(priv_mgr_global.tenant_infos_.count(), priv_mgr_.tenant_infos_.count());
  ASSERT_EQ(priv_mgr_global.user_infos_.count(), priv_mgr_.user_infos_.count());
  ASSERT_EQ(priv_mgr_global.db_privs_.count(), priv_mgr_.db_privs_.count());
  ASSERT_EQ(priv_mgr_global.table_privs_.count(), priv_mgr_.table_privs_.count());
}

TEST_F(TestPrivManager, test_tenant_locked)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantSchema> tenant_info_array;
  ObTenantSchema tenant;
  ObArray<ObString> zone_list;
  zone_list.push_back("127.0.0.1");
  FILL_TENANT_INFO(tenant, 1, "ob", 2, zone_list, "127.0.0.1", 0, "");
  tenant_info_array.push_back(tenant);
  FILL_TENANT_INFO(tenant, 2, "ob2", 2, zone_list, "127.0.0.1", 1, "");
  tenant_info_array.push_back(tenant);
  ret = priv_mgr_.add_new_tenant_info_array(tenant_info_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  uint64_t tenant_id = OB_INVALID_ID;
  priv_mgr_.get_tenant_id("ob2", tenant_id);
  ASSERT_EQ(OB_INVALID_ID, tenant_id);

  //----add user priv
  ObArray<ObUserInfo> user_info_array;
  ObUserInfo user;
  //user: tenant_id, user_id, user_name, passwd, info, is_locked, priv_set
  FILL_USER_INFO(user, 1, 1, "yz1", "", "student in class one", false, OB_PRIV_SELECT);
  user_info_array.push_back(user);

  user.reset();
  FILL_USER_INFO(user, 1, 2, "yz2", "", "student in class one", true, 0);
  user_info_array.push_back(user);

  user.reset();
  FILL_USER_INFO(user, 1, 3, "user", "passwd", "test", false, 0);
  user_info_array.push_back(user);

  user.reset();
  FILL_USER_INFO(user, 2, 1, "t2_u1", "", "", false, 0);
  user_info_array.push_back(user);

  ret = priv_mgr_.add_new_user_info_array(user_info_array);
  ASSERT_TRUE(OB_SUCC(ret));

  ObSessionPrivInfo session_priv;
  ret = priv_mgr_.check_user_access(ObUserLoginInfo("ob", "yz2", OB_DEFAULT_HOST_NAME, "", "ali"), session_priv);
  ASSERT_EQ(OB_ERR_USER_IS_LOCKED, ret);
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
