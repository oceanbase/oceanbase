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
#include "share/schema/ob_priv_mgr.h"
#include "ob_schema_test_utils.cpp"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
class TestPrivMgr : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  ObPrivMgr priv_mgr_;
};

void TestPrivMgr::SetUp()
{
  //obsys::ObSysConfig c1;
  //priv_mgr_.init();
}

#define DB_PRIV_EQUAL(db_priv, db_priv_other) \
  ASSERT_EQ(db_priv.get_tenant_id(), db_priv_other.get_tenant_id()); \
  ASSERT_EQ(db_priv.get_user_id(), db_priv_other.get_user_id()); \
  ASSERT_EQ(db_priv.get_database_name_str(), db_priv_other.get_database_name_str()); \
  ASSERT_EQ(db_priv.get_priv_set(), db_priv_other.get_priv_set());

#define TABLE_PRIV_EQUAL(table_priv, table_priv_other) \
  ASSERT_EQ(table_priv.get_tenant_id(), table_priv_other.get_tenant_id()); \
  ASSERT_EQ(table_priv.get_user_id(), table_priv_other.get_user_id()); \
  ASSERT_EQ(table_priv.get_database_name_str(), table_priv_other.get_database_name_str()); \
  ASSERT_EQ(table_priv.get_table_name_str(), table_priv_other.get_table_name_str()); \
  ASSERT_EQ(table_priv.get_priv_set(), table_priv_other.get_priv_set());


TEST_F(TestPrivMgr, privs_test)
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

  ret = priv_mgr_.add_db_privs(db_priv_array);
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

  ret = priv_mgr_.add_table_privs(table_priv_array);
  ASSERT_TRUE(OB_SUCC(ret));

  ObArray<const ObDBPriv *>db_priv_pointers;
  ret = priv_mgr_.get_db_privs_in_tenant(2, db_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, db_priv_pointers.count());
  db_priv_pointers.reset();
  ret = priv_mgr_.get_db_privs_in_user(1, 2, db_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, db_priv_pointers.count());

  ObArray<const ObTablePriv *>table_priv_pointers;
  ret = priv_mgr_.get_table_privs_in_user(OB_INVALID, OB_INVALID_ID, table_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, table_priv_pointers.count());
  ret = priv_mgr_.get_table_privs_in_user(1, 2, table_priv_pointers);
  ASSERT_EQ(1, table_priv_pointers.count());
  table_priv_pointers.reset();
  ret = priv_mgr_.get_table_privs_in_tenant(OB_INVALID_ID, table_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  table_priv_pointers.reset();
  ret = priv_mgr_.get_table_privs_in_tenant(1, table_priv_pointers);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, table_priv_pointers.count());


  //-----------------get priv test--------------------
  //get db priv
  const ObDBPriv *p_db_priv = NULL;
  ret  = priv_mgr_.get_db_priv(ObOriginalDBKey(1, 2, "db"), p_db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_db_priv);
  ASSERT_TRUE(1 == p_db_priv->get_tenant_id());
  ASSERT_TRUE(OB_PRIV_SELECT == p_db_priv->get_priv_set());

  //'alipay_inc' match 'alipay%', because 'alipay' is more specific than 'ali%'
  ret = priv_mgr_.get_db_priv(ObOriginalDBKey(1, 2, "alipay_inc"), p_db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_db_priv);
  ASSERT_TRUE(1 == p_db_priv->get_tenant_id());
  ASSERT_TRUE(OB_PRIV_CREATE == p_db_priv->get_priv_set());

  //get table priv
  const ObTablePriv *p_table_priv = NULL;
  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(1, 2, "ali", "sale"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_table_priv);
  ASSERT_TRUE(1 == p_table_priv->get_tenant_id());
  ASSERT_TRUE(2 == p_table_priv->get_user_id());
  ASSERT_TRUE(p_table_priv->get_database_name_str() == "ali");
  ASSERT_TRUE(p_table_priv->get_table_name_str() == "sale");
  ASSERT_TRUE((OB_PRIV_SELECT | OB_PRIV_INSERT) == p_table_priv->get_priv_set());
  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(1, 2, "ali", "fsdfs"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL == p_table_priv);


  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(1, 1, "ali", "customer"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_table_priv);
  ASSERT_TRUE(1 == p_table_priv->get_tenant_id());
  ASSERT_TRUE(1 == p_table_priv->get_user_id());
  ASSERT_TRUE(p_table_priv->get_database_name_str() == "ali");
  ASSERT_TRUE(p_table_priv->get_table_name_str() == "customer");
  ASSERT_TRUE((OB_PRIV_CREATE | OB_PRIV_UPDATE) == p_table_priv->get_priv_set());

  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(2, 1, "db2", "sale"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_table_priv);
  ASSERT_TRUE(2 == p_table_priv->get_tenant_id());
  ASSERT_TRUE(1 == p_table_priv->get_user_id());
  ASSERT_TRUE(p_table_priv->get_database_name_str() == "db2");
  ASSERT_TRUE(p_table_priv->get_table_name_str() == "sale");
  ASSERT_TRUE((OB_PRIV_CREATE | OB_PRIV_UPDATE) == p_table_priv->get_priv_set());

  //----------------delete priv test------------------
  //----delete db priv
  ObArray<ObOriginalDBKey> db_keys;

  ObOriginalDBKey db_key;
  db_key.tenant_id_ = 1;
  db_key.user_id_ = 2;
  db_key.db_ = "db";
  db_keys.push_back(db_key);

  db_key.tenant_id_ = 1;
  db_key.user_id_ = 2;
  db_key.db_ = "ali%";
  db_keys.push_back(db_key);

  ret = priv_mgr_.del_db_privs(db_keys);
  ASSERT_EQ(OB_SUCCESS, ret);
  //db priv with key (1, 2, db) deleted
  ret = priv_mgr_.get_db_priv(ObOriginalDBKey(1, 2, "db"), p_db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL == p_db_priv);

  //db priv with key (1, 2, ali%) deleted
  ret = priv_mgr_.get_db_priv(ObOriginalDBKey(1, 2, "ali%"), p_db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL == p_db_priv);

  //db priv with key (1, 2, alipay%) has not been deleted
  ret = priv_mgr_.get_db_priv(ObOriginalDBKey(1, 2, "alipay%"), p_db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_db_priv);
  ret = priv_mgr_.del_db_privs(db_keys);
  ASSERT_NE(OB_SUCCESS, ret);

  //----delete table priv
  ObArray<ObTablePrivSortKey> table_keys;

  ObTablePrivSortKey table_key;
  table_key.tenant_id_ = 1;
  table_key.user_id_ = 2;
  table_key.db_ = "ali";
  table_key.table_ = "sale";
  table_keys.push_back(table_key);

  ret = priv_mgr_.del_table_privs(table_keys);
  ASSERT_EQ(OB_SUCCESS, ret);

  //table priv with key(1, 2, ali, sale) deleted
  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(1, 2, "ali", "sale"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL == p_table_priv);

  //table priv with key(2, 1, db2, sale) has not been deleted
  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(2, 1, "db2", "sale"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_table_priv);
  ret = priv_mgr_.del_table_priv(ObTablePrivSortKey(2, 1, "db2", "sale"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = priv_mgr_.get_table_priv(ObTablePrivSortKey(2, 1, "db2", "sale"), p_table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL == p_table_priv);
  ret = priv_mgr_.del_table_priv(ObTablePrivSortKey(2, 1, "db2", "sale"));
  ASSERT_NE(OB_SUCCESS, ret);

  // dump
  priv_mgr_.dump();

}

TEST_F(TestPrivMgr, reset_and_assign)
{
  int ret = OB_SUCCESS;
  ObPrivMgr priv_mgr;
  // db priv
  ObDBPriv db_priv;
  ObArray<ObDBPriv> db_privs;
  FILL_DB_PRIV(db_priv, 1, 2, "nijia", OB_PRIV_ALTER);
  db_privs.push_back(db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  FILL_DB_PRIV(db_priv, 1, 3, "test", OB_PRIV_ALTER);
  db_privs.push_back(db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  FILL_DB_PRIV(db_priv, 1, 3, "haha", OB_PRIV_ALTER);
  db_privs.push_back(db_priv);
  ret = priv_mgr.add_db_privs(db_privs);
  ASSERT_EQ(OB_SUCCESS, ret);
  // table priv
  ObTablePriv table_priv;
  ObArray<ObTablePriv> table_privs;
  FILL_TABLE_PRIV(table_priv, 1, 2, "nijia", "t1", OB_PRIV_ALTER);
  table_privs.push_back(table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  FILL_TABLE_PRIV(table_priv, 1, 3, "test", "t2", OB_PRIV_ALTER);
  table_privs.push_back(table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  FILL_TABLE_PRIV(table_priv, 1, 3, "haha", "t3", OB_PRIV_ALTER);
  table_privs.push_back(table_priv);
  ret = priv_mgr.add_table_privs(table_privs);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPrivMgr new_priv_mgr;
  ret = new_priv_mgr.assign(priv_mgr);
  for (int64_t i = 0 ; i < db_privs.count(); ++i) {
    const ObDBPriv &cur_db_priv = db_privs.at(i);
    const ObDBPriv *p_db_priv = NULL;
    ret = new_priv_mgr.get_db_priv(cur_db_priv.get_original_key(), p_db_priv);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL != p_db_priv);
    DB_PRIV_EQUAL(cur_db_priv, (*p_db_priv));
  }
  for (int64_t i = 0 ; i < table_privs.count(); ++i) {
    const ObTablePriv &cur_table_priv = table_privs.at(i);
    const ObTablePriv *p_table_priv = NULL;
    ret = new_priv_mgr.get_table_priv(cur_table_priv.get_sort_key(), p_table_priv);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL != p_table_priv);
    TABLE_PRIV_EQUAL(cur_table_priv, (*p_table_priv));
  }
  new_priv_mgr.reset();
  for (int64_t i = 0 ; i < db_privs.count(); ++i) {
    const ObDBPriv &cur_db_priv = db_privs.at(i);
    const ObDBPriv *p_db_priv = NULL;
    ret = new_priv_mgr.get_db_priv(cur_db_priv.get_original_key(), p_db_priv);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL == p_db_priv);
  }
  for (int64_t i = 0 ; i < table_privs.count(); ++i) {
    const ObTablePriv &cur_table_priv = table_privs.at(i);
    const ObTablePriv *p_table_priv = NULL;
    ret = new_priv_mgr.get_table_priv(cur_table_priv.get_sort_key(), p_table_priv);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL == p_table_priv);
  }
}

TEST_F(TestPrivMgr, external_allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator global_allocator;
  ObPrivMgr priv_mgr(global_allocator);
  ObDBPriv db_priv;
  FILL_DB_PRIV(db_priv, 1, 2, "nijia", OB_PRIV_ALTER);
  ret = priv_mgr.add_db_priv(db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObPrivMgr new_priv_mgr;
  ret = new_priv_mgr.assign(priv_mgr);
  const ObDBPriv *p_db_priv = NULL;
  ret = new_priv_mgr.get_db_priv(db_priv.get_original_key(), p_db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != p_db_priv);
  DB_PRIV_EQUAL(db_priv, (*p_db_priv));
}

TEST_F(TestPrivMgr, get_priv_set)
{
  int ret = OB_SUCCESS;
  ObPrivMgr priv_mgr;
  // db priv
  ObDBPriv db_priv;
  FILL_DB_PRIV(db_priv, 1, 2, "nijia", OB_PRIV_ALTER);
  ret = priv_mgr.add_db_priv(db_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObPrivSet priv_set;
  ret = priv_mgr.get_db_priv_set(db_priv.get_original_key(), priv_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_PRIV_ALTER, priv_set);
  // table_priv
  ObTablePriv table_priv;
  FILL_TABLE_PRIV(table_priv, 1, 2, "nijia", "t1", OB_PRIV_ALTER);
  ret = priv_mgr.add_table_priv(table_priv);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = priv_mgr.get_table_priv_set(table_priv.get_sort_key(), priv_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_PRIV_ALTER, priv_set);
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
