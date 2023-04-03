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
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/utility/utility.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_schema_test_utils.cpp"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

TEST(ObPrivStructTest, ob_tenant_user_id)
{
  ObTenantUserId tenant_user_id(2, 1);
  ObTenantUserId tenant_user_id_eq(2, 1);
  ObTenantUserId tenant_user_id_big(2, 2);

  ASSERT_TRUE(tenant_user_id == tenant_user_id_eq);
  ASSERT_TRUE(tenant_user_id.hash() == tenant_user_id_eq.hash());
  ASSERT_FALSE(tenant_user_id == tenant_user_id_big);
  ASSERT_TRUE(tenant_user_id < tenant_user_id_big);

  const int64_t buf_len = 256;
  char buffer[buf_len];
  int64_t pos = 0;
  tenant_user_id.serialize(buffer, buf_len, pos);
  ObTenantUserId tenant_user_id_des;
  pos = 0;
  tenant_user_id_des.deserialize(buffer, buf_len, pos);
  ASSERT_TRUE(tenant_user_id == tenant_user_id_des);
}

TEST(ObPrivStructTest, ob_priv)
{
  ObPrivSet priv_set = OB_PRIV_CREATE | OB_PRIV_SELECT;
  ObPrivSet priv_set_other = OB_PRIV_CREATE | OB_PRIV_ALTER;

  const int64_t schema_version = INT64_MAX;
  ObPriv ob_priv(2, 1, schema_version, priv_set);
  ObPriv ob_priv_eq(2, 1, schema_version, priv_set);
  ObPriv ob_priv_big(2, 2, schema_version, priv_set_other);

  ASSERT_EQ(priv_set, ob_priv.get_priv_set());
  ASSERT_EQ(priv_set_other, ob_priv_big.get_priv_set());
  //cmp_tenant_user_id lhs->get_tenant_user_id < tenant_user_id
  ASSERT_TRUE(ObPriv::cmp_tenant_user_id(&ob_priv, ob_priv_big.get_tenant_user_id()));
  ASSERT_FALSE(ObPriv::cmp_tenant_user_id(&ob_priv, ob_priv_eq.get_tenant_user_id()));
  ASSERT_FALSE(ObPriv::cmp_tenant_user_id(&ob_priv_big, ObTenantUserId(2, 2)));
  ASSERT_TRUE(ObPriv::equal_tenant_user_id(&ob_priv, ob_priv_eq.get_tenant_user_id()));

  ob_priv.set_tenant_id(3);
  ASSERT_EQ(3UL, ob_priv.get_tenant_id());
  ASSERT_TRUE(ObPriv::cmp_tenant_id(&ob_priv_eq, ob_priv.get_tenant_id()));
  ob_priv.set_user_id(4);
  ASSERT_EQ(4UL, ob_priv.get_user_id());

  ob_priv.set_priv_set(priv_set_other);
  ASSERT_EQ(priv_set_other, ob_priv.get_priv_set());

  const int64_t buf_len = 256;
  char buffer[buf_len];
  int64_t pos = 0;
  ob_priv.serialize(buffer, buf_len, pos);
  pos = 0;
  ObPriv ob_priv_des;
  ob_priv_des.deserialize(buffer, buf_len, pos);
  ASSERT_EQ(ob_priv.get_tenant_id(), ob_priv_des.get_tenant_id());
  ASSERT_EQ(ob_priv.get_user_id(), ob_priv_des.get_user_id());
  ASSERT_EQ(ob_priv.get_priv_set(), ob_priv_des.get_priv_set());
}

TEST(ObPrivStructTest, ob_user_info_hash_wrapper)
{
  ObUserInfoHashWrapper user_info_hash(2, ObString::make_string("user"));
  ObUserInfoHashWrapper user_info_hash_eq(2, ObString::make_string("user"));
  ObUserInfoHashWrapper user_info_hash_diff(2, ObString::make_string("other"));

  ASSERT_EQ(2UL, user_info_hash.get_tenant_id());
  ASSERT_TRUE(ObString::make_string("user") == user_info_hash.get_user_name());
  ASSERT_TRUE(user_info_hash == user_info_hash_eq);
  ASSERT_TRUE(user_info_hash.hash() == user_info_hash_eq.hash());
  ASSERT_FALSE(user_info_hash == user_info_hash_diff);
  ASSERT_FALSE(user_info_hash.hash() == user_info_hash_diff.hash());
}

TEST(ObPrivStructTest, ob_user_info)
{
  ObUserInfo user_info;
  ObUserInfo user_info_eq;
  ObUserInfo user_info_other;
  ObPrivSet priv_set = OB_PRIV_CREATE | OB_PRIV_SELECT;
  FILL_USER_INFO(user_info, 2, 2, "user", "123", "", false, priv_set);
  FILL_USER_INFO(user_info_eq, 2, 2, "user", "123", "", false, priv_set);
  FILL_USER_INFO(user_info_other, 2, 3, "LiLei", "123", "", false, priv_set);

  ASSERT_TRUE(ObUserInfo::cmp(&user_info, &user_info_other));
  ASSERT_FALSE(ObUserInfo::cmp(&user_info, &user_info_eq));
  ASSERT_TRUE(ObUserInfo::equal(&user_info, &user_info_eq));
  ASSERT_FALSE(ObUserInfo::equal(&user_info, &user_info_other));

  const int64_t buf_len = 256;
  char buffer[buf_len];
  int64_t pos = 0;
  user_info.serialize(buffer, buf_len, pos);
  pos = 0;
  ObUserInfo user_info_des;
  user_info_des.deserialize(buffer, buf_len, pos);
  ASSERT_TRUE(ObUserInfo::equal(&user_info, &user_info_des));
  ASSERT_TRUE(ObString::make_string("user") == user_info_des.get_user_name_str());
}

TEST(ObPrivStructTest, ob_db_priv_sort_key)
{
  ObDBPrivSortKey db_priv_key(2, 2, 3);
  ObDBPrivSortKey db_priv_key_eq(2, 2, 3);
  ObDBPrivSortKey db_priv_key_big(2, 3, 3);
  ObDBPrivSortKey db_priv_key_little(2, 2, 100);

  ASSERT_TRUE(db_priv_key == db_priv_key_eq);
  ASSERT_FALSE(db_priv_key == db_priv_key_little);
  ASSERT_TRUE(db_priv_key != db_priv_key_little);
  ASSERT_FALSE(db_priv_key != db_priv_key_eq);

  //sort value from big to small
  ASSERT_TRUE(db_priv_key_little < db_priv_key);
  ASSERT_TRUE(db_priv_key < db_priv_key_big);
}

TEST(ObPrivStructTest, ob_org_db_key)
{
  ObOriginalDBKey org_key(2, 2, ObString::make_string("test"));
  ObOriginalDBKey org_key_eq(2, 2, ObString::make_string("test"));
  ObOriginalDBKey org_key_diff1(2, 3, ObString::make_string("test"));
  ObOriginalDBKey org_key_diff2(2, 2, ObString::make_string("db"));

  ASSERT_EQ(org_key.hash(), org_key_eq.hash());
  ASSERT_NE(org_key.hash(), org_key_diff1.hash());
  ASSERT_NE(org_key.hash(), org_key_diff2.hash());
  //operator== cmp tenant_id, user_id, db_name
  ASSERT_TRUE(org_key == org_key_eq);
  ASSERT_FALSE(org_key == org_key_diff1);
  ASSERT_FALSE(org_key == org_key_diff2);

  //operator< cmp tenant_id, user_id
  ASSERT_TRUE(org_key < org_key_diff1);
}

TEST(ObPrivStructTest, ob_db_priv)
{
  ObDBPriv db_priv;
  ObDBPriv db_priv_eq;
  ObDBPriv db_priv_diff1;
  ObDBPriv db_priv_diff2;
  ObPrivSet priv_set = OB_PRIV_CREATE | OB_PRIV_SELECT;
  FILL_DB_PRIV(db_priv, 2, 2, ObString::make_string("test"), priv_set);
  FILL_DB_PRIV(db_priv_eq, 2, 2, ObString::make_string("db"), priv_set);
  FILL_DB_PRIV(db_priv_diff1, 2, 2, ObString::make_string("db%"), priv_set);
  FILL_DB_PRIV(db_priv_diff2, 2, 3, ObString::make_string("db"), priv_set);

  //db% is less specific;
  ASSERT_TRUE(ObDBPriv::cmp(&db_priv, &db_priv_diff1));
  ASSERT_TRUE(ObDBPriv::cmp(&db_priv, &db_priv_diff2));
  ASSERT_TRUE(ObDBPriv::equal(&db_priv, &db_priv_eq));

  const int64_t buf_len = 256;
  char buffer[buf_len];
  int64_t pos = 0;
  db_priv.serialize(buffer, buf_len, pos);
  pos = 0;
  ObDBPriv db_priv_des;
  db_priv_des.deserialize(buffer, buf_len, pos);

  ASSERT_EQ(db_priv.get_tenant_id(), db_priv_des.get_tenant_id());
  ASSERT_EQ(db_priv.get_user_id(), db_priv_des.get_user_id());
  ASSERT_TRUE(db_priv.get_database_name_str() == db_priv_des.get_database_name_str());
  ASSERT_EQ(db_priv.get_priv_set(), db_priv_des.get_priv_set());
}

TEST(ObPrivStructTest, ob_table_priv_db_key)
{
  ObTablePrivDBKey table_db_key(2, 2, ObString::make_string("test"));
  ObTablePrivDBKey table_db_key_eq(2, 2, ObString::make_string("test"));
  ObTablePrivDBKey table_db_key_diff(2, 2, ObString::make_string("db"));

  ASSERT_TRUE(table_db_key == table_db_key_eq);
  ASSERT_TRUE(table_db_key_diff < table_db_key);

}

TEST(ObPrivStructTest, ob_table_priv_sort_key)
{
  ObTablePrivSortKey table_priv_key(2, 2, "test", "table");
  ObTablePrivSortKey table_priv_key_eq(2, 2, "test", "table");
  ObTablePrivSortKey table_priv_key_diff1(2, 2, "test", "index");
  ObTablePrivSortKey table_priv_key_diff2(2, 3, "test", "table");

  ASSERT_TRUE(table_priv_key == table_priv_key_eq);
  ASSERT_TRUE(table_priv_key_diff1 < table_priv_key);
  ASSERT_TRUE(table_priv_key < table_priv_key_diff2);

  ASSERT_EQ(table_priv_key.hash(), table_priv_key_eq.hash());
  ASSERT_NE(table_priv_key.hash(), table_priv_key_diff1.hash());
}

TEST(ObPrivStructTest, ob_table_priv)
{
  ObTablePriv table_priv;
  ObTablePriv table_priv_eq;
  ObTablePriv table_priv_diff1;
  ObTablePriv table_priv_diff2;
  ObPrivSet priv_set = OB_PRIV_INSERT | OB_PRIV_SELECT;
  FILL_TABLE_PRIV(table_priv, 2, 2, "test", "table", priv_set);
  FILL_TABLE_PRIV(table_priv_eq, 2, 2, "test", "table", priv_set);
  FILL_TABLE_PRIV(table_priv_diff1, 2, 2, "test", "index", priv_set);
  FILL_TABLE_PRIV(table_priv_diff2, 2, 3, "test", "table", priv_set);

  ASSERT_TRUE(ObTablePriv::equal(&table_priv, &table_priv_eq));
  ASSERT_TRUE(ObTablePriv::cmp(&table_priv_diff1, &table_priv));
  ASSERT_TRUE(ObTablePriv::cmp(&table_priv, &table_priv_diff2));

  const int64_t buf_len = 256;
  char buffer[buf_len];
  int64_t pos = 0;
  table_priv.serialize(buffer, buf_len, pos);
  pos = 0;
  ObTablePriv table_priv_des;
  table_priv_des.deserialize(buffer, buf_len, pos);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("ERROR");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
