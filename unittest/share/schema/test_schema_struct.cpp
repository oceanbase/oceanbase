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
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {
static const int64_t BUF_SIZE = 1024 * 10;

#if 0
//-----test for func-----//
TEST(ObSchemaStructTest, ob_view_check_option_str_test)
{
  ASSERT_EQ(0, memcmp("none", ob_view_check_option_str(VIEW_CHECK_OPTION_NONE), strlen("none")));
  ASSERT_EQ(0, memcmp("local", ob_view_check_option_str(VIEW_CHECK_OPTION_LOCAL),
                      strlen("local")));
  ASSERT_EQ(0, memcmp("cascade", ob_view_check_option_str(VIEW_CHECK_OPTION_CASCADED),
                      strlen("cascade")));
  ASSERT_EQ(0, memcmp("invalid", ob_view_check_option_str(VIEW_CHECK_OPTION_MAX),
                      strlen("invalid")));
}

TEST(ObSchemaStructTest, ob_index_status_str_test)
{
  ASSERT_EQ(0, memcmp("not_found", ob_index_status_str(INDEX_STATUS_NOT_FOUND),
                      strlen("not_found")));
  ASSERT_EQ(0, memcmp("unavailable", ob_index_status_str(INDEX_STATUS_UNAVAILABLE),
                      strlen("unavailable")));
  ASSERT_EQ(0, memcmp("available", ob_index_status_str(INDEX_STATUS_AVAILABLE),
                      strlen("available")));
  ASSERT_EQ(0, memcmp("unique_checking", ob_index_status_str(INDEX_STATUS_UNIQUE_CHECKING),
                      strlen("unique_checking")));
  ASSERT_EQ(0, memcmp("unique_inelegible", ob_index_status_str(INDEX_STATUS_UNIQUE_INELIGIBLE),
                      strlen("unique_inelegible")));
  ASSERT_EQ(0, memcmp("index_error", ob_index_status_str(INDEX_STATUS_INDEX_ERROR),
                      strlen("index_error")));
  ASSERT_EQ(0, memcmp("invalid", ob_index_status_str(INDEX_STATUS_MAX), strlen("invalid")));
}
//--------test for ObTenantTableId-------//
TEST(ObSchemaStructTest, ob_tenant_table_id_test)
{
  ObTenantTableId a;
  ObTenantTableId b(1, 2);
  ObTenantTableId c(1, 3);
  ObTenantTableId d(2, 1);
  ObTenantTableId e(3, 1);
  ObTenantTableId f = e;
  ASSERT_FALSE(a.is_valid());
  ASSERT_TRUE(b.is_valid());
  a.reset();
  a.tenant_id_ = 1;
  a.table_id_ = 2;
  ASSERT_TRUE(a == b);
  ASSERT_FALSE(c == b);
  ASSERT_NE(c.hash(), b.hash());
  ASSERT_EQ(d.hash(), e.hash());
  ASSERT_TRUE(b < c);
  ASSERT_TRUE(b < d);
  ASSERT_TRUE(e == f);
}
//--------test for ObTenantDatabaseId-------//
TEST(ObSchemaStructTest, ob_tenant_database_id_test)
{
  ObTenantDatabaseId a;
  ObTenantDatabaseId b(1, 2);
  ObTenantDatabaseId c(1, 3);
  ObTenantDatabaseId d(2, 1);
  ObTenantDatabaseId e(3, 1);
  ObTenantDatabaseId f(3, 1);
  ASSERT_FALSE(a.is_valid());
  ASSERT_TRUE(b.is_valid());
  a.reset();
  a.tenant_id_ = 1;
  a.database_id_ = 2;
  ASSERT_TRUE(a == b);
  ASSERT_FALSE(c == b);
  ASSERT_TRUE(e == f);
  ASSERT_NE(c.hash(), b.hash());
  ASSERT_EQ(d.hash(), e.hash());
  ASSERT_TRUE(b < c);
  ASSERT_TRUE(b < d);
}
//--------test for ObTenantTablegroupId-------//
TEST(ObSchemaStructTest, ob_tenant_tablegroup_id_test)
{
  ObTenantTablegroupId a;
  ObTenantTablegroupId b(1, 2);
  ObTenantTablegroupId c(1, 3);
  ObTenantTablegroupId d(2, 1);
  ObTenantTablegroupId e(3, 1);
  ObTenantTablegroupId f(3, 1);
  ASSERT_FALSE(a.is_valid());
  ASSERT_TRUE(b.is_valid());
  a.reset();
  a.tenant_id_ = 1;
  a.tablegroup_id_ = 2;
  ASSERT_TRUE(a == b);
  ASSERT_FALSE(c == b);
  ASSERT_TRUE(e == f);
  ASSERT_NE(c.hash(), b.hash());
  ASSERT_EQ(d.hash(), e.hash());
  ASSERT_TRUE(b < c);
  ASSERT_TRUE(b < d);
}
//------------test for ObTenantResource-------------//
TEST(ObSchemaStructTest, ob_tenant_resource)
{
  ObTenantResource tenant_resource;
  ASSERT_FALSE(tenant_resource.is_valid());
  tenant_resource.tenant_id_ = 1;
  ASSERT_TRUE(tenant_resource.is_valid());
  tenant_resource.cpu_reserved_ = -1;
  ASSERT_FALSE(tenant_resource.is_valid());
  tenant_resource.cpu_reserved_ = 200;
  tenant_resource.cpu_max_ = 100;
  ASSERT_FALSE(tenant_resource.is_valid());
  tenant_resource.cpu_reserved_ = 100;
  tenant_resource.cpu_max_ = 100;
  ASSERT_TRUE(tenant_resource.is_valid());
  tenant_resource.cpu_reserved_ = 80;
  tenant_resource.cpu_max_ = 100;
  ASSERT_TRUE(tenant_resource.is_valid());
  tenant_resource.reset();
  ASSERT_FALSE(tenant_resource.is_valid());
}
//------------test for ObUser----------//
TEST(ObSchemaStructTest, ob_user)
{
  ObUser user;
  ASSERT_FALSE(user.is_valid());
  user.tenant_id_ = 1;
  ASSERT_FALSE(user.is_valid());
  user.user_id_ = 1;
  ASSERT_TRUE(user.is_valid());
  user.reset();
  ASSERT_TRUE(user.priv_all_);
  ASSERT_TRUE(user.priv_alter_);
  ASSERT_TRUE(user.priv_create_);
  ASSERT_TRUE(user.priv_create_user_);
  ASSERT_TRUE(user.priv_delete_);
  ASSERT_TRUE(user.priv_drop_);
  ASSERT_TRUE(user.priv_grant_option_);
  ASSERT_TRUE(user.priv_insert_);
  ASSERT_TRUE(user.priv_update_);
  ASSERT_TRUE(user.priv_select_);
  ASSERT_TRUE(user.priv_replace_);
  ASSERT_TRUE(user.is_locked_);
}
//------------test for ObDataBasePrivilege-----------//
TEST(ObSchemaStructTest, ob_database_privilege)
{
  ObDatabasePrivilege database_priv;
  ASSERT_FALSE(database_priv.is_valid());
  database_priv.tenant_id_ = 1;
  ASSERT_FALSE(database_priv.is_valid());
  database_priv.user_id_ = 1;
  ASSERT_FALSE(database_priv.is_valid());
  database_priv.database_id_ = 1;
  ASSERT_TRUE(database_priv.is_valid());
  database_priv.reset();
  ASSERT_TRUE(database_priv.priv_all_);
  ASSERT_TRUE(database_priv.priv_alter_);
  ASSERT_TRUE(database_priv.priv_create_);
  ASSERT_TRUE(database_priv.priv_delete_);
  ASSERT_TRUE(database_priv.priv_drop_);
  ASSERT_TRUE(database_priv.priv_grant_option_);
  ASSERT_TRUE(database_priv.priv_insert_);
  ASSERT_TRUE(database_priv.priv_update_);
  ASSERT_TRUE(database_priv.priv_select_);
  ASSERT_TRUE(database_priv.priv_replace_);
}
//------------test for ObSysParam-------------//
TEST(ObSchemaStructTest, ob_sys_param)
{
  ObSysParam sys_param;
  int ret = OB_SUCCESS;
  char long_name[OB_MAX_SYS_PARAM_NAME_LENGTH + 1];
  char long_value[OB_MAX_SYS_PARAM_VALUE_LENGTH + 1];
  char long_info[OB_MAX_SYS_PARAM_INFO_LENGTH + 1];
  ASSERT_FALSE(sys_param.is_valid());
  ret = sys_param.init(1, ObZone("test"), "short_name", 1, "short_value", "", "", "short_info", 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  memset(long_name, '1', sizeof(long_name));
  long_name[OB_MAX_SYS_PARAM_NAME_LENGTH] = '\0';
  memset(long_value, '1', sizeof(long_value));
  long_value[OB_MAX_SYS_PARAM_VALUE_LENGTH] = '\0';
  memset(long_info, '1', sizeof(long_info));
  long_info[OB_MAX_SYS_PARAM_INFO_LENGTH] = '\0';
  ret = sys_param.init(1, ObZone("test"), long_name, 1, "short_value", "", "", "short_info", 0);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  ret = sys_param.init(1, ObZone("test"), "short_name", 1, long_value, "", "", "short_info", 0);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  ret = sys_param.init(1, ObZone("test"), "short_name", 1, "short_value", "", "", long_info, 0);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  ret = sys_param.init(1, ObZone("test"), long_name, 1, long_value, "", "", long_info, 0);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}
//------------test for ObSchema--------//
void test_ob_schema_string_array2str(void)
{
  int ret = OB_SUCCESS;
  ObSchema schema;
  common::ObArray<common::ObString> string_array;
  char str[BUF_SIZE] = "\0";
  ret = schema.string_array2str(string_array, str, BUF_SIZE);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_STREQ("", str);

  string_array.push_back(ObString::make_string("test1"));
  ret = schema.string_array2str(string_array, str, BUF_SIZE);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_STREQ("test1", str);

  string_array.push_back(ObString::make_string("test2"));
  string_array.push_back(ObString::make_string("test3"));
  ret = schema.string_array2str(string_array, str, BUF_SIZE);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_STREQ("test1;test2;test3", str);

  ret = schema.string_array2str(string_array, str, 10);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
}
void test_ob_schema_str2_string_array(void)
{
  int ret = OB_SUCCESS;
  char str[BUF_SIZE] = "\0";
  ObSchema schema;
  common::ObArray<common::ObString> string_array;
  ret = schema.str2string_array(NULL, string_array);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(0, string_array.count());

  ret = schema.str2string_array(str, string_array);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(0, string_array.count());

  string_array.reset();
  memset(str, 0, sizeof(str) / sizeof(char));
  memcpy(str, "test", strlen("test"));
  ret = schema.str2string_array(str, string_array);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(1, string_array.count());
  ASSERT_STREQ("test", string_array.at(0).ptr());

  string_array.reset();
  memset(str, 0, sizeof(str) / sizeof(char));
  memcpy(str, "test1;", strlen("test1;"));
  ret = schema.str2string_array(str, string_array);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(1, string_array.count());
  ASSERT_STREQ("test1", string_array.at(0).ptr());

  string_array.reset();
  memset(str, 0, sizeof(str) / sizeof(char));
  memcpy(str, "test1;test2", strlen("test1;test2"));
  ret = schema.str2string_array(str, string_array);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(2, string_array.count());
  ASSERT_STREQ("test1", string_array.at(0).ptr());
  ASSERT_STREQ("test2", string_array.at(1).ptr());

  string_array.reset();
  memset(str, 0, sizeof(str) / sizeof(char));
  memcpy(str, "test1;test2;", strlen("test1;test2;"));
  ret = schema.str2string_array(str, string_array);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(2, string_array.count());
  ASSERT_STREQ("test1", string_array.at(0).ptr());
  ASSERT_STREQ("test2", string_array.at(1).ptr());
}
//-----test protected methods for ObSchema----//
class ObSchemaTest:  public ObSchema
{
public:
  ObSchemaTest(){}
  void test_deep_copy_str(void);
  void test_deep_copy_obj(void);
  void test_deep_copy_string_array(void);
  void test_add_string_to_array(void);
  void test_serialize_string_array(void);
  void test_string_array_serialize_size(void);
  void test_reset_string(void);
  void test_reset_string_array(void);
};
void ObSchemaTest::test_deep_copy_str(void)
{
  int ret = OB_SUCCESS;
  char str[BUF_SIZE];
  ObString dest;
  ret = deep_copy_str(NULL, dest);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  memset(str, 0, sizeof(str) / sizeof(char));
  ret = deep_copy_str(str, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(str, dest.ptr());
  memcpy(str, "a test string", strlen("a test string"));
  ret = deep_copy_str(str, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(str, dest.ptr());

  char str1[BUF_SIZE] = "";
  char str2[BUF_SIZE] = "test";
  ObString src1(str1);
  ObString src2(str2);
  ret = deep_copy_str(src1, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, src1.compare(dest));
  ret = deep_copy_str(src2, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, src2.compare(dest));

  //test schema error
  ASSERT_TRUE(is_valid());
  error_ret_ = OB_SCHEMA_ERROR;
  ret = deep_copy_str(src2, dest);
  ASSERT_EQ(OB_SCHEMA_ERROR, ret);
  ASSERT_FALSE(is_valid());
  error_ret_ = OB_SUCCESS;
}
void ObSchemaTest::test_deep_copy_obj(void)
{
  int ret = OB_SUCCESS;
  ObObj src1 = ObObj::make_max_obj();
  ObObj src2 = ObObj::make_min_obj();
  ObObj src3 = ObObj::make_nop_obj();
  ObObj dest;
  ret = deep_copy_obj(src1, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(src1 == dest);
  ret = deep_copy_obj(src2, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(src2 == dest);
  ret = deep_copy_obj(src3, dest);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(src3 == dest);

  //test schema error
  error_ret_ = OB_SCHEMA_ERROR;
  ret = deep_copy_obj(src2, dest);
  ASSERT_EQ(OB_SCHEMA_ERROR, ret);
  error_ret_ = OB_SUCCESS;
}
void ObSchemaTest::test_deep_copy_string_array(void)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> src_array;
  ObArrayHelper<ObString> dst_array;
  ret = deep_copy_string_array(src_array, dst_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(src_array.count(), dst_array.count());

  src_array.push_back(ObString::make_string("test1"));
  ret = deep_copy_string_array(src_array, dst_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, dst_array.count());
  ASSERT_STREQ("test1", dst_array.at(0).ptr());

  src_array.push_back(ObString::make_string("test2"));
  dst_array.reset();
  ret = deep_copy_string_array(src_array, dst_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ("test1", dst_array.at(0).ptr());
  ASSERT_STREQ("test2", dst_array.at(1).ptr());

  ret = deep_copy_string_array(src_array, dst_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, dst_array.count());
  ASSERT_STREQ("test1", dst_array.at(0).ptr());
  ASSERT_STREQ("test2", dst_array.at(1).ptr());
}
void ObSchemaTest::test_add_string_to_array(void)
{
  int ret = OB_SUCCESS;
  ObArrayHelper<ObString> array;
  ObString str("test");
  ret = add_string_to_array(str, array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, array.count());
  ASSERT_STREQ("test", array.at(0).ptr());

  array.reset();
  for (int64_t i = 0; i < 10; ++i) {
    ret = add_string_to_array(str, array);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(10, array.count());
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_STREQ("test", array.at(i).ptr());
  }
}
void ObSchemaTest::test_serialize_string_array(void)
{
  const int TEST_ELEMENTS = 2;
  char buf1[BUF_SIZE];
  char buf2[BUF_SIZE];
  char buf3[BUF_SIZE];
  int64_t pos = 0;
  int64_t buf_len = 0;
  int ret = OB_SUCCESS;
  ObArrayHelper<ObString> array1;
  ObArrayHelper<ObString> array2;
  char str1[BUF_SIZE] = "test1";
  char str2[BUF_SIZE] = "test2";
  int64_t alloc_size = TEST_ELEMENTS * static_cast<int64_t>(sizeof(ObString));
  void *tmp = ob_malloc(alloc_size);
  ASSERT_NE(static_cast<void *>(NULL), tmp);
  array1.init(TEST_ELEMENTS, static_cast<ObString *>(tmp));
  array1.push_back(ObString::make_string(str1));
  array1.push_back(ObString::make_string(str2));
  ret = serialize_string_array(buf1, sizeof(buf1) / sizeof(char), pos, array1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, array1.get_array_size());
  buf_len = pos;
  pos = 0;
  ret = deserialize_string_array(buf1, buf_len, pos, array2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, array1.count());
  ASSERT_EQ(2, array2.count());
  ASSERT_EQ(2, array1.get_array_size());
  ASSERT_EQ(2, array2.get_array_size());
  ASSERT_STREQ(array1.at(0).ptr(), array2.at(0).ptr());
  ASSERT_STREQ(array1.at(1).ptr(), array2.at(1).ptr());
  ob_free(tmp);
  tmp = NULL;

  ObArrayHelper<ObString> array3;
  ObArrayHelper<ObString> array4;
  tmp = ob_malloc(alloc_size);
  ASSERT_NE(static_cast<void *>(NULL), tmp);
  array3.init(TEST_ELEMENTS, static_cast<ObString *>(tmp));
  array3.push_back(ObString::make_string(str1));
  ASSERT_EQ(1, array3.count());
  pos = 0;
  ret = serialize_string_array(buf2, sizeof(buf2) / sizeof(char), pos, array3);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = deserialize_string_array(buf2, buf_len, pos, array4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, array3.count());
  ASSERT_EQ(1, array4.count());
  ASSERT_STREQ(array3.at(0).ptr(), array4.at(0).ptr());
  ob_free(tmp);
  tmp = NULL;

  ObArrayHelper<ObString> array5;
  ObArrayHelper<ObString> array6;
  pos = 0;
  ret = serialize_string_array(buf3, sizeof(buf3) / sizeof(char), pos, array5);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = deserialize_string_array(buf3, buf_len, pos, array6);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, array5.count());
  ASSERT_EQ(0, array6.count());
}

void ObSchemaTest::test_string_array_serialize_size(void)
{
  const int TEST_ELEMENTS = 2;
  ObArrayHelper<ObString> array;
  int64_t expected_serialize_size = 0;
  int64_t calculated_serialize_size = 0;
  int64_t alloc_size = TEST_ELEMENTS * static_cast<int64_t>(sizeof(ObString));
  void *tmp = ob_malloc(alloc_size);
  ASSERT_NE(static_cast<void *>(NULL), tmp);
  array.init(TEST_ELEMENTS, static_cast<ObString *>(tmp));
  expected_serialize_size = serialization::encoded_length_vi64(0);
  calculated_serialize_size = get_string_array_serialize_size(array);
  ASSERT_EQ(expected_serialize_size, calculated_serialize_size);

  array.push_back(ObString::make_string("test1"));
  expected_serialize_size = serialization::encoded_length_vi64(1);
  expected_serialize_size += array.at(0).get_serialize_size();
  calculated_serialize_size = get_string_array_serialize_size(array);
  ASSERT_EQ(expected_serialize_size, calculated_serialize_size);

  array.push_back(ObString::make_string("test2"));
  expected_serialize_size = serialization::encoded_length_vi64(2);
  expected_serialize_size += array.at(0).get_serialize_size();
  expected_serialize_size += array.at(1).get_serialize_size();
  calculated_serialize_size = get_string_array_serialize_size(array);
  ASSERT_EQ(expected_serialize_size, calculated_serialize_size);
  ob_free(tmp);
  tmp = NULL;
}
void ObSchemaTest::test_reset_string(void)
{
  ObString str;
  reset_string(str);
  ASSERT_TRUE(str.empty());
  str = ObString("test");
  ASSERT_FALSE(str.empty());
  reset_string(str);
  ASSERT_TRUE(str.empty());
}
void ObSchemaTest::test_reset_string_array(void)
{
  ObArrayHelper<ObString> array;
  int64_t alloc_size = 2 * sizeof(ObString);
  void *buf = ob_malloc(alloc_size);
  reset_string_array(array);
  ASSERT_EQ(0, array.count());
  array.init(2, static_cast<ObString *>(buf), 0);
  array.push_back(ObString("test1"));
  array.push_back(ObString("test2"));
  ASSERT_EQ(2, array.count());
  reset_string_array(array);
  ASSERT_EQ(0, array.count());
  ob_free(buf);
  buf = NULL;
}
void test_ob_schema_protected_methods(void)
{
  ObSchemaTest ost;
  ost.test_deep_copy_str();
  ost.test_deep_copy_obj();
  ost.test_deep_copy_string_array();
  ost.test_add_string_to_array();
  ost.test_serialize_string_array();
  ost.test_string_array_serialize_size();
  ost.test_reset_string();
  ost.test_reset_string_array();
}
TEST(ObSchemaStructTest, ob_schema_test)
{
  test_ob_schema_string_array2str();
  test_ob_schema_str2_string_array();
  test_ob_schema_protected_methods();
}
//------test for ObTenantSchema--------//
TEST(ObSchemaStructTest, ob_tenant_schema_test)
{
  ObTenantSchema tenant_schema;
  ObArray<ObString> zone;
  zone.push_back(ObString::make_string("zone1"));
  ASSERT_FALSE(tenant_schema.is_valid());
  tenant_schema.set_tenant_id(1);
  tenant_schema.set_schema_version(11);
  tenant_schema.set_tenant_name("charles");
  tenant_schema.set_comment("a tenant");
  tenant_schema.set_zone_list(zone);
  tenant_schema.set_primary_zone(ObString::make_string("zone1"));
  tenant_schema.add_zone(ObString::make_string("zone2"));
  tenant_schema.set_locked(false);
  ASSERT_EQ(1, tenant_schema.get_tenant_id());
  ASSERT_EQ(11, tenant_schema.get_schema_version());
  ASSERT_STREQ("charles", tenant_schema.get_tenant_name());
  ASSERT_STREQ("a tenant", tenant_schema.get_comment());
  ASSERT_STREQ("charles", tenant_schema.get_tenant_name_str().ptr());
  ASSERT_STREQ("zone1", tenant_schema.get_primary_zone().ptr());
  ASSERT_FALSE(tenant_schema.get_locked());
  ASSERT_STREQ("a tenant", tenant_schema.get_comment_str().ptr());

  ObTenantSchema *new_schema = NULL;
  const int64_t size = tenant_schema.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObTenantSchema), size - sizeof(ObTenantSchema));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  new_schema = new(buf)ObTenantSchema(&data_buf);
  ASSERT_NE(static_cast<ObTenantSchema *>(NULL), new_schema);

  *new_schema = tenant_schema;
  ASSERT_EQ(true, new_schema->is_valid());
  ASSERT_TRUE(ObTenantSchema::equal(&tenant_schema, new_schema));
  ASSERT_TRUE(ObTenantSchema::equal_tenant_id(new_schema, 1));

  ObTenantSchema copy_schema(tenant_schema);
  ASSERT_EQ(true, copy_schema.is_valid());
  ASSERT_TRUE(ObTenantSchema::equal(&tenant_schema, &copy_schema));
  ASSERT_TRUE(ObTenantSchema::equal_tenant_id(&copy_schema, 1));
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObTenantSchema schema(tenant_schema);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = schema.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = schema.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(schema.get_serialize_size(), tenant_schema.get_serialize_size());
}
//------test for ObDatabaseSchema------//
TEST(ObSchemaStructTest, ob_database_schema_test)
{
  int ret = OB_SUCCESS;
  ObDatabaseSchema database_schema;
  database_schema.set_tenant_id(1);
  database_schema.set_database_id(1);
  database_schema.set_database_name("yyy");
  database_schema.set_comment("oceanbase RD");
  database_schema.add_zone("zone1");

  ObDatabaseSchema *new_database = NULL;
  const int64_t size = database_schema.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObDatabaseSchema), size - sizeof(ObDatabaseSchema));
  ASSERT_TRUE(NULL != buf);
  //test construct
  new_database = new(buf)ObDatabaseSchema(&data_buf);
  ASSERT_TRUE(NULL != new_database);

  //test assign
  *new_database = database_schema;
  ASSERT_EQ(true, new_database->is_valid());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_database->get_tenant_id(), database_schema.get_tenant_id());
  ASSERT_EQ(new_database->get_database_id(), database_schema.get_database_id());
  ASSERT_EQ(new_database->get_database_name_str(), database_schema.get_database_name_str());
  ASSERT_EQ(new_database->get_comment_str(), database_schema.get_comment_str());
  ASSERT_EQ(new_database->get_tenant_database_id(), database_schema.get_tenant_database_id());

  ObDatabaseSchema copy_schema(database_schema);
  ASSERT_EQ(copy_schema.get_tenant_id(), database_schema.get_tenant_id());
  ASSERT_EQ(copy_schema.get_database_id(), database_schema.get_database_id());
  ASSERT_EQ(copy_schema.get_database_name_str(), database_schema.get_database_name_str());
  ASSERT_EQ(copy_schema.get_comment_str(), database_schema.get_comment_str());
  ASSERT_EQ(copy_schema.get_tenant_database_id(), database_schema.get_tenant_database_id());
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObDatabaseSchema schema(database_schema);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  ret = schema.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = schema.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(schema.get_serialize_size(), database_schema.get_serialize_size());
}
//--------test for ObTablegroupSchema-------//
TEST(ObSchemaStructTest, ob_tablegroup_schema_test)
{
  ObTablegroupSchema tablegroup_schema;
  tablegroup_schema.set_tenant_id(1);
  tablegroup_schema.set_tablegroup_id(1);
  tablegroup_schema.set_tablegroup_name("yyy");
  tablegroup_schema.set_comment("oceanbase RD");
  tablegroup_schema.set_schema_version(ObTimeUtility::current_time());

  ObTablegroupSchema *new_tablegroup = NULL;
  const int64_t size = tablegroup_schema.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObTablegroupSchema), size - sizeof(ObTablegroupSchema));
  ASSERT_TRUE(NULL != buf);
  //test construct
  new_tablegroup = new(buf)ObTablegroupSchema(&data_buf);
  ASSERT_TRUE(NULL != new_tablegroup);

  //test assign()
  *new_tablegroup = tablegroup_schema;
  EXPECT_EQ(true, new_tablegroup->is_valid());
  ASSERT_EQ(new_tablegroup->get_tenant_id(), tablegroup_schema.get_tenant_id());
  ASSERT_EQ(new_tablegroup->get_tablegroup_id(), tablegroup_schema.get_tablegroup_id());
  ASSERT_EQ(new_tablegroup->get_tablegroup_name_str(),
            tablegroup_schema.get_tablegroup_name_str());
  ASSERT_EQ(new_tablegroup->get_comment_str(), tablegroup_schema.get_comment_str());
  ASSERT_EQ(new_tablegroup->get_tenant_tablegroup_id(),
            tablegroup_schema.get_tenant_tablegroup_id());

  //test copy constructor
  ObTablegroupSchema schema(*new_tablegroup);
  EXPECT_EQ(true, schema.is_valid());
  ASSERT_EQ(new_tablegroup->get_tenant_id(), schema.get_tenant_id());
  ASSERT_EQ(new_tablegroup->get_tablegroup_id(), schema.get_tablegroup_id());
  ASSERT_EQ(new_tablegroup->get_tablegroup_name_str(), schema.get_tablegroup_name_str());
  ASSERT_EQ(new_tablegroup->get_comment_str(), schema.get_comment_str());
  ASSERT_EQ(new_tablegroup->get_tenant_tablegroup_id(), schema.get_tenant_tablegroup_id());
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObTablegroupSchema copy_schema(tablegroup_schema);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = copy_schema.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = copy_schema.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(copy_schema.get_serialize_size(), tablegroup_schema.get_serialize_size());
}
//--------test for ObPartitionOption--------//
TEST(ObSchemaStructTest, ob_partition_option)
{
  ObPartitionOption partition_option;
  ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_KEY;
  ASSERT_TRUE(partition_option.is_valid());
  partition_option.set_part_expr(ObString::make_string("test_ob_partition_option"));
  partition_option.set_part_num(1111);
  partition_option.set_part_func_type(part_func_type);
  ASSERT_STREQ("test_ob_partition_option", partition_option.get_part_func_expr_str().ptr());
  ASSERT_STREQ("test_ob_partition_option", partition_option.get_part_func_expr());
  ASSERT_EQ(1111, partition_option.get_part_num());
  ASSERT_EQ(PARTITION_FUNC_TYPE_KEY, partition_option.get_part_func_type());

  ObPartitionOption *new_option = NULL;
  const int64_t size = partition_option.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObPartitionOption), size - sizeof(ObPartitionOption));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  new_option = new(buf)ObPartitionOption(&data_buf);
  ASSERT_NE(static_cast<ObPartitionOption *>(NULL), new_option);

  *new_option = partition_option;
  ASSERT_TRUE(*new_option == partition_option);
  ASSERT_FALSE(*new_option != partition_option);

  ObPartitionOption copy_option(partition_option);
  ASSERT_TRUE(*new_option == copy_option);
  ASSERT_TRUE(copy_option == partition_option);
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObPartitionOption option(partition_option);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = option.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = option.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(option.get_serialize_size(), partition_option.get_serialize_size());
}
//--------test for ObViewSchema-------//
TEST(ObSchemaStructTest, ob_view_schema_test)
{
  ObViewSchema view_schema;
  view_schema.set_view_is_updatable(false);
  view_schema.set_view_definition("abc");
  view_schema.set_view_definition("chenqun");

  ObViewSchema *new_view = NULL;
  const int64_t size = view_schema.get_convert_size();
  char *buf = (char*)ob_malloc(size);
  ObDataBuffer data_buf(buf + sizeof(ObViewSchema), size - sizeof(ObViewSchema));
  ASSERT_TRUE(NULL != buf);
  //test construct
  new_view = new(buf)ObViewSchema(&data_buf);
  ASSERT_TRUE(NULL != new_view);

  //test assign()
  *new_view = view_schema;
  EXPECT_EQ(true, new_view->is_valid());
  ASSERT_EQ(new_view->get_view_definition_str(), view_schema.get_view_definition_str());
  ASSERT_EQ(new_view->get_view_is_updatable(), view_schema.get_view_is_updatable());

  //test copy constructor
  ObViewSchema copy_schema(view_schema);
  EXPECT_EQ(true, copy_schema.is_valid());
  ASSERT_EQ(copy_schema.get_view_definition_str(), view_schema.get_view_definition_str());
  ASSERT_EQ(copy_schema.get_view_is_updatable(), view_schema.get_view_is_updatable());
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObViewSchema schema(view_schema);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = schema.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = schema.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(schema.get_serialize_size(), view_schema.get_serialize_size());
}
//--------test for ObTableSchemaHashWrapper-------//
TEST(ObSchemaStructTest, ob_table_schema_hash_wrapper_test)
{
  ObString name_1 = ObString::make_string("name_1");
  ObString name_2 = ObString::make_string("name_2");
  uint64_t tenant_id_1 = 1;
  uint64_t tenant_id_2 = 2;
  uint64_t database_id_1 = 1;
  uint64_t database_id_2 = 2;
  ObTableSchemaHashWrapper  tmp;
  ObNameCaseMode mode = OB_ORIGIN_AND_INSENSITIVE;
  ObTableSchemaHashWrapper  a(tenant_id_1, database_id_1, mode, name_1);
  ObTableSchemaHashWrapper  b(tenant_id_2, database_id_2, mode, name_2);
  ASSERT_NE(a.get_tenant_id(), b.get_tenant_id());
  ASSERT_NE(a.get_database_id(), b.get_database_id());
  ASSERT_NE(a.get_table_name(), b.get_table_name());
  ASSERT_NE(a.hash(), b.hash());
  ASSERT_FALSE(a == b);
}
//--------test for ObDatabaseSchemaHashWrapper-------//
TEST(ObSchemaStructTest, ob_database_schema_hash_wrapper_test)
{
  ObString name_1 = ObString::make_string("name_1");
  ObString name_2 = ObString::make_string("name_2");
  uint64_t tenant_id_1 = 1;
  uint64_t tenant_id_2 = 2;
  ObDatabaseSchemaHashWrapper  tmp;
  ObNameCaseMode mode = OB_ORIGIN_AND_INSENSITIVE;
  ObDatabaseSchemaHashWrapper  a(tenant_id_1, mode, name_1);
  ObDatabaseSchemaHashWrapper  b(tenant_id_2, mode, name_2);
  ASSERT_NE(a.get_tenant_id(), b.get_tenant_id());
  ASSERT_NE(a.get_database_name(), b.get_database_name());
  ASSERT_NE(a.hash(), b.hash());
  ASSERT_FALSE(a == b);
}
//--------test for ObTablegroupSchemaHashWrapper-------//
TEST(ObSchemaStructTest, ob_tablegroup_schema_hash_wrapper_test)
{
  ObString name_1 = ObString::make_string("name_1");
  ObString name_2 = ObString::make_string("name_2");
  uint64_t tenant_id_1 = 1;
  uint64_t tenant_id_2 = 2;
  ObTablegroupSchemaHashWrapper  tmp;
  ObTablegroupSchemaHashWrapper  a(tenant_id_1,  name_1);
  ObTablegroupSchemaHashWrapper  b(tenant_id_2,  name_2);
  ASSERT_NE(a.get_tenant_id(), b.get_tenant_id());
  ASSERT_NE(a.get_tablegroup_name(), b.get_tablegroup_name());
  ASSERT_NE(a.hash(), b.hash());
  ASSERT_FALSE(a == b);
}
//--------test for ObTenantUserId----------//
TEST(ObSchemaStructTest, ob_tenant_user_id)
{
  ObTenantUserId a;
  ASSERT_FALSE(a.is_valid());
  ObTenantUserId b(1, 1);
  ObTenantUserId c(1, 2);
  ObTenantUserId d(2, 1);
  ObTenantUserId e(2, 1);
  ObTenantUserId f = c;
  ASSERT_TRUE(b.is_valid());
  ASSERT_TRUE(c.is_valid());
  ASSERT_TRUE(d.is_valid());
  ASSERT_TRUE(e.is_valid());
  ASSERT_TRUE(d == e);
  ASSERT_TRUE(b < c);
  ASSERT_TRUE(b < d);
  ASSERT_FALSE(d < e);
  ASSERT_TRUE(c < d);
  ASSERT_TRUE(c == f);
  ASSERT_EQ(d.hash(), e.hash());
  ASSERT_TRUE(b != c);
  ASSERT_FALSE(d != e);
}
//--------test for ObPriv--------------//
TEST(ObSchemaStructTest, ob_priv)
{
  ObPriv op;
  ASSERT_FALSE(op.is_valid());
  op.set_tenant_id(1);
  op.set_user_id(1);
  op.set_schema_version(11);
  op.set_priv(1);
  op.set_priv_set(1111);
  ASSERT_TRUE(op.is_valid());
  ASSERT_EQ(1, op.get_tenant_id());
  ASSERT_EQ(1, op.get_user_id());
  ASSERT_EQ(1111, op.get_priv_set());

  ObPriv copy_op = op;
  ObTenantUserId tuid1(2, 2);
  ObTenantUserId tuid2(1, 1);
  ASSERT_TRUE(ObPriv::cmp_tenant_user_id(&op, tuid1));
  ASSERT_TRUE(ObPriv::equal_tenant_user_id(&op, tuid2));
  ASSERT_TRUE(ObPriv::cmp_tenant_id(&op, 2));
}
//--------test ObUserInfoHashWrapper----------//
TEST(ObSchemaStructTest, ob_user_info_hash_wrapper)
{
  ObString name_1 = ObString::make_string("name_1");
  ObString name_2 = ObString::make_string("name_2");
  uint64_t tenant_id_1 = 1;
  uint64_t tenant_id_2 = 2;
  ObUserInfoHashWrapper  tmp;
  ObUserInfoHashWrapper  a(tenant_id_1,  name_1);
  ObUserInfoHashWrapper  b(tenant_id_2,  name_2);
  ASSERT_NE(a.get_tenant_id(), b.get_tenant_id());
  ASSERT_NE(a.get_user_name(), b.get_user_name());
  ASSERT_NE(a.hash(), b.hash());
  ASSERT_FALSE(a == b);
}
//---------test ObUserInfo-----------//
TEST(ObSchemaStructTest, ob_user_info)
{
  ObUserInfo info;
  ASSERT_FALSE(info.is_valid());
  info.set_user_name("charles");
  ASSERT_STREQ("charles", info.get_user_name());
  info.set_user_name(ObString::make_string("charles2"));
  ASSERT_STREQ("charles2", info.get_user_name_str().ptr());
  info.set_host("host1");
  ASSERT_STREQ("host1", info.get_host());
  info.set_host(ObString::make_string("host2"));
  ASSERT_STREQ("host2", info.get_host_str().ptr());
  info.set_passwd("passwd1");
  ASSERT_STREQ("passwd1", info.get_passwd());
  info.set_passwd(ObString::make_string("passwd2"));
  ASSERT_STREQ("passwd2", info.get_passwd_str().ptr());
  info.set_info("info1");
  ASSERT_STREQ("info1", info.get_info());
  info.set_info(ObString::make_string("info2"));
  ASSERT_STREQ("info2", info.get_info_str().ptr());
  info.set_is_locked(true);
  ASSERT_TRUE(info.get_is_locked());

  ObUserInfo *new_info = NULL;
  const int64_t size = info.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObUserInfo), size - sizeof(ObUserInfo));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  new_info = new(buf)ObUserInfo(&data_buf);
  ASSERT_NE(static_cast<ObUserInfo *>(NULL), new_info);

  *new_info = info;
  ASSERT_TRUE(ObUserInfo::equal(&info, new_info));
  ASSERT_FALSE(ObUserInfo::cmp(&info, new_info));

  ObUserInfo copy_info(info);
  ASSERT_TRUE(ObUserInfo::equal(&info, new_info));
  ASSERT_FALSE(ObUserInfo::cmp(&info, new_info));
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObUserInfo serialize_info(info);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = serialize_info.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = serialize_info.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(serialize_info.get_serialize_size(), info.get_serialize_size());
}
//---------test for ObDBPrivSortKey------------//
TEST(ObSchemaStructTest, ob_db_priv_sort_key)
{
  ObDBPrivSortKey key;
  key.tenant_id_ = 1;
  key.user_id_ = 1;
  ObDBPrivSortKey new_key(1, 1, 0);
  ASSERT_EQ(key, new_key);
  ASSERT_FALSE(key != new_key);
  ASSERT_FALSE(key < new_key);
}
//----------test for ObOriginalDBKey------------//
TEST(ObSchemaStructTest, ob_original_db_key)
{
  ObOriginalDBKey key;
  ASSERT_FALSE(key.is_valid());
  key.tenant_id_ = 1;
  key.user_id_ = 1;
  key.db_ = ObString::make_string("database");
  ObOriginalDBKey new_key(1, 1, ObString::make_string("database"));
  ASSERT_EQ(key, new_key);
  ASSERT_FALSE(key != new_key);
  ASSERT_FALSE(key < new_key);
  ASSERT_EQ(key.hash(), new_key.hash());
}
//---------test for ObDBPriv-----------//
TEST(ObSchemaStructTest, ob_db_priv)
{
  ObDBPriv priv;
  ASSERT_FALSE(priv.is_valid());
  priv.set_database_name("database");
  ASSERT_STREQ("database", priv.get_database_name());
  priv.set_database_name(ObString::make_string("database2"));
  ASSERT_STREQ("database2", priv.get_database_name_str().ptr());
  priv.set_sort(1);
  ASSERT_EQ(1, priv.get_sort());

  ObDBPriv *new_priv = NULL;
  const int64_t size = priv.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObDBPriv), size - sizeof(ObDBPriv));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  new_priv = new(buf)ObDBPriv(&data_buf);
  ASSERT_NE(static_cast<ObDBPriv *>(NULL), new_priv);

  *new_priv = priv;
  ASSERT_FALSE(ObDBPriv::cmp(&priv, new_priv));
  ASSERT_FALSE(ObDBPriv::cmp_sort_key(new_priv, priv.get_sort_key()));
  ASSERT_TRUE(ObDBPriv::equal(&priv, new_priv));
  priv.reset();
  ASSERT_FALSE(ObDBPriv::cmp(&priv, new_priv));
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObDBPriv copy_priv(priv);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = copy_priv.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = copy_priv.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(copy_priv.get_serialize_size(), priv.get_serialize_size());
}
//---------test for ObTablePrivDBKey----------//
TEST(ObSchemaStructTest, ob_table_priv_db_key)
{
  ObTablePrivDBKey key1;
  ObTablePrivDBKey key2(1, 1, ObString::make_string("test"));
  ASSERT_NE(key1, key2);
  key1.tenant_id_ = 1;
  key1.user_id_ = 1;
  key1.db_ = ObString::make_string("test");
  ASSERT_EQ(key1, key2);
  ASSERT_FALSE(key1 < key2);
}
//--------test for ObTablePrivSortKey--------//
TEST(ObSchemaStructTest, ob_table_priv_sort_key)
{
  ObTablePrivSortKey key1;
  ObTablePrivSortKey key2(1, 1, ObString("database"), ObString("table"));
  ASSERT_FALSE(key1.is_valid());
  ASSERT_TRUE(key2.is_valid());
  ASSERT_NE(key1, key2);
  ASSERT_NE(key1.hash(), key2.hash());
  key1.tenant_id_ = 1;
  key1.user_id_ = 1;
  key1.db_ = ObString::make_string("database");
  key1.table_ = ObString::make_string("table");
  ASSERT_EQ(key1, key2);
  ASSERT_FALSE(key1 < key2);
  ASSERT_EQ(key1.hash(), key2.hash());
}
//---------test for ObTablePriv----------//
TEST(ObSchemaStructTest, ob_table_priv)
{
  ObTablePriv priv;
  ASSERT_FALSE(priv.is_valid());
  priv.set_database_name("database1");
  ASSERT_STREQ("database1", priv.get_database_name());
  priv.set_database_name(ObString("database2"));
  ASSERT_STREQ("database2", priv.get_database_name_str().ptr());
  priv.set_table_name("table1");
  ASSERT_STREQ("table1", priv.get_table_name());
  priv.set_table_name(ObString("table2"));
  ASSERT_STREQ("table2", priv.get_table_name_str().ptr());

  ObTablePriv *new_priv = NULL;
  const int64_t size = priv.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObTablePriv), size - sizeof(ObTablePriv));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  new_priv = new(buf)ObTablePriv(&data_buf);
  ASSERT_NE(static_cast<ObTablePriv *>(NULL), new_priv);

  *new_priv = priv;
  ASSERT_FALSE(ObTablePriv::cmp(&priv, new_priv));
  ASSERT_FALSE(ObTablePriv::cmp_sort_key(new_priv, priv.get_sort_key()));
  ASSERT_TRUE(ObTablePriv::equal(&priv, new_priv));
  ASSERT_FALSE(ObTablePriv::cmp_db_key(new_priv, priv.get_db_key()));
  priv.reset();
  ASSERT_FALSE(priv.is_valid());
  ob_free(buf);
  buf = NULL;

  //test serialize and deserialize
  ObTablePriv copy_priv(priv);
  char tmp[BUF_SIZE];
  int64_t buf_len = BUF_SIZE;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = copy_priv.serialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  buf_len = pos;
  pos = 0;
  ret = copy_priv.deserialize(tmp, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(copy_priv.get_serialize_size(), priv.get_serialize_size());
}
//---------test for ob_priv_level_str---------//
TEST(ObSchemaStructTest, ob_priv_level_str)
{
  ASSERT_STREQ("Unknown", ob_priv_level_str(OB_PRIV_INVALID_LEVEL));
  ASSERT_STREQ("USER_LEVEL", ob_priv_level_str(OB_PRIV_USER_LEVEL));
  ASSERT_STREQ("DB_LEVEL", ob_priv_level_str(OB_PRIV_DB_LEVEL));
  ASSERT_STREQ("TABLE_LEVEL", ob_priv_level_str(OB_PRIV_TABLE_LEVEL));
  ASSERT_STREQ("DB_ACCESS_LEVEL", ob_priv_level_str(OB_PRIV_DB_ACCESS_LEVEL));
  ASSERT_STREQ("Unknown", ob_priv_level_str(OB_PRIV_MAX_LEVEL));
}
//----------test for ObNeedPriv-----------//
TEST(ObSchemaStructTest, ob_need_priv)
{
  int ret = OB_SUCCESS;
  ObNeedPriv priv1;
  ObNeedPriv priv2;
  void *data = ob_malloc(BUF_SIZE);
  memset(data, 0, BUF_SIZE);
  ObDataBuffer buf(static_cast<char *>(data), BUF_SIZE);
  priv1.db_ = ObString::make_string("database");
  priv1.priv_level_ = OB_PRIV_DB_LEVEL;
  ret = priv2.deep_copy(priv1, buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(priv1.db_.ptr(), priv2.db_.ptr());
  ASSERT_NE(priv1.db_.ptr(), priv2.db_.ptr());
  ob_free(data);
  data = NULL;
}
//--------test for ObStmtNeedPrivs----------//
TEST(ObSchemaStructTest, ob_stmt_need_privs)
{
  ObStmtNeedPrivs privs1;
  ObStmtNeedPrivs privs2;
  ObNeedPriv priv;
  void *data = ob_malloc(BUF_SIZE);
  ObDataBuffer buf(static_cast<char *>(data), BUF_SIZE);
  ASSERT_EQ(0, privs1.need_privs_.count());
  ASSERT_EQ(0, privs2.need_privs_.count());
  privs1.need_privs_.push_back(priv);
  privs1.need_privs_.push_back(priv);
  privs2.deep_copy(privs1, buf);
  ASSERT_EQ(privs1.need_privs_.count(), privs2.need_privs_.count());
  privs1.reset();
  ASSERT_EQ(0, privs1.need_privs_.count());
  ob_free(data);
  data = NULL;
}
//--------test for ObSessionPrivInfo--------//
TEST(ObSchemaStructTest, ob_session_priv_info)
{
  ObSessionPrivInfo info1;
  ObSessionPrivInfo info2(1, 1, ObString("test"), 1111, 11);
  ASSERT_FALSE(info1.is_valid());
  ASSERT_TRUE(info2.is_valid());
}
//--------test for ObUserLoginInfo--------//
TEST(ObSchemaStructTest, ob_user_login_info)
{
  ObUserLoginInfo info1;
  ObString tenant("charles");
  ObString user("ccc");
  ObString passwd("word");
  ObString db("db_test");
  ObUserLoginInfo info2(tenant, user, passwd, db);
  info1.tenant_name_ = tenant;
  info1.user_name_ = user;
  info1.passwd_ = passwd;
  info1.db_ = db;
  ASSERT_EQ(info1.tenant_name_.ptr(), info2.tenant_name_.ptr());
  ASSERT_EQ(info1.user_name_.ptr(), info2.user_name_.ptr());
  ASSERT_EQ(info1.passwd_.ptr(), info2.passwd_.ptr());
  ASSERT_EQ(info1.db_.ptr(), info2.db_.ptr());
}
#endif

#if 0
TEST(ObTablePartitionKeyIter, partition_key_iter)
{
  const uint64_t table_id = 1;
  const int64_t part_num = 3;
  const int64_t subpart_num = 5;
  //PARTITION LEVEL TWO
  ObTablePartitionKeyIter key_iter(table_id, part_num, subpart_num, PARTITION_LEVEL_TWO);

  ASSERT_EQ(part_num * subpart_num, key_iter.get_partition_cnt());
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_num * subpart_num; ++i) {
    int64_t part_idx = i / subpart_num;
    int64_t subpart_idx = i % subpart_num;
    ObPartitionKey pkey;
    if (OB_FAIL(key_iter.next_partition_key(pkey))) {
    } else {
      int64_t partition_id = pkey.get_partition_id();
      ASSERT_EQ(table_id, pkey.get_table_id());
      ASSERT_EQ(part_idx, extract_part_idx(partition_id));
      ASSERT_EQ(subpart_idx, extract_subpart_idx(partition_id));
    }
  }
  //Partition level one
  ObTablePartitionKeyIter key_iter_one(table_id, part_num, 0, PARTITION_LEVEL_ONE);
  ASSERT_EQ(part_num, key_iter_one.get_partition_cnt());
  for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
    ObPartitionKey pkey;
    if (OB_FAIL(key_iter.next_partition_key(pkey))) {
    } else {
      int64_t partition_id = pkey.get_partition_id();
      ASSERT_EQ(i, partition_id);
      ASSERT_EQ(table_id, pkey.get_table_id());
    }

  }

}
#endif

TEST(ObTablePartitionKeyIter, partition_key_iter_v2)
{
  const uint64_t table_id = 1;
  ObTableSchema table;

  // zero
  table.set_table_id(table_id);
  table.set_part_level(PARTITION_LEVEL_ZERO);
  table.set_partition_num(1);

  bool check_dropped_schema = false;
  ObTablePartitionKeyIter key_iter(table, check_dropped_schema);
  ASSERT_EQ(1, key_iter.get_partition_cnt());
  ObPartitionKey pkey;
  ASSERT_EQ(OB_SUCCESS, key_iter.next_partition_key_v2(pkey));
  ASSERT_EQ(table_id, pkey.get_table_id());
  ASSERT_EQ(0, pkey.get_partition_id());

  // level one
  table.reset();
  table.set_table_id(table_id);
  table.set_part_level(PARTITION_LEVEL_ONE);
  table.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE);
  table.get_part_option().set_part_num(3);
  ObPartition part;
  part.set_part_id(1);
  ObString part_name("p1");
  table.add_partition(part);
  part.set_part_id(3);
  part_name = ObString("p3");
  part.set_part_name(part_name);
  table.add_partition(part);
  part.set_part_id(5);
  part_name = ObString("p5");
  part.set_part_name(part_name);
  table.add_partition(part);

  ObTablePartitionKeyIter key_iter_level_one(table, check_dropped_schema);
  ASSERT_EQ(3, key_iter_level_one.get_partition_cnt());

  ASSERT_EQ(OB_SUCCESS, key_iter_level_one.next_partition_key_v2(pkey));
  ASSERT_EQ(table_id, pkey.get_table_id());
  ASSERT_EQ(1, pkey.get_partition_id());

  ASSERT_EQ(OB_SUCCESS, key_iter_level_one.next_partition_key_v2(pkey));
  ASSERT_EQ(table_id, pkey.get_table_id());
  ASSERT_EQ(3, pkey.get_partition_id());

  ASSERT_EQ(OB_SUCCESS, key_iter_level_one.next_partition_key_v2(pkey));
  ASSERT_EQ(table_id, pkey.get_table_id());
  ASSERT_EQ(5, pkey.get_partition_id());

  // level two
  table.reset();
  table.set_table_id(table_id);
  const int64_t part_num = 2;
  const int64_t subpart_num = 3;
  table.set_part_level(PARTITION_LEVEL_TWO);
  table.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE);
  table.get_part_option().set_part_num(part_num);
  table.get_sub_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table.get_sub_part_option().set_part_num(subpart_num);

  part.set_part_id(1);
  part_name = ObString("p1");
  part.set_part_name(part_name);
  table.add_partition(part);
  part.set_part_id(3);
  part_name = ObString("p3");
  part.set_part_name(part_name);
  table.add_partition(part);

  ObTablePartitionKeyIter key_iter_level_two(table, check_dropped_schema);
  int64_t part_ids[] = {1, 3};
  int64_t subpart_ids[] = {0, 1, 2};
  ASSERT_EQ(6, key_iter_level_two.get_partition_cnt());
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < 3; ++j) {
      ASSERT_EQ(table_id, pkey.get_table_id());
      ASSERT_EQ(OB_SUCCESS, key_iter_level_two.next_partition_key_v2(pkey));
      ASSERT_EQ(common::generate_phy_part_id(part_ids[i], subpart_ids[j]), pkey.get_partition_id());
    }
  }
}

// TEST_F(TestSchemaStruct, hash_map)
TEST(ObSchemaStructTest, hash_map)
{
  ObArenaAllocator allocator;
  ColumnHashMap map(allocator);
  int32_t value = 0;
  int ret = map.get(1, value);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = map.set(1, 20);
  ASSERT_EQ(OB_NOT_INIT, ret);

  int64_t bucket_num = 100;
  ret = map.init(bucket_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = map.init(bucket_num);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  ret = map.get(1, value);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);
  ret = map.set(1, 20);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = map.set(1, 30);
  ASSERT_EQ(OB_HASH_EXIST, ret);
  ret = map.get(1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(20, value);

  for (int64_t i = 1; i < 1000; ++i) {
    ret = map.set(i + 1, 88);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = map.get(i + 1, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(88, value);
  }
}

// TEST_F(TestSchemaStruct, hash_map)
TEST(ObSchemaStructTest, ObHostnameStuct)
{
  ObString host_name("192.168.0.0/16");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.1", host_name));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.0", host_name));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.255", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.-1", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.-1.1", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.256.1", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.256", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.169.1.1", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.0.1.1", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name));

  ObString host_name_ipv6("fe80:90fa:2017:ff00::/56");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:02", host_name_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("fe80:90fa:2017:fe00:00e8:0074:0a:02", host_name_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match(":90fa:2017:fe00:00e8:0074:0a:02", host_name_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff:", host_name_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("::", host_name_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("::1", host_name_ipv6));

  ObString host_name2("0.0.0.0/0");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.1", host_name2));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.0", host_name2));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.255", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.-1", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.-1.1", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.256.1", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.256", host_name2));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.169.1.1", host_name2));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.0.1.1", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name2));

  ObString host_name2_ipv6("::/0");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name2_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff03::", host_name2_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("::", host_name2_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("::1", host_name2_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.1", host_name2));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match(":2017:ff03::", host_name2_ipv6));

  ObString host_name3("192.168.1.1/32");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.0", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.255", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.-1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.-1.1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.256.1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.256", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.169.1.1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.0.1.1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name3));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name3));

  ObString host_name4("192.168.11.0/255.255.255.0");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.1", host_name4));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.0", host_name4));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.255", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.-1", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.256", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.1", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.10.1", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.0.1.1", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name4));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name4));

  ObString host_name4_ipv6("fe80:90fa:2017:ff80::/ffff:ffff:ffff:ffff:0:0:0:0");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff80:00e8:ff74:0a:02", host_name4_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff80::", host_name4_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff80:e8::0a:02", host_name4_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:02", host_name4_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff00::", host_name4_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("::", host_name4_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("::1", host_name4_ipv6));

  ObString host_name5("0.0.0.0/0.0.0.0");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.1", host_name5));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.0", host_name5));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.255", host_name5));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.-1", host_name5));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.256", host_name5));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.1.1", host_name5));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.10.1", host_name5));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.0.1.1", host_name5));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name5));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name5));

  ObString host_name5_ipv6("::/::");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:02", host_name5_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("::" host_name5_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("::1", host_name5_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("ff:ff:ff:ff:ff:ff:ff:ff", host_name5_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("90fa:2017:ff00:00e8:0074:0a:02", host_name5_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:-1", host_name5_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:g1", host_name5_ipv6));

  ObString host_name6("192.168.11.1/255.255.255.255");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.1", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.0", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.255", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.-1", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.256", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.1", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.10.1", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.0.1.1", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name6));

  ObString host_name10("192.168.11.1");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("192.168.11.1", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.0", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.255", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.-1", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.256", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.1.1", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.10.1", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.0.1.1", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.1", host_name10));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("hello", host_name10));

  ObString host_name10_ipv6("2017::a:2");
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("2017:0:0:0:0:0:a:2", host_name10_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_ip_match("2017::0a:02", host_name10_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("2017:2::0a:02", host_name10_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("::", host_name10_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("::1", host_name10_ipv6));

  ObString host_name7("192.168.11.1/33");
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.1", host_name7));
  ObString host_name8("192.168.11.1/-1");
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.1", host_name8));
  ObString host_name9("192.168.11.1/");
  EXPECT_EQ(false, ObHostnameStuct::is_ip_match("192.168.11.1", host_name9));

  ObString host_name11("192.168.11.%");
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("192.168.11.0", host_name11));
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("192.168.11.255", host_name11));
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("192.168.11.-1", host_name11));
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("192.168.11.256", host_name11));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.1.1", host_name11));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.10.1", host_name11));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.0.1.1", host_name11));

  ObString host_name11_ipv6("fe80:fe20:ffff:%");
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("fe80:fe20:ffff:2014:0506:02:56:80", host_name11_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("fe80:fe20:ffff::", host_name11_ipv6));
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("fe80:fe20:ffff:2019::", host_name11_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("fe80:fe20:fffe::", host_name11_ipv6));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("fe80:fe20:8fff::2084:92:23:78:ff30", host_name11_ipv6));

  ObString host_name12("192.168.11._");
  EXPECT_EQ(true, ObHostnameStuct::is_wild_match("192.168.11.0", host_name12));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.11.255", host_name12));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.11.-1", host_name12));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.11.256", host_name12));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.1.1", host_name12));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.168.10.1", host_name12));
  EXPECT_EQ(false, ObHostnameStuct::is_wild_match("192.0.1.1", host_name12));

  int64_t value = 0;
  EXPECT_EQ(OB_SUCCESS, ObHostnameStuct::get_int_value("12", value));
  EXPECT_EQ(12, value);
  EXPECT_EQ(OB_SUCCESS, ObHostnameStuct::get_int_value("-12", value));
  EXPECT_EQ(-12, value);
  EXPECT_EQ(OB_SUCCESS, ObHostnameStuct::get_int_value("0", value));
  EXPECT_EQ(0, value);
  EXPECT_EQ(OB_SUCCESS, ObHostnameStuct::get_int_value("65536", value));
  EXPECT_EQ(65536, value);
  EXPECT_EQ(OB_SUCCESS, ObHostnameStuct::get_int_value("-65536", value));
  EXPECT_EQ(-65536, value);
  EXPECT_EQ(OB_INVALID_DATA, ObHostnameStuct::get_int_value("12sds", value));
  EXPECT_EQ(OB_INVALID_DATA, ObHostnameStuct::get_int_value("12 sddf", value));
  EXPECT_EQ(OB_INVALID_DATA, ObHostnameStuct::get_int_value("++12", value));
  EXPECT_EQ(OB_INVALID_DATA, ObHostnameStuct::get_int_value("--12", value));

  ObString ip_white_list1("");
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list1));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("%", ip_white_list1));

  ObString ip_white_list2;
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list2));

  ObString ip_white_list3("%");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list3));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list3));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("hello", ip_white_list3));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("", ip_white_list3));

  ObString ip_white_list4("192.0.1.0/24");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list4));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list4));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("hello", ip_white_list4));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("", ip_white_list4));

  ObString ip_white_list5("192.0.1.0/24, %");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list5));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list5));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("hello", ip_white_list5));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("", ip_white_list5));

  ObString ip_white_list6("192.0.1.0/24,%");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list6));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list6));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("hello", ip_white_list6));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("", ip_white_list6));

  ObString ip_white_list7(",192.0.1,192.0.1.0/24");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("192.0.1.1", ip_white_list7));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list7));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("hello", ip_white_list7));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("", ip_white_list7));

  ObString ip_white_list8("10.125.224.0/255.255.252.0");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("10.125.224.15", ip_white_list8));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("10.125.224.5", ip_white_list8));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list8));

  ObString ip_white_list9("10.125.224.0/22");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("10.125.224.15", ip_white_list9));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("10.125.224.5", ip_white_list9));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("192.0.1.-1", ip_white_list9));

  ObString ip_white_list10("255.255.224.0/22");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.15", ip_white_list10));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.5", ip_white_list10));
  ASSERT_EQ(false, ObHostnameStuct::is_in_white_list("255.255.255.15", ip_white_list10));

  ObString ip_white_list11("255.255.224.15");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.15", ip_white_list11));

  ObString ip_white_list12("255.255.224.15,255.255.224.14,255.255.224.13,255.255.224.12,255.255.224.11");
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.15", ip_white_list12));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.14", ip_white_list12));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.13", ip_white_list12));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.12", ip_white_list12));
  ASSERT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.11", ip_white_list12));

  ObString ip_white_list13(" 255.255.224.15, 255.255.224.14, 255.255.224.13, 255.255.224.12");
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.15", ip_white_list13));
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.14", ip_white_list13));
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.13", ip_white_list13));
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.12", ip_white_list13));

  ObString ip_white_list14(" 255.255.224.15/32 , 255.255.224.14/32 , 255.255.224.13/32 , 255.255.224.12/32");
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.15", ip_white_list14));
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.14", ip_white_list14));
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.13", ip_white_list14));
  EXPECT_EQ(true, ObHostnameStuct::is_in_white_list("255.255.224.12", ip_white_list14));
}
}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  //  OB_LOGGER.set_file_name("test_schema.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
