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
#include <gmock/gmock.h>
#define private public
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/random/ob_random.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/allocator/page_arena.h"
#define private public
#include "share/schema/ob_schema_cache.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "schema_test_utils.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {

class GetBatchSchemasFuncs0 {
public:
  // return non schema
  int get_batch_tenants(const int64_t schema_version, ObArray<uint64_t>& tenant_ids, ObISQLClient& client,
      ObIArray<ObTenantSchema>& tenant_infos)
  {
    UNUSED(schema_version);
    UNUSED(tenant_ids);
    UNUSED(client);
    UNUSED(tenant_infos);
    return OB_SUCCESS;
  }
  int get_batch_users(
      const int64_t schema_version, ObArray<uint64_t>& user_ids, ObISQLClient& client, ObIArray<ObUserInfo>& user_infos)
  {
    UNUSED(schema_version);
    UNUSED(user_ids);
    UNUSED(client);
    UNUSED(user_infos);
    return OB_SUCCESS;
  }
  int get_batch_databases(const int64_t schema_version, ObArray<uint64_t>& db_ids, ObISQLClient& client,
      ObIArray<ObDatabaseSchema>& db_schemas)
  {
    UNUSED(schema_version);
    UNUSED(db_ids);
    UNUSED(client);
    UNUSED(db_schemas);
    return OB_SUCCESS;
  }
  int get_batch_tablegroups(const int64_t schema_version, ObArray<uint64_t>& tg_ids, ObISQLClient& client,
      ObIArray<ObTablegroupSchema>& tg_schemas)
  {
    UNUSED(schema_version);
    UNUSED(tg_ids);
    UNUSED(client);
    UNUSED(tg_schemas);
    return OB_SUCCESS;
  }
  int get_batch_outlines(const int64_t schema_version, ObArray<uint64_t>& outline_ids, ObISQLClient& client,
      ObIArray<ObOutlineInfo>& outline_infos)
  {
    UNUSED(schema_version);
    UNUSED(outline_ids);
    UNUSED(client);
    UNUSED(outline_infos);
    return OB_SUCCESS;
  }
  int get_table_schema(const uint64_t table_id, const int64_t schema_version, ObISQLClient& client,
      ObIAllocator& allocator, ObTableSchema*& table_schema)
  {
    UNUSED(table_id);
    UNUSED(schema_version);
    UNUSED(client);
    UNUSED(allocator);
    UNUSED(table_schema);
    return OB_SUCCESS;
  }

  int get_tablegroup_schema(const uint64_t tablegroup_id, const int64_t schema_version,
      common::ObISQLClient& sql_client, common::ObIAllocator& allocator, ObTablegroupSchema*& tablegroup_schema)
  {
    UNUSED(tablegroup_id);
    UNUSED(schema_version);
    UNUSED(sql_client);
    UNUSED(allocator);
    UNUSED(tablegroup_schema);
    return OB_SUCCESS;
  }
};

class GetBatchSchemasFuncs1 {
public:
  // return one schema
  int get_batch_tenants(const int64_t schema_version, ObArray<uint64_t>& tenant_ids, ObISQLClient& client,
      ObIArray<ObTenantSchema>& tenant_infos)
  {
    UNUSED(schema_version);
    UNUSED(tenant_ids);
    UNUSED(client);
    ObTenantSchema tenant_schema;
    tenant_schema.set_tenant_id(1);
    tenant_schema.set_tenant_name("tenant");
    tenant_infos.push_back(tenant_schema);
    return OB_SUCCESS;
  }
  int get_batch_users(
      const int64_t schema_version, ObArray<uint64_t>& user_ids, ObISQLClient& client, ObIArray<ObUserInfo>& user_infos)
  {
    UNUSED(schema_version);
    UNUSED(user_ids);
    UNUSED(client);
    ObUserInfo user_info;
    user_info.set_tenant_id(1);
    user_info.set_user_id(combine_id(1, 1));
    user_info.set_user_name("user");
    user_info.set_host(OB_DEFAULT_HOST_NAME);
    user_infos.push_back(user_info);
    return OB_SUCCESS;
  }
  int get_batch_databases(const int64_t schema_version, ObArray<uint64_t>& db_ids, ObISQLClient& client,
      ObIArray<ObDatabaseSchema>& db_schemas)
  {
    UNUSED(schema_version);
    UNUSED(db_ids);
    UNUSED(client);
    ObDatabaseSchema db_schema;
    db_schema.set_tenant_id(1);
    db_schema.set_database_id(combine_id(1, 1));
    db_schema.set_database_name("db");
    db_schemas.push_back(db_schema);
    return OB_SUCCESS;
  }
  int get_batch_tablegroups(const int64_t schema_version, ObArray<uint64_t>& tg_ids, ObISQLClient& client,
      ObIArray<ObTablegroupSchema>& tg_schemas)
  {
    UNUSED(schema_version);
    UNUSED(tg_ids);
    UNUSED(client);
    ObTablegroupSchema tg_schema;
    tg_schema.set_tenant_id(1);
    tg_schema.set_tablegroup_id(combine_id(1, 1));
    tg_schema.set_tablegroup_name("tg");
    tg_schemas.push_back(tg_schema);
    return OB_SUCCESS;
  }
  int get_table_schema(const uint64_t table_id, const int64_t schema_version, ObISQLClient& client,
      ObIAllocator& allocator, ObTableSchema*& table_schema)
  {
    UNUSED(table_id);
    UNUSED(schema_version);
    UNUSED(client);
    UNUSED(allocator);
    ObTableSchema table;
    // table_info.set_tenant_id(1);
    // table_info.set_table_id(combine_id(1,1));
    // table_info.set_table_name("table");
    table_schema = &table;
    return OB_SUCCESS;
  }
  int get_batch_outlines(const int64_t schema_version, ObArray<uint64_t>& outline_ids, ObISQLClient& client,
      ObIArray<ObOutlineInfo>& outline_infos)
  {
    UNUSED(schema_version);
    UNUSED(outline_ids);
    UNUSED(client);
    ObOutlineInfo outline_info;
    outline_info.set_tenant_id(1);
    outline_info.set_outline_id(combine_id(1, 1));
    outline_info.set_name("outline");
    outline_infos.push_back(outline_info);
    return OB_SUCCESS;
  }
  int get_tablegroup_schema(const uint64_t tablegroup_id, const int64_t schema_version,
      common::ObISQLClient& sql_client, common::ObIAllocator& allocator, ObTablegroupSchema*& tablegroup_schema)
  {
    UNUSED(tablegroup_id);
    UNUSED(schema_version);
    UNUSED(sql_client);
    UNUSED(allocator);
    ObTablegroupSchema tg_schema;
    tablegroup_schema = &tg_schema;
    return OB_SUCCESS;
  }
};

class TestSchemaCache : public ::testing::Test {
public:
  virtual void SetUp();
  virtual void TearDown();

private:
  GetBatchSchemasFuncs0 get_batch_schemas_funcs0;
  GetBatchSchemasFuncs1 get_batch_schemas_funcs1;
};

void TestSchemaCache::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestSchemaCache::SetUp()
{
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
}

TEST_F(TestSchemaCache, schema_key)
{
  int ret = OB_SUCCESS;
  // hash && operator ==
  ObSchemaCacheKey cache_key_a(TENANT_SCHEMA, 1, 1);
  ObSchemaCacheKey cache_key_b(TENANT_SCHEMA, 1, 1);
  ObSchemaCacheKey cache_key_c(TENANT_SCHEMA, 2, 1);
  ASSERT_TRUE(cache_key_a.hash() == cache_key_b.hash());
  ASSERT_TRUE(cache_key_a == cache_key_b);
  ASSERT_FALSE(cache_key_a.hash() == cache_key_c.hash());
  ASSERT_FALSE(cache_key_a == cache_key_c);
  // get_tenant_id
  ASSERT_EQ(OB_SYS_TENANT_ID, cache_key_a.get_tenant_id());
  // size
  ASSERT_EQ(sizeof(ObSchemaCacheKey), cache_key_a.size());
  // deep_copy
  char* buf = new char[cache_key_a.size()];
  ObIKVCacheKey* tmp_cache_key = NULL;
  ret = cache_key_a.deep_copy(buf, cache_key_a.size(), tmp_cache_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != tmp_cache_key);
  ASSERT_EQ(cache_key_a, *static_cast<ObSchemaCacheKey*>(tmp_cache_key));
  // to_string
  LOG_INFO("test schema key to_string", K(cache_key_a));
}

TEST_F(TestSchemaCache, schema_value)
{
  int ret = OB_SUCCESS;
#define DEEP_COPY(xx_schema, SCHEMA, SCHEMA_TYPE_ENUM, SCHEMA_TYPE)                                                  \
  {                                                                                                                  \
    ObSchemaCacheValue cache_value(SCHEMA_TYPE_ENUM, &xx_schema);                                                    \
    LOG_INFO("test schema value to_string", K(cache_value));                                                         \
    ASSERT_EQ(sizeof(ObSchemaCacheValue) + xx_schema.get_convert_size() + sizeof(ObDataBuffer), cache_value.size()); \
    char* buf = new char[cache_value.size()];                                                                        \
    ObIKVCacheValue* tmp_cache_value = NULL;                                                                         \
    ret = cache_value.deep_copy(buf, cache_value.size(), tmp_cache_value);                                           \
    ASSERT_EQ(OB_SUCCESS, ret);                                                                                      \
    ASSERT_TRUE(NULL != tmp_cache_value);                                                                            \
    const ObSchemaCacheValue* tmp_schema_cache_value = static_cast<ObSchemaCacheValue*>(tmp_cache_value);            \
    ASSERT_TRUE(SchemaTestUtils::equal_##SCHEMA##_schema(                                                            \
        xx_schema, *static_cast<SCHEMA_TYPE*>(tmp_schema_cache_value->schema_)));                                    \
  }
  // tenant
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(1);
  tenant_schema.set_tenant_name("tenant");
  DEEP_COPY(tenant_schema, tenant, TENANT_SCHEMA, ObTenantSchema);
  // user
  ObUserInfo user_schema;
  user_schema.set_tenant_id(1);
  user_schema.set_user_id(combine_id(1, 1));
  user_schema.set_user_name("user");
  user_schema.set_host(OB_DEFAULT_HOST_NAME);
  DEEP_COPY(user_schema, user, USER_SCHEMA, ObUserInfo);
  // database
  ObDatabaseSchema database_schema;
  database_schema.set_tenant_id(1);
  database_schema.set_database_id(combine_id(1, 1));
  database_schema.set_database_name("database");
  DEEP_COPY(database_schema, database, DATABASE_SCHEMA, ObDatabaseSchema);
  // tablegroup
  ObTablegroupSchema tablegroup_schema;
  tablegroup_schema.set_tenant_id(1);
  tablegroup_schema.set_tablegroup_id(combine_id(1, 1));
  tablegroup_schema.set_tablegroup_name("tablegroup");
  DEEP_COPY(tablegroup_schema, tablegroup, TABLEGROUP_SCHEMA, ObTablegroupSchema);
  // table
  ObTableSchema table_schema;
  table_schema.set_tenant_id(1);
  table_schema.set_database_id(combine_id(1, 1));
  table_schema.set_table_id(combine_id(1, 1));
  table_schema.set_table_name("table");
  DEEP_COPY(table_schema, table, TABLE_SCHEMA, ObTableSchema);
  // outline
  ObOutlineInfo outline_schema;
  outline_schema.set_tenant_id(1);
  outline_schema.set_database_id(combine_id(1, 1));
  outline_schema.set_outline_id(combine_id(1, 1));
  outline_schema.set_name("outline");
  outline_schema.set_signature("sig");
  DEEP_COPY(outline_schema, outline, OUTLINE_SCHEMA, ObOutlineInfo);
}

TEST_F(TestSchemaCache, is_valid_key)
{
  ObSchemaCache schema_cache;
  ASSERT_EQ(OB_SUCCESS, schema_cache.init());
  ASSERT_FALSE(schema_cache.is_valid_key(OB_MAX_SCHEMA, OB_INVALID_VERSION, -1));
  ASSERT_FALSE(schema_cache.is_valid_key(TABLE_SCHEMA, OB_INVALID_VERSION, -1));
  ASSERT_FALSE(schema_cache.is_valid_key(TABLE_SCHEMA, 1, -1));
  ASSERT_TRUE(schema_cache.is_valid_key(TABLE_SCHEMA, 1, 1));
}

TEST_F(TestSchemaCache, need_use_sys_cache)
{
  ObSchemaCache schema_cache;
  ASSERT_EQ(OB_SUCCESS, schema_cache.init());
  ObSchemaCacheKey cache_key;
  ASSERT_TRUE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TENANT_SCHEMA, 1, 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TENANT_SCHEMA, 2, 1)));
  ASSERT_TRUE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(USER_SCHEMA, combine_id(1, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(USER_SCHEMA, combine_id(2, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(DATABASE_SCHEMA, combine_id(1, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(DATABASE_SCHEMA, combine_id(2, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TABLEGROUP_SCHEMA, combine_id(1, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TABLEGROUP_SCHEMA, combine_id(2, 1), 1)));
  ASSERT_TRUE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TABLE_SCHEMA, combine_id(1, 1), 1)));
  ASSERT_TRUE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TABLE_SCHEMA, combine_id(1, 50000), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TABLE_SCHEMA, combine_id(1, 50001), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(TABLE_SCHEMA, combine_id(2, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(OUTLINE_SCHEMA, combine_id(1, 1), 1)));
  ASSERT_FALSE(schema_cache.need_use_sys_cache(cache_key = ObSchemaCacheKey(OUTLINE_SCHEMA, combine_id(1, 2), 1)));
}

TEST_F(TestSchemaCache, schema_cache)
{
  int ret = OB_SUCCESS;
  ObSchemaCache schema_cache;
  ObKVCacheHandle handle;
  const ObSchema* dst_schema = NULL;
  // not init when get
  ret = schema_cache.get_schema(TENANT_SCHEMA, 1, 1, handle, dst_schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ASSERT_EQ(OB_SUCCESS, schema_cache.init());
  // invalid arg when get
  ret = schema_cache.get_schema(OB_MAX_SCHEMA, 1, 1, handle, dst_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_cache.get_schema(TENANT_SCHEMA, OB_INVALID_ID, 1, handle, dst_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_cache.get_schema(TENANT_SCHEMA, 1, -1, handle, dst_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // not exist
  ret = schema_cache.get_schema(TENANT_SCHEMA, 1, 1, handle, dst_schema);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_TRUE(NULL == dst_schema);
  // sys cache
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(1);
  tenant_schema.set_tenant_name("fds");
  tenant_schema.set_schema_version(1);
  // not init when put
  schema_cache.is_inited_ = false;
  ret = schema_cache.put_schema(TENANT_SCHEMA, 1, 1, tenant_schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  schema_cache.is_inited_ = true;
  // invalid arg when put
  ret = schema_cache.put_schema(OB_MAX_SCHEMA, 1, 1, tenant_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_cache.put_schema(TENANT_SCHEMA, OB_INVALID_ID, 1, tenant_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_cache.put_schema(TENANT_SCHEMA, 1, -1, tenant_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_cache.put_schema(TENANT_SCHEMA, 1, 1, tenant_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_cache.get_schema(TENANT_SCHEMA, 1, 1, handle, dst_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != dst_schema);
  ASSERT_TRUE(SchemaTestUtils::equal_tenant_schema(tenant_schema, *static_cast<const ObTenantSchema*>(dst_schema)));
  // no sys cache
  ret = schema_cache.get_schema(DATABASE_SCHEMA, combine_id(1, 1), 1, handle, dst_schema);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_TRUE(NULL == dst_schema);
  ObDatabaseSchema db_schema;
  db_schema.set_tenant_id(1);
  db_schema.set_database_id(combine_id(1, 1));
  db_schema.set_database_name("fds");
  db_schema.set_schema_version(1);
  ret = schema_cache.put_schema(DATABASE_SCHEMA, combine_id(1, 1), 1, db_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_cache.get_schema(DATABASE_SCHEMA, combine_id(1, 1), 1, handle, dst_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != dst_schema);
  ASSERT_TRUE(SchemaTestUtils::equal_database_schema(db_schema, *static_cast<const ObDatabaseSchema*>(dst_schema)));
}

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
class MockSchemaServiceSQLImpl : public ObSchemaServiceSQLImpl {
public:
  MOCK_METHOD4(get_batch_tenants, int(const int64_t, ObArray<uint64_t>&, ObISQLClient&, ObIArray<ObTenantSchema>&));
  MOCK_METHOD4(get_batch_users, int(const int64_t, ObArray<uint64_t>&, ObISQLClient&, ObIArray<ObUserInfo>&));
  MOCK_METHOD4(get_batch_databases, int(const int64_t, ObArray<uint64_t>&, ObISQLClient&, ObIArray<ObDatabaseSchema>&));
  MOCK_METHOD4(
      get_batch_tablegroups, int(const int64_t, ObArray<uint64_t>&, ObISQLClient&, ObIArray<ObTablegroupSchema>&));
  MOCK_METHOD4(get_batch_outlines, int(const int64_t, ObArray<uint64_t>&, ObISQLClient&, ObIArray<ObOutlineInfo>&));
  MOCK_METHOD5(get_table_schema, int(const uint64_t, const int64_t, ObISQLClient&, ObIAllocator&, ObTableSchema*&));
  MOCK_METHOD5(
      get_tablegroup_schema, int(const uint64_t, const int64_t, ObISQLClient&, ObIAllocator&, ObTablegroupSchema*&));
  virtual int can_read_schema_version(int64_t expected_version)
  {
    UNUSED(expected_version);
    return common::OB_SUCCESS;
  }
};

TEST_F(TestSchemaCache, schema_fetcher)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSchemaFetcher schema_fetcher;
  MockSchemaServiceSQLImpl schema_service;
  ObSchema* schema = NULL;
  // mock sql_clent for init
  ObISQLClient* sql_client = reinterpret_cast<ObISQLClient*>(this);

  // schema_fetcher is not inited
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, OB_INVALID_ID, -1, allocator, schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = schema_fetcher.fetch_schema(USER_SCHEMA, OB_INVALID_ID, -1, allocator, schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = schema_fetcher.fetch_schema(DATABASE_SCHEMA, OB_INVALID_ID, -1, allocator, schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = schema_fetcher.fetch_schema(TABLEGROUP_SCHEMA, OB_INVALID_ID, -1, allocator, schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = schema_fetcher.fetch_schema(TABLE_SCHEMA, OB_INVALID_ID, -1, allocator, schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = schema_fetcher.fetch_schema(OUTLINE_SCHEMA, OB_INVALID_ID, -1, allocator, schema);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);

  ret = schema_fetcher.init(NULL, sql_client);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_fetcher.init(&schema_service, NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = schema_fetcher.init(&schema_service, sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  // invalid arg
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, OB_INVALID_ID, 1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, 1, -1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(USER_SCHEMA, OB_INVALID_ID, 1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(USER_SCHEMA, 1, -1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(DATABASE_SCHEMA, OB_INVALID_ID, 1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(DATABASE_SCHEMA, 1, -1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(TABLEGROUP_SCHEMA, OB_INVALID_ID, 1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(TABLEGROUP_SCHEMA, 1, -1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(TABLE_SCHEMA, OB_INVALID_ID, 1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(TABLE_SCHEMA, 1, -1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(OUTLINE_SCHEMA, OB_INVALID_ID, 1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = schema_fetcher.fetch_schema(OUTLINE_SCHEMA, 1, -1, allocator, schema);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // schema_serice's get batch funcs failed
  ON_CALL(schema_service, get_batch_tenants(_, _, _, _)).WillByDefault(Return(OB_ERROR));
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(ret, OB_ERROR);
  ON_CALL(schema_service, get_batch_users(_, _, _, _)).WillByDefault(Return(OB_ERROR));
  ret = schema_fetcher.fetch_schema(USER_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(ret, OB_ERROR);
  ON_CALL(schema_service, get_batch_databases(_, _, _, _)).WillByDefault(Return(OB_ERROR));
  ret = schema_fetcher.fetch_schema(DATABASE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(ret, OB_ERROR);
  ON_CALL(schema_service, get_tablegroup_schema(_, _, _, _, _)).WillByDefault(Return(OB_ERROR));
  ret = schema_fetcher.fetch_schema(TABLEGROUP_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(ret, OB_ERROR);
  ON_CALL(schema_service, get_table_schema(_, _, _, _, _)).WillByDefault(Return(OB_ERROR));
  ret = schema_fetcher.fetch_schema(TABLE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(ret, OB_ERROR);
  ON_CALL(schema_service, get_batch_outlines(_, _, _, _)).WillByDefault(Return(OB_ERROR));
  ret = schema_fetcher.fetch_schema(OUTLINE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(ret, OB_ERROR);

  // schema_service's get batch func succeed, but no schema return
  ON_CALL(schema_service, get_batch_databases(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs0, &GetBatchSchemasFuncs0::get_batch_databases));
  ret = schema_fetcher.fetch_schema(DATABASE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ON_CALL(schema_service, get_tablegroup_schema(_, _, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs0, &GetBatchSchemasFuncs0::get_tablegroup_schema));
  ret = schema_fetcher.fetch_schema(TABLEGROUP_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ON_CALL(schema_service, get_batch_users(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs0, &GetBatchSchemasFuncs0::get_batch_users));
  ret = schema_fetcher.fetch_schema(USER_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ON_CALL(schema_service, get_batch_tenants(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs0, &GetBatchSchemasFuncs0::get_batch_tenants));
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ON_CALL(schema_service, get_batch_outlines(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs0, &GetBatchSchemasFuncs0::get_batch_outlines));
  ret = schema_fetcher.fetch_schema(OUTLINE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ON_CALL(schema_service, get_table_schema(_, _, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs0, &GetBatchSchemasFuncs0::get_table_schema));
  ret = schema_fetcher.fetch_schema(TABLE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  // schema_service's get batch func succeed
  ON_CALL(schema_service, get_batch_tenants(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_batch_tenants));
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != schema);
  ON_CALL(schema_service, get_batch_users(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_batch_users));
  ret = schema_fetcher.fetch_schema(USER_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != schema);
  ON_CALL(schema_service, get_batch_databases(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_batch_databases));
  ret = schema_fetcher.fetch_schema(DATABASE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != schema);
  ON_CALL(schema_service, get_tablegroup_schema(_, _, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_tablegroup_schema));
  ret = schema_fetcher.fetch_schema(TABLEGROUP_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != schema);
  ON_CALL(schema_service, get_table_schema(_, _, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_table_schema));
  ret = schema_fetcher.fetch_schema(TABLE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != schema);
  ON_CALL(schema_service, get_batch_outlines(_, _, _, _))
      .WillByDefault(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_batch_outlines));
  ret = schema_fetcher.fetch_schema(OUTLINE_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != schema);
}

TEST_F(TestSchemaCache, schema_fetcher_connect_error)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSchemaFetcher schema_fetcher;
  MockSchemaServiceSQLImpl schema_service;
  ObSchema* schema = NULL;
  // mock sql_client for init
  ObISQLClient* sql_client = reinterpret_cast<ObISQLClient*>(this);

  ret = schema_fetcher.init(&schema_service, sql_client);
  ASSERT_EQ(OB_SUCCESS, ret);

  // schema_service return OB_CONNECT_ERROR
  // EXPECT_CALL(schema_service,
  // get_batch_tenants(_,_,_,_)).Times(2).WillOnce(Return(OB_CONNECT_ERROR)).WillOnce(Return(OB_SUCCESS));
  EXPECT_CALL(schema_service, get_batch_tenants(_, _, _, _))
      .Times(2)
      .WillOnce(Return(OB_CONNECT_ERROR))
      .WillOnce(Invoke(&get_batch_schemas_funcs1, &GetBatchSchemasFuncs1::get_batch_tenants));
  ret = schema_fetcher.fetch_schema(TENANT_SCHEMA, 1, 1, allocator, schema);
  ASSERT_EQ(OB_SUCCESS, ret);
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_schema_cache.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
