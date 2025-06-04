/**
 * Copyright (c) 2023 OceanBase
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

#include "share/catalog/ob_cached_catalog_meta_getter.h"

namespace oceanbase
{
namespace share
{

bool ObCatalogSchemaCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObCatalogSchemaCacheKey &other_key = reinterpret_cast<const ObCatalogSchemaCacheKey &>(other);
  return this->tenant_id_ == other_key.tenant_id_ && this->catalog_id_ == other_key.catalog_id_
         && this->namespace_name_ == other_key.namespace_name_ && this->table_name_ == other_key.table_name_;
}

int64_t ObCatalogSchemaCacheKey::size() const { return sizeof(*this) + namespace_name_.length() + table_name_.length(); }

int ObCatalogSchemaCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(buf, buf_len);
  ObCatalogSchemaCacheKey *new_value = NULL;
  if (OB_ISNULL(new_value = OB_NEWx(ObCatalogSchemaCacheKey, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    new_value->tenant_id_ = tenant_id_;
    new_value->catalog_id_ = catalog_id_;
    if (OB_FAIL(ob_write_string(allocator, namespace_name_, new_value->namespace_name_))) {
      LOG_WARN("failed to deep copy filter name", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, table_name_, new_value->table_name_))) {
      LOG_WARN("failed to deep copy definition", K(ret));
    } else {
      key = new_value;
    }
  }
  return ret;
}

uint64_t ObCatalogSchemaCacheKey::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = murmurhash(&tenant_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(&catalog_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(namespace_name_.ptr(), namespace_name_.length(), hash_ret);
  hash_ret = murmurhash(table_name_.ptr(), table_name_.length(), hash_ret);
  return hash_ret;
}

ObCachedCatalogSchemaMgr &ObCachedCatalogSchemaMgr::get_instance()
{
  static ObCachedCatalogSchemaMgr instance_;
  return instance_;
}

int ObCachedCatalogSchemaMgr::init()
{
  int ret = OB_SUCCESS;
  OZ(schema_cache_.init("external_catalog_schema_cache"));
  // OZ(file_cache_.init("catalog_files_cache"));
  return ret;
}

// 正常情况下
// schema_cache_ 调用 get() 获取 Schema -> 检查 Schema 是否过期
// 如果 Schema 在 Cache 中不存在，执行如下步骤：
// 加锁->再次从 schema_cache_.get() 尝试获取->检查 Schema 是否过期->从远端拉取 Schema -> 写入 schema_cache_
// 这样意味着这，如果 Schema 在 schema_cache_ 中不存在的话，一定会发起两次是否过期检查，也就是两次 PRC。
// 所以我们扩大锁的临界区，减少检查 Schema 过期的次数。
int ObCachedCatalogSchemaMgr::get_table_schema(ObCatalogMetaGetter *meta_getter,
                                               const uint64_t tenant_id,
                                               const uint64_t catalog_id,
                                               const common::ObString &ns_name,
                                               const common::ObString &tbl_name,
                                               const ObNameCaseMode case_mode,
                                               share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;

  ObCatalogSchemaCacheKey cache_key;
  cache_key.tenant_id_ = tenant_id;
  cache_key.catalog_id_ = catalog_id;
  cache_key.namespace_name_ = ns_name;
  cache_key.table_name_ = tbl_name;

  const int64_t bucket_id = cache_key.hash() % LOAD_CACHE_LOCK_CNT;
  int64_t total_wait_secs = 0;

  while (OB_FAIL(fill_cache_locks_[bucket_id].lock(LOCK_TIMEOUT)) && OB_TIMEOUT == ret && OB_SUCC(THIS_WORKER.check_status())) {
    total_wait_secs += (LOCK_TIMEOUT / 1000000);
    LOG_WARN("ObCachedCatalogSchemaMgr cache wait", K(total_wait_secs));
  }
  // 进入临界区

  const schema::ObSchemaCacheValue *cached_value = NULL;
  const ObTableSchema *cached_table_schema = NULL;

  bool is_cache_valid = false;
  if (OB_FAIL(schema_cache_.get(cache_key, cached_value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get from KVCache", K(ret), K(cache_key));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(cached_value) || OB_ISNULL(cached_value->schema_) || ObSchemaType::TABLE_SCHEMA != cached_value->schema_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get error cached value", K(ret), K(cache_key));
  } else if (OB_FALSE_IT(cached_table_schema = static_cast<const ObTableSchema *>(cached_value->schema_))) {
  } else if (OB_FAIL(check_table_schema_cache_valid_(meta_getter,
                                                     cached_table_schema->get_schema_version(),
                                                     tenant_id,
                                                     catalog_id,
                                                     ns_name,
                                                     tbl_name,
                                                     case_mode,
                                                     is_cache_valid))) {
    LOG_WARN("fail to check table cache valid", K(ret));
  } else if (is_cache_valid) {
    // cache is valid
    OZ(table_schema.assign(*cached_table_schema));
  }

  // cache not found, fetch from remote
  if ((OB_SUCC(ret) && !is_cache_valid)) {
    if (OB_FAIL(meta_getter->fetch_table_schema(tenant_id, catalog_id, ns_name, tbl_name, case_mode, table_schema))) {
      LOG_WARN("fail to fetch table schema from remote", K(ret), K(cache_key));
    } else {
      ObSchemaCacheValue tmp_cache_value{ObSchemaType::TABLE_SCHEMA, &table_schema};
      table_schema.set_schema_version(ObTimeUtil::current_time_s());
      OZ(schema_cache_.put(cache_key, tmp_cache_value, true));
    }
  }

  if (fill_cache_locks_[bucket_id].self_locked()) {
    fill_cache_locks_[bucket_id].unlock();
  }
  return ret;
}

int ObCachedCatalogSchemaMgr::check_table_schema_cache_valid_(ObCatalogMetaGetter *meta_getter,
                                                              const int64_t cached_time,
                                                              const uint64_t tenant_id,
                                                              const uint64_t catalog_id,
                                                              const common::ObString &ns_name,
                                                              const common::ObString &tbl_name,
                                                              const ObNameCaseMode case_mode,
                                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObCatalogBasicTableInfo table_info;
  if (OB_ISNULL(meta_getter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta_getter", K(ret));
  } else if (OB_FAIL(meta_getter->fetch_basic_table_info(tenant_id, catalog_id, ns_name, tbl_name, case_mode, table_info))) {
    LOG_WARN("fetch_table_basic_info failed", K(ret));
  } else if (cached_time >= table_info.last_modification_time_s) {
    is_valid = true;
  }
  return ret;
}

int ObCachedCatalogMetaGetter::list_namespace_names(const uint64_t tenant_id,
                                                    const uint64_t catalog_id,
                                                    common::ObIArray<common::ObString> &ns_names)
{
  return delegate_.list_namespace_names(tenant_id, catalog_id, ns_names);
}

int ObCachedCatalogMetaGetter::list_table_names(const uint64_t tenant_id,
                                                const uint64_t catalog_id,
                                                const common::ObString &ns_name,
                                                const ObNameCaseMode case_mode,
                                                common::ObIArray<common::ObString> &tbl_names)
{
  return delegate_.list_table_names(tenant_id, catalog_id, ns_name, case_mode, tbl_names);
}

int ObCachedCatalogMetaGetter::fetch_namespace_schema(const uint64_t tenant_id,
                                                      const uint64_t catalog_id,
                                                      const common::ObString &ns_name,
                                                      const ObNameCaseMode case_mode,
                                                      share::schema::ObDatabaseSchema &database_schema)
{
  return delegate_.fetch_namespace_schema(tenant_id, catalog_id, ns_name, case_mode, database_schema);
}

int ObCachedCatalogMetaGetter::fetch_table_schema(const uint64_t tenant_id,
                                                  const uint64_t catalog_id,
                                                  const common::ObString &ns_name,
                                                  const common::ObString &tbl_name,
                                                  const ObNameCaseMode case_mode,
                                                  share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t original_db_id = table_schema.get_database_id();
  const uint64_t original_tbl_id = table_schema.get_table_id();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id) || !is_external_object_id(original_db_id)
                  || !is_external_object_id(original_tbl_id) || ns_name.empty() || tbl_name.empty() || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id), K(ns_name), K(tbl_name), K(case_mode));
  } else if (OB_FAIL(ObCachedCatalogSchemaMgr::get_instance()
                         .get_table_schema(&delegate_, tenant_id, catalog_id, ns_name, tbl_name, case_mode, table_schema))) {
    LOG_WARN("get cached table schema failed", K(ret), K(tenant_id), K(catalog_id), K(ns_name), K(tbl_name), K(case_mode));
  } else {
    table_schema.set_database_id(original_db_id);
    table_schema.set_table_id(original_tbl_id);
  }
  return ret;
}

} // namespace share
} // namespace oceanbase