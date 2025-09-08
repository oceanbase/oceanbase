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

#include "share/catalog/ob_catalog_utils.h"
#include "share/schema/ob_iceberg_table_schema.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/odps/ob_odps_table_metadata.h"

#include <s2/base/casts.h>

namespace oceanbase
{
namespace share
{

bool ObLakeTableMetadataCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObLakeTableMetadataCacheKey &other_key
      = reinterpret_cast<const ObLakeTableMetadataCacheKey &>(other);
  return this->tenant_id_ == other_key.tenant_id_ && this->catalog_id_ == other_key.catalog_id_
         && this->namespace_name_ == other_key.namespace_name_
         && this->table_name_ == other_key.table_name_;
}

int64_t ObLakeTableMetadataCacheKey::size() const
{
  return sizeof(*this) + namespace_name_.length() + table_name_.length();
}

int ObLakeTableMetadataCacheKey::deep_copy(char *buf,
                                           const int64_t buf_len,
                                           ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(buf, buf_len);
  ObLakeTableMetadataCacheKey *new_value = NULL;
  if (OB_ISNULL(new_value = OB_NEWx(ObLakeTableMetadataCacheKey, &allocator))) {
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

uint64_t ObLakeTableMetadataCacheKey::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = murmurhash(&tenant_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(&catalog_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(namespace_name_.ptr(), namespace_name_.length(), hash_ret);
  hash_ret = murmurhash(table_name_.ptr(), table_name_.length(), hash_ret);
  return hash_ret;
}

int64_t ObLakeTableMetadataCacheValue::size() const
{
  return sizeof(ObLakeTableMetadataCacheValue)
         + (NULL != lake_table_metadata_ ? lake_table_metadata_->get_convert_size() : 0)
         + sizeof(ObDataBuffer);
}

int ObLakeTableMetadataCacheValue::deep_copy(char *buf,
                                             const int64_t buf_len,
                                             ObIKVCacheValue *&value) const
{
#define DEEP_COPY_LAKE_TABLE_METADATA(LAKE_TABLE_METADATA)                                         \
  pvalue = new (buf) ObLakeTableMetadataCacheValue();                                              \
  const LAKE_TABLE_METADATA *old_var                                                               \
      = static_cast<const LAKE_TABLE_METADATA *>(lake_table_metadata_);                            \
  LAKE_TABLE_METADATA *new_var = NULL;                                                             \
  if (OB_FAIL(ObCatalogUtils::deep_copy_lake_table_metadata(buf + sizeof(*this),                   \
                                                            *old_var,                              \
                                                            new_var))) {                           \
    LOG_WARN("deep copy lake table meta data failed", K(ret));                                     \
  } else {                                                                                         \
    pvalue->lake_table_metadata_ = new_var;                                                        \
  }

  int ret = OB_SUCCESS;

  if (OB_ISNULL(lake_table_metadata_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid init state", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf_len), K(size()));
  } else {
    ObLakeTableMetadataCacheValue *pvalue = NULL;
    switch (lake_table_metadata_->get_format_type()) {
      case ObLakeTableFormat::ICEBERG: {
        DEEP_COPY_LAKE_TABLE_METADATA(sql::iceberg::ObIcebergTableMetadata);
        break;
      }
      case ObLakeTableFormat::HIVE: {
        DEEP_COPY_LAKE_TABLE_METADATA(sql::hive::ObHiveTableMetadata);
        break;
      }
      case ObLakeTableFormat::ODPS: {
        DEEP_COPY_LAKE_TABLE_METADATA(sql::odps::ObODPSTableMetadata);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid format type", K(lake_table_metadata_->get_format_type()), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      value = pvalue;
    }
  }
#undef DEEP_COPY_LAKE_TABLE_METADATA
  return ret;
}

ObCachedCatalogSchemaMgr &ObCachedCatalogSchemaMgr::get_instance()
{
  static ObCachedCatalogSchemaMgr instance_;
  return instance_;
}

int ObCachedCatalogSchemaMgr::init()
{
  int ret = OB_SUCCESS;
  OZ(lake_metadata_cache_.init("catalog_lake_metadata_cache"));
  // OZ(file_cache_.init("catalog_files_cache"));
  return ret;
}

// 正常情况下
// schema_cache_ 调用 get() 获取 Schema -> 检查 Schema 是否过期
// 如果 Schema 在 Cache 中不存在，执行如下步骤：
// 加锁->再次从 schema_cache_.get() 尝试获取->检查 Schema 是否过期->从远端拉取 Schema -> 写入
// schema_cache_ 这样意味着这，如果 Schema 在 schema_cache_
// 中不存在的话，一定会发起两次是否过期检查，也就是两次 PRC。 所以我们扩大锁的临界区，减少检查
// Schema 过期的次数。
int ObCachedCatalogSchemaMgr::get_lake_table_metadata(ObIAllocator &allocator,
                                                      ObCatalogMetaGetter *meta_getter,
                                                      const uint64_t tenant_id,
                                                      const uint64_t catalog_id,
                                                      const uint64_t database_id,
                                                      const uint64_t table_id,
                                                      const common::ObString &ns_name,
                                                      const common::ObString &tbl_name,
                                                      const ObNameCaseMode case_mode,
                                                      ObILakeTableMetadata *&lake_table_metadata)
{
#define ASSIGN_VALUE_FROM_CACHE(TABLE_METADATA)                                                    \
  TABLE_METADATA *metadata = OB_NEWx(TABLE_METADATA, &allocator, allocator);                       \
  if (OB_ISNULL(metadata)) {                                                                       \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                                               \
    LOG_WARN("failed to allocate memory for lake table metadata", K(ret));                         \
  } else if (OB_FAIL(metadata->assign(                                                             \
                 *down_cast<const TABLE_METADATA *>(cached_value->lake_table_metadata_)))) {       \
    LOG_WARN("failed to assign hive table metadata", K(ret));                                      \
  } else {                                                                                         \
    lake_table_metadata = metadata;                                                                \
  }

  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;

  ObLakeTableMetadataCacheKey cache_key;
  cache_key.tenant_id_ = tenant_id;
  cache_key.catalog_id_ = catalog_id;
  cache_key.namespace_name_ = ns_name;
  cache_key.table_name_ = tbl_name;

  const int64_t bucket_id = cache_key.hash() % LOAD_CACHE_LOCK_CNT;
  int64_t total_wait_secs = 0;

  while (OB_FAIL(fill_cache_locks_[bucket_id].lock(LOCK_TIMEOUT)) && OB_TIMEOUT == ret
         && OB_SUCC(THIS_WORKER.check_status())) {
    total_wait_secs += (LOCK_TIMEOUT / 1000000);
    LOG_WARN("ObCachedCatalogSchemaMgr cache wait", K(total_wait_secs));
  }
  // 进入临界区

  lake_table_metadata = NULL;
  const ObLakeTableMetadataCacheValue *cached_value = NULL;

  bool is_cache_valid = false;
  int64_t latest_schema_version = 0;
  if (OB_FAIL(lake_metadata_cache_.get(cache_key, cached_value, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get from KVCache", K(ret), K(cache_key));
    }
  } else if (OB_ISNULL(cached_value) || OB_ISNULL(cached_value->lake_table_metadata_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get error cached value", K(ret), K(cache_key));
  } else if (OB_FAIL(meta_getter->fetch_latest_table_schema_version(tenant_id,
                                                                    catalog_id,
                                                                    ns_name,
                                                                    tbl_name,
                                                                    case_mode,
                                                                    latest_schema_version))) {
    LOG_WARN("failed to fetch latest table schema version", K(ret));
  } else {
    int64_t cached_table_metadata_version
        = cached_value->lake_table_metadata_->lake_table_metadata_version_;
    is_cache_valid = (cached_table_metadata_version == latest_schema_version);
    if (is_cache_valid) {
      switch (cached_value->lake_table_metadata_->get_format_type()) {
        case ObLakeTableFormat::HIVE: {
          ASSIGN_VALUE_FROM_CACHE(sql::hive::ObHiveTableMetadata);
          break;
        }
        case ObLakeTableFormat::ODPS: {
          ASSIGN_VALUE_FROM_CACHE(sql::odps::ObODPSTableMetadata);
          break;
        }
        case ObLakeTableFormat::ICEBERG: {
          ASSIGN_VALUE_FROM_CACHE(sql::iceberg::ObIcebergTableMetadata)
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unreachable code", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && is_cache_valid && OB_NOT_NULL(lake_table_metadata)) {
      // 需要重新分配临时生成的 database_id 和 table_id
      lake_table_metadata->database_id_ = database_id;
      lake_table_metadata->table_id_ = table_id;
    }
  }

  // cache not found, fetch from remote
  if ((OB_SUCC(ret) && !is_cache_valid)) {
    lake_table_metadata = NULL;
    if (OB_FAIL(meta_getter->fetch_lake_table_metadata(allocator,
                                                       tenant_id,
                                                       catalog_id,
                                                       database_id,
                                                       table_id,
                                                       ns_name,
                                                       tbl_name,
                                                       case_mode,
                                                       lake_table_metadata))) {
      LOG_WARN("failed to fetch lake table metadata", K(ret));
    } else if (OB_ISNULL(lake_table_metadata)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lake_table_metadata is null", K(ret));
    } else if (ObLakeTableFormat::ICEBERG == lake_table_metadata->get_format_type()
               || ObLakeTableFormat::HIVE == lake_table_metadata->get_format_type()) {
      // todo
      // do not cache now
    } else {
      ObLakeTableMetadataCacheValue tmp_cache_value(lake_table_metadata);
      OZ(lake_metadata_cache_.put(cache_key, tmp_cache_value));
    }
  }

  if (fill_cache_locks_[bucket_id].self_locked()) {
    fill_cache_locks_[bucket_id].unlock();
  }
#undef ASSIGN_VALUE_FROM_CACHE
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
                                                      const uint64_t database_id,
                                                      const common::ObString &ns_name,
                                                      const ObNameCaseMode case_mode,
                                                      share::schema::ObDatabaseSchema *&database_schema)
{
  return delegate_.fetch_namespace_schema(tenant_id,
                                          catalog_id,
                                          database_id,
                                          ns_name,
                                          case_mode,
                                          database_schema);
}

int ObCachedCatalogMetaGetter::fetch_lake_table_metadata(ObIAllocator &allocator,
                                                         const uint64_t tenant_id,
                                                         const uint64_t catalog_id,
                                                         const uint64_t database_id,
                                                         const uint64_t table_id,
                                                         const common::ObString &ns_name,
                                                         const common::ObString &tbl_name,
                                                         const ObNameCaseMode case_mode,
                                                         ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || !is_external_object_id(database_id) || !is_external_object_id(table_id)
                  || ns_name.empty() || tbl_name.empty() || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_id),
             K(table_id),
             K(ns_name),
             K(tbl_name),
             K(case_mode));
  } else if (OB_FAIL(ObCachedCatalogSchemaMgr::get_instance().get_lake_table_metadata(
                 allocator,
                 &delegate_,
                 tenant_id,
                 catalog_id,
                 database_id,
                 table_id,
                 ns_name,
                 tbl_name,
                 case_mode,
                 table_metadata))) {
    LOG_WARN("get cached table schema failed",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(ns_name),
             K(tbl_name),
             K(case_mode));
  }
  return ret;
}

int ObCachedCatalogMetaGetter::fetch_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  return delegate_.fetch_table_statistics(allocator,
                                          table_metadata,
                                          partition_values,
                                          column_names,
                                          external_table_stat,
                                          external_table_column_stats);
}

int ObCachedCatalogMetaGetter::fetch_partitions(ObIAllocator &allocator,
                                                const ObILakeTableMetadata *table_metadata,
                                                Partitions &partitions)
{
  return delegate_.fetch_partitions(allocator, table_metadata, partitions);
}

int ObCachedCatalogMetaGetter::get_cache_refresh_interval_sec(
    const ObILakeTableMetadata *table_metadata,
    int64_t &sec)
{
  return delegate_.get_cache_refresh_interval_sec(table_metadata, sec);
}

} // namespace share
} // namespace oceanbase