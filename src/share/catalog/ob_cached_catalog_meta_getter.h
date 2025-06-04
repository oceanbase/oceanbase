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

#ifndef __SHARE_OB_CACHED_CATALOG_META_GETTER_H__
#define __SHARE_OB_CACHED_CATALOG_META_GETTER_H__

#include "share/catalog/ob_catalog_meta_getter.h"
#include "share/catalog/ob_external_catalog.h"

namespace oceanbase
{
namespace share
{

class ObCatalogSchemaCacheKey final : public common::ObIKVCacheKey
{
public:
  ObCatalogSchemaCacheKey() : tenant_id_(OB_INVALID), catalog_id_(OB_INVALID) {}
  ~ObCatalogSchemaCacheKey() override {}
  bool operator==(const common::ObIKVCacheKey &other) const override;
  uint64_t hash() const override;
  uint64_t get_tenant_id() const override { return tenant_id_; }
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(namespace_name_), K(table_name_));

public:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  common::ObString namespace_name_;
  common::ObString table_name_;
};

class ObCachedCatalogSchemaMgr
{
public:
  ObCachedCatalogSchemaMgr() = default;
  int init();
  static ObCachedCatalogSchemaMgr &get_instance();
  int get_table_schema(ObCatalogMetaGetter *meta_getter,
                       const uint64_t tenant_id,
                       const uint64_t catalog_id,
                       const common::ObString &ns_name,
                       const common::ObString &tbl_name,
                       const ObNameCaseMode case_mode,
                       share::schema::ObTableSchema &table_schema);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCachedCatalogSchemaMgr);
  int check_table_schema_cache_valid_(ObCatalogMetaGetter *meta_getter,
                                      const int64_t cached_time,
                                      const uint64_t tenant_id,
                                      const uint64_t catalog_id,
                                      const common::ObString &ns_name,
                                      const common::ObString &tbl_name,
                                      const ObNameCaseMode case_mode,
                                      bool &is_valid);
  static constexpr int64_t LOAD_CACHE_LOCK_CNT = 16;
  static const int64_t LOCK_TIMEOUT = 2 * 1000000L;
  common::ObSpinLock fill_cache_locks_[LOAD_CACHE_LOCK_CNT];
  common::ObKVCache<ObCatalogSchemaCacheKey, schema::ObSchemaCacheValue> schema_cache_;
  // common::ObKVCache<ObCatalogSchemaCacheKey, ObExternalTableFiles> file_cache_;
};

// 判断 Cache 是否过期的逻辑，只做在 ObCachingCatalogMetaGetter 这一层，不要侵入内部 Catalog 的 API
class ObCachedCatalogMetaGetter final : public ObICatalogMetaGetter
{
public:
  ObCachedCatalogMetaGetter(ObSchemaGetterGuard &schema_getter_guard, ObIAllocator &allocator)
      : delegate_(ObCatalogMetaGetter{schema_getter_guard, allocator})
  {
  }

  ~ObCachedCatalogMetaGetter() override {}

  int list_namespace_names(const uint64_t tenant_id, const uint64_t catalog_id, common::ObIArray<common::ObString> &ns_names) override;

  int list_table_names(const uint64_t tenant_id,
                       const uint64_t catalog_id,
                       const common::ObString &ns_name,
                       const ObNameCaseMode case_mode,
                       common::ObIArray<common::ObString> &tbl_names) override;

  int fetch_namespace_schema(const uint64_t tenant_id,
                             const uint64_t catalog_id,
                             const common::ObString &ns_name,
                             const ObNameCaseMode case_mode,
                             share::schema::ObDatabaseSchema &database_schema) override;

  int fetch_table_schema(const uint64_t tenant_id,
                         const uint64_t catalog_id,
                         const common::ObString &ns_name,
                         const common::ObString &tbl_name,
                         const ObNameCaseMode case_mode,
                         share::schema::ObTableSchema &table_schema) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCachedCatalogMetaGetter);
  ObCatalogMetaGetter delegate_;
};

} // namespace share
} // namespace oceanbase

#endif //__SHARE_OB_CACHED_CATALOG_META_GETTER_H__
