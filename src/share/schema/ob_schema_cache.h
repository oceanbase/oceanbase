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

#ifndef OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_
#define OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_udf.h"

namespace oceanbase {
namespace common {
class ObKVCacheHandle;
class ObISQLClient;
class ObIAllocator;
}  // namespace common
namespace share {
namespace schema {

class ObSchemaCacheKey : public common::ObIKVCacheKey {
public:
  ObSchemaCacheKey();
  ObSchemaCacheKey(const ObSchemaType schema_type, const uint64_t schema_id, const uint64_t schema_version);
  virtual ~ObSchemaCacheKey()
  {}
  virtual uint64_t get_tenant_id() const;
  virtual bool operator==(const ObIKVCacheKey& other) const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, int64_t buf_len, ObIKVCacheKey*& key) const;
  TO_STRING_KV(K_(schema_type), K_(schema_id), K_(schema_version));

  ObSchemaType schema_type_;
  uint64_t schema_id_;
  uint64_t schema_version_;
};

class ObSchemaCacheValue : public common::ObIKVCacheValue {
public:
  ObSchemaCacheValue();
  ObSchemaCacheValue(ObSchemaType schema_type, const ObSchema* schema);
  virtual ~ObSchemaCacheValue()
  {}
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, int64_t buf_len, ObIKVCacheValue*& value) const;
  TO_STRING_KV(K_(schema_type), KP_(schema));

  ObSchemaType schema_type_;
  const ObSchema* schema_;
};

class ObSchemaCache {
  static const int64_t OB_SCHEMA_CACHE_SYS_CACHE_MAP_BUCKET_NUM = 512;

public:
  ObSchemaCache();
  virtual ~ObSchemaCache();

  int init();
  int get_schema(ObSchemaType schema_type, uint64_t schema_id, int64_t schema_version, common::ObKVCacheHandle& handle,
      const ObSchema*& schema);
  int put_schema(ObSchemaType schema_type, uint64_t schema_id, int64_t schema_version, const ObSchema& schema,
      const bool is_force = false);
  int put_and_fetch_schema(ObSchemaType schema_type, uint64_t schema_id, int64_t schema_version, const ObSchema& schema,
      common::ObKVCacheHandle& handle, const ObSchema*& new_schema);
  const ObTableSchema* get_all_core_table() const;

private:
  bool check_inner_stat() const;
  bool is_valid_key(ObSchemaType schema_type, uint64_t schema_id, int64_t schema_version) const;
  bool need_use_sys_cache(const ObSchemaCacheKey& cache_key) const;
  int init_all_core_table();
  int put_sys_schema(const ObSchemaCacheKey& cache_key, const ObSchema& schema, const bool is_force);

private:
  typedef common::hash::ObHashMap<ObSchemaCacheKey, const ObSchemaCacheValue*, common::hash::ReadWriteDefendMode>
      NoSwapCache;
  typedef common::ObKVCache<ObSchemaCacheKey, ObSchemaCacheValue> KVCache;

  lib::MemoryContext* mem_context_;
  NoSwapCache sys_cache_;
  KVCache cache_;
  bool is_inited_;
  ObTableSchema all_core_table_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaCache);
};

class ObSchemaService;
class ObTableSchema;
class ObSchemaFetcher {
public:
  ObSchemaFetcher();
  virtual ~ObSchemaFetcher()
  {}
  bool check_inner_stat() const;
  int init(ObSchemaService* schema_service, common::ObISQLClient* sql_client);
  int fetch_schema(ObSchemaType schema_type, const ObRefreshSchemaStatus& schema_status, uint64_t schema_id,
      int64_t schema_version, common::ObIAllocator& allocator, ObSchema*& schema);

private:
  int fetch_tenant_schema(
      uint64_t tenant_id, int64_t schema_version, common::ObIAllocator& allocator, ObTenantSchema*& tenant_schema);
  int fetch_sys_variable_schema(const ObRefreshSchemaStatus& schema_status, uint64_t tenant_id, int64_t schema_version,
      common::ObIAllocator& allocator, ObSysVariableSchema*& sys_variable_schema);
  int fetch_database_schema(const ObRefreshSchemaStatus& schema_status, uint64_t database_id, int64_t schema_version,
      common::ObIAllocator& allocator, ObDatabaseSchema*& database_schema);
  int fetch_tablegroup_schema(const ObRefreshSchemaStatus& schema_status, uint64_t tablegroup_id,
      int64_t schema_version, common::ObIAllocator& allocator, ObTablegroupSchema*& tablegroup_schema);
  int fetch_table_schema(const ObRefreshSchemaStatus& schema_status, uint64_t table_id, int64_t schema_version,
      common::ObIAllocator& allocator, ObTableSchema*& table_schema);
  int fetch_table_schema(const ObRefreshSchemaStatus& schema_status, uint64_t table_id, int64_t schema_version,
      common::ObIAllocator& allocator, ObSimpleTableSchemaV2*& table_schema);

#ifndef DEF_SCHEMA_INFO_FETCHER
#define DEF_SCHEMA_INFO_FETCHER(OBJECT_NAME, OBJECT_SCHEMA_TYPE)             \
  int fetch_##OBJECT_NAME##_info(const ObRefreshSchemaStatus& schema_status, \
      uint64_t object_id,                                                    \
      int64_t schema_version,                                                \
      common::ObIAllocator& allocator,                                       \
      OBJECT_SCHEMA_TYPE*& object_schema)

  DEF_SCHEMA_INFO_FETCHER(user, ObUserInfo);
  DEF_SCHEMA_INFO_FETCHER(outline, ObOutlineInfo);
  DEF_SCHEMA_INFO_FETCHER(synonym, ObSynonymInfo);
  DEF_SCHEMA_INFO_FETCHER(plan_baseline, ObPlanBaselineInfo);
  DEF_SCHEMA_INFO_FETCHER(udf, ObUDF);
  DEF_SCHEMA_INFO_FETCHER(sequence, ObSequenceSchema);
  DEF_SCHEMA_INFO_FETCHER(profile, ObProfileSchema);
#undef DEF_SCHEMA_INFO_FETCHER
#endif

private:
  ObSchemaService* schema_service_;
  common::ObISQLClient* sql_client_;
  bool is_inited_;
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_
