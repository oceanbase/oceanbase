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
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_trigger_info.h"
#include "share/schema/ob_udf.h"
#include "share/schema/ob_udt_info.h"
#include "share/schema/ob_schema_mgr.h"

namespace oceanbase
{
namespace common
{
class ObKVCacheHandle;
class ObISQLClient;
class ObIAllocator;
}
namespace share
{
namespace schema
{

class ObSchemaCacheKey : public common::ObIKVCacheKey
{
public:
  ObSchemaCacheKey();
  ObSchemaCacheKey(const ObSchemaType schema_type,
                   const uint64_t tenant_id,
                   const uint64_t schema_id,
                   const uint64_t schema_version);
  virtual ~ObSchemaCacheKey() {}
  virtual uint64_t get_tenant_id() const;
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t hash() const;
  virtual int hash(uint64_t &hash_value) const  { hash_value = hash(); return OB_SUCCESS; }
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheKey *&key) const;
  TO_STRING_KV(K_(schema_type),
               K_(tenant_id),
               K_(schema_id),
               K_(schema_version));

  ObSchemaType schema_type_;
  uint64_t tenant_id_;
  uint64_t schema_id_;
  uint64_t schema_version_;
};

class ObSchemaCacheValue : public common::ObIKVCacheValue
{
public:
  ObSchemaCacheValue();
  ObSchemaCacheValue(ObSchemaType schema_type,
                     const ObSchema *schema);
  virtual ~ObSchemaCacheValue() {}
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheValue *&value) const;
  TO_STRING_KV(K_(schema_type),
               KP_(schema));

  ObSchemaType schema_type_;
  const ObSchema *schema_;
};

class ObSchemaHistoryCacheValue : public common::ObIKVCacheValue
{
public:
  ObSchemaHistoryCacheValue();
  ObSchemaHistoryCacheValue(const int64_t schema_version);
  virtual ~ObSchemaHistoryCacheValue() {}
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        int64_t buf_len,
                        ObIKVCacheValue *&value) const;
  TO_STRING_KV(K_(schema_version));

  int64_t schema_version_;
};

class ObTabletCacheKey : public common::ObIKVCacheKey
{
public:
  ObTabletCacheKey();
  ObTabletCacheKey(const uint64_t tenant_id,
                   const ObTabletID &tablet_id,
                   const uint64_t schema_version);
  int init(const uint64_t tenant_id,
           const ObTabletID &tablet_id,
           const uint64_t schema_version);
  bool is_valid() const;
  virtual ~ObTabletCacheKey() {}
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  // here get_tenant_id() means tenant_id which is used for alloca memory for kvcache
  virtual uint64_t get_tenant_id() const;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheKey *&key) const;
  TO_STRING_KV(K_(tenant_id),
               K_(tablet_id),
               K_(schema_version));
private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
  int64_t schema_version_;
};

class ObTabletCacheValue : public common::ObIKVCacheValue
{
public:
  ObTabletCacheValue();
  ObTabletCacheValue(const uint64_t table_id);
  int init(const uint64_t table_id);
  virtual ~ObTabletCacheValue() {}
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheValue *&value) const;

  uint64_t get_table_id() const { return table_id_; }

  TO_STRING_KV(K_(table_id));
private:
  uint64_t table_id_;
};

class ObSchemaCache
{
  static const int64_t OB_SCHEMA_CACHE_SYS_CACHE_MAP_BUCKET_NUM = 512;
public:
  ObSchemaCache();
  virtual ~ObSchemaCache();

  int init();
  void destroy();
  int get_schema(const ObSchemaType schema_type,
                 const uint64_t tenant_id,
                 const uint64_t schema_id,
                 const int64_t schema_version,
                 common::ObKVCacheHandle &handle,
                 const ObSchema *&schema);
  int put_schema(const ObSchemaType schema_type,
                 const uint64_t tenant_id,
                 const uint64_t schema_id,
                 const int64_t schema_version,
                 const ObSchema &schema);
  int put_and_fetch_schema(const ObSchemaType schema_type,
                           const uint64_t tenant_id,
                           const uint64_t schema_id,
                           const int64_t schema_version,
                           const ObSchema &schema,
                           common::ObKVCacheHandle &handle,
                           const ObSchema *&new_schema);
  int get_schema_history_cache(const ObSchemaType schema_type,
                               const uint64_t tenant_id,
                               const uint64_t schema_id,
                               const int64_t schema_version,
                               int64_t &precise_schema_version);
  int put_schema_history_cache(const ObSchemaType schema_type,
                               const uint64_t tenant_id,
                               const uint64_t schema_id,
                               const int64_t schema_version,
                               const int64_t precise_schema_version);
  const ObTableSchema *get_all_core_table() const;
  const ObSimpleTenantSchema *get_simple_gts_tenant() const;
  const ObTenantSchema *get_full_gts_tenant() const;


  // @param[in]:
  // - key: (tenant_id, tablet_id, schema_version)
  // @param[out]:
  // - table_id: table_id is OB_INVALID_ID means that
  //             tablet-table history doesn't exist
  //             or tablet is dropped under specific schema_version.
  // @return: OB_ENTRY_NOT_EXIST will be returned if cache not hit.
  int get_tablet_cache(const ObTabletCacheKey &key,
                       uint64_t &table_id);
  // @param[in]:
  // - key: (tenant_id, tablet_id, schema_version)
  // - table_id: (table_id)
  int put_tablet_cache(const ObTabletCacheKey &key,
                       const uint64_t table_id);
private:
  bool check_inner_stat() const;
  bool is_valid_key(const ObSchemaType schema_type,
                    const uint64_t tenant_id,
                    const uint64_t schema_id,
                    const int64_t schema_version) const;
  bool need_use_sys_cache(const ObSchemaCacheKey &cache_key) const;
  int init_all_core_table();
  int init_gts_tenant_schema();
  int put_sys_schema(
      const ObSchemaCacheKey &cache_key,
      const ObSchema &schema);

private:
  typedef common::hash::ObHashMap<ObSchemaCacheKey,
                                  const ObSchemaCacheValue*,
                                  common::hash::ReadWriteDefendMode> NoSwapCache;
  typedef common::ObKVCache<ObSchemaCacheKey, ObSchemaCacheValue> KVCache;
  typedef common::ObKVCache<ObSchemaCacheKey, ObSchemaHistoryCacheValue> HistoryCache;
  typedef common::ObKVCache<ObTabletCacheKey, ObTabletCacheValue> TabletCache;

  lib::MemoryContext mem_context_;
  NoSwapCache sys_cache_;
  KVCache cache_;
  HistoryCache history_cache_;
  bool is_inited_;
  ObTableSchema all_core_table_;
  ObSimpleTenantSchema simple_gts_tenant_;
  ObTenantSchema full_gts_tenant_;
  TabletCache tablet_cache_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaCache);
};

class ObSchemaService;
class ObTableSchema;
class ObSchemaFetcher
{
public:
  ObSchemaFetcher();
  virtual ~ObSchemaFetcher() {}
  bool check_inner_stat() const;
  int init(ObSchemaService *schema_service,
           common::ObISQLClient *sql_client);
  int fetch_schema(ObSchemaType schema_type,
                   const ObRefreshSchemaStatus &schema_status,
                   uint64_t schema_id,
                   int64_t schema_version,
                   common::ObIAllocator &allocator,
                   ObSchema *&schema);
private:
  int fetch_tenant_schema(uint64_t tenant_id,
                          int64_t schema_version,
                          common::ObIAllocator &allocator,
                          ObTenantSchema *&tenant_schema);
  int fetch_sys_variable_schema(
      const ObRefreshSchemaStatus &schema_status,
      uint64_t tenant_id,
      int64_t schema_version,
      common::ObIAllocator &allocator,
      ObSysVariableSchema *&sys_variable_schema);
  int fetch_database_schema(const ObRefreshSchemaStatus &schema_status,
                            uint64_t database_id,
                            int64_t schema_version,
                            common::ObIAllocator &allocator,
                            ObDatabaseSchema *&database_schema);
  int fetch_tablegroup_schema(const ObRefreshSchemaStatus &schema_status,
                              uint64_t tablegroup_id,
                              int64_t schema_version,
                              common::ObIAllocator &allocator,
                              ObTablegroupSchema *&tablegroup_schema);
  int fetch_table_schema(const ObRefreshSchemaStatus &schema_status,
                         uint64_t table_id,
                         int64_t schema_version,
                         common::ObIAllocator &allocator,
                         ObTableSchema *&table_schema);
  int fetch_table_schema(const ObRefreshSchemaStatus &schema_status,
                         uint64_t table_id,
                         int64_t schema_version,
                         common::ObIAllocator &allocator,
                         ObSimpleTableSchemaV2 *&table_schema);

#ifndef DEF_SCHEMA_INFO_FETCHER
#define DEF_SCHEMA_INFO_FETCHER(OBJECT_NAME, OBJECT_SCHEMA_TYPE) \
    int fetch_##OBJECT_NAME##_info(const ObRefreshSchemaStatus &schema_status, \
                                   uint64_t object_id, \
                                   int64_t schema_version, \
                                   common::ObIAllocator &allocator, \
                                   OBJECT_SCHEMA_TYPE *&object_schema)

  DEF_SCHEMA_INFO_FETCHER(user, ObUserInfo);
  DEF_SCHEMA_INFO_FETCHER(outline, ObOutlineInfo);
  DEF_SCHEMA_INFO_FETCHER(synonym, ObSynonymInfo);
  DEF_SCHEMA_INFO_FETCHER(package, ObPackageInfo);
  DEF_SCHEMA_INFO_FETCHER(routine, ObRoutineInfo);
  DEF_SCHEMA_INFO_FETCHER(trigger, ObTriggerInfo);
  DEF_SCHEMA_INFO_FETCHER(udf, ObUDF);
  DEF_SCHEMA_INFO_FETCHER(sequence, ObSequenceSchema);
  DEF_SCHEMA_INFO_FETCHER(udt, ObUDTTypeInfo);
  DEF_SCHEMA_INFO_FETCHER(keystore, ObKeystoreSchema);
  DEF_SCHEMA_INFO_FETCHER(label_se_policy, ObLabelSePolicySchema);
  DEF_SCHEMA_INFO_FETCHER(label_se_component, ObLabelSeComponentSchema);
  DEF_SCHEMA_INFO_FETCHER(label_se_label, ObLabelSeLabelSchema);
  DEF_SCHEMA_INFO_FETCHER(label_se_user_level, ObLabelSeUserLevelSchema);
  DEF_SCHEMA_INFO_FETCHER(tablespace, ObTablespaceSchema); 
  DEF_SCHEMA_INFO_FETCHER(profile, ObProfileSchema);
  DEF_SCHEMA_INFO_FETCHER(mock_fk_parent_table, ObMockFKParentTableSchema);
#undef DEF_SCHEMA_INFO_FETCHER
#endif

private:
  ObSchemaService *schema_service_;
  common::ObISQLClient *sql_client_;
  bool is_inited_;
};

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_
