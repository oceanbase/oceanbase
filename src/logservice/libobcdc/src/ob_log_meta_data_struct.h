/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_META_DATA_STRUCT_H_
#define OCEANBASE_LOG_META_DATA_STRUCT_H_

#include "lib/hash/ob_linear_hash_map.h"                  // ObLinearHashMap
#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"   // ObConcurrentFIFOAllocator
#include "logservice/data_dictionary/ob_data_dict_struct.h"

namespace oceanbase
{
namespace libobcdc
{
class TenantSchemaInfo;
class DBSchemaInfo;
class MetaDataKey
{
public:
  MetaDataKey() { reset(); }
  MetaDataKey(const uint64_t schema_id) :
    schema_id_(schema_id)
  {}
  ~MetaDataKey() { reset(); }

  void reset()
  {
    schema_id_ = common::OB_INVALID_ID;
  }

  bool is_valid() const
  {
    return common::OB_INVALID_ID != schema_id_;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&schema_id_, sizeof(schema_id_), hash_val);

    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  int compare(const MetaDataKey &other) const
  {
    int cmp_ret = 0;

    if (schema_id_ > other.schema_id_) {
      cmp_ret = 1;
    } else if (schema_id_ < other.schema_id_) {
      cmp_ret = -1;
    } else {
      cmp_ret = 0;
    }

    return cmp_ret;
  }

  bool operator==(const MetaDataKey &other) const { return 0 == compare(other); }
  bool operator!=(const MetaDataKey &other) const { return !operator==(other); }
  bool operator<(const MetaDataKey &other) const { return -1 == compare(other); }

  TO_STRING_KV(K_(schema_id));

private:
  uint64_t schema_id_;
};

class MetaDataKey2
{
public:
  MetaDataKey2() { reset(); }
  MetaDataKey2(const uint64_t tenant_id, const uint64_t schema_id) :
    tenant_id_(tenant_id),
    schema_id_(schema_id)
  {}
  ~MetaDataKey2() { reset(); }

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    schema_id_ = common::OB_INVALID_ID;
  }

  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && common::OB_INVALID_ID != schema_id_;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&schema_id_, sizeof(schema_id_), hash_val);

    return hash_val;
  }

  int compare(const MetaDataKey2 &other) const
  {
    int cmp_ret = 0;

    if (tenant_id_ > other.tenant_id_) {
      cmp_ret = 1;
    } else if (tenant_id_ < other.tenant_id_) {
      cmp_ret = -1;
    } else if (schema_id_ > other.schema_id_) {
      cmp_ret = 1;
    } else if (schema_id_ < other.schema_id_) {
      cmp_ret = -1;
    } else {
      cmp_ret = 0;
    }

    return cmp_ret;
  }

  bool operator==(const MetaDataKey2 &other) const { return 0 == compare(other); }
  bool operator!=(const MetaDataKey2 &other) const { return !operator==(other); }
  bool operator<(const MetaDataKey2 &other) const { return -1 == compare(other); }

  TO_STRING_KV(K_(tenant_id), K_(schema_id));

private:
  uint64_t tenant_id_;
  uint64_t schema_id_;
};

typedef common::LinkHashValue<MetaDataKey> DataDictValue;

class ObDictTenantInfo : public DataDictValue
{
public:
  ObDictTenantInfo();
  ~ObDictTenantInfo();
  int init();
  void destroy();

public:
  common::ObArenaAllocator &get_arena_allocator() { return arena_allocator_; }

  // Tenant Meta
  datadict::ObDictTenantMeta &get_dict_tenant_meta() { return dict_tenant_meta_; }
  common::ObCompatibilityMode get_compatibility_mode() const { return dict_tenant_meta_.get_compatibility_mode(); }
  uint64_t get_tenant_id() const { return dict_tenant_meta_.get_tenant_id(); }
  const char *get_tenant_name() const { return dict_tenant_meta_.get_tenant_name(); }
  int replace_dict_tenant_meta(datadict::ObDictTenantMeta *new_dict_tenant_meta);
  int incremental_data_update(const share::ObLSAttr &ls_atrr);

  // Database Meta
  int alloc_dict_db_meta(datadict::ObDictDatabaseMeta *&dict_db_meta);
  int free_dict_db_meta(datadict::ObDictDatabaseMeta *dict_db_meta);
  int insert_dict_db_meta(datadict::ObDictDatabaseMeta *dict_db_meta);
  int replace_dict_db_meta(const datadict::ObDictDatabaseMeta &new_dict_db_meta);

  // Table Meta
  int alloc_dict_table_meta(datadict::ObDictTableMeta *&dict_table_meta);
  int free_dict_table_meta(datadict::ObDictTableMeta *dict_table_meta);
  int insert_dict_table_meta(datadict::ObDictTableMeta *dict_table_meta);
  int replace_dict_table_meta(const datadict::ObDictTableMeta &new_dict_table_meta);
  int remove_table_meta(const uint64_t table_id);

  // Get TenantSchemaInfo
  int get_tenant_schema_info(TenantSchemaInfo &tenant_schema_info);

  // Get all table metas that are included by a tenant
  int get_table_metas_in_tenant(
      common::ObIArray<const datadict::ObDictTableMeta *> &table_metas);

  // Get ObDictTableMeta based on the TableID
  //
  // @param [in]   tenant_id     Tenant ID
  // @param [out]  table_meta    return ObDictTableMeta
  //
  // @retval OB_SUCCESS          Get success
  // @retval OB_ENTRY_NOT_EXIST  The table_meta for TableID does not exist
  // @retval other_err_code      Unexpected error
  int get_table_meta(
      const uint64_t table_id,
      datadict::ObDictTableMeta *&table_meta);

  // Compatiable with SchemaGetterGuard.
  int get_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const datadict::ObDictTableMeta *&table_schema,
      int64_t timeout = 1000);

  // Get ObDictDatabaseMeta based on the DataBaseID
  //
  // @param [in]   tenant_id     Tenant ID
  // @param [out]  table_meta    return ObDictTableMeta
  //
  // @retval OB_SUCCESS          Get success
  // @retval OB_ENTRY_NOT_EXIST  The db_meta for DataBaseID does not exist
  // @retval other_err_code      Unexpected error
  int get_db_meta(
      const uint64_t db_id,
      datadict::ObDictDatabaseMeta *&db_meta);

  // Get DBSchemaInfo based on the DataBaseID
  int get_database_schema_info(
      const uint64_t db_id,
      DBSchemaInfo &db_schema_info);

  TO_STRING_KV(
      K_(is_inited),
      K_(dict_tenant_meta),
      "db_count", db_map_.count(),
      "table_count", table_map_.count());

private:
  typedef common::ObLinearHashMap<MetaDataKey, datadict::ObDictDatabaseMeta *> DataDictDBMap;
  typedef common::ObLinearHashMap<MetaDataKey, datadict::ObDictTableMeta *> DataDictTableMap;
  typedef common::ObConcurrentFIFOAllocator CFIFOAllocator;
  static const int64_t ALLOCATOR_TOTAL_LIMIT = 10L * 1024L * 1024L * 1024L;
  static const int64_t ALLOCATOR_HOLD_LIMIT = common::OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

  struct TableMetasGetter
  {
    common::ObIArray<const datadict::ObDictTableMeta *> &table_metas_;
    TableMetasGetter(common::ObIArray<const datadict::ObDictTableMeta *> &table_metas) : table_metas_(table_metas) {}
    bool operator()(const MetaDataKey &key, datadict::ObDictTableMeta *value);
  };

private:
  bool is_inited_;
  common::ObArenaAllocator arena_allocator_;
  CFIFOAllocator cfifo_allocator_;
  datadict::ObDictTenantMeta dict_tenant_meta_;
  DataDictDBMap db_map_;
  DataDictTableMap table_map_;

  DISALLOW_COPY_AND_ASSIGN(ObDictTenantInfo);
};

//////////////////////////// ObDictTenantInfoGuard /////////////////////////
class ObDictTenantInfoGuard final
{
public:
  ObDictTenantInfoGuard() : tenant_info_(NULL) {}
  ~ObDictTenantInfoGuard() { revert_tenant(); }
public:
  ObDictTenantInfo *get_tenant_info() { return tenant_info_; }
  void set_tenant(ObDictTenantInfo *tenant_info) { tenant_info_ = tenant_info; }

  TO_STRING_KV(KPC_(tenant_info));
private:
  void revert_tenant();
private:
  ObDictTenantInfo *tenant_info_;
  DISALLOW_COPY_AND_ASSIGN(ObDictTenantInfoGuard);
};

typedef common::ObLinkHashMap<MetaDataKey, ObDictTenantInfo> DataDictTenantMap;

} // namespace libobcdc
} // namespace oceanbase

#endif
