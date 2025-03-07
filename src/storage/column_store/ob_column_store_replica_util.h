/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_COLUMN_OB_COLUMN_STORE_REPLICA_UTIL_H_
#define OCEANBASE_STORAGE_COLUMN_OB_COLUMN_STORE_REPLICA_UTIL_H_

#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace storage
{

enum class ObCSReplicaTabletStatus : uint8_t {
  NORMAL                 = 0,  // local ls is not cs replica (F/R), local ls is cs replica (C), but no need to be column store (like index or inner table)
  NOT_COMPLETE           = 1,  // ha status not data complete, need wait transfer/migration/rebuild finish
  NO_MAJOR_SSTABLE       = 2,  // offline ddl or direct load data not finish
  NEED_CO_CONVERT_MERGE  = 3,  // lastest major is row store MAJOR
  NEED_CS_STORAGE_SCHEMA = 4,  // lastest major is column store CO_MAJOR but storage schema is row store.
  NORMAL_CS_REPLICA      = 5,  // valid schema and valid major in cs replica
  MAX_STATUS
};

inline bool is_valid_cs_replica_status(const ObCSReplicaTabletStatus &status)
{
  return ObCSReplicaTabletStatus::NORMAL <= status && status < ObCSReplicaTabletStatus::MAX_STATUS;
}

inline bool is_normal_status(const ObCSReplicaTabletStatus &status)
{
  return ObCSReplicaTabletStatus::NORMAL == status
      || ObCSReplicaTabletStatus::NORMAL_CS_REPLICA == status;
}

inline bool is_need_wait_status(const ObCSReplicaTabletStatus &status)
{
  return ObCSReplicaTabletStatus::NOT_COMPLETE == status
      || ObCSReplicaTabletStatus::NO_MAJOR_SSTABLE == status;
}

inline bool is_need_major_convert_status(const ObCSReplicaTabletStatus &status)
{
  return ObCSReplicaTabletStatus::NEED_CO_CONVERT_MERGE == status;
}

inline bool is_need_cs_storage_schema_status(const ObCSReplicaTabletStatus &status)
{
  return ObCSReplicaTabletStatus::NEED_CS_STORAGE_SCHEMA == status;
}

class ObCSReplicaUtil
{
public:
  // for construct storage schema for merge from table_schema
  static int check_is_cs_replica(
      const ObTableSchema &table_schema,
      const ObTablet &tablet,
      bool &is_cs_replica);
  // is local ls cs replica
  static int check_local_is_cs_replica(
      const share::ObLSID &ls_id,
      bool &is_cs_replica);
  static int check_has_cs_replica(
      const share::ObLSID &ls_id,
      bool &has_column_store_replica);
  // local ls need process column store replica for specific tablet
  static int check_need_generate_cs_replica_cg_array(
      const ObLS &ls,
      const ObTabletID &tablet_id,
      const ObStorageSchema &schema,
      bool &need_generate_cs_replica_cg_array);
  // whole ls replica set need process column store replica for specific tablet
  static int check_need_process_for_cs_replica_for_ddl(
      const ObTablet &tablet,
      const ObStorageSchema &schema,
      bool &need_process_cs_replica);
  static int check_cs_replica_global_visible(
      const ObLSInfo &ls_info,
      bool &is_global_visible);
  static int get_cs_replica_ls_set(
      const ObIArray<share::ObLSID> &ls_id_array,
      int64_t tenant_id,
      hash::ObHashSet<share::ObLSID> &contain_cs_replica_ls_id_set);
  static int check_need_process_cs_replica_for_offline_ddl(
      const ObTableSchema &orig_table_schema,
      bool &need_process);
  static int check_need_wait_for_report(
      const ObLS &ls,
      const ObTablet &tablet,
      bool &need_wait_for_report);
  static int init_cs_replica_tablet_status(
      const ObLS &ls,
      const ObTablet &tablet,
      ObCSReplicaTabletStatus &cs_replica_status);
  static int check_need_process_cs_replica(
      const ObLS &ls,
      const ObTablet &tablet,
      bool &need_process_cs_replica);
  static int get_full_column_array_from_table_schema(
      common::ObIAllocator &allocator,
      const ObUpdateCSReplicaSchemaParam &update_param,
      const ObStorageSchema &simplified_schema,
      common::ObFixedArray<ObStorageColumnSchema, common::ObIAllocator> &column_array);
  static int get_column_array_from_full_storage_schema(
      common::ObIAllocator &allocator,
      const int64_t expected_stored_column_cnt,
      const ObStorageSchema &full_storage_schema,
      common::ObFixedArray<ObStorageColumnSchema, common::ObIAllocator> &column_array);
  static int get_rebuild_storage_schema(
      common::ObIAllocator &allocator,
      const ObUpdateCSReplicaSchemaParam &param,
      const ObStorageSchema &simplified_schema,
      ObStorageSchema *&full_storage_schema);
public:
  static const int64_t DEFAULT_CHECK_LS_REPLICA_LOCATION_TIMEOUT = 10 * 1000 * 1000L; // 10s
};

class ObCSReplicaStorageSchemaGuard
{
public:
    ObCSReplicaStorageSchemaGuard();
    ~ObCSReplicaStorageSchemaGuard();
    int init(const ObTabletHandle &tablet_handle, compaction::ObCompactionMemoryContext &mem_ctx);
    void reset();
    OB_INLINE bool is_inited() const { return is_inited_; };
    int load(ObStorageSchema *&storage_schema);
    TO_STRING_KV(K_(is_inited), KP_(schema));
private:
    bool is_inited_;
    ObStorageSchema *schema_;
    DISALLOW_COPY_AND_ASSIGN(ObCSReplicaStorageSchemaGuard);
};

class ObGlobalCSReplicaMgr final
{
public:
  ObGlobalCSReplicaMgr();
  ~ObGlobalCSReplicaMgr();
  int try_init(const int64_t tenant_id, const ObIArray<share::ObLSID> &ls_id_array);
  int check_cs_replica_global_visible(const share::ObLSID &ls_id, bool &is_global_visible) const;
  TO_STRING_KV(K_(is_inited));
public:
  static const int64_t CS_REPLICA_LS_ID_SET_BUCKET_NUM = 31;
private:
  hash::ObHashSet<share::ObLSID> cs_replica_ls_id_set_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObGlobalCSReplicaMgr);
};

} // namespace storage
} // namespace oceanbase

#endif