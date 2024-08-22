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
  // is migrated tablet need convert co major sstable
  static bool check_need_convert_cs_when_migration(
      const ObTablet &tablet,
      const ObStorageSchema& schema_on_tablet);
  static int check_has_cs_replica(
      const share::ObLSID &ls_id,
      bool &has_column_store_replica);
  // local ls need process column store replica for specific tablet
  static int check_need_process_cs_replica(
      const ObLS &ls,
      const ObTabletID &tablet_id,
      const ObStorageSchema &schema,
      bool &need_process_cs_replica);
  static int check_need_wait_major_convert(
      const ObLS &ls,
      const ObTabletID &tablet_id,
      const ObTablet &tablet,
      bool &need_wait_major_convert);
  // whole ls replica set need process column store replica for specific tablet
  static int check_replica_set_need_process_cs_replica(
      const ObLS &ls,
      const ObTabletID &tablet_id,
      const ObStorageSchema &schema,
      bool &need_process_cs_replica);
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


} // namespace storage
} // namespace oceanbase

#endif