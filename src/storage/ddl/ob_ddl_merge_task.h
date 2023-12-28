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

#ifndef OCEANBASE_STORAGE_DDL_MERGE_TASK_
#define OCEANBASE_STORAGE_DDL_MERGE_TASK_

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{

namespace share
{
struct ObDDLChecksumItem;
}

namespace storage
{

class ObLS;
class ObCOSSTableV2;

class ObDDLTableMergeDag : public share::ObIDag
{
public:
  ObDDLTableMergeDag();
  virtual ~ObDDLTableMergeDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(ddl_param));
public:
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;

  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool ignore_warning() override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; } // TODO@wenqu: confirm it
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
private:
  bool is_inited_;
  ObDDLTableMergeDagParam ddl_param_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLTableMergeDag);
};

class ObDDLMacroBlock;
class ObDDLKV;


class ObDDLTableMergeTask : public share::ObITask
{
public:
  ObDDLTableMergeTask();
  virtual ~ObDDLTableMergeTask();
  int init(const ObDDLTableMergeDagParam &ddl_dag_param, const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs);
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(merge_param));
private:
  bool is_inited_;
  ObDDLTableMergeDagParam merge_param_;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLTableMergeTask);
};


class ObTabletDDLUtil
{
public:
  static int prepare_index_data_desc(
      ObTablet &tablet,
      const int64_t cg_idx, // negative means row store
      const int64_t snapshot_version,
      const uint64_t data_format_version,
      const blocksstable::ObSSTable *first_ddl_sstable,
      const share::SCN &end_scn,
      const ObStorageSchema *storage_schema,
      blocksstable::ObWholeDataStoreDesc &data_desc);

  static int create_ddl_sstable(
      ObTablet &tablet,
      const ObTabletDDLParam &ddl_param,
      const ObIArray<const blocksstable::ObDataMacroBlockMeta *> &meta_array,
      const blocksstable::ObSSTable *first_ddl_sstable,
      const ObStorageSchema *storage_schema,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &sstable_handle);

  static int update_ddl_table_store(
      ObLS &ls,
      ObTablet &tablet,
      const ObTabletDDLParam &ddl_param,
      const ObStorageSchema *storage_schema,
      common::ObArenaAllocator &allocator,
      blocksstable::ObSSTable *sstable);

  static int compact_ddl_kv(
      ObLS &ls,
      ObTablet &tablet,
      ObTableStoreIterator &ddl_sstable_iter,
      const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
      const ObTabletDDLParam &ddl_param,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &compacted_sstable_handle);

  static int report_ddl_checksum(const share::ObLSID &ls_id,
                                 const ObTabletID &tablet_id,
                                 const uint64_t table_id,
                                 const int64_t execution_id,
                                 const int64_t ddl_task_id,
                                 const int64_t *column_checksums,
                                 const int64_t column_count);

  static int check_and_get_major_sstable(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const blocksstable::ObSSTable *&first_major_sstable,
      ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper);

  static int get_compact_scn(
      const share::SCN &ddl_start_scn,
      ObTableStoreIterator &ddl_sstable_iter,
      const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
      share::SCN &compact_start_scn,
      share::SCN &compact_end_scn);

  static int freeze_ddl_kv(const ObDDLTableMergeDagParam &param);

  static int check_data_continue(
      ObTableStoreIterator &ddl_sstable_iter,
      bool &is_data_continue,
      share::SCN &compact_start_scn,
      share::SCN &compact_end_scn);

private:

  static int create_ddl_sstable(
      ObTablet &tablet,
      blocksstable::ObSSTableIndexBuilder *sstable_index_builder,
      const ObTabletDDLParam &ddl_param,
      const blocksstable::ObSSTable *first_ddl_sstable,
      const int64_t macro_block_column_count,
      const ObStorageSchema *storage_schema,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &sstable_handle);

  static int check_data_continue(
      const ObIArray<ObDDLKVHandle> &ddl_kvs,
      bool &is_data_continue,
      share::SCN &compact_start_scn,
      share::SCN &compact_end_scn);

};

} // namespace storage
} // namespace oceanbase

#endif
