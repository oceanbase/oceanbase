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

#ifndef OCEABASE_STORAGE_PHYSICAL_COPY_TASK_
#define OCEABASE_STORAGE_PHYSICAL_COPY_TASK_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_ha_macro_block_writer.h"
#include "ob_storage_ha_reader.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_storage_restore_struct.h"
#include "ob_storage_ha_dag.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace storage
{

struct ObICopyTabletCtx
{
public:
  virtual int set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status) = 0;
  virtual int get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const = 0;
};

struct ObPhysicalCopyCtx
{
  ObPhysicalCopyCtx();
  virtual ~ObPhysicalCopyCtx();
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(src_info), KP_(bandwidth_throttle),
      KP_(svr_rpc_proxy), K_(is_leader_restore), KP_(restore_base_info),
      KP_(meta_index_store), KP_(second_meta_index_store), KP_(ha_dag),
      KP_(sstable_index_builder), KP_(restore_macro_block_id_mgr), K_(need_check_seq), K_(ls_rebuild_seq), K_(table_key));
  common::SpinRWLock lock_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObStorageHASrcInfo src_info_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  bool is_leader_restore_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObStorageHADag *ha_dag_;
  ObSSTableIndexBuilder *sstable_index_builder_;
  ObRestoreMacroBlockIdMgr *restore_macro_block_id_mgr_;
  bool need_sort_macro_meta_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCopyCtx);
};

class ObTabletCopyFinishTask;
struct ObPhysicalCopyTaskInitParam final
{
  ObPhysicalCopyTaskInitParam();
  ~ObPhysicalCopyTaskInitParam();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(src_info), KP_(sstable_param),
      K_(sstable_macro_range_info), KP_(tablet_copy_finish_task),
      KP_(ls), K_(is_leader_restore), KP_(restore_base_info), KP_(second_meta_index_store));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObStorageHASrcInfo src_info_;
  const ObMigrationSSTableParam *sstable_param_;
  ObCopySSTableMacroRangeInfo sstable_macro_range_info_;
  ObTabletCopyFinishTask *tablet_copy_finish_task_;
  ObLS *ls_;
  bool is_leader_restore_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  bool need_sort_macro_meta_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCopyTaskInitParam);
};

class ObSSTableCopyFinishTask;
class ObPhysicalCopyTask : public share::ObITask
{
public:
  ObPhysicalCopyTask();
  virtual ~ObPhysicalCopyTask();
  int init(
      ObPhysicalCopyCtx *copy_ctx,
      ObSSTableCopyFinishTask *finish_task,
      const int64_t task_idx);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV(K("ObPhysicalCopyFinishTask"), KP(this), KPC(copy_ctx_));
private:
  int fetch_macro_block_with_retry_(
      ObMacroBlocksWriteCtx &copied_ctx);
  int fetch_macro_block_(
      const int64_t retry_times,
      ObMacroBlocksWriteCtx &copied_ctx);
  int build_macro_block_copy_info_(ObSSTableCopyFinishTask *finish_task);
  int get_macro_block_reader_(
      ObICopyMacroBlockReader *&reader);
  int get_macro_block_ob_reader_(
      const ObCopyMacroBlockReaderInitParam &init_param,
      ObICopyMacroBlockReader *&reader);
  int get_macro_block_restore_reader_(
      const ObCopyMacroBlockReaderInitParam &init_param,
      ObICopyMacroBlockReader *&reader);
  int get_macro_block_writer_(
      ObICopyMacroBlockReader *reader,
      ObIndexBlockRebuilder *index_block_rebuilder,
      ObStorageHAMacroBlockWriter *&writer);
  void free_macro_block_reader_(ObICopyMacroBlockReader *&reader);
  void free_macro_block_writer_(ObStorageHAMacroBlockWriter *&writer);
  int build_copy_macro_block_reader_init_param_(
      ObCopyMacroBlockReaderInitParam &init_param);
  int record_server_event_();

private:
  static const int64_t MAX_RETRY_TIMES = 3;
  static const int64_t OB_FETCH_MAJOR_BLOCK_RETRY_INTERVAL = 1 * 1000 * 1000L;// 1s
  bool is_inited_;
  ObPhysicalCopyCtx *copy_ctx_;
  ObSSTableCopyFinishTask *finish_task_;
  ObITable::TableKey copy_table_key_;
  const ObCopyMacroRangeInfo *copy_macro_range_info_;
  int64_t task_idx_;

  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCopyTask);
};

class ObSSTableCopyFinishTask : public share::ObITask
{
public:
  ObSSTableCopyFinishTask();
  virtual ~ObSSTableCopyFinishTask();
  int init(
      const ObPhysicalCopyTaskInitParam &init_param);
  virtual int process() override;
  ObPhysicalCopyCtx *get_copy_ctx() { return &copy_ctx_; }
  const ObMigrationSSTableParam *get_sstable_param() { return sstable_param_; }
  int get_macro_block_copy_info(
      ObITable::TableKey &copy_table_key,
      const ObCopyMacroRangeInfo *&copy_macro_range_info);
  int check_is_iter_end(bool &is_end);
  int get_tablet_finish_task(ObTabletCopyFinishTask *&finish_task);

  VIRTUAL_TO_STRING_KV(K("ObSSTableCopyFinishTask"), KP(this), K(copy_ctx_));

private:
  int prepare_sstable_index_builder_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMigrationSSTableParam *sstable_param);
  int prepare_data_store_desc_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMigrationSSTableParam *sstable_param,
      ObWholeDataStoreDesc &desc);
  int get_merge_type_(
      const ObMigrationSSTableParam *sstable_param,
      compaction::ObMergeType &merge_type);
  int create_sstable_();
  int create_empty_sstable_();
  int build_create_sstable_param_(
      ObTabletCreateSSTableParam &param);

  int create_sstable_with_index_builder_();
  int build_create_sstable_param_(
      ObTablet *tablet,
      const blocksstable::ObSSTableMergeRes &res,
      ObTabletCreateSSTableParam &param);
  int build_restore_macro_block_id_mgr_(
      const ObPhysicalCopyTaskInitParam &init_param);
  int check_sstable_valid_();
  int check_sstable_meta_(
      const ObMigrationSSTableParam &src_meta,
      const ObSSTableMeta &write_meta);

private:
  bool is_inited_;
  ObPhysicalCopyCtx copy_ctx_;
  common::SpinRWLock lock_;
  const ObMigrationSSTableParam *sstable_param_;
  ObCopySSTableMacroRangeInfo sstable_macro_range_info_;
  int64_t macro_range_info_index_;
  ObTabletCopyFinishTask *tablet_copy_finish_task_;
  storage::ObLS *ls_;
  ObLSTabletService *tablet_service_;
  ObSSTableIndexBuilder sstable_index_builder_;
  ObRestoreMacroBlockIdMgr *restore_macro_block_id_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableCopyFinishTask);
};

class ObTabletCopyFinishTask : public share::ObITask
{
public:
  ObTabletCopyFinishTask();
  virtual ~ObTabletCopyFinishTask();
  int init(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      observer::ObIMetaReport *reporter,
      const ObTabletRestoreAction::ACTION &restore_action,
      const ObMigrationTabletParam *src_tablet_meta,
      ObICopyTabletCtx *copy_tablet_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletCopyFinishTask"), KP(this));
  int add_sstable(ObTableHandleV2 &table_handle);
  int get_sstable(
      const ObITable::TableKey &table_key,
      ObTableHandleV2 &table_handle);
  common::ObArenaAllocator &get_allocator() { return arena_allocator_; } // TODO: @jinzhu remove me later.
  int set_tablet_status(const ObCopyTabletStatus::STATUS &status);
  int get_tablet_status(ObCopyTabletStatus::STATUS &status);

  const ObMigrationTabletParam *get_src_tablet_meta() const { return src_tablet_meta_; }
private:
  int create_new_table_store_with_major_();
  int create_new_table_store_with_minor_();
  int trim_tablet_();

  int check_finish_copy_tablet_data_valid_();
  int get_tables_handle_ptr_(
      const ObITable::TableKey &table_key,
      ObTablesHandleArray *&table_handle_ptr);
  int check_tablet_valid_();
  int check_log_replay_to_mds_sstable_end_scn_();

private:
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObTabletID tablet_id_;
  ObLS *ls_;
  observer::ObIMetaReport *reporter_;
  ObStorageHADag *ha_dag_;
  common::ObArenaAllocator arena_allocator_;
  ObTablesHandleArray minor_tables_handle_;
  ObTablesHandleArray ddl_tables_handle_;
  ObTablesHandleArray major_tables_handle_;
  ObTablesHandleArray mds_tables_handle_;
  ObTabletRestoreAction::ACTION restore_action_;
  const ObMigrationTabletParam *src_tablet_meta_;
  ObICopyTabletCtx *copy_tablet_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletCopyFinishTask);
};

}
}
#endif
