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

#ifndef OCEABASE_STORAGE_SSTABLE_COPY_FINISH_TASK_
#define OCEABASE_STORAGE_SSTABLE_COPY_FINISH_TASK_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_restore_struct.h"
#include "ob_storage_ha_dag.h"
#include "src/storage/high_availability/ob_physical_copy_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObTabletCopyFinishTask;
class ObSSTableCopyFinishTask;
struct ObPhysicalCopyTaskInitParam final
{
  ObPhysicalCopyTaskInitParam();
  ~ObPhysicalCopyTaskInitParam();
  void reset();
  bool is_valid() const;


  TO_STRING_KV(K_(tenant_id), 
               K_(ls_id), 
               K_(tablet_id), 
               K_(src_info), 
               KPC_(sstable_param),
               K_(sstable_macro_range_info), 
               KP_(tablet_copy_finish_task),
               KP_(ls), 
               K_(is_leader_restore), 
               K_(restore_action),
               KP_(restore_base_info),
               KP_(meta_index_store),
               KP_(second_meta_index_store),
               K_(need_sort_macro_meta),
               K_(need_check_seq),
               K_(ls_rebuild_seq),
               KP_(macro_block_reuse_mgr),
               KPC_(extra_info));


  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObStorageHASrcInfo src_info_;
  const ObMigrationSSTableParam *sstable_param_;
  ObCopySSTableMacroRangeInfo sstable_macro_range_info_;
  ObTabletCopyFinishTask *tablet_copy_finish_task_;
  ObLS *ls_;
  bool is_leader_restore_;
  ObTabletRestoreAction::ACTION restore_action_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  bool need_sort_macro_meta_; // not use
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
  ObCopyTabletRecordExtraInfo *extra_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCopyTaskInitParam);
};


class ObCopiedSSTableCreatorImpl
{
public:
  ObCopiedSSTableCreatorImpl();
  virtual ~ObCopiedSSTableCreatorImpl() {}

  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMigrationSSTableParam *src_sstable_param,
      ObSSTableIndexBuilder *sstable_index_builder,
      common::ObArenaAllocator *allocator,
      ObSSTableCopyFinishTask *finish_task);

  virtual int create_sstable() = 0;

protected:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const = 0;
  int init_create_sstable_param_(ObTabletCreateSSTableParam &param) const;
  int init_create_sstable_param_(
      const blocksstable::ObSSTableMergeRes &res, 
      ObTabletCreateSSTableParam &param) const;
  int do_create_sstable_(
      const ObTabletCreateSSTableParam &param, 
      ObTableHandleV2 &table_handle) const;


protected:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  const ObMigrationSSTableParam *src_sstable_param_;
  ObSSTableIndexBuilder *sstable_index_builder_;
  common::ObArenaAllocator *allocator_;
  ObSSTableCopyFinishTask *finish_task_;
};


// Create empty SSTable whose index does not need to be rebuilt. Empty SSTable
// is the SSTable with data_macro_block_count 0, except for only shared macro 
// blocks SSTable, whose data_macro_block_count is 0 but is not empty.
class ObCopiedEmptySSTableCreator final : public ObCopiedSSTableCreatorImpl
{
public:
  ObCopiedEmptySSTableCreator() : ObCopiedSSTableCreatorImpl() {}

  virtual int create_sstable() override;

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const override;

  DISALLOW_COPY_AND_ASSIGN(ObCopiedEmptySSTableCreator);
};


// Create non-shared-macro-blocks SSTable which is not empty and its index needs to be rebuilt.
class ObCopiedSSTableCreator final : public ObCopiedSSTableCreatorImpl
{
public:
  ObCopiedSSTableCreator() : ObCopiedSSTableCreatorImpl() {}

  virtual int create_sstable() override;

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const override;

  DISALLOW_COPY_AND_ASSIGN(ObCopiedSSTableCreator);
};


#ifdef OB_BUILD_SHARED_STORAGE
// Create non empty shared SSTable in shared storage mode, only during leader restore.
class ObRestoredSharedSSTableCreator final : public ObCopiedSSTableCreatorImpl
{
public:
  ObRestoredSharedSSTableCreator() : ObCopiedSSTableCreatorImpl() {}

  virtual int create_sstable() override;

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const override;

  DISALLOW_COPY_AND_ASSIGN(ObRestoredSharedSSTableCreator);
};


// Create shared-only-macro-blocks SSTable, currently only ddl sstable in shared storage mode. This kind of SSTable
// does not own an index, but should record the macro ids on meta. Macro blocks are required to copy only during 
// leader restore, otherwise, are not.
class ObCopiedSharedMacroBlocksSSTableCreator final : public ObCopiedSSTableCreatorImpl
{
public:
  ObCopiedSharedMacroBlocksSSTableCreator() : ObCopiedSSTableCreatorImpl() {}

  virtual int create_sstable() override;

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const override;

  int get_shared_macro_id_list_(common::ObIArray<MacroBlockId> &macro_block_id_array);

  DISALLOW_COPY_AND_ASSIGN(ObCopiedSharedMacroBlocksSSTableCreator);
};
#endif


// Create shared SSTable which is not empty. Shared SSTable is the SSTable whose macro blocks, including data and 
// index blocks, are all in shared or backup storage. Macro blocks need not copy and index does not need to be 
// rebuilt, just put the ObMigrationSSTableParam from source into local table store. This is happen during migration
// or follower restore, when source SSTable is shared.
class ObCopiedSharedSSTableCreator final : public ObCopiedSSTableCreatorImpl
{
public:
  ObCopiedSharedSSTableCreator() : ObCopiedSSTableCreatorImpl() {}

  virtual int create_sstable() override;

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const override;

  DISALLOW_COPY_AND_ASSIGN(ObCopiedSharedSSTableCreator);
};


class ObSSTableCopyFinishTask : public share::ObITask
{
public:
  ObSSTableCopyFinishTask();
  virtual ~ObSSTableCopyFinishTask();
  int init(
      const ObPhysicalCopyTaskInitParam &init_param);
  ObPhysicalCopyCtx *get_copy_ctx() { return &copy_ctx_; }
  const ObMigrationSSTableParam *get_sstable_param() { return sstable_param_; }
  int get_next_macro_block_copy_info(
      ObITable::TableKey &copy_table_key,
      const ObCopyMacroRangeInfo *&copy_macro_range_info);
  int check_is_iter_end(bool &is_end);
  int get_tablet_finish_task(ObTabletCopyFinishTask *&finish_task);

  int add_sstable(ObTableHandleV2 &table_handle);
  int add_sstable(ObTableHandleV2 &table_handle, const int64_t last_meta_macro_seq);
  int get_copy_macro_range_array(const common::ObArray<ObCopyMacroRangeInfo> *&macro_range_array) const;
  int64_t get_next_copy_task_id();
  int64_t get_max_next_copy_task_id();

  virtual int process() override;


  VIRTUAL_TO_STRING_KV(K("ObSSTableCopyFinishTask"), KP(this), K(copy_ctx_));

private:
  bool is_sstable_should_rebuild_index_(const ObMigrationSSTableParam *sstable_param) const;
  bool is_shared_sstable_without_copy_(const ObMigrationSSTableParam *sstable_param) const;
  int get_cluster_version_(
      const ObPhysicalCopyTaskInitParam &init_param,
      int64_t &cluster_version);
  int prepare_sstable_index_builder_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMigrationSSTableParam *sstable_param,
      const int64_t cluster_version);
  int prepare_data_store_desc_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMigrationSSTableParam *sstable_param,
      const int64_t cluster_version,
      ObWholeDataStoreDesc &desc);
  int get_merge_type_(
      const ObMigrationSSTableParam *sstable_param,
      compaction::ObMergeType &merge_type);
  int create_sstable_();
  int create_empty_sstable_();
  int build_create_empty_sstable_param_(
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
  int update_major_sstable_reuse_info_();
  int update_copy_tablet_record_extra_info_();
  int create_pure_remote_sstable_();
  int build_create_pure_remote_sstable_param_(
      ObTabletCreateSSTableParam &param);
  int alloc_and_init_sstable_creator_(ObCopiedSSTableCreatorImpl *&sstable_creator);
  void free_sstable_creator_(ObCopiedSSTableCreatorImpl *&sstable_creator);
  int get_space_optimization_mode_(
      const ObMigrationSSTableParam *sstable_param, 
      ObSSTableIndexBuilder::ObSpaceOptimizationMode &mode);

private:
  bool is_inited_;
  int64_t next_copy_task_id_;
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

}
}
#endif
