/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_BATCH_SSTABLE_COPY_TASK_H
#define OCEANBASE_STORAGE_OB_BATCH_SSTABLE_COPY_TASK_H

#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace common
{
class ObInOutBandwidthThrottle;
}
namespace blocksstable
{
class ObMigrationSSTableParam;
}
namespace obrpc
{
class ObStorageRpcProxy;
}
namespace storage
{

struct ObMigrationTabletParam;
struct ObPhysicalCopyCtx;
class ObCopyTabletRecordExtraInfo;
class ObLS;
class ObStorageHACopySSTableInfoMgr;
class ObStorageHATableInfoMgr;
class ObTabletCopyFinishTask;
class ObTabletMigrationTask;

// Holds all CG SSTable keys selected for one batch chain. Each task claims a
// bounded slice during init(), so no task keeps a pointer to the holder.
struct ObBatchSSTableKeysHolder
{
  explicit ObBatchSSTableKeysHolder(const uint64_t tenant_id);
  int assign(const common::ObIArray<ObITable::TableKey> &keys);
  void reset();
  int take_next(
      const int64_t max_count,
      common::ObIArray<ObITable::TableKey> &keys,
      bool &has_more);
  bool has_more() const;
  TO_STRING_KV(K_(cursor), "pending_count", pending_keys_.count());

private:
  common::ObArray<ObITable::TableKey> pending_keys_;
  int64_t cursor_;
};

// Shared, read-only state of all ObBatchSSTableCopyTask instances generated
// for one tablet. Every pointer is owned by the enclosing migration DAG.
struct ObBatchSSTableCopyTaskParam
{
  ObBatchSSTableCopyTaskParam();
  bool is_valid() const;
  TO_STRING_KV(KP_(ls), K_(tablet_id), K_(src_info), KP_(src_tablet_meta),
      KP_(table_info_mgr), KP_(copy_sstable_info_mgr),
      KP_(tablet_copy_finish_task), KP_(extra_info),
      K_(ha_svc_ctx), K_(src_ls_rebuild_seq));

  ObLS *ls_;
  common::ObTabletID tablet_id_;
  ObStorageHASrcInfo src_info_;
  const ObMigrationTabletParam *src_tablet_meta_;
  ObStorageHATableInfoMgr *table_info_mgr_;
  ObStorageHACopySSTableInfoMgr *copy_sstable_info_mgr_;
  ObTabletCopyFinishTask *tablet_copy_finish_task_;
  ObCopyTabletRecordExtraInfo *extra_info_;
  ObStorageHAServiceCtx ha_svc_ctx_;
  int64_t src_ls_rebuild_seq_;
};

// Copies multiple CG SSTables in one DAG task. Tasks in the same batch chain
// run serially and therefore safely reuse ObTabletCopyFinishTask's allocator.
class ObBatchSSTableCopyTask final : public share::ObITask
{
public:
  static constexpr int64_t MAX_SSTABLE_PER_BATCH = 128;

  ObBatchSSTableCopyTask();
  virtual ~ObBatchSSTableCopyTask();

  int init(
      const ObBatchSSTableCopyTaskParam &param,
      ObBatchSSTableKeysHolder &keys_holder);
  virtual int process() override;

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(param), K_(my_keys));

private:
  static void fill_copy_ctx_from_param_(
      const ObBatchSSTableCopyTaskParam &param,
      share::ObIDag *dag,
      ObPhysicalCopyCtx &copy_ctx);
  int prepare_macro_block_reuse_(
      const ObPhysicalCopyCtx &copy_ctx,
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const ObCopySSTableMacroRangeInfo *sstable_macro_range_info,
      const ObTabletHandle &tablet_handle,
      ObTableHandleV2 &split_src_sstable_handle);
  int prepare_sstable_macro_range_info_(
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const ObCopySSTableMacroRangeInfo &sstable_macro_range_info,
      bool &reuse_supported);

private:
  bool is_inited_;
  ObBatchSSTableCopyTaskParam param_;
  common::ObSEArray<ObITable::TableKey, MAX_SSTABLE_PER_BATCH> my_keys_;
  ObMacroBlockReuseMgr macro_block_reuse_mgr_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchSSTableCopyTask);
};

// Low-conflict adapter used by ObTabletMigrationTask. It takes over one
// MDS/minor/major/DDL stage only when that stage has eligible CG SSTables,
// batches those keys, and builds the legacy chain for the remaining keys.
class ObBatchSSTableCopyTaskGenerator final
{
public:
  typedef bool (*IsRightTypeSSTableFunc)(const ObITable::TableType table_type);

  static int try_generate_copy_tasks(
      ObTabletMigrationTask &migration_task,
      IsRightTypeSSTableFunc is_right_type_sstable,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task,
      bool &is_generated);

private:
  static void check_batch_cg_copy_enabled_(
      const ObTabletMigrationTask &migration_task,
      bool &enabled);
  static int classify_copy_keys_(
      ObTabletMigrationTask &migration_task,
      IsRightTypeSSTableFunc is_right_type_sstable,
      common::ObIArray<ObITable::TableKey> &batch_keys,
      common::ObIArray<ObITable::TableKey> &legacy_keys);
  static int build_batch_copy_tasks_(
      ObTabletMigrationTask &migration_task,
      const common::ObIArray<ObITable::TableKey> &batch_keys,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  static int build_legacy_copy_tasks_(
      ObTabletMigrationTask &migration_task,
      const common::ObIArray<ObITable::TableKey> &legacy_keys,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);

private:
  ObBatchSSTableCopyTaskGenerator() = delete;
  ~ObBatchSSTableCopyTaskGenerator() = delete;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_BATCH_SSTABLE_COPY_TASK_H
