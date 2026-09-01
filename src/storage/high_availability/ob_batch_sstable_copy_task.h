/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_BATCH_SSTABLE_COPY_TASK_H
#define OCEANBASE_STORAGE_OB_BATCH_SSTABLE_COPY_TASK_H

#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/high_availability/ob_sstable_copy_chain_utils.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace common
{
class ObInOutBandwidthThrottle;
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
      KP_(bandwidth_throttle), KP_(svr_rpc_proxy), K_(src_ls_rebuild_seq),
      KP_(macro_block_reuse_mgr));

  ObLS *ls_;
  common::ObTabletID tablet_id_;
  ObStorageHASrcInfo src_info_;
  const ObMigrationTabletParam *src_tablet_meta_;
  ObStorageHATableInfoMgr *table_info_mgr_;
  ObStorageHACopySSTableInfoMgr *copy_sstable_info_mgr_;
  ObTabletCopyFinishTask *tablet_copy_finish_task_;
  ObCopyTabletRecordExtraInfo *extra_info_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  int64_t src_ls_rebuild_seq_;
  // Owned and built by copy_tablet_ctx_. It is used only for major keys;
  // nullptr means macro-block reuse is disabled.
  ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
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
  int update_major_sstable_reuse_info_(
      const ObITable::TableKey &table_key,
      const ObTabletHandle &tablet_handle,
      ObTableHandleV2 &table_handle);

private:
  bool is_inited_;
  ObBatchSSTableCopyTaskParam param_;
  common::ObSEArray<ObITable::TableKey, MAX_SSTABLE_PER_BATCH> my_keys_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchSSTableCopyTask);
};

// Read-only inputs of the batch copy planner. Built per driver round by the migration
// copy chain driver ops; every pointer is owned by the enclosing migration DAG.
struct ObBatchSSTableCopyPlanParam
{
  ObBatchSSTableCopyPlanParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(op_type), K_(tablet_id),
      KP_(table_info_mgr), KP_(copy_sstable_info_mgr));

  uint64_t tenant_id_;
  ObMigrationOpType::TYPE op_type_;
  common::ObTabletID tablet_id_;
  ObStorageHATableInfoMgr *table_info_mgr_;
  ObStorageHACopySSTableInfoMgr *copy_sstable_info_mgr_;
};

// Low-conflict adapter driven by the migration copy chain driver
// (ObTabletMigrationCopyChainDriverOps). Every driver round asks it whether the next
// sstables of the copy table key array are eligible CG SSTables that can be copied
// inline in one batch task; if not, the driver falls back to the per-sstable copy chain.
class ObBatchSSTableCopyTaskGenerator final
{
public:
  // Plan the batch copy unit starting at start_index: walk forward while the keys are
  // batch-eligible CG SSTables and collect at most MAX_SSTABLE_PER_BATCH of them.
  // unit.is_batch() == false means the sstable at start_index must be copied by the
  // per-sstable copy chain, in which case the unit consumes that one sstable only.
  static int plan_batch_copy_unit(
      const ObBatchSSTableCopyPlanParam &param,
      const common::ObIArray<ObITable::TableKey> &copy_table_key_array,
      const int64_t start_index,
      ObISSTableCopyScanPolicy &scan_policy,
      ObSSTableCopyUnit &unit);

  // Build the batch copy task of batch_keys: parent_task -> batch task -> child_task.
  static int generate_batch_copy_task(
      const ObBatchSSTableCopyTaskParam &param,
      const common::ObIArray<ObITable::TableKey> &batch_keys,
      share::ObIDag *dag,
      share::ObITask *parent_task,
      share::ObITask *child_task);

private:
  static void check_batch_cg_copy_enabled_(
      const ObBatchSSTableCopyPlanParam &param,
      bool &enabled);
  // whether this single CG sstable can be copied by a batch task
  static int check_key_batch_eligible_(
      const ObBatchSSTableCopyPlanParam &param,
      const ObITable::TableKey &key,
      bool &is_eligible);

private:
  ObBatchSSTableCopyTaskGenerator() = delete;
  ~ObBatchSSTableCopyTaskGenerator() = delete;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_BATCH_SSTABLE_COPY_TASK_H
