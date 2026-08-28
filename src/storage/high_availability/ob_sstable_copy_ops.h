/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_COPY_OPS_H
#define OCEANBASE_STORAGE_OB_SSTABLE_COPY_OPS_H

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/ob_i_table.h"
#include "ob_physical_copy_ctx.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_ha_reader.h"  // ObCopyMacroBlockReaderInitParam, ObCopyMacroRangeInfo

namespace oceanbase
{
namespace blocksstable
{
class ObMigrationSSTableParam;
class ObMacroBlocksWriteCtx;
class ObIndexBlockRebuilder;
}

namespace storage
{

class ObLS;
class ObTablet;
struct ObMigrationTabletParam;
class ObIHADagNetCtx;
class ObIStorageHAMacroBlockWriter;

// Static operations used by ObBatchSSTableCopyTask:
//
//   copy one sstable = copy a group of macro ranges (+ index_builder finalize)
//   copy one macro   = atomic leaf
//
// Each operation is a pure static function: inputs in, outputs out, no class
// state. ObBatchSSTableCopyTask calls copy_one_sstable serially for a group of
// CG SSTables; copy_one_sstable in turn loops over their prefetched macro
// ranges.
//
// Scope / limitations (shared-nothing migration only). Restore-specific
// readers/branches are intentionally absent: is_leader_restore is always false and
//     restore_action is always RESTORE_NONE here.
// ObPhysicalCopyCtx carries the current HA service context used by readers.
//
// Macro ranges are prefetched by ObStorageHACopySSTableInfoMgr and owned by the
// enclosing tablet-copy context. copy_one_sstable only borrows a const pointer;
// it never copies the potentially large range array or issues another range
// RPC. A non-empty SSTable must have valid range info whose table
// key matches the source SSTable. The range array itself may be empty; in that
// case the empty index builder is finalized and the final meta check decides
// whether the source changed incompatibly. Empty SSTables may pass nullptr.
//
// ObPhysicalCopyCtx is the shared input container; ops read inputs from it
// and pass dag_id / ha_dag_net_ctx through via ctx.ha_dag_.
class ObSSTableCopyOps final
{
public:
  // Standard source-side macro range size: the single definition shared by
  // range prefetch (ObStorageHACopySSTableInfoMgr) and the CG batch copy
  // thresholds (ob_batch_sstable_copy_task.cpp).
  static constexpr int64_t MACRO_RANGE_MAX_MACRO_COUNT = 128;

  // True when the sstable's macro blocks will actually be copied (so its
  // index needs rebuilding). Empty placeholders and shared-without-copy
  // sstables return false -- their dest sstable is materialized directly
  // from src_sstable_param without an index_builder.
  static bool is_sstable_should_rebuild_index(
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const bool is_leader_restore);

  // Copy one src SSTable end-to-end (serial). The range info is borrowed from
  // ObStorageHACopySSTableInfoMgr. It may be nullptr only when the SSTable does
  // not need an index rebuild (currently an empty SSTable). OB_TABLET_NOT_EXIST
  // from any lower layer is returned unchanged for the batch task to handle.
  static int copy_one_sstable(
      ObPhysicalCopyCtx &copy_ctx,
      ObLS &ls,
      const ObMigrationTabletParam &src_tablet_meta,
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const ObCopySSTableMacroRangeInfo *sstable_macro_range_info,
      const ObTablet *cached_dest_tablet,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &out_table_handle);

private:
  // Pick mode + build desc + init the supplied ObSSTableIndexBuilder.
  // Pre-condition: caller already verified is_sstable_should_rebuild_index()
  // and owns out_builder's lifetime.
  static int init_sstable_index_builder(
      ObLS &ls,
      const common::ObTabletID &tablet_id,
      const ObMigrationTabletParam &src_tablet_meta,
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const int64_t cluster_version,
      const ObTabletRestoreAction::ACTION restore_action,
      const ObTablet *cached_dest_tablet, // caller-cached tablet; null -> fetch
      blocksstable::ObSSTableIndexBuilder &out_builder);

  // Copy one macro range. Migration-only leaf. Skeleton: init
  // ObIndexBlockRebuilder + ObCopyMacroBlockObReader + ObStorageHALocalMacroBlockWriter,
  // drive writer.process via run_macro_range_writer_pipeline, then close
  // rebuilder + free reader. copy_ctx.sstable_index_builder_ collects the
  // rebuilt index entries. Major CGs may reuse local macro blocks through
  // copy_ctx.macro_block_reuse_mgr_.
  // copy_task_concurrent_cnt feeds the writer's ObStorageHASmallSSTableWriteOpt
  // -- the batch path passes the range count of the sstable.
  static int copy_macro_range(
      const ObPhysicalCopyCtx &copy_ctx,
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const ObCopyMacroRangeIdInfo &copy_macro_range_id_info,
      const int64_t task_idx,
      const int64_t copy_task_concurrent_cnt,
      blocksstable::ObMacroBlocksWriteCtx &copied_ctx);

  // Shared tail of the macro-range copy pipeline: drive writer.process and
  // verify the dest macro_block_count matches the src range info. Caller owns
  // reader/writer lifecycles AND must close the rebuilder on its own teardown
  // path. Called from copy_macro_range above.
  static int run_macro_range_writer_pipeline(
      ObIStorageHAMacroBlockWriter &writer,
      ObIHADagNetCtx &ha_dag_net_ctx,
      const ObCopyMacroRangeInfo &copy_macro_range_info,
      const ObITable::TableKey &table_key,
      blocksstable::ObMacroBlocksWriteCtx &copied_ctx);

  // Materialize the dest ObSSTable handle. For empty sstables (no macro
  // blocks copied) pass index_builder = nullptr -- finalize uses empty
  // block lists. For non-empty, finalize calls index_builder->close(res),
  // init_for_ha(src_sstable_param, res), then ObTabletCreateDeleteHelper::create_sstable.
  // CO sstables dispatch through ObCOSSTableV2 specialization automatically.
  static int finalize_sstable(
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      blocksstable::ObSSTableIndexBuilder *index_builder,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &out_handle);

  ObSSTableCopyOps() = delete;
  ~ObSSTableCopyOps() = delete;

  // Mirror of ObSSTableCopyFinishTask::get_merge_type_. mds sstable maps to
  // MDS_MINI_MERGE; everything else to MAJOR/MINOR_MERGE per table_key.
  static int get_merge_type_(
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      compaction::ObMergeType &merge_type);

  // Pick the right ObSpaceOptimizationMode for the dest sstable. ddl /
  // restore_remote_sstable always DISABLE; replace_remote / small_sstable
  // use ENABLE; otherwise DISABLE.
  static int get_space_optimization_mode_(
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const ObTabletRestoreAction::ACTION restore_action,
      blocksstable::ObSSTableIndexBuilder::ObSpaceOptimizationMode &mode);

  // Build the ObWholeDataStoreDesc used to init an ObSSTableIndexBuilder.
  // Reads tablet (for cg/transfer_seq/reorganization_scn) and uses
  // src_tablet_meta's storage_schema (or the mds schema for mds sstables).
  static int prepare_data_store_desc_(
      ObLS &ls,
      const common::ObTabletID &tablet_id,
      const ObMigrationTabletParam &src_tablet_meta,
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const int64_t cluster_version,
      const ObTablet *cached_dest_tablet, // caller-cached tablet; null -> fetch
      blocksstable::ObWholeDataStoreDesc &out_desc);

  // Build ObCopyMacroBlockReaderInitParam from a copy_ctx + sstable_param +
  // copy_macro_range_info. Mirrors
  // ObPhysicalCopyTask::build_copy_macro_block_reader_init_param_ (44x fields).
  static int build_copy_macro_block_reader_init_param_(
      const ObPhysicalCopyCtx &copy_ctx,
      const blocksstable::ObMigrationSSTableParam &sstable_param,
      const ObCopyMacroRangeIdInfo &copy_macro_range_id_info,
      ObCopyMacroBlockReaderInitParam &out_init_param);

};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SSTABLE_COPY_OPS_H
