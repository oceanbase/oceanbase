/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_sstable_copy_ops.h"
#include "ob_storage_ha_macro_block_writer.h"
#include "ob_storage_ha_small_sstable_write_opt.h"
#include "ob_storage_ha_reader.h"
#include "ob_storage_ha_dag.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/ob_storage_schema.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace share;
using namespace compaction;

namespace storage
{

// ---------------------------------------------------------------------------
// get_merge_type / is_sstable_should_rebuild_index / get_space_optimization_mode
// ---------------------------------------------------------------------------

int ObSSTableCopyOps::get_merge_type_(
    const ObMigrationSSTableParam &sstable_param,
    ObMergeType &merge_type)
{
  int ret = OB_SUCCESS;
  merge_type = ObMergeType::INVALID_MERGE_TYPE;
  const ObITable::TableKey &table_key = sstable_param.table_key_;
  if (table_key.is_major_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (table_key.is_minor_sstable()) {
    merge_type = ObMergeType::MINOR_MERGE;
  } else if (table_key.is_inc_major_type_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (table_key.is_ddl_dump_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (table_key.is_inc_major_ddl_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (table_key.is_mds_sstable()) {
    merge_type = ObMergeType::MDS_MINI_MERGE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table key type", K(ret), K(table_key));
  }
  return ret;
}

bool ObSSTableCopyOps::is_sstable_should_rebuild_index(
    const ObMigrationSSTableParam &sstable_param,
    const bool is_leader_restore)
{
  const bool is_shared_without_copy = !is_leader_restore && sstable_param.is_shared_sstable();
  return !sstable_param.is_empty_sstable() && !is_shared_without_copy;
}

int ObSSTableCopyOps::get_space_optimization_mode_(
    const ObMigrationSSTableParam &sstable_param,
    const ObTabletRestoreAction::ACTION restore_action,
    ObSSTableIndexBuilder::ObSpaceOptimizationMode &mode)
{
  int ret = OB_SUCCESS;
  if (sstable_param.table_key_.is_ddl_sstable()
      || sstable_param.table_key_.is_inc_major_ddl_sstable()) {
    mode = ObSSTableIndexBuilder::DISABLE;
  } else if (ObTabletRestoreAction::is_restore_remote_sstable(restore_action)) {
    mode = ObSSTableIndexBuilder::DISABLE;
  } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action)) {
    mode = ObSSTableIndexBuilder::ENABLE;
  } else if (sstable_param.is_small_sstable_) {
    mode = ObSSTableIndexBuilder::ENABLE;
  } else {
    mode = ObSSTableIndexBuilder::DISABLE;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// prepare_data_store_desc -- adapted from
// ObSSTableCopyFinishTask::prepare_data_store_desc_; src_tablet_meta + ls are
// now explicit params, while preserving the current private-transfer-epoch
// and mocked column-group schema rules.
// ---------------------------------------------------------------------------

int ObSSTableCopyOps::prepare_data_store_desc_(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const ObMigrationTabletParam &src_tablet_meta,
    const ObMigrationSSTableParam &sstable_param,
    const int64_t cluster_version,
    const ObTablet *cached_dest_tablet,
    ObWholeDataStoreDesc &out_desc)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObMergeType merge_type;
  const ObStorageSchema *storage_schema = &src_tablet_meta.storage_schema_;
  ObTabletHandle tablet_handle;

  if (OB_UNLIKELY(!tablet_id.is_valid() || cluster_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(cluster_version));
  } else if (sstable_param.table_key_.is_mds_sstable()
      && FALSE_IT(storage_schema = ObMdsSchemaHelper::get_instance().get_storage_schema())) {
  } else if (OB_FAIL(get_merge_type_(sstable_param, merge_type))) {
    LOG_WARN("failed to get merge type", K(ret), K(sstable_param));
  } else if (OB_NOT_NULL(cached_dest_tablet) && FALSE_IT(tablet = const_cast<ObTablet *>(cached_dest_tablet))) {
    // Reuse the dest tablet cached by the batch task -- transfer_seq /
    // snapshot_version / micro_index_clustered / reorganization_scn are constant
    // across a tablet's sstables, so re-fetching per sstable is pure waste
    // (columnar tablets fan out to hundreds of CG sstables). FALSE_IT falls through.
  } else if (OB_ISNULL(tablet) && OB_FAIL(ls.ha_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to ha get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet) && OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id));
  } else {
    const ObITable::TableKey &table_key = sstable_param.table_key_;
    const uint16_t cg_idx = table_key.get_column_group_id();
    const ObStorageColumnGroupSchema *cg_schema = nullptr;
    bool use_mock_cg_schema = false;

    if (!storage_schema->is_row_store() || !table_key.is_column_store_sstable()) {
      int64_t fetch_idx = cg_idx;
      if (HIDDEN_ROWKEY_COLUMN_GROUP_IDX == cg_idx
          && !storage_schema->has_hidden_rowkey_column_group()) {
        fetch_idx = 0;
      } else if (storage_schema->has_hidden_rowkey_column_group()
          && table_key.is_co_sstable()
          && ObCOSSTableBaseType::ROWKEY_CG_TYPE
              == static_cast<ObCOSSTableBaseType>(sstable_param.co_base_type_)) {
        fetch_idx = HIDDEN_ROWKEY_COLUMN_GROUP_IDX;
      }
      if (OB_FAIL(storage_schema->get_cg_schema_with_column_group_idx(
              fetch_idx, cg_schema))) {
        LOG_WARN("failed to get cg schema from storage schema",
            K(ret), K(cg_idx), K(fetch_idx), KPC(storage_schema));
      }
    } else if (table_key.is_co_sstable()
        && ObCOSSTableBaseType::ALL_CG_TYPE
            == static_cast<ObCOSSTableBaseType>(sstable_param.co_base_type_)) {
      if (OB_FAIL(storage_schema->get_cg_schema_with_column_group_idx(
              0 /* base cg idx */, cg_schema))) {
        LOG_WARN("failed to get base cg schema from storage schema",
            K(ret), KPC(storage_schema));
      }
    } else if (OB_UNLIKELY(!src_tablet_meta.mock_rowkey_cg_schema_.is_valid()
        || !src_tablet_meta.mock_single_cg_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mock cg schemas", K(ret), K(src_tablet_meta));
    } else if (HIDDEN_ROWKEY_COLUMN_GROUP_IDX == cg_idx
        || table_key.is_co_sstable()) {
      cg_schema = &src_tablet_meta.mock_rowkey_cg_schema_;
    } else {
      use_mock_cg_schema = true;
      cg_schema = &src_tablet_meta.mock_single_cg_schema_;
    }

    if (OB_SUCC(ret) && OB_ISNULL(cg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get cg schema", K(ret), KPC(storage_schema), K(cg_idx));
    }

    UNUSED(cluster_version);
    int32_t private_transfer_epoch = -1;
    if (OB_SUCC(ret)
        && OB_FAIL(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get private transfer epoch",
          K(ret), "tablet_meta", tablet->get_tablet_meta());
    } else if (OB_SUCC(ret) && OB_FAIL(out_desc.init(
          false /*is_ddl*/,
          *storage_schema,
          ls.get_ls_id(),
          tablet_id,
          merge_type,
          tablet->get_snapshot_version(),
          0 /*cluster_version*/,
          tablet->get_tablet_meta().micro_index_clustered_,
          private_transfer_epoch,
          0 /*concurrent_cnt*/,
          tablet->get_reorganization_scn(),
          sstable_param.table_key_.get_end_scn(),
          cg_schema,
          cg_idx,
          ObExecMode::EXEC_MODE_LOCAL))) {
      LOG_WARN("failed to init data store desc",
          K(ret), K(cg_idx), K(sstable_param), K(cg_schema));
    } else if (OB_SUCC(ret)) {
      const int64_t column_cnt = sstable_param.basic_meta_.column_cnt_;
      if (use_mock_cg_schema) {
        out_desc.get_col_desc().agg_meta_array_.reset();
        LOG_INFO("cannot generate skip index with mocked cg schema",
            K(sstable_param));
      }
      if (OB_FAIL(out_desc.get_col_desc().mock_valid_col_default_checksum_array(
              column_cnt))) {
        LOG_WARN("failed to mock valid column checksum array", K(ret));
      } else if (OB_FAIL(out_desc.get_desc().update_basic_info_from_macro_meta(
                     sstable_param.basic_meta_))) {
        LOG_WARN("failed to update basic info from macro meta", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableCopyOps::init_sstable_index_builder(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const ObMigrationTabletParam &src_tablet_meta,
    const ObMigrationSSTableParam &sstable_param,
    const int64_t cluster_version,
    const ObTabletRestoreAction::ACTION restore_action,
    const ObTablet *cached_dest_tablet,
    ObSSTableIndexBuilder &out_builder)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc desc;
  ObSSTableIndexBuilder::ObSpaceOptimizationMode mode = ObSSTableIndexBuilder::DISABLE;
  if (OB_FAIL(get_space_optimization_mode_(sstable_param, restore_action, mode))) {
    LOG_WARN("failed to get space optimization mode", K(ret), K(sstable_param));
  } else if (OB_FAIL(prepare_data_store_desc_(
                 ls, tablet_id, src_tablet_meta, sstable_param, cluster_version, cached_dest_tablet, desc))) {
    LOG_WARN("failed to prepare data store desc", K(ret), K(tablet_id), K(cluster_version));
  } else if (OB_FAIL(out_builder.init(desc.get_desc(), mode))) {
    LOG_WARN("failed to init sstable index builder", K(ret), K(desc), K(mode));
  }
  return ret;
}

// ---------------------------------------------------------------------------
// copy_macro_range -- migration leaf for the CG batch path.
// ---------------------------------------------------------------------------

int ObSSTableCopyOps::build_copy_macro_block_reader_init_param_(
    const ObPhysicalCopyCtx &copy_ctx,
    const ObMigrationSSTableParam &sstable_param,
    const ObCopyMacroRangeIdInfo &copy_macro_range_id_info,
    ObCopyMacroBlockReaderInitParam &out_init_param)
{
  int ret = OB_SUCCESS;
  out_init_param.tenant_id_ = copy_ctx.tenant_id_;
  out_init_param.ls_id_ = copy_ctx.ls_id_;
  out_init_param.table_key_ = copy_ctx.table_key_;
  out_init_param.is_leader_restore_ = copy_ctx.is_leader_restore_;
  out_init_param.restore_action_ = copy_ctx.restore_action_;
  out_init_param.src_info_ = copy_ctx.src_info_;
  out_init_param.ha_svc_ctx_ = copy_ctx.ha_svc_ctx_;
  out_init_param.restore_base_info_ = copy_ctx.restore_base_info_;
  out_init_param.meta_index_store_ = copy_ctx.meta_index_store_;
  out_init_param.second_meta_index_store_ = copy_ctx.second_meta_index_store_;
  out_init_param.restore_macro_block_id_mgr_ = copy_ctx.restore_macro_block_id_mgr_;
  out_init_param.copy_macro_range_info_ = &copy_macro_range_id_info.range_info_;
  out_init_param.need_check_seq_ = copy_ctx.need_check_seq_;
  out_init_param.ls_rebuild_seq_ = copy_ctx.ls_rebuild_seq_;
  out_init_param.backfill_tx_scn_ = sstable_param.basic_meta_.filled_tx_scn_;
  out_init_param.macro_block_reuse_mgr_ = copy_ctx.macro_block_reuse_mgr_;
  out_init_param.data_version_ = 0;
  out_init_param.copy_macro_block_infos_.reset();

  for (int64_t i = 0;
       OB_SUCC(ret) && i < copy_macro_range_id_info.macro_block_ids_.count();
       ++i) {
    const ObLogicMacroBlockId &logic_id =
        copy_macro_range_id_info.macro_block_ids_.at(i);
    ObCopyMacroBlockInfo copy_macro_info;
    MacroBlockId macro_id;
    int64_t data_checksum = 0;
    copy_macro_info.logical_id_ = logic_id;

    if (OB_ISNULL(copy_ctx.macro_block_reuse_mgr_)) {
      copy_macro_info.data_type_ = ObCopyMacroBlockDataType::MACRO_DATA;
    } else if (OB_FAIL(copy_ctx.macro_block_reuse_mgr_->
                   get_macro_block_reuse_info(
                       logic_id, macro_id, data_checksum))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        copy_macro_info.data_type_ = ObCopyMacroBlockDataType::MACRO_DATA;
      } else {
        LOG_WARN("failed to get macro block reuse info",
            K(ret), K(logic_id), K(copy_ctx));
      }
    } else {
      copy_macro_info.data_type_ = ObCopyMacroBlockDataType::MACRO_META_ROW;
    }

    if (OB_SUCC(ret)
        && OB_FAIL(out_init_param.copy_macro_block_infos_.push_back(
               copy_macro_info))) {
      LOG_WARN("failed to append macro block copy info",
          K(ret), K(copy_macro_info));
    }
  }
  return ret;
}

int ObSSTableCopyOps::copy_macro_range(
    const ObPhysicalCopyCtx &copy_ctx,
    const ObMigrationSSTableParam &sstable_param,
    const ObCopyMacroRangeIdInfo &copy_macro_range_id_info,
    const int64_t task_idx,
    const int64_t copy_task_concurrent_cnt,
    ObMacroBlocksWriteCtx &copied_ctx)
{
  // Unlike the legacy ObPhysicalCopyTask, there is no fetch-retry loop here:
  // the legacy task retried transient read failures in place, while the batch
  // path deliberately fails fast and relies on the dag-level retry (the whole
  // tablet migration dag is rebuilt) as the backstop. Retrying inside a batch
  // task would need to tear down and re-init the reader/writer/rebuilder
  // pipeline mid-sstable, which is exactly the complexity this ops layer
  // avoids. OB_TABLET_NOT_EXIST still propagates unchanged for the batch
  // task's status handling.
  int ret = OB_SUCCESS;
  ObCopyMacroBlockObReader *reader = nullptr;
  ObStorageHALocalMacroBlockWriter writer;
  ObIndexBlockRebuilder index_block_rebuilder;
  ObMacroSeqParam macro_seq_param;
  macro_seq_param.start_ = 0;
  macro_seq_param.seq_type_ = ObMacroSeqParam::SeqType::SEQ_TYPE_INC;
  ObCopyMacroBlockReaderInitParam reader_param;
  const ObCopyMacroRangeInfo &copy_macro_range_info =
      copy_macro_range_id_info.range_info_;
  // ObIndexBlockRebuilder::init takes the task_idx by pointer.
  int64_t local_task_idx = task_idx;

  if (OB_UNLIKELY(task_idx < 0 || copy_task_concurrent_cnt <= 0
                  || OB_ISNULL(copy_ctx.sstable_index_builder_)
                  || OB_ISNULL(copy_ctx.ha_dag_)
                  || OB_ISNULL(copy_ctx.ha_dag_->get_ha_dag_net_ctx()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_idx), K(copy_task_concurrent_cnt), K(copy_ctx));
  } else if (OB_FAIL(index_block_rebuilder.init(
                 *copy_ctx.sstable_index_builder_, macro_seq_param, &local_task_idx,
                 copy_ctx.table_key_))) {
    LOG_WARN("failed to init index block rebuilder", K(ret), K(copy_ctx.table_key_));
  } else if (OB_FAIL(build_copy_macro_block_reader_init_param_(
                 copy_ctx, sstable_param, copy_macro_range_id_info,
                 reader_param))) {
    LOG_WARN("failed to build reader init param", K(ret));
  } else if (!reader_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader param is invalid", K(ret), K(reader_param));
  } else if (OB_ISNULL(reader = MTL_NEW(ObCopyMacroBlockObReader, "BatchMacroObRd"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc macro block reader", K(ret));
  } else if (OB_FAIL(reader->init(reader_param))) {
    LOG_WARN("failed to init macro block reader", K(ret), K(reader_param));
  } else if (OB_FAIL(writer.init(
                 copy_ctx.tenant_id_, copy_ctx.ls_id_, copy_ctx.tablet_id_,
                 copy_ctx.ha_dag_->get_dag_id(), &sstable_param,
                 ObStorageHASmallSSTableWriteOpt(copy_task_concurrent_cnt,
                     copy_macro_range_info.macro_block_count_),
                 reader, &index_block_rebuilder, copy_ctx.extra_info_))) {
    LOG_WARN("failed to init writer", K(ret));
  } else if (OB_FAIL(run_macro_range_writer_pipeline(
                 writer, *copy_ctx.ha_dag_->get_ha_dag_net_ctx(),
                 copy_macro_range_info, copy_ctx.table_key_, copied_ctx))) {
    LOG_WARN("failed to run macro range writer pipeline", K(ret), K(copy_ctx.table_key_));
  }

  if (FAILEDx(index_block_rebuilder.close())) {
    LOG_WARN("failed to close index block rebuilder", K(ret));
  }

  if (OB_NOT_NULL(reader)) {
    MTL_DELETE(ObCopyMacroBlockObReader, "BatchMacroObRd", reader);
    reader = nullptr;
  }
  return ret;
}

int ObSSTableCopyOps::run_macro_range_writer_pipeline(
    ObIStorageHAMacroBlockWriter &writer,
    ObIHADagNetCtx &ha_dag_net_ctx,
    const ObCopyMacroRangeInfo &copy_macro_range_info,
    const ObITable::TableKey &table_key,
    ObMacroBlocksWriteCtx &copied_ctx)
{
  // Caller owns the rebuilder; close it on the caller's teardown path so that
  // close runs even if writer.init failed earlier. Mirrors the original
  // ObPhysicalCopyTask teardown order.
  int ret = OB_SUCCESS;
  if (OB_FAIL(writer.process(copied_ctx, ha_dag_net_ctx))) {
    LOG_WARN("failed to process writer", K(ret), K(table_key));
  } else if (OB_UNLIKELY(copy_macro_range_info.macro_block_count_ != copied_ctx.get_macro_block_count())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("list count not match", K(ret), K(table_key),
              K(copy_macro_range_info),
              K(copied_ctx.get_macro_block_count()), K(copied_ctx));
  }
  return ret;
}

// ---------------------------------------------------------------------------
// finalize_sstable -- mirrors ObCopiedSSTableCreator + ObCopiedEmptySSTableCreator's
// create_sstable bodies. index_builder = nullptr means "empty" path.
// ---------------------------------------------------------------------------

int ObSSTableCopyOps::finalize_sstable(
    const ObMigrationSSTableParam &sstable_param,
    ObSSTableIndexBuilder *index_builder,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &out_handle)
{
  int ret = OB_SUCCESS;
  out_handle.reset();
  SMART_VAR(ObTabletCreateSSTableParam, create_param) {
    if (nullptr == index_builder) {
      // Empty sstable path.
      ObSEArray<MacroBlockId, 1> data_block_ids;
      ObSEArray<MacroBlockId, 1> other_block_ids;
      if (OB_FAIL(create_param.init_for_ha(sstable_param, data_block_ids, other_block_ids))) {
        LOG_WARN("failed to init create_param for empty sstable", K(ret), K(sstable_param));
      }
    } else {
      ObSSTableMergeRes res;
      if (OB_FAIL(index_builder->close(res))) {
        LOG_WARN("failed to close index_builder", K(ret));
      } else if (OB_FAIL(create_param.init_for_ha(sstable_param, res))) {
        LOG_WARN("failed to init create_param", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (sstable_param.table_key_.is_co_sstable()) {
        if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
                create_param, allocator, out_handle))) {
          LOG_WARN("failed to create co sstable", K(ret), K(create_param));
        }
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(
                     create_param, allocator, out_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(create_param));
      }
    }

    // Validate the built sstable's meta against the source migration param,
    // mirroring ObSSTableCopyFinishTask::check_sstable_valid_. Catches a copy
    // that silently diverged (data_checksum / row_count / column_cnt / ...) at
    // copy time instead of leaving it for a later cross-replica checksum
    // mismatch. Runs for empty sstables too (matches the legacy finish task).
    if (OB_SUCC(ret)) {
      ObSSTable *sstable = nullptr;
      ObSSTableMetaHandle meta_hdl;
      if (OB_FAIL(out_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable from handle", K(ret), K(sstable_param));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("built sstable should not be null", K(ret), K(sstable_param));
      } else if (OB_FAIL(sstable->get_meta(meta_hdl))) {
        LOG_WARN("failed to get built sstable meta", K(ret), K(sstable_param));
      } else if (OB_FAIL(ObSSTableMetaChecker::check_sstable_meta(
                     sstable_param, meta_hdl.get_sstable_meta()))) {
        LOG_WARN("built sstable meta mismatch with src migration param",
                 K(ret), K(sstable_param), K(meta_hdl.get_sstable_meta()));
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// copy_one_sstable -- single-SSTable serial path: init builder -> copy the
// prefetched macro ranges -> finalize. This function never issues range RPCs.
// ---------------------------------------------------------------------------

int ObSSTableCopyOps::copy_one_sstable(
    ObPhysicalCopyCtx &copy_ctx,
    ObLS &ls,
    const ObMigrationTabletParam &src_tablet_meta,
    const ObMigrationSSTableParam &sstable_param,
    const ObCopySSTableMacroRangeInfo *sstable_macro_range_info,
    const ObTablet *cached_dest_tablet,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &out_table_handle)
{
  int ret = OB_SUCCESS;
  out_table_handle.reset();

  if (OB_UNLIKELY(copy_ctx.is_leader_restore_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("restore path not supported by ops; caller must use task-local impl",
             K(ret), K(copy_ctx));
  } else if (OB_UNLIKELY(!sstable_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable param", K(ret), K(sstable_param));
  } else if (sstable_param.basic_meta_.table_shared_flag_.is_shared_macro_blocks()
             || sstable_param.is_shared_sstable()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("shared sstable not supported in serial copy_one_sstable",
             K(ret), K(sstable_param));
  } else {
    const bool need_rebuild_index =
        is_sstable_should_rebuild_index(sstable_param, false /*is_leader_restore*/);
    if (need_rebuild_index
        && (OB_ISNULL(sstable_macro_range_info)
            || !sstable_macro_range_info->is_valid()
            || sstable_macro_range_info->copy_table_key_ != sstable_param.table_key_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid prefetched macro range info for sstable",
               K(ret), K(sstable_param), KPC(sstable_macro_range_info));
    } else {
      copy_ctx.table_key_ = sstable_param.table_key_;
      copy_ctx.total_macro_count_ = 0;
      copy_ctx.reuse_macro_count_ = 0;
      copy_ctx.sstable_index_builder_ = nullptr;

      const int64_t t_start = ObTimeUtility::current_time();
      int64_t t_after_init = t_start;
      int64_t t_after_copy = t_start;
      int64_t t_after_finalize = t_start;
      const int64_t range_cnt = need_rebuild_index
          ? sstable_macro_range_info->copy_macro_range_array_.count()
          : 0;

      ObSSTableIndexBuilder *index_builder = nullptr;
      if (need_rebuild_index) {
        index_builder = MTL_NEW(
            ObSSTableIndexBuilder,
            "BatchSSTIdx",
            false /* not use double write buffer */);
        if (OB_ISNULL(index_builder)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate sstable index builder", K(ret), K(sstable_param));
        } else if (OB_FAIL(init_sstable_index_builder(
                       ls, copy_ctx.tablet_id_, src_tablet_meta, sstable_param,
                       0 /*cluster_version*/, ObTabletRestoreAction::RESTORE_NONE,
                       cached_dest_tablet, *index_builder))) {
          LOG_WARN("failed to init sstable index builder", K(ret), K(copy_ctx));
          MTL_DELETE(ObSSTableIndexBuilder, "BatchSSTIdx", index_builder);
        } else {
          copy_ctx.sstable_index_builder_ = index_builder;
        }
      }
      t_after_init = ObTimeUtility::current_time();

      for (int64_t i = 0; OB_SUCC(ret) && i < range_cnt; ++i) {
        const ObCopyMacroRangeIdInfo &cur_range =
            sstable_macro_range_info->copy_macro_range_array_.at(i);
        ObMacroBlocksWriteCtx copied_ctx;
        if (OB_FAIL(copy_macro_range(
                copy_ctx, sstable_param, cur_range, i /*task_idx*/,
                range_cnt /*copy_task_concurrent_cnt*/,
                copied_ctx))) {
          LOG_WARN("failed to copy macro range", K(ret), K(copy_ctx), K(i), K(sstable_param));
        }
        // Keep the legacy finish-task accounting semantics: include any
        // blocks already copied even when this range subsequently reports an
        // error. In particular, OB_TABLET_NOT_EXIST remains unchanged in ret.
        copy_ctx.total_macro_count_ += copied_ctx.get_macro_block_count();
        copy_ctx.reuse_macro_count_ += copied_ctx.use_old_macro_block_count_;
      }
      t_after_copy = ObTimeUtility::current_time();

      if (OB_SUCC(ret)
          && OB_FAIL(finalize_sstable(
              sstable_param, index_builder, allocator, out_table_handle))) {
        LOG_WARN("failed to finalize sstable", K(ret), K(copy_ctx), K(sstable_param));
      }
      t_after_finalize = ObTimeUtility::current_time();

      if (OB_NOT_NULL(index_builder)) {
        copy_ctx.sstable_index_builder_ = nullptr;
        MTL_DELETE(ObSSTableIndexBuilder, "BatchSSTIdx", index_builder);
      }

      LOG_INFO("copy_one_sstable profile",
               "tablet_id", copy_ctx.tablet_id_,
               "table_key", sstable_param.table_key_,
               K(range_cnt),
               "init_us", t_after_init - t_start,
               "copy_macro_us", t_after_copy - t_after_init,
               "finalize_us", t_after_finalize - t_after_copy,
               "total_us", t_after_finalize - t_start);
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
