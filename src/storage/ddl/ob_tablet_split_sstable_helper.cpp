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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_tablet_split_sstable_helper.h"
#include "storage/ddl/ob_tablet_split_task.h"

namespace oceanbase
{
using namespace compaction;
namespace storage
{
// ObSplitIndexBuilderCtx
ObSplitIndexBuilderCtx::ObSplitIndexBuilderCtx() :
  data_store_desc_(nullptr), index_builder_(nullptr)
{}

ObSplitIndexBuilderCtx::~ObSplitIndexBuilderCtx()
{
  reset();
}

void ObSplitIndexBuilderCtx::reset()
{
  data_store_desc_ = nullptr;
  index_builder_ = nullptr;
}

int ObSplitIndexBuilderCtx::assign(const ObSplitIndexBuilderCtx &other) // shallow copy.
{
  data_store_desc_ = other.data_store_desc_;
  index_builder_ = other.index_builder_;
  return OB_SUCCESS;
}

bool ObSplitIndexBuilderCtx::is_valid() const
{
  return data_store_desc_ != nullptr && data_store_desc_->is_valid() && index_builder_ != nullptr;
}

// HELPER INIT PARAM.
ObSSTSplitHelperInitParam::ObSSTSplitHelperInitParam()
  : param_(nullptr),
    context_(nullptr),
    table_key_(),
    sstable_(nullptr)
{
}

ObSSTSplitHelperInitParam::~ObSSTSplitHelperInitParam()
{
  param_ = nullptr;
  context_ = nullptr;
  table_key_.reset();
  sstable_ = nullptr;
}

bool ObSSTSplitHelperInitParam::is_valid() const
{
  bool is_valid = nullptr != param_ && nullptr != context_ && table_key_.is_valid();
  if (is_valid) {
    if (table_key_.is_mds_sstable()) {
      is_valid = sstable_ == nullptr;
    } else {
      is_valid = sstable_ != nullptr;
    }
  }
  return is_valid;
}

ObColSSTSplitHelperInitParam::ObColSSTSplitHelperInitParam()
  : ObSSTSplitHelperInitParam(), end_partkey_rowids_()
{
}

ObColSSTSplitHelperInitParam::~ObColSSTSplitHelperInitParam()
{
  end_partkey_rowids_.reset();
}

bool ObColSSTSplitHelperInitParam::is_valid() const
{
  bool is_valid = ObSSTSplitHelperInitParam::is_valid()
    && sstable_ != nullptr && sstable_->is_column_store_sstable();
  if (is_valid) {
    if (sstable_->is_co_sstable()) {
      is_valid = end_partkey_rowids_.empty();
    } else {
      is_valid = end_partkey_rowids_.count() == param_->dest_tablets_id_.count();
    }
  }
  return is_valid;
}

// SSTABLE SPLIT HELPER.
ObSSTableSplitHelper::ObSSTableSplitHelper()
    : is_inited_(false),
    param_(nullptr),
    context_(nullptr)
{
}

ObSSTableSplitHelper::~ObSSTableSplitHelper()
{
  context_ = nullptr;
  param_ = nullptr;
  is_inited_ = false;
}

int ObSSTableSplitHelper::prepare_index_builder_ctxs(
    ObIAllocator &allocator,
    const ObTabletSplitParam &param,
    const ObTabletSplitCtx &split_ctx,
    const ObSSTable &sstable,
    const ObStorageSchema &clipped_storage_schema,
    const ObStorageColumnGroupSchema *cg_schema,
    ObIArray<ObSplitIndexBuilderCtx> &index_builder_ctx_arr)
{
  int ret = OB_SUCCESS;
  index_builder_ctx_arr.reset();
  if (OB_UNLIKELY(!param.is_valid()
      || !split_ctx.is_valid()
      || !sstable.is_valid()
      || !clipped_storage_schema.is_valid()
      || (sstable.is_column_store_sstable() && (cg_schema == nullptr || !cg_schema->is_valid())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param), K(sstable), K(clipped_storage_schema), KPC(cg_schema));
  } else {
    ObTabletHandle tablet_handle;
    compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() ?
        compaction::ObExecMode::EXEC_MODE_OUTPUT : compaction::ObExecMode::EXEC_MODE_LOCAL;
    const share::SCN split_reorganization_scn = split_ctx.split_scn_.is_valid() ? split_ctx.split_scn_ : SCN::min_scn()/*use min_scn to avoid invalid*/;
    const uint16_t table_cg_idx = sstable.get_column_group_id();
    for (int64_t j = 0; OB_SUCC(ret) && j < param.dest_tablets_id_.count(); j++) {
      tablet_handle.reset();
      ObSplitIndexBuilderCtx index_builder_ctx;
      const ObTabletID dst_tablet_id = param.dest_tablets_id_.at(j);
      const ObMergeType merge_type = sstable.is_major_sstable() ? MAJOR_MERGE : MINOR_MERGE;
      const int64_t snapshot_version = sstable.is_major_sstable() ?
        sstable.get_snapshot_version() : sstable.get_end_scn().get_val_for_tx();
      int32_t transfer_epoch = -1;
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(split_ctx.ls_handle_, dst_tablet_id, tablet_handle))) {
        LOG_WARN("get tablet failed", K(ret));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_private_transfer_epoch(transfer_epoch))) {
        LOG_WARN("failed to get transfer epoch", K(ret), "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
      } else if (OB_ISNULL(index_builder_ctx.data_store_desc_ = OB_NEWx(ObWholeDataStoreDesc, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create data store desc", K(ret));
      } else if (OB_FAIL(index_builder_ctx.data_store_desc_->init(
          true/*is_ddl*/, clipped_storage_schema, param.ls_id_,
          dst_tablet_id, merge_type, snapshot_version, param.data_format_version_,
          tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_,
          transfer_epoch/*use dest_tablet_id's transfer_epoch*/,
          0/*concurrent_cnt*/,
          split_reorganization_scn, sstable.get_end_scn(),
          cg_schema/*cg_schema*/,
          table_cg_idx/*table_cg_idx*/,
          exec_mode))) {
        LOG_WARN("fail to init data store desc", K(ret), K(dst_tablet_id), K(param));
      } else if (OB_ISNULL(index_builder_ctx.index_builder_ = OB_NEWx(ObSSTableIndexBuilder, &allocator, false/*use double write buffer*/))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create index builder", K(ret));
      } else if (OB_FAIL(index_builder_ctx.index_builder_->init(index_builder_ctx.data_store_desc_->get_desc(), ObSSTableIndexBuilder::DISABLE))) {
        LOG_WARN("init sstable index builder failed", K(ret));
      } else if (OB_FAIL(index_builder_ctx_arr.push_back(index_builder_ctx))) {
        LOG_WARN("push back index builder ctx failed", K(ret));
      }
      if (OB_FAIL(ret)) {
        // other newly-allocated sstable index builders will be deconstructed when deconstruct the ctx.
        destroy_split_object(allocator, index_builder_ctx.index_builder_);
        destroy_split_object(allocator, index_builder_ctx.data_store_desc_);
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < index_builder_ctx_arr.count(); i++) {
        destroy_split_object(allocator, index_builder_ctx_arr.at(i).index_builder_);
        destroy_split_object(allocator, index_builder_ctx_arr.at(i).data_store_desc_);
      }
    }
  }
  return ret;
}

// ObSSTableSplitWriteHelper
ObSSTableSplitWriteHelper::ObSSTableSplitWriteHelper()
  : ObSSTableSplitHelper(), arena_allocator_("SplitHelper", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    sstable_(nullptr), default_row_(), split_point_macros_(),
    index_read_info_(nullptr), index_builder_ctx_arr_()
{
}

ObSSTableSplitWriteHelper::~ObSSTableSplitWriteHelper()
{
  sstable_ = nullptr;
  default_row_.reset();
  split_point_macros_.reset();
  index_read_info_ = nullptr;
  for (int64_t i = 0; i < index_builder_ctx_arr_.count(); i++) {
    destroy_split_object(arena_allocator_, index_builder_ctx_arr_.at(i).index_builder_);
    destroy_split_object(arena_allocator_, index_builder_ctx_arr_.at(i).data_store_desc_);
  }
  arena_allocator_.reset();
}

int ObSSTableSplitWriteHelper::inner_init_common(const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("TmpInitSplitW", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(init_param));
  } else {
    uint16_t table_cg_idx = 0;
    const ObStorageSchema *clipped_storage_schema = nullptr;
    const ObStorageColumnGroupSchema *cg_schema = nullptr;
    ObArray<ObColDesc> multi_version_cols_desc;
    ObDatumRow tmp_default_row;
    const ObTabletSplitParam &split_param = *init_param.param_;
    const ObTabletSplitCtx &split_ctx = *init_param.context_;
    if (OB_FAIL(ObTabletSplitUtil::get_clipped_storage_schema_on_demand(
        tmp_arena,
        split_param.source_tablet_id_,
        *init_param.sstable_,
        *split_ctx.mds_storage_schema_,
        clipped_storage_schema))) {
      LOG_WARN("get storage schema via sstable failed", K(ret));
    } else if (OB_UNLIKELY(nullptr == clipped_storage_schema || !clipped_storage_schema->is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("sys error to get a null schema", K(ret), KPC(init_param.sstable_), KPC(split_ctx.mds_storage_schema_), KPC(clipped_storage_schema));
    } else if (OB_FAIL(prepare_sstable_cg_infos(
        *clipped_storage_schema,
        *init_param.sstable_,
        cg_schema,
        table_cg_idx,
        multi_version_cols_desc))) {
      LOG_WARN("prepare cg infos failed", K(ret), KPC(init_param.sstable_), KPC(clipped_storage_schema));
    } else if (OB_FAIL(tmp_default_row.init(tmp_arena, multi_version_cols_desc.count()))) { // tmp arena to alloc, and reset after.
      LOG_WARN("init tmp default row failed", K(ret));
    } else if (OB_FAIL(clipped_storage_schema->get_orig_default_row(multi_version_cols_desc, true/*need_trim*/, tmp_default_row))) {
      LOG_WARN("init default row failed", K(ret), KPC(sstable_), KPC(clipped_storage_schema));
    } else if (OB_FAIL(default_row_.init(arena_allocator_, multi_version_cols_desc.count()))) {
      LOG_WARN("init default row failed", K(ret));
    } else if (OB_FAIL(default_row_.deep_copy(tmp_default_row/*src*/, arena_allocator_))) {
      LOG_WARN("failed to deep copy default row", K(ret), KPC(sstable_), KPC(clipped_storage_schema));
    } else if (OB_FAIL(ObLobManager::fill_lob_header(arena_allocator_, multi_version_cols_desc, default_row_))) {
      LOG_WARN("fail to fill lob header for default row", K(ret));
    } else if (OB_FAIL(prepare_index_builder_ctxs(
        arena_allocator_,
        split_param,
        split_ctx,
        *init_param.sstable_,
        *clipped_storage_schema,
        cg_schema,
        index_builder_ctx_arr_))) {
    // TODO, optimize the mem by allocating when task processing.
      LOG_WARN("prepare index builder ctxs failed", K(ret));
    }
  }
  return ret;
}

int ObSSTableSplitWriteHelper::prepare_macro_seq_param(
    const int64_t task_idx,
    ObIArray<ObMacroSeqParam> &macro_seq_param_arr)
{
  int ret = OB_SUCCESS;
  macro_seq_param_arr.reset();
  ObMacroDataSeq macro_start_seq(0);
  ObSSTableMetaHandle meta_handle;
  meta_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sstable_->get_meta(meta_handle))) {
    LOG_WARN("get sstable meta failed", K(ret));
  } else if (OB_FAIL(macro_start_seq.set_sstable_seq(meta_handle.get_sstable_meta().get_sstable_seq()))) {
    LOG_WARN("set sstable logical seq failed", K(ret), "sst_meta", meta_handle.get_sstable_meta());
  } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_idx))) {
    LOG_WARN("set parallel degree failed", K(ret));
  } else {
    ObMacroSeqParam macro_seq_param;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = macro_start_seq.macro_data_seq_;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      if (OB_FAIL(macro_seq_param_arr.push_back(macro_seq_param))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableSplitWriteHelper::prepare_macro_block_writer(
  ObIAllocator &allocator,
  const int64_t task_idx,
  const ObIArray<ObMacroSeqParam> &macro_seq_param_arr,
  ObIArray<ObDataStoreDesc *> &data_desc_arr,
  ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr)
{
  int ret = OB_SUCCESS;
  data_desc_arr.reset();
  macro_block_writer_arr.reset();
  const ObIArray<ObSplitIndexBuilderCtx> &index_builder_ctx_arr = index_builder_ctx_arr_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(task_idx < 0 || task_idx >= context_->data_split_ranges_.count()
    || macro_seq_param_arr.count() != index_builder_ctx_arr.count()
    || macro_seq_param_arr.count() != param_->dest_tablets_id_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected error", K(ret), K(task_idx),
      K(index_builder_ctx_arr.count()), K(macro_seq_param_arr.count()), KPC(param_), KPC(context_));
  } else {
    ObPreWarmerParam pre_warm_param;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      pre_warm_param.reset();
      ObDataStoreDesc *data_desc = nullptr;
      ObMacroBlockWriter *macro_block_writer = nullptr;
      ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
      const ObTabletID &dst_tablet_id = param_->dest_tablets_id_.at(i);
      const ObMergeType merge_type = sstable_->is_major_sstable() ? MAJOR_MERGE : MINOR_MERGE;
      const int64_t snapshot_version = sstable_->is_major_sstable() ?
          sstable_->get_snapshot_version() : sstable_->get_end_scn().get_val_for_tx();
      compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() ?
          compaction::ObExecMode::EXEC_MODE_OUTPUT : compaction::ObExecMode::EXEC_MODE_LOCAL;
      const ObSplitIndexBuilderCtx &index_builder_ctx = index_builder_ctx_arr.at(i);
      const ObMacroSeqParam &macro_seq_param = macro_seq_param_arr.at(i);
      if (OB_UNLIKELY(!index_builder_ctx.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(i), K(index_builder_ctx));
      } else if (OB_ISNULL(data_desc = OB_NEWx(ObDataStoreDesc, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(data_desc->shallow_copy(index_builder_ctx.data_store_desc_->get_desc()))) {
        LOG_WARN("shallow copy data store desc failed", K(ret));
      } else if (OB_FALSE_IT(data_desc->sstable_index_builder_ = index_builder_ctx.index_builder_)) {
      } else if (OB_FAIL(data_desc_arr.push_back(data_desc))) {
        LOG_WARN("push back data desc failed", K(ret));
      } else if (OB_ISNULL(macro_block_writer = OB_NEWx(ObMacroBlockWriter, &allocator, true/*is_need_macro_buffer*/))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(pre_warm_param.init(param_->ls_id_, dst_tablet_id))) {
        LOG_WARN("failed to init pre warm param", K(ret), K(dst_tablet_id), KPC(param_));
      } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(*data_desc, object_cleaner))) {
        LOG_WARN("failed to get cleaner from data store desc", K(ret));
      } else if (OB_FAIL(macro_block_writer->open(*data_desc, task_idx/*parallel_idx*/,
          macro_seq_param, pre_warm_param, *object_cleaner))) {
        LOG_WARN("open macro_block_writer failed", K(ret), KPC(data_desc));
      } else if (OB_FAIL(macro_block_writer_arr.push_back(macro_block_writer))) {
        LOG_WARN("push back failed", K(ret));
      }
      if (OB_FAIL(ret)) {
        // allocated memory in array will be freed by the caller.
        destroy_split_object(allocator, macro_block_writer);
      }
    }
  }
  return ret;
}

int ObSSTableSplitWriteHelper::prepare_write_context(
    ObIAllocator &allocator,
    const int64_t task_idx,
    const ObStorageSchema *&clipped_storage_schema,
    ObIArray<ObDataStoreDesc *> &data_desc_arr,
    ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr)
{
  int ret = OB_SUCCESS;
  clipped_storage_schema = nullptr;
  data_desc_arr.reset();
  macro_block_writer_arr.reset();
  ObArray<ObMacroSeqParam> macro_seq_param_arr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY((param_->can_reuse_macro_block_ && task_idx != 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_idx), KPC(param_), KPC(context_));
  } else if (OB_UNLIKELY(!param_->can_reuse_macro_block_ &&
      (task_idx < 0 || task_idx >= context_->data_split_ranges_.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_idx), KPC(param_), KPC(context_));
  } else if (OB_FAIL(ObTabletSplitUtil::get_clipped_storage_schema_on_demand(
      allocator, param_->source_tablet_id_, *sstable_, *context_->mds_storage_schema_, clipped_storage_schema))) {
    LOG_WARN("get storage schema via sstable failed", K(ret));
  } else if (OB_UNLIKELY(nullptr == clipped_storage_schema || !clipped_storage_schema->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("sys error to get a null schema", K(ret), KPC(sstable_), KPC(context_->mds_storage_schema_), KPC(clipped_storage_schema));
  } else if (OB_FAIL(prepare_macro_seq_param(task_idx, macro_seq_param_arr))) {
    LOG_WARN("prepare macro seq param failed", K(ret));
  } else if (OB_FAIL(prepare_macro_block_writer(allocator, task_idx, macro_seq_param_arr,
      data_desc_arr, macro_block_writer_arr))) {
    LOG_WARN("prepare macro block writer failed", K(ret));
  }
  return ret;
}

int ObSSTableSplitWriteHelper::build_create_sstable_param(
    const int64_t dest_tablet_index,
    ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  ObSSTableMergeRes res;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(dest_tablet_index < 0
      || dest_tablet_index >= index_builder_ctx_arr_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), K(index_builder_ctx_arr_));
  } else if (OB_ISNULL(index_builder_ctx_arr_.at(dest_tablet_index).index_builder_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("index builder is null", K(ret), K(dest_tablet_index), K(index_builder_ctx_arr_));
  } else if (OB_FAIL(index_builder_ctx_arr_.at(dest_tablet_index).index_builder_->close(res))) {
    LOG_WARN("close sstable index builder failed", K(ret));
  } else if (OB_FAIL(sstable_->get_meta(meta_handle))) {
    LOG_WARN("get sstable meta failed", K(ret));
  } else {
    const ObTabletID &dst_tablet_id = param_->dest_tablets_id_.at(dest_tablet_index);
    const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
    if (OB_FAIL(create_sstable_param.init_for_split(dst_tablet_id, *sstable_, basic_meta,
                                                    basic_meta.schema_version_,
                                                    ObArray<MacroBlockId>(),
                                                    res))) {
      LOG_WARN("init sstable param fail", K(ret), K(dst_tablet_id), K(sstable_->get_key()), K(basic_meta), K(res));
    }
  }
  return ret;
}

int ObSSTableSplitWriteHelper::fill_tail_column_datums(
    const blocksstable::ObDatumRow &scan_row,
    blocksstable::ObDatumRow &write_row)
{
  int ret = OB_SUCCESS;
  write_row.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!scan_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(scan_row));
  } else if (OB_UNLIKELY(scan_row.get_column_count() > default_row_.get_column_count()
                      || default_row_.get_column_count() != write_row.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(scan_row), K(default_row_), K(write_row));
  } else if (OB_FAIL(write_row.copy_attributes_except_datums(scan_row))) {
    LOG_WARN("copy attribute except storage datums failed", K(ret), K(scan_row));
  } else {
    write_row.count_ = default_row_.get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < default_row_.get_column_count(); i++) {
      if (i < scan_row.get_column_count()) {
        // scan columns number exceeds columns number of the macro block, the scanner
        // will return default NOP datums.
        const ObStorageDatum &scan_datum = scan_row.storage_datums_[i];
        if (sstable_->is_major_sstable() && scan_datum.is_nop()) {
          write_row.storage_datums_[i] = default_row_.storage_datums_[i];
        } else {
          write_row.storage_datums_[i] = scan_datum;
        }
      } else if (sstable_->is_major_sstable()) {
        write_row.storage_datums_[i] = default_row_.storage_datums_[i];
      } else {
        write_row.storage_datums_[i].set_nop();
      }
    }
  }
  return ret;
}

int ObSSTableSplitWriteHelper::process_macro_blocks(
    ObIAllocator &allocator,
    const ObStorageSchema &clipped_storage_schema,
    const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr)
{
  int ret = OB_SUCCESS;
  ObDatumRow write_row;
  int64_t rewrite_row_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(macro_block_writer_arr.count() != param_->dest_tablets_id_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), "writer count", macro_block_writer_arr.count(), KPC(param_));
  } else if (OB_FAIL(write_row.init(allocator, default_row_.get_column_count()))) {
    LOG_WARN("init write row failed", K(ret));
  } else {
    SMART_VAR(ObSplitReuseBlockIter, reuse_block_iter) {
    ObDatumRange whole_range;
    whole_range.set_whole_range();
    ObSplitScanParam scan_param(param_->table_id_, *(context_->tablet_handle_.get_obj()), whole_range,
      clipped_storage_schema, param_->data_format_version_);
    if (OB_FAIL(reuse_block_iter.init(allocator, scan_param, this))) {
      LOG_WARN("init reuse block iter failed", K(ret));
    } else {
      ObDataMacroBlockMeta macro_meta;
      ObMacroBlockDesc data_macro_desc;
      data_macro_desc.macro_meta_ = &macro_meta;
      while (OB_SUCC(ret)) { // iter macro block.
        data_macro_desc.reuse();
        bool can_reuse = false;
        int64_t dest_tablet_index = -1;
        const ObMicroBlockData *clustered_micro_block_data = nullptr;
        if (OB_FAIL(reuse_block_iter.get_next_macro_block(data_macro_desc, clustered_micro_block_data, dest_tablet_index, can_reuse))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next macro block failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (can_reuse) {
          if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= macro_block_writer_arr.count())) {
            ret = OB_ERR_SYS;
            LOG_WARN("error sys", K(ret), K(dest_tablet_index), K(macro_block_writer_arr.count()));
          } else if (OB_FAIL(macro_block_writer_arr.at(dest_tablet_index)->append_macro_block(data_macro_desc, clustered_micro_block_data))) {
            LOG_WARN("append macro row failed", K(ret));
          } else {
            (void) ATOMIC_AAFx(&context_->row_inserted_, data_macro_desc.row_count_, 0/*unused id*/);
            LOG_TRACE("process current macro block finish", K(ret), K(dest_tablet_index), K(data_macro_desc), "table_key", sstable_->get_key());
          }
        } else if (OB_FAIL(split_point_macros_.push_back(data_macro_desc.macro_block_id_))) {
	  LOG_WARN("push back failed", K(ret));
	} else {
          while (OB_SUCC(ret)) { // iter micro block.
            const ObMicroBlock *cur_micro_block = nullptr;
            if (OB_FAIL(reuse_block_iter.get_next_micro_block(cur_micro_block, dest_tablet_index, can_reuse))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("get next micro block failed", K(ret));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else if (can_reuse) {
              if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= macro_block_writer_arr.count())) {
                ret = OB_ERR_SYS;
                LOG_WARN("error sys", K(ret), K(dest_tablet_index), K(macro_block_writer_arr.count()));
              } else if (OB_FAIL(macro_block_writer_arr.at(dest_tablet_index)->append_micro_block(*cur_micro_block, &data_macro_desc))) {
                LOG_WARN("append micro block failed", K(ret), K(dest_tablet_index), K(data_macro_desc), KPC(cur_micro_block));
              } else {
                (void) ATOMIC_AAFx(&context_->row_inserted_, cur_micro_block->header_.row_count_, 0/*unused id*/);
                LOG_TRACE("append micro block successfully", K(ret), K(dest_tablet_index), K(data_macro_desc), KPC(cur_micro_block), "table_key", sstable_->get_key());
              }
            } else {
              const ObDatumRow *cur_row = nullptr;
              while (OB_SUCC(ret)) { // iter row.
                if (OB_FAIL(reuse_block_iter.get_next_row(cur_row, dest_tablet_index))) {
                  if (OB_ITER_END != ret) {
                    LOG_WARN("get next row failed", K(ret));
                  } else {
                    ret = OB_SUCCESS;
                    break;
                  }
                } else if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= macro_block_writer_arr.count())) {
                  ret = OB_ERR_SYS;
                  LOG_WARN("error sys", K(ret), K(dest_tablet_index), K(macro_block_writer_arr.count()));
                } else if (OB_FAIL(fill_tail_column_datums(*cur_row, write_row))) {
                  LOG_WARN("fill tail column datums failed", K(ret));
                } else if (OB_FAIL(macro_block_writer_arr.at(dest_tablet_index)->append_row(write_row))) {
                  LOG_WARN("append row failed", K(ret), KPC(cur_row), K(write_row));
                } else {
                  if (++rewrite_row_cnt % 100 == 0) {
                    (void) ATOMIC_AAFx(&context_->row_inserted_, 100, 0/*unused id*/);
                  }
                  LOG_TRACE("append row successfully", K(ret), K(dest_tablet_index), KPC(cur_row), "table_key", sstable_->get_key());
                }
              } // end iter row.
            }
          } // end iter micro block.
        }
      } // end iter macro block.
    }
    } // end SMART_VAR.
  }
  (void) ATOMIC_AAFx(&context_->row_inserted_, rewrite_row_cnt % 100, 0/*unused id*/);
  return ret;
}

int ObSSTableSplitWriteHelper::process_rows(
    ObIAllocator &allocator,
    const ObStorageSchema &clipped_storage_schema,
    const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
    const ObDatumRange &query_range)
{
  int ret = OB_SUCCESS;
  ObDatumRow write_row;
  ObTabletSplitMdsUserData src_split_data;
  ObSEArray<ObTabletSplitMdsUserData, 2> dst_split_datas;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(param_->dest_tablets_id_.count() != macro_block_writer_arr.count()
    || !query_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(query_range), K(macro_block_writer_arr), KPC(param_));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::prepare_calc_split_dst(
          *context_->ls_handle_.get_ls(),
          *context_->tablet_handle_.get_obj(),
          ObTimeUtility::current_time() + (1 + param_->dest_tablets_id_.count()) * ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
          src_split_data,
          dst_split_datas))) {
    LOG_WARN("failed to prepare calc split dst", K(ret), KPC(param_));
  } else if (OB_FAIL(write_row.init(allocator, default_row_.get_column_count()))) {
    LOG_WARN("init row failed", K(ret), K(default_row_));
  } else {
    // rewrite each row.
    ObRowScan row_scan_iter;
    ObSplitScanParam row_scan_param(param_->table_id_, *(context_->tablet_handle_.get_obj()), query_range,
      clipped_storage_schema, param_->data_format_version_);
    if (OB_FAIL(row_scan_iter.init(row_scan_param, *sstable_))) {
      LOG_WARN("init row scan iterator failed", K(ret));
    } else {
      ObArenaAllocator new_row_allocator;
      int64_t tmp_row_inserted = 0;
      const ObITableReadInfo &rowkey_read_info = context_->tablet_handle_.get_obj()->get_rowkey_read_info();
      const int64_t schema_rowkey_cnt = rowkey_read_info.get_schema_rowkey_count();
      while (OB_SUCC(ret)) { // exit when iter row end.
        new_row_allocator.reuse();
        const ObDatumRow *datum_row = nullptr;
        ObDatumRowkey rowkey;
        ObTabletID tablet_id;
        int64_t dst_idx = 0;
        if (OB_FAIL(row_scan_iter.get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(rowkey.assign(datum_row->storage_datums_, schema_rowkey_cnt))) {
          LOG_WARN("Failed to assign rowkey", K(ret), K(schema_rowkey_cnt));
        } else if (OB_FAIL(src_split_data.calc_split_dst(rowkey_read_info, dst_split_datas, rowkey, tablet_id, dst_idx))) {
          LOG_WARN("failed to calc split dst tablet", K(ret));
        } else {
          bool is_row_append = false;
          for (int64_t i = 0; OB_SUCC(ret) && !is_row_append && i < param_->dest_tablets_id_.count(); i++) {
            if (param_->dest_tablets_id_.at(i) == tablet_id) {
              if (OB_FAIL(fill_tail_column_datums(*datum_row, write_row))) {
                LOG_WARN("fill tail column datums failed", K(ret));
              } else if (OB_FAIL(macro_block_writer_arr.at(i)->append_row(write_row))) {
                LOG_WARN("append row failed", K(ret), KPC(datum_row), K(write_row));
              }

              if (OB_SUCC(ret)) {
                is_row_append = true;
              }
            }
          }
          if (!is_row_append) {
            // defensive code.
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
            LOG_WARN("append row failed", K(ret), K(tablet_id), K(write_row), KPC(datum_row));
          } else if (++tmp_row_inserted % 100 == 0) {
            (void) ATOMIC_AAFx(&context_->row_inserted_, 100, 0/*placeholder*/);
          }
        }
      }
      (void) ATOMIC_AAFx(&context_->row_inserted_, tmp_row_inserted % 100, 0/*unused id*/);
    }
  }
  return ret;
}

int ObSSTableSplitWriteHelper::split_data(
      ObIAllocator &allocator,
      const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ObDataStoreDesc *> data_desc_arr;
  ObArray<ObMacroBlockWriter *> macro_block_writer_arr;
  const ObStorageSchema *clipped_storage_schema = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(prepare_write_context(allocator, task_idx, clipped_storage_schema, data_desc_arr, macro_block_writer_arr))) {
    LOG_WARN("prepare context failed", K(ret), K(task_idx), KPC(this));
  } else if (param_->can_reuse_macro_block_) {
    if (OB_FAIL(process_macro_blocks(allocator, *clipped_storage_schema, macro_block_writer_arr))) {
      LOG_WARN("failed to process macro blocks", K(ret), K(task_idx), KPC(this));
    }
  } else {
    if (OB_FAIL(process_rows(allocator, *clipped_storage_schema, macro_block_writer_arr, context_->data_split_ranges_.at(task_idx)))) {
      LOG_WARN("failed to process rows", K(ret), K(task_idx), KPC(this));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_writer_arr.count(); i++) {
    if (OB_ISNULL(macro_block_writer_arr.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(task_idx), KPC(this));
    } else if (OB_FAIL(macro_block_writer_arr.at(i)->close())) {
      LOG_WARN("close macro block writer failed", K(ret), K(task_idx), KPC(this));
    }
  }
  // free.
  destroy_split_array(allocator, macro_block_writer_arr);
  destroy_split_array(allocator, data_desc_arr);
  return ret;
}

int ObSSTableSplitWriteHelper::generate_sstable()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      const int64_t dest_tablet_index = i;
      HEAP_VAR(ObTabletCreateSSTableParam, create_sstable_param) {
      if (OB_FAIL(build_create_sstable_param(dest_tablet_index, create_sstable_param))) {
        LOG_WARN("build create sstable param failed", K(ret), KPC(sstable_));
      } else if (OB_FAIL(context_->generate_sstable(dest_tablet_index, create_sstable_param))) {
        LOG_WARN("failed to generate sstable", K(ret), KPC(sstable_));
      }
      } // HEAP_VAR
    }
  }
  return ret;
}


// ObRowSSTableSplitWriteHelper
ObRowSSTableSplitWriteHelper::ObRowSSTableSplitWriteHelper()
  : ObSSTableSplitWriteHelper(), end_partkeys_()
{
}

ObRowSSTableSplitWriteHelper::~ObRowSSTableSplitWriteHelper()
{
  end_partkeys_.reset();
}

int ObRowSSTableSplitWriteHelper::prepare_split_partkeys(
    const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(init_param));
  } else {
    ObTabletHandle tablet_handle;
    ObTabletSplitMdsUserData data;
    ObDatumRowkey mds_partkey, high_bound;
    const ObITableReadInfo &rowkey_read_info = init_param.context_->tablet_handle_.get_obj()->get_rowkey_read_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < init_param.param_->dest_tablets_id_.count(); i++) {
      int cmp_ret = 0;
      high_bound.reset();
      mds_partkey.reset();
      data.reset();
      tablet_handle.reset();
      const ObTabletID &tablet_id = init_param.param_->dest_tablets_id_.at(i);
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(init_param.context_->ls_handle_, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
        LOG_WARN("failed to get split data", K(ret));
      } else if (OB_FAIL(data.get_end_partkey(mds_partkey))) {
        LOG_WARN("failed to get end partkey", K(ret), K(tablet_handle.get_obj()->get_tablet_meta()));
      } else if (OB_FAIL(mds_partkey.deep_copy(high_bound, arena_allocator_))) {
        LOG_WARN("failed to deep copy", K(ret));
      } else if (init_param.param_->can_reuse_macro_block_ && i > 0) { // can check part key order.
        // local index's part key order is not same as primary key, can not check the part key order via the index tablet's rowkey_read_info.
        if (OB_FAIL(high_bound.compare(end_partkeys_.at(i - 1), rowkey_read_info.get_datum_utils(), cmp_ret))) {
          LOG_WARN("compare failed", K(ret), K(i),
              "dest_tablets_id", init_param.param_->dest_tablets_id_,
              K(high_bound), "prev_bound", end_partkeys_.at(i - 1), K(rowkey_read_info));
        } else if (OB_UNLIKELY(cmp_ret <= 0)) {
          // check in ASC rowkey order.
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet bound arr not in asc order", K(ret), K(cmp_ret), K(i),
            "dest_tablets_id", init_param.param_->dest_tablets_id_,
            K(high_bound), "prev_bound", end_partkeys_.at(i - 1), K(rowkey_read_info));
        }
      }
      if (FAILEDx(end_partkeys_.push_back(high_bound))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}


int ObRowSSTableSplitWriteHelper::init(const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(init_param));
  } else if (OB_FAIL(inner_init_common(init_param))) {
    LOG_WARN("inner init common failed", K(ret));
  } else if (OB_FAIL(prepare_split_partkeys(init_param))) {
    LOG_WARN("prepare split partkeys failed", K(ret));
  } else {
    param_ = init_param.param_;
    context_ = init_param.context_;
    sstable_ = init_param.sstable_;
    index_read_info_ = &init_param.context_->tablet_handle_.get_obj()->get_rowkey_read_info();
    is_inited_ = true;
  }
  return ret;
}

int ObRowSSTableSplitWriteHelper::prepare_sstable_cg_infos(
    const ObStorageSchema &clipped_storage_schema,
    const ObSSTable &sstable,
    const ObStorageColumnGroupSchema *&cg_schema,
    uint16_t &table_cg_idx,
    ObIArray<ObColDesc> &multi_version_cols_desc)
{
  UNUSED(sstable);
  int ret = OB_SUCCESS;
  cg_schema = nullptr;
  table_cg_idx = 0;
  multi_version_cols_desc.reset();
  if (OB_UNLIKELY(!clipped_storage_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(clipped_storage_schema));
  } else if (OB_FAIL(clipped_storage_schema.get_multi_version_column_descs(multi_version_cols_desc))) {
    LOG_WARN("get mult version cols desc failed", K(ret), K(clipped_storage_schema));
  } else {
    table_cg_idx = 0;
    cg_schema = nullptr;
  }
  return ret;
}


// ObColSSTableSplitWriteHelper
ObColSSTableSplitWriteHelper::ObColSSTableSplitWriteHelper()
  : ObSSTableSplitWriteHelper(), mocked_row_store_cg_(), end_partkey_rowids_()
{
}

ObColSSTableSplitWriteHelper::~ObColSSTableSplitWriteHelper()
{
  mocked_row_store_cg_.reset();
  end_partkey_rowids_.reset();
}

int ObColSSTableSplitWriteHelper::prepare_index_read_info(
    const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(init_param));
  } else if (init_param.sstable_->is_cg_sstable()) {
    // rowkey cg/all cg.
    if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info_))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret));
    }
  } else {
    index_read_info_ = &init_param.context_->tablet_handle_.get_obj()->get_rowkey_read_info();
  }
  return ret;
}

int ObColSSTableSplitWriteHelper::prepare_split_rowids(
   const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("CnvCSRanges", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  end_partkey_rowids_.reset();
  const ObColSSTSplitHelperInitParam &col_init_param = static_cast<const ObColSSTSplitHelperInitParam &>(init_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!col_init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(col_init_param));
  } else if (col_init_param.sstable_->is_cg_sstable()) {
    if (OB_FAIL(end_partkey_rowids_.assign(col_init_param.end_partkey_rowids_))) {
      LOG_WARN("failed to assign end partkey rowids", K(ret));
    }
  } else {
    ObTabletHandle tablet_handle;
    ObDatumRange query_range, cs_range;
    query_range.start_key_.set_min_rowkey();
    query_range.set_left_closed();
    query_range.set_right_open();
    const ObTabletSplitParam &split_param = *init_param.param_;
    const ObTabletSplitCtx &split_ctx = *init_param.context_;
    ObTabletSplitMdsUserData prev_part_split_data, curr_part_split_data; // hold life of the end_part_key.
    const int64_t dest_parts_cnt = split_param.dest_tablets_id_.count();
    const ObITableReadInfo &rowkey_read_info = split_ctx.tablet_handle_.get_obj()->get_rowkey_read_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_parts_cnt; i++) {
      cs_range.reset();
      tablet_handle.reset();
      tmp_arena.reuse();
      const ObTabletID &tablet_id = split_param.dest_tablets_id_.at(i);
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(split_ctx.ls_handle_, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(curr_part_split_data,
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
        LOG_WARN("failed to get split data", K(ret));
      } else if (OB_FAIL(curr_part_split_data.get_end_partkey(query_range.end_key_))) {
        LOG_WARN("failed to get end partkey", K(ret), K(tablet_id));
      } else if (OB_FAIL(init_param.sstable_->get_cs_range(query_range, rowkey_read_info, tmp_arena, cs_range))) {
        LOG_WARN("failed to get cs range", K(ret));
      } else if (cs_range.is_whole_range()) {
        if (OB_FAIL(end_partkey_rowids_.push_back(INT64_MAX/*MAX_ROW_ID*/))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (OB_UNLIKELY(cs_range.end_key_.get_datum_cnt() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cs range", K(ret), K(cs_range));
      } else {
        const ObCSRowId &end_row_id = cs_range.end_key_.get_datum(0/*datum_idx*/).get_int();
        if (OB_FAIL(end_partkey_rowids_.push_back(end_row_id >= INT64_MAX ? INT64_MAX : end_row_id + 1))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(prev_part_split_data.assign(curr_part_split_data))) { // to hold life of the query_range.start_key_.
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(prev_part_split_data.get_end_partkey(query_range.start_key_))) {
          LOG_WARN("failed to get end partkey", K(ret), K(tablet_id));
        }
      }
    }
  }
  LOG_TRACE("prepare split point row offsets finished", K(ret), K(end_partkey_rowids_), K(init_param));
  return ret;
}

int ObColSSTableSplitWriteHelper::init(const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(init_param));
  } else if (OB_FAIL(inner_init_common(init_param))) {
    LOG_WARN("inner init common failed", K(ret), K(init_param));
  } else if (OB_FAIL(prepare_index_read_info(init_param))) {
    LOG_WARN("prepare index read info failed", K(ret));
  } else if (OB_FAIL(prepare_split_rowids(init_param))) {
    LOG_WARN("prepare split point rowids failed", K(ret));
  } else {
    param_ = init_param.param_;
    context_ = init_param.context_;
    sstable_ = init_param.sstable_;
    is_inited_ = true;
  }
  return ret;
}

int ObColSSTableSplitWriteHelper::prepare_sstable_cg_infos(
    const ObStorageSchema &clipped_storage_schema,
    const ObSSTable &sstable,
    const ObStorageColumnGroupSchema *&cg_schema,
    uint16_t &table_cg_idx,
    ObIArray<ObColDesc> &multi_version_cols_desc)
{
  int ret = OB_SUCCESS;
  cg_schema = nullptr;
  table_cg_idx = 0;
  multi_version_cols_desc.reset();
  ObArray<ObColDesc> full_mv_cols_desc;
  if (OB_UNLIKELY(!clipped_storage_schema.is_valid() || !sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(clipped_storage_schema), K(sstable));
  } else if (OB_FAIL(clipped_storage_schema.get_multi_version_column_descs(full_mv_cols_desc))) {
    LOG_WARN("get multi version column descs failed", K(ret), K(clipped_storage_schema));
  } else {
    table_cg_idx = sstable.get_column_group_id();
    const int64_t column_groups_cnt = clipped_storage_schema.get_column_group_count();
    if (OB_UNLIKELY(table_cg_idx < 0 || table_cg_idx >= column_groups_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column group id", K(ret), K(table_cg_idx), K(column_groups_cnt), K(clipped_storage_schema));
    } else if (sstable.is_cg_sstable()) {
      cg_schema = &clipped_storage_schema.get_column_groups().at(table_cg_idx);
    } else {
      const ObCOSSTableV2 &co_sstable = static_cast<const ObCOSSTableV2&>(sstable);
      cg_schema = &clipped_storage_schema.get_column_groups().at(table_cg_idx);
      if (cg_schema->is_rowkey_column_group() && co_sstable.is_all_cg_base()) {
        cg_schema = nullptr;
        if (OB_UNLIKELY(!co_sstable.is_cgs_empty_co_table())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), K(co_sstable));
        } else if (OB_FAIL(clipped_storage_schema.mock_row_store_cg(mocked_row_store_cg_))) {
          LOG_WARN("mock row store cg failed", K(ret), K(co_sstable), K(clipped_storage_schema));
        } else {
          cg_schema = &mocked_row_store_cg_;
        }
        LOG_INFO("Split mock_row_store_cg", K(ret), K(table_cg_idx), KPC(cg_schema), K(co_sstable), K(clipped_storage_schema));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(cg_schema)) {
      ret = OB_ERR_SYS;
      LOG_WARN("unexpected error", K(ret), KPC(cg_schema), K(clipped_storage_schema));
    } else if (cg_schema->is_all_column_group()) {
      if (OB_FAIL(multi_version_cols_desc.assign(full_mv_cols_desc))) {
        LOG_WARN("get mult version column descs failed", K(ret), K(full_mv_cols_desc), K(clipped_storage_schema));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cg_schema->get_column_count(); i++) {
        const uint16_t column_idx = cg_schema->get_column_idx(i); // include mult versions.
        if (OB_UNLIKELY(column_idx >= full_mv_cols_desc.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret), K(column_idx), "multi_version_cols_desc", full_mv_cols_desc);
        } else if (OB_FAIL(multi_version_cols_desc.push_back(full_mv_cols_desc.at(column_idx)))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

// ObSpecialSplitWriteHelper
int ObSpecialSplitWriteHelper::init(const ObSSTSplitHelperInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(init_param));
  } else {
    param_ = init_param.param_;
    context_ = init_param.context_;
    is_inited_ = true;
  }
  return ret;
}

int ObSpecialSplitWriteHelper::split_data(
    ObIAllocator &allocator,
    const int64_t task_idx)
{
  UNUSED(allocator);
  UNUSED(task_idx);
  return OB_SUCCESS;
}

int ObSpecialSplitWriteHelper::create_empty_minor_sstable()
{
  int ret = OB_SUCCESS;
  ObSSTable *last_minor_sstable = nullptr;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> participants;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      share::ObSplitSSTableType::SPLIT_MINOR, context_->table_store_iterator_, false/*is_table_restore*/,
      context_->skipped_split_major_keys_, false/*filter_normal_cg_sstables*/,
      participants))) {
    LOG_WARN("get participants failed", K(ret));
  } else if (participants.empty()) {
    LOG_INFO("no need to create empty minor", K(ret));
  } else if (OB_ISNULL(last_minor_sstable = static_cast<ObSSTable *>(participants.at(participants.count() - 1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(participants));
  } else {
    SCN end_scn;
    ObTabletID dest_tablet_id;
    ObSSTableMetaHandle meta_handle;
    bool need_fill_empty_sstable = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      meta_handle.reset();
      end_scn.reset();
      const ObTabletID &dest_tablet_id = param_->dest_tablets_id_.at(i);
      HEAP_VAR(ObTabletCreateSSTableParam, create_sstable_param) {
      if (OB_FAIL(ObTabletSplitUtil::check_need_fill_empty_sstable(
          context_->ls_handle_,
          true/*is_minor_sstable*/,
          last_minor_sstable->get_key(),
          dest_tablet_id,
          need_fill_empty_sstable,
          end_scn))) {
        LOG_WARN("failed to check need fill", K(ret));
      } else if (need_fill_empty_sstable) {
        if (OB_FAIL(last_minor_sstable->get_meta(meta_handle))) {
          LOG_WARN("get meta failed", K(ret));
        } else if (OB_FAIL(create_sstable_param.init_for_split_empty_minor_sstable(dest_tablet_id,
            last_minor_sstable->get_key().get_end_scn()/*start_scn*/, end_scn,
            meta_handle.get_sstable_meta().get_basic_meta()))) {
          LOG_WARN("init sstable param fail", K(ret), K(dest_tablet_id), K(end_scn), KPC(last_minor_sstable));
        } else if (OB_FAIL(context_->generate_sstable(i/*dest_tablet_index*/, create_sstable_param))) {
          LOG_WARN("failed to create sstable", K(ret));
        }
      }
      } // HEAP_VAR
    }
  }
  return ret;
}

int ObSpecialSplitWriteHelper::create_mds_sstable()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("BldSplitMDS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
  #ifdef OB_BUILD_SHARED_STORAGE
    const ObSSDataSplitHelper &ss_mds_split_helper = context_->ss_mds_split_helper_;
  #endif
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      HEAP_VARS_3 ((compaction::ObTabletMergeDagParam, param),
                  (compaction::ObTabletMergeCtx, tablet_merge_ctx, param, tmp_arena),
                  (ObMdsTableMiniMerger, mds_mini_merger)) {
        bool has_mds_row = false;
        int64_t index_tree_start_seq = 0;
        const int64_t dest_tablet_index = i;
        const ObTabletID &dest_tablet_id = param_->dest_tablets_id_.at(i);
        if (OB_FAIL(ObTabletSplitUtil::build_mds_sstable(
              context_->ls_handle_,
              context_->tablet_handle_,
              dest_tablet_index,
              dest_tablet_id,
              context_->split_scn_,
            #ifdef OB_BUILD_SHARED_STORAGE
              ss_mds_split_helper,
            #endif
              mds_mini_merger,
              tablet_merge_ctx,
              has_mds_row))) {
          LOG_WARN("build lost medium mds sstable failed", K(ret), KPC(this));
        } else if (!has_mds_row) {
          LOG_INFO("no need to build mds sstable", K(dest_tablet_id));
      #ifdef OB_BUILD_SHARED_STORAGE
        } else if (GCTX.is_shared_storage_mode()
          && OB_FAIL(ss_mds_split_helper.generate_minor_macro_seq_info(
              dest_tablet_index/*index in dest_tables_id*/,
              0/*the index in the generated minors*/,
              1/*the parallel cnt in one sstable*/,
              1/*the parallel idx in one sstable*/,
              index_tree_start_seq))) {
          LOG_WARN("get macro seq failed", K(ret));
      #endif
        } else if (OB_FAIL(context_->generate_mds_sstable(
          tablet_merge_ctx, dest_tablet_index, index_tree_start_seq, mds_mini_merger))) {
          LOG_WARN("generate mds sstable failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSpecialSplitWriteHelper::generate_sstable()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(create_empty_minor_sstable())) {
    LOG_WARN("failed to create empty minor sstable", K(ret));
  } else if (OB_FAIL(create_mds_sstable())) {
    LOG_WARN("failed to create mds sstable", K(ret));
  }
  return ret;
}


// ObSSSplitWriteHelperCommon
#ifdef OB_BUILD_SHARED_STORAGE
int ObSSSplitWriteHelperCommon::prepare_macro_seq_param_impl(
    const int64_t task_idx,
    ObSSTableSplitWriteHelper &helper,
    ObIArray<ObMacroSeqParam> &macro_seq_param_arr)
{
  int ret = OB_SUCCESS;
  macro_seq_param_arr.reset();
  if (OB_UNLIKELY(task_idx < 0
      || nullptr == helper.get_sstable()
      || nullptr == helper.get_split_param()
      || nullptr == helper.get_split_context())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_idx), K(helper));
  } else {
    const ObSSTable &src_sstable = *helper.get_sstable();
    const ObTabletSplitParam &split_param = *helper.get_split_param();
    const ObTabletSplitCtx &split_ctx = *helper.get_split_context();
    const int64_t parallel_cnt = split_ctx.parallel_cnt_of_each_sstable_;
    const ObSSDataSplitHelper &ss_split_helper = split_ctx.ss_minor_split_helper_;
    for (int64_t i = 0; OB_SUCC(ret) && i < split_param.dest_tablets_id_.count(); i++) {
      int64_t sstable_index = -1;
      ObMacroDataSeq macro_start_seq(0);
      const int64_t dest_tablet_index = i;
      if (OB_FAIL(split_ctx.get_index_in_source_sstables(src_sstable, sstable_index))) {
        LOG_WARN("get major/minor index from sstables failed", K(ret));
      } else if (src_sstable.is_major_sstable()) {
        if (OB_FAIL(ss_split_helper.generate_major_macro_seq_info(
              sstable_index/*the index in the generated majors*/,
              parallel_cnt/*the parallel cnt in one sstable*/,
              task_idx/*the parallel idx in one sstable*/,
              macro_start_seq.macro_data_seq_))) {
          LOG_WARN("generate macro start seq failed", K(ret), K(sstable_index), K(parallel_cnt), K(task_idx));
        }
      } else {
        if (OB_FAIL(ss_split_helper.generate_minor_macro_seq_info(
            dest_tablet_index/*index in dest_tables_id*/,
            sstable_index/*the index in the generated minors*/,
            parallel_cnt/*the parallel cnt in one sstable*/,
            task_idx/*the parallel idx in one sstable*/,
            macro_start_seq.macro_data_seq_))) {
          LOG_WARN("generate macro start seq failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObMacroSeqParam macro_seq_param;
        macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
        macro_seq_param.start_ = macro_start_seq.macro_data_seq_;
        if (OB_FAIL(macro_seq_param_arr.push_back(macro_seq_param))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      LOG_TRACE("prepare macro seq param for ss", K(ret),
            K(dest_tablet_index),
            K(sstable_index),
            K(parallel_cnt),
            K(task_idx),
            "end_scn", src_sstable.get_end_scn(),
            "macro_start_seq", macro_start_seq.macro_data_seq_);
    }
  }
  return ret;
}

int ObSSSplitWriteHelperCommon::build_create_sstable_param_impl(
      const int64_t dest_tablet_index,
      ObSSTableSplitWriteHelper &helper,
      ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dest_tablet_index < 0
      || nullptr == helper.get_sstable()
      || nullptr == helper.get_split_param()
      || nullptr == helper.get_split_context())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), K(helper));
  } else {
    const ObSSTable &src_sstable = *helper.get_sstable();
    const ObTabletSplitParam &split_param = *helper.get_split_param();
    const ObTabletSplitCtx &split_ctx = *helper.get_split_context();
    const ObIArray<ObSplitIndexBuilderCtx> &index_builder_ctx_arr = helper.get_index_builder_ctx_arr();
    const int64_t parallel_cnt = split_ctx.parallel_cnt_of_each_sstable_;
    const ObSSDataSplitHelper &ss_split_helper = split_ctx.ss_minor_split_helper_;
    if (OB_UNLIKELY(dest_tablet_index < 0
        || dest_tablet_index >= split_param.dest_tablets_id_.count()
        || dest_tablet_index >= index_builder_ctx_arr.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), K(helper));
    } else {
      int64_t sstable_index = -1;
      int64_t index_tree_start_seq = 0;
      int64_t new_root_start_seq = 0;
      share::ObPreWarmerParam pre_warm_param;
      ObSSTableMergeRes sstable_merge_res;
      const int64_t parallel_cnt = split_ctx.parallel_cnt_of_each_sstable_;
      const ObSSDataSplitHelper &ss_split_helper = split_ctx.ss_minor_split_helper_;
      const ObTabletID &dst_tablet_id = split_param.dest_tablets_id_.at(dest_tablet_index);
      const ObSplitIndexBuilderCtx &index_builder_ctx = index_builder_ctx_arr.at(dest_tablet_index);
      if (OB_ISNULL(index_builder_ctx.index_builder_)) {
        ret = OB_ERR_SYS;
        LOG_WARN("index builder is null", K(ret), K(dest_tablet_index), K(helper));
      } else if (OB_FAIL(pre_warm_param.init(split_param.ls_id_, dst_tablet_id))) {
        LOG_WARN("failed to init pre warm param", K(ret));
      } else if (OB_FAIL(split_ctx.get_index_in_source_sstables(src_sstable, sstable_index))) {
        LOG_WARN("get index in source sstables failed", K(ret));
      } else if (src_sstable.is_major_sstable()) {
        const int64_t total_majors_cnt = split_ctx.split_majors_count_;
        if (OB_FAIL(ss_split_helper.generate_major_macro_seq_info(
            sstable_index/*the index in the generated majors*/,
            parallel_cnt/*the parallel cnt in one sstable*/,
            parallel_cnt/*the parallel idx in one sstable*/,
            index_tree_start_seq))) {
          LOG_WARN("get macro seq failed", K(ret));
        } else if (OB_FAIL(index_builder_ctx.index_builder_->close_with_macro_seq(
                      sstable_merge_res,
                      index_tree_start_seq/*start seq of the cluster n-1/n-2 IndexTree*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                      0 /*nested_offset*/, pre_warm_param))) {
          LOG_WARN("close with seq failed", K(ret), K(index_tree_start_seq));
        } else if (OB_FAIL(ss_split_helper.get_major_macro_seq_by_stage(
            ObGetMacroSeqStage::GET_NEW_ROOT_MACRO_SEQ,
            total_majors_cnt,
            parallel_cnt,
            new_root_start_seq))) {
          LOG_WARN("get new root macro seq failed", K(ret), K(total_majors_cnt), K(parallel_cnt));
        } else {
          sstable_merge_res.root_macro_seq_ = new_root_start_seq;
        }
      } else {
        if (OB_FAIL(ss_split_helper.generate_minor_macro_seq_info(
              dest_tablet_index/*index in dest_tables_id*/,
              sstable_index/*the index in the generated minors*/,
              parallel_cnt/*the parallel cnt in one sstable*/,
              parallel_cnt/*the parallel idx in one sstable*/,
              index_tree_start_seq))) {
          LOG_WARN("get macro seq failed", K(ret));
        } else if (OB_FAIL(index_builder_ctx.index_builder_->close_with_macro_seq(
                      sstable_merge_res,
                      index_tree_start_seq/*start seq of the cluster n-1/n-2 IndexTree*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                      0 /*nested_offset*/, pre_warm_param))) {
          LOG_WARN("close with seq failed", K(ret), K(index_tree_start_seq));
        }
      }

      if (OB_SUCC(ret)) {
        ObSSTableMetaHandle meta_handle;
        if (OB_FAIL(src_sstable.get_meta(meta_handle))) {
          LOG_WARN("get sstable meta failed", K(ret));
        } else {
          ObArray<MacroBlockId> empty_macros;
          const ObIArray<MacroBlockId> &split_point_macros = helper.get_split_point_macros();
          const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
          if (OB_FAIL(create_sstable_param.init_for_split(dst_tablet_id, src_sstable, basic_meta,
                                                          basic_meta.schema_version_,
                                                          (dest_tablet_index == 0) ? split_point_macros : empty_macros,
                                                          sstable_merge_res))) {
            LOG_WARN("init sstable param fail", K(ret), K(dst_tablet_id), K(src_sstable.get_key()), K(basic_meta), K(sstable_merge_res));
          }
        }
      }
      LOG_TRACE("build create sstable param for ss", K(ret),
              K(dest_tablet_index),
              K(sstable_index),
              K(parallel_cnt),
              "table_key", src_sstable.get_key(),
              K(index_tree_start_seq),
              K(new_root_start_seq));
    }
  }
  return ret;
}

// ObSSRowSSTableSplitWriteHelper
int ObSSRowSSTableSplitWriteHelper::prepare_macro_seq_param(
  const int64_t task_idx,
  ObIArray<ObMacroSeqParam> &macro_seq_param_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ss_common_helper_.prepare_macro_seq_param_impl(task_idx, *this, macro_seq_param_arr))) {
    LOG_WARN("prepare_macro_seq_param_impl failed", K(ret));
  }
  return ret;
}

int ObSSRowSSTableSplitWriteHelper::build_create_sstable_param(
  const int64_t dest_tablet_index,
  ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ss_common_helper_.build_create_sstable_param_impl(dest_tablet_index, *this, create_sstable_param))) {
    LOG_WARN("build_create_sstable_param_impl failed", K(ret));
  }
  return ret;
}

// ObSSColSSTableSplitWriteHelper
int ObSSColSSTableSplitWriteHelper::prepare_macro_seq_param(
  const int64_t task_idx,
  ObIArray<ObMacroSeqParam> &macro_seq_param_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ss_common_helper_.prepare_macro_seq_param_impl(task_idx, *this, macro_seq_param_arr))) {
    LOG_WARN("prepare_macro_seq_param_impl failed", K(ret));
  }
  return ret;
}

int ObSSColSSTableSplitWriteHelper::build_create_sstable_param(
  const int64_t dest_tablet_index,
  ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ss_common_helper_.build_create_sstable_param_impl(dest_tablet_index, *this, create_sstable_param))) {
    LOG_WARN("build_create_sstable_param_impl failed", K(ret));
  }
  return ret;
}

#endif


}  // end namespace storage
}  // end namespace oceanbase
