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
#include "ob_old_sstable.h"
#include <math.h>
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_fixed_array_iterator.h"
#include "share/stat/ob_table_stat.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "blocksstable/ob_macro_block_writer.h"
#include "blocksstable/ob_block_cache_working_set.h"
#include "ob_partition_scheduler.h"
#include "ob_warm_up_request.h"
#include "ob_multiple_merge.h"
#include "storage/ob_sstable_row_exister.h"
#include "storage/ob_sstable_row_multi_exister.h"
#include "storage/ob_sstable_row_getter.h"
#include "storage/ob_sstable_row_multi_getter.h"
#include "storage/ob_sstable_row_scanner.h"
#include "storage/ob_sstable_row_multi_scanner.h"
#include "storage/ob_sstable_multi_version_row_iterator.h"
#include "storage/ob_sstable_row_whole_scanner.h"
#include "storage/ob_sstable_estimator.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_partition_split_task.h"
#include "storage/ob_tenant_config_mgr.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace storage {

ObOldSSTable::ObSSTableGroupMacroBlocks::ObSSTableGroupMacroBlocks(
    const MacroBlockArray& data_macro_blocks, const MacroBlockArray& lob_macro_blocks)
    : data_macro_blocks_(data_macro_blocks), lob_macro_blocks_(lob_macro_blocks)
{}

ObOldSSTable::ObSSTableGroupMacroBlocks::~ObSSTableGroupMacroBlocks()
{}

const MacroBlockId& ObOldSSTable::ObSSTableGroupMacroBlocks::at(const int64_t idx) const
{
  if (OB_UNLIKELY(0 > idx || count() <= idx)) {
    STORAGE_LOG(ERROR, "SSTable get macro blocks out of range", K(idx));
  }
  if (idx < data_macro_block_count()) {
    return data_macro_blocks_.at(idx);
  } else {
    return lob_macro_blocks_.at(idx - data_macro_block_count());
  }
}

int ObOldSSTable::ObSSTableGroupMacroBlocks::at(const int64_t idx, MacroBlockId& obj) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || count() <= idx)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    STORAGE_LOG(ERROR, "SSTable get macro blocks out of range", K(idx));
  } else if (idx < data_macro_block_count()) {
    ret = data_macro_blocks_.at(idx, obj);
  } else {
    ret = lob_macro_blocks_.at(idx - data_macro_block_count(), obj);
  }
  return ret;
}

int64_t ObOldSSTable::ObSSTableGroupMacroBlocks::to_string(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;

  J_NAME("data_blocks");
  J_COLON();
  pos += data_macro_blocks_.to_string(buf + pos, buf_len - pos);
  J_NAME("lob_blocks");
  J_COLON();
  pos += lob_macro_blocks_.to_string(buf + pos, buf_len - pos);

  return pos;
}

ObOldSSTable::ObOldSSTable()
    : allocator_(),
      meta_(allocator_),
      macro_block_second_indexes_(),
      macro_block_metas_(&allocator_),
      lob_macro_block_second_indexes_(),
      lob_macro_block_metas_(&allocator_),
      rowkey_helper_(),
      sstable_merge_info_(),
      status_(SSTABLE_NOT_INIT),
      total_macro_blocks_(meta_.macro_block_array_, meta_.lob_macro_block_array_),
      total_meta_macro_blocks_(macro_block_second_indexes_, lob_macro_block_second_indexes_),
      exist_invalid_collation_free_meta_(false),
      micro_block_count_(0)
{}

ObOldSSTable::~ObOldSSTable()
{
  destroy();
}

int ObOldSSTable::init(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_NOT_INIT != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is not in close status.", K(ret), K_(status));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(table_key), K(ret));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    TRANS_LOG(WARN, "failed to set_table_key", K(ret), K(table_key));
  } else {
    meta_.reset();
    meta_.total_sstable_count_ = 0;
    status_ = SSTABLE_INIT;
    allocator_.set_attr(ObMemAttr(
        extract_tenant_id(table_key.table_id_), ObModIds::OB_SSTABLE, ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID));
  }
  return ret;
}

int ObOldSSTable::replay_set_table_key(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (key_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "cannot set table key twice.", K(ret), K_(key));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(table_key), K(ret));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    TRANS_LOG(WARN, "failed to set_table_key", K(ret), K(table_key));
  }
  return ret;
}

int ObOldSSTable::open(const ObCreateSSTableParam& param)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  return ret;
}

int ObOldSSTable::open(const blocksstable::ObSSTableBaseMeta& meta)
{
  int ret = OB_SUCCESS;
  ObSSTableColumnMeta col_meta;
  if (SSTABLE_INIT != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(meta));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
    LOG_WARN("Failed to init file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
    LOG_WARN("Failed to init lob file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
    LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
  } else {
    meta_.index_id_ = meta.index_id_;
    meta_.row_count_ = 0;
    meta_.occupy_size_ = 0;
    meta_.data_checksum_ = 0;
    meta_.row_checksum_ = 0;
    meta_.data_version_ = meta.data_version_;
    meta_.rowkey_column_count_ = meta.rowkey_column_count_;
    meta_.table_type_ = meta.table_type_;
    meta_.index_type_ = meta.index_type_;
    meta_.available_version_ = meta.available_version_;
    meta_.macro_block_count_ = 0;
    meta_.use_old_macro_block_count_ = 0;
    meta_.lob_macro_block_count_ = 0;
    meta_.bloom_filter_block_id_.reset();
    meta_.bloom_filter_block_id_in_files_ = 0;
    meta_.column_cnt_ = meta.column_cnt_;
    meta_.sstable_format_version_ = meta.sstable_format_version_;
    meta_.multi_version_rowkey_type_ = meta.multi_version_rowkey_type_;
    if (is_major_sstable()) {
      if (meta_.sstable_format_version_ <= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_4) {
        meta_.logical_data_version_ = meta_.data_version_;
      } else {
        meta_.logical_data_version_ = meta.logical_data_version_;
      }
    } else {
      if (meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6) {
        meta_.logical_data_version_ = get_snapshot_version();
      } else {
        meta_.logical_data_version_ = meta.logical_data_version_;
      }
    }
    if (meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6) {
      set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
      meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    } else if (ObMultiVersionRowkeyHelpper::MVRC_VERSION_MAX == meta_.multi_version_rowkey_type_) {
      set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0);
    }
    meta_.max_logic_block_index_ = meta.max_logic_block_index_;
    meta_.build_on_snapshot_ = meta.build_on_snapshot_;
    meta_.create_index_base_version_ = meta.create_index_base_version_;
    meta_.schema_version_ = meta.schema_version_;
    meta_.progressive_merge_start_version_ = meta.progressive_merge_start_version_;
    meta_.progressive_merge_end_version_ = meta.progressive_merge_end_version_;
    meta_.create_snapshot_version_ = meta.create_snapshot_version_;
    meta_.checksum_method_ = meta.checksum_method_;
    meta_.total_sstable_count_ = meta.total_sstable_count_;
    meta_.column_metas_.set_capacity(static_cast<uint32_t>(meta_.column_cnt_));
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.column_cnt_; ++i) {
      col_meta.column_id_ = meta.column_metas_.at(i).column_id_;
      col_meta.column_default_checksum_ = meta.column_metas_.at(i).column_default_checksum_;
      col_meta.column_checksum_ =
          blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_ ? meta.column_metas_.at(i).column_checksum_ : 0;
      if (OB_FAIL(meta_.column_metas_.push_back(col_meta))) {
        STORAGE_LOG(WARN, "Fail to push column meta, ", K(ret));
      }
    }

    meta_.new_column_metas_.set_capacity(static_cast<uint32_t>(meta.new_column_metas_.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.new_column_metas_.count(); ++i) {
      col_meta.column_id_ = meta.new_column_metas_.at(i).column_id_;
      col_meta.column_default_checksum_ = meta.new_column_metas_.at(i).column_default_checksum_;
      col_meta.column_checksum_ = blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_
                                      ? meta.new_column_metas_.at(i).column_checksum_
                                      : 0;
      if (OB_FAIL(meta_.new_column_metas_.push_back(col_meta))) {
        STORAGE_LOG(WARN, "fail to push back col meta", K(ret));
      }
    }

    meta_.progressive_merge_round_ = meta.progressive_merge_round_;
    meta_.progressive_merge_step_ = meta.progressive_merge_step_;
    meta_.table_mode_ = meta.table_mode_;

    if (OB_SUCC(ret)) {
      status_ = SSTABLE_WRITE_BUILDING;
    }
  }
  return ret;
}

int ObOldSSTable::close()
{
  int ret = OB_SUCCESS;
  if (SSTABLE_WRITE_BUILDING == status_) {
    sstable_merge_info_.occupy_size_ = meta_.occupy_size_;
  } else if (SSTABLE_INIT != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_block_item_map())) {
      STORAGE_LOG(WARN, "failed to build block id set", K(ret));
    } else if (OB_FAIL(build_lob_block_map())) {
      STORAGE_LOG(WARN, "Failed to build lob block id map", K(ret));
    } else if (OB_FAIL(check_collation_free_valid())) {
      STORAGE_LOG(WARN, "fail to check collation free valid", K(ret));
    } else if (OB_FAIL(check_logical_data_version())) {
      STORAGE_LOG(WARN, "fail to check logical data version for sstable", K(ret), K_(meta));
    } else if (blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_ &&
               meta_.data_version_ == meta_.progressive_merge_end_version_ - 1 && meta_.new_column_metas_.count() > 0) {
      meta_.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
      for (int64_t i = 0; i < meta_.column_cnt_; ++i) {
        meta_.column_metas_.at(i).column_id_ = meta_.new_column_metas_.at(i).column_id_;
        meta_.column_metas_.at(i).column_default_checksum_ = meta_.new_column_metas_.at(i).column_default_checksum_;
        meta_.column_metas_.at(i).column_checksum_ = meta_.new_column_metas_.at(i).column_checksum_;
        STORAGE_LOG(INFO, "copy column checksum", K(meta_.column_metas_.at(i)));
      }
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(clean_lob_column_checksum())) {
      STORAGE_LOG(WARN, "Failed to clean lob column checksums", K(ret));
    } else {
      status_ = SSTABLE_READY_FOR_READ;
    }
  }
  return ret;
}

void ObOldSSTable::destroy()
{
  STORAGE_LOG(INFO, "destroy sstable.", K_(key));
  rowkey_helper_.reset();
  for (int64_t i = 0; i < macro_block_second_indexes_.count(); ++i) {
    OB_STORE_FILE.dec_ref(macro_block_second_indexes_.at(i));
  }
  macro_block_second_indexes_.reset();
  for (int64_t i = 0; i < macro_block_metas_.count(); ++i) {
    macro_block_metas_.at(i)->~ObMacroBlockMetaHandle();
  }
  macro_block_metas_.reset();

  (void)block_id_map_.destroy();

  for (int64_t i = 0; i < lob_macro_block_second_indexes_.count(); ++i) {
    OB_STORE_FILE.dec_ref(lob_macro_block_second_indexes_.at(i));
  }
  lob_macro_block_second_indexes_.reset();
  for (int64_t i = 0; i < lob_macro_block_metas_.count(); ++i) {
    lob_macro_block_metas_.at(i)->~ObMacroBlockMetaHandle();
  }
  lob_macro_block_metas_.reset();
  lob_block_id_map_.destroy();

  for (int64_t i = 0; i < meta_.macro_block_array_.count(); ++i) {
    OB_STORE_FILE.dec_ref(meta_.macro_block_array_.at(i));
  }

  for (int64_t i = 0; i < meta_.lob_macro_block_array_.count(); ++i) {
    OB_STORE_FILE.dec_ref(meta_.lob_macro_block_array_.at(i));
  }

  if (meta_.bloom_filter_block_id_.is_valid()) {
    OB_STORE_FILE.dec_ref(meta_.bloom_filter_block_id_);
  }

  // TODO(): release ofs files
  meta_.reset();
  status_ = SSTABLE_NOT_INIT;
}

void ObOldSSTable::clear()
{
  meta_.reset();  // avoid release ofs file and macro block ref
  destroy();
}

int ObOldSSTable::append_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "cannot append macro block in this status", K(ret), K_(status));
  } else if (OB_FAIL(append_macro_block_write_ctx(
                 macro_block_write_ctx, meta_.macro_block_array_, meta_.macro_block_idx_array_, meta_.file_ctx_))) {
    LOG_WARN("failed to append_macro_block_write_ctx", K(ret));
  }
  return ret;
}

int ObOldSSTable::append_lob_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "cannot append macro block in this status", K(ret), K_(status));
  } else if (OB_FAIL(append_macro_block_write_ctx(macro_block_write_ctx,
                 meta_.lob_macro_block_array_,
                 meta_.lob_macro_block_idx_array_,
                 meta_.lob_file_ctx_))) {
    LOG_WARN("failed to append_macro_block_write_ctx", K(ret));
  }
  return ret;
}

int ObOldSSTable::append_bf_macro_blocks(ObMacroBlocksWriteCtx& macro_block_write_ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "cannot append macro block in this status", K(ret), K_(status));
  } else if (meta_.bloom_filter_file_ctx_.file_system_type_ != macro_block_write_ctx.file_ctx_.file_system_type_ ||
             meta_.bloom_filter_file_ctx_.file_type_ != macro_block_write_ctx.file_ctx_.file_type_ ||
             meta_.bloom_filter_file_ctx_.block_count_per_file_ !=
                 macro_block_write_ctx.file_ctx_.block_count_per_file_ ||
             !macro_block_write_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid args", K(ret), K(macro_block_write_ctx), K(meta_.bloom_filter_file_ctx_));
  } else if (macro_block_write_ctx.is_empty()) {
    // pass
  } else if (macro_block_write_ctx.get_macro_block_count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected macro block write ctx count", K(macro_block_write_ctx), K(ret));
  } else if (OB_FAIL(add_macro_block_meta(macro_block_write_ctx.macro_block_list_.at(0)))) {
    LOG_WARN("Failed to add macro block meta", K(macro_block_write_ctx.macro_block_list_.at(0)), K(ret));
  } else {
    meta_.bloom_filter_block_id_ = macro_block_write_ctx.macro_block_list_.at(0);
    meta_.bloom_filter_block_id_in_files_ = 0;
    if (macro_block_write_ctx.file_ctx_.need_file_id_list()) {
      if (macro_block_write_ctx.file_ctx_.file_list_.count() <= 0 ||
          macro_block_write_ctx.file_ctx_.file_list_.count() > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected macro block file ctx count", K(macro_block_write_ctx.file_ctx_), K(ret));
      } else if (OB_FAIL(meta_.bloom_filter_file_ctx_.file_list_.push_back(
                     macro_block_write_ctx.file_ctx_.file_list_.at(0)))) {
        LOG_ERROR("failed to add file list, fatal error", K(ret));
        ob_abort();
      }
    }
    if (OB_SUCC(ret)) {
      OB_STORE_FILE.inc_ref(meta_.bloom_filter_block_id_);
    }
  }

  return ret;
}

int ObOldSSTable::get_combine_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx));
  } else if (idx < meta_.macro_block_array_.count()) {
    if (OB_FAIL(get_macro_block_ctx(idx, ctx))) {
      LOG_WARN("failed to get macro block ctx", K(ret), K(idx));
    }
  } else {
    if (OB_FAIL(get_lob_macro_block_ctx(idx - meta_.macro_block_array_.count(), ctx))) {
      LOG_WARN("failed to get lob macro block ctx", K(ret), K(idx));
    }
  }
  return ret;
}

int ObOldSSTable::get_macro_block_ctx(const blocksstable::MacroBlockId& macro_id, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;
  int64_t block_idx = 0;
  if (OB_FAIL(block_id_map_.get(macro_id, block_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      STORAGE_LOG(WARN, "Fail to get macro block idx from map, ", K(ret), K(macro_id));
    }
  } else if (OB_FAIL(get_macro_block_ctx(block_idx, ctx))) {
    STORAGE_LOG(WARN, "Fail to get macro block ctx use idx, ", K(ret), K(block_idx));
  } else if (OB_UNLIKELY(macro_id != ctx.sstable_block_id_.macro_block_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro_id is not same,", K(ret), K(macro_id), K(ctx));
    ctx.reset();
  }
  return ret;
}

int ObOldSSTable::get_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (OB_FAIL(get_macro_block_ctx(
                 idx, meta_.macro_block_array_, meta_.macro_block_idx_array_, meta_.file_ctx_, ctx))) {
    LOG_WARN("failed to get macro block ctx", K(ret), K(idx));
  }
  return ret;
}

int ObOldSSTable::get_lob_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (OB_FAIL(get_macro_block_ctx(
                 idx, meta_.lob_macro_block_array_, meta_.lob_macro_block_idx_array_, meta_.lob_file_ctx_, ctx))) {
    LOG_WARN("failed to get lob macro block ctx", K(ret), K(idx));
  }
  return ret;
}

int ObOldSSTable::get_bf_macro_block_ctx(ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (!meta_.bloom_filter_block_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid bloomfilter macro id to get", K(meta_.bloom_filter_block_id_), K(ret));
  } else {
    ctx.reset();
    ctx.file_ctx_ = &meta_.bloom_filter_file_ctx_;
    ctx.sstable_block_id_.macro_block_id_ = meta_.bloom_filter_block_id_;
    ctx.sstable_block_id_.macro_block_id_in_files_ = 0;
  }
  return ret;
}

int ObOldSSTable::get_macro_block_ctx(const int64_t idx,
    const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids,
    const common::ObIArray<int64_t>& block_id_in_files, const blocksstable::ObStoreFileCtx& file_ctx,
    blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;
  ctx.reset();

  if (idx < 0 || idx >= macro_block_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, cannot get macro block ctx",
        K(ret),
        K(idx),
        "macro_block_count",
        macro_block_ids.count(),
        K(meta_));
  } else {
    ctx.file_ctx_ = &file_ctx;
    ctx.sstable_block_id_.macro_block_id_ = macro_block_ids.at(idx);
    if (file_ctx.need_file_id_list()) {
      if (macro_block_ids.count() != block_id_in_files.count()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("macro block id count not match block id in files count",
            K(ret),
            "macro id count",
            macro_block_ids.count(),
            "id in file count",
            block_id_in_files.count());
      } else {
        ctx.sstable_block_id_.macro_block_id_in_files_ = block_id_in_files.at(idx);
      }
    } else {
      ctx.sstable_block_id_.macro_block_id_in_files_ = idx;
    }
  }
  return ret;
}

int ObOldSSTable::get_macro_block_meta(const int64_t idx, const blocksstable::ObMacroBlockMeta*& macro_meta)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle* meta_handle = NULL;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (idx < 0 || idx >= meta_.macro_block_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, cannot get macro block meta",
        K(ret),
        K(idx),
        "macro_block_count",
        meta_.macro_block_array_.count());
  } else if (OB_FAIL(macro_block_metas_.at(idx, meta_handle))) {
    STORAGE_LOG(WARN, "Fail to get macro block id, ", K(ret));
  } else if (OB_ISNULL(meta_handle) || OB_ISNULL(macro_meta = meta_handle->get_meta())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, ", K(ret), KP(meta_handle), KP(macro_meta));
  }

  return ret;
}

// only used during observer reboot
int ObOldSSTable::set_sstable(ObOldSSTable& src_sstable)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_NOT_INIT != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is not in close status.", K(ret), K_(status));
  } else if (!src_sstable.macro_block_second_indexes_.empty() || !src_sstable.lob_macro_block_second_indexes_.empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("src sstable should not has second index during startup", K(ret), K(src_sstable));
  } else if (OB_FAIL(init(src_sstable.get_key()))) {
    LOG_WARN("failed to init sstable", K(ret), K(src_sstable));
  } else if (OB_FAIL(open(src_sstable.get_meta()))) {
    LOG_WARN("failed to open sstable", K(ret), K(src_sstable));
  } else if (OB_FAIL(meta_.macro_block_array_.assign(src_sstable.meta_.macro_block_array_))) {
    LOG_WARN("failed to copy macro block array", K(ret));
  } else if (OB_FAIL(meta_.lob_macro_block_array_.assign(src_sstable.meta_.lob_macro_block_array_))) {
    LOG_WARN("failed to copy lob macro block array", K(ret));
  } else if (OB_FAIL(meta_.macro_block_idx_array_.assign(src_sstable.meta_.macro_block_idx_array_))) {
    LOG_WARN("failed to copy macro block idx array", K(ret));
  } else if (OB_FAIL(meta_.lob_macro_block_idx_array_.assign(src_sstable.meta_.lob_macro_block_idx_array_))) {
    LOG_WARN("failed to copy lob macro block idx array", K(ret));
  } else if (OB_FAIL(add_macro_block_meta(meta_.macro_block_array_))) {
    LOG_WARN("failed to add macro block meta", K(ret));
  } else if (OB_FAIL(add_macro_block_meta(meta_.lob_macro_block_array_))) {
    LOG_WARN("failed to add lob macro block meta", K(ret));
  } else {
    meta_.file_ctx_.reset();
    meta_.lob_file_ctx_.reset();
    meta_.bloom_filter_file_ctx_.reset();
    if (OB_FAIL(meta_.file_ctx_.assign(src_sstable.meta_.file_ctx_))) {
      LOG_WARN("failed to copy file ctx", K(ret));
    } else if (OB_FAIL(meta_.lob_file_ctx_.assign(src_sstable.meta_.lob_file_ctx_))) {
      LOG_WARN("Failed to copy lob file ctx", K(ret));
    } else if (OB_FAIL(meta_.bloom_filter_file_ctx_.assign(src_sstable.meta_.bloom_filter_file_ctx_))) {
      LOG_WARN("Failed to copy bloomfilter file ctx", K(ret));
    } else {
      sstable_merge_info_ = src_sstable.get_sstable_merge_info();
      meta_.use_old_macro_block_count_ = src_sstable.meta_.use_old_macro_block_count_;
      meta_.lob_use_old_macro_block_count_ = src_sstable.get_meta().lob_use_old_macro_block_count_;
      meta_.build_on_snapshot_ = src_sstable.meta_.build_on_snapshot_;
      meta_.create_index_base_version_ = src_sstable.meta_.create_index_base_version_;
      for (int64_t i = 0; i < meta_.column_cnt_; i++) {
        meta_.column_metas_.at(i).column_checksum_ = src_sstable.get_meta().column_metas_.at(i).column_checksum_;
      }
      for (int64_t i = 0; i < src_sstable.get_meta().new_column_metas_.count(); i++) {
        meta_.new_column_metas_.at(i).column_checksum_ =
            src_sstable.get_meta().new_column_metas_.at(i).column_checksum_;
      }
      meta_.bloom_filter_block_id_ = src_sstable.meta_.bloom_filter_block_id_;
      meta_.bloom_filter_block_id_in_files_ = src_sstable.meta_.bloom_filter_block_id_in_files_;
      if (meta_.bloom_filter_block_id_.is_valid() && OB_FAIL(add_macro_block_meta(meta_.bloom_filter_block_id_))) {
        LOG_WARN("failed to add bloomfilter macro block meta", K(meta_.bloom_filter_block_id_), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(close())) {
      LOG_WARN("failed to close sstable", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeed to set sstable, clear src sstable", K(ret), K(src_sstable), K(*this));
    src_sstable.clear();
  } else {
    LOG_WARN("failed to set sstable, clear dest sstable", K(ret), K(src_sstable));
    clear();
  }
  return ret;
}

int ObOldSSTable::set_column_checksum(const int64_t column_cnt, const int64_t* column_checksum)
{
  int ret = OB_SUCCESS;
  if (meta_.column_cnt_ != column_cnt || NULL == column_checksum) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(meta_.column_cnt_), K(column_cnt), KP(column_checksum));
  } else {
    for (int64_t i = 0; i < meta_.column_cnt_; ++i) {
      meta_.column_metas_.at(i).column_checksum_ = column_checksum[i];
    }
    STORAGE_LOG(INFO, "set column checksum", K(meta_.column_metas_));
  }
  return ret;
}

int ObOldSSTable::set_column_checksum(const hash::ObHashMap<int64_t, int64_t>& column_checksum_map)
{
  int ret = OB_SUCCESS;
  if (0 == column_checksum_map.size()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(column_checksum_map.size()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.column_cnt_; ++i) {
      const int64_t column_id = meta_.column_metas_.at(i).column_id_;
      int64_t column_checksum = 0;
      if (column_id >= OB_MIN_SHADOW_COLUMN_ID) {
        meta_.column_metas_.at(i).column_checksum_ = 0;
        continue;
      } else if (OB_FAIL(column_checksum_map.get_refactored(column_id, column_checksum))) {
        STORAGE_LOG(WARN, "fail to get from map", K(ret), K(column_id));
      } else {
        meta_.column_metas_.at(i).column_checksum_ = column_checksum;
      }
    }
  }
  return ret;
}

int ObOldSSTable::add_sstable_merge_info(const ObSSTableMergeInfo& sstable_merge_info)
{
  int ret = OB_SUCCESS;
  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is in invalid status, cannot add sstable info", K(ret), K_(status));
  } else if (OB_FAIL(sstable_merge_info_.add(sstable_merge_info))) {
    STORAGE_LOG(WARN, "Fail to add sstable merge info, ", K(ret));
  }
  return ret;
}

int ObOldSSTable::bf_may_contain_rowkey(const ObStoreRowkey& rowkey, bool& contain)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(rowkey, contain);

  return ret;
}

bool ObOldSSTable::is_valid() const
{
  return key_.is_valid() && meta_.is_valid() && SSTABLE_READY_FOR_READ == status_;
}

int ObOldSSTable::get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const common::ObExtStoreRowkey& rowkey, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (!rowkey.get_store_rowkey().is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid get arguments.", K(ret), K(param), K(context), K(rowkey));
  } else {
    void* buf = NULL;
    ObISSTableRowIterator* row_getter = NULL;
    if (is_multi_version_minor_sstable() && context.is_multi_version_read(get_snapshot_version())) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowGetter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_getter = new (buf) ObSSTableMultiVersionRowGetter();
      }
    } else {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowGetter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_getter = new (buf) ObSSTableRowGetter();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_getter->init(param, context, this, &rowkey))) {
        STORAGE_LOG(WARN, "Fail to open getter, ", K(ret), K(param), K(context), K(rowkey));
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != row_getter) {
        row_getter->~ObISSTableRowIterator();
      }
    } else {
      row_iter = row_getter;
    }
  }
  return ret;
}

int ObOldSSTable::multi_get(const ObTableIterParam& param, ObTableAccessContext& context,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret), K(meta_), K(status_));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (rowkeys.count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(param), K(context), K(rowkeys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      if (!rowkeys.at(i).get_store_rowkey().is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "rowkey is invalid", K(ret), K(i), K(rowkeys));
      }
    }
    if (OB_SUCC(ret)) {
      void* buf = NULL;
      ObISSTableRowIterator* row_getter = NULL;
      if (is_multi_version_minor_sstable() && context.is_multi_version_read(get_snapshot_version())) {
        if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowMultiGetter)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          row_getter = new (buf) ObSSTableMultiVersionRowMultiGetter();
        }
      } else {
        if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowMultiGetter)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          row_getter = new (buf) ObSSTableRowMultiGetter();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(row_getter->init(param, context, this, &rowkeys))) {
          STORAGE_LOG(WARN, "Fail to open row multi getter, ", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        if (NULL != row_getter) {
          row_getter->~ObISSTableRowIterator();
        }
      } else {
        row_iter = row_getter;
      }
    }
  }
  return ret;
}

int ObOldSSTable::get_last_rowkey(ObStoreRowkey& rowkey, ObArenaAllocator& allocator)
{
  int ret = OB_SUCCESS;

  ObMacroBlockMetaHandle meta_handle;
  const ObMacroBlockMeta* meta = NULL;
  ObStoreRowkey temp_key;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;

  if (macro_blocks.count() <= 0) {
    // no need to set rowkey
  } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(
                 macro_blocks.at(macro_blocks.count() - 1), meta_handle))) {
    STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
  } else if (NULL == (meta = meta_handle.get_meta())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro block meta is NULL.", K(ret), KP(meta), K(macro_blocks));
  } else {
    temp_key.assign(meta->endkey_, meta->rowkey_column_number_);
    if (OB_FAIL(temp_key.deep_copy(rowkey, allocator))) {
      STORAGE_LOG(WARN, "failed to deep copy rowkey", K(ret));
    }
  }

  return ret;
}

int ObOldSSTable::scan(const ObTableIterParam& param, ObTableAccessContext& context,
    const common::ObExtStoreRange& key_range, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is not in not ready for access", K(ret), K_(meta), K_(status));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (!key_range.get_range().is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(param), K(context), K(key_range), K(ret));
  } else {
    void* buf = NULL;
    ObISSTableRowIterator* row_scanner = NULL;
    if (context.query_flag_.is_whole_macro_scan()) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowWholeScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableRowWholeScanner();
      }
    } else if (is_multi_version_minor_sstable()) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableMultiVersionRowScanner();
      }
    } else {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableRowScanner();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_scanner->init(param, context, this, &key_range))) {
        STORAGE_LOG(WARN, "Fail to open row scanner, ", K(ret), K(param), K(context), K(key_range));
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != row_scanner) {
        row_scanner->~ObISSTableRowIterator();
      }
    } else {
      row_iter = row_scanner;
    }
  }

  return ret;
}

int ObOldSSTable::dump2text(const share::schema::ObTableSchema& schema, const char* fname)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE* fd = NULL;
  ObArenaAllocator allocator(ObModIds::OB_CS_MERGER);
  ObArenaAllocator stmt_allocator(ObModIds::OB_CS_MERGER);
  ObArray<ObColDesc, ObIAllocator&> column_ids(2048, allocator);
  const storage::ObStoreRow* curr_row;
  ObExtStoreRange range;
  range.get_range().set_whole_range();
  ObTableIterParam param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  const uint64_t tenant_id = extract_tenant_id(schema.get_table_id());
  ObBlockCacheWorkingSet block_cache_ws;
  ObStoreRowIterator* scanner = NULL;

  STORAGE_LOG(INFO, "dump2text", K_(meta));
  if (OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fanme is NULL");
  } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
  } else {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true, /*is daily merge scan*/
        true, /*is read multiple macro block*/
        true, /*sys task scan, read one macro block in single io*/
        false /*is full row scan?*/,
        false,
        false);
    param.table_id_ = schema.get_table_id();
    param.rowkey_cnt_ = schema.get_rowkey_column_num();
    param.schema_version_ = schema.get_schema_version();
    context.query_flag_ = query_flag;
    context.store_ctx_ = &store_ctx;
    context.allocator_ = &allocator;
    context.block_cache_ws_ = &block_cache_ws;
    context.stmt_allocator_ = &stmt_allocator;
    context.trans_version_range_ = key_.trans_version_range_;
    context.is_inited_ = true;  // just used for dump

    pret =
        snprintf(real_fname, sizeof(real_fname), "%s.%ld", fname, ::oceanbase::common::ObTimeUtility::current_time());
    if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "fname too long", K(fname));
    } else if (NULL == (fd = fopen(real_fname, "w"))) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN, "open file fail:", K(fname));
    } else if (OB_FAIL(schema.get_column_ids(column_ids, true))) {
      STORAGE_LOG(WARN, "Fail to get column ids. ", K(ret));
    } else if (FALSE_IT(param.out_cols_ = &column_ids)) {
    } else if (OB_FAIL(scan(param, context, range, scanner))) {
      STORAGE_LOG(WARN, "Fail to scan range, ", K(ret));
    } else {
      fprintf(fd, "sstable: sstable_meta=%s\n", to_cstring(meta_));
      for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCCESS == (ret = scanner->get_next_row(curr_row)); row_idx++) {
        if (REACH_TIME_INTERVAL(1000 * 1000) && 0 != access(real_fname, W_OK)) {
          STORAGE_LOG(ERROR, "file not exist when dump", K(real_fname));
          ret = OB_IO_ERROR;
        } else if (NULL == curr_row) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "iter NULL value");
        } else {
          fprintf(fd, "row_idx=%ld %s\n", row_idx, to_cstring(*curr_row));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (NULL != scanner) {
    scanner->~ObStoreRowIterator();
  }
  if (NULL != fd) {
    fprintf(fd, "end of sstable\n");
    fclose(fd);
    fd = NULL;
    STORAGE_LOG(INFO, "succ to dump");
  }
  return ret;
}

int ObOldSSTable::multi_scan(const ObTableIterParam& param, ObTableAccessContext& context,
    const common::ObIArray<common::ObExtStoreRange>& ranges, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = NULL;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (0 >= ranges.count()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(param), K(context), K(ranges.count()), K(ret));
  } else {
    void* buf = NULL;
    ObISSTableRowIterator* row_scanner = NULL;
    if (is_multi_version_minor_sstable()) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowMultiScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableMultiVersionRowMultiScanner();
      }
    } else {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowMultiScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableRowMultiScanner();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_scanner->init(param, context, this, &ranges))) {
        STORAGE_LOG(WARN, "Fail to open row scanner, ", K(ret), K(param), K(context), K(ranges));
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != row_scanner) {
        row_scanner->~ObISSTableRowIterator();
      }
    } else {
      row_iter = row_scanner;
    }
  }

  return ret;
}

int ObOldSSTable::estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObPartitionEst& part_est)
{
  UNUSEDx(query_flag, table_id, rowkeys, part_est);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObExtStoreRange& key_range, ObPartitionEst& part_est)
{
  UNUSEDx(query_flag, table_id, key_range, part_est);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::estimate_multi_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRange>& ranges, ObPartitionEst& part_est)
{
  UNUSEDx(query_flag, table_id, ranges, part_est);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
    const common::ObIArray<share::schema::ObColDesc>& columns, ObStoreRowIterator& row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(rowkey_size);
  UNUSED(columns);
  UNUSED(row_iter);
  STORAGE_LOG(ERROR, "sstable not support set operation", K(ret), K(table_id));
  return ret;
}

int ObOldSSTable::scan_macro_block_totally(ObMacroBlockIterator& macro_block_iter)
{
  UNUSED(macro_block_iter);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::scan_macro_block(ObMacroBlockIterator& macro_block_iter, const bool is_reverse_scan)
{
  UNUSEDx(macro_block_iter, is_reverse_scan);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::scan_macro_block(
    const ObExtStoreRange& range, ObMacroBlockIterator& macro_block_iter, const bool is_reverse_scan)
{
  UNUSEDx(range, macro_block_iter, is_reverse_scan);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::get_range(
    const int64_t idx, const int64_t concurrent_cnt, common::ObIAllocator& allocator, common::ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle meta_handle;
  const ObMacroBlockMeta* start = NULL;
  const ObMacroBlockMeta* last = NULL;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;
  ObStoreRowkey rowkey;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sstable is not in open status, ", K(ret), K_(status));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid idx, ", K(ret), K(idx));
  } else {
    int64_t macro_cnts = (macro_blocks.count() + concurrent_cnt - 1) / concurrent_cnt;
    int64_t begin = idx * macro_cnts;
    int64_t end = (idx + 1) * macro_cnts - 1;
    if (0 == begin) {
      range.get_range().set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
      range.get_range().set_left_open();
    } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(macro_blocks.at(begin - 1), meta_handle))) {
      STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
    } else if (NULL == (start = meta_handle.get_meta())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block meta is NULL.", K(ret), K(begin), K(macro_blocks));
    } else {
      rowkey.assign(start->endkey_, start->rowkey_column_number_);
      if (OB_FAIL(rowkey.deep_copy(range.get_range().get_start_key(), allocator))) {
        STORAGE_LOG(WARN, "fail to deep copy start key", K(ret), K(start->endkey_));
      } else {
        range.get_range().set_left_open();
      }
    }

    if (OB_SUCC(ret)) {
      if (end >= macro_blocks.count() - 1) {
        range.get_range().set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
        range.get_range().set_right_open();
      } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(macro_blocks.at(end), meta_handle))) {
        STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
      } else if (NULL == (last = meta_handle.get_meta())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro block meta is NULL.", K(ret), K(end), K(macro_blocks));
      } else {
        rowkey.assign(last->endkey_, last->rowkey_column_number_);
        if (OB_FAIL(rowkey.deep_copy(range.get_range().get_end_key(), allocator))) {
          STORAGE_LOG(WARN, "fail to deep copy start key", K(ret), K(last->endkey_));
        } else {
          range.get_range().set_right_closed();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
        STORAGE_LOG(WARN, "Failed to transform range to collation free and range cutoff", K(range), K(ret));
      } else {
        STORAGE_LOG(INFO,
            "succ to get range, ",
            K(ret),
            K(meta_.index_id_),
            K(idx),
            K(concurrent_cnt),
            K(range.get_range()),
            K(macro_cnts),
            "macro_block_count",
            macro_blocks.count());
      }
    }
  }

  return ret;
}

DEFINE_SERIALIZE(ObOldSSTable)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6 != meta_.sstable_format_version_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "cannot serialize old format sstable", K(ret));
  } else if (OB_FAIL(meta_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "sstable meta fail to serialize.", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(ObITable::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "failed to serialize table key", K(ret), K(buf_len), K(pos));
  } else {
    LOG_INFO("succeed to serialize sstable", K(*this));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObOldSSTable)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(buf), K(data_len));
  } else if (SSTABLE_NOT_INIT != status_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "sstable in this status cannot deserialize.", K(ret), K_(status));
  } else if (!macro_block_second_indexes_.empty() || !lob_macro_block_second_indexes_.empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable second indexes not empty", K(ret), K(*this));
  } else if (OB_FAIL(meta_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "sstable meta fail to deserialize.", K(ret), K(data_len), K(pos));
  } else if (!meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta is not valid", K_(meta));
  } else if (meta_.sstable_format_version_ >= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_2 &&
             OB_FAIL(ObITable::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize ob i table", K(ret), K(data_len), K(pos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_array_.count(); ++i) {
      OB_STORE_FILE.inc_ref(meta_.macro_block_array_.at(i));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.lob_macro_block_array_.count(); ++i) {
      OB_STORE_FILE.inc_ref(meta_.lob_macro_block_array_.at(i));
    }

    if (meta_.bloom_filter_block_id_.is_valid()) {
      OB_STORE_FILE.inc_ref(meta_.bloom_filter_block_id_);
    }

    if (0 != meta_.macro_block_second_index_) {
      // for ob 1.4, only used during reboot
      for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_array_.count(); ++i) {
        const MacroBlockId& macro_block_id = meta_.macro_block_array_.at(i);
        if (OB_FAIL(macro_block_second_indexes_.push_back(macro_block_id))) {
          STORAGE_LOG(ERROR, "add macro block id failed, ", K(ret), K(i), K(macro_block_id));
          macro_block_second_indexes_.reset();
        }
      }

      if (OB_SUCC(ret)) {
        meta_.macro_block_array_.reset();
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < meta_.lob_macro_block_array_.count(); ++i) {
        const MacroBlockId& macro_block_id = meta_.lob_macro_block_array_.at(i);
        if (OB_FAIL(lob_macro_block_second_indexes_.push_back(macro_block_id))) {
          STORAGE_LOG(ERROR, "add lob macro block id failed, ", K(ret), K(i), K(macro_block_id));
          lob_macro_block_second_indexes_.reset();
        }
      }

      if (OB_SUCC(ret)) {
        meta_.lob_macro_block_array_.reset();
      }
    }

    if (OB_SUCC(ret)) {
      status_ = SSTABLE_READY_FOR_READ;  // status need to serialized if need support continue migrate after reboot
    }
  }

  if (OB_SUCC(ret) && meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_5) {
    if (meta_.sstable_format_version_ <= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_2) {
      if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
        LOG_WARN("failed to init file ctx", K(ret));
      } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
        LOG_WARN("failed to init lob file ctx", K(ret));
      }
    }
    if (OB_SUCC(ret) && meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_4) {
      if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
        LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
      } else {
        meta_.bloom_filter_block_id_.reset();
        meta_.bloom_filter_block_id_in_files_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      meta_.logical_data_version_ = meta_.data_version_;
    }
  }
  if (OB_SUCC(ret) && meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6) {
    meta_.logical_data_version_ = is_major_sstable() ? meta_.logical_data_version_ : get_snapshot_version();
    meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
  }
  LOG_TRACE("finish to deserialize sstable", K(ret), K(*this));
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObOldSSTable)
{
  return ObITable::get_serialize_size() + meta_.get_serialize_size();
}

int ObOldSSTable::append_macro_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx& write_ctx,
    common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, common::ObIArray<int64_t>& block_id_in_files,
    blocksstable::ObStoreFileCtx& file_ctx)
{
  int ret = OB_SUCCESS;
  int64_t new_count = macro_block_ids.count() + write_ctx.macro_block_list_.count();
  int64_t new_file_count = file_ctx.file_list_.count() + write_ctx.file_ctx_.file_list_.count();
  int64_t cur_file_id_files = file_ctx.block_count_per_file_ * file_ctx.file_list_.count();

  if ((file_ctx.need_file_id_list() && macro_block_ids.count() != block_id_in_files.count()) ||
      file_ctx.file_system_type_ != write_ctx.file_ctx_.file_system_type_ ||
      file_ctx.file_type_ != write_ctx.file_ctx_.file_type_ ||
      file_ctx.block_count_per_file_ != write_ctx.file_ctx_.block_count_per_file_ || !write_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        "block_ids_count",
        macro_block_ids.count(),
        "block_in_files",
        block_id_in_files.count(),
        K(write_ctx),
        K(file_ctx));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.macro_block_list_.count(); ++i) {
    const MacroBlockId& macro_block_id = write_ctx.macro_block_list_.at(i);
    if (OB_FAIL(add_macro_block_meta(macro_block_id))) {
      LOG_WARN("failed to add macro block meta", K(ret), K(macro_block_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_block_ids.reserve(new_count))) {
      LOG_WARN("Failed to reserve macro block ids", K(ret), K(new_count));
    } else if (write_ctx.file_ctx_.need_file_id_list()) {
      if (OB_FAIL(block_id_in_files.reserve(new_count))) {
        LOG_WARN("failed to reserve block id in files", K(ret), K(new_count));
      } else if (OB_FAIL(file_ctx.file_list_.reserve(new_file_count))) {
        LOG_WARN("failed to reserve new file count", K(ret), K(new_file_count));
      }
    }
  }

  // has reserved, should not fail
  for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.macro_block_list_.count(); ++i) {
    const MacroBlockId& macro_block_id = write_ctx.macro_block_list_.at(i);
    if (OB_FAIL(macro_block_ids.push_back(macro_block_id))) {
      LOG_ERROR("failed to add macro block id, fatal error", K(ret), K(macro_block_id));
      ob_abort();
    } else if (write_ctx.file_ctx_.need_file_id_list() && OB_FAIL(block_id_in_files.push_back(cur_file_id_files + i))) {
      LOG_ERROR("Failed to add block id in files, fatal error", K(ret));
      ob_abort();
    } else {
      OB_STORE_FILE.inc_ref(macro_block_id);
    }
  }

  // has reserved, should not fail
  if (OB_SUCC(ret) && write_ctx.file_ctx_.need_file_id_list()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.file_ctx_.file_list_.count(); ++i) {
      if (OB_FAIL(file_ctx.file_list_.push_back(write_ctx.file_ctx_.file_list_.at(i)))) {
        LOG_ERROR("failed to add file list, fatal error", K(ret));
        ob_abort();
      }
    }
  }

  return ret;
}

int ObOldSSTable::add_macro_block_meta(const ObIArray<MacroBlockId>& macro_block_ids)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
    if (OB_FAIL(add_macro_block_meta(macro_block_ids.at(i)))) {
      LOG_WARN("failed to add macro block meta", K(ret), K(i));
    }
  }
  return ret;
}

int ObOldSSTable::add_macro_block_meta(const MacroBlockId& macro_block_id)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle meta_handle;
  const ObMacroBlockMeta* meta = NULL;

  if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(macro_block_id, meta_handle))) {
    STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret), K(macro_block_id));
  } else if ((NULL == (meta = meta_handle.get_meta())) || !meta->is_data_block()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid macro meta, ", K(ret), K(macro_block_id), KP(meta));
  } else if (!meta->is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "meta is not valid", K(ret), K(meta));
  } else if (meta->is_lob_data_block()) {
    meta_.lob_macro_block_count_++;
    if (meta->data_version_ < meta_.data_version_) {
      meta_.lob_use_old_macro_block_count_++;
    }
    STORAGE_LOG(DEBUG, "[LOB] sstable add lob macro block", K(*meta), K(meta_), K(ret));
  } else if (meta->is_sstable_data_block()) {
    meta_.macro_block_count_++;
    if (meta->data_version_ < meta_.data_version_) {
      meta_.use_old_macro_block_count_++;
    }
    meta_.row_count_ += meta->row_count_;
  }

  if (OB_SUCC(ret) && !meta->is_bloom_filter_data_block()) {
    meta_.occupy_size_ += meta->occupy_size_;
    meta_.data_checksum_ = ob_crc64_sse42(meta_.data_checksum_, &meta->data_checksum_, sizeof(meta_.data_checksum_));
    if (OB_FAIL(accumulate_macro_column_checksum(*meta))) {
      STORAGE_LOG(WARN, "fail to accumulate macro column checksum", K(ret));
    }
  }
  return ret;
}

int ObOldSSTable::accumulate_macro_column_checksum(const blocksstable::ObMacroBlockMeta& macro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(macro_meta));
  } else if (macro_meta.is_lob_data_block()) {
    // lob column checksum temporarily disabled
  } else if (macro_meta.is_sstable_data_block()) {
    if (blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_) {
      if (OB_FAIL(accumulate_macro_column_checksum_impl(macro_meta, meta_.new_column_metas_))) {
        STORAGE_LOG(WARN, "fail to accumulate macro block column checksum", K(ret), K(macro_meta));
      }
    } else if (blocksstable::CCM_VALUE_ONLY == meta_.checksum_method_) {
      if (OB_FAIL(accumulate_macro_column_checksum_impl(macro_meta, meta_.column_metas_))) {
        STORAGE_LOG(WARN, "fail to accumulate macro block column checksum", K(ret), K(macro_meta));
      }
    }
  }
  return ret;
}

int ObOldSSTable::accumulate_macro_column_checksum_impl(
    const blocksstable::ObMacroBlockMeta& macro_meta, common::ObIArray<ObSSTableColumnMeta>& column_metas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(macro_meta));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_metas.count(); ++i) {
      int64_t j = 0;
      for (; OB_SUCC(ret) && j < macro_meta.column_number_; ++j) {
        if (column_metas.at(i).column_id_ == macro_meta.column_id_array_[j]) {
          break;
        }
      }
      if (j < macro_meta.column_number_) {
        column_metas.at(i).column_checksum_ += macro_meta.column_checksum_[j];
      } else {
        // column not found in macro meta, it it a new added column, add default column checksum
        column_metas.at(i).column_checksum_ += macro_meta.row_count_ * column_metas.at(i).column_default_checksum_;
      }
    }
  }
  return ret;
}

int ObOldSSTable::find_macros(const ExtStoreRowkeyArray& rowkeys, ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks)
{
  UNUSEDx(rowkeys, macro_blocks);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::find_macros(
    const ObExtStoreRange& range, common::ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks)
{
  UNUSEDx(range, macro_blocks);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::fill_split_handles(ObSSTableSplitCtx& split_ctx, const int64_t merge_round)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(split_ctx, merge_round);
  return ret;
}

int ObOldSSTable::fill_split_handle(const ObCreateSSTableParam& split_table_param, const int64_t macro_split_point,
    const int64_t lob_split_point, ObTableHandle& split_table_handle)
{
  UNUSEDx(split_table_param, macro_split_point, lob_split_point, split_table_handle);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::locate_lob_macro(
    const ObExtStoreRowkey& ext_rowkey, const bool upper_bound, MacroBlockArray::iterator& iter)
{
  UNUSEDx(ext_rowkey, upper_bound, iter);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::find_lob_split_point(const int64_t macro_split_point, int64_t& lob_split_point)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle* meta_handle;
  const ObMacroBlockMeta* meta = NULL;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not in not ready for acccess", K(ret), K_(status));
  } else if (OB_UNLIKELY(macro_split_point < 0 || macro_split_point > meta_.macro_block_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument to find lob macro offset",
        K(macro_split_point),
        K(meta_.macro_block_array_.count()),
        K(ret));
  } else if (macro_split_point == meta_.macro_block_array_.count()) {
    lob_split_point = meta_.lob_macro_block_array_.count();
  } else if (OB_ISNULL(meta_handle = macro_block_metas_.at(macro_split_point - 1))) {
    STORAGE_LOG(WARN, "Fail to get macro block meta", K(ret));
  } else if (OB_ISNULL(meta = meta_handle->get_meta())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret), KP(meta), K_(meta_.macro_block_array));
  } else if (OB_UNLIKELY(!meta->is_valid())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Invalid macro block meta", K(ret), K(*meta));
  } else {
    ObExtStoreRowkey ext_rowkey;
    MacroBlockArray::iterator iter;
    ext_rowkey.get_store_rowkey().assign(meta->endkey_, meta->rowkey_column_number_);
    if (OB_FAIL(locate_lob_macro(ext_rowkey, true, iter))) {
      STORAGE_LOG(WARN, "Failed to locate lob macro block", K(ext_rowkey), K(ret));
    } else {
      lob_split_point = iter - meta_.lob_macro_block_array_.begin();
    }
  }

  return ret;
}

int ObOldSSTable::find_lob_macros(const ObExtStoreRange& range, ObIArray<MacroBlockId>& lob_macro_blocks)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not in not ready for acccess", K(ret), K_(status));
  } else if (!range.get_range().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(range));
  } else if (!has_lob_macro_blocks()) {
    // no lob macros
  } else {
    MacroBlockArray::iterator iter_start, iter_end;
    if (OB_FAIL(locate_lob_macro(
            range.get_ext_start_key(), !range.get_range().get_border_flag().inclusive_start(), iter_start))) {
      STORAGE_LOG(WARN, "Failed to locate lob start macro block", K(range.get_ext_start_key()), K(ret));
    } else if (iter_start == meta_.lob_macro_block_array_.end()) {
      // no block found
    } else if (OB_FAIL(locate_lob_macro(range.get_ext_end_key(), true, iter_end))) {
      STORAGE_LOG(WARN, "Failed to locate lob end macro block", K(range.get_ext_end_key()), K(ret));
    } else {
      for (; OB_SUCC(ret) && iter_start < iter_end; iter_start++) {
        if (OB_FAIL(lob_macro_blocks.push_back(*iter_start))) {
          STORAGE_LOG(WARN, "Failed to push lob macro block to array", K(*iter_start), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObOldSSTable::find_macros(const ObIArray<common::ObExtStoreRange>& ranges,
    ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks, ObIArray<int64_t>& end_block_idx_of_ranges)
{
  UNUSEDx(ranges, macro_blocks, end_block_idx_of_ranges);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::sort_ranges(const ObIArray<ObStoreRange>& ranges, ObIArray<ObStoreRange>& ordered_ranges)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 32> pos;
  ordered_ranges.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    int64_t j = 0;
    const ObStoreRange& range = ranges.at(i);
    for (j = 0; OB_SUCC(ret) && j < pos.count(); j++) {
      if ((range.get_start_key().simple_equal(ranges.at(pos.at(j)).get_start_key()) &&
              range.get_border_flag().inclusive_start() && !ranges.at(pos.at(j)).get_border_flag().inclusive_start()) ||
          (0 > range.get_start_key().compare(ranges.at(pos.at(j)).get_start_key()))) {
        break;
      }
    }

    if (j == pos.count()) {
      ret = pos.push_back(i);
    } else {

      int64_t prev = pos.at(j);
      pos.at(j) = i;
      for (int64_t k = j + 1; k < pos.count(); ++k) {
        // swap prev and k-th value
        prev = prev ^ pos.at(k);
        pos.at(k) = prev ^ pos.at(k);
        prev = prev ^ pos.at(k);
      }
      ret = pos.push_back(prev);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < pos.count(); ++i) {
    ret = ordered_ranges.push_back(ranges.at(pos.at(i)));
  }

  return ret;
}

// Note: assuming inputs are ordered
int ObOldSSTable::remove_duplicate_ordered_block_id(
    const ObIArray<blocksstable::ObMacroBlockCtx>& origin_blocks, ObIArray<blocksstable::ObMacroBlockCtx>& blocks)
{
  int ret = OB_SUCCESS;
  blocks.reset();
  if (origin_blocks.count() > 0) {
    ret = blocks.push_back(origin_blocks.at(0));
    for (int64_t i = 1; OB_SUCC(ret) && i < origin_blocks.count(); ++i) {
      if (origin_blocks.at(i).get_macro_block_id() != origin_blocks.at(i - 1).get_macro_block_id()) {
        ret = blocks.push_back(origin_blocks.at(i));
      }
    }
  }
  return ret;
}

/*
  SPLIT the input ranges into group based on their corresponding MACROS

  allocator         IN			allocator used to deep copy ranges
  ranges            IN   		all input ranges
  type						  IN      decide the performance of this function
  macros_count		  OUT			the macros count belong to these ranges
  total_task_count  IN     	total_task_count
  splitted_ranges   OUT   	all output ranges
  split_index       OUT   	position of the last range of the group, one
                            for each group
 */
int ObOldSSTable::query_range_to_macros(ObIAllocator& allocator, const ObIArray<ObStoreRange>& ranges,
    const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
    ObIArray<ObStoreRange>* splitted_ranges, ObIArray<int64_t>* split_index)
{
  int ret = OB_SUCCESS;
  int64_t expected_task_count = 0;
  uint64_t tmp_count = 0;
  if ((sql::OB_GET_MACROS_COUNT_BY_QUERY_RANGE == type && macros_count == nullptr) ||
      (sql::OB_GET_BLOCK_RANGE == type &&
          (total_task_count == nullptr || splitted_ranges == nullptr || split_index == nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid params", K(type));
  } else if ((sql::OB_GET_BLOCK_RANGE == type) && FALSE_IT(expected_task_count = *total_task_count)) {

  } else if (ranges.count() >= 1) {
    ObSEArray<ObStoreRange, 32> ordered_ranges;
    if (OB_FAIL(sort_ranges(ranges, ordered_ranges))) {
      STORAGE_LOG(WARN, "failed to sort input ranges", K(ret));
    } else {

      ObSEArray<ObExtStoreRange, 32> ext_ranges;
      ObSEArray<blocksstable::ObMacroBlockCtx, 32> macro_blocks;

      // generate all ext_ranges
      for (int64_t i = 0; OB_SUCC(ret) && i < ordered_ranges.count(); ++i) {
        ObExtStoreRange ext_range(ordered_ranges.at(i));
        if (OB_FAIL(ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
          STORAGE_LOG(WARN, "failed to set start/end key", K(ret));
        } else if (OB_FAIL(ext_ranges.push_back(ext_range))) {
          STORAGE_LOG(WARN, "failed add ext_range", K(ret), K(ext_range));
        } else {
          STORAGE_LOG(DEBUG, "ext range added", K(ext_range), K(ext_ranges.at(ext_ranges.count() - 1)));
        }
      }

      if (OB_SUCC(ret)) {
        ObSEArray<int64_t, 32> end_block_idx_of_ranges;
        ObSEArray<blocksstable::ObMacroBlockCtx, 32> blocks;
        // find all macros
        if (OB_FAIL(find_macros(ext_ranges, macro_blocks, end_block_idx_of_ranges))) {
          STORAGE_LOG(WARN, "failed to find macros", K(ret), K(ext_ranges));
        } else if (OB_FAIL(remove_duplicate_ordered_block_id(macro_blocks, blocks))) {
          STORAGE_LOG(WARN, "failed to remove duplicate macro blocks", K(ret), K(macro_blocks));
        } else if (sql::OB_GET_MACROS_COUNT_BY_QUERY_RANGE == type) {
          tmp_count = blocks.count();
        } else if (sql::OB_GET_BLOCK_RANGE == type && expected_task_count <= 1) {
          if (1 == expected_task_count) {
            FOREACH_CNT_X(it, ranges, OB_SUCC(ret))
            {
              if (OB_FAIL(splitted_ranges->push_back(*it))) {
                LOG_WARN("push back ranges failed", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
              LOG_WARN("push back offsets failed", K(ret));
            }
          }
        } else if (sql::OB_GET_BLOCK_RANGE == type) {
          // number of macros for each splitted range
          ObSEArray<int64_t, 32> num_macros;

          if (blocks.count() < expected_task_count) {
            for (int64_t i = 0; OB_SUCC(ret) && i < blocks.count(); ++i) {
              ret = num_macros.push_back(1);
            }
          } else {
            // set the base number
            for (int64_t i = 0; OB_SUCC(ret) && i < expected_task_count; ++i) {
              if (OB_FAIL(num_macros.push_back((int)(blocks.count() / expected_task_count)))) {}
            }
            // add the remainder
            for (int64_t i = 0; OB_SUCC(ret) && i < blocks.count() % expected_task_count; ++i) {
              num_macros.at(i) += 1;
            }
          }

          STORAGE_LOG(DEBUG,
              "found macros",
              K(ret),
              K(ordered_ranges),
              "# of macros",
              blocks.count(),
              K(expected_task_count),
              K(num_macros));

          // split the range
          if (OB_SUCC(ret) && blocks.count() > 1) {

            // the last macro block for current group
            ObMacroBlockMetaHandle meta_handle;
            const ObMacroBlockMeta* end_block_meta = NULL;
            int64_t group_id = 0;
            int64_t curr_block_index = num_macros.at(group_id) - 1;

            // For each range, decide which group it belongs - split it
            // into multiple ranges if necessary.
            for (int64_t idx = 0; OB_SUCC(ret) && idx < ordered_ranges.count();) {

              if (curr_block_index >= blocks.count() || curr_block_index < 0) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "invalid argument", K(curr_block_index), K(blocks));
              } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(
                             blocks.at(curr_block_index).get_macro_block_id(), meta_handle))) {
                STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
              } else if (OB_ISNULL(end_block_meta = meta_handle.get_meta())) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "The macro block meta is NULL, ", K(ret));
              } else {
                ObStoreRowkey block_endkey(end_block_meta->endkey_, end_block_meta->rowkey_column_number_);
                ObStoreRange& cur_range = ordered_ranges.at(idx);

                if (cur_range.get_start_key().compare(block_endkey) > 0 &&
                    !(0 < idx && (end_block_idx_of_ranges.at(idx) == end_block_idx_of_ranges.at(idx - 1)))) {

                  // current range belongs to the next group
                  if (group_id >= num_macros.count() - 1) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN,
                        "invalid group",
                        K(curr_block_index),
                        K(group_id),
                        K(num_macros),
                        K(cur_range),
                        K(block_endkey));
                  } else if (splitted_ranges->count() < 1) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "invalid argument", K(splitted_ranges->count()));
                  } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
                    STORAGE_LOG(WARN, "failed to add split index", K(splitted_ranges->count()));
                  } else {
                    // move to the next group's end block
                    curr_block_index += num_macros.at(++group_id);
                  }
                } else if (  // first range and it has no blocks
                    -1 == end_block_idx_of_ranges.at(idx)
                    // non-first range and has no blocks
                    || (0 < idx && (end_block_idx_of_ranges.at(idx) == end_block_idx_of_ranges.at(idx - 1)))
                    // regular range within the boundary
                    || !(cur_range.get_end_key().compare(block_endkey) > 0)
                    // last block and range end key is larger - i.e. MAX
                    || (group_id == num_macros.count() - 1 && curr_block_index == blocks.count() - 1)) {

                  // current range is WITHIN the boundary of current macro
                  // block or has no blocks

                  ObStoreRange* copied_range = NULL;
                  if (OB_FAIL(deep_copy_range(allocator, cur_range, copied_range))) {
                    STORAGE_LOG(WARN, "failed to deep copy range", K(ret));
                  } else if (OB_ISNULL(copied_range)) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "invalid copied range");
                  } else if (OB_FAIL(splitted_ranges->push_back(*copied_range))) {
                    STORAGE_LOG(WARN, "failed to add copied range");
                  } else {
                    // move to the next range
                    ++idx;
                    continue;
                  }
                } else if (group_id == num_macros.count() - 1) {
                  ret = OB_ERR_UNEXPECTED;
                  STORAGE_LOG(WARN,
                      "invalid group id",
                      K(end_block_idx_of_ranges),
                      K(cur_range.get_end_key()),
                      K(block_endkey),
                      K(num_macros),
                      K(group_id),
                      K(idx),
                      K(ordered_ranges));
                } else {

                  // current range is BEYOND the boundary of the current block
                  // split the current range into two parts, one within the
                  // boundary of the block and one beyond

                  // all macro block are left-open and right-close and the
                  // current range's start key is guaranteed to be larger than
                  // the start key of the current block's start. Therefore, we
                  // split the current ranges into two parts.
                  // The first part is from range's start key to block's
                  // end_key(inclusive) and the second is from block's
                  // end_key(exclusive) to range's end key.
                  ObStoreRange first_half;
                  first_half.set_table_id(cur_range.get_table_id());
                  first_half.set_border_flag(cur_range.get_border_flag());
                  first_half.set_right_closed();
                  first_half.set_start_key(cur_range.get_start_key());
                  first_half.set_end_key(block_endkey);

                  ObStoreRange second_half;
                  second_half.set_table_id(cur_range.get_table_id());
                  second_half.set_border_flag(cur_range.get_border_flag());
                  second_half.set_left_open();
                  second_half.set_start_key(block_endkey);
                  second_half.set_end_key(cur_range.get_end_key());

                  // move to the next group's end block
                  curr_block_index += num_macros.at(++group_id);

                  ObStoreRange* copied_first_half = NULL;
                  ObStoreRange* copied_second_half = NULL;
                  if (OB_FAIL(deep_copy_range(allocator, first_half, copied_first_half))) {
                    STORAGE_LOG(WARN, "failed to deep copy range", K(ret));
                  } else if (OB_FAIL(deep_copy_range(allocator, second_half, copied_second_half))) {
                    STORAGE_LOG(WARN, "failed to deep copy range", K(ret));
                  } else if (OB_ISNULL(copied_first_half) || OB_ISNULL(copied_second_half)) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "invalid copied range", K(copied_first_half), K(copied_second_half));
                  } else if (OB_FAIL(splitted_ranges->push_back(*copied_first_half))) {
                    STORAGE_LOG(WARN, "failed to add splitted range", K(ret));
                  } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
                    STORAGE_LOG(WARN, "failed to add split index", K(splitted_ranges->count()));
                  } else {
                    // replace the current range with the second half
                    cur_range = *copied_second_half;
                    // note that we DID NOT increment idx
                    STORAGE_LOG(DEBUG,
                        "split macros",
                        K(splitted_ranges),
                        K(*copied_first_half),
                        K(*copied_second_half),
                        K(idx),
                        K(curr_block_index));
                  }
                }
              }
            }

            // last group
            if (OB_SUCC(ret)) {
              if (splitted_ranges->count() == 0) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "failed to split ranges", K(ret));
              } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
                STORAGE_LOG(WARN, "failed to end splitting", K(ret));
              } else {
                STORAGE_LOG(DEBUG, "finish splitting ranges", K(splitted_ranges), K(split_index));
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "undefined type", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && sql::OB_GET_MACROS_COUNT_BY_QUERY_RANGE == type) {
    *macros_count = tmp_count;
  }
  return ret;
}

int ObOldSSTable::prefix_exist(ObRowsInfo& rows_info, bool& may_exist)
{
  int ret = OB_SUCCESS;
  ObSSTableRowExister exister;
  if (!rows_info.is_valid() || rows_info.table_id_ != meta_.index_id_) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rows_info), K(meta_.index_id_));
  } else if (rows_info.table_id_ != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(rows_info.table_id_), K(*this));
  } else if (meta_.macro_block_array_.count() == 0) {  // skip
  } else {
    const ObExtStoreRowkey& rowkey = rows_info.get_prefix_rowkey();
    if (OB_FAIL(exister.init(
            rows_info.exist_helper_.table_iter_param_, rows_info.exist_helper_.table_access_context_, this, &rowkey))) {
      STORAGE_LOG(WARN, "Failed to init sstable row exister", K(ret), K(rows_info));
    } else {
      const ObStoreRow* store_row = NULL;
      may_exist = false;
      if (OB_FAIL(exister.get_next_row(store_row))) {
        STORAGE_LOG(WARN, "Failed to get exist row", K(rowkey), K(ret));
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null store row", K(ret));
      } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
      } else if (ObActionFlag::OP_DEL_ROW == store_row->flag_ || ObActionFlag::OP_ROW_EXIST == store_row->flag_) {
        may_exist = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected row flag", K(ret), K(store_row->flag_));
      }
      if (lib::is_diagnose_info_enabled()) {
        exister.report_stat();
      }
    }
  }

  return ret;
}

int ObOldSSTable::exist(ObRowsInfo& rows_info, bool& is_exist, bool& all_rows_found)
{
  UNUSEDx(rows_info, is_exist, all_rows_found);
  return OB_NOT_SUPPORTED;
}

int ObOldSSTable::exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
    const common::ObIArray<share::schema::ObColDesc>& columns, bool& is_exist, bool& has_found)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_STORE_ROW_EXISTER);
  ObExtStoreRowkey ext_rowkey(rowkey);

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not ready for access", K(ret), K_(status));
  } else if (table_id != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(table_id), K(*this));
  } else if (!rowkey.is_valid() || columns.count() <= 0 || table_id != meta_.index_id_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid arguments", K(ret), "column count", columns.count(), K(rowkey), K(table_id), K(meta_.index_id_));
  } else if (OB_FAIL(ext_rowkey.to_collation_free_on_demand_and_cutoff_range(allocator))) {
    STORAGE_LOG(WARN, "Failed to collation free on demand", K(ext_rowkey), K(ret));
  } else {
    ObTableIterParam iter_param;
    ObTableAccessContext access_context;
    ObBlockCacheWorkingSet block_cache_ws;
    common::ObVersionRange trans_version_range;
    common::ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;

    if (OB_FAIL(block_cache_ws.init(extract_tenant_id(meta_.index_id_)))) {
      STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(meta_.index_id_));
    } else if (OB_FAIL(
                   access_context.init(query_flag, ctx, allocator, allocator, block_cache_ws, trans_version_range))) {
      LOG_WARN("failed to init access context", K(ret), "pkey", key_.pkey_);
    } else {
      iter_param.table_id_ = meta_.index_id_;
      iter_param.schema_version_ = 0;
      iter_param.rowkey_cnt_ = rowkey.get_obj_cnt();
      iter_param.out_cols_ = &columns;
    }

    const ObStoreRow* store_row = NULL;
    is_exist = false;
    has_found = false;
    ObStoreRowIterator* iter = NULL;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(build_exist_iterator(iter_param, access_context, ext_rowkey, iter))) {
        LOG_WARN("failed to build exist iterator", K(ret));
      } else if (OB_FAIL(iter->get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null store row", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
      } else if (ObActionFlag::OP_DEL_ROW == store_row->flag_) {
        has_found = true;
      } else if (ObActionFlag::OP_ROW_EXIST == store_row->flag_) {
        is_exist = true;
        has_found = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected row flag", K(ret), K(store_row->flag_));
      }
    } else if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_NOT_NULL(iter)) {
      if (lib::is_diagnose_info_enabled()) {
        iter->report_stat();
      }
      iter->~ObStoreRowIterator();
    }
  }

  return ret;
}

int ObOldSSTable::get_block_meta(const MacroBlockId& macro_id, const ObMacroBlockMeta*& macro_meta)
{
  int ret = OB_SUCCESS;
  int64_t block_idx = 0;
  ObMacroBlockMetaHandle* meta_handle = NULL;
  macro_meta = NULL;
  if (OB_FAIL(block_id_map_.get(macro_id, block_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      STORAGE_LOG(WARN, "Fail to get macro block idx from map, ", K(ret), K(macro_id));
    }
  } else if (OB_FAIL(macro_block_metas_.at(block_idx, meta_handle))) {
    STORAGE_LOG(WARN, "Fail to get macro meta handle, ", K(ret), K(block_idx));
  } else {
    macro_meta = meta_handle->get_meta();
  }
  return ret;
}

int ObOldSSTable::build_block_item_map()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObMacroBlockMetaHandle* meta_handle = NULL;
  const ObMacroBlockMeta* macro_meta = NULL;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;

  block_id_map_.destroy();
  micro_block_count_ = 0;

  if (meta_.macro_block_count_ != macro_blocks.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
        "macro_block_count is not consistent",
        K(meta_.macro_block_count_),
        "macro_block_count",
        macro_blocks.count(),
        K(meta_));
  } else if (meta_.macro_block_count_ > 0 && OB_FAIL(block_id_map_.create(meta_.macro_block_count_, &allocator_))) {
    STORAGE_LOG(WARN, "Fail to create block id map, ", K(ret));
  } else {
    for (int64_t i = 0; i < macro_block_metas_.count(); ++i) {
      if (NULL != (meta_handle = macro_block_metas_.at(i))) {
        meta_handle->~ObMacroBlockMetaHandle();
      }
    }
    macro_block_metas_.reset();
  }

  macro_block_metas_.set_capacity(static_cast<uint32_t>(meta_.macro_block_count_));
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_count_; ++i) {
    bool is_added = false;
    if (NULL == (buf = allocator_.alloc(sizeof(ObMacroBlockMetaHandle)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
    } else if (NULL == (meta_handle = new (buf) ObMacroBlockMetaHandle())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Fail to new meta handle, ", K(ret));
    } else if (OB_FAIL(macro_block_metas_.push_back(meta_handle))) {
      STORAGE_LOG(WARN, "Fail to push macro meta handle to array, ", K(ret));
    } else {
      is_added = true;
      if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(macro_blocks.at(i), *meta_handle))) {
        STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret), K(i), "macro block id", macro_blocks.at(i));
      } else if (OB_ISNULL(macro_meta = meta_handle->get_meta())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid argument", K(i), K(ret));
      } else if (OB_FAIL(block_id_map_.set(macro_blocks.at(i), i))) {
        STORAGE_LOG(WARN, "Fail to set block id map, ", K(ret), K(i), K(macro_blocks.at(i)));
      } else {
        micro_block_count_ += macro_meta->micro_block_count_;
      }
    }
    if (OB_FAIL(ret) && !is_added) {
      if (NULL != meta_handle) {
        meta_handle->~ObMacroBlockMetaHandle();
        meta_handle = NULL;
      }
    }
  }
  return ret;
}

int ObOldSSTable::build_lob_block_map()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObMacroBlockMetaHandle* meta_handle = NULL;
  const ObMacroBlockMeta* macro_meta = NULL;
  const ObIArray<MacroBlockId>& lob_macro_blocks = meta_.lob_macro_block_array_;

  lob_block_id_map_.destroy();
  if (meta_.lob_macro_block_count_ == 0) {
  } else if (meta_.lob_macro_block_count_ != lob_macro_blocks.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
        "lob_macro_block_count is not consistent",
        K(meta_.lob_macro_block_count_),
        "lob_macro_block_count",
        lob_macro_blocks.count());
  } else if (meta_.lob_macro_block_count_ > 0 &&
             OB_FAIL(lob_block_id_map_.create(meta_.lob_macro_block_count_, &allocator_))) {
    STORAGE_LOG(WARN, "Failed to create lob_block_id_map", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < lob_macro_block_metas_.count(); ++i) {
      if (NULL != (meta_handle = lob_macro_block_metas_.at(i))) {
        meta_handle->~ObMacroBlockMetaHandle();
      }
    }
    lob_macro_block_metas_.reset();
  }

  lob_macro_block_metas_.set_capacity(static_cast<uint32_t>(meta_.lob_macro_block_count_));
  if (OB_SUCC(ret) && meta_.lob_macro_block_count_ > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.lob_macro_block_count_; ++i) {
      bool is_added = false;
      const MacroBlockId& macro_block_id = lob_macro_blocks.at(i);
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroBlockMetaHandle)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else if (OB_ISNULL(meta_handle = new (buf) ObMacroBlockMetaHandle())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Fail to new meta handle, ", K(ret));
      } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(macro_block_id, *meta_handle))) {
        STORAGE_LOG(WARN, "Fail to get lob macro block meta", K(ret), K(i), K(macro_block_id));
      } else if (OB_ISNULL(macro_meta = meta_handle->get_meta())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid argument", K(i), K(ret));
      } else if (OB_UNLIKELY(!macro_meta->is_valid() || !macro_meta->is_lob_data_block())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Invalid lob macro block meta", K(*macro_meta), K(macro_block_id), K(ret));
      } else if (OB_FAIL(lob_macro_block_metas_.push_back(meta_handle))) {
        STORAGE_LOG(WARN, "Fail to push lob macro meta handle to array", K(ret));
      } else {
        int64_t lob_data_version = 0;
        is_added = true;
        if (macro_meta->data_version_ > 0) {
          lob_data_version = macro_meta->data_version_;
        } else if (macro_meta->snapshot_version_ > 0 && 0 == macro_meta->data_version_) {
          lob_data_version = macro_meta->snapshot_version_;
        } else {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "Invalid snapshot version and data version of lob macro meta", K(*macro_meta), K(ret));
        }
        if (OB_SUCC(ret)) {
          ObMacroDataSeq macro_data_seq(macro_meta->data_seq_);
          ObLogicMacroBlockId logical_id(macro_meta->data_seq_, lob_data_version);
          if (OB_UNLIKELY(!logical_id.is_valid() || !macro_data_seq.is_valid() || !macro_data_seq.is_lob_block())) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(WARN,
                "lob macro block logical id is invalid",
                K(logical_id),
                K(macro_data_seq),
                K(*macro_meta),
                K(macro_block_id),
                K(ret));
          } else if (OB_FAIL(lob_block_id_map_.set(logical_id, macro_block_id))) {
            STORAGE_LOG(WARN, "Failed to set lob macro block logic map", K(logical_id), K(macro_block_id), K(ret));
          }
        }
      }
      if (OB_FAIL(ret) && !is_added) {
        if (NULL != meta_handle) {
          meta_handle->~ObMacroBlockMetaHandle();
          meta_handle = NULL;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    lob_block_id_map_.destroy();
  }
  return ret;
}

int ObOldSSTable::check_logical_data_version()
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle* meta_handle;
  const ObMacroBlockMeta* meta = NULL;

  // buffer table will use baseline macroblock, no need to check logical data version
  if (is_major_sstable()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_metas_.count(); ++i) {
      if (OB_ISNULL(meta_handle = macro_block_metas_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro meta handle", K(ret));
      } else if (OB_ISNULL(meta = meta_handle->get_meta())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret));
      } else if (OB_UNLIKELY(!meta->is_valid())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Invalid macro block meta", K(*meta), K(ret));
      } else if (meta->data_version_ > meta_.logical_data_version_) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Invalid data_version of macro block", K(ret), K(*meta), K(meta_.logical_data_version_));
      }
    }
    if (OB_SUCC(ret) && has_lob_macro_blocks()) {
      // data version of LOB macro block should same to the data macro block
      for (int64_t i = 0; OB_SUCC(ret) && i < lob_macro_block_metas_.count(); ++i) {
        if (OB_ISNULL(meta_handle = lob_macro_block_metas_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null lob macro meta handle", K(ret));
        } else if (OB_ISNULL(meta = meta_handle->get_meta())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret));
        } else if (OB_UNLIKELY(!meta->is_valid())) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "Invalid macro block meta", K(*meta), K(ret));
        } else if (meta->data_version_ > meta_.logical_data_version_) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "Invalid data_version of macro block", K(ret), K(*meta), K(meta_.logical_data_version_));
        }
      }
    }
  }

  return ret;
}

int ObOldSSTable::clean_lob_column_checksum()
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle* meta_handle;
  const ObMacroBlockMeta* meta = NULL;
  ObBitSet<OB_ROW_MAX_COLUMNS_COUNT> lob_col_bitset;

  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_count_; ++i) {
    if (OB_ISNULL(meta_handle = macro_block_metas_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null macro meta handle", K(ret));
    } else if (OB_ISNULL(meta = meta_handle->get_meta())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret));
    } else if (OB_UNLIKELY(!meta->is_valid())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Invalid macro block meta", K(*meta), K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < meta->column_number_; j++) {
        if (!ob_is_large_text(meta->column_type_array_[j].get_type())) {
          // tinytext/tinyblob skip
        } else if (!lob_col_bitset.has_member(meta->column_id_array_[j]) &&
                   OB_FAIL(lob_col_bitset.add_member(meta->column_id_array_[j]))) {
          STORAGE_LOG(WARN, "Fail to lob column id to bitset", "column_id", meta->column_id_array_[j], K(j), K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && lob_col_bitset.num_members() > 0) {
    STORAGE_LOG(INFO, "Clean lob column checksum in sstable", K(ret));
    for (int64_t i = 0; i < meta_.column_cnt_; i++) {
      if (lob_col_bitset.has_member(meta_.column_metas_.at(i).column_id_)) {
        meta_.column_metas_.at(i).column_checksum_ = 0;
        meta_.column_metas_.at(i).column_default_checksum_ = 0;
      }
    }
  }

  return ret;
}

int ObOldSSTable::get_concurrent_cnt(int64_t tablet_size, int64_t& concurrent_cnt)
{
  int ret = OB_SUCCESS;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;

  if (tablet_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tablet size is invalid", K(tablet_size), K(ret));
  } else if (0 == tablet_size) {
    concurrent_cnt = 1;
  } else {
    const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
    if (((macro_blocks.count() * macro_block_size + tablet_size - 1) / tablet_size) <= MAX_MERGE_THREAD) {
      concurrent_cnt = (macro_blocks.count() * macro_block_size + tablet_size - 1) / tablet_size;
      if (0 == concurrent_cnt) {
        concurrent_cnt = 1;
      }
    } else {
      int64_t macro_cnts = (macro_blocks.count() + MAX_MERGE_THREAD - 1) / MAX_MERGE_THREAD;
      concurrent_cnt = (macro_blocks.count() + macro_cnts - 1) / macro_cnts;
    }
  }
  return ret;
}

int ObOldSSTable::check_collation_free_valid()
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle* meta_handle;
  const ObMacroBlockMeta* macro_meta = NULL;
  bool is_collation_free_valid = false;

  exist_invalid_collation_free_meta_ = false;
  for (int64_t i = 0; OB_SUCC(ret) && !exist_invalid_collation_free_meta_ && i < macro_block_metas_.count(); i++) {
    if (OB_ISNULL(meta_handle = macro_block_metas_.at(i))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Unexpected null macro block handle", K(ret), K(i));
    } else if (OB_ISNULL(macro_meta = meta_handle->get_meta())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret), K(i));
    } else if (OB_FAIL(macro_meta->check_collation_free_valid(is_collation_free_valid))) {
      STORAGE_LOG(WARN, "fail to check collation free is valid", K(ret), K(*macro_meta));
    } else if (!is_collation_free_valid) {
      exist_invalid_collation_free_meta_ = true;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && !exist_invalid_collation_free_meta_ && i < lob_macro_block_metas_.count(); i++) {
    if (OB_ISNULL(meta_handle = lob_macro_block_metas_.at(i))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Unexpected null macro block handle", K(ret), K(i));
    } else if (OB_ISNULL(macro_meta = meta_handle->get_meta())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret), K(i));
    } else if (OB_FAIL(macro_meta->check_collation_free_valid(is_collation_free_valid))) {
      STORAGE_LOG(WARN, "fail to check collation free is valid", K(ret), K(*macro_meta));
    } else if (!is_collation_free_valid) {
      exist_invalid_collation_free_meta_ = true;
    }
  }

  return ret;
}

int ObOldSSTable::get_table_stat(common::ObTableStat& stat)
{
  int ret = OB_SUCCESS;
  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else {
    stat.set_data_version(meta_.data_version_);
    stat.set_row_count(meta_.row_count_);
    stat.set_data_size(meta_.occupy_size_);
    if (0 == meta_.row_count_) {
      stat.set_average_row_size(0);
    } else {
      stat.set_average_row_size(meta_.occupy_size_ / meta_.row_count_);
    }
    stat.set_macro_blocks_num(meta_.macro_block_array_.count());
  }
  return ret;
}

int ObOldSSTable::get_frozen_schema_version(int64_t& schema_version) const
{
  int ret = OB_SUCCESS;
  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else {
    schema_version = meta_.schema_version_;
  }
  return ret;
}

int ObOldSSTable::fill_old_meta_info(
    const int64_t schema_version, const int64_t step_merge_start_version, const int64_t step_merge_end_version)
{
  int ret = OB_SUCCESS;

  if (meta_.sstable_format_version_ != ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("only old version sstable can fill old meta info", K(ret), K(meta_));
  } else if (schema_version < 0 || step_merge_end_version < 0 || step_merge_start_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(schema_version), K(step_merge_end_version), K(step_merge_start_version));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
    LOG_WARN("failed to init file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
    LOG_WARN("failed to init lob file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
    LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
  } else {
    meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
    meta_.max_logic_block_index_ = 0;
    meta_.build_on_snapshot_ = 0;
    meta_.create_index_base_version_ = 0;
    meta_.schema_version_ = schema_version;
    meta_.progressive_merge_start_version_ = step_merge_start_version;
    meta_.progressive_merge_end_version_ = step_merge_end_version;
    meta_.create_snapshot_version_ = 0;
    meta_.bloom_filter_block_id_.reset();
    meta_.bloom_filter_block_id_in_files_ = 0;
  }
  return ret;
}

int ObOldSSTable::get_row_max_trans_version(const ObStoreRowkey& rowkey, int64_t& version)
{
  int ret = OB_SUCCESS;
  version = 0;
  ObStoreRowIterator* row_iter = NULL;
  if (!rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get row max trans version get invalid argument", K(ret), K(rowkey));
  } else if (!is_multi_version_minor_sstable(key_.table_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("major sstable can not get row trans version", K(ret), K(rowkey));
  } else if (key_.is_minor_sstable()) {
    ObArenaAllocator allocator;
    ObTableAccessParam access_param;
    ObTableAccessContext access_context;
    ObStoreCtx ctx;
    ObBlockCacheWorkingSet block_cache_ws;
    ObSEArray<int32_t, common::OB_DEFAULT_COL_DEC_NUM> out_cols_project;
    ObSEArray<ObColDesc, common::OB_DEFAULT_COL_DEC_NUM> column_ids;
    ObColDesc col_desc;
    ObExtStoreRowkey ext_rowkey(rowkey);
    const ObStoreRow* store_row = NULL;
    const int64_t rowkey_cnt = rowkey.get_obj_cnt();
    const int64_t multi_version_rowkey_cnt = rowkey_cnt + 1;

    for (int64_t i = 0; OB_SUCC(ret) && i < multi_version_rowkey_cnt; ++i) {
      col_desc.reset();
      const ObSSTableColumnMeta& column_meta = meta_.column_metas_.at(i);
      col_desc.col_id_ = column_meta.column_id_;
      col_desc.col_order_ = ObOrderType::ASC;
      if (i < rowkey_cnt) {
        col_desc.col_type_ = rowkey.get_obj_ptr()[i].meta_;
      } else if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID != col_desc.col_id_) {
        ret = OB_ERR_SYS;
        LOG_WARN("multi version can not get trans version column id", K(ret), K(meta_), K(col_desc));
      } else {
        const ObMultiVersionExtraRowkey& mv_ext_rowkey = OB_MULTI_VERSION_EXTRA_ROWKEY[0];
        col_desc.col_id_ = mv_ext_rowkey.column_index_;
        (col_desc.col_type_.*mv_ext_rowkey.set_meta_type_func_)();
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("failed to push col desc into column ids", K(ret), K(col_desc));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int32_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        if (OB_FAIL(out_cols_project.push_back(i))) {
          STORAGE_LOG(WARN, "fail to push column index into out cols project", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObQueryFlag query_flag(ObQueryFlag::Forward,  // scan_order
          false,                                    // daily_merge
          false,                                    // optimize
          false,                                    // sys scan
          false,                                    // full_row
          false,                                    // index_back
          false,                                    // query_stat
          ObQueryFlag::MysqlMode,                   // sql_mode
          true                                      // read_latest
      );
      query_flag.set_not_use_row_cache();
      query_flag.set_use_block_cache();
      ObVersionRange trans_version_range;

      if (OB_FAIL(access_param.out_col_desc_param_.init())) {
        STORAGE_LOG(WARN, "fail to init out cols", K(ret));
      } else if (OB_FAIL(access_param.out_col_desc_param_.assign(column_ids))) {
        STORAGE_LOG(WARN, "fail to assign out cols", K(ret));
      } else {
        // init access_param
        access_param.iter_param_.table_id_ = meta_.index_id_;
        access_param.iter_param_.rowkey_cnt_ = rowkey_cnt;
        access_param.iter_param_.schema_version_ = meta_.schema_version_;
        access_param.iter_param_.out_cols_project_ = &out_cols_project;
        access_param.iter_param_.out_cols_ = &access_param.out_col_desc_param_.get_col_descs();

        // init access_context
        trans_version_range.snapshot_version_ = key_.trans_version_range_.snapshot_version_;
        trans_version_range.multi_version_start_ = key_.trans_version_range_.snapshot_version_;
        trans_version_range.base_version_ = 0;

        if (OB_FAIL(block_cache_ws.init(extract_tenant_id(meta_.index_id_)))) {
          STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(extract_tenant_id(meta_.index_id_)));
        } else if (OB_FAIL(access_context.init(
                       query_flag, ctx, allocator, allocator, block_cache_ws, trans_version_range))) {
          STORAGE_LOG(WARN, "failed to init access context", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ext_rowkey.to_collation_free_on_demand_and_cutoff_range(allocator))) {
        LOG_WARN("failed to collation free on demand and cutoff range", K(ret), K(ext_rowkey));
      } else if (OB_FAIL(get(access_param.iter_param_, access_context, ext_rowkey, row_iter))) {
        LOG_WARN("failed to get row iter", K(ret), K(ext_rowkey), K(access_param), K(access_context));
      } else if (OB_ISNULL(row_iter)) {
        ret = OB_ERR_SYS;
        LOG_WARN("row iter should not be NULL", K(ret), KP(row_iter));
      } else if (OB_FAIL(row_iter->get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_ISNULL(store_row) || store_row->row_val_.count_ != multi_version_rowkey_cnt) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to get multi version compacted row", K(ret), KP(store_row));
      } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
        ret = OB_ITER_END;
      } else if (!store_row->row_val_.cells_[rowkey_cnt].meta_.is_int()) {
        ret = OB_ERR_SYS;
        LOG_WARN("get multi version row trans version is invalid",
            K(ret),
            K(*store_row),
            K(out_cols_project),
            K(column_ids),
            K(meta_));
      } else if (OB_FAIL(store_row->row_val_.cells_[rowkey_cnt].get_int(version))) {
        LOG_WARN("failed to get trans version", K(ret), K(*store_row), K(out_cols_project), K(column_ids), K(meta_));
      } else if (version >= 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("store row trans version should not bigger than 0",
            K(ret),
            K(version),
            K(*store_row),
            K(out_cols_project),
            K(column_ids),
            K(meta_));
      } else {
        version = -version;
      }
    }
  }
  return ret;
}

int ObOldSSTable::get_macro_max_row_count(int64_t& count)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle* meta_handle = NULL;
  const ObMacroBlockMeta* macro_meta = NULL;
  count = 0;
  if (meta_.row_count_ > 0 && macro_block_metas_.count() > 1) {
    for (int64_t i = 0; i < macro_block_metas_.count(); ++i) {
      if (OB_ISNULL(meta_handle = macro_block_metas_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected macro meta handle is null", "meta idx", i, K(ret));
      } else if (OB_ISNULL(macro_meta = meta_handle->get_meta())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected macro meta is null", "meta idx", i, K(ret));
      } else if (count < macro_meta->row_count_) {
        count = macro_meta->row_count_;
      }
    }
  }
  return ret;
}

int ObOldSSTable::build_exist_iterator(const ObTableIterParam& iter_param, ObTableAccessContext& access_context,
    const ObExtStoreRowkey& ext_rowkey, ObStoreRowIterator*& iter)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObSSTableRowExister* exister = NULL;
  if (NULL == (buf = access_context.allocator_->alloc(sizeof(ObSSTableRowExister)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    exister = new (buf) ObSSTableRowExister();
    if (OB_FAIL(exister->init(iter_param, access_context, this, &ext_rowkey))) {
      STORAGE_LOG(WARN, "Failed to init sstable row exister", K(ret), K(iter_param), K(access_context), K(ext_rowkey));
    } else {
      iter = exister;
    }
  }

  return ret;
}

int ObOldSSTable::build_multi_exist_iterator(ObRowsInfo& rows_info, ObStoreRowIterator*& iter)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObSSTableRowMultiExister* multi_exister = NULL;
  if (NULL ==
      (buf = rows_info.exist_helper_.table_access_context_.allocator_->alloc(sizeof(ObSSTableRowMultiExister)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    multi_exister = new (buf) ObSSTableRowMultiExister();
    if (OB_FAIL(multi_exister->init(rows_info.exist_helper_.table_iter_param_,
            rows_info.exist_helper_.table_access_context_,
            this,
            &rows_info.ext_rowkeys_))) {
      STORAGE_LOG(WARN, "Failed to init sstable row exister", K(ret), K(rows_info));
    } else {
      iter = multi_exister;
    }
  }

  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
