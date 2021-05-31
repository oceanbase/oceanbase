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

#include "ob_sstable_estimator.h"
#include "storage/blocksstable/ob_micro_block_scanner.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
namespace storage {

ObSSTableEstimateContext::ObSSTableEstimateContext() : sstable_(NULL), rowkeys_(NULL)
{}

ObSSTableEstimateContext::~ObSSTableEstimateContext()
{}

void ObSSTableEstimateContext::reset()
{
  sstable_ = NULL;
  rowkeys_ = NULL;
  macro_blocks_.reset();
  cache_context_.reset();
  multi_version_range_.reset();
}

bool ObSSTableEstimateContext::is_valid() const
{
  return NULL != sstable_ && NULL != rowkeys_ && cache_context_.is_valid();
}

ObStoreRowMultiGetEstimator::ObStoreRowMultiGetEstimator() : ObISSTableEstimator()
{}

ObStoreRowMultiGetEstimator::~ObStoreRowMultiGetEstimator()
{}

int ObStoreRowMultiGetEstimator::set_context(ObSSTableEstimateContext& context)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObISSTableEstimator::set_context(context))) {
    STORAGE_LOG(WARN, "failed to set context", K(ret));
  } else if (OB_FAIL(context_.sstable_->find_macros(*context_.rowkeys_, context_.macro_blocks_))) {
    STORAGE_LOG(WARN, "failed to find macro blocks", K(ret));
  }
  return ret;
}

int ObStoreRowMultiGetEstimator::estimate_row_count(ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_contain = true;
  ObRowValueHandle handle;
  ObStorageFileHandle file_handle;
  ObStorageFile* file = nullptr;
  if (OB_FAIL(file_handle.assign(context_.sstable_->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(context_.sstable_->get_storage_file_handle()));
  } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < context_.rowkeys_->count(); ++i) {
      const ObStoreRowkey& rowkey = context_.rowkeys_->at(i).get_store_rowkey();
      const ObMacroBlockCtx& block_ctx = context_.macro_blocks_.at(i);
      const MacroBlockId& block_id = block_ctx.get_macro_block_id();
      if (block_id.is_valid()) {
        if (OB_SUCCESS != (tmp_ret = context_.cache_context_.bf_cache_->may_contain(
                               context_.sstable_->get_table_id(), block_id, file->get_file_id(), rowkey, is_contain))) {
          if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            STORAGE_LOG(WARN, "failed to check may contain", K(tmp_ret), K_(context), K(block_ctx), K(rowkey));
          }
        }
        if (is_contain || (OB_SUCCESS != ret)) {
          part_est.logical_row_count_++;
        }
      }
    }
  }
  return ret;
}

int ObSSTableScanEstimator::alloc_estimator(const ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (sstable.is_multi_version_minor_sstable()) {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultiVersionSingleScanEstimator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      estimator_ = new (buf) ObMultiVersionSingleScanEstimator(allocator_);
    }
  } else {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStoreRowSingleScanEstimator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc ObStoreRowSingleScanEstimator", K(ret));
    } else {
      estimator_ = new (buf) ObStoreRowSingleScanEstimator();
    }
  }
  return ret;
}

ObStoreRowSingleScanEstimator::ObStoreRowSingleScanEstimator()
    : ObISSTableEstimator(), is_empty_scan_(false), is_first_scan_(true)
{}

ObStoreRowSingleScanEstimator::~ObStoreRowSingleScanEstimator()
{}

int ObStoreRowSingleScanEstimator::set_context(ObSSTableEstimateContext& context)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObISSTableEstimator::set_context(context))) {
    STORAGE_LOG(WARN, "failed to set context", K(ret));
  } else if (OB_FAIL(context_.sstable_->find_macros(*context_.range_, context_.macro_blocks_))) {
    STORAGE_LOG(WARN, "fail to find macros", K(ret));
  }
  return ret;
}

int ObStoreRowSingleScanEstimator::open()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (1 == context_.macro_blocks_.count()) {
    // check bloom filter to identify if the scan is empty
    const MacroBlockId& macro_block_id = context_.macro_blocks_.at(0).get_macro_block_id();
    ObStoreRowkey rowkey;

    ObStorageFileHandle file_handle;
    ObStorageFile* file = nullptr;
    if (OB_FAIL(file_handle.assign(context_.sstable_->get_storage_file_handle()))) {
      STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(context_.sstable_->get_storage_file_handle()));
    } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle));
    } else if (OB_FAIL(get_common_rowkey(context_.range_->get_range(), rowkey))) {
      STORAGE_LOG(WARN, "failed to get common rowkey", K(ret), K(context_.range_));
    } else if (rowkey.get_obj_cnt() > 0) {
      // allow check contain fail, should not overwrite ret
      bool is_contain = false;
      if (OB_SUCCESS !=
          (tmp_ret = context_.cache_context_.bf_cache_->may_contain(
               context_.sstable_->get_table_id(), macro_block_id, file->get_file_id(), rowkey, is_contain))) {
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {
          STORAGE_LOG(WARN, "failed to check may contain", K(tmp_ret), K_(context), K(macro_block_id), K(rowkey));
        }
      } else if (!is_contain) {
        is_empty_scan_ = true;
      }
    }
  } else if (0 == context_.macro_blocks_.count()) {
    is_empty_scan_ = true;
  }

  return ret;
}

void ObStoreRowSingleScanEstimator::reset()
{
  ObISSTableEstimator::reset();
  is_empty_scan_ = false;
  is_first_scan_ = true;
}

int ObStoreRowSingleScanEstimator::estimate_row_count(ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(open())) {
    STORAGE_LOG(WARN, "failed to open single scan estimator", K(ret));
  } else {
    // do calculate cost metrics by macro block scan.
    int64_t total_macro_block_count = context_.macro_blocks_.count();

    if (context_.range_->get_range().is_whole_range()) {
      part_est.logical_row_count_ += context_.sstable_->get_meta().row_count_;
    } else if (!is_empty_scan_) {
      MacroBlockId macro_block_id;

      for (int64_t i = 0; OB_SUCC(ret) && i < total_macro_block_count; ++i) {
        const bool is_start_block = (0 == i);
        const bool is_last_block = (total_macro_block_count - 1 == i);
        const ObMacroBlockCtx& macro_block_ctx = context_.macro_blocks_.at(i);
        if (OB_FAIL(estimate_macro_row_count(macro_block_ctx, is_start_block, is_last_block, part_est))) {
          STORAGE_LOG(WARN,
              "cannot estimate cost of macro block.",
              K(ret),
              K(macro_block_ctx),
              K(i),
              K(total_macro_block_count));
        }
      }
    }
    part_est.physical_row_count_ = part_est.logical_row_count_;
  }
  return ret;
}

int ObStoreRowSingleScanEstimator::estimate_macro_row_count(const ObMacroBlockCtx& macro_block_ctx,
    const bool is_start_block, const bool is_last_block, ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;

  if (!macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(macro_block_ctx));
  } else {
    ObFullMacroBlockMeta meta;
    ObArray<ObMicroBlockInfo> micro_infos;

    if (OB_FAIL(context_.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), meta))) {
      STORAGE_LOG(WARN, "Fail to get macro meta, ", K(ret), K(macro_block_ctx));
    } else if (!meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The macro block meta is NULL, ", K(ret), K(macro_block_ctx));
    } else if ((!is_start_block) && (!is_last_block)) {
      // in the middle of scan ranges;
      part_est.logical_row_count_ += meta.meta_->row_count_;
    } else if (OB_FAIL(context_.cache_context_.block_index_cache_->get_micro_infos(context_.sstable_->get_table_id(),
                   macro_block_ctx,
                   context_.range_->get_range(),
                   is_start_block,
                   is_last_block,
                   micro_infos))) {
      if (OB_BEYOND_THE_RANGE == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to get micro infos", K(ret), K_(context), K(macro_block_ctx));
      }
    } else {
      // FIXME:
      // 1. io_micro_block_ costs need multiply block cache hit percent.
      // 2. row costs are not precise, need check in micro block.
      int64_t average_row_count = 0;
      int64_t average_row_size = 0;
      int64_t micro_block_count = micro_infos.count();
      if (0 != meta.meta_->micro_block_count_) {
        average_row_count = meta.meta_->row_count_ / meta.meta_->micro_block_count_;
      }
      if (0 != meta.meta_->row_count_) {
        average_row_size =
            (meta.meta_->micro_block_index_offset_ - meta.meta_->micro_block_data_offset_) / meta.meta_->row_count_;
      }
      if (OB_SUCCESS == ret && is_start_block && micro_block_count > 0) {
        if (OB_FAIL(estimate_border_cost(micro_infos,
                macro_block_ctx,
                true /*check_start*/,
                average_row_count,
                average_row_size,
                micro_block_count,
                part_est))) {
          STORAGE_LOG(WARN, "failed to estimate border cost", K(ret));
        }
      }

      if (OB_SUCCESS == ret && is_last_block && micro_block_count > 0) {
        if (OB_FAIL(estimate_border_cost(micro_infos,
                macro_block_ctx,
                false /*check_start*/,
                average_row_count,
                average_row_size,
                micro_block_count,
                part_est))) {
          STORAGE_LOG(WARN, "failed to estimate border cost", K(ret));
        }
      }

      if (OB_SUCCESS == ret && micro_block_count > 0) {
        int64_t other_row_count = average_row_count * micro_block_count;
        part_est.logical_row_count_ += other_row_count;
      }
    }
  }
  return ret;
}

int ObStoreRowSingleScanEstimator::estimate_border_cost(
    const common::ObIArray<blocksstable::ObMicroBlockInfo>& micro_infos, const ObMacroBlockCtx& macro_block_ctx,
    const bool check_start, const int64_t average_row_count, const int64_t average_row_size, int64_t& micro_block_count,
    ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  int64_t result_row_count = 0;
  int64_t first = check_start ? 0 : micro_infos.count() - 1;

  if (first < 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid first", K(ret), K(first), K(micro_infos.count()), K(check_start));
  } else if (!macro_block_ctx.is_valid() || average_row_count < 0 || average_row_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(macro_block_ctx), K(average_row_count), K(average_row_size));
  } else if (micro_block_count > 0) {
    if (OB_FAIL(estimate_border_count_(
            micro_infos.at(first), macro_block_ctx, average_row_size, result_row_count, micro_block_count, part_est))) {
      STORAGE_LOG(WARN, "fail to estimate_border_block_row_count, ", K(ret));
    } else if (micro_block_count > 0 && 0 == result_row_count) {
      int64_t second = check_start ? 1 : micro_infos.count() - 2;
      if (second < 0) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "invalid second , ", K(ret), K(second), K(check_start), K(micro_infos.count()));
      } else if (OB_FAIL(estimate_border_count_(micro_infos.at(second),
                     macro_block_ctx,
                     average_row_size,
                     result_row_count,
                     micro_block_count,
                     part_est))) {
        STORAGE_LOG(WARN, "fail to estimate_border_block_row_count, ", K(ret));
      }
    }
  }
  return ret;
}

int ObStoreRowSingleScanEstimator::estimate_border_count_(const ObMicroBlockInfo& micro_info,
    const blocksstable::ObMacroBlockCtx& macro_block_ctx, const int64_t average_row_size, int64_t& result_row_count,
    int64_t& micro_block_count, ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  int64_t logical_row_count = 0;

  if (!micro_info.is_valid() || !macro_block_ctx.is_valid() || average_row_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args, ", K(ret), K(micro_info), K(macro_block_ctx), K(average_row_size));
  } else if (OB_FAIL(
                 estimate_border_row_count(micro_info, macro_block_ctx, false, logical_row_count, result_row_count))) {
    STORAGE_LOG(WARN, "failed to estimate_border_row_count, ", K(ret));
  } else if (result_row_count < 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid result_row_count, ", K(ret), K(result_row_count));
  } else {
    --micro_block_count;
    part_est.logical_row_count_ += logical_row_count;
  }

  return ret;
}

int ObISSTableEstimator::estimate_border_row_count(const ObMicroBlockInfo& micro_info,
    const blocksstable::ObMacroBlockCtx& macro_block_ctx, bool consider_multi_version, int64_t& logical_row_count,
    int64_t& physical_row_count)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta meta;
  ObBlockCacheWorkingSet* block_cache_ws = context_.cache_context_.block_cache_ws_;
  ObMicroBlockBufferHandle micro_handle;
  ObMicroBlockData block_data;
  ObMacroBlockReader reader;
  ObMacroBlockHandle macro_handle;
  ObArenaAllocator allocator;
  ObStorageFileHandle file_handle;
  ObStorageFile* file = nullptr;
  logical_row_count = 0;
  physical_row_count = 0;

  if (!micro_info.is_valid() || !macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(micro_info), K(macro_block_ctx), K(physical_row_count));
  } else if (NULL == block_cache_ws) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "block cache ws must not null", K(ret));
  } else if (OB_FAIL(file_handle.assign(context_.sstable_->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(context_.sstable_->get_storage_file_handle()));
  } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle));
  } else if (OB_FAIL(context_.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), meta))) {
    STORAGE_LOG(WARN, "Fail to get macro meta, ", K(ret));
  } else if (!meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect error, meta is NULL.", K(ret), K(meta), KP(block_cache_ws), K(macro_block_ctx));
  } else if (OB_FAIL(block_cache_ws->get_cache_block(context_.sstable_->get_table_id(),
                 macro_block_ctx.get_macro_block_id(),
                 file->get_file_id(),
                 micro_info.offset_,
                 micro_info.size_,
                 micro_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get cache block", K(ret), K_(context), K(macro_block_ctx));
    } else {
      // we cannot always get micro block from block cache
      // now we still need to load data from disk and overwrite the return code
      bool is_compressed = false;
      ObMacroBlockReadInfo read_info;
      read_info.macro_block_ctx_ = &macro_block_ctx;
      read_info.offset_ = micro_info.offset_;
      read_info.size_ = micro_info.size_;
      read_info.io_desc_.category_ = USER_IO;
      read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
      macro_handle.set_file(file);
      if (OB_FAIL(file->read_block(read_info, macro_handle))) {
        STORAGE_LOG(WARN, "Fail to read micro block from io, ", K(ret));
      } else if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(
                     macro_handle.get_buffer(), micro_info.size_, MICRO_BLOCK_HEADER_MAGIC))) {
        STORAGE_LOG(WARN, "micro block data is corrupted.", K(ret));
      } else if (OB_FAIL(reader.decompress_data(meta,
                     macro_handle.get_buffer(),
                     micro_info.size_,
                     block_data.get_buf(),
                     block_data.get_buf_size(),
                     is_compressed))) {
        STORAGE_LOG(WARN, "Fail to decompress data, ", K(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        ObMicroBlockCacheKey tmp_key(context_.sstable_->get_table_id(),
            macro_block_ctx.get_macro_block_id(),
            file->get_file_id(),
            micro_info.offset_,
            micro_info.size_);
        ObMicroBlockCacheValue tmp_value(block_data.get_buf(), block_data.get_buf_size());
        if (OB_SUCCESS != (tmp_ret = block_cache_ws->put(tmp_key, tmp_value))) {
          STORAGE_LOG(WARN, "Fail to put micro block to block cache, ", K(tmp_ret));
        }
        STORAGE_LOG(DEBUG, "Success to decompress micro block, ", K(macro_block_ctx));
      }
    }
  } else if (!micro_handle.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid micro_handle", K(ret), K(micro_handle));
  } else {
    block_data = *micro_handle.get_block_data();
  }

  if (OB_SUCC(ret)) {
    ObMicroBlockScanner* block_scanner = NULL;
    ObColumnMap* column_map = NULL;
    common::ObArray<share::schema::ObColDesc> columns;

    if (NULL == (column_map = OB_NEW(ObColumnMap, ObModIds::OB_CS_SSTABLE_READER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "allocate memory for column map failed.", K(ret));
    } else if (NULL == (block_scanner = OB_NEW(ObMicroBlockScanner, ObModIds::OB_CS_SSTABLE_READER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "allocate memory for macro block scanner failed.", K(ret));
    } else if (OB_FAIL(columns.reserve(meta.meta_->rowkey_column_number_))) {
      STORAGE_LOG(WARN, "reserve memory for rowkey columns desc failed.", K(ret));
    } else if (OB_FAIL(get_rowkey_column_desc(meta, columns))) {
      STORAGE_LOG(WARN, "Fail to get_rowkey_column_desc ", K(ret));
    } else if (OB_FAIL(column_map->init(allocator,
                   meta.meta_->schema_version_,
                   meta.meta_->rowkey_column_number_,
                   meta.meta_->column_number_,
                   columns))) {
      STORAGE_LOG(WARN, "Fail to init column map, ", K(ret));
    } else if (OB_FAIL(block_scanner->estimate_row_count(allocator,
                   context_.range_->get_range(),
                   *column_map,
                   block_data,
                   static_cast<ObRowStoreType>(meta.meta_->row_store_type_),
                   consider_multi_version,
                   logical_row_count,
                   physical_row_count))) {
      STORAGE_LOG(WARN, "Fail to set scan param for micro block scanner, ", K(ret));
    }
    if (NULL != column_map) {
      OB_DELETE(ObColumnMap, ObModIds::OB_CS_SSTABLE_READER, column_map);
      column_map = NULL;
    }
    if (NULL != block_scanner) {
      OB_DELETE(ObMicroBlockScanner, ObModIds::OB_CS_SSTABLE_READER, block_scanner);
      block_scanner = NULL;
    }
  }

  return ret;
}

int ObISSTableEstimator::get_rowkey_column_desc(
    const blocksstable::ObFullMacroBlockMeta& meta, common::ObIArray<share::schema::ObColDesc>& columns)
{
  // static func, no need to check inner stat
  int ret = OB_SUCCESS;
  share::schema::ObColDesc desc;

  if (!meta.is_valid() || columns.count() != 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(meta), K(columns));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta.meta_->rowkey_column_number_; ++i) {
    desc.col_id_ = meta.schema_->column_id_array_[i];
    desc.col_type_ = meta.schema_->column_type_array_[i];
    desc.col_order_ = meta.schema_->column_order_array_[i];
    if (OB_FAIL(columns.push_back(desc))) {
      STORAGE_LOG(WARN, "push column desc failed.", K(ret), K(desc));
    }
  }
  return ret;
}

int ObStoreRowSingleScanEstimator::get_common_rowkey(const ObStoreRange& range, ObStoreRowkey& rowkey)
{
  // static func, no need to check inner stat
  int ret = OB_SUCCESS;
  int64_t prefix_len = 0;

  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(range));
  } else if (OB_FAIL(ObStoreRowkey::get_common_prefix_length(range.get_start_key(), range.get_end_key(), prefix_len))) {
    STORAGE_LOG(WARN, "failed to get common prefix length", K(ret), K(range));
  } else {
    rowkey.reset();
    rowkey.assign(const_cast<ObObj*>(range.get_start_key().get_obj_ptr()), prefix_len);
  }

  return ret;
}

int ObMultiVersionSingleScanEstimator::set_context(ObSSTableEstimateContext& context)
{
  int ret = OB_SUCCESS;
  ObVersionRange version_range;
  version_range.snapshot_version_ = ObVersionRange::MAX_VERSION;
  version_range.base_version_ = ObVersionRange::MAX_VERSION;
  version_range.multi_version_start_ = ObVersionRange::MAX_VERSION;
  if (OB_FAIL(ObISSTableEstimator::set_context(context))) {
    STORAGE_LOG(WARN, "failed to set context", K(ret));
  } else if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
                 *context.range_, version_range, allocator_, context_.multi_version_range_))) {
    STORAGE_LOG(WARN, "failed to convert to multi version range", K(ret));
  } else if (OB_FAIL(context_.sstable_->find_macros(context_.multi_version_range_, context_.macro_blocks_))) {
    STORAGE_LOG(WARN, "failed to find macros", K(ret), K_(context_.multi_version_range));
  }
  return ret;
}

int ObMultiVersionSingleScanEstimator::estimate_row_count(ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  int64_t total_macro_block_count = context_.macro_blocks_.count();

  MacroBlockId macro_block_id;
  bool is_left_border = false;
  bool is_right_border = false;
  int64_t gap_size = 0;
  int64_t purged_phy_row_count = 0;
  const bool is_whole_range = context_.range_->get_range().is_whole_range();

  for (int64_t i = 0; OB_SUCC(ret) && i < total_macro_block_count; ++i) {
    const ObMacroBlockCtx& macro_block_ctx = context_.macro_blocks_.at(i);
    is_left_border = is_whole_range ? false : 0 == i;
    is_right_border = is_whole_range ? false : total_macro_block_count - 1 == i;
    if (OB_FAIL(estimate_macro_row_count(
            macro_block_ctx, is_left_border, is_right_border, part_est, gap_size, purged_phy_row_count))) {
      STORAGE_LOG(WARN,
          "cannot estimate cost of macro block.",
          K(ret),
          K(macro_block_id),
          K(is_left_border),
          K(is_right_border),
          K(i),
          K(total_macro_block_count));
    }
  }
  return ret;
}

int ObMultiVersionSingleScanEstimator::estimate_macro_row_count(const blocksstable::ObMacroBlockCtx& macro_block_ctx,
    const bool is_left_border, const bool is_right_border, ObPartitionEst& part_est, int64_t& gap_size,
    int64_t& purged_phy_row_count)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta meta;

  if (!macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "macro block id is invalid", K(ret));
  } else if (OB_FAIL(context_.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), meta))) {
    STORAGE_LOG(WARN, "failed to get macro block meta", K(ret), K(macro_block_ctx));
  } else if (!meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro block meta is null", K(ret), K(macro_block_ctx));
  } else {
    const int64_t delta = meta.meta_->row_count_delta_;
    // if delta < 0, it means this macro block really delete rows already exist in older table
    // so we should minus gap_size * 2 from physical row count since rows from older table can
    // be skipped
    const int64_t gap_ratio = 1 + (delta < 0);
    int64_t macro_purged_row_count = 0;
    bool need_reset_gap = false;
    if (!(is_left_border || is_right_border)) {
      part_est.logical_row_count_ += meta.meta_->row_count_delta_;
      part_est.physical_row_count_ += meta.meta_->row_count_;
      if (meta.meta_->macro_block_deletion_flag_) {
        gap_size += meta.meta_->row_count_;
        purged_phy_row_count += meta.meta_->row_count_ * gap_ratio;
      } else {
        need_reset_gap = true;
        if (delta <= 0) {
          if (OB_FAIL(context_.cache_context_.block_index_cache_->cal_macro_purged_row_count(
                  context_.sstable_->get_table_id(), macro_block_ctx, macro_purged_row_count))) {
            STORAGE_LOG(WARN, "failed to cal_macro_phy_row_count", K(ret), K(macro_block_ctx));
          } else {
            part_est.physical_row_count_ -= macro_purged_row_count;
          }
        }
      }
    } else {
      need_reset_gap = true;
      bool need_check_micro_block = false;
      ObSEArray<ObMicroBlockInfo, 1> micro_infos;
      if (OB_FAIL(context_.cache_context_.block_index_cache_->cal_border_row_count(context_.sstable_->get_table_id(),
              macro_block_ctx,
              context_.multi_version_range_.get_range(),
              is_left_border,
              is_right_border,
              part_est.logical_row_count_,
              part_est.physical_row_count_,
              need_check_micro_block))) {
        STORAGE_LOG(WARN, "failed to get row count delta", K_(context), K(macro_block_ctx), K(ret));
      } else if (need_check_micro_block) {
        // Fix problem in issue #32813975 (the estimation is imprecise after minor freeze)
        // open the specific micro block to get the row count and take multiple versions of row keys
        // into consideration.
        int64_t logical_row_count = 0, physical_row_count = 0;
        if (OB_FAIL(context_.cache_context_.block_index_cache_->get_micro_infos(context_.sstable_->get_table_id(),
                macro_block_ctx,
                context_.range_->get_range(),
                is_left_border,
                is_right_border,
                micro_infos))) {
          if (OB_BEYOND_THE_RANGE == ret) {
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "failed to get micro infos", K(ret), K_(context), K(macro_block_ctx));
          }
        } else if (1 != micro_infos.count()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected error, should only 1 micro block, ", K(ret));
        } else if (OB_FAIL(estimate_border_row_count(
                       micro_infos.at(0), macro_block_ctx, true, logical_row_count, physical_row_count))) {
          STORAGE_LOG(WARN, "failed to estimate_border_row_count for multi version, ", K(ret));
        } else {
          part_est.logical_row_count_ = logical_row_count;
          part_est.physical_row_count_ = physical_row_count;
        }
      }
    }
    if (OB_SUCC(ret) && need_reset_gap) {
      if (gap_size >= OB_SKIP_RANGE_LIMIT) {
        part_est.physical_row_count_ -= purged_phy_row_count;
      }
      gap_size = 0;
      purged_phy_row_count = 0;
    }
    STORAGE_LOG(DEBUG,
        "estimate macro row count",
        K(part_est),
        K(delta),
        K(macro_block_ctx),
        K(gap_size),
        K(purged_phy_row_count));
  }
  return ret;
}

ObStoreRowMultiScanEstimator::ObStoreRowMultiScanEstimator() : ObISSTableEstimator()
{}

ObStoreRowMultiScanEstimator::~ObStoreRowMultiScanEstimator()
{}

int ObStoreRowMultiScanEstimator::estimate_row_count(ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag;
  ObArray<ObExtStoreRowkey> rowkeys;
  ObPartitionEst tmp_cost;
  for (int64_t i = 0; OB_SUCC(ret) && i < context_.ranges_->count(); ++i) {
    const ObExtStoreRange& range = context_.ranges_->at(i);
    if (OB_FAIL(context_.sstable_->estimate_scan_row_count(
            query_flag, context_.sstable_->get_table_id(), range, tmp_cost))) {
      STORAGE_LOG(WARN, "fail to estimate scan cost of sstable", K(ret));
    } else if (OB_FAIL(part_est.add(tmp_cost))) {
      STORAGE_LOG(WARN, "failed to add cost metrics", K(ret));
    }
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
