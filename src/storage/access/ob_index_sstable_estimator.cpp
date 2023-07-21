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

#include "ob_index_sstable_estimator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_index_block_row_scanner.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/tablet/ob_tablet.h"
#include "share/schema/ob_column_schema.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
using namespace blocksstable;
ObPartitionEst::ObPartitionEst()
    : logical_row_count_(0),
      physical_row_count_(0)
{
}

int ObPartitionEst::add(const ObPartitionEst &pe)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ += pe.logical_row_count_;
  physical_row_count_ += pe.physical_row_count_;

  return ret;
}


int ObPartitionEst::deep_copy(const ObPartitionEst &src)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ = src.logical_row_count_;
  physical_row_count_ = src.physical_row_count_;

  return ret;
}

ObIndexBlockScanEstimator::ObIndexBlockScanEstimator(const ObIndexSSTableEstimateContext &context)
  : level_(0), context_(context), allocator_("OB_STORAGE_EST")
{
  tenant_id_ = MTL_ID();
}

ObIndexBlockScanEstimator::~ObIndexBlockScanEstimator()
{
  root_index_block_.reset();
  level_ = 0;
  for (int64_t i = 0; i < DEFAULT_GET_MICRO_DATA_HANDLE_CNT; ++i) {
    micro_handles_[i].reset();
  }
  index_block_data_.reset();
  index_block_row_scanner_.reset();
}

int ObIndexBlockScanEstimator::estimate_row_count(ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;
  ObEstimatedResult result;

  common::ObSEArray<int32_t, 1> agg_projector;
  common::ObSEArray<share::schema::ObColumnSchemaV2, 1> agg_column_schema;
  if (OB_UNLIKELY(!context_.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "estimate context is not valid", K(ret), K(context_));
  } else if (OB_FAIL(index_block_row_scanner_.init(
              agg_projector,
              agg_column_schema,
              context_.tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils(),
              allocator_,
              context_.query_flag_,
              context_.sstable_.get_macro_offset()))) {
    STORAGE_LOG(WARN, "Failed to init index block row scanner", K(ret), K(agg_projector), K(agg_column_schema));
  } else if (OB_FAIL(context_.sstable_.get_index_tree_root(root_index_block_))) {
    STORAGE_LOG(WARN, "Failed to get index tree root", K(ret));
  } else if (OB_FAIL(cal_total_row_count(result))) {
    STORAGE_LOG(WARN, "Failed to get total_row_count_delta", K(ret), K(root_index_block_));
  } else if (result.total_row_count_ > 0) {
    if (context_.range_.is_whole_range()) {
    } else {
      if (!context_.range_.get_start_key().is_min_rowkey()) {
        if (OB_FAIL(estimate_excluded_border_row_count(true, result))) {
          STORAGE_LOG(WARN, "Failed to estimate left excluded row count", K(ret));
        }
      }
      if (OB_SUCC(ret) && !context_.range_.get_end_key().is_max_rowkey()) {
        level_ = 0;
        if (OB_FAIL(estimate_excluded_border_row_count(false, result))) {
          STORAGE_LOG(WARN, "Failed to estimate right excluded row count", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_est.physical_row_count_ = result.total_row_count_ - result.excluded_row_count_;
    if (context_.sstable_.is_multi_version_minor_sstable()) {
      part_est.logical_row_count_ = result.total_row_count_delta_ - result.excluded_row_count_delta_;
    } else {
      part_est.logical_row_count_ = part_est.physical_row_count_;
    }
  }
  STORAGE_LOG(DEBUG, "estimate result", K(ret), K(result), K(part_est));
  return ret;
}

int ObIndexBlockScanEstimator::cal_total_row_count(ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;
  // TODO remove this if we can get row_count_delta from sstable meta directly
  // result.total_row_count_ = context_.sstable_->get_meta().get_row_count();
  blocksstable::ObDatumRange whole_range;
  whole_range.set_whole_range();
  if (OB_FAIL(index_block_row_scanner_.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      root_index_block_,
      whole_range,
      0,
      true,
      true))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "Failed to open whole range", K(ret), K(root_index_block_));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    blocksstable::ObMicroIndexInfo tmp_micro_index_info;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(index_block_row_scanner_.get_next(tmp_micro_index_info))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Failed to get next index row", K(ret), K(index_block_row_scanner_));
        }
      } else {
        result.total_row_count_ += tmp_micro_index_info.get_row_count();
        result.total_row_count_delta_ += tmp_micro_index_info.get_row_count_delta();
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObIndexBlockScanEstimator::estimate_excluded_border_row_count(bool is_left, ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRange excluded_range;
  const blocksstable::ObDatumRange &orig_range = context_.range_;
  if (is_left) {
    excluded_range.start_key_.set_min_rowkey();
    excluded_range.set_end_key(orig_range.get_start_key());
    if (orig_range.get_border_flag().inclusive_start()) {
      excluded_range.set_right_open();
    } else {
      excluded_range.set_right_closed();
    }
  } else {
    excluded_range.set_start_key(orig_range.get_end_key());
    excluded_range.end_key_.set_max_rowkey();
    if (orig_range.get_border_flag().inclusive_end()) {
      excluded_range.set_left_open();
    } else {
      excluded_range.set_left_closed();
    }
  }
  if (excluded_range.is_valid()) {
    index_block_row_scanner_.reuse();
    int64_t index_row_count = 0;
    if (OB_FAIL(index_block_row_scanner_.open(
        ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
        root_index_block_,
        excluded_range,
        0,
        true,
        true))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "Failed to open excluded range", K(ret), K(root_index_block_), K(excluded_range));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      int64_t idx = 0;
      blocksstable::ObMicroIndexInfo tmp_micro_index_info, border_micro_index_info;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(index_block_row_scanner_.get_index_row_count(index_row_count))) {
          STORAGE_LOG(WARN, "Failed to get index row count", K(ret), K(index_block_row_scanner_));
        } else if (index_row_count > 0) {
          idx = 0;
          while (OB_SUCC(ret)) {
            bool is_border = is_left ? index_row_count - 1 == idx : 0 == idx;
            if (OB_FAIL(index_block_row_scanner_.get_next(tmp_micro_index_info))) {
              if (OB_ITER_END != ret) {
                STORAGE_LOG(WARN, "Failed to get next index row", K(ret), K(index_block_row_scanner_));
              }
            } else if (is_border) {
              border_micro_index_info = tmp_micro_index_info;
            } else {
              result.excluded_row_count_ += tmp_micro_index_info.get_row_count();
              if (context_.sstable_.is_multi_version_minor_sstable()) {
                result.excluded_row_count_delta_ += tmp_micro_index_info.get_row_count_delta();
              }
            }
            idx++;
          }

          if (OB_ITER_END == ret && idx > 0) {
            if (OB_FAIL(goto_next_level(excluded_range, border_micro_index_info, result))) {
              if (OB_ITER_END != ret) {
                STORAGE_LOG(WARN, "Failed to go to next level", K(ret),
                    K(border_micro_index_info), K(index_block_row_scanner_));
              }
            }
          }
        } else {
          break;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIndexBlockScanEstimator::goto_next_level(
    const blocksstable::ObDatumRange &range,
    const blocksstable::ObMicroIndexInfo &micro_index_info,
    ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDataHandle &micro_handle = get_read_handle();
  micro_handle.reset();
  ObMacroBlockHandle macro_handle;
  micro_handle.io_handle_ = macro_handle;
  if (OB_FAIL(prefetch_index_block_data(micro_index_info, micro_handle))) {
    STORAGE_LOG(WARN, "Failed to prefetch index block", K(ret), K(micro_index_info));
  } else if (micro_index_info.is_data_block()) {
    ObPartitionEst tmp_part_est;
    int64_t logical_row_count = 0, physical_row_count = 0;
    if (OB_FAIL(estimate_data_block_row_count(
        range,
        micro_handle,
        context_.sstable_.is_multi_version_minor_sstable(),
        tmp_part_est))) {
      STORAGE_LOG(WARN, "Failed to estimate data block row count", K(ret), K(micro_handle));
    } else {
      result.excluded_row_count_ += tmp_part_est.physical_row_count_;
      if (context_.sstable_.is_multi_version_minor_sstable()) {
        result.excluded_row_count_delta_ += tmp_part_est.logical_row_count_;
      }
      ret = OB_ITER_END;
    }
  } else {
    index_block_data_.reset();
    index_block_row_scanner_.reuse();
    if (OB_FAIL(micro_handle.get_index_block_data(index_block_data_))) {
      STORAGE_LOG(WARN, "Failed to get index block data", K(ret), K(micro_handle));
    } else if (OB_FAIL(index_block_row_scanner_.open(
        micro_index_info.get_macro_id(), index_block_data_, range, 0, true, true))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "Failed to open range", K(ret), K(index_block_row_scanner_), K(range));
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObIndexBlockScanEstimator::prefetch_index_block_data(
    const blocksstable::ObMicroIndexInfo &micro_index_info,
    ObMicroBlockDataHandle &micro_handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const MacroBlockId &macro_id = micro_index_info.get_macro_id();
  const int64_t offset = micro_index_info.get_block_offset();
  const int64_t size = micro_index_info.get_block_size();
  ObIMicroBlockCache *cache = nullptr;
  if (micro_index_info.is_data_block()) {
    cache = &blocksstable::ObStorageCacheSuite::get_instance().get_block_cache();
  } else {
    cache = &blocksstable::ObStorageCacheSuite::get_instance().get_index_block_cache();
  }
  if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null block cache", K(ret), KP(cache));
  } else if (OB_FAIL(cache->get_cache_block(
      tenant_id_, macro_id, offset, size, micro_handle.cache_handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get cache block", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    found = true;
    micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
  }
  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(cache->prefetch(tenant_id_, macro_id, micro_index_info, context_.query_flag_, micro_handle.io_handle_))) {
      STORAGE_LOG(WARN, "Failed to prefetch data micro block", K(ret), K(micro_index_info));
    } else if (ObSSTableMicroBlockState::UNKNOWN_STATE == micro_handle.block_state_) {
      micro_handle.tenant_id_ = tenant_id_;
      micro_handle.macro_block_id_ = micro_index_info.get_macro_id();
      micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      micro_handle.micro_info_.offset_ = micro_index_info.get_block_offset();
      micro_handle.micro_info_.size_ = micro_index_info.get_block_size();
      micro_handle.allocator_ = &allocator_;
    }
  }
  return ret;
}

int ObIndexBlockScanEstimator::estimate_data_block_row_count(
    const blocksstable::ObDatumRange &range,
    ObMicroBlockDataHandle &micro_handle,
    bool consider_multi_version,
    ObPartitionEst &est)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMicroBlockData block_data;
  blocksstable::ObMicroBlockRowScanner block_scanner(allocator_);
  if (OB_FAIL(micro_handle.get_data_block_data(macro_reader_, block_data))) {
    STORAGE_LOG(WARN, "Failed to get block data", K(ret), K(micro_handle));
  } else if (OB_FAIL(block_scanner.estimate_row_count(
		      context_.tablet_handle_.get_obj()->get_rowkey_read_info(),
              block_data,
              range,
              consider_multi_version,
              est))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "Failed to estimate row count", K(ret), K(block_data), K(range));
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

}
}
