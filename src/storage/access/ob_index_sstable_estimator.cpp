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
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

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
  : level_(0),
    context_(context),
    allocator_("OB_STORAGE_EST", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID())
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

void ObIndexBlockScanEstimator::reuse()
{
  root_index_block_.reset();
  index_block_row_scanner_.reuse();
  level_ = 0;
  for (int64_t i = 0; i < DEFAULT_GET_MICRO_DATA_HANDLE_CNT; ++i) {
    micro_handles_[i].reset();
  }
  index_block_data_.reset();

}

int ObIndexBlockScanEstimator::init_index_scanner(ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!context_.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init index scanner", K(ret), K(context_));
  } else if (index_block_row_scanner_.is_valid()) {
    // need reuse index block row scanner
    index_block_row_scanner_.switch_context(
            sstable, nullptr, context_.index_read_info_.get_datum_utils(), context_.query_flag_);
  } else if (OB_FAIL(index_block_row_scanner_.init(
              context_.index_read_info_.get_datum_utils(),
              allocator_,
              context_.query_flag_,
              sstable.get_macro_offset()))) {
    STORAGE_LOG(WARN, "Failed to init index block row scanner", K(ret));
  }
  if (FAILEDx(sstable.get_index_tree_root(root_index_block_))) {
    STORAGE_LOG(WARN, "Failed to get index tree root", K(ret));
  } else if (sstable.is_ddl_merge_sstable()) {
    if (OB_ISNULL(context_.tablet_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null tablet handle", K(ret), K(context_));
    } else {
      index_block_row_scanner_.set_iter_param(&sstable, context_.tablet_handle_->get_obj());
    }
  }
  return ret;
}

int ObIndexBlockScanEstimator::estimate_row_count(ObSSTable &sstable,
    const blocksstable::ObDatumRange &datum_range,
    ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;
  ObEstimatedResult result;
  if (OB_FAIL(cal_total_estimate_result(sstable, datum_range, result))) {
    STORAGE_LOG(WARN, "Failed to get total estimate result", K(ret), K(root_index_block_));
  } else {
    part_est.physical_row_count_ = result.total_row_count_ - result.excluded_row_count_;
    if (sstable.is_multi_version_minor_sstable()) {
      part_est.logical_row_count_ = result.total_row_count_delta_ - result.excluded_row_count_delta_;
    } else {
      part_est.logical_row_count_ = part_est.physical_row_count_;
    }
  }
  STORAGE_LOG(DEBUG, "estimate row count result", K(ret), K(result), K(part_est));
  return ret;
}

int ObIndexBlockScanEstimator::estimate_block_count(ObSSTable &sstable,
                                                    const blocksstable::ObDatumRange &datum_range,
                                                    int64_t &macro_block_cnt,
                                                    int64_t &micro_block_cnt)
{
  int ret = OB_SUCCESS;
  ObEstimatedResult result(true /* for block */);
  if (OB_FAIL(cal_total_estimate_result(sstable, datum_range, result))) {
    STORAGE_LOG(WARN, "Failed to get total estimate result", K(ret), K(root_index_block_));
  } else {
    macro_block_cnt = MAX(result.macro_block_cnt_, 1);
    micro_block_cnt = MAX(result.micro_block_cnt_, 1);
    STORAGE_LOG(TRACE, "estimate block count result", K(ret), K(result), K(macro_block_cnt), K(micro_block_cnt));
  }
  return ret;
}

int ObIndexBlockScanEstimator::cal_total_estimate_result(
    ObSSTable &sstable,
    const blocksstable::ObDatumRange &datum_range,
    ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;
  // TODO remove this if we can get row_count_delta from sstable meta directly
  // result.total_row_count_ = context_.sstable_->get_meta().get_row_count();
  if (sstable.is_ddl_merge_sstable() && context_.tablet_handle_ == nullptr) {
    if (OB_FAIL(cal_total_estimate_result_for_ddl(sstable, datum_range, result))) {
      STORAGE_LOG(WARN, "Failed to cal estimate result for ddl merge sstable", K(ret));
    }
  } else if (OB_FAIL(init_index_scanner(sstable))) {
    STORAGE_LOG(WARN, "Failed to init index scanner", K(ret));
  } else {
    ObDatumRange whole_range;
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
      ObMicroIndexInfo tmp_micro_index_info;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(index_block_row_scanner_.get_next(tmp_micro_index_info))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "Failed to get next index row", K(ret), K(index_block_row_scanner_));
          }
        } else {
          result.total_row_count_ += tmp_micro_index_info.get_row_count();
          result.total_row_count_delta_ += tmp_micro_index_info.get_row_count_delta();
          result.macro_block_cnt_ += tmp_micro_index_info.get_macro_block_count();
          result.micro_block_cnt_ += tmp_micro_index_info.get_micro_block_count();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && result.total_row_count_ > 0) {
      if (datum_range.is_whole_range()) {
      } else {
        const bool is_multi_version_minor = sstable.is_multi_version_minor_sstable();
        const bool is_major = sstable.is_major_sstable();
        if (!datum_range.get_start_key().is_min_rowkey()) {
          if (OB_FAIL(estimate_excluded_border_result(
                  is_multi_version_minor, is_major, datum_range, true, result))) {
            STORAGE_LOG(WARN, "Failed to estimate left excluded row count", K(ret));
          }
        }
        if (OB_SUCC(ret) && !datum_range.get_end_key().is_max_rowkey()) {
          level_ = 0;
          if (OB_FAIL(estimate_excluded_border_result(
                  is_multi_version_minor, is_major, datum_range, false, result))) {
            STORAGE_LOG(WARN, "Failed to estimate right excluded row count", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexBlockScanEstimator::cal_total_estimate_result_for_ddl(ObSSTable &sstable,
                                                                 const blocksstable::ObDatumRange &datum_range,
                                                                 ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;

  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    STORAGE_LOG(WARN, "get sstable meta failed", K(ret));
  } else {
    int64_t factor = 1;
    const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
    result.total_row_count_ = basic_meta.row_count_;
    result.total_row_count_delta_ = 0;
    result.macro_block_cnt_ = basic_meta.get_data_macro_block_count();
    result.micro_block_cnt_ = basic_meta.get_data_micro_block_count();
    if (OB_SUCC(ret) && result.total_row_count_ > 0) {
      if (datum_range.is_whole_range()) {
      } else {
        if (!datum_range.get_start_key().is_min_rowkey()) {
          factor = 10;
        }
        if (OB_SUCC(ret) && !datum_range.get_end_key().is_max_rowkey()) {
          factor *= 2;
        }
        result.total_row_count_ = MAX(1, result.total_row_count_ / factor);
        result.macro_block_cnt_ = MAX(1, result.macro_block_cnt_ / factor);
        result.micro_block_cnt_ = MAX(1, result.micro_block_cnt_ / factor);
      }
    }
  }

  return ret;
}

int ObIndexBlockScanEstimator::estimate_excluded_border_result(const bool is_multi_version_minor,
                                                               const bool is_major,
                                                               const blocksstable::ObDatumRange &datum_range,
                                                               bool is_left,
                                                               ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRange excluded_range;
  if (is_left) {
    excluded_range.start_key_.set_min_rowkey();
    excluded_range.set_end_key(datum_range.get_start_key());
    if (datum_range.get_border_flag().inclusive_start()) {
      excluded_range.set_right_open();
    } else {
      excluded_range.set_right_closed();
    }
  } else {
    excluded_range.set_start_key(datum_range.get_end_key());
    excluded_range.end_key_.set_max_rowkey();
    if (datum_range.get_border_flag().inclusive_end()) {
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
              result.macro_block_cnt_ -= tmp_micro_index_info.get_macro_block_count();
              result.micro_block_cnt_ -= tmp_micro_index_info.get_micro_block_count();
              if (is_multi_version_minor) {
                result.excluded_row_count_delta_ += tmp_micro_index_info.get_row_count_delta();
              }
            }
            idx++;
          }

          if (OB_ITER_END == ret && idx > 0) {
            int64_t ratio = 0;
            if (0 == border_micro_index_info.get_row_count()) {
              ret = common::OB_INVALID_ARGUMENT;
              STORAGE_LOG(WARN, "Border micro index row count should not be 0", K(ret));
            } else if (((is_left && border_micro_index_info.is_data_block()) || !is_left) && is_major) {
              ratio = (result.total_row_count_ - result.excluded_row_count_) / border_micro_index_info.get_row_count();
            }
            if (OB_ITER_END == ret && ratio < RANGE_ROWS_IN_AND_BORDER_RATIO_THRESHOLD) {
              if (OB_FAIL(goto_next_level(excluded_range, border_micro_index_info, is_multi_version_minor, result))) {
                if (OB_ITER_END != ret) {
                  STORAGE_LOG(WARN, "Failed to go to next level", K(ret),
                      K(border_micro_index_info), K(index_block_row_scanner_));
                }
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
    const bool is_multi_version_minor,
    ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDataHandle &micro_handle = get_read_handle();
  micro_handle.reset();
  if (OB_FAIL(prefetch_index_block_data(micro_index_info, micro_handle))) {
    STORAGE_LOG(WARN, "Failed to prefetch index block", K(ret), K(micro_index_info));
  } else if (micro_index_info.is_data_block()) {
    if (result.only_block_) {
      ret = OB_ITER_END;
    } else {
      ObPartitionEst tmp_part_est;
      int64_t logical_row_count = 0, physical_row_count = 0;
      if (OB_FAIL(estimate_data_block_row_count(
              range,
              micro_handle,
              is_multi_version_minor,
              tmp_part_est))) {
        STORAGE_LOG(WARN, "Failed to estimate data block row count", K(ret), K(micro_handle));
      } else {
        result.excluded_row_count_ += tmp_part_est.physical_row_count_;
        if (is_multi_version_minor) {
          result.excluded_row_count_delta_ += tmp_part_est.logical_row_count_;
        }
        ret = OB_ITER_END;
      }
    }
  } else {
    index_block_data_.reset();
    index_block_row_scanner_.reuse();
    if (OB_FAIL(micro_handle.get_micro_block_data(nullptr, index_block_data_, false))) {
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
  micro_handle.allocator_ = &allocator_;
  ObMicroBlockCacheKey key(tenant_id_, micro_index_info);

  ObIMicroBlockCache *cache = nullptr;
  if (micro_index_info.is_data_block()) {
    cache = &blocksstable::ObStorageCacheSuite::get_instance().get_block_cache();
  } else {
    cache = &blocksstable::ObStorageCacheSuite::get_instance().get_index_block_cache();
  }
  if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null block cache", K(ret), KP(cache));
  } else if (OB_FAIL(cache->get_cache_block(key, micro_handle.cache_handle_))) {
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
    if (OB_FAIL(micro_index_info.row_header_->fill_micro_des_meta(true /* deep_copy_key */, micro_handle.des_meta_))) {
      STORAGE_LOG(WARN, "Failed to fill micro block deserialize meta", K(ret));
    } else if (OB_FAIL(cache->prefetch(tenant_id_, macro_id, micro_index_info,
            context_.query_flag_.is_use_block_cache(), micro_handle.io_handle_, &allocator_))) {
      STORAGE_LOG(WARN, "Failed to prefetch data micro block", K(ret), K(micro_index_info));
    } else if (ObSSTableMicroBlockState::UNKNOWN_STATE == micro_handle.block_state_) {
      micro_handle.tenant_id_ = tenant_id_;
      micro_handle.macro_block_id_ = micro_index_info.get_macro_id();
      micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      micro_handle.micro_info_.set(micro_index_info.get_block_offset(),
                                   micro_index_info.get_block_size(),
                                   micro_index_info.get_logic_micro_id(),
                                   micro_index_info.get_data_checksum());
    }
  }
  STORAGE_LOG(DEBUG, "get cache block", K(ret), K(key), K(macro_id), K(micro_index_info));
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
  if (OB_FAIL(micro_handle.get_micro_block_data(&macro_reader_, block_data))) {
    STORAGE_LOG(WARN, "Failed to get block data", K(ret), K(micro_handle));
  } else if (OB_FAIL(block_scanner.estimate_row_count(
              context_.index_read_info_,
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
