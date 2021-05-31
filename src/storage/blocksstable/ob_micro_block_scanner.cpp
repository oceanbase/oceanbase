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

#include "ob_micro_block_scanner.h"
#include "storage/ob_i_store.h"
#include "ob_column_map.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {
ObMicroBlockScanner::ObMicroBlockScanner()
    : reader_(),
      current_(ObIMicroBlockReader::INVALID_ROW_INDEX),
      start_(ObIMicroBlockReader::INVALID_ROW_INDEX),
      last_(ObIMicroBlockReader::INVALID_ROW_INDEX),
      reverse_scan_(false),
      is_setuped_(false),
      need_read_meta_(false),
      step_(0)
{}

ObMicroBlockScanner::~ObMicroBlockScanner()
{}

void ObMicroBlockScanner::reset()
{
  if (NULL != reader_) {
    reader_->reset();
  }
  current_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  last_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  reverse_scan_ = false;
  is_setuped_ = false;
  step_ = 0;
}

int ObMicroBlockScanner::get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (!is_setuped_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockScanner should be setuped first, ", K(ret));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_ITER_END != ret && OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "fail to judge end of block or not, ", K(ret));
    }
  } else {
    trans_id_.reset();
    row_.trans_id_ptr_ = &trans_id_;
    row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
    row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
    row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
    if (OB_FAIL(reader_->get_row(current_, row_))) {
      STORAGE_LOG(WARN, "micro block reader fail to get row.", K(ret));
    } else {
      current_ += step_;
      row = &row_;
    }
  }
  return ret;
}

int ObMicroBlockScanner::set_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  switch (store_type) {
    case FLAT_ROW_STORE: {
      reader_ = &flat_reader_;
      break;
    }
    case SPARSE_ROW_STORE: {
      reader_ = &sparse_reader_;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported row store type", K(ret), K(store_type));
  }
  if (OB_SUCC(ret) && reader_->is_inited()) {
    reader_->reset();
  }

  return ret;
}

int ObMicroBlockScanner::set_scan_param(const ObColumnMap& column_map, const ObStoreRange& range,
    const ObMicroBlockData& block_data, const bool is_bound_block, const bool is_reverse_scan,
    const ObRowStoreType store_type, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  UNUSEDx(allocator);
  reset();
  if (!column_map.is_valid() || !range.is_valid() || !block_data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), K(range), K(block_data));
  } else if (OB_FAIL(set_reader(store_type))) {
    LOG_WARN("set reader failed", K(ret), K(store_type));
  } else if (OB_FAIL(reader_->init(block_data, &column_map))) {
    STORAGE_LOG(WARN, "micro block reader fail to init.", K(ret));
  } else if (OB_FAIL(set_base_scan_param(range, is_bound_block, is_reverse_scan))) {
    STORAGE_LOG(WARN, "micro block scanner fail to set param.", K(ret));
  } else {
    is_setuped_ = true;
  }

  return ret;
}

int ObMicroBlockScanner::set_base_scan_param(
    const ObStoreRange& range, const bool is_bound_block, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  int64_t start;
  int64_t last;

  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), K(range));
  } else if (OB_FAIL(locate_range_pos(range, is_bound_block, is_bound_block, start, last))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(DEBUG, "micro block scanner fail to locate start pos.", K(ret));
    }
  } else {
    reverse_scan_ = is_reverse_scan;
    step_ = is_reverse_scan ? -1 : 1;
    if (is_reverse_scan) {
      current_ = last;
      start_ = last;
      last_ = start;
    } else {
      current_ = start;
      start_ = start;
      last_ = last;
    }
  }

  if (OB_BEYOND_THE_RANGE == ret) {
    current_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
    start_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
    last_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObMicroBlockScanner::estimate_row_count(common::ObIAllocator& allocator, const common::ObStoreRange& range,
    const ObColumnMap& column_map, const ObMicroBlockData& block_data, const ObRowStoreType store_type,
    bool consider_multi_version, int64_t& logical_row_count, int64_t& physical_row_count)
{
  int ret = OB_SUCCESS;
  logical_row_count = 0;
  physical_row_count = 0;

  int64_t start = ObIMicroBlockReader::INVALID_ROW_INDEX;
  int64_t last = ObIMicroBlockReader::INVALID_ROW_INDEX;

  if (OB_FAIL(set_reader(store_type))) {
    LOG_WARN("set reader failed", K(ret), K(store_type));
  } else if (OB_FAIL(reader_->init(block_data, &column_map))) {
    STORAGE_LOG(WARN, "micro block reader fail to init.", K(ret));
  } else if (OB_FAIL(locate_range_pos(range, true, true, start, last))) {
    if (OB_BEYOND_THE_RANGE == ret) {
      ret = OB_SUCCESS;
    }
  } else if (ObIMicroBlockReader::INVALID_ROW_INDEX != last && ObIMicroBlockReader::INVALID_ROW_INDEX != start) {
    physical_row_count = last - start + 1;
    if (physical_row_count < 0)
      physical_row_count = 0;
    logical_row_count = physical_row_count;

    if (physical_row_count > 0 && consider_multi_version) {
      if (OB_FAIL(set_scan_param(column_map, range, block_data, true, true, store_type, allocator))) {
        STORAGE_LOG(WARN, "fail to set scan param, ", K(ret));
      } else {
        const ObStoreRow* row;
        logical_row_count = 0;
        while (OB_SUCC(get_next_row(row))) {
          if (row->row_type_flag_.is_first_multi_version_row()) {
            logical_row_count += row->get_delta();
          }
        }
        if (OB_ITER_END == ret || OB_BEYOND_THE_RANGE == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

int ObMicroBlockScanner::locate_range_pos(const ObStoreRange& range, const bool is_left_bound_block,
    const bool is_right_bound_block, int64_t& begin, int64_t& end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader_ is null", K(ret), KP(reader_));
  } else if (OB_FAIL(reader_->locate_range(range, is_left_bound_block, is_right_bound_block, begin, end))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      LOG_WARN("fail to locate range", K(ret));
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
