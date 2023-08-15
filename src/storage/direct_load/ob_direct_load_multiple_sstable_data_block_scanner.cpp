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

#include "storage/direct_load/ob_direct_load_multiple_sstable_data_block_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_index_entry_compare.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObDirectLoadMultipleSSTableDataBlockScanner::ObDirectLoadMultipleSSTableDataBlockScanner()
  : sstable_(nullptr),
    range_(nullptr),
    datum_utils_(nullptr),
    left_data_block_offset_(0),
    right_data_block_offset_(0),
    is_inited_(false)
{
}

ObDirectLoadMultipleSSTableDataBlockScanner::~ObDirectLoadMultipleSSTableDataBlockScanner()
{
}

int ObDirectLoadMultipleSSTableDataBlockScanner::init(
  ObDirectLoadMultipleSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadMultipleDatumRange &range, const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableDataBlockScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() ||
                         !table_data_desc.is_valid() || !range.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable), K(table_data_desc), K(range), KP(datum_utils));
  } else {
    sstable_ = sstable;
    range_ = &range;
    datum_utils_ = datum_utils;
    IndexBlockReader index_block_reader;
    DataBlockReader data_block_reader;
    if (OB_FAIL(index_block_reader.init(table_data_desc.sstable_index_block_size_,
                                        table_data_desc.compressor_type_))) {
      LOG_WARN("fail to index block reader", KR(ret));
    } else if (OB_FAIL(data_block_reader.init(table_data_desc.sstable_data_block_size_,
                                              sstable->get_meta().max_data_block_size_,
                                              table_data_desc.compressor_type_))) {
      LOG_WARN("fail to data block reader", KR(ret));
    } else if (OB_FAIL(locate_left_border(index_block_reader, data_block_reader))) {
      LOG_WARN("fail to locate left border", KR(ret));
    } else if (OB_FAIL(locate_right_border(index_block_reader, data_block_reader))) {
      LOG_WARN("fail to locate right border", KR(ret));
    } else if (OB_UNLIKELY(left_index_entry_iter_ > right_index_entry_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected locate result", KR(ret), K(left_index_entry_iter_),
               K(right_index_entry_iter_));
    } else {
      current_index_entry_iter_ = left_index_entry_iter_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableDataBlockScanner::locate_left_border(
  IndexBlockReader &index_block_reader, DataBlockReader &data_block_reader)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (range_->start_key_.is_min_rowkey()) {
    left_index_entry_iter_ = sstable_->index_entry_begin();
    left_data_block_offset_ = 0;
  } else if (OB_FAIL(
               range_->start_key_.compare(sstable_->get_start_key(), *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  } else if (cmp_ret < 0 ||
             (cmp_ret == 0 && range_->is_left_closed())) { // range.start_key_ < sstable
    left_index_entry_iter_ = sstable_->index_entry_begin();
    left_data_block_offset_ = 0;
  } else if (OB_FAIL(range_->start_key_.compare(sstable_->get_end_key(), *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  } else if (cmp_ret > 0 ||
             (cmp_ret == 0 && range_->is_left_open())) { // range.start_key_ > sstable
    left_index_entry_iter_ = sstable_->index_entry_end();
    left_data_block_offset_ = 0;
  } else {
    // locate left border index entry
    ObDirectLoadMultipleSSTableIndexEntryEndKeyCompare compare(
      ret, sstable_, datum_utils_, index_block_reader, data_block_reader);
    if (range_->is_left_open()) {
      // start_key < entry_last_key
      left_index_entry_iter_ = std::upper_bound(
        sstable_->index_entry_begin(), sstable_->index_entry_end(), range_->start_key_, compare);
    } else {
      // start_key <= entry_last_key
      left_index_entry_iter_ = std::lower_bound(
        sstable_->index_entry_begin(), sstable_->index_entry_end(), range_->start_key_, compare);
    }
    if (OB_FAIL(ret)) {
    } else if (left_index_entry_iter_ == sstable_->index_entry_end()) {
      left_data_block_offset_ = 0;
    } else {
      // read entry
      const int64_t entries_per_block = ObDirectLoadSSTableIndexBlock::get_entries_per_block(
        sstable_->get_meta().index_block_size_);
      const ObDirectLoadMultipleSSTableFragment &fragment =
        sstable_->get_fragments().at(left_index_entry_iter_.fragment_idx_);
      const int64_t index_block_idx = left_index_entry_iter_.index_entry_idx_ / entries_per_block;
      const int64_t index_block_offset = sstable_->get_meta().index_block_size_ * index_block_idx;
      const int64_t index_entry_idx = left_index_entry_iter_.index_entry_idx_ % entries_per_block;
      const ObDirectLoadSSTableIndexEntry *entry = nullptr;
      index_block_reader.reuse();
      if (OB_FAIL(index_block_reader.open(fragment.index_file_handle_, index_block_offset,
                                          sstable_->get_meta().index_block_size_))) {
        LOG_WARN("fail to open index file", KR(ret), K(fragment), K(index_block_offset));
      } else if (OB_FAIL(index_block_reader.get_entry(index_entry_idx, entry))) {
        LOG_WARN("fail to get entry", KR(ret));
      } else {
        left_data_block_offset_ = entry->offset_;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableDataBlockScanner::locate_right_border(
  IndexBlockReader &index_block_reader, DataBlockReader &data_block_reader)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (range_->end_key_.is_max_rowkey()) {
    right_index_entry_iter_ = sstable_->index_entry_end();
    right_data_block_offset_ = 0;
  } else if (OB_FAIL(range_->end_key_.compare(sstable_->get_start_key(), *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  } else if (cmp_ret < 0 || (cmp_ret == 0 && range_->is_right_open())) { // range.end_key_ < sstable
    right_index_entry_iter_ = sstable_->index_entry_begin();
    right_data_block_offset_ = 0;
  } else if (OB_FAIL(range_->end_key_.compare(sstable_->get_end_key(), *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  } else if (cmp_ret > 0 ||
             (cmp_ret == 0 && range_->is_right_closed())) { // range.end_key_ > sstable
    right_index_entry_iter_ = sstable_->index_entry_end();
    right_data_block_offset_ = 0;
  } else {
    // locate right border index entry
    ObDirectLoadMultipleSSTableIndexEntryStartKeyCompare compare(
      ret, sstable_, datum_utils_, index_block_reader, data_block_reader);
    if (range_->is_right_closed()) {
      // range.end_key < entry_first_key
      right_index_entry_iter_ = std::upper_bound(
        sstable_->index_entry_begin(), sstable_->index_entry_end(), range_->end_key_, compare);
    } else {
      // range.end_key <= entry_first_key
      right_index_entry_iter_ = std::lower_bound(
        sstable_->index_entry_begin(), sstable_->index_entry_end(), range_->end_key_, compare);
    }
    if (OB_FAIL(ret)) {
    } else if (right_index_entry_iter_ == sstable_->index_entry_end()) {
      right_data_block_offset_ = 0;
    } else {
      // read entry
      const int64_t entries_per_block = ObDirectLoadSSTableIndexBlock::get_entries_per_block(
        sstable_->get_meta().index_block_size_);
      const ObDirectLoadMultipleSSTableFragment &fragment =
        sstable_->get_fragments().at(right_index_entry_iter_.fragment_idx_);
      const int64_t index_block_idx = right_index_entry_iter_.index_entry_idx_ / entries_per_block;
      const int64_t index_block_offset = sstable_->get_meta().index_block_size_ * index_block_idx;
      const int64_t index_entry_idx = right_index_entry_iter_.index_entry_idx_ % entries_per_block;
      const ObDirectLoadSSTableIndexEntry *entry = nullptr;
      index_block_reader.reuse();
      if (OB_FAIL(index_block_reader.open(fragment.index_file_handle_, index_block_offset,
                                          sstable_->get_meta().index_block_size_))) {
        LOG_WARN("fail to open index file", KR(ret), K(fragment), K(index_block_offset));
      } else if (OB_FAIL(index_block_reader.get_entry(index_entry_idx, entry))) {
        LOG_WARN("fail to get entry", KR(ret));
      } else {
        right_data_block_offset_ = entry->offset_;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableDataBlockScanner::get_next_data_block(
  ObDirectLoadSSTableDataBlockDesc &data_block_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableDataBlockScanner not init", KR(ret), KP(this));
  } else if (current_index_entry_iter_ == right_index_entry_iter_) {
    ret = OB_ITER_END;
  } else {
    const ObDirectLoadMultipleSSTableFragment &fragment =
      sstable_->get_fragments().at(current_index_entry_iter_.fragment_idx_);
    ObDirectLoadMultipleSSTable::IndexEntryIterator next_index_entry_iter;
    if (current_index_entry_iter_.fragment_idx_ + 1 > right_index_entry_iter_.fragment_idx_) {
      next_index_entry_iter = right_index_entry_iter_;
    } else {
      next_index_entry_iter.sstable_ = sstable_;
      next_index_entry_iter.fragment_idx_ = current_index_entry_iter_.fragment_idx_ + 1;
      next_index_entry_iter.index_entry_idx_ = 0;
    }
    // set data_block_desc
    data_block_desc.fragment_idx_ = current_index_entry_iter_.fragment_idx_;
    data_block_desc.offset_ =
      (current_index_entry_iter_.fragment_idx_ == left_index_entry_iter_.fragment_idx_
         ? left_data_block_offset_
         : 0);
    data_block_desc.size_ =
      (current_index_entry_iter_.fragment_idx_ == right_index_entry_iter_.fragment_idx_
         ? right_data_block_offset_
         : fragment.data_file_size_) -
      data_block_desc.offset_;
    data_block_desc.block_count_ = (next_index_entry_iter - current_index_entry_iter_);
    data_block_desc.is_left_border_ = (current_index_entry_iter_ == left_index_entry_iter_);
    data_block_desc.is_right_border_ = (next_index_entry_iter == right_index_entry_iter_);
    // next fragment
    current_index_entry_iter_ = next_index_entry_iter;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
