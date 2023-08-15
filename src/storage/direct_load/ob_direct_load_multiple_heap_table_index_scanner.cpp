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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_entry_compare.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadMultipleHeapTableIndexWholeScanner
 */

ObDirectLoadMultipleHeapTableIndexWholeScanner::ObDirectLoadMultipleHeapTableIndexWholeScanner()
  : is_inited_(false)
{
}

ObDirectLoadMultipleHeapTableIndexWholeScanner::~ObDirectLoadMultipleHeapTableIndexWholeScanner()
{
}

int ObDirectLoadMultipleHeapTableIndexWholeScanner::init(
  ObDirectLoadMultipleHeapTable *heap_table,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableIndexWholeScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == heap_table || !heap_table->is_valid() ||
                         !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(heap_table), K(table_data_desc));
  } else {
    if (OB_FAIL(index_block_reader_.init(table_data_desc.sstable_index_block_size_,
                                         table_data_desc.compressor_type_))) {
      LOG_WARN("fail to init data block reader", KR(ret));
    } else if (OB_FAIL(index_block_reader_.open(heap_table->get_index_file_handle(), 0,
                                                heap_table->get_meta().index_file_size_))) {
      LOG_WARN("fail to open index file", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexWholeScanner::init(
  const ObDirectLoadTmpFileHandle &index_file_handle,
  int64_t file_size,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableIndexWholeScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!index_file_handle.is_valid() || file_size <= 0 ||
                         !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(index_file_handle), K(file_size), K(table_data_desc));
  } else {
    if (OB_FAIL(index_block_reader_.init(table_data_desc.sstable_index_block_size_,
                                         table_data_desc.compressor_type_))) {
      LOG_WARN("fail to init data block reader", KR(ret));
    } else if (OB_FAIL(index_block_reader_.open(index_file_handle, 0, file_size))) {
      LOG_WARN("fail to open index file", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexWholeScanner::get_next_index(
  const ObDirectLoadMultipleHeapTableTabletIndex *&tablet_index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableIndexWholeScanner not init", KR(ret), KP(this));
  } else {
    tablet_index = nullptr;
    if (OB_FAIL(index_block_reader_.get_next_index(tablet_index))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next index", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleHeapTableTabletIndexWholeScanner
 */

ObDirectLoadMultipleHeapTableTabletIndexWholeScanner::
  ObDirectLoadMultipleHeapTableTabletIndexWholeScanner()
  : heap_table_(nullptr), is_iter_end_(false), is_inited_(false)
{
}

ObDirectLoadMultipleHeapTableTabletIndexWholeScanner::
  ~ObDirectLoadMultipleHeapTableTabletIndexWholeScanner()
{
}

int ObDirectLoadMultipleHeapTableTabletIndexWholeScanner::init(
  ObDirectLoadMultipleHeapTable *heap_table,
  const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableTabletIndexWholeScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == heap_table || !heap_table->is_valid() ||
                         !tablet_id.is_valid() || !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(heap_table), K(tablet_id), K(table_data_desc));
  } else {
    heap_table_ = heap_table;
    tablet_id_ = tablet_id;
    if (OB_FAIL(index_block_reader_.init(table_data_desc.sstable_index_block_size_,
                                         table_data_desc.compressor_type_))) {
      LOG_WARN("fail to init data block reader", KR(ret));
    } else if (OB_FAIL(locate_left_border())) {
      LOG_WARN("fail to locate left border", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableTabletIndexWholeScanner::locate_left_border()
{
  int ret = OB_SUCCESS;
  // locate index entry
  ObDirectLoadMultipleSSTableIndexEntryCompare compare(ret, heap_table_, index_block_reader_);
  ObDirectLoadMultipleHeapTable::IndexEntryIterator found_iter = std::lower_bound(
    heap_table_->index_entry_begin(), heap_table_->index_entry_end(), tablet_id_, compare);
  if (OB_FAIL(ret)) {
  } else if (found_iter == heap_table_->index_entry_end()) {
    is_iter_end_ = true;
  } else {
    // seek pos
    const int64_t entries_per_block =
      ObDirectLoadMultipleHeapTableIndexBlock::get_entries_per_block(
        heap_table_->get_meta().index_block_size_);
    const int64_t index_block_idx = found_iter.index_entry_idx_ / entries_per_block;
    const int64_t index_block_offset = heap_table_->get_meta().index_block_size_ * index_block_idx;
    const int64_t index_entry_idx = found_iter.index_entry_idx_ % entries_per_block;
    index_block_reader_.reuse();
    if (OB_FAIL(index_block_reader_.open(
          heap_table_->get_index_file_handle(), index_block_offset,
          heap_table_->get_meta().index_file_size_ - index_block_offset))) {
      LOG_WARN("fail to open index file", KR(ret), K(index_block_offset));
    } else if (OB_FAIL(index_block_reader_.seek_index(index_entry_idx))) {
      LOG_WARN("fail to get entry", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableTabletIndexWholeScanner::get_next_index(
  const ObDirectLoadMultipleHeapTableTabletIndex *&tablet_index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableTabletIndexWholeScanner not init", KR(ret), KP(this));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    tablet_index = nullptr;
    if (OB_FAIL(index_block_reader_.get_next_index(tablet_index))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next index", KR(ret));
      }
    } else {
      int cmp_ret = tablet_index->tablet_id_.compare(tablet_id_);
      if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet index", KR(ret), K(tablet_id_), KPC(tablet_index));
      } else if (cmp_ret > 0) {
        ret = OB_ITER_END;
        is_iter_end_ = true;
        tablet_index = nullptr;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
