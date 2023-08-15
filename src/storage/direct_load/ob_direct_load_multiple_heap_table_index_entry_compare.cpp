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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_entry_compare.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadMultipleSSTableIndexEntryCompare
 */

ObDirectLoadMultipleSSTableIndexEntryCompare::ObDirectLoadMultipleSSTableIndexEntryCompare(
  int &ret, ObDirectLoadMultipleHeapTable *heap_table, IndexBlockReader &index_block_reader)
  : ret_(ret),
    heap_table_(heap_table),
    index_block_reader_(index_block_reader),
    entries_per_block_(ObDirectLoadMultipleHeapTableIndexBlock::get_entries_per_block(
      heap_table->get_meta().index_block_size_))
{
}

bool ObDirectLoadMultipleSSTableIndexEntryCompare::operator()(
  const ObTabletID &tablet_id, const ObDirectLoadMultipleHeapTable::IndexEntryIterator &iter)
{
  int &ret = ret_;
  int cmp_ret = 0;
  const int64_t index_block_idx = iter.index_entry_idx_ / entries_per_block_;
  const int64_t index_block_offset = heap_table_->get_meta().index_block_size_ * index_block_idx;
  const int64_t index_entry_idx = iter.index_entry_idx_ % entries_per_block_;
  const ObDirectLoadMultipleHeapTableTabletIndex *entry = nullptr;
  index_block_reader_.reuse();
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(index_block_reader_.open(heap_table_->get_index_file_handle(),
                                              index_block_offset,
                                              heap_table_->get_meta().index_block_size_))) {
    LOG_WARN("fail to open index file", KR(ret), K(index_block_offset));
  } else if (OB_FAIL(index_block_reader_.get_index(index_entry_idx, entry))) {
    LOG_WARN("fail to get entry", KR(ret));
  } else {
    cmp_ret = tablet_id.compare(entry->tablet_id_);
  }
  return cmp_ret < 0;
}

bool ObDirectLoadMultipleSSTableIndexEntryCompare::operator()(
  const ObDirectLoadMultipleHeapTable::IndexEntryIterator &iter, const ObTabletID &tablet_id)
{
  int &ret = ret_;
  int cmp_ret = 0;
  const int64_t index_block_idx = iter.index_entry_idx_ / entries_per_block_;
  const int64_t index_block_offset = heap_table_->get_meta().index_block_size_ * index_block_idx;
  const int64_t index_entry_idx = iter.index_entry_idx_ % entries_per_block_;
  const ObDirectLoadMultipleHeapTableTabletIndex *entry = nullptr;
  index_block_reader_.reuse();
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(index_block_reader_.open(heap_table_->get_index_file_handle(),
                                              index_block_offset,
                                              heap_table_->get_meta().index_block_size_))) {
    LOG_WARN("fail to open index file", KR(ret), K(index_block_offset));
  } else if (OB_FAIL(index_block_reader_.get_index(index_entry_idx, entry))) {
    LOG_WARN("fail to get entry", KR(ret));
  } else {
    cmp_ret = entry->tablet_id_.compare(tablet_id);
  }
  return cmp_ret < 0;
}

} // namespace storage
} // namespace oceanbase
