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

#include "storage/direct_load/ob_direct_load_multiple_sstable_index_block_compare.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare::ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare(
  int &ret,
  ObDirectLoadMultipleSSTable *sstable,
  const ObStorageDatumUtils *datum_utils,
  IndexBlockReader &index_block_reader,
  DataBlockReader &data_block_reader)
  : ret_(ret),
    sstable_(sstable),
    datum_utils_(datum_utils),
    index_block_reader_(index_block_reader),
    data_block_reader_(data_block_reader)
{
}

bool ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare::operator()(
  const ObDirectLoadMultipleDatumRowkey &rowkey,
  const ObDirectLoadMultipleSSTable::IndexBlockIterator &iter)
{
  int &ret = ret_;
  int cmp_ret = 0;
  const ObDirectLoadMultipleSSTableFragment &fragment =
    sstable_->get_fragments().at(iter.fragment_idx_);
  const int64_t index_block_offset = sstable_->get_meta().index_block_size_ * iter.index_block_idx_;
  const ObDirectLoadSSTableIndexEntry *last_index_entry = nullptr;
  const RowType *row = nullptr;
  index_block_reader_.reuse();
  data_block_reader_.reuse();
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(index_block_reader_.open(fragment.index_file_handle_, index_block_offset,
                                              sstable_->get_meta().index_block_size_))) {
    LOG_WARN("fail to open index file", KR(ret), K(fragment), K(index_block_offset));
  } else if (OB_FAIL(index_block_reader_.get_last_entry(last_index_entry))) {
    LOG_WARN("fail to get last entry", KR(ret));
  } else if (OB_FAIL(data_block_reader_.open(fragment.data_file_handle_, last_index_entry->offset_,
                                             last_index_entry->size_))) {
    LOG_WARN("fail to open data file", KR(ret), K(fragment), KPC(last_index_entry));
  } else if (OB_FAIL(data_block_reader_.get_last_row(row))) {
    LOG_WARN("fail to get last row", KR(ret));
  } else if (OB_FAIL(rowkey.compare(row->rowkey_, *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  }
  return cmp_ret < 0;
}

bool ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare::operator()(
  const ObDirectLoadMultipleSSTable::IndexBlockIterator &iter,
  const ObDirectLoadMultipleDatumRowkey &rowkey)
{
  int &ret = ret_;
  int cmp_ret = 0;
  const ObDirectLoadMultipleSSTableFragment &fragment =
    sstable_->get_fragments().at(iter.fragment_idx_);
  const int64_t index_block_offset = sstable_->get_meta().index_block_size_ * iter.index_block_idx_;
  const ObDirectLoadSSTableIndexEntry *first_index_entry = nullptr;
  const RowType *row = nullptr;
  index_block_reader_.reuse();
  data_block_reader_.reuse();
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(index_block_reader_.open(fragment.index_file_handle_, index_block_offset,
                                              sstable_->get_meta().index_block_size_))) {
    LOG_WARN("fail to open index file", KR(ret), K(fragment), K(index_block_offset));
  } else if (OB_FAIL(index_block_reader_.get_next_entry(first_index_entry))) {
    LOG_WARN("fail to get last entry", KR(ret));
  } else if (OB_FAIL(data_block_reader_.open(fragment.data_file_handle_, first_index_entry->offset_,
                                             first_index_entry->size_))) {
    LOG_WARN("fail to open data file", KR(ret), K(fragment), KPC(first_index_entry));
  } else if (OB_FAIL(data_block_reader_.get_next_row(row))) {
    LOG_WARN("fail to get last row", KR(ret));
  } else if (OB_FAIL(row->rowkey_.compare(rowkey, *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  }
  return cmp_ret < 0;
}

} // namespace storage
} // namespace oceanbase
