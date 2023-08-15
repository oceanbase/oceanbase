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
#pragma once

#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block_reader.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleDatumRange;
class ObDirectLoadMultipleDatumRow;
class ObDirectLoadSSTableDataBlockDesc;

class ObDirectLoadMultipleSSTableDataBlockScanner
{
  typedef ObDirectLoadMultipleDatumRow RowType;
  typedef ObDirectLoadSSTableIndexBlockReader IndexBlockReader;
  typedef ObDirectLoadSSTableDataBlockReader<RowType> DataBlockReader;
public:
  ObDirectLoadMultipleSSTableDataBlockScanner();
  ~ObDirectLoadMultipleSSTableDataBlockScanner();
  int init(ObDirectLoadMultipleSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadMultipleDatumRange &range,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  int get_next_data_block(ObDirectLoadSSTableDataBlockDesc &data_block_desc);
private:
  int locate_left_border(IndexBlockReader &index_block_reader, DataBlockReader &data_block_reader);
  int locate_right_border(IndexBlockReader &index_block_reader, DataBlockReader &data_block_reader);
private:
  ObDirectLoadMultipleSSTable *sstable_;
  const ObDirectLoadMultipleDatumRange *range_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  // [left, right)
  ObDirectLoadMultipleSSTable::IndexEntryIterator left_index_entry_iter_;
  int64_t left_data_block_offset_;
  ObDirectLoadMultipleSSTable::IndexEntryIterator right_index_entry_iter_;
  int64_t right_data_block_offset_;
  ObDirectLoadMultipleSSTable::IndexEntryIterator current_index_entry_iter_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
