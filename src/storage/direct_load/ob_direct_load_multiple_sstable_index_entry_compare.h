// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block_reader.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleSSTableIndexEntryStartKeyCompare
{
  typedef ObDirectLoadMultipleDatumRow RowType;
  typedef ObDirectLoadSSTableIndexBlockReader IndexBlockReader;
  typedef ObDirectLoadSSTableDataBlockReader<RowType> DataBlockReader;
public:
  ObDirectLoadMultipleSSTableIndexEntryStartKeyCompare(
    int &ret,
    ObDirectLoadMultipleSSTable *sstable,
    const blocksstable::ObStorageDatumUtils *datum_utils,
    IndexBlockReader &index_block_reader,
    DataBlockReader &data_block_reader);
  // for upper_bound
  bool operator()(const ObDirectLoadMultipleDatumRowkey &rowkey,
                  const ObDirectLoadMultipleSSTable::IndexEntryIterator &iter);
  // for lower_bound
  bool operator()(const ObDirectLoadMultipleSSTable::IndexEntryIterator &iter,
                  const ObDirectLoadMultipleDatumRowkey &rowkey);
private:
  int &ret_;
  ObDirectLoadMultipleSSTable *sstable_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  IndexBlockReader &index_block_reader_;
  DataBlockReader &data_block_reader_;
  const int64_t entries_per_block_;
};

class ObDirectLoadMultipleSSTableIndexEntryEndKeyCompare
{
  typedef ObDirectLoadMultipleDatumRow RowType;
  typedef ObDirectLoadSSTableIndexBlockReader IndexBlockReader;
  typedef ObDirectLoadSSTableDataBlockReader<RowType> DataBlockReader;
public:
  ObDirectLoadMultipleSSTableIndexEntryEndKeyCompare(
    int &ret,
    ObDirectLoadMultipleSSTable *sstable,
    const blocksstable::ObStorageDatumUtils *datum_utils,
    IndexBlockReader &index_block_reader,
    DataBlockReader &data_block_reader);
  // for upper_bound
  bool operator()(const ObDirectLoadMultipleDatumRowkey &rowkey,
                  const ObDirectLoadMultipleSSTable::IndexEntryIterator &iter);
  // for lower_bound
  bool operator()(const ObDirectLoadMultipleSSTable::IndexEntryIterator &iter,
                  const ObDirectLoadMultipleDatumRowkey &rowkey);
private:
  int &ret_;
  ObDirectLoadMultipleSSTable *sstable_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  IndexBlockReader &index_block_reader_;
  DataBlockReader &data_block_reader_;
  const int64_t entries_per_block_;
};

} // namespace storage
} // namespace oceanbase
