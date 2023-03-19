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

class ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare
{
  typedef ObDirectLoadMultipleDatumRow RowType;
  typedef ObDirectLoadSSTableIndexBlockReader IndexBlockReader;
  typedef ObDirectLoadSSTableDataBlockReader<RowType> DataBlockReader;
public:
  ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare(
    int &ret,
    ObDirectLoadMultipleSSTable *sstable,
    const blocksstable::ObStorageDatumUtils *datum_utils,
    IndexBlockReader &index_block_reader,
    DataBlockReader &data_block_reader);
  // for upper_bound
  bool operator()(const ObDirectLoadMultipleDatumRowkey &rowkey,
                  const ObDirectLoadMultipleSSTable::IndexBlockIterator &iter);
  // for lower_bound
  bool operator()(const ObDirectLoadMultipleSSTable::IndexBlockIterator &iter,
                  const ObDirectLoadMultipleDatumRowkey &rowkey);
private:
  int &ret_;
  ObDirectLoadMultipleSSTable *sstable_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  IndexBlockReader &index_block_reader_;
  DataBlockReader &data_block_reader_;
};

} // namespace storage
} // namespace oceanbase
