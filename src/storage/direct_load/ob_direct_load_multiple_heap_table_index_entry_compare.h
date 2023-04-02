// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleHeapTableIndexBlockReader;

class ObDirectLoadMultipleSSTableIndexEntryCompare
{
  typedef ObDirectLoadMultipleHeapTableIndexBlockReader IndexBlockReader;
public:
  ObDirectLoadMultipleSSTableIndexEntryCompare(int &ret, ObDirectLoadMultipleHeapTable *heap_table,
                                               IndexBlockReader &index_block_reader);
  // for upper_bound
  bool operator()(const common::ObTabletID &tablet_id,
                  const ObDirectLoadMultipleHeapTable::IndexEntryIterator &iter);
  // for lower_bound
  bool operator()(const ObDirectLoadMultipleHeapTable::IndexEntryIterator &iter,
                  const common::ObTabletID &tablet_id);
private:
  int &ret_;
  ObDirectLoadMultipleHeapTable *heap_table_;
  IndexBlockReader &index_block_reader_;
  const int64_t entries_per_block_;
};

} // namespace storage
} // namespace oceanbase
