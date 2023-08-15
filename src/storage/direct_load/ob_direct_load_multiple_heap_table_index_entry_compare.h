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
