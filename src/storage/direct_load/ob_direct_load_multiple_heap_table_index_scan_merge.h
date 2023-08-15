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

#include "lib/container/ob_loser_tree.h"
#include "storage/access/ob_simple_rows_merger.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scan_merge_loser_tree.h"

namespace oceanbase
{
namespace storage
{
class ObIDirectLoadMultipleHeapTableIndexScanner;

class ObDirectLoadMultipleHeapTableIndexScanMerge
{
public:
  static const int64_t MAX_SCANNER_COUNT = 1024;
  typedef ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem LoserTreeItem;
  typedef ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare LoserTreeCompare;
  typedef ObSimpleRowsMerger<LoserTreeItem, LoserTreeCompare> ScanSimpleMerger;
  typedef common::ObLoserTree<LoserTreeItem, LoserTreeCompare, MAX_SCANNER_COUNT>
    ScanMergeLoserTree;
public:
  ObDirectLoadMultipleHeapTableIndexScanMerge();
  ~ObDirectLoadMultipleHeapTableIndexScanMerge();
  int init(const common::ObIArray<ObIDirectLoadMultipleHeapTableIndexScanner *> &scanners);
  int get_next_index(int64_t &idx, const ObDirectLoadMultipleHeapTableTabletIndex *&tablet_index);
private:
  int init_rows_merger(int64_t count);
  int supply_consume();
  int inner_get_next_index(int64_t &idx,
                           const ObDirectLoadMultipleHeapTableTabletIndex *&tablet_index);
private:
  common::ObArenaAllocator allocator_;
  const common::ObIArray<ObIDirectLoadMultipleHeapTableIndexScanner *> *scanners_;
  int64_t *consumers_;
  int64_t consumer_cnt_;
  LoserTreeCompare compare_;
  ScanSimpleMerger *simple_merge_;
  ScanMergeLoserTree *loser_tree_;
  common::ObRowsMerger<LoserTreeItem, LoserTreeCompare> *rows_merger_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
