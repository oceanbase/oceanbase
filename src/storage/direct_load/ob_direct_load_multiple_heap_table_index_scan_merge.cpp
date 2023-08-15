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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scan_merge.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scanner.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadMultipleHeapTableIndexScanMerge::ObDirectLoadMultipleHeapTableIndexScanMerge()
  : allocator_("TLD_ScanMerge"),
    scanners_(nullptr),
    consumers_(nullptr),
    consumer_cnt_(0),
    simple_merge_(nullptr),
    loser_tree_(nullptr),
    rows_merger_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadMultipleHeapTableIndexScanMerge::~ObDirectLoadMultipleHeapTableIndexScanMerge()
{
}

int ObDirectLoadMultipleHeapTableIndexScanMerge::init(
  const ObIArray<ObIDirectLoadMultipleHeapTableIndexScanner *> &scanners)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableIndexScanMerge init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(scanners.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(scanners.count()));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (scanners.count() > 1) {
      // init consumers
      if (OB_ISNULL(consumers_ = static_cast<int64_t *>(
                      allocator_.alloc(sizeof(int64_t) * scanners.count())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret));
      } else {
        for (int64_t i = 0; i < scanners.count(); ++i) {
          consumers_[i] = i;
        }
        consumer_cnt_ = scanners.count();
      }
      // init rows merger
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_rows_merger(scanners.count()))) {
        LOG_WARN("fail to init rows merger", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      scanners_ = &scanners;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexScanMerge::init_rows_merger(int64_t count)
{
  int ret = OB_SUCCESS;
  if (count <= ObScanSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    if (OB_ISNULL(simple_merge_ = OB_NEWx(ScanSimpleMerger, (&allocator_), compare_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ScanSimpleMerger", KR(ret));
    } else {
      rows_merger_ = simple_merge_;
    }
  } else {
    if (OB_ISNULL(loser_tree_ = OB_NEWx(ScanMergeLoserTree, (&allocator_), compare_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ScanMergeLoserTree", KR(ret));
    } else {
      rows_merger_ = loser_tree_;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rows_merger_->init(count, allocator_))) {
      LOG_WARN("fail to init rows merger", KR(ret), K(count));
    } else if (FALSE_IT(rows_merger_->reuse())) {
    } else if (OB_FAIL(rows_merger_->open(count))) {
      LOG_WARN("fail to open rows merger", KR(ret), K(count));
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexScanMerge::supply_consume()
{
  int ret = OB_SUCCESS;
  LoserTreeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < consumer_cnt_; ++i) {
    const int64_t iter_idx = consumers_[i];
    ObIDirectLoadMultipleHeapTableIndexScanner *scanner = scanners_->at(iter_idx);
    if (OB_FAIL(scanner->get_next_index(item.index_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next index from scanner", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      item.iter_idx_ = iter_idx;
      if (OB_FAIL(rows_merger_->push(item))) {
        LOG_WARN("fail to push to loser tree", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // no worry, if no new items pushed, the rebuild will quickly exit
    if (OB_FAIL(rows_merger_->rebuild())) {
      LOG_WARN("fail to rebuild loser tree", KR(ret), K(consumer_cnt_));
    } else {
      consumer_cnt_ = 0;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexScanMerge::inner_get_next_index(
  int64_t &idx, const ObDirectLoadMultipleHeapTableTabletIndex *&tablet_index)
{
  int ret = OB_SUCCESS;
  tablet_index = nullptr;
  if (rows_merger_->empty()) {
    ret = OB_ITER_END;
  } else {
    const LoserTreeItem *top_item = nullptr;
    if (OB_FAIL(rows_merger_->top(top_item))) {
      LOG_WARN("fail to get top item", KR(ret));
    } else {
      idx = top_item->iter_idx_;
      tablet_index = top_item->index_;
      consumers_[consumer_cnt_++] = top_item->iter_idx_;
      if (OB_FAIL(rows_merger_->pop())) {
        LOG_WARN("fail to pop item", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexScanMerge::get_next_index(
  int64_t &idx, const ObDirectLoadMultipleHeapTableTabletIndex *&tablet_index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableIndexScanMerge not init", KR(ret), KP(this));
  } else if (1 == scanners_->count()) {
    // direct get next row from scanner
    idx = 0;
    if (OB_FAIL(scanners_->at(0)->get_next_index(tablet_index))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next index from scanner", KR(ret));
      }
    }
  } else {
    // get next row from loser tree
    if (consumer_cnt_ > 0 && OB_FAIL(supply_consume())) {
      LOG_WARN("fail to supply consume", KR(ret));
    } else if (OB_FAIL(inner_get_next_index(idx, tablet_index))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to do inner get next index", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
