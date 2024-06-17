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

#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scanner.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace sql;

/**
 * ObDirectLoadMultipleSSTableScanMergeParam
 */

ObDirectLoadMultipleSSTableScanMergeParam::ObDirectLoadMultipleSSTableScanMergeParam()
  : datum_utils_(nullptr), dml_row_handler_(nullptr)
{
}

ObDirectLoadMultipleSSTableScanMergeParam::~ObDirectLoadMultipleSSTableScanMergeParam() {}

bool ObDirectLoadMultipleSSTableScanMergeParam::is_valid() const
{
  return table_data_desc_.is_valid() && nullptr != datum_utils_ && nullptr != dml_row_handler_;
}

/**
 * ObDirectLoadMultipleSSTableScanMerge
 */

ObDirectLoadMultipleSSTableScanMerge::ObDirectLoadMultipleSSTableScanMerge()
  : allocator_("TLD_ScanMerge"),
    datum_utils_(nullptr),
    dml_row_handler_(nullptr),
    range_(nullptr),
    consumers_(nullptr),
    consumer_cnt_(0),
    simple_merge_(nullptr),
    loser_tree_(nullptr),
    rows_merger_(nullptr),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  scanners_.set_tenant_id(MTL_ID());
  rows_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleSSTableScanMerge::~ObDirectLoadMultipleSSTableScanMerge() { reset(); }

void ObDirectLoadMultipleSSTableScanMerge::reset()
{
  table_data_desc_.reset();
  datum_utils_ = nullptr;
  dml_row_handler_ = nullptr;
  range_ = nullptr;
  for (int64_t i = 0; i < scanners_.count(); ++i) {
    scanners_[i]->~ObDirectLoadMultipleSSTableScanner();
  }
  scanners_.reset();
  consumers_ = nullptr;
  consumer_cnt_ = 0;
  if (nullptr != simple_merge_) {
    simple_merge_->~ScanSimpleMerger();
    simple_merge_ = nullptr;
  }
  if (nullptr != loser_tree_) {
    loser_tree_->~ScanMergeLoserTree();
    loser_tree_ = nullptr;
  }
  loser_tree_ = nullptr;
  allocator_.reset();
  is_inited_ = false;
}

int ObDirectLoadMultipleSSTableScanMerge::init(
  const ObDirectLoadMultipleSSTableScanMergeParam &param,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDirectLoadMultipleDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableScanMerge init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || sstable_array.count() > MAX_SSTABLE_COUNT ||
                         !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(sstable_array), K(range));
  } else {
    // construct scanners
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      ObDirectLoadMultipleSSTable *sstable = sstable_array.at(i);
      ObDirectLoadMultipleSSTableScanner *scanner = nullptr;
      if (sstable->is_empty()) {
      } else if (OB_FAIL(sstable->scan(param.table_data_desc_, range, param.datum_utils_,
                                       allocator_, scanner))) {
        LOG_WARN("fail to scan sstable", KR(ret));
      } else if (OB_FAIL(scanners_.push_back(scanner))) {
        LOG_WARN("fail to push back scanner", KR(ret));
      }
    }
    if (OB_SUCC(ret) && !scanners_.empty()) {
      // init consumers
      if (OB_ISNULL(consumers_ = static_cast<int64_t *>(
                      allocator_.alloc(sizeof(int64_t) * scanners_.count())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret));
      } else {
        for (int64_t i = 0; i < scanners_.count(); ++i) {
          consumers_[i] = i;
        }
        consumer_cnt_ = scanners_.count();
      }
      // init rows merger
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(compare_.init(param.datum_utils_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else if (OB_FAIL(init_rows_merger(scanners_.count()))) {
        LOG_WARN("fail to init rows merger", KR(ret));
      } else if (OB_FAIL(datum_row_.init(param.table_data_desc_.column_count_))) {
        LOG_WARN("fail to init datum row", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      table_data_desc_ = param.table_data_desc_;
      datum_utils_ = param.datum_utils_;
      dml_row_handler_ = param.dml_row_handler_;
      range_ = &range;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanMerge::init_rows_merger(int64_t sstable_count)
{
  int ret = OB_SUCCESS;
  if (sstable_count <= ObScanSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
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
    if (OB_FAIL(rows_merger_->init(sstable_count, allocator_))) {
      LOG_WARN("fail to init rows merger", KR(ret), K(sstable_count));
    } else if (FALSE_IT(rows_merger_->reuse())) {
    } else if (OB_FAIL(rows_merger_->open(sstable_count))) {
      LOG_WARN("fail to open rows merger", KR(ret), K(sstable_count));
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanMerge::supply_consume()
{
  int ret = OB_SUCCESS;
  LoserTreeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < consumer_cnt_; ++i) {
    const int64_t iter_idx = consumers_[i];
    ObDirectLoadMultipleSSTableScanner *scanner = scanners_.at(iter_idx);
    if (OB_FAIL(scanner->get_next_row(item.row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from scanner", KR(ret));
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

int ObDirectLoadMultipleSSTableScanMerge::inner_get_next_row(
  const ObDirectLoadMultipleDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (rows_merger_->empty()) {
    ret = OB_ITER_END;
  } else {
    const LoserTreeItem *top_item = nullptr;
    datum_row = nullptr;
    rows_.reuse();
    while (OB_SUCC(ret) && !rows_merger_->empty() && nullptr == datum_row) {
      if (OB_FAIL(rows_merger_->top(top_item))) {
        LOG_WARN("fail to get top item", KR(ret));
      } else if (OB_FAIL(rows_.push_back(top_item->row_))) {
        LOG_WARN("fail to push back", KR(ret));
      } else if (OB_LIKELY(rows_merger_->is_unique_champion())) {
        if (OB_LIKELY(rows_.count() == 1)) {
          datum_row = rows_.at(0);
        } else if (OB_FAIL(dml_row_handler_->handle_update_row(rows_, datum_row))) {
          LOG_WARN("fail to handle update row", KR(ret), K(rows_));
        }
      }
      if (OB_SUCC(ret)) {
        consumers_[consumer_cnt_++] = top_item->iter_idx_;
        if (OB_FAIL(rows_merger_->pop())) {
          LOG_WARN("fail to pop item", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanMerge::get_next_row(
  const ObDirectLoadMultipleDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableScanMerge not init", KR(ret), KP(this));
  } else if (scanners_.empty()) {
    ret = OB_ITER_END;
  } else if (1 == scanners_.count()) {
    // direct get next row from scanner
    if (OB_FAIL(scanners_.at(0)->get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from scanner", KR(ret));
      }
    }
  } else {
    // get next row from loser tree
    if (consumer_cnt_ > 0 && OB_FAIL(supply_consume())) {
      LOG_WARN("fail to supply consume", KR(ret));
    } else if (OB_FAIL(inner_get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to do inner get next row", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanMerge::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableScanMerge not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadMultipleDatumRow *multiple_datum_row = nullptr;
    if (OB_FAIL(get_next_row(multiple_datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next external row", KR(ret));
      }
    } else if (OB_FAIL(
                 multiple_datum_row->to_datums(datum_row_.storage_datums_, datum_row_.count_))) {
      LOG_WARN("fail to transfer datum row", KR(ret));
    } else {
      datum_row = &datum_row_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
