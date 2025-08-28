/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SERVER
#include "ob_hbase_tablet_merge_iterator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

int ObHbaseCellRowIter::next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cell_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null cell iter", K(ret));
  } else if (OB_FAIL(cell_iter_->get_next_cell(cur_row_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get cell", K(ret));
    } else {
      set_valid(false);
      LOG_DEBUG("one cell iter end", K(ret), KP(this));
    }
  } else {
    LOG_DEBUG("one cell iter get row", K(ret), KP(this), K(cur_row_));
  }
  return ret;
}

ObHbaseTabletMergeIterator::ObHbaseTabletMergeIterator(ObTableExecCtx &exec_ctx, const ObTableQuery &query)
    : ObHbaseICellIter(),
      allocator_(ObMemAttr(MTL_ID(), "HbaseTbltMerge")),
      exec_ctx_(exec_ctx), query_(query),
      compare_(nullptr), merge_iter_(nullptr),
      adapter_guard_(allocator_, exec_ctx)
{
  cell_iters_.set_attr(ObMemAttr(MTL_ID(), "HbaseCellIters"));
}

ObHbaseTabletMergeIterator::~ObHbaseTabletMergeIterator()
{
  if (OB_NOT_NULL(merge_iter_)) {
    merge_iter_->~ResultMergeIterator();
    merge_iter_ = nullptr;
  }
  if (OB_NOT_NULL(compare_)) {
    compare_->~ObTableMergeFilterCompare();
    compare_ = nullptr;
  }
  for (int i = 0; i < cell_iters_.count(); i++) {
    ObHbaseCellRowIter *cell_iter = cell_iters_.at(i);
    if (OB_NOT_NULL(cell_iter)) {
      cell_iter->~ObHbaseCellRowIter();
    }
  }
  cell_iters_.reset();
}

int ObHbaseTabletMergeIterator::open()
{
  int ret = OB_SUCCESS;
  ObQueryFlag::ScanOrder scan_order = query_.get_scan_order();
  if (OB_FAIL(init_cell_iters())) {
    LOG_WARN("fail to init cell iterators", K(ret));
  } else if (OB_ISNULL(merge_iter_ = OB_NEWx(ResultMergeIterator, &allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create merge_iter_", K(ret));
  } else if (OB_FAIL(merge_iter_->assign_inner_row_iters(cell_iters_))) {
    LOG_WARN("fail to assign results", K(ret));
  } else if (OB_FAIL(init_row_compare(scan_order, compare_))) {
    LOG_WARN("fail to init row compare", K(ret), K(scan_order));
  } else if (OB_FAIL(merge_iter_->init(compare_))) {
    LOG_WARN("fail to build merge_iter_", K(ret));
  } else {}

  if (OB_FAIL(ret) && OB_NOT_NULL(merge_iter_)) {
    merge_iter_->~ResultMergeIterator();
    allocator_.free(merge_iter_);
    merge_iter_ = nullptr;
  }
  return ret;
}

int ObHbaseTabletMergeIterator::init_row_compare(ObQueryFlag::ScanOrder scan_order,
                                                 ObTableMergeFilterCompare *&compare)
{
  int ret = OB_SUCCESS;
  if (scan_order == ObQueryFlag::Reverse && OB_ISNULL(compare =
      static_cast<ObTableMergeFilterCompare *>(OB_NEWx(ObHbaseRowReverseCompare, &allocator_, true)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (scan_order == ObQueryFlag::Forward && OB_ISNULL(compare =
      static_cast<ObTableMergeFilterCompare *>(OB_NEWx(ObHbaseRowForwardCompare, &allocator_, true)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  }
  return ret;
}

int ObHbaseTabletMergeIterator::close()
{
  for (int i = 0; i < cell_iters_.count(); i++) {
    ObHbaseCellRowIter *cell_iter = cell_iters_.at(i);
    if (OB_NOT_NULL(cell_iter)) {
      cell_iter->close();
    }
  }
  return OB_SUCCESS;
}

int ObHbaseTabletMergeIterator::init_cell_iters()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 4> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "HbaseMrgTbltIds"));
  ObIArray<ObTabletID> &query_tablet_ids = const_cast<ObTableQuery &>(query_).get_tablet_ids();
  if (OB_FAIL(tablet_ids.assign(query_tablet_ids))) {
    LOG_WARN("fail to assign tablet ids", K(ret), K(query_tablet_ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    query_tablet_ids.reuse();
    ObIHbaseAdapter *hbase_adapter = nullptr;
    ObHbaseICellIter *tmp_cell_iter = nullptr;
    ObHbaseCellRowIter *tmp_cell_row_iter = nullptr;
    if (OB_FAIL(query_tablet_ids.push_back(tablet_ids.at(i)))) {
      LOG_WARN("fail to add tablet id", K(ret), K(tablet_ids.at(i)));
    } else if (OB_FAIL(adapter_guard_.get_hbase_adapter(hbase_adapter))) {
      LOG_WARN("fail to get hbase adapter", K(ret));
    } else if (OB_FAIL(hbase_adapter->scan(allocator_, exec_ctx_, query_, tmp_cell_iter))) {
      LOG_WARN("fail to scan", K(ret), K_(query));
    } else {
      if (OB_FAIL(tmp_cell_iter->open())) {
        LOG_WARN("fail to open iter", K(ret));
      } else if (OB_ISNULL(tmp_cell_row_iter = OB_NEWx(ObHbaseCellRowIter, &allocator_, tmp_cell_iter))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObHbaseCellRowIter))); 
      } else {}

      if (OB_FAIL(ret)) {
        tmp_cell_iter->close();
        tmp_cell_iter->~ObHbaseICellIter();
      } else if (OB_FAIL(cell_iters_.push_back(tmp_cell_row_iter))) {
        LOG_WARN("fail to add cell iter", K(ret));
        tmp_cell_row_iter->~ObHbaseCellRowIter();
      }
    }
  }
  LOG_DEBUG("ObHbaseTabletMergeIterator::init_cell_iters", K(ret), K(tablet_ids), K(query_));
  return ret;
}

int ObHbaseTabletMergeIterator::get_next_cell(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null merge iter", K(ret));
  } else if (OB_FAIL(merge_iter_->get_next_row(row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get cell", K(ret));
    } else {
      LOG_DEBUG("tablet merge iter end", K(ret), KP(merge_iter_));
    }
  } else {
    LOG_DEBUG("tablet merge iter get one cell", K(ret), KP(this), KPC(row));
  }
  return ret;
}

int ObHbaseTabletMergeIterator::rescan(ObHbaseRescanParam &rescan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null merge iter", K(ret));
  } else if (OB_FAIL(merge_iter_->rescan(rescan_param))) {
    LOG_WARN("fail to rescan", K(ret));
  } else {}
  return ret;
}

int ObHbaseTSTabletMergeIter::init_row_compare(ObQueryFlag::ScanOrder scan_order,
                                               ObTableMergeFilterCompare *&compare)
{
  int ret = OB_SUCCESS;
  if (scan_order == ObQueryFlag::Reverse && OB_ISNULL(compare =
      static_cast<ObTableMergeFilterCompare *>(OB_NEWx(ObHbaseRowKeyReverseCompare, &allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (scan_order == ObQueryFlag::Forward && OB_ISNULL(compare =
      static_cast<ObTableMergeFilterCompare *>(OB_NEWx(ObHbaseRowKeyForwardCompare, &allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase