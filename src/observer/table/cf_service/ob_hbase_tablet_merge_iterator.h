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

#ifndef _OB_HBASE_TABLET_MERGE_ITERATOR
#define _OB_HBASE_TABLET_MERGE_ITERATOR

#include "observer/table/adapters/ob_hbase_cell_iter.h"
#include "observer/table/ob_table_merge_filter.h"
#include "observer/table/adapters/ob_hbase_adapter_factory.h"
#include "observer/table/ob_htable_filter_operator.h"
#include "observer/table/common/ob_hbase_common_struct.h"

namespace oceanbase
{
namespace table
{

class ObHbaseCellRowIter : public ObTableRowIterator<common::ObNewRow, ObHbaseRescanParam>
{
public:
  ObHbaseCellRowIter(ObHbaseICellIter *cell_iter)
    : cur_row_(nullptr),
      cell_iter_(cell_iter)
  {}
  virtual ~ObHbaseCellRowIter()
  {
    if (OB_NOT_NULL(cell_iter_)) {
      cell_iter_->close();
      cell_iter_->~ObHbaseICellIter();
      cell_iter_ = nullptr;
    }
  }
  int next_row();
  common::ObNewRow *get_cur_row() { return cur_row_; }
  const common::ObNewRow *get_cur_row() const { return cur_row_; }
  int rescan(ObHbaseRescanParam &rescan_param)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(cell_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected null cell iter", K(ret));
    } else if (OB_FAIL(cell_iter_->rescan(rescan_param))) {
      SERVER_LOG(WARN, "fail to rescan", K(ret));
    } else {}
    return ret;
  }
  void close()
  {
    if (OB_NOT_NULL(cell_iter_)) {
      cell_iter_->close();
    }
  }

  TO_STRING_KV(KPC_(cur_row), K_(valid));
private:
  // current row
  common::ObNewRow *cur_row_;
  ObHbaseICellIter *cell_iter_;
};

class ObHbaseTabletMergeIterator : public ObHbaseICellIter 
{
  using ResultMergeIterator = ObTableMergeRowIterator<common::ObNewRow, table::ObTableMergeFilterCompare, ObHbaseCellRowIter>;
public:
  ObHbaseTabletMergeIterator(ObTableExecCtx &exec_ctx, const ObTableQuery &query);
  virtual ~ObHbaseTabletMergeIterator();
  virtual int open() override;
  virtual int get_next_cell(ObNewRow *&row) override;
  virtual int rescan(ObHbaseRescanParam &rescan_param) override;
  virtual int close() override;
protected:
  virtual int init_row_compare(ObQueryFlag::ScanOrder scan_order,
                               ObTableMergeFilterCompare *&compare);
private:
  int init_cell_iters();
protected:
  common::ObArenaAllocator allocator_;
private:
  ObTableExecCtx &exec_ctx_;
  const ObTableQuery &query_;
  common::ObSEArray<ObHbaseCellRowIter *, 4> cell_iters_;
  ObTableMergeFilterCompare *compare_;
  ResultMergeIterator *merge_iter_;
  ObHbaseAdapterGuard adapter_guard_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHbaseTabletMergeIterator);
};

// unlike normal hbase model, time series hbase model return unordered qualifier
class ObHbaseTSTabletMergeIter : public ObHbaseTabletMergeIterator
{
public:
  ObHbaseTSTabletMergeIter(ObTableExecCtx &exec_ctx, const ObTableQuery &query)
  : ObHbaseTabletMergeIterator(exec_ctx, query) 
  {}
  virtual ~ObHbaseTSTabletMergeIter() = default;
private:
  virtual int init_row_compare(ObQueryFlag::ScanOrder scan_order,
                               ObTableMergeFilterCompare *&compare) override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHbaseTSTabletMergeIter);
};


} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_HBASE_TABLET_MERGE_ITERATOR
