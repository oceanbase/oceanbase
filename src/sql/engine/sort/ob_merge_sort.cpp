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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_merge_sort.h"

#include <algorithm>
#include "storage/ob_partition_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/cell/ob_cell_reader.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObMergeSort::ObMergeSort(ObIAllocator& allocator)
    : cur_round_(&sort_run_[0]),
      next_round_(&sort_run_[1]),
      merge_run_count_(0),
      last_run_idx_(-1),
      cur_row_(),
      comp_ret_(0),
      comparer_(NULL, comp_ret_),
      heap_(comparer_, &allocator),
      allocator_(allocator)
{}

ObMergeSort::~ObMergeSort()
{}

void ObMergeSort::reset()
{
  sort_run_[0].reset();
  sort_run_[1].reset();
  cur_round_ = NULL;
  next_round_ = NULL;
  merge_run_count_ = 0;
  last_run_idx_ = -1;
  comp_ret_ = 0;
  heap_.reset();
}

void ObMergeSort::reuse()
{
  sort_run_[0].reset();
  sort_run_[1].reset();
  cur_round_ = NULL;
  next_round_ = NULL;
  merge_run_count_ = 0;
  last_run_idx_ = -1;
  comp_ret_ = 0;
  heap_.reset();
}

int ObMergeSort::clean_up()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int i = 0; i < 2; ++i) {
    if (sort_run_[i].is_inited() && OB_SUCCESS != (tmp_ret = sort_run_[i].clean_up())) {
      ret = tmp_ret;
      LOG_WARN("failed to clean up sort run", K(ret));
    }
  }
  cur_round_ = NULL;
  next_round_ = NULL;
  merge_run_count_ = 0;
  last_run_idx_ = -1;
  comp_ret_ = 0;
  heap_.reset();
  return ret;
}

int ObMergeSort::init(const ObIArray<ObSortColumn>& sort_columns, const ObNewRow& cur_row, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; i++) {
    if (OB_FAIL(OB_I(t1) sort_run_[i].init(FILE_BUF_SIZE, EXPIRE_TIMESTAMP, &allocator_, tenant_id))) {
      LOG_WARN("init sort_run failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    void* buf = NULL;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObObj) * cur_row.count_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc obobjs", K(ret));
    } else {
      cur_row_.cells_ = new (buf) ObObj();
      cur_row_.count_ = cur_row.count_;
      cur_row_.projector_ = cur_row.projector_;
      cur_row_.projector_size_ = cur_row.projector_size_;
      comp_ret_ = 0;
      comparer_.set_sort_columns(&sort_columns);
      cur_round_ = &sort_run_[0];
      next_round_ = &sort_run_[1];
      // div 2 for prefetch
      merge_run_count_ = MEM_LIMIT_SIZE / FILE_BUF_SIZE / 2;
    }
  }
  return ret;
}

int ObMergeSort::dump_base_run(ObOuterRowIterator& row_iterator, bool build_fragment)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur run file is null", K(ret));
  } else {
    // write macro
    while (OB_SUCC(ret) && OB_SUCC(row_iterator.get_next_row(cur_row_))) {
      if (OB_FAIL(OB_I(t2) cur_round_->add_item(cur_row_))) {
        LOG_WARN("write item failed", K(ret));
      } else {
        LOG_DEBUG("[MERGE_SORT: ADD ROW SUCC]", K(cur_row_));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (build_fragment && OB_FAIL(OB_I(t2) cur_round_->build_fragment())) {
        LOG_WARN("build fragment failed", K(ret));
      }
    }
  }
  return ret;
}

// manual to build fragment
int ObMergeSort::build_cur_fragment()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur run file is null", K(ret));
  } else if (OB_FAIL(cur_round_->build_fragment())) {
    LOG_WARN("build fragment failed", K(ret));
  }
  return ret;
}

ObMergeSort::HeapComparer::HeapComparer(const ObIArray<ObSortColumn>* sort_columns, int& ret)
    : sort_columns_(sort_columns), ret_(ret)
{}

bool ObMergeSort::HeapComparer::operator()(const RowRun& run1, const RowRun& run2)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_)) {
  } else if (OB_ISNULL(run1.row_) || OB_ISNULL(run2.row_) || OB_ISNULL(sort_columns_)) {
    LOG_ERROR("run1.row or run2.row or sort_columns is null", K(run1.row_), K(sort_columns_));
  } else {
    int cmp = 0;
    ObCompareCtx cmp_ctx;
    cmp_ctx.cmp_type_ = ObMaxType;
    cmp_ctx.cmp_cs_type_ = CS_TYPE_INVALID;
    cmp_ctx.is_null_safe_ = true;
    cmp_ctx.tz_off_ = INVALID_TZ_OFF;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < sort_columns_->count(); ++i) {
      int64_t index = sort_columns_->at(i).index_;
      if (index > run1.row_->get_count() || index > run2.row_->get_count()) {
        LOG_ERROR(
            "index or count is invalid", K(ret), K(index), K(*run2.row_), K(*run1.row_), K(run1.id_), K(run2.id_));
      } else {
        cmp_ctx.null_pos_ = sort_columns_->at(i).get_cmp_null_pos();
        cmp_ctx.cmp_cs_type_ = sort_columns_->at(i).cs_type_;
        cmp = run1.row_->cells_[index].compare(run2.row_->cells_[index], cmp_ctx);
        if (cmp < 0) {
          bret = !sort_columns_->at(i).is_ascending();
        } else if (cmp > 0) {
          bret = sort_columns_->at(i).is_ascending();
        } else {
        }
      }
    }  // end for
    ret_ = ret;
  }
  return bret;
}

int ObMergeSort::build_merge_heap(const int64_t column_count, const int64_t start_idx, const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_round is null", K(ret));
  } else if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count is invalid", K(ret), K(column_count));
  } else {
    heap_.reset();
    RowRun row_run;
    const ObNewRow* row = NULL;
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      if (OB_ISNULL(cur_round_->iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null", K(ret));
      } else if (OB_FAIL(OB_I(t3) cur_round_->iters_.at(i)->get_next_item(row))) {
        LOG_WARN("failed to read next row", K(ret), K(i));
      } else if (OB_ITER_END == ret) {
        // ignore this empty run
        ret = OB_SUCCESS;
      } else {
        row_run.id_ = i;
        row_run.row_ = row;
        if (OB_FAIL(heap_.push(row_run))) {
          LOG_WARN("failed to push back to array", K(ret));
        } else if (OB_FAIL(OB_I(t3) comparer_.get_error_code())) {
          LOG_WARN("failed to compare items", K(ret));
        }
      }
    }  // end for
  }
  return ret;
}

// dump the result of each round merge
int ObMergeSort::dump_merge_run()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(next_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next round file is null", K(ret));
  } else {
    ObNewRow row = cur_row_;
    while (OB_SUCC(ret) && OB_SUCC(do_get_next_row(row, false /* no clear on iterate end */))) {
      if (OB_FAIL(OB_I(t4) next_round_->add_item(row))) {
        LOG_WARN("failed to append row", K(ret), K(row));
      } else {
        LOG_DEBUG("[MERGE_SORT: ADD ROW SUCC]", K(row));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(OB_I(t4) next_round_->build_fragment())) {
        LOG_WARN("build fragment failed", K(ret));
      }
    }
  }
  return ret;
}

// one Round merge
int ObMergeSort::do_one_round(const int64_t column_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_round is null", K(ret));
  } else if (cur_round_->iters_.count() <= 0 || merge_run_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dump_run_count or merge_run_count is invalid", K(cur_round_->iters_.count()), K_(merge_run_count));
  } else {
    // merge merge_count times, such as merge_count = 3,
    // merge(r1,r2,r3)->newr1, merge(r4,r5,6)->newr2...
    ObSortRun::FragmentIteratorList iters;
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_round_->iters_.count(); i += merge_run_count_) {
      // do merge run
      int64_t end_read_idx =
          cur_round_->iters_.count() > i + merge_run_count_ ? i + merge_run_count_ : cur_round_->iters_.count();
      // reset
      iters.reset();
      for (int64_t j = i; OB_SUCC(ret) && j < end_read_idx; ++j) {
        if (OB_FAIL(iters.push_back(cur_round_->iters_.at(j)))) {
          LOG_WARN("push back read iter failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(OB_I(t5) cur_round_->prefetch(iters))) {
          LOG_WARN("prefetch iters failed", K(ret));
        } else if (OB_FAIL(OB_I(t5) build_merge_heap(column_count, i, end_read_idx))) {
          LOG_WARN("merge run failed", K(ret));
        } else if (OB_FAIL(dump_merge_run())) {
          LOG_WARN("dump merge run failed", K(ret));
        } else {
        }
      }
    }
    if (OB_SUCC(ret)) {  // wait io end
      if (OB_FAIL(next_round_->writer_.sync())) {
        LOG_WARN("fail to finish writer");
      } else {
        next_round_->writer_.reset();
        next_round_->is_writer_opened_ = false;
      }
    }
  }
  return ret;
}

// the merge sort's entry
int ObMergeSort::do_merge_sort(const int64_t column_count)
{
  int ret = OB_SUCCESS;
  int64_t round_id = 0;
  if (OB_ISNULL(cur_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_round is null", K(ret));
  } else if (OB_FAIL(cur_round_->writer_.sync())) {  // wait io end for dump base run
    LOG_WARN("fail to finish writer");
  } else {
    cur_round_->writer_.reset();
    cur_round_->is_writer_opened_ = false;
  }
  // heap_.count = merge_run_count_ -> run count for each merge
  while (OB_SUCC(ret) && cur_round_->iters_.count() > merge_run_count_) {
    if (OB_FAIL(OB_I(t6) do_one_round(column_count))) {
      LOG_WARN("do one round failed", K(ret), K(round_id));
    } else if (OB_ISNULL(cur_round_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur run file is null", K(ret));
    } else if (OB_FAIL(cur_round_->reuse())) {
      LOG_WARN("clean up next round failed", K(ret));
    } else {
      ++round_id;
      std::swap(cur_round_, next_round_);
    }
  }
  // do final round merge
  if (OB_SUCC(ret)) {
    // final merge, no need dump run, build heap, then return row directly
    if (OB_ISNULL(cur_round_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_round is null", K(ret));
    } else if (OB_FAIL(cur_round_->prefetch(cur_round_->iters_))) {
      LOG_WARN("prefetch iters failed", K(ret));
    } else if (OB_FAIL(build_merge_heap(column_count, 0, cur_round_->iters_.count()))) {
      LOG_WARN("build merge heap failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObMergeSort::get_next_row(ObNewRow& row)
{
  return do_get_next_row(row, true /* clear on iterator end */);
}

int ObMergeSort::do_get_next_row(common::ObNewRow& row, const bool clear_on_end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_round_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur round is null", K(ret));
  } else if (last_run_idx_ >= 0 && last_run_idx_ < cur_round_->iters_.count()) {
    RowRun row_run;
    // get the next row from the run_idx run
    if (OB_ISNULL(cur_round_->iters_.at(last_run_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur run_idx iter is null", K(ret));
    } else if (OB_FAIL(OB_I(t7) cur_round_->iters_.at(last_run_idx_)->get_next_item(row_run.row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read next row", K(ret), K(last_run_idx_));
      } else if (OB_FAIL(heap_.pop())) {  // cover common::OB_ITER_END,by design
        LOG_WARN("heap pop failed", K(ret));
      } else if (OB_FAIL(comparer_.get_error_code())) {
        LOG_WARN("failed to compare items", K(ret));
      }
    } else if (OB_ISNULL(row_run.row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null", K(ret), K(row_run.id_));
    } else {
      row_run.id_ = last_run_idx_;
      if (OB_FAIL(heap_.replace_top(row_run))) {
        LOG_WARN("failed to replace heap top", K(ret));
      } else {
        LOG_DEBUG("[MERGE_SORT: REPLACE HEAP TOP", K(*row_run.row_), K(last_run_idx_));
      }
    }
    last_run_idx_ = -1;
  }

  // no row
  if (OB_SUCC(ret) && heap_.empty()) {
    ret = OB_ITER_END;
    if (clear_on_end) {
      cur_round_->reuse();
    }
  }

  if (OB_SUCC(ret)) {
    const RowRun* top_item = NULL;
    if (OB_FAIL(heap_.top(top_item))) {
      LOG_WARN("failed to get heap top item", K(ret));
    } else if (OB_ISNULL(top_item) || OB_ISNULL(top_item->row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("top_item or top_item's row is null", K(ret), K(top_item));
    } else if (OB_FAIL(attach_row(*top_item->row_, row))) {
      LOG_WARN("failed to attach row", K(ret));
    } else {
      last_run_idx_ = top_item->id_;
    }
  }
  return ret;
}

int ObMergeSort::attach_row(const common::ObNewRow& src, common::ObNewRow& dst)
{
  int ret = OB_SUCCESS;
  if (src.count_ > dst.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array row count is invalid", K(ret), K(src.count_), K(dst.count_));
  } else {
    int64_t idx = 0;
    while (idx < src.count_) {
      dst.cells_[idx] = src.cells_[idx];
      ++idx;
    }
  }
  return ret;
}
