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

#include "ob_multiple_scan_merge.h"
#include <math.h>
#include "share/ob_get_compat_mode.h"
#include "ob_block_row_store.h"
#include "storage/ob_row_fuse.h"
#include "ob_aggregated_store.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;
namespace storage
{

ObMultipleScanMerge::ObMultipleScanMerge()
  : tree_cmp_(),
    simple_merge_(nullptr),
    loser_tree_(nullptr),
    rows_merger_(nullptr),
    iter_del_row_(false),
    consumer_cnt_(0),
    range_(NULL),
    cow_range_()
{
  type_ = ObQRIterType::T_SINGLE_SCAN;
}

ObMultipleScanMerge::~ObMultipleScanMerge()
{
}

int ObMultipleScanMerge::open(const ObDatumRange &range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!range.is_valid())) {
    STORAGE_LOG(WARN, "Invalid range, ", K(range), K(ret));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else {
    range_ = &range;
    if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    }
  }

  return ret;
}

int ObMultipleScanMerge::init(
  const ObTableAccessParam &param,
  ObTableAccessContext &context,
  const ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultipleMerge::init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "failed to init ObMultipleMerge", K(ret), K(param), K(context), K(get_table_param));
  } else {
    const ObITableReadInfo *read_info = nullptr;
    if (OB_ISNULL(read_info = access_param_->iter_param_.get_read_info())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Failed to get out col descs", K(ret));
    } else if (OB_FAIL(tree_cmp_.init(access_param_->iter_param_.get_schema_rowkey_count(),
                                      read_info->get_datum_utils(),
                                      access_ctx_->query_flag_.is_reverse_scan()))) {
      STORAGE_LOG(WARN, "init tree cmp fail", K(ret), K(access_param_->iter_param_));
    }
  }
  return ret;
}

int ObMultipleScanMerge::inner_get_next_rows()
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = nullptr;
  if (OB_UNLIKELY(consumer_cnt_ != 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument for fast iter next row", K(ret), K_(consumer_cnt));
  } else if (OB_ISNULL(iter = iters_.at(consumers_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null iter", K(ret), K(consumers_[0]));
  } else if (OB_FAIL(iter->get_next_rows())) {
    if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
      STORAGE_LOG(WARN, "Failed to get next row from iterator", K(ret));
    }
  }

  return ret;
}

int ObMultipleScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (NULL == access_ctx_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multiple scan merge not inited", K(ret));
  } else {
    if (range_ != &cow_range_) {
      cow_range_ = *range_;
      range_ = &cow_range_;
    }
    cow_range_.change_boundary(curr_rowkey_, access_ctx_->query_flag_.is_reverse_scan());
  }

  return ret;
}

int ObMultipleScanMerge::is_range_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "range is null", K(ret));
  } else if (!range_->is_valid()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultipleScanMerge::prepare()
{
  int ret = OB_SUCCESS;
  consumer_cnt_ = 0;
  return ret;
}

void ObMultipleScanMerge::collect_merge_stat(ObTableStoreStat &stat) const
{
  stat.single_scan_stat_.call_cnt_++;
  stat.single_scan_stat_.output_row_cnt_ += access_ctx_->table_store_stat_.output_row_cnt_;
}

int ObMultipleScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "range is NULL", K(ret));
  } else if (OB_UNLIKELY(iters_.count() > 0 && iters_.count() != tables_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter cnt is not equal to table cnt", K(ret), "iter cnt", iters_.count(),
        "table cnt", tables_.count(), KP(this));
  } else if (tables_.count() > 0) {
    ObITable *table = NULL;
    ObStoreRowIterator *iter = NULL;
    const ObTableIterParam *iter_pram = NULL;
    const bool use_cache_iter = iters_.count() > 0;
    const int64_t table_cnt = tables_.count()  - 1;

    if (OB_FAIL(set_rows_merger(tables_.count()))) {
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret), K(tables_));
    }

    consumer_cnt_ = 0;
    for (int64_t i = table_cnt; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(tables_.at(i, table))) {
        STORAGE_LOG(WARN, "Fail to get ith store, ", K(i), K(ret));
      } else if (OB_ISNULL(iter_pram = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get access param", K(i), K(ret), K(*table));
      } else if (!use_cache_iter) {
        if (OB_FAIL(table->scan(*iter_pram, *access_ctx_, *range_, iter))) {
          STORAGE_LOG(WARN, "Fail to get iterator", K(ret), K(i), K(*iter_pram));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret), K(i));
        }
      } else if (OB_ISNULL(iter = iters_.at(table_cnt - i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null iter", K(ret), "idx", table_cnt - i, K_(iters));
      } else if (OB_FAIL(iter->init(*iter_pram, *access_ctx_, table, range_))) {
        STORAGE_LOG(WARN, "failed to init scan iter", K(ret), "idx", table_cnt - i);
      }

      if (OB_SUCC(ret)) {
        consumers_[consumer_cnt_++] = i;
        STORAGE_LOG(TRACE, "add iter for consumer", KPC(table), KPC(access_param_));
      }
    }
  }
  return ret;
}

void ObMultipleScanMerge::reset()
{
  if (nullptr != access_ctx_ && nullptr != access_ctx_->stmt_allocator_) {
    if (nullptr != simple_merge_) {
      simple_merge_->~ObScanSimpleMerger();
      access_ctx_->stmt_allocator_->free(simple_merge_);
    }
    if (nullptr != loser_tree_) {
      loser_tree_->~ObScanMergeLoserTree();
      access_ctx_->stmt_allocator_->free(loser_tree_);
    }
  }
  simple_merge_ = nullptr;
  loser_tree_ = nullptr;
  rows_merger_ = nullptr;
  tree_cmp_.reset();
  iter_del_row_ = false;
  consumer_cnt_ = 0;
  range_ = NULL;
  cow_range_.reset();
  ObMultipleMerge::reset();
}

void ObMultipleScanMerge::reuse()
{
  ObMultipleMerge::reuse();
  iter_del_row_ = false;
  consumer_cnt_ = 0;
}

int ObMultipleScanMerge::supply_consume()
{
  int ret = OB_SUCCESS;
  ObScanMergeLoserTreeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < consumer_cnt_; ++i) {
    const int64_t iter_idx = consumers_[i];
    ObStoreRowIterator *iter = iters_.at(iter_idx);
    if (NULL == iter) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error", K(ret), K(iter));
    } else if (OB_FAIL(iter->get_next_row_ext(item.row_, item.iter_flag_))) {
      if (OB_ITER_END != ret) {
        if (OB_PUSHDOWN_STATUS_CHANGED != ret) {
          STORAGE_LOG(WARN, "failed to get next row from iterator", "index", iter_idx, "iterator", *iter);
        }
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(item.row_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get next row return NULL row", "iter_index", iter_idx, K(ret));
    } else {
      item.iter_idx_ = iter_idx;
      if (1 == consumer_cnt_) {
        if (OB_FAIL(rows_merger_->push_top(item))) {
          STORAGE_LOG(WARN, "push top error", K(ret));
        }
      } else {
        if (OB_FAIL(rows_merger_->push(item))) {
          STORAGE_LOG(WARN, "loser tree push error", K(ret));
        }
      }

      // TODO: Ambiguous here, typically base_row only means row in major sstable.
      //       And iter_idx==0 doesn't necessarily mean iterator for memtable.
      0 == iter_idx ? ++row_stat_.inc_row_count_ : ++row_stat_.base_row_count_;
    }
  }

  if (OB_SUCC(ret)) {
    // no worry, if no new items pushed, the rebuild will quickly exit
    if (rows_merger_->empty()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(rows_merger_->rebuild())) {
      STORAGE_LOG(WARN, "loser tree rebuild fail", K(ret), K(consumer_cnt_));
    } else {
      consumer_cnt_ = 0;
    }
  }
  return ret;
}

int ObMultipleScanMerge::inner_get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObScanMergeLoserTreeItem item;
  ObStoreRowIterator *iter = nullptr;
  bool final_result = false;
  bool need_supply_consume = true;

  if (OB_UNLIKELY(0 == iters_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret)) {
      STORAGE_LOG(DEBUG, "[PUSHDOWN] check condition of blockscan",
                  K(access_param_->iter_param_.pd_storage_flag_), K(consumer_cnt_));

      final_result = false;
      need_supply_consume = true;
      row.count_ = 0;
      row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);

      if (access_param_->iter_param_.enable_pd_blockscan() &&
          1 == consumer_cnt_) {
        // single consumer
        if (OB_ISNULL(iter = iters_.at(consumers_[0]))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null iter", K(ret), K_(consumer_cnt));
        } else if (iter->can_blockscan()) {
          if (OB_FAIL(iter->get_next_row_ext(item.row_, item.iter_flag_))) {
            if (OB_ITER_END == ret) {
              consumer_cnt_ = 0;
              ret = OB_SUCCESS;
            } else if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret)) {
              STORAGE_LOG(WARN, "Failed to get next row from iterator", K(ret));
            }
          } else if (!iter->can_blockscan() && !rows_merger_->empty()) {
            item.iter_idx_ = consumers_[0];
            if (OB_FAIL(rows_merger_->push_top(item))) {
              STORAGE_LOG(WARN, "push top error", K(ret));
            } else if (OB_FAIL(rows_merger_->rebuild())) {
              STORAGE_LOG(WARN, "loser tree rebuild fail", K(ret), K(consumer_cnt_));
            } else {
              consumer_cnt_ = 0;
              need_supply_consume = false;
              ++row_stat_.inc_row_count_;
            }
          } else if (OB_FAIL(ObRowFuse::fuse_row(*(item.row_), row, nop_pos_, final_result))) {
            STORAGE_LOG(WARN, "failed to merge rows", K(ret), KPC(item.row_), K(row));
          } else if (row.row_flag_.is_exist_without_delete() || (iter_del_row_ && row.row_flag_.is_delete())) {
            //success to get row directly from iterator without merge
            need_supply_consume = false;
            row.scan_index_ = item.row_->scan_index_;
            row.fast_filter_skipped_ = item.row_->fast_filter_skipped_;
            ++row_stat_.result_row_count_;
            ++row_stat_.base_row_count_;
            break;
          } else {
            //need retry
            consumer_cnt_ = 1;
            ++row_stat_.filt_del_count_;
            continue;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (need_supply_consume && OB_FAIL(supply_consume())) {
          if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
            STORAGE_LOG(WARN, "Failed to supply consume row, ", K(ret));
          }
        } else if (OB_FAIL(inner_merge_row(row))) {
          STORAGE_LOG(WARN, "Failed to inner merge row, ", K(ret));
        } else {
          //check row
          if (row.row_flag_.is_exist_without_delete() || (iter_del_row_ && row.row_flag_.is_delete())) {
            //success to get row
            ++row_stat_.result_row_count_;
            break;
          } else {
            //need retry
            ++row_stat_.filt_del_count_;
            if (0 == (row_stat_.filt_del_count_ % 10000) && !access_ctx_->query_flag_.is_daily_merge()) {
              if (OB_FAIL(THIS_WORKER.check_status())) {
                STORAGE_LOG(WARN, "query interrupt, ", K(ret));
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(range_)) {
    row.group_idx_ = range_->get_group_idx();
  }

  return ret;
}

int ObMultipleScanMerge::inner_merge_row(ObDatumRow &row)
{
  int ret = common::OB_SUCCESS;
  bool final_result = false;
  bool has_same_rowkey = false;
  bool first_row = true;
  const ObScanMergeLoserTreeItem *top_item = nullptr;

  row.count_ = 0;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  while (OB_SUCC(ret) && !rows_merger_->empty() && (has_same_rowkey || first_row)) {
    has_same_rowkey = !rows_merger_->is_unique_champion();
    if (OB_FAIL(rows_merger_->top(top_item))) {
      STORAGE_LOG(WARN, "get top item fail", K(ret));
    } else if (nullptr == top_item || nullptr == top_item->row_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "item or row is null", K(ret), KP(top_item));
    } else {
      STORAGE_LOG(DEBUG, "top_item", K(top_item->iter_idx_), K(*top_item->row_), K(row),
          K(has_same_rowkey), K(first_row), K(top_item->iter_flag_));
    }

    if (OB_SUCC(ret)) {
      // fuse the rows with the same min rowkey
      if (!final_result) {
        row.scan_index_ = top_item->row_->scan_index_;
        if (OB_FAIL(ObRowFuse::fuse_row(*(top_item->row_), row, nop_pos_, final_result))) {
          STORAGE_LOG(WARN, "failed to merge rows", K(ret), "first_row", *(top_item->row_),
              "second_row", row);
        } else if (!first_row) {
          ++row_stat_.merge_row_count_;
        }
      }

      if (OB_SUCC(ret)) {
        // record the consumer
        consumers_[consumer_cnt_++] = top_item->iter_idx_;
        if (first_row && !has_same_rowkey) {
          // row data is only from the top item
          break;
        } else {
          if (first_row) {
            first_row = false;
          }
          // make the current rowkey's next row to the top
          if (has_same_rowkey && OB_FAIL(rows_merger_->pop())) {
            STORAGE_LOG(WARN, "loser tree pop error", K(ret), KPC(rows_merger_));
          }
        }
      }
    }
  }

  // pop current rowkey's last row
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rows_merger_->pop())) {
      STORAGE_LOG(WARN, "loser tree pop error", K(ret), K(has_same_rowkey), KPC(rows_merger_));
    }
  }

  if (OB_SUCC(ret)) {
    if (access_param_->iter_param_.enable_pd_blockscan() && 1 == consumer_cnt_) {
      ObStoreRowIterator *iter = iters_.at(consumers_[0]);
      if (nullptr == iter) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null iter", K(ret), K(consumers_[0]));
      } else if (iter->is_sstable_iter()) {
        if (OB_FAIL(prepare_blockscan(*iter))) {
          STORAGE_LOG(WARN, "Failed to check blockscan", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMultipleScanMerge::can_batch_scan(bool &can_batch)
{
  int ret = OB_SUCCESS;
  can_batch = false;
  bool can_batched_agg = true;
  if (access_param_->iter_param_.enable_pd_aggregate()) {
    ObAggregatedStore *agg_row_store = reinterpret_cast<ObAggregatedStore *>(block_row_store_);
    can_batched_agg = agg_row_store->can_batched_aggregate();
  }
  if (can_batched_agg && access_param_->iter_param_.enable_pd_filter() && 1 == consumer_cnt_) {
    ObStoreRowIterator *iter = nullptr;
    if (OB_UNLIKELY(consumers_[0] >= iters_.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected iter cnt", K(ret), K(consumers_[0]), K(iters_.count()), K(*this));
    } else if (OB_ISNULL(iter = iters_.at(consumers_[0]))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null iter", K(ret), K(consumers_[0]), K(iters_), K(*this));
    } else if (iter->filter_applied()) {
      can_batch = true;
    }
  }
  return ret;
}

int ObMultipleScanMerge::prepare_blockscan(ObStoreRowIterator &iter)
{
  int ret = OB_SUCCESS;
  const ObScanMergeLoserTreeItem *top_item = nullptr;
  ObDatumRowkey rowkey;
  const int64_t rowkey_col_cnt = access_param_->iter_param_.get_schema_rowkey_count();
  if (rows_merger_->empty()) {
    if (access_ctx_->query_flag_.is_reverse_scan()) {
      rowkey.set_min_rowkey();
    } else {
      rowkey.set_max_rowkey();
    }
  } else if (OB_FAIL(rows_merger_->top(top_item))) {
    STORAGE_LOG(WARN, "Failed to get top item", K(ret));
  } else if (OB_ISNULL(top_item) || OB_ISNULL(top_item->row_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "item or row is null", K(ret), KP(top_item));
  } else if (OB_FAIL(rowkey.assign(top_item->row_->storage_datums_, rowkey_col_cnt))) {
    STORAGE_LOG(WARN, "assign rowkey failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(iter.refresh_blockscan_checker(rowkey))) {
    STORAGE_LOG(WARN, "Failed to check pushdown skip", K(ret), K(rowkey));
  }
  return ret;
}

int ObMultipleScanMerge::set_rows_merger(const int64_t table_cnt)
{
  int ret = OB_SUCCESS;
  if (table_cnt <= ObScanSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    STORAGE_LOG(DEBUG, "Use simple rows merger", K(table_cnt));
    if (nullptr == simple_merge_) {
      if (OB_ISNULL(simple_merge_ = OB_NEWx(ObScanSimpleMerger, access_ctx_->stmt_allocator_, tree_cmp_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc simple rows merger", K(ret));
      }
    }
    rows_merger_ = simple_merge_;
  } else {
    STORAGE_LOG(DEBUG, "Use loser tree", K(table_cnt));
    if (nullptr == loser_tree_) {
      if (OB_ISNULL(loser_tree_ = OB_NEWx(ObScanMergeLoserTree, access_ctx_->stmt_allocator_, tree_cmp_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc simple rows merger", K(ret));
      }
    }
    rows_merger_ = loser_tree_;
  }

  if (OB_SUCC(ret)) {
    if (!rows_merger_->is_inited()) {
      if (OB_FAIL(rows_merger_->init(table_cnt, *access_ctx_->stmt_allocator_))) {
        STORAGE_LOG(WARN, "Failed to init rows merger", K(ret), K(table_cnt));
      }
    } else if (FALSE_IT(rows_merger_->reuse())) {
    } else if (OB_FAIL(rows_merger_->open(table_cnt))) {
      STORAGE_LOG(WARN, "Failed to open rows merger", K(ret), K(table_cnt));
    }
  }
  return ret;
}


}
}
