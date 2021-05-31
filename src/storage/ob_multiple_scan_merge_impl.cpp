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

#include "ob_multiple_scan_merge_impl.h"
#include "share/ob_get_compat_mode.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;

ObMultipleScanMergeImpl::ObMultipleScanMergeImpl()
    : tree_cmp_(), loser_tree_(tree_cmp_), iter_del_row_(false), consumer_(), try_push_top_item_(false)
{}

ObMultipleScanMergeImpl::~ObMultipleScanMergeImpl()
{}

void ObMultipleScanMergeImpl::reset()
{
  ObMultipleMerge::reset();
  loser_tree_.reset();
  tree_cmp_.reset();
  iter_del_row_ = false;
  consumer_.reset();
  try_push_top_item_ = false;
}

void ObMultipleScanMergeImpl::reuse()
{
  ObMultipleMerge::reuse();
  loser_tree_.reset();
  iter_del_row_ = false;
  consumer_.reset();
  try_push_top_item_ = false;
}

int ObMultipleScanMergeImpl::init(
    const ObTableAccessParam& param, ObTableAccessContext& context, const ObGetTableParam& get_table_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultipleMerge::init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "failed to init ObMultipleMerge", K(ret), K(param), K(context), K(get_table_param));
  } else {
    ObWorker::CompatMode mode;
    const uint64_t table_id = access_param_->iter_param_.table_id_;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(extract_tenant_id(table_id), mode))) {
      STORAGE_LOG(WARN, "Failed to get compat mode", K(table_id), K(ret));
    } else if (OB_FAIL(tree_cmp_.init(access_param_->iter_param_.rowkey_cnt_,
                   access_param_->out_col_desc_param_.get_col_descs(),
                   access_ctx_->query_flag_.is_reverse_scan(),
                   (mode == ObWorker::CompatMode::ORACLE) && !is_sys_table(table_id),
                   true, /*TODO: judge index table if possible*/
                   *access_ctx_->stmt_allocator_))) {
      STORAGE_LOG(WARN, "init tree cmp fail", K(ret));
    }
  }
  return ret;
}

int ObMultipleScanMergeImpl::supply_consume()
{
  int ret = OB_SUCCESS;
  ObScanMergeLoserTreeItem item;
  const int64_t consume_num = consumer_.get_consumer_num();
  for (int64_t i = 0; OB_SUCC(ret) && i < consume_num; ++i) {
    const int64_t iter_idx = consumer_.get_consumer_iters()[i];
    ObStoreRowIterator* iter = iters_.at(iter_idx);
    if (NULL == iter) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error", K(ret), K(iter));
    } else if (OB_FAIL(iter->get_next_row_ext(item.row_, item.iter_flag_))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next row from iterator", "index", iter_idx, "iterator", *iter);
      } else {
        ret = common::OB_SUCCESS;
      }
    } else if (OB_ISNULL(item.row_)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get next row return NULL row", "iter_index", iter_idx, K(ret));
    } else {
      item.iter_idx_ = iter_idx;
      if (try_push_top_item_) {
        if (1 != consume_num) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "try_push_top_item_ mismatch", K(ret), K(consume_num));
        } else if (OB_FAIL(loser_tree_.push_top(item))) {
          STORAGE_LOG(WARN, "push top error", K(ret));
        }
        try_push_top_item_ = false;
      } else if (OB_FAIL(loser_tree_.push(item))) {
        STORAGE_LOG(WARN, "loser tree push error", K(ret));
      }

      // TODO: Ambiguous here, typically base_row only means row in major sstable.
      //       And iter_idx==0 doesn't necessarily mean iterator for memtable.
      0 == iter_idx ? ++row_stat_.inc_row_count_ : ++row_stat_.base_row_count_;
      range_purger_.on_push((int)item.iter_idx_, item.iter_flag_);
    }
  }

  if (OB_SUCC(ret)) {
    // no worry, if no new items pushed, the rebuild will quickly exit
    if (OB_FAIL(loser_tree_.rebuild())) {
      STORAGE_LOG(WARN, "loser tree rebuild fail", K(ret), K(consumer_), K(try_push_top_item_));
    } else {
      consumer_.set_consumer_num(0);
      try_push_top_item_ = false;
    }
  }
  return ret;
}

int ObMultipleScanMergeImpl::reset_range(
    int idx, int64_t range_idx, const ObStoreRowkey* rowkey, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  // purge items in loser tree
  ObScanMergeLoserTreeItem items[common::MAX_TABLE_CNT_IN_STORAGE];
  ObScanMergeLoserTreeItem gap_item;
  ObStoreRow row;
  row.row_val_.assign(const_cast<ObObj*>(rowkey->get_obj_ptr()), rowkey->get_obj_cnt());
  row.scan_index_ = range_idx;
  row.flag_ = ObActionFlag::OP_ROW_EXIST;
  gap_item.iter_idx_ = idx;
  gap_item.row_ = &row;
  int64_t remain_item = 0;
  bool compare_result = false;
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
  const int64_t tables_cnt = tables.count();
  const ObScanMergeLoserTreeItem* top_item = nullptr;
  while (OB_SUCC(ret) && !loser_tree_.empty()) {
    if (OB_FAIL(loser_tree_.top(top_item))) {
      STORAGE_LOG(WARN, "get loser tree top item fail", K(ret));
    } else if (nullptr == top_item || nullptr == top_item->row_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "item or row is null", K(ret), KP(top_item));
    } else {
      const bool is_memtable = tables.at(tables_cnt - top_item->iter_idx_ - 1)->is_memtable();
      if (top_item->iter_idx_ < idx ||
          (!is_memtable &&
              (compare_result = (tree_cmp_(gap_item, *top_item) < 0 || (!include_gap_key && 0 == compare_result))) &&
              OB_SUCCESS == tree_cmp_.get_error_code())) {
        items[remain_item++] = *top_item;
      }
      if (OB_FAIL(tree_cmp_.get_error_code())) {
        STORAGE_LOG(WARN,
            "compare item fail",
            K(ret),
            K(idx),
            K(gap_item.iter_idx_),
            K(top_item->iter_idx_),
            K(*gap_item.row_),
            K(*top_item->row_));
      } else if (OB_FAIL(loser_tree_.pop())) {
        STORAGE_LOG(WARN, "pop loser tree fail", K(ret), K(top_item->iter_idx_), K(*top_item->row_));
      } else {
        STORAGE_LOG(DEBUG,
            "reset range compare",
            K(*gap_item.row_),
            K(*top_item->row_),
            K(compare_result),
            K(top_item->iter_idx_),
            K(idx),
            K(is_memtable));
      }
    }
  }
  for (int64_t i = remain_item - 1; OB_SUCC(ret) && i >= 0; --i) {
    STORAGE_LOG(DEBUG, "push left item", K(*items[i].row_));
    ret = loser_tree_.push(items[i]);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(loser_tree_.rebuild())) {
      STORAGE_LOG(WARN, "rebuild loser tree fail", K(ret));
    } else {
      consumer_.remove_le(idx);
    }
  }

  // reset iter
  for (int64_t i = iters_.count() - 1; OB_SUCC(ret) && i >= idx; i--) {
    if (OB_FAIL(iters_[i]->skip_range(range_idx, rowkey, include_gap_key))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "skip range fail", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool found = false;
      for (int64_t j = 0; j < remain_item && !found; ++j) {
        found = items[j].iter_idx_ == i;
      }
      if (!found) {
        consumer_.add_consumer(i);
      }
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "consumer info", K(consumer_));
  }
  return ret;
}

int ObMultipleScanMergeImpl::try_skip_range(const ObStoreRow* row, int idx, uint8_t flag, bool first_pop, bool& skipped)
{
  int ret = OB_SUCCESS;
  const bool is_del = ObActionFlag::OP_DEL_ROW == row->flag_;
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
  if (first_pop) {
    memtable::ObIMemtableCtx* mem_ctx = access_ctx_->store_ctx_->mem_ctx_;
    if (OB_ISNULL(mem_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "memtable context is null, unexpected error", K(ret), KP(mem_ctx));
    } else {
      bool read_elr_data = mem_ctx->has_read_elr_data();
      if (ObClockGenerator::getClock() - row->last_purge_ts_ > 5000000) {
        range_purger_.try_purge(row->scan_index_, idx, is_del, read_elr_data);
        const_cast<ObStoreRow*>(row)->last_purge_ts_ = ObClockGenerator::getClock();
      }
    }
  }
  if (OB_SUCC(ret) && is_del && range_skip_.is_enabled()) {
    const int64_t limit = 256;
    int64_t range_idx = -1;
    const ObStoreRowkey* gap_key = NULL;
    if (idx < 0 || idx >= tables.count()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), K(idx), K(tables.count()));
    } else {
      const bool is_memtable = tables.at(tables.count() - idx - 1)->is_memtable();
      if (OB_FAIL(range_skip_.inspect_gap(idx, is_memtable, flag, range_idx, gap_key, limit))) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(reset_range(idx, range_idx, gap_key, true /*include gap key*/))) {
        STORAGE_LOG(WARN, "skip_gap fail", K(ret));
      } else {
        range_purger_.skip_range(idx);
        skipped = true;
      }
    }
  }
  return ret;
}

int ObMultipleScanMergeImpl::prepare_range_skip()
{
  int ret = OB_SUCCESS;
  memtable::ObIMemtableCtx* mem_ctx = access_ctx_->store_ctx_->mem_ctx_;
  ObITable* last_table = NULL;
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
  if (tables.count() > 0) {
    if (OB_FAIL(tables.at(tables.count() - 1, last_table))) {
      last_table = NULL;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, there is no tables, ", K(ret), K(tables.count()));
  }

  if (NULL != last_table) {
    const bool skip_switch = true;
    bool enable_skip = skip_switch && (access_param_->iter_param_.table_id_ & 0xffffff) > 50000 &&
                       !access_ctx_->query_flag_.is_whole_macro_scan();
    bool enable_purge = skip_switch && NULL != mem_ctx && mem_ctx->get_read_snapshot() < INT64_MAX - 1024 &&
                        mem_ctx->get_read_snapshot() > last_table->get_base_version();
    if (enable_skip) {
      range_skip_.init(&iters_);
    } else {
      range_skip_.reset();
    }
    if (enable_purge) {
      range_purger_.init(tables, iters_);
    } else {
      range_purger_.reset();
    }
  }
  return ret;
}

int ObMultipleScanMergeImpl::inner_get_next_row(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  if (OB_UNLIKELY(0 == iters_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && need_retry) {
      if (OB_FAIL(supply_consume())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "supply consume failed", K(ret));
        }
      } else if (is_scan_end()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(ObMultipleScanMergeImpl::inner_get_next_row(row, need_retry))) {
        STORAGE_LOG(WARN, "fail to inner get next row from ObMultipleScanMergeHelper", K(ret));
      } else {
        // succeed to get next row
      }
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "scan_merge: get_next_row", K(row));
    row.range_array_idx_ = 0;
  }

  return ret;
}

int ObMultipleScanMergeImpl::inner_get_next_row(ObStoreRow& row, bool& need_retry)
{
  int ret = common::OB_SUCCESS;
  bool final_result = false;
  bool has_same_rowkey = false;
  bool first_row = true;
  const ObScanMergeLoserTreeItem* top_item = nullptr;
  bool range_skip = false;

  need_retry = false;
  row.row_val_.count_ = 0;
  row.flag_ = common::ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  row.from_base_ = false;
  while (OB_SUCC(ret) && !loser_tree_.empty() && (has_same_rowkey || first_row)) {
    has_same_rowkey = !loser_tree_.is_unique_champion();
    if (OB_FAIL(loser_tree_.top(top_item))) {
      STORAGE_LOG(WARN, "get top item fail", K(ret));
    } else if (nullptr == top_item || nullptr == top_item->row_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "item or row is null", K(ret), KP(top_item));
    } else {
      STORAGE_LOG(DEBUG,
          "top_item",
          K(top_item->iter_idx_),
          K(*top_item->row_),
          K(row),
          K(has_same_rowkey),
          K(first_row),
          K(top_item->iter_flag_));
    }

    if (OB_SUCC(ret)) {
      range_skip = false;
      if ((!iter_del_row_ &&
              OB_FAIL(try_skip_range(
                  top_item->row_, (int)top_item->iter_idx_, top_item->iter_flag_, first_row, range_skip))) ||
          range_skip) {
        break;
      }

      // fuse the rows with the same min rowkey
      if (!final_result) {
        row.scan_index_ = top_item->row_->scan_index_;
        if (OB_FAIL(ObRowFuse::fuse_row(*(top_item->row_), row, nop_pos_, final_result))) {
          STORAGE_LOG(WARN, "failed to merge rows", K(ret), "first_row", *(top_item->row_), "second_row", row);
        } else if (!first_row) {
          ++row_stat_.merge_row_count_;
        }
        if (OB_UNLIKELY(iter_del_row_) && OB_SUCC(ret) && common::ObActionFlag::OP_DEL_ROW == row.flag_) {
          // set delete row cells if we need iterate delete rows
          row.row_val_.count_ = top_item->row_->row_val_.count_;
          for (int64_t i = 0; i < row.row_val_.count_; ++i) {
            row.row_val_.cells_[i] = (nullptr == out_cols_projector_)
                                         ? top_item->row_->row_val_.cells_[i]
                                         : top_item->row_->row_val_.cells_[out_cols_projector_->at(i)];
          }
        }
      }

      if (OB_SUCC(ret)) {
        // record the consumer
        consumer_.add_consumer(top_item->iter_idx_);
        if (first_row && !has_same_rowkey) {
          // row data is only from the top item
          try_push_top_item_ = true;
          break;
        } else {
          if (first_row) {
            first_row = false;
          }
          // make the current rowkey's next row to the top
          if (has_same_rowkey && OB_FAIL(loser_tree_.pop())) {
            STORAGE_LOG(WARN, "loser tree pop error", K(ret));
          }
        }
      }
    }
  }

  // pop current rowkey's last row
  if (OB_SUCC(ret) && !range_skip) {
    if (OB_FAIL(loser_tree_.pop())) {
      STORAGE_LOG(WARN, "loser tree pop error", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (common::ObActionFlag::OP_ROW_EXIST == row.flag_ ||
        (iter_del_row_ && common::ObActionFlag::OP_DEL_ROW == row.flag_)) {
      ++row_stat_.result_row_count_;
    } else {
      need_retry = true;
      ++row_stat_.filt_del_count_;
      if (0 == (row_stat_.filt_del_count_ % 10000) && !access_ctx_->query_flag_.is_daily_merge()) {
        if (OB_FAIL(THIS_WORKER.check_status())) {
          STORAGE_LOG(WARN, "query interrupt, ", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMultipleScanMergeImpl::prepare_loser_tree()
{
  int ret = common::OB_SUCCESS;
  try_push_top_item_ = false;
  loser_tree_.reset();
  return ret;
}
