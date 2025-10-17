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
#include "ob_hbase_cf_iterator.h"
#include "share/ob_ls_id.h"
#include "ob_hbase_tablet_merge_iterator.h"
#include "share/table/ob_table_util.h"

namespace oceanbase
{
namespace table
{

////////////////////////////////////////////////////////////////
ObHbaseExplicitColumnTracker::ObHbaseExplicitColumnTracker()
    :columns_(),
     curr_column_idx_(0),
     curr_column_(NULL)
{}

int ObHbaseExplicitColumnTracker::init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family)
{
  int ret = OB_SUCCESS;
  max_versions_ = htable_filter.get_max_versions();
  const ObIArray<ObString> &qualifiers = htable_filter.get_columns();
  const int64_t N = qualifiers.count();
  if (OB_SUCC(ret) && N <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not use ExplicitColumnTracker", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObString real_qualifier = qualifier_with_family ? qualifiers.at(i).after('.') : qualifiers.at(i);
    if (OB_FAIL(columns_.push_back(std::make_pair(real_qualifier, 0)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }  // end for
  if (OB_SUCC(ret)) {
    // sort qualifiers
    ColumnCount *end = &columns_.at(columns_.count() - 1);
    ++end;
    lib::ob_sort(&columns_.at(0), end, ObHTableColumnTracker::ColumnCountComparator());
  }

  if (OB_SUCC(ret)) {
    // check duplicated qualifiers
    for (int64_t i = 0; OB_SUCC(ret) && i < N - 1; ++i) {
      if (columns_.at(i).first == columns_.at(i + 1).first) {
        ret = OB_ERR_PARAM_DUPLICATE;
        LOG_WARN("duplicated qualifiers", K(ret), "cq", columns_.at(i).first, K(i));
      }
    }  // end for
  }
  return ret;
}

bool ObHbaseExplicitColumnTracker::done() const
{
  return curr_column_idx_ >= columns_.count();
}

int ObHbaseExplicitColumnTracker::check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  do {
    // No more columns left, we are done with this query
    if (done()) {
      match_code = ObHTableMatchCode::SEEK_NEXT_ROW;  // done row
      break;
    }
    // No more columns to match against, done with storefile
    // @todo here
    // Compare specific column to current column
    //
    cmp_ret = ObHTableUtils::compare_qualifier(cell.get_qualifier(), curr_column_->first);

    // Column Matches. Return include code. The caller would call checkVersions
    // to limit the number of versions.
    if (0 == cmp_ret) {
      match_code = ObHTableMatchCode::INCLUDE;
      break;
    }
    // reset_timestamp();
    if (cmp_ret < 0) {
      // The current KV is smaller than the column the ExplicitColumnTracker
      // is interested in, so seek to that column of interest.
      match_code = ObHTableMatchCode::SEEK_NEXT_COL;
      break;
    }
    // The current KV is bigger than the column the ExplicitColumnTracker
    // is interested in. That means there is no more data for the column
    // of interest. Advance the ExplicitColumnTracker state to next
    // column of interest, and check again.
    if (cmp_ret > 0) {
      ++curr_column_idx_;
      if (done()) {
        match_code = ObHTableMatchCode::SEEK_NEXT_ROW;  // done row
        break;
      }
      curr_column_ = &columns_.at(curr_column_idx_);
      // continue for next column of interest
    }
  } while (true);
  return ret;
}

int ObHbaseExplicitColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int32_t count = ++curr_column_->second;
  int64_t timestamp = cell.get_timestamp();
  LOG_DEBUG("check versions", K(count), K_(max_versions), K(timestamp),
            K_(min_versions), K_(oldest_stamp), "is_expired", is_expired(timestamp));
  // in reverse scan, check the cell timeout condition with ttl. Kick off the timeout cell from res.
  if (count >= max_versions_ || (is_expired(timestamp) && count >= min_versions_)) {
        // Done with versions for this column
    ++curr_column_idx_;
    if (done()) {
      match_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW;  // done row
    } else {
      // We are done with current column; advance to next column
      // of interest.
      curr_column_ = &columns_.at(curr_column_idx_);
      match_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL;
      column_has_expired_ = true;
    }
  } else {
    match_code = ObHTableMatchCode::INCLUDE;
  }
  return ret;
}

ObHTableMatchCode ObHbaseExplicitColumnTracker::check_version(int64_t timestamp)
{
  ObHTableMatchCode match_code = ObHTableMatchCode::INCLUDE;
  if (!is_expired(timestamp)) {
    // keep the KV if required by minversions or it is not expired, yet
    match_code = ObHTableMatchCode::INCLUDE;
  } else {
    match_code = ObHTableMatchCode::SEEK_NEXT_COL;
  }
  return match_code;
}

// Called between every row.
void ObHbaseExplicitColumnTracker::reset()
{
  curr_column_idx_ = 0;
  column_has_expired_ = false;
  curr_column_ = &columns_.at(curr_column_idx_);
  const int64_t N = columns_.count();
  for (int64_t i = 0; i < N; ++i) {
    columns_.at(i).second = 0;
  }  // end for
}

void ObHbaseExplicitColumnTracker::done_with_column(const ObHTableCell &cell)
{
  while (NULL != curr_column_) {
    int cmp_ret = ObHTableUtils::compare_qualifier(cell.get_qualifier(), curr_column_->first);
    if (cmp_ret < 0) {
      break;
    } else {
      ++curr_column_idx_;
      if (done()) {
        curr_column_ = NULL;
      } else {
        curr_column_ = &columns_.at(curr_column_idx_);
      }
      if (0 == cmp_ret) {
        break;
      }
    }
  }
}

int ObHbaseExplicitColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  done_with_column(cell);
  if (NULL == curr_column_) {
    match_code = ObHTableMatchCode::SEEK_NEXT_ROW;
  } else {
    match_code = ObHTableMatchCode::SEEK_NEXT_COL;
  }
  return OB_SUCCESS;
}

int32_t ObHbaseExplicitColumnTracker::get_cur_version()
{
  int32_t cur_version = 0;
  if (OB_NOT_NULL(curr_column_)) {
    cur_version = curr_column_->second;
  }
  return cur_version;
}

////////////////////////////////////////////////////////////////
ObHbaseWildcardColumnTracker::ObHbaseWildcardColumnTracker()
    :allocator_(ObMemAttr(MTL_ID(), "HbaseWildColTrk")),
     current_qualifier_(),
     current_count_(0)
{}

int ObHbaseWildcardColumnTracker::init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family)
{
  int ret = OB_SUCCESS;
  max_versions_ = htable_filter.get_max_versions();
  // @todo set min_versions_ and oldest_stamp_
  return ret;
}

int ObHbaseWildcardColumnTracker::check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::INCLUDE;
  return OB_SUCCESS;
}

int ObHbaseWildcardColumnTracker::reset_cell(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  current_count_ = 0;
  allocator_.reuse();
  if (OB_FAIL(ob_write_string(allocator_, cell.get_qualifier(), current_qualifier_))) {
    LOG_WARN("failed to copy qualifier", K(ret));
  }
  return ret;
}

int ObHbaseWildcardColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int cmp_ret;
  if (current_qualifier_.empty()) {
    // first iteration
    if (OB_FAIL(reset_cell(cell))) {
      LOG_WARN("fail to reset cell", K(ret), K(cell));
    } else {
      match_code = check_version(cell.get_timestamp());
    }
  } else {
    cmp_ret = ObHTableUtils::compare_qualifier(cell.get_qualifier(), current_qualifier_);
    if (0 == cmp_ret) {
      match_code = check_version(cell.get_timestamp());
    } else {
      // a new qualifier
      if (OB_FAIL(reset_cell(cell))) {
        LOG_WARN("fail to reset cell", K(ret), K(cell));
      } else {
        match_code = check_version(cell.get_timestamp());
      }
    }
  }
  return ret;
}

ObHTableMatchCode ObHbaseWildcardColumnTracker::check_version(int64_t timestamp)
{
  ObHTableMatchCode match_code = ObHTableMatchCode::INCLUDE;
  ++current_count_;
  if (current_count_ > max_versions_) {
    match_code = ObHTableMatchCode::SEEK_NEXT_COL;
    column_has_expired_ = true;
  } else if (current_count_ <= min_versions_
            || !is_expired(timestamp)) {
    // keep the KV if required by minversions or it is not expired, yet
    match_code = ObHTableMatchCode::INCLUDE;
  } else {
    match_code = ObHTableMatchCode::SEEK_NEXT_COL;
    column_has_expired_ = true;
  }
  return match_code;
}

int ObHbaseWildcardColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::SEEK_NEXT_COL;
  return OB_SUCCESS;
}

void ObHbaseWildcardColumnTracker::reset()
{
  LOG_DEBUG("reset qualifier");
  column_has_expired_ = false;
  current_qualifier_.reset();
}

int32_t ObHbaseWildcardColumnTracker::get_cur_version()
{
  return current_count_;
}

/**
 * ---------------------------------------- ObHBaseRowIterator ----------------------------------------
 */
ObHbaseRowIterator::ObHbaseRowIterator(const ObHbaseQuery &hbase_query, ObTableExecCtx &exec_ctx, bool is_timeseries_table)
    : allocator_("HbaseRowItAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      forward_range_alloc_("HbaseForwRanAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      hbase_query_(hbase_query),
      cell_iter_(NULL),
      curr_cell_(),
      scan_order_(hbase_query.get_query().get_scan_order()),
      matcher_(NULL),
      has_more_cells_(true),
      is_inited_(false),
      need_verify_cell_ttl_(false),
      forward_rescan_param_(),
      hbase_adapter_guard_(exec_ctx.get_allocator()),
      exec_ctx_(exec_ctx),
      htable_filter_(hbase_query.get_query().get_htable_filter()),
      hfilter_(NULL),
      limit_per_row_per_cf_(htable_filter_.get_max_results_per_column_family()),
      offset_per_row_per_cf_(htable_filter_.get_row_offset_per_column_family()),
      max_result_size_(hbase_query.get_query().get_max_result_size()),
      batch_size_(hbase_query.get_query().get_batch()),
      time_to_live_(0),
      max_version_(0),
      column_tracker_(NULL),
      column_tracker_wildcard_(),
      column_tracker_explicit_(),
      ts_column_tracker_wildcard_(),
      ts_column_tracker_explicit_(),
      matcher_impl_(htable_filter_),
      cell_count_(0),
      count_per_row_(0),
      is_first_result_(true),
      family_name_(),
      is_cur_row_expired_(false),
      allow_partial_results_(false),
      is_cache_block_(true),
      scanner_context_(NULL),
      tablet_ids_(),
      is_timeseries_table_(is_timeseries_table)
{
  tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "HbRowIterTbtIds"));
  if (hbase_query.get_query().get_scan_ranges().count() != 0) {
    start_row_key_ = hbase_query.get_query().get_scan_ranges().at(0).start_key_;
    stop_row_key_ = hbase_query.get_query().get_scan_ranges().at(0).end_key_;
  }
}

int ObHbaseRowIterator::get_and_init_cell_iter(ObIAllocator &allocator,
                                               ObTableExecCtx &ctx,
                                               const ObTableQuery &query,
                                               ObHbaseICellIter *&cell_iter)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTabletID> &tablet_ids = query.get_tablet_ids();
  int64_t tablet_cnt = tablet_ids.count();
  ObHbaseICellIter *tmp_cell_iter = nullptr;

  if (tablet_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet ids count", K(ret), K(tablet_cnt));
  } else if (tablet_cnt == 1) {
    ObIHbaseAdapter *hbase_adapter = nullptr;
    if (OB_FAIL(hbase_adapter_guard_.get_hbase_adapter(hbase_adapter,
        exec_ctx_.get_schema_cache_guard().get_hbase_mode_type()))) {
      LOG_WARN("fail to get hbase adapter", K(ret));
    } else if (OB_FAIL(hbase_adapter->scan(exec_ctx_.get_allocator(), exec_ctx_, query, tmp_cell_iter))) {
      LOG_WARN("fail to scan", K(ret), K(query));
    } else if (OB_FAIL(tmp_cell_iter->open())) {
      LOG_WARN("fail to open cell iter", K(ret));
    }
  } else if (!is_timeseries_table_) {
    tmp_cell_iter = OB_NEWx(ObHbaseTabletMergeIterator, &allocator, exec_ctx_, query);
    if (OB_ISNULL(tmp_cell_iter)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(tmp_cell_iter->open())) {
      LOG_WARN("fail to open tablet merge iterator", K(ret));
    }
  } else {
    tmp_cell_iter = OB_NEWx(ObHbaseTSTabletMergeIter, &allocator, exec_ctx_, query);
    if (OB_ISNULL(tmp_cell_iter)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(tmp_cell_iter->open())) {
      LOG_WARN("fail to open tablet merge iterator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    cell_iter = tmp_cell_iter;
  } else if (OB_NOT_NULL(tmp_cell_iter)) {
    tmp_cell_iter->close();
    tmp_cell_iter->~ObHbaseICellIter();
  }

  LOG_DEBUG("finish init cell iter for hbase row iter",
    K(ret), K(tablet_cnt), KP(tmp_cell_iter), K_(hbase_query), K_(is_timeseries_table));

  return ret;
}

int ObHbaseRowIterator::init(ScannerContext &scanner_context, const ObHBaseParams *hbase_params, hfilter::Filter *filter)
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = hbase_query_.get_query();
  ObKvSchemaCacheGuard &schema_cache_guard = exec_ctx_.get_schema_cache_guard();
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  ObKVAttr kv_attributes;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHbaseRowIterator has been inited", K(ret));
  } else if (!schema_cache_guard.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_cache_cache is not inited", K(ret));
  } else if (OB_FAIL(schema_cache_guard.get_kv_attributes(kv_attributes))) {
    LOG_WARN("get kv attributes failed", K(ret));
  } else {
    ObHColumnDescriptor desc;
    desc.from_kv_attribute(kv_attributes);
    if (desc.get_time_to_live() > 0) {
      time_to_live_ = desc.get_time_to_live();
    }
    if (desc.get_max_version() > 0) {
      max_version_ = desc.get_max_version();
    }
    if ((select_columns.empty() ||
            ObTableUtils::has_exist_in_columns(select_columns, ObHTableConstants::TTL_CNAME_STR)) &&
        schema_cache_guard.get_schema_flags().has_hbase_ttl_column_) {
      set_need_verify_cell_ttl(true);
    }
  }

  if (OB_SUCC(ret)) {
    scanner_context_ = &scanner_context;
    set_hfilter(filter);
    if (OB_NOT_NULL(hbase_params)) {
      allow_partial_results_ = hbase_params->allow_partial_results_;
      is_cache_block_ = hbase_params->is_cache_block_;
    }
  }

  if (OB_SUCC(ret)) {
    if (hbase_query_.get_table_name().empty()) { // to avoid set table name twice, maybe set at multi-cf init
      const_cast<ObHbaseQuery &>(hbase_query_).set_table_name(schema_cache_guard.get_table_name_str());
    }
    init_family_name();
    const ObTableQuery &query = hbase_query_.get_query();
    if (OB_FAIL(tablet_ids_.assign(query.get_tablet_ids()))) {
      LOG_WARN("fail to assign tablet ids", K(ret), K(query.get_tablet_ids()));
    } else {
      ObHbaseICellIter *tmp_cell_iter = nullptr;
      if (OB_FAIL(get_and_init_cell_iter(exec_ctx_.get_allocator(), exec_ctx_, query, tmp_cell_iter))) {
        LOG_WARN("fail to scan", K(ret), K(query));
      } else if (OB_ISNULL(tmp_cell_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cell iter is NULL", K(ret));
      } else {
        cell_iter_ = tmp_cell_iter;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObHbaseRowIterator::close()
{
  if (OB_NOT_NULL(cell_iter_)) {
    cell_iter_->close();
  }

  is_inited_ = false;
}

int ObHbaseRowIterator::next_cell()
{
  ObNewRow *cell = NULL;
  int ret = cell_iter_->get_next_cell(cell);
  LOG_DEBUG("fetch cell from cell iter", K(cell));
  if (OB_SUCCESS == ret) {
    curr_cell_.set_ob_row(cell);
    if (OB_FAIL(try_record_expired_rowkey(curr_cell_))) {
      LOG_WARN("failed to record expired rowkey", K(ret));
    }
    LOG_DEBUG("fetch next cell", K_(curr_cell));
  } else if (OB_ITER_END == ret) {
    has_more_cells_ = false;
    curr_cell_.set_ob_row(NULL);
    matcher_->clear_curr_row();
    LOG_DEBUG("iterator end", K_(has_more_cells));
  } else {
    LOG_WARN("fail to get next cell", K(ret));
  }
  return ret;
}

int ObHbaseRowIterator::get_next_cell_hint()
{
  int ret = OB_SUCCESS;
  ObHTableCell *hint_cell = nullptr;
  if (OB_FAIL(matcher_->get_next_cell_hint(curr_cell_, hint_cell))) {
    LOG_WARN("failed to get next hint cell of filter", K(ret), K_(curr_cell));
  } else {
    if (hint_cell != NULL) {
      int32_t cur_version = column_tracker_->get_cur_version();
      bool enter_hint_key = false;
      ObHTableCell *next_cell = nullptr;
      ObHTableCell *tmp_cell = nullptr;
      while (OB_SUCC(ret) && !enter_hint_key) {
        if (ObHTableUtils::compare_cell(curr_cell_, *hint_cell, false) >= 0) {
          enter_hint_key = true;
          LOG_INFO("curr_cell is bigger than hint_cell", K(ret), K(curr_cell_), K(hint_cell));
        } else if (OB_FAIL(ObHTableUtils::create_last_cell_on_row_col(allocator_, curr_cell_, next_cell))) {
          LOG_WARN("failed to create last cell", K(ret));
        } else {
          int32_t skipped_count = 0;
          if (ObHTableUtils::compare_cell(*next_cell, *hint_cell, false) >= 0) {
            enter_hint_key = true;  // curr_cell's rowkey and qualifier is equal to hint
            tmp_cell = hint_cell;
          } else {
            tmp_cell = next_cell;
          }
          if (OB_FAIL(seek(*tmp_cell, skipped_count))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to seek next cell", K(ret));
            }
          }

          if ((OB_SUCCESS == ret || OB_ITER_END == ret)) {
            // need record current rowkey and mark it recorded
            // record using the next_cell, because the curr_cell_
            // is not accessible when return OB_ITER_END
            try_record_expired_rowkey(cur_version + skipped_count, tmp_cell->get_rowkey());
          }
          cur_version = 1;
          next_cell = nullptr;
          allocator_.reuse();
        }
      }
      hint_cell->~ObHTableCell();
    } else if (OB_FAIL(next_cell())) {
      LOG_WARN("failed to get next cell", K(ret));
    }
  }
  return ret;
}

int ObHbaseRowIterator::get_next_row(ObTableQueryDListResult *&out_result)
{
  int ret = OB_SUCCESS;
  one_iterable_hbase_row_.reset();
  out_result = &one_iterable_hbase_row_;
  if (OB_FAIL(get_next_row_internal(out_result))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

int ObHbaseRowIterator::get_next_row(ObTableQueryResult *&out_result)
{
  int ret = OB_SUCCESS;
  one_hbase_row_.reset();
  out_result = &one_hbase_row_;
  if (OB_FAIL(get_next_row_internal(out_result))) {
    LOG_WARN("fail to get next row", K(ret));
  }
  return ret;
}

template <typename ResultType>
int ObHbaseRowIterator::get_next_row_internal(ResultType *&result)
{
  int ret = OB_SUCCESS;
  bool has_filter_row = (NULL != hfilter_) && (hfilter_->has_filter_row());
  if (scanner_context_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scanner_context_ meet unexecpted nullptr", K(ret));
  }
  if (OB_SUCC(ret) && NULL == column_tracker_) {
    // first iteration
    if (htable_filter_.get_columns().count() <= 0 || hbase_query_.use_wildcard_column_tracker()) {
      if (!is_timeseries_table_) {
        column_tracker_ = &column_tracker_wildcard_;
      } else {
        column_tracker_ = &ts_column_tracker_wildcard_;
      }
    } else {
      if (!is_timeseries_table_) {
        column_tracker_ = &column_tracker_explicit_;
      } else {
        column_tracker_ = &ts_column_tracker_explicit_;
      }
    }
    if (OB_FAIL(column_tracker_->init(htable_filter_, hbase_query_.get_qualifier_with_family()))) {
      LOG_WARN("failed to init column tracker", K(ret), K(htable_filter_), K(hbase_query_));
    } else {
      if (time_to_live_ > 0) {
        column_tracker_->set_ttl(time_to_live_);
      }
      if (max_version_ > 0) {
        int32_t real_max_version = std::min(column_tracker_->get_max_version(), max_version_);
        column_tracker_->set_max_version(real_max_version);
      }
    }
  }
  if (OB_SUCC(ret) && NULL == matcher_) {
    matcher_ = &matcher_impl_;
    matcher_->init(column_tracker_, hfilter_);
  }
  if (OB_SUCC(ret) && NULL == curr_cell_.get_ob_row()) {
    ret = next_cell();
  }
  if (OB_SUCC(ret) && matcher_->is_curr_row_empty()) {
    count_per_row_ = 0;
    ret = matcher_->set_to_new_row(curr_cell_);
  }
  if (OB_SUCC(ret)) {
    if (has_filter_row) {
      scanner_context_->limits_.set_size_scope(LimitScope::Scope::BETWEEN_ROWS);
      scanner_context_->limits_.set_time_scope(LimitScope::Scope::BETWEEN_ROWS);
    } else if (allow_partial_results_) {
      scanner_context_->limits_.set_size_scope(LimitScope::Scope::BETWEEN_CELLS);
      scanner_context_->limits_.set_time_scope(LimitScope::Scope::BETWEEN_CELLS);
    }
  }
  if (OB_SUCC(ret)) {
    if (NULL == matcher_->get_curr_row()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("matcher should have valid first cell", K(ret));
    } else if (NULL != hfilter_) {
      const ObHTableCell &first_cell = curr_cell_;
      bool filtered = false;
      if (OB_FAIL(hfilter_->filter_row_key(first_cell, filtered))) {
        LOG_WARN("failed to filter row key", K(ret), K(first_cell));
      } else if (filtered) {
        // filter out the current row and fetch the next row
        hfilter_->reset();
        LOG_DEBUG("filter_row_key skip the row", K(ret));
        ret = seek_or_skip_to_next_row(curr_cell_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_timeseries_table_) {
      if (OB_FAIL(get_next_row_internal_normal(result))) {
        LOG_WARN("fail to get next row internal normal", K(ret));
      }
    } else {
      if (OB_FAIL(get_next_row_internal_timeseries(result))) {
        LOG_WARN("fail to get next row internal timeseries", K(ret));
      }
    }
  }

  return ret;
}


template <typename ResultType>
int ObHbaseRowIterator::get_next_row_internal_normal(ResultType *&result)
{
  int ret = OB_SUCCESS;
  bool loop = true;
  ObHTableMatchCode match_code = ObHTableMatchCode::DONE_SCAN;  // initialize
  while (OB_SUCC(ret) && loop) {
    match_code = ObHTableMatchCode::DONE_SCAN;
    curr_cell_.set_family(family_name_);
    if (OB_FAIL(matcher_->match(curr_cell_, match_code))) {
      LOG_WARN("failed to match cell", K(ret));
    } else {
      if (NULL == curr_cell_.get_ob_row()) {
        LOG_DEBUG("matcher, curr_cell=NULL ", K(match_code));
      } else {
        LOG_DEBUG("matcher", K_(curr_cell), K(match_code));
      }
      switch (match_code) {
        case ObHTableMatchCode::INCLUDE:
        case ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW:
        case ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL:
          if (NULL != hfilter_) {
            const ObHTableCell *new_cell = nullptr;
            if (OB_FAIL(hfilter_->transform_cell(allocator_, curr_cell_))) {
              LOG_WARN("failed to tranform cell", K(ret));
              break;
            }
          }
          ++count_per_row_;
          // whether reach per row limit
          if (limit_per_row_per_cf_ > -1 /*not unlimited*/
              && count_per_row_ > (offset_per_row_per_cf_ + limit_per_row_per_cf_)) {
            ret = seek_or_skip_to_next_row(curr_cell_);
            loop = false;
            LOG_DEBUG(
                "reach per row limit", K(ret), K_(offset_per_row_per_cf), K_(limit_per_row_per_cf), K_(count_per_row));
            break;
          }
          // whether skip offset
          if (count_per_row_ > offset_per_row_per_cf_) {
            int64_t timestamp = 0;
            if (OB_FAIL(curr_cell_.get_ob_row()->get_cell(ObHTableConstants::COL_IDX_T).get_int(timestamp))) {
              LOG_WARN("failed to get timestamp", K(ret));
            } else {
              const_cast<ObNewRow*>(curr_cell_.get_ob_row())->get_cell(ObHTableConstants::COL_IDX_T).set_int(-timestamp);
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(add_result_cell(*(curr_cell_.get_ob_row()), result))) {
                LOG_WARN("failed to add cell to result", K(ret));
              } else {
                scanner_context_->increment_batch_progress(1);
                scanner_context_->increment_size_progress(curr_cell_.get_ob_row()->get_serialize_size());
                ++cell_count_;
                LOG_DEBUG(
                    "add cell", K_(cell_count), K_(curr_cell), K_(count_per_row), K_(offset_per_row_per_cf));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW == match_code) {
              ret = seek_or_skip_to_next_row(curr_cell_);
            } else if (ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL == match_code) {
              ret = seek_or_skip_to_next_col(curr_cell_);
            } else {
              ret = next_cell();
            }
            if (OB_SUCC(ret)) {
              if (scanner_context_->check_any_limit(LimitScope::Scope::BETWEEN_CELLS)) {
                loop = false;
              }
            } else if (OB_ITER_END == ret) {
              loop = false;
            }
          }
          break;
        case ObHTableMatchCode::DONE:
          // done current row
          matcher_->clear_curr_row();
          loop = false;
          break;
        case ObHTableMatchCode::DONE_SCAN:
          has_more_cells_ = false;
          loop = false;
          // need to scan the last kq for recording expired rowkey
          // when scan return OB_ITER_END, cur_cell_ will be empty
          // but the ret code will be covered
          if (NULL != curr_cell_.get_ob_row()) {
            ret = seek_or_skip_to_next_col(curr_cell_);
          }
          break;
        case ObHTableMatchCode::SEEK_NEXT_ROW:
          ret = seek_or_skip_to_next_row(curr_cell_);
          break;
        case ObHTableMatchCode::SEEK_NEXT_COL:
          ret = seek_or_skip_to_next_col(curr_cell_);
          break;
        case ObHTableMatchCode::SKIP:
          ret = next_cell();
          break;
        case ObHTableMatchCode::SEEK_NEXT_USING_HINT:
          ret = get_next_cell_hint();
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          break;
      }  // end switch
    }
  }  // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to execute with match_code", K(ret), K(match_code));
  }
  return ret;
}

void ObHbaseRowIterator::init_family_name()
{
  const ObString &table_name = hbase_query_.get_table_name();
  family_name_ = table_name.after('$');
}

int ObHbaseRowIterator::add_result_cell(const ObNewRow &row, ObTableQueryDListResult *&out_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_result->add_row(row, family_name_))) {
    LOG_WARN("failed to add row to result", K(ret));
  }
  return ret;
}

int ObHbaseRowIterator::add_result_cell(const ObNewRow &row, ObTableQueryResult *&out_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_result->add_row(row))) {
    LOG_WARN("failed to add row to result", K(ret));
  }
  return ret;
}

/// Seek the scanner at or after the specified KeyValue.
/// return the skipped cell count
int ObHbaseRowIterator::seek(ObHTableCell &key, int32_t &skipped_count)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (false) {
    // TODO When using it in the future, remember to adapt it to the TTL
    if (OB_FAIL(seek_first_cell_on_hint(static_cast<ObHTableCellEntity *>(&key)->get_ob_row()))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to seek first cell on hint", K(ret));
      }
    }
  } else {
    skipped_count = 0;
    while (OB_SUCC(next_cell())) {
      cmp_ret = ObHTableUtils::compare_cell(curr_cell_, key, scan_order_);
      if (cmp_ret >= 0) {
        LOG_DEBUG("seek to", K(key), K_(curr_cell));
        break;
      }
    }
    ++skipped_count;
  }
  return ret;
}
int ObHbaseRowIterator::rescan_and_get_next_row(table::ObHbaseICellIter *cell_iter, ObHbaseRescanParam &rescan_param, ObNewRow *&ob_next_row)
{
  int ret = OB_SUCCESS;
  ObNewRow *tmp_next_row = nullptr;
  if (OB_ISNULL(cell_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cell_iter is null", K(ret));
  } else if (OB_FAIL(cell_iter->rescan(rescan_param))) {
    LOG_WARN("failed to rescan executor", K(ret));
  } else {
    if (OB_FAIL(cell_iter->get_next_cell(tmp_next_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else {
      ob_next_row = tmp_next_row;
    }
  }
  return ret;
}

int ObHbaseRowIterator::seek_first_cell_on_hint(const ObNewRow *ob_row)
{
  int ret = OB_SUCCESS;
  ObHbaseICellIter *child_cell_iter = get_child_cell_iter();
  ObHbaseRescanParam &rescan_param = get_rescan_param();
  ObNewRange &forward_range = rescan_param.get_scan_range();
  int64_t limit = -1;
  forward_range_alloc_.reuse();
  if (scan_order_ == ObQueryFlag::Forward) {
    forward_range.end_key_ = stop_row_key_;
  } else {
    forward_range.end_key_.set_max_row();
  }
  ObObj *start_key = nullptr;
  if (ob_row->count_ < 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_row start_key obj_cnt_ is less than 3", K(ret), K(ob_row->count_));
  } else if (OB_ISNULL(start_key = static_cast<ObObj *>(forward_range_alloc_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ob_row->count_ && i < 3; ++i) {
      if (OB_FAIL(ob_write_obj(forward_range_alloc_, ob_row->cells_[i], start_key[i]))) {
        LOG_WARN("failed to write obj", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    forward_range.start_key_.assign(start_key, 3);
    ObNewRow *first_cell_on_row = nullptr;
    if (OB_FAIL(rescan_and_get_next_row(child_cell_iter, rescan_param, first_cell_on_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to rescan and get next row", K(ret));
      } else {
        has_more_cells_ = false;
        curr_cell_.set_ob_row(NULL);
        if (OB_NOT_NULL(matcher_)) {
          matcher_->clear_curr_row();
        }
        LOG_DEBUG("iterator end", K_(has_more_cells));
      }
    } else {
      curr_cell_.set_ob_row(first_cell_on_row);
    }
  }
  return ret;
}


int ObHbaseRowIterator::seek_or_skip_to_next_row(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  int32_t cur_version = column_tracker_->get_cur_version();
  ObHTableCell *next_cell = nullptr;

  bool enter_next_key = false;
  while (OB_SUCC(ret) && !enter_next_key) {
    if (OB_FAIL(ObHTableUtils::create_last_cell_on_row_col(allocator_, curr_cell_, next_cell))) {
      LOG_WARN("failed to create last cell", K(ret), K(curr_cell_));
    } else {
      int32_t skipped_count = 0;
      if (OB_FAIL(seek(*next_cell, skipped_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to seek next cell", K(ret));
        }
      } else if (ObHTableUtils::compare_rowkey(curr_cell_, *next_cell) != 0) {
        enter_next_key = true;  // hbase rowkey changed
      }

      if ((OB_SUCCESS == ret || OB_ITER_END == ret)) {
        // need record current rowkey and mark it recorded
        // record using the next_cell, because the curr_cell_
        // is not accessible when return OB_ITER_END
        try_record_expired_rowkey(cur_version + skipped_count, next_cell->get_rowkey());
      }
      cur_version = 1;
      next_cell->~ObHTableCell();
      allocator_.reuse();
    }
  }
  matcher_->clear_curr_row();
  return ret;
}

int ObHbaseRowIterator::seek_or_skip_to_next_col(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ObHTableCell *next_cell = NULL;
  if (OB_FAIL(ObHTableUtils::create_last_cell_on_row_col(allocator_, cell, next_cell))) {
    LOG_WARN("failed to create next cell", K(ret), K(cell));
  } else {
    int32_t cur_version = column_tracker_->get_cur_version();
    int32_t skipped_count = 0;
    ret = seek(*next_cell, skipped_count);

    if ((OB_SUCCESS == ret || OB_ITER_END == ret)) {
      // need record current rowkey and mark it recorded
      // record using the next_cell, because the curr_cell_
      // is not accessible when return OB_ITER_END
      try_record_expired_rowkey(cur_version + skipped_count, next_cell->get_rowkey());
    }

    next_cell->~ObHTableCell();
    allocator_.reuse();
  }
  return ret;
}

bool ObHbaseRowIterator::reach_batch_limit() const
{
  // @todo currently not supported
  return false;
}

bool ObHbaseRowIterator::reach_size_limit() const
{
  // @todo
  return false;
}

void ObHbaseRowIterator::set_hfilter(table::hfilter::Filter *hfilter)
{
  hfilter_ = hfilter;
  if (nullptr != matcher_) {
    matcher_->set_hfilter(hfilter);
  }
  ObTableAuditCtx *audit_ctx = exec_ctx_.get_audit_ctx();
  if (OB_NOT_NULL(audit_ctx)) {
    audit_ctx->filter_ = hfilter;
  }
}

void ObHbaseRowIterator::set_need_verify_cell_ttl(bool need_verify_cell_ttl) {
  need_verify_cell_ttl_ = need_verify_cell_ttl;
  matcher_impl_.set_need_verify_cell_ttl(need_verify_cell_ttl);
}

int ObHbaseRowIterator::try_record_expired_rowkey(const ObHTableCellEntity &cell)
{
  int ret = OB_SUCCESS;
  // TODO: rowkeyTTL should be collected and exeucte on LS leader, which
  // maybe not work in secondary-part scenarios
  // if (!is_timeseries_table_ && !is_cur_row_expired_) {
  //   if (need_verify_cell_ttl_ && OB_NOT_NULL(matcher_) && OB_FAIL(matcher_->is_cell_ttl_expired(cell, is_cur_row_expired_))) {
  //     LOG_WARN("failed to is cell ttl expired", K(ret));
  //   } else if (is_cur_row_expired_ || (time_to_live_ > 0 && OB_NOT_NULL(column_tracker_) &&
  //                                         column_tracker_->is_expired(cell.get_timestamp()))) {
  //     is_cur_row_expired_ = true;
  //     MTL(ObHTableRowkeyMgr *)
  //         ->record_htable_rowkey(
  //             INVALID_LS, hbase_query_.get_table_id(), tablet_ids_, cell.get_rowkey());
  //   }
  // }
  return ret;
}

void ObHbaseRowIterator::try_record_expired_rowkey(const int32_t versions, const ObString &rowkey)
{
  // TODO: rowkeyTTL should be collected and exeucte on LS leader, which
  // maybe not work in secondary-part scenarios
  // if (!is_timeseries_table_ && max_version_ > 0 && !is_cur_row_expired_ && versions > max_version_) {
  //   is_cur_row_expired_ = true;
  //   MTL(ObHTableRowkeyMgr*)->record_htable_rowkey(INVALID_LS,
  //                                                 hbase_query_.get_table_id(),
  //                                                 tablet_ids_,
  //                                                 rowkey);
  // }
}

ObHbaseCFIterator::ObHbaseCFIterator(const ObHbaseQuery &hbase_query, ObTableExecCtx &exec_ctx)
    : ObHbaseQueryResultIterator(hbase_query, exec_ctx),
      allocator_("HbaseCFIterAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      hbase_query_(hbase_query),
      exec_ctx_(exec_ctx),
      row_iterator_(NULL),
      iterable_result_(),
      filter_parser_(),
      batch_size_(hbase_query_.get_query().get_batch()),
      ob_kv_params_(hbase_query.get_query().get_ob_params()),
      caching_(-1),
      batch_(hbase_query_.get_query().get_batch()),
      max_result_size_(std::min(hbase_query_.get_query().get_max_result_size(),
        static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024))),
      is_first_result_(true),
      check_existence_only_(false),
      scanner_context_(),
      is_inited_(false),
      row_count_(0),
      filter_(nullptr),
      is_timeseries_table_(false)
{
}

ObHbaseCFIterator::~ObHbaseCFIterator()
{
  if (OB_NOT_NULL(row_iterator_)) {
    row_iterator_->~ObHbaseRowIterator();
    row_iterator_ = nullptr;
  }
}

bool ObHbaseCFIterator::reach_limit()
{
  return hbase_query_.get_query().get_limit() > 0 && row_count_ >= hbase_query_.get_query().get_limit();
}

bool ObHbaseCFIterator::reach_caching_limit(int num_of_row)
{
  return caching_ >= 0 && num_of_row >= caching_;
}


int ObHbaseCFIterator::get_next_result(ObTableQueryResult &next_result)
{
  int ret = OB_SUCCESS;
  next_result.reset_except_property();

  if (OB_FAIL(get_next_result_internal(next_result))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next result", K(ret));
    }
  }
  return ret;
}

int ObHbaseCFIterator::get_next_result(ObTableQueryIterableResult &next_result)
{
  int ret = OB_SUCCESS;
  next_result.reset();
  if (OB_FAIL(get_next_result_internal(next_result))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next result", K(ret));
    }
  }
  return ret;
}

int ObHbaseCFIterator::get_next_result(ObTableQueryIterableResult *&next_result)
{
  int ret = OB_SUCCESS;
  iterable_result_.reset();
  next_result = &iterable_result_;
  if (OB_FAIL(get_next_result_internal(iterable_result_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next result", K(ret));
    }
  }
  return ret;
}

template <typename ResultType>
int ObHbaseCFIterator::get_next_result_internal(ResultType &next_result)
{
  int ret = OB_SUCCESS;
  bool has_filter_row = (NULL != filter_) && (filter_->has_filter_row());
  ObTableQueryDListResult *htable_row = nullptr;
  int num_of_row = 0;
  scanner_context_.progress_.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHbaseCFIterator not inited", K(ret));
  }
  while (OB_SUCC(ret) && row_iterator_->has_more_result() && !reach_caching_limit(num_of_row) &&
         OB_SUCC(row_iterator_->get_next_row(htable_row))) {
    LOG_DEBUG("got one row", "cells_count", htable_row->get_row_count());
    bool is_empty_row = (htable_row->get_row_count() == 0);
    if (is_empty_row) {
      if (nullptr != filter_) {
        filter_->reset();
      }
      continue;
    }
    bool exclude = false;
    if (has_filter_row) {
      filter_->filter_row_cells(*htable_row);
      is_empty_row = (htable_row->get_row_count() == 0);
      if (!is_empty_row) {
        // last chance to drop entire row based on the sequence of filter calls
        if (filter_->filter_row()) {
          LOG_DEBUG("filter out the row");
          exclude = true;
        }
      }
    }
    if (is_empty_row || exclude) {
      if (NULL != filter_) {
        filter_->reset();
      }
      // fetch next row
      continue;
    }

    // if only check exist, add one row
    // cannot just set row_count_ as 1 in multi-cf situation
    if (check_existence_only_) {
      ObHTableCellEntity *row = nullptr;
      if (OB_FAIL(htable_row->get_row(row))) {
        LOG_WARN("fail to get row from htable row", K(ret));
      } else if (OB_FAIL(next_result.add_one_row_for_exist_only(*row->get_ob_row(), row->get_family()))) {
        LOG_WARN("failed to add one row for exist only", K(ret));
      } else {
        ret = OB_ITER_END;
      }
      break; // break out this while loops
    }

    if (OB_FAIL(next_result.add_all_row(*htable_row))) {
      LOG_WARN("failed to add cells to row", K(ret));
    }
    if (NULL != filter_) {
      filter_->reset();
    }
    if (OB_SUCC(ret)) {
      if (next_result.reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
        break;
      }
    }
    num_of_row++;
    row_count_++;
    if (reach_limit()) {
      row_iterator_->no_more_result();
    }
    if (reach_caching_limit(num_of_row)) {
      LOG_TRACE("reach cache limit:", K(num_of_row));
    }
  }  // end while

  if (OB_SUCC(ret) && !row_iterator_->has_more_result()) {
    ret = OB_ITER_END;
  }

  if (OB_ITER_END == ret && next_result.get_row_count() > 0) {
    ret = OB_SUCCESS;
  }

  LOG_DEBUG("get_next_result", K(ret), "row_count", next_result.get_row_count());
  return ret;
}

int ObHbaseCFIterator::init()
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = hbase_query_.get_query();
  const ObHBaseParams *hbase_params = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHbaseCFIterator inited twice", K(ret));
  } else if (OB_FAIL(init_hbase_params(hbase_params))) {
    LOG_WARN("fail to init hbase params", K(ret));
  } else {
    uint64_t table_id = hbase_query_.get_table_id();
    const ObTableSchema *table_schema = nullptr;
    ObHbaseModeType hbase_mode_type;
    ObSchemaGetterGuard &schema_guard = exec_ctx_.get_schema_guard();
    if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (OB_FAIL(ObHTableUtils::get_mode_type(*table_schema, hbase_mode_type))) {
      LOG_WARN("fail to get habse model type", K(ret));
    } else {
      is_timeseries_table_ = hbase_mode_type == ObHbaseModeType::OB_HBASE_SERIES_TYPE;
    }
  }

  if (OB_SUCC(ret) && !is_timeseries_table_) {
    if (OB_FAIL(init_hbase_filter(exec_ctx_.get_allocator(), hbase_params, filter_))) {
      LOG_WARN("fail to init hbase filter", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_timeseries_table_ && ObQueryFlag::Reverse == query.get_scan_order()) {
      if (OB_ISNULL(row_iterator_ = static_cast<ObHbaseReversedRowIterator *>(
          OB_NEWx(ObHbaseReversedRowIterator, &exec_ctx_.get_allocator(), hbase_query_, exec_ctx_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("row_iterator_ init nullptr", K(ret));
      } else if (OB_FAIL(row_iterator_->init(scanner_context_, hbase_params, filter_))) {
        LOG_WARN("fail to init hbase row iterator", K(ret));
      }
    } else {
      if (OB_ISNULL(row_iterator_ = static_cast<ObHbaseRowIterator *>(
          OB_NEWx(ObHbaseRowIterator, &exec_ctx_.get_allocator(), hbase_query_, exec_ctx_, is_timeseries_table_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("row_iterator_ init nullptr", K(ret));
      } else if (OB_FAIL(row_iterator_->init(scanner_context_, hbase_params, filter_))) {
        LOG_WARN("fail to init hbase row iterator", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    LOG_DEBUG("ObHbaseCFIterator init success", K(ret));
  }
  return ret;
}

int ObHbaseCFIterator::init_hbase_filter(ObIAllocator &allocator, const ObHBaseParams *hbase_params, hfilter::Filter *&filter)
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = hbase_query_.get_query();
  const ObString &hfilter_string = query.get_htable_filter().get_filter();
  if (hfilter_string.empty()) {
    filter = NULL;
  } else if (OB_FAIL(filter_parser_.init(&allocator))) {
    LOG_WARN("failed to init filter_parser", K(ret));
  } else if (OB_FAIL(filter_parser_.parse_filter(hfilter_string, filter))) {
    LOG_WARN("failed to parse filter", K(ret), K(hfilter_string));
  } else {
    filter->set_reversed(ObQueryFlag::Reverse == query.get_scan_order());
    if (OB_NOT_NULL(hbase_params) && !hbase_params->hbase_version_.empty()) {
      filter->set_hbase_version(hbase_params->hbase_version_);
    }
  }
  return ret;
}

int ObHbaseCFIterator::init_hbase_params(const ObHBaseParams *&hbase_params)
{
  int ret = OB_SUCCESS;
  scanner_context_.limits_.set_batch(batch_);
  if (ob_kv_params_.is_valid()) {
    if (OB_FAIL(ob_kv_params_.get_hbase_params(hbase_params))) {
      LOG_WARN("init ob_params fail", K(ret), K(ob_kv_params_));
    } else {
      caching_ = hbase_params->caching_;
      scanner_context_.limits_.set_fields(
          max_result_size_, hbase_params->call_timeout_, LimitScope(LimitScope::Scope::BETWEEN_ROWS));
      check_existence_only_ = hbase_params->check_existence_only_;
    }
  }
  return ret;
}

/**
 * ---------------------------------------- ObHbaseReversedRowIterator ----------------------------------------
 */

ObHbaseReversedRowIterator::ObHbaseReversedRowIterator(const ObHbaseQuery &hbase_query,
                                                       ObTableExecCtx &exec_ctx)
    : ObHbaseRowIterator(hbase_query, exec_ctx, false),
      iter_allocator_("HbaseRevIterAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      reversed_range_alloc_("HbaseRevRanAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      forward_cell_iter_(nullptr),
      reverse_rescan_param_(),
      forward_query_(nullptr)
{}

ObHbaseReversedRowIterator::~ObHbaseReversedRowIterator()
{
  if (OB_NOT_NULL(forward_cell_iter_)) {
    forward_cell_iter_->~ObHbaseICellIter();
    forward_cell_iter_ = nullptr;
  }
  OB_DELETEx(ObTableQuery, &exec_ctx_.get_allocator(), forward_query_);
}

int ObHbaseReversedRowIterator::init(ScannerContext &scanner_context, const ObHBaseParams *hbase_params, hfilter::Filter *filter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHbaseReversedRowIterator inited twice", K(ret));
  } else if (start_row_key_.length() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start_row_key failed init", K(ret), K(start_row_key_));
  } else if (start_row_key_.get_obj_cnt() != 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start_key obj cnt less than 3", K(ret), K(start_row_key_.get_obj_cnt()));
  } else if (stop_row_key_.length() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stop_row_key_ failed init", K(ret), K(stop_row_key_));
  } else if (stop_row_key_.get_obj_cnt() != 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stop_row_key_ obj cnt less than 3", K(ret), K(stop_row_key_.get_obj_cnt()));
  } else if (OB_FAIL(ObHbaseRowIterator::init(scanner_context, hbase_params, filter))) {
    LOG_WARN("fail to init hbase row iterator", K(ret));
  } else if (OB_FAIL(create_forward_cell_iter(exec_ctx_))) {
    LOG_WARN("create_reversed_child_op failed", K(ret));
  } else {
    // curr_cell_ needs to be initialized, and the pointer should be directed
    // to either the last `startKey` or the first cell of the preceding row.
    ObNewRow *tmp_next_row = nullptr;
    ObHTableCell *new_cell = nullptr;
    if (OB_FAIL(cell_iter_->get_next_cell(tmp_next_row))) {
      if (OB_ITER_END == ret) {
        has_more_cells_ = false;
        ret = OB_SUCCESS;
        LOG_DEBUG("no data in table", K(ret));
      } else {
        LOG_WARN("failed to rescan and get next row", K(ret));
      }
    } else if (OB_FAIL(ObHTableUtils::create_first_cell_on_row(
                   allocator_, tmp_next_row->get_cell(ObHTableConstants::COL_IDX_K).get_string(), new_cell))) {
      LOG_WARN("failed to create first cell on row", K(ret));
    } else if (OB_FAIL(seek_first_cell_on_hint(static_cast<ObHTableCellEntity *>(new_cell)->get_ob_row()))) {
      LOG_WARN("failed to get first cell on row in init", K(ret));
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::next_cell()
{
  int ret = OB_SUCCESS;
  ObNewRow *ob_row = NULL;
  ObHTableCellEntity cell_clone;
  if (OB_FAIL(cell_clone.deep_copy_ob_row(curr_cell_.get_ob_row(), allocator_))) {
    LOG_WARN("allocate memory for row_key failed", K(ret));
  } else if (OB_FAIL(forward_cell_iter_->get_next_cell(ob_row))) {
    if (OB_ITER_END == ret) {
      // cover recode when ret = OB_OB_ITER_END
      ret = OB_SUCCESS;
      if (OB_FAIL(seek_or_skip_to_next_row(cell_clone))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to seek to next row when get next cell", K(ret));
        }
      }
    } else {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (FALSE_IT(curr_cell_.set_ob_row(ob_row))) {
  } else if (0 == ObHTableUtils::compare_rowkey(cell_clone.get_rowkey(), curr_cell_.get_rowkey())) {
    // same rowkey
    if (OB_FAIL(try_record_expired_rowkey(curr_cell_))) {
      LOG_WARN("failed to try record expired rowkey", K(ret));
    }
  } else {
    if (OB_FAIL(seek_or_skip_to_next_row(cell_clone))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to seek to next row when get next cell", K(ret));
      }
    }
  }
  allocator_.reuse();
  return ret;
}

/*
 * In a reverse scan, advancing to the "next" row effectively means moving to the previous row.
 * This process entails initially constructing the minimum possible key (min-min) for the current cell.
 * Then, by taking a single reverse step with this min-min key, the row key of the row above (i.e., preceding) is
 * retrieved. Subsequently, another min-min key for a hypothetical cell at the start of this newfound row is
 * reconstructed. Finally, progressing with a forward step using this new min-min key lands you on the first cell of
 * the row preceding the original starting point.
 */
int ObHbaseReversedRowIterator::seek_or_skip_to_next_row(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  reversed_range_alloc_.reuse();
  if (OB_FAIL(ob_write_string(reversed_range_alloc_, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("allocate memory for row_key failed", K(ret));
  } else if (OB_FAIL(seek_or_skip_to_next_row_inner(rowkey_clone))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to seek or skip to next row", K(ret));
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::seek_or_skip_to_next_row(const ObString &rowkey)
{
  int ret = OB_SUCCESS;
  reversed_range_alloc_.reuse();
  if (OB_FAIL(seek_or_skip_to_next_row_inner(rowkey))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to seek or skip to next row", K(ret));
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::seek_or_skip_to_next_row_inner(const ObString &rowkey)
{
  int ret = OB_SUCCESS;
  ObHbaseRescanParam &rescan_param = forward_rescan_param_;
  ObNewRange &reverse_range = rescan_param.get_scan_range();
  reverse_range.start_key_ = start_row_key_;
  ObObj *end_key = nullptr;
  if (OB_ISNULL(end_key = static_cast<ObObj *>(reversed_range_alloc_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for end_key failed", K(ret));
  } else {
    end_key[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey);
    end_key[ObHTableConstants::COL_IDX_Q].set_min_value();
    end_key[ObHTableConstants::COL_IDX_T].set_min_value();
    reverse_range.end_key_.assign(end_key, 3);
    ObNewRow *ob_next_row = NULL;
    if (OB_FAIL(rescan_and_get_next_row(cell_iter_, rescan_param, ob_next_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to rescan and get next row", K(ret));
      } else {
        LOG_DEBUG("reverse scan has no more cell", K(ret), K(has_more_cells_));
        has_more_cells_ = false;
      }
    } else {
      ObObj hint_cell[3];
      hint_cell[ObHTableConstants::COL_IDX_K].set_varbinary(
          ob_next_row->get_cell(ObHTableConstants::COL_IDX_K).get_varchar());
      hint_cell[ObHTableConstants::COL_IDX_Q].set_min_value();
      hint_cell[ObHTableConstants::COL_IDX_T].set_min_value();
      const ObNewRow first_cell_on_row(hint_cell, 3);
      if (OB_FAIL(seek_first_cell_on_hint(&first_cell_on_row))) {
        LOG_WARN("failed to seek first cell on row", K(ret));
      }
    }
    if (OB_NOT_NULL(matcher_)) {
      matcher_->clear_curr_row();
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::seek_or_skip_to_next_col(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  forward_range_alloc_.reuse();
  ObString rowkey_clone;
  ObString qualifier_clone;
  if (OB_FAIL(ob_write_string(forward_range_alloc_, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("allocate memory for row_key failed", K(ret));
  } else if (OB_FAIL(ob_write_string(forward_range_alloc_, cell.get_qualifier(), qualifier_clone))) {
    LOG_WARN("allocate memory for qualifier_key failed", K(ret));
  } else if (OB_FAIL(seek_or_skip_to_next_col_inner(rowkey_clone, qualifier_clone))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to seek_or_skip_to_next_col", K(ret));
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::seek_or_skip_to_next_col(const ObString &rowkey, const ObString &qualifier)
{
  int ret = OB_SUCCESS;
  forward_range_alloc_.reuse();
  if (OB_FAIL(seek_or_skip_to_next_col_inner(rowkey, qualifier))) {
    LOG_WARN("failed to seek_or_skip_to_next_col", K(ret));
  }
  return ret;
}

int ObHbaseReversedRowIterator::seek_or_skip_to_next_col_inner(const ObString &rowkey, const ObString &qualifier)
{
  int ret = OB_SUCCESS;
  ObHbaseRescanParam &rescan_param = reverse_rescan_param_;
  ObNewRange &column_range = rescan_param.get_scan_range();
  ObObj *start_key = nullptr;
  ObObj *end_key = nullptr;
  if (OB_ISNULL(start_key = static_cast<ObObj *>(forward_range_alloc_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_key failed", K(ret));
  } else if (OB_ISNULL(end_key = static_cast<ObObj *>(forward_range_alloc_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for end_key failed", K(ret));
  } else {
    end_key[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey);
    end_key[ObHTableConstants::COL_IDX_Q].set_max_value();
    end_key[ObHTableConstants::COL_IDX_T].set_max_value();
    start_key[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey);
    start_key[ObHTableConstants::COL_IDX_Q].set_varbinary(qualifier);
    start_key[ObHTableConstants::COL_IDX_T].set_max_value();
    column_range.start_key_.assign(start_key, 3);
    column_range.end_key_.assign(end_key, 3);
    ObNewRow *ob_next_column_row = NULL;
    if (OB_FAIL(rescan_and_get_next_row(forward_cell_iter_, rescan_param, ob_next_column_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to rescan and get next row", K(ret));
      } else {
        if (OB_FAIL(seek_or_skip_to_next_row(rowkey))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row in seek_next_col", K(ret));
          }
        }
      }
    } else {
      allocator_.reuse();
      curr_cell_.set_ob_row(ob_next_column_row);
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::seek(ObHTableCell &key, int32_t &skipped_count)
{
  int ret = OB_SUCCESS;
  UNUSED(skipped_count);
  if (OB_FAIL(seek_first_cell_on_hint(static_cast<ObHTableCellEntity *>(&key)->get_ob_row()))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to seek first cell on hint", K(ret));
    }
  }
  return ret;
}

int ObHbaseReversedRowIterator::create_forward_cell_iter(ObTableExecCtx &exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = hbase_query_.get_query();
  ObHbaseICellIter *tmp_cell_iter = nullptr;
  if (OB_ISNULL(forward_query_ = OB_NEWx(ObTableQuery, &exec_ctx.get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for forward_query_ failed", K(ret));
  } else if (OB_FAIL(query.deep_copy(exec_ctx.get_allocator(), *forward_query_))) {
    LOG_WARN("deep copy query failed", K(ret));
  } else {
    forward_query_->set_scan_order(ObQueryFlag::Forward);
    forward_query_->get_tablet_ids().reset();
    forward_query_->get_tablet_ids().assign(get_tablet_ids());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_and_init_cell_iter(exec_ctx.get_allocator(), exec_ctx, *forward_query_, tmp_cell_iter))) {
    LOG_WARN("fail to scan", K(ret), K(query));
  } else {
    forward_cell_iter_ = tmp_cell_iter;
  }
  return ret;
}

void ObHbaseReversedRowIterator::close()
{
  if (OB_NOT_NULL(forward_cell_iter_)) {
    forward_cell_iter_->close();
  }
  ObHbaseRowIterator::close();
}

void ObHbaseCFIterator::close()
{
  if (OB_NOT_NULL(row_iterator_)) {
    row_iterator_->close();
  }
}

bool ObHbaseCFIterator::has_more_result() const
{
  bool bret = false;
  if (OB_NOT_NULL(row_iterator_)) {
    bret = row_iterator_->has_more_result();
  }
  return bret;
}
template <typename ResultType>
int ObHbaseRowIterator::get_next_row_internal_timeseries(ResultType *&result)
{
  int ret = OB_SUCCESS;
  bool loop = true;
  ObHTableMatchCode match_code = ObHTableMatchCode::DONE_SCAN;  // initialize
  while (OB_SUCC(ret) && loop) {
    match_code = ObHTableMatchCode::DONE_SCAN;
    curr_cell_.set_family(family_name_);
    if (OB_FAIL(matcher_->match(curr_cell_, match_code))) {
      LOG_WARN("failed to match cell", K(ret));
    } else {
      if (NULL == curr_cell_.get_ob_row()) {
        LOG_DEBUG("matcher, curr_cell=NULL ", K(match_code));
      } else {
        LOG_DEBUG("matcher", K_(curr_cell), K(match_code));
      }
      switch (match_code) {
        case ObHTableMatchCode::INCLUDE:
          ++count_per_row_;
          // whether reach per row limit
          if (limit_per_row_per_cf_ > -1 /*not unlimited*/
              && count_per_row_ > (offset_per_row_per_cf_ + limit_per_row_per_cf_)) {
            ret = seek_or_skip_to_next_row(curr_cell_);
            loop = false;
            LOG_DEBUG(
                "reach per row limit", K(ret), K_(offset_per_row_per_cf), K_(limit_per_row_per_cf), K_(count_per_row));
            break;
          }

          // whether skip offset
          if (count_per_row_ > offset_per_row_per_cf_) {
            int64_t timestamp = 0;
            if (OB_FAIL(curr_cell_.get_ob_row()->get_cell(ObHTableConstants::COL_IDX_T).get_int(timestamp))) {
              LOG_WARN("failed to get timestamp", K(ret));
            } else {
              const_cast<ObNewRow*>(curr_cell_.get_ob_row())->get_cell(ObHTableConstants::COL_IDX_T).set_int(-timestamp);
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(add_result_cell(*(curr_cell_.get_ob_row()), result))) {
                LOG_WARN("failed to add cell to result", K(ret));
              } else {
                scanner_context_->increment_batch_progress(1);
                scanner_context_->increment_size_progress(curr_cell_.get_ob_row()->get_serialize_size());
                ++cell_count_;
                LOG_DEBUG(
                    "add cell", K_(cell_count), K_(curr_cell), K_(count_per_row), K_(offset_per_row_per_cf));
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(next_cell())) {
              if (ret != OB_ITER_END) {
                LOG_WARN("fail to get next cell", K(ret));
              } else {
                loop = false;
              }
            } else if (scanner_context_->check_any_limit(LimitScope::Scope::BETWEEN_CELLS)) {
              loop = false;
            }
          }
          break;
        case ObHTableMatchCode::DONE:
          // done current row
          matcher_->clear_curr_row();
          loop = false;
          break;
        case ObHTableMatchCode::DONE_SCAN:
          has_more_cells_ = false;
          loop = false;
          break;
        // case ObHTableMatchCode::SEEK_NEXT_ROW:
        //   ret = seek_or_skip_to_next_row(curr_cell_);
        //   break;
        case ObHTableMatchCode::SKIP:
          ret = next_cell();
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected match code for timeseries hbase iterator", K(ret), K(match_code));
          break;
      }  // end switch
    }
  }  // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObHbaseTSExplicitColumnTracker::init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &qualifiers = htable_filter.get_columns();
  const int64_t N = qualifiers.count();
  ObMemAttr bucket_attr(MTL_ID(), "HbaseTSCoSetBkt");
  ObMemAttr node_attr(MTL_ID(), "HbaseTSCoSetNde");
  if (OB_SUCC(ret) && N <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not use ExplicitColumnTracker", K(ret));
  } else if (OB_FAIL(column_set_.create(N, bucket_attr, node_attr))) {
    LOG_WARN("fail to create column set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObString real_qualifier = qualifier_with_family ? qualifiers.at(i).after('.') : qualifiers.at(i);
    if (OB_FAIL(column_set_.exist_refactored(real_qualifier))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_PARAM_DUPLICATE;
        LOG_WARN("duplicated qualifiers", K(ret), K(real_qualifier));
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to set refactored column set", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(column_set_.set_refactored(real_qualifier))) {
      LOG_WARN("failed to push back", K(ret), K(real_qualifier));
    }
  }
  return ret;
}

bool ObHbaseTSExplicitColumnTracker::done() const
{
  return false;
}

int ObHbaseTSExplicitColumnTracker::check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_FAIL(column_set_.exist_refactored(cell.get_qualifier()))) {
    if (OB_HASH_EXIST == ret) {
      match_code = ObHTableMatchCode::INCLUDE;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      match_code = ObHTableMatchCode::SKIP;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to set refactored column set", K(ret), K(cell.get_qualifier()));
    }
  }
  LOG_DEBUG("ObHbaseTSExplicitColumnTracker::check_column", K(ret), K(cell), K(match_code));
  return ret;
}

int ObHbaseTSExplicitColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = cell.get_timestamp();
  if (is_expired(timestamp)) {
    match_code = ObHTableMatchCode::SKIP;
  } else {
    match_code = ObHTableMatchCode::INCLUDE;
  }
  return ret;
}

// Called between every row.
void ObHbaseTSExplicitColumnTracker::reset()
{
}

int ObHbaseTSExplicitColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::SKIP;
  return OB_SUCCESS;
}

int32_t ObHbaseTSExplicitColumnTracker::get_cur_version()
{
  return 0;
}

bool ObHbaseTSExplicitColumnTracker::is_done(int64_t timestamp) const
{
  UNUSED(timestamp);
  return false;
}

int ObHbaseTSWildcardColumnTracker::init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family)
{
  UNUSEDx(htable_filter, qualifier_with_family);
  int ret = OB_SUCCESS;
  return ret;
}

int ObHbaseTSWildcardColumnTracker::check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::INCLUDE;
  return OB_SUCCESS;
}

int ObHbaseTSWildcardColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  match_code = ObHTableMatchCode::INCLUDE;
  int64_t timestamp = cell.get_timestamp();
  if (is_expired(timestamp)) {
    match_code = ObHTableMatchCode::SKIP;
  } else {
    match_code = ObHTableMatchCode::INCLUDE;
  }
  return ret;
}

int ObHbaseTSWildcardColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::SKIP;
  return OB_SUCCESS;
}

void ObHbaseTSWildcardColumnTracker::reset()
{
  LOG_DEBUG("reset qualifier");
}

int32_t ObHbaseTSWildcardColumnTracker::get_cur_version()
{
  return 0;
}

bool ObHbaseTSWildcardColumnTracker::is_done(int64_t timestamp) const
{
  UNUSED(timestamp);
  return false;
}

} // end of namespace table
} // end of namespace oceanbase