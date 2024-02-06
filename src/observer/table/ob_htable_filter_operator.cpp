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

#define USING_LOG_PREFIX SERVER
#include "ob_htable_filter_operator.h"
#include "ob_htable_utils.h"
#include "lib/json/ob_json.h"
#include "share/ob_errno.h"
#include "share/table/ob_ttl_util.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::table::hfilter;
using namespace oceanbase::share::schema;

// format: {"Hbase": {"TimeToLive": 3600, "MaxVersions": 3}}
int ObHColumnDescriptor::from_string(const common::ObString &kv_attributes)
{
  reset();
  int ret = OB_SUCCESS;
  if (kv_attributes.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(kv_attributes, max_version_, time_to_live_))) {
    LOG_WARN("fail to parse kv attributes", K(ret), K(kv_attributes));
  }
  return ret;
}

void ObHColumnDescriptor::reset()
{
  time_to_live_ = 0;
  max_version_ = 0;
}

////////////////////////////////////////////////////////////////
class ObHTableColumnTracker::ColumnCountComparator
{
public:
  bool operator()(const ColumnCount &a, const ColumnCount &b) const
  {
    return a.first.compare(b.first) < 0;
  }
};

class ObHTableColumnTracker::ColumnCountReverseComparator
{
public:
  bool operator()(const ColumnCount &a, const ColumnCount &b) const
  {
    return a.first.compare(b.first) > 0;
  }
};

void ObHTableColumnTracker::set_ttl(int32_t ttl_value)
{
  if (ttl_value > 0) {
    int64_t now = ObTimeUtility::current_time();
    now = now / 1000;  // us -> ms
    oldest_stamp_ = now - (ttl_value * 1000LL);
    LOG_DEBUG("[yzfdebug] set ttl", K(ttl_value), K(now), K_(oldest_stamp));
    NG_TRACE_EXT(t, OB_ID(arg1), ttl_value, OB_ID(arg2), oldest_stamp_);
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ttl value", K(ttl_value));
  }
}

void ObHTableColumnTracker::set_max_version(int32_t max_version)
{
  if (max_version > 0) {
    max_versions_ = max_version;
    LOG_DEBUG("set max_version", K(max_version));
    NG_TRACE_EXT(version, OB_ID(arg1), max_version);
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid max version value", K(max_version));
  }
}

bool ObHTableColumnTracker::is_done(int64_t timestamp) const
{
  return min_versions_ <= 0 && is_expired(timestamp);
}

////////////////////////////////////////////////////////////////
ObHTableExplicitColumnTracker::ObHTableExplicitColumnTracker()
    :columns_(),
     curr_column_idx_(0),
     curr_column_(NULL)
{}

int ObHTableExplicitColumnTracker::init(const table::ObHTableFilter &htable_filter, common::ObQueryFlag::ScanOrder &scan_order)
{
  int ret = OB_SUCCESS;
  max_versions_ = htable_filter.get_max_versions();
  if (ObQueryFlag::Forward == scan_order || ObQueryFlag::Reverse == scan_order){
    set_scan_order(scan_order);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the scan order doesn't match what we support", K(ret),K(scan_order));
  }

  const ObIArray<ObString> &qualifiers = htable_filter.get_columns();
  const int64_t N = qualifiers.count();
  if (OB_SUCC(ret) && N <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not use ExplicitColumnTracker", K(ret));
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (OB_FAIL(columns_.push_back(std::make_pair(qualifiers.at(i), 0)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } // end for
  if (OB_SUCC(ret)) {
    // sort qualifiers
    ColumnCount *end = &columns_.at(columns_.count() - 1);
    ++end;
    if (common::ObQueryFlag::Reverse == tracker_scan_order_) {
      std::sort(&columns_.at(0), end, ColumnCountReverseComparator());
    } else {
      std::sort(&columns_.at(0), end, ColumnCountComparator());
    }
  }

  if (OB_SUCC(ret)) {
    // check duplicated qualifiers
    for (int64_t i = 0; OB_SUCCESS == ret && i < N - 1; ++i)
    {
      if (columns_.at(i).first == columns_.at(i+1).first) {
        ret = OB_ERR_PARAM_DUPLICATE;
        LOG_WARN("duplicated qualifiers", K(ret), "cq", columns_.at(i).first, K(i));
      }
    } // end for
  }
  return ret;
}

bool ObHTableExplicitColumnTracker::done() const
{
  return curr_column_idx_ >= columns_.count();
}

int ObHTableExplicitColumnTracker::check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
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
    if (ObQueryFlag::Reverse == tracker_scan_order_) {
      cmp_ret = ObHTableUtils::compare_qualifier(curr_column_->first, cell.get_qualifier());
    } else {
      cmp_ret = ObHTableUtils::compare_qualifier(cell.get_qualifier(), curr_column_->first);
    }
    LOG_DEBUG("[sldebug] compare qualifier res and scan order", K(tracker_scan_order_), K(cell.get_qualifier()), K(cell.get_rowkey()), K(cmp_ret), K(curr_column_idx_));
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

int ObHTableExplicitColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int32_t count = ++curr_column_->second;
  int64_t timestamp = cell.get_timestamp();
  LOG_DEBUG("[yzfdebug] check versions", K(count), K_(max_versions), K(timestamp),
            K_(min_versions), K_(oldest_stamp), "is_expired", is_expired(timestamp));
  // in reverse scan, check the cell timeout condition with ttl. Kick off the timeout cell from res.
  if(ObQueryFlag::Reverse == tracker_scan_order_) {
    match_code = check_version(cell.get_timestamp());
  } else {
    if (count >= max_versions_
          || (is_expired(timestamp) && count >= min_versions_)) {
        // Done with versions for this column
        ++curr_column_idx_;
        if (done()) {
          match_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW;  // done row
        } else {
          // We are done with current column; advance to next column
          // of interest.
          curr_column_ = &columns_.at(curr_column_idx_);
          match_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL;
        }
      } else {
        match_code = ObHTableMatchCode::INCLUDE;
      }
  }
  return ret;
}

ObHTableMatchCode ObHTableExplicitColumnTracker::check_version(int64_t timestamp)
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
void ObHTableExplicitColumnTracker::reset()
{
  curr_column_idx_ = 0;
  curr_column_ = &columns_.at(curr_column_idx_);
  const int64_t N = columns_.count();
  for (int64_t i = 0; i < N; ++i)
  {
    columns_.at(i).second = 0;
  } // end for
}

void ObHTableExplicitColumnTracker::done_with_column(const ObHTableCell &cell)
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

int ObHTableExplicitColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  done_with_column(cell);
  if (NULL == curr_column_) {
    match_code = ObHTableMatchCode::SEEK_NEXT_ROW;
  } else {
    match_code = ObHTableMatchCode::SEEK_NEXT_COL;
  }
  return OB_SUCCESS;
}

////////////////////////////////////////////////////////////////
ObHTableWildcardColumnTracker::ObHTableWildcardColumnTracker()
    :allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     current_qualifier_(),
     current_count_(0)
{}

int ObHTableWildcardColumnTracker::init(const table::ObHTableFilter &htable_filter, common::ObQueryFlag::ScanOrder &scan_order)
{
  int ret = OB_SUCCESS;
  max_versions_ = htable_filter.get_max_versions();
  if (ObQueryFlag::Forward == scan_order || ObQueryFlag::Reverse == scan_order){
    set_scan_order(scan_order);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the scan order doesn't match what we support", K(ret),K(scan_order));
  }
  // @todo set min_versions_ and oldest_stamp_
  return ret;
}

int ObHTableWildcardColumnTracker::check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::INCLUDE;
  return OB_SUCCESS;
}

int ObHTableWildcardColumnTracker::reset_cell(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  current_count_ = 0;
  allocator_.reuse();
  if (OB_FAIL(ob_write_string(allocator_, cell.get_qualifier(), current_qualifier_))) {
    LOG_WARN("failed to copy qualifier", K(ret));
  }
  return ret;
}

int ObHTableWildcardColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int cmp_ret;
  if (current_qualifier_.empty()) {
    // first iteration
    ret = reset_cell(cell);
    if (OB_SUCC(ret)) {
      match_code = check_version(cell.get_timestamp());
    }
  } else {
    if (common::ObQueryFlag::Reverse == tracker_scan_order_) {
      cmp_ret = ObHTableUtils::compare_qualifier(current_qualifier_, cell.get_qualifier());
    } else {
      cmp_ret = ObHTableUtils::compare_qualifier(cell.get_qualifier(),current_qualifier_);
    }
    if (0 == cmp_ret) {
      match_code = check_version(cell.get_timestamp());
    } else if (cmp_ret > 0) {
      // a new qualifier
      ret = reset_cell(cell);
      if (OB_SUCC(ret)) {
        match_code = check_version(cell.get_timestamp());
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("cell qualifier less than current one", K(ret), K(cell), K_(current_qualifier));
    }
  }
  return ret;
}

ObHTableMatchCode ObHTableWildcardColumnTracker::check_version(int64_t timestamp)
{
  ObHTableMatchCode match_code = ObHTableMatchCode::INCLUDE;
  if (ObQueryFlag::Reverse == tracker_scan_order_) {
    if (!is_expired(timestamp)) {
      // keep the KV if required by minversions or it is not expired, yet
      match_code = ObHTableMatchCode::INCLUDE;
    } else {
      match_code = ObHTableMatchCode::SEEK_NEXT_COL;
    }
  } else {
    ++current_count_;
    if (current_count_ > max_versions_) {
      match_code = ObHTableMatchCode::SEEK_NEXT_COL;
    } else if (current_count_ <= min_versions_
              || !is_expired(timestamp)) {
      // keep the KV if required by minversions or it is not expired, yet
      match_code = ObHTableMatchCode::INCLUDE;
    } else {
      match_code = ObHTableMatchCode::SEEK_NEXT_COL;
    }
  }
  return match_code;
}

int ObHTableWildcardColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::SEEK_NEXT_COL;
  return OB_SUCCESS;
}

void ObHTableWildcardColumnTracker::reset()
{
  LOG_DEBUG("[yzfdebug] reset qualifier");
  current_qualifier_.reset();
}

////////////////////////////////////////////////////////////////
ObHTableScanMatcher::ObHTableScanMatcher(const table::ObHTableFilter &htable_filter,
                                         ObHTableColumnTracker *column_tracker /*= nullptr*/)
    :time_range_(-htable_filter.get_max_stamp(), -htable_filter.get_min_stamp()),
     column_tracker_(column_tracker),
     hfilter_(NULL),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     curr_row_()
{
}

int ObHTableScanMatcher::pre_check(const ObHTableCell &cell, ObHTableMatchCode &match_code, bool &need_match_column)
{
  int ret = OB_SUCCESS;
  need_match_column = false;
  if (is_curr_row_empty()) {
    // Since the curCell is null it means we are already sure that we have moved over to the next
    // row
    match_code = ObHTableMatchCode::DONE;
  } else if (0 != ObHTableUtils::compare_rowkey(curr_row_, cell)) {
    // if row key is changed, then we know that we have moved over to the next row
    // WildcardColumnTracker will come to this branch
    match_code = ObHTableMatchCode::DONE;
    LOG_DEBUG("[yzfdebug] row changed", K(match_code), K(cell));
  } else if (column_tracker_->done()) {
    match_code = ObHTableMatchCode::SEEK_NEXT_ROW;
  }
  // check for early out based on timestamp alone
  else if (column_tracker_->is_done(cell.get_timestamp())) {
    if (OB_FAIL(column_tracker_->get_next_column_or_row(cell, match_code))) {
      LOG_WARN("failed to get next column or row", K(ret), K(cell));
    }
  }
  // @todo check if the cell is expired by cell TTL
  else
  {
    // continue
    need_match_column = true;
  }
  LOG_DEBUG("[yzfdebug] pre_check", K(ret), K(match_code), K(need_match_column));
  return ret;
}

int ObHTableScanMatcher::match_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = cell.get_timestamp();
  int cmp_tr = time_range_.compare(timestamp);
  LOG_DEBUG("[yzfdebug] compare time range", K(timestamp), K(cmp_tr), K_(time_range));
  // STEP 0: Check if the timestamp is in the range
  // @note timestamp is negative and in the descending order!
  if (cmp_tr > 0) {
    match_code = ObHTableMatchCode::SKIP;
  } else if (cmp_tr < 0) {
    if (OB_FAIL(column_tracker_->get_next_column_or_row(cell, match_code))) {
      LOG_WARN("failed to get next column or row", K(ret), K(cell));
    }
  }
  // STEP 1: Check if the column is part of the requested columns
  else if (OB_FAIL(column_tracker_->check_column(cell, match_code))) {
    LOG_WARN("failed to check column", K(ret), K(cell));
  } else if (ObHTableMatchCode::INCLUDE != match_code) {
    // nothing
  } else {
    /*
     * STEP 2: check the number of versions needed. This method call returns SKIP, SEEK_NEXT_COL,
     * INCLUDE, INCLUDE_AND_SEEK_NEXT_COL, or INCLUDE_AND_SEEK_NEXT_ROW.
     */
    if (OB_FAIL(column_tracker_->check_versions(cell, match_code))) {
    } else {
      switch(match_code) {
        case ObHTableMatchCode::SKIP:
        case ObHTableMatchCode::SEEK_NEXT_COL:
          break;
        default: {
          // It means it is INCLUDE, INCLUDE_AND_SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_ROW.
          // in reverse scan order, functions which is filter cell and merge filter return code are completed at add_same_kq_to_res function
          if (ObQueryFlag::Reverse != column_tracker_->get_scan_order()){
            if (NULL != hfilter_) {
              hfilter::Filter::ReturnCode filter_rc;
              if (OB_FAIL(hfilter_->filter_cell(cell, filter_rc))) {
                LOG_WARN("failed to filter cell", K(ret));
              } else {
                ObHTableMatchCode orig_code = match_code;
                match_code = merge_filter_return_code(cell, match_code, filter_rc);
                LOG_DEBUG("[yzfdebug] filter cell", K(filter_rc), K(orig_code), K(match_code));
              }
            }
          }
          break;
        }
      }
    }
  }
  return ret;
}
/*
 * Call this when scan has filter. Decide the desired behavior by checkVersions's MatchCode and
 * filterCell's ReturnCode. Cell may be skipped by filter, so the column versions in result may be
 * less than user need. It need to check versions again when filter and columnTracker both include
 * the cell.
 */
ObHTableMatchCode ObHTableScanMatcher::merge_filter_return_code(const ObHTableCell &cell,
                                                                const ObHTableMatchCode match_code, hfilter::Filter::ReturnCode filter_rc)
{
  ObHTableMatchCode ret_code = ObHTableMatchCode::INCLUDE;
  common::ObQueryFlag::ScanOrder scan_order = column_tracker_->get_scan_order();
  UNUSED(cell);
  UNUSED(match_code);
  UNUSED(filter_rc);
  switch(filter_rc) {
    case Filter::ReturnCode::INCLUDE:
      break;
    case Filter::ReturnCode::INCLUDE_AND_NEXT_COL:
      if (ObQueryFlag::Reverse == scan_order) {
        ret_code = ObHTableMatchCode::INCLUDE;
      } else {
        if (ObHTableMatchCode::INCLUDE == match_code) {
            ret_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL;
        }
      }

      break;
    case Filter::ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW:
      ret_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW;
      break;
    case Filter::ReturnCode::SKIP:
      if (match_code == ObHTableMatchCode::INCLUDE) {
        return ObHTableMatchCode::SKIP;
      } else if (match_code == ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL) {
        return ObHTableMatchCode::SEEK_NEXT_COL;
      } else if (match_code == ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW) {
        return ObHTableMatchCode::SEEK_NEXT_ROW;
      }
      break;
    case Filter::ReturnCode::NEXT_COL:
      if (match_code == ObHTableMatchCode::INCLUDE
          || match_code == ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL) {
        (void)column_tracker_->get_next_column_or_row(cell, ret_code);
        return ret_code;
      } else if (match_code == ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW) {
        return ObHTableMatchCode::SEEK_NEXT_ROW;
      }
      break;
    case Filter::ReturnCode::NEXT_ROW:
      return ObHTableMatchCode::SEEK_NEXT_ROW;
    case Filter::ReturnCode::SEEK_NEXT_USING_HINT:
      return ObHTableMatchCode::SEEK_NEXT_USING_HINT;
    default:
      break;
  }
  // We need to make sure that the number of cells returned will not exceed max version in scan
  // when the match code is INCLUDE* case.
  // @todo FIXME
  return ret_code;
}

int ObHTableScanMatcher::match(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  bool need_match_column = false;
  if (NULL != hfilter_ && hfilter_->filter_all_remaining()) {
    match_code = ObHTableMatchCode::DONE_SCAN;
  } else if (OB_FAIL(pre_check(cell, match_code, need_match_column))) {
  } else if (need_match_column) {
    if (OB_FAIL(match_column(cell, match_code))) {
    }
  }
  return ret;
}

int ObHTableScanMatcher::create_key_for_next_col(common::ObArenaAllocator &allocator,
                                                 const ObHTableCell &cell, ObHTableCell *&next_cell)
{
  int ret = OB_SUCCESS;
  const ObHTableColumnTracker::ColumnCount *curr_column = column_tracker_->get_curr_column();
  if (NULL == curr_column) {
    ret = ObHTableUtils::create_last_cell_on_row_col(allocator, cell, next_cell);
  } else {
    ret = ObHTableUtils::create_first_cell_on_row_col(allocator, cell, curr_column->first, next_cell);
  }
  return ret;
}

const ObHTableCell* ObHTableScanMatcher::get_curr_row() const
{
  const ObHTableCell* p = NULL;
  if (NULL != curr_row_.get_ob_row()) {
    p = &curr_row_;
  }
  return p;
}

int ObHTableScanMatcher::set_to_new_row(const ObHTableCell &arg_curr_row)
{
  int ret = OB_SUCCESS;
  const ObHTableCellEntity &curr_row = dynamic_cast<const ObHTableCellEntity&>(arg_curr_row);
  // deep copy curr_row
  if (NULL != curr_row_.get_ob_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state", K(ret));
  } else if (NULL == curr_row.get_ob_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obrow is null", K(ret));
  } else {
    curr_ob_row_.reset();
    allocator_.reuse();
    if (OB_FAIL(ob_write_row(allocator_, *curr_row.get_ob_row(), curr_ob_row_))) {
      LOG_WARN("failed to copy row", K(ret));
    } else {
      curr_row_.set_ob_row(&curr_ob_row_);
    }
  }
  // reset tracker
  if (OB_SUCC(ret)) {
    column_tracker_->reset();
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObHTableRowIterator::ObHTableRowIterator(const ObTableQuery &query)
    : ObTableQueryResultIterator(&query),
      child_op_(NULL),
      htable_filter_(query.get_htable_filter()),
      hfilter_(NULL),
      limit_per_row_per_cf_(htable_filter_.get_max_results_per_column_family()),
      offset_per_row_per_cf_(htable_filter_.get_row_offset_per_column_family()),
      max_result_size_(query.get_max_result_size()),
      batch_size_(query.get_batch()),
      time_to_live_(0),
      curr_cell_(),
      allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      column_tracker_(NULL),
      matcher_(NULL),
      column_tracker_wildcard_(),
      column_tracker_explicit_(),
      matcher_impl_(htable_filter_),
      scan_order_(query.get_scan_order()),
      cell_count_(0),
      count_per_row_(0),
      has_more_cells_(true),
      is_first_result_(true)
{}

ObHTableRowIterator::~ObHTableRowIterator()
{
}

int ObHTableRowIterator::next_cell()
{
  ObNewRow *ob_row = NULL;
  int ret = child_op_->get_next_row(ob_row, false);
  if (OB_SUCCESS == ret) {
    curr_cell_.set_ob_row(ob_row);
    LOG_DEBUG("[yzfdebug] fetch next cell", K_(curr_cell));
  } else if (OB_ITER_END == ret) {
    has_more_cells_ = false;
    curr_cell_.set_ob_row(NULL);
    matcher_->clear_curr_row();
    LOG_DEBUG("[yzfdebug] iterator end", K_(has_more_cells));
  }
  return ret;
}

int ObHTableRowIterator::reverse_next_cell(ObIArray<common::ObNewRow> &same_kq_cells, ObTableQueryResult *&out_result)
{
  ObNewRow *ob_row = NULL;
  int ret = child_op_->get_next_row(ob_row, false);
  if ((ObQueryFlag::Reverse == scan_order_ && OB_ITER_END == ret) ||
      (ObQueryFlag::Reverse == scan_order_ && OB_SUCCESS == ret &&
      NULL != hfilter_ && hfilter_->filter_all_remaining())) {
    ret = add_same_kq_to_res(same_kq_cells, out_result);
    same_kq_cells.reset();
    has_more_cells_ = false;
    curr_cell_.set_ob_row(NULL);
    matcher_->clear_curr_row();
    LOG_DEBUG("[sldebug] iterator end", K_(has_more_cells));
  } else if (OB_SUCCESS == ret) {
    curr_cell_.set_ob_row(ob_row);
    LOG_DEBUG("[sldebug] curr cell", K_(curr_cell));
  } else if (OB_SUCCESS != ret && OB_ITER_END != ret){
    LOG_WARN("the ret doesn't match what we had been expected",K(ret));
  }
  return ret;
}

int ObHTableRowIterator::add_same_kq_to_res(ObIArray<ObNewRow> &same_kq_cells, ObTableQueryResult *&out_result) {
  int ret = OB_SUCCESS;
  int N = same_kq_cells.count();
  int M = htable_filter_.get_max_versions();
  int end_idx = (N - M) > 0 ? (N - M) : 0;
  for (int i = N - 1; OB_SUCC(ret) && i >= end_idx; i--) {
    ObNewRow &tmp = same_kq_cells.at(i);
    ObHTableCellEntity tmp_cell;
    tmp_cell.set_ob_row(&tmp);

    //make timestamp as positive
    int64_t timestamp = 0;
    if (OB_FAIL(tmp.get_cell(ObHTableConstants::COL_IDX_T).get_int(timestamp))) {
      LOG_WARN("failed to get timestamp",K(ret));
    } else {
      tmp.get_cell(ObHTableConstants::COL_IDX_T).set_int(-timestamp);
    }
    if (OB_SUCC(ret)) {
      ObHTableMatchCode match_code = ObHTableMatchCode::INCLUDE;
      if (NULL != hfilter_ && ObHTableMatchCode::INCLUDE == match_code) {
        hfilter::Filter::ReturnCode filter_rc;
        if (OB_FAIL(hfilter_->filter_cell(tmp_cell, filter_rc))) {
          LOG_WARN("failed to filter cell", K(ret));
        } else {
          ObHTableMatchCode orig_code = match_code;
          match_code = matcher_->merge_filter_return_code(tmp_cell, match_code, filter_rc);
          LOG_DEBUG("[sldebug] filter cell", K(filter_rc), K(orig_code), K(match_code));
        }
      }

      if (OB_SUCC(ret) && NULL != hfilter_ && hfilter_->filter_all_remaining()) {
        match_code = ObHTableMatchCode::DONE_SCAN;
        has_more_cells_ = false;
      }

      if (OB_SUCC(ret) && ObHTableMatchCode::INCLUDE == match_code) {
        if (OB_FAIL(out_result->add_row(tmp))) {
          LOG_WARN("failed to add row to result", K(ret));
        } else {
          ++cell_count_;
          LOG_DEBUG("reverse add cell", K_(cell_count), K(tmp), K(out_result->get_row_count()));
        }
      }
    }
  }

  return ret;
}

int ObHTableRowIterator::get_next_result(ObTableQueryResult *&out_result)
{
  int ret = OB_SUCCESS;
  out_result = &one_hbase_row_;
  one_hbase_row_.reset();
  ObIArray<common::ObNewRow>& same_kq_cells = get_same_kq_cells();
  ObHTableMatchCode match_code = ObHTableMatchCode::DONE_SCAN;  // initialize
  if (ObQueryFlag::Reverse == scan_order_ && (-1 != limit_per_row_per_cf_ || 0 != offset_per_row_per_cf_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("server don't support set limit_per_row_per_cf_ and offset_per_row_per_cf_ in reverse scan yet",
              K(ret), K(scan_order_), K(limit_per_row_per_cf_), K(offset_per_row_per_cf_));
  }
  if (OB_SUCC(ret) && NULL == column_tracker_) {
    // first iteration
    if (htable_filter_.get_columns().count() <= 0) {
      column_tracker_ = &column_tracker_wildcard_;
    } else {
      column_tracker_ = &column_tracker_explicit_;
    }
    if (OB_FAIL(column_tracker_->init(htable_filter_, scan_order_))) {
      LOG_WARN("failed to init column tracker", K(ret));
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
  bool loop = true;
  if (OB_SUCC(ret)) {
    if (NULL == matcher_->get_curr_row()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("matcher should have valid first cell", K(ret));
    } else if (NULL != hfilter_) {
      const ObHTableCell &first_cell = curr_cell_;
      if (hfilter_->filter_row_key(first_cell)) {
        // filter out the current row and fetch the next row
        hfilter_->reset();
        LOG_DEBUG("[yzfdebug] filter_row_key skip the row", K(ret));
        //loop = false;
        matcher_->clear_curr_row();
        ret = seek_or_skip_to_next_row(curr_cell_);
      }
    }
  }
  while (OB_SUCC(ret) && loop) {
    match_code = ObHTableMatchCode::DONE_SCAN;  // initialize
    if (OB_FAIL(matcher_->match(curr_cell_, match_code))) {
      LOG_WARN("failed to match cell", K(ret));
    } else {
      if (NULL == curr_cell_.get_ob_row()) {
        LOG_DEBUG("[yzfdebug] matcher, curr_cell=NULL ", K(match_code));
      } else {
        LOG_DEBUG("[yzfdebug] matcher", K_(curr_cell), K(match_code));
      }
      switch(match_code) {
        case ObHTableMatchCode::INCLUDE:
        case ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW:
        case ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL:
        case ObHTableMatchCode::DONE_REVERSE_SCAN:
          if (NULL != hfilter_) {
            const ObHTableCell *new_cell = NULL;
            if (OB_FAIL(hfilter_->transform_cell(curr_cell_, new_cell))) {
              LOG_WARN("failed to tranform cell", K(ret));
              break;
            } else {
              // @todo FIXME real transform
            }
          }
          ++count_per_row_;
          // whether reach per row limit
          if (limit_per_row_per_cf_ > -1/*not unlimited*/
              && count_per_row_ > (offset_per_row_per_cf_ + limit_per_row_per_cf_)) {
            // do what SEEK_NEXT_ROW does.
            matcher_->clear_curr_row();
            if (ObQueryFlag::Reverse == scan_order_) {
              ret = add_same_kq_to_res(same_kq_cells, out_result);
              same_kq_cells.reset();
            } else {
              ret = seek_or_skip_to_next_row(curr_cell_);
            }
            loop = false;
            LOG_DEBUG("[yzfdebug] reach per row limit", K(ret), K_(offset_per_row_per_cf), K_(limit_per_row_per_cf), K_(count_per_row));
            break;
          }
          // whether skip offset
          if (count_per_row_ > offset_per_row_per_cf_) {
            if(OB_SUCC(ret) && ObQueryFlag::Reverse == scan_order_) {
              // reverse scan if the current cell has the same key and qualifier with the last, match_code = INCLUDE; else match_code = DONE_REVERSE_SCAN;
              // INCELUDE: put current cell into result vector
              // DONE_REVERSE_SCAN: end this round scan, choose cell to return
              ObString pre_key;
              ObString pre_qualifier;
              if (OB_SUCC(ret) && same_kq_cells.count() > 0) {
                if (OB_FAIL(same_kq_cells.at(same_kq_cells.count() - 1).get_cell(ObHTableConstants::COL_IDX_K).get_string(pre_key))) {
                  LOG_WARN("failed to get pre key!",K(ret));
                } else if (OB_FAIL(same_kq_cells.at(same_kq_cells.count() - 1).get_cell(ObHTableConstants::COL_IDX_Q).get_string(pre_qualifier))) {
                  LOG_WARN("failed to get pre qualifier",K(ret));
                } else {
                  if (OB_SUCC(ret)) {
                    if (pre_key != curr_cell_.get_rowkey() || pre_qualifier != curr_cell_.get_qualifier()) {
                      match_code = ObHTableMatchCode::DONE_REVERSE_SCAN;
                    } else {
                      match_code = ObHTableMatchCode::INCLUDE;
                    }
                  }
                }
              } else {
                match_code = ObHTableMatchCode::INCLUDE;
              }
            } else {
              int64_t timestamp = 0;
              if (OB_FAIL(curr_cell_.get_ob_row()->get_cell(ObHTableConstants::COL_IDX_T).get_int(timestamp))) {
                LOG_WARN("failed to get timestamp",K(ret));
              } else {
                const_cast<ObNewRow*>(curr_cell_.get_ob_row())->get_cell(ObHTableConstants::COL_IDX_T).set_int(-timestamp);
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(out_result->add_row(*(curr_cell_.get_ob_row())))) {
                  LOG_WARN("failed to add row to result", K(ret));
                } else {
                  ++cell_count_;
                  LOG_DEBUG("[yzfdebug] add cell", K_(cell_count), K_(curr_cell),
                            K_(count_per_row), K_(offset_per_row_per_cf));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_ROW == match_code) {
              matcher_->clear_curr_row();
              ret = seek_or_skip_to_next_row(curr_cell_);
            } else if (ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL == match_code) {
              ret = seek_or_skip_to_next_col(curr_cell_);
            } else if (ObHTableMatchCode::DONE_REVERSE_SCAN == match_code) {
              ret = add_same_kq_to_res(same_kq_cells, out_result);
              same_kq_cells.reset();
              ObNewRow new_row;
              if (OB_SUCC(ret) && OB_FAIL(ob_write_row(allocator_, *(curr_cell_.get_ob_row()), new_row))) {
                LOG_WARN("failed to copy row", K(ret));
              } else {
                if (OB_FAIL(same_kq_cells.push_back(new_row))) {
                  LOG_DEBUG("[sldebug] get curr_cell", K(*(curr_cell_.get_ob_row())), K_(curr_cell));
                  LOG_WARN("put this row failed",K(ret));
                } else if (OB_FAIL(reverse_next_cell(same_kq_cells, out_result))) {
                  LOG_WARN("get next cell failed in reverse scan",K(ret));
                }
              }
            } else {
              if (ObQueryFlag::Reverse == scan_order_) {
                ObNewRow new_row;
                if (OB_FAIL(ob_write_row(allocator_, *(curr_cell_.get_ob_row()), new_row))) {
                  LOG_WARN("failed to copy row", K(ret));
                } else {
                  if (OB_FAIL(same_kq_cells.push_back(new_row))) {
                  LOG_WARN("put this row failed",K(ret));
                } else if (OB_FAIL(reverse_next_cell(same_kq_cells, out_result))) {
                  LOG_WARN("get next cell failed in reverse scan",K(ret));
                }
                }
              } else {
                ret = next_cell();
              }
            }
            if (OB_SUCC(ret)) {
              if (reach_batch_limit() || reach_size_limit()) {
                loop = false;
              }
            } else if (OB_ITER_END == ret) {
              loop = false;
            }
          }
          break;
        case ObHTableMatchCode::DONE:
          // done current row
           if (ObQueryFlag::Reverse == scan_order_) {
            ret = add_same_kq_to_res(same_kq_cells, out_result);
            same_kq_cells.reset();
          }
          matcher_->clear_curr_row();
          loop = false;
          break;
        case ObHTableMatchCode::DONE_SCAN:
          has_more_cells_ = false;
          loop = false;
          break;
        case ObHTableMatchCode::SEEK_NEXT_ROW:
          if (ObQueryFlag::Reverse == scan_order_) {
            ret = reverse_next_cell(same_kq_cells, out_result);
          } else {
            matcher_->clear_curr_row();
            ret = seek_or_skip_to_next_row(curr_cell_);
          }
          break;
        case ObHTableMatchCode::SEEK_NEXT_COL:
          if (ObQueryFlag::Reverse == scan_order_) {
            ret = reverse_next_cell(same_kq_cells, out_result);
          } else {
            ret = seek_or_skip_to_next_col(curr_cell_);
          }
          break;
        case ObHTableMatchCode::SKIP:
          if (ObQueryFlag::Reverse == scan_order_) {
            ret = reverse_next_cell(same_kq_cells, out_result);
          } else {
            ret = next_cell();
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          break;
      }  // end switch
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }  // end while
  return ret;
}

/// Seek the scanner at or after the specified KeyValue.
int ObHTableRowIterator::seek(const ObHTableCell &key)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  while (OB_SUCC(next_cell()))
  {
    cmp_ret = ObHTableUtils::compare_cell(curr_cell_, key, scan_order_);
    if (cmp_ret >= 0) {
      LOG_DEBUG("[yzfdebug] seek to", K(key), K_(curr_cell));
      break;
    }
  }
  return ret;
}

int ObHTableRowIterator::seek_or_skip_to_next_row(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ObHTableCell* next_cell = NULL;
  if (OB_FAIL(ObHTableUtils::create_last_cell_on_row(allocator_, cell, next_cell))) {
    LOG_WARN("failed to create last cell", K(ret));
  } else {
    ret = seek(*next_cell);
    next_cell->~ObHTableCell();
    allocator_.reuse();
  }
  return ret;
}

int ObHTableRowIterator::seek_or_skip_to_next_col(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ObHTableCell* next_cell = NULL;
  if (OB_FAIL(matcher_->create_key_for_next_col(allocator_, cell, next_cell))) {
    LOG_WARN("failed to create next cell", K(ret));
  } else {
    ret = seek(*next_cell);
    next_cell->~ObHTableCell();
    allocator_.reuse();
  }
  return ret;
}

bool ObHTableRowIterator::reach_batch_limit() const
{
  // @todo currently not supported
  return false;
}

bool ObHTableRowIterator::reach_size_limit() const
{
  // @todo
  return false;
}

void ObHTableRowIterator::set_hfilter(table::hfilter::Filter *hfilter)
{
  hfilter_ = hfilter;
  if (nullptr != matcher_) {
    matcher_->set_hfilter(hfilter);
  }
}

void ObHTableRowIterator::set_ttl(int32_t ttl_value)
{
  time_to_live_ = ttl_value;
}

////////////////////////////////////////////////////////////////
ObHTableFilterOperator::ObHTableFilterOperator(const ObTableQuery &query,
                                               table::ObTableQueryResult &one_result)
    : ObTableQueryResultIterator(&query),
      row_iterator_(query),
      one_result_(&one_result),
      hfilter_(NULL),
      batch_size_(query.get_batch()),
      max_result_size_(std::min(query.get_max_result_size(),
                                static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024))),
      is_first_result_(true)
{
}

// @param one_result for one batch
int ObHTableFilterOperator::get_next_result(ObTableQueryResult *&next_result)
{
  int ret = OB_SUCCESS;
  one_result_->reset_except_property();
  bool has_filter_row = (NULL != hfilter_) && (hfilter_->has_filter_row());
  next_result = one_result_;
  ObTableQueryResult *htable_row = nullptr;
  // ObNewRow first_entity;
  // ObObj first_entity_cells[4];
  // first_entity.cells_ = first_entity_cells;
  // first_entity.count_ = 4;
  while (OB_SUCC(ret) && row_iterator_.has_more_result()
      && OB_SUCC(row_iterator_.get_next_result(htable_row))) {
    LOG_DEBUG("[yzfdebug] got one row", "cells_count", htable_row->get_row_count());
    bool is_empty_row = (htable_row->get_row_count() == 0);
    if (is_empty_row) {
      if (nullptr != hfilter_) {
        hfilter_->reset();
      }
      continue;
    }
    /*
    if (NULL != hfilter_) {
      // for RowFilter etc. which filter according to rowkey
      if (OB_FAIL(htable_row->get_first_row(first_entity))) {
        LOG_WARN("failed to get first cell", K(ret));
      } else {
        ObHTableCellEntity first_cell_entity(&first_entity);
        if (hfilter_->filter_row_key(first_cell_entity)) {
          // filter out the current row and fetch the next row
          hfilter_->reset();
          LOG_DEBUG("[yzfdebug] skip the row", K(ret));
          continue;
        }
      }
    }
    */
    bool exclude = false;
    if (has_filter_row) {
      // FIXME @todo allows direct modification of the final list to be submitted
      hfilter_->filter_row_cells(*htable_row);
      is_empty_row = (htable_row->get_row_count() == 0);
      if (!is_empty_row) {
        // last chance to drop entire row based on the sequence of filter calls
        if (hfilter_->filter_row()) {
          LOG_DEBUG("[yzfdebug] filter out the row");
          exclude = true;
        }
      }
    }
    if (is_empty_row || exclude) {
      if (NULL != hfilter_) {
        hfilter_->reset();
      }
      // fetch next row
      continue;
    }
    /* @todo check batch limit and size limit */
    // We have got one hbase row, store it to this batch
    if (OB_FAIL(one_result_->add_all_row(*htable_row))) {
      LOG_WARN("failed to add cells to row", K(ret));
    }
    if (NULL != hfilter_) {
      hfilter_->reset();
    }
    if (OB_SUCC(ret)) {
      if (one_result_->reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
        break;
      }
    }
  } // end while

  if (!row_iterator_.has_more_result()) {
    ret = OB_ITER_END;
  }

  if (OB_ITER_END == ret
      && one_result_->get_row_count() > 0) {
    ret = OB_SUCCESS;
  }

  LOG_DEBUG("[yzfdebug] get_next_result", K(ret), "row_count", one_result_->get_row_count());
  return ret;
}

int ObHTableFilterOperator::parse_filter_string(common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  const ObString &hfilter_string = query_->get_htable_filter().get_filter();
  if (hfilter_string.empty()) {
    hfilter_ = NULL;
  } else if (NULL == allocator) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (OB_FAIL(filter_parser_.init(allocator))) {
    LOG_WARN("failed to init filter_parser", K(ret));
  } else if (OB_FAIL(filter_parser_.parse_filter(hfilter_string, hfilter_))) {
    LOG_WARN("failed to parse filter", K(ret), K(hfilter_string));
  } else {
    LOG_DEBUG("[yzfdebug] parse filter success", K(hfilter_string), "hfilter", *hfilter_);
    row_iterator_.set_hfilter(hfilter_);
  }
  return ret;
}
