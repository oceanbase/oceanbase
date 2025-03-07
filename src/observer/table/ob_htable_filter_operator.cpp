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
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::table::hfilter;
using namespace oceanbase::share::schema;

int ObTableHbaseRowKeyDefaultCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = 0;
    for (int i = 0; i < ObHTableConstants::COL_IDX_T && cmp_ret == 0; ++i) {
      cmp_ret = lhs.get_cell(i).get_string().compare(rhs.get_cell(i).get_string());
    }
  }
  return ret;
}

bool ObTableHbaseRowKeyDefaultCompare::operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs)
{
  int cmp_ret = 0;
  result_code_ = compare(lhs, rhs, cmp_ret);
  return cmp_ret < 0;
}


int ObTableHbaseRowKeyReverseCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = lhs.get_cell(ObHTableConstants::COL_IDX_K).get_string().compare(rhs.get_cell(ObHTableConstants::COL_IDX_K).get_string());
    if (cmp_ret == 0) {
      cmp_ret = rhs.get_cell(ObHTableConstants::COL_IDX_Q).get_string().compare(lhs.get_cell(ObHTableConstants::COL_IDX_Q).get_string());
    }
  }
  return ret;
}

bool ObTableHbaseRowKeyReverseCompare::operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs)
{
  int cmp_ret = 0;
  result_code_ = compare(lhs, rhs, cmp_ret);
  return cmp_ret > 0;
}

// format: {"Hbase": {"TimeToLive": 3600, "MaxVersions": 3}}
int ObHColumnDescriptor::from_string(const common::ObString &kv_attributes)
{
  reset();
  int ret = OB_SUCCESS;
  ObKVAttr attr;
  if (kv_attributes.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(kv_attributes, attr))) {
    LOG_WARN("fail to parse kv attributes", K(ret), K(kv_attributes));
  } else {
    from_kv_attribute(attr);
  }
  return ret;
}

// format: {"Hbase": {"TimeToLive": 3600, "MaxVersions": 3}}
void ObHColumnDescriptor::from_kv_attribute(const ObKVAttr &kv_attributes)
{
  reset();
  if (kv_attributes.is_empty()) {
    // do nothing
  } else {
    max_version_ = kv_attributes.max_version_;
    time_to_live_ = kv_attributes.ttl_;
  }
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

void ObHTableColumnTracker::set_ttl(int32_t ttl_value)
{
  if (ttl_value > 0) {
    int64_t now = ObTimeUtility::current_time();
    now = now / 1000;  // us -> ms
    oldest_stamp_ = now - (ttl_value * 1000LL);
    LOG_DEBUG("set ttl", K(ttl_value), K(now), K_(oldest_stamp));
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

int ObHTableExplicitColumnTracker::init(const table::ObHTableFilter &htable_filter)
{
  int ret = OB_SUCCESS;
  max_versions_ = htable_filter.get_max_versions();
  const ObIArray<ObString> &qualifiers = htable_filter.get_columns();
  const int64_t N = qualifiers.count();
  if (OB_SUCC(ret) && N <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not use ExplicitColumnTracker", K(ret));
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    if (OB_FAIL(columns_.push_back(std::make_pair(qualifiers.at(i), 0)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }  // end for
  if (OB_SUCC(ret)) {
    // sort qualifiers
    ColumnCount *end = &columns_.at(columns_.count() - 1);
    ++end;
    lib::ob_sort(&columns_.at(0), end, ColumnCountComparator());
  }

  if (OB_SUCC(ret)) {
    // check duplicated qualifiers
    for (int64_t i = 0; OB_SUCCESS == ret && i < N - 1; ++i) {
      if (columns_.at(i).first == columns_.at(i + 1).first) {
        ret = OB_ERR_PARAM_DUPLICATE;
        LOG_WARN("duplicated qualifiers", K(ret), "cq", columns_.at(i).first, K(i));
      }
    }  // end for
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

int ObHTableExplicitColumnTracker::check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code)
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
  column_has_expired_ = false;
  curr_column_ = &columns_.at(curr_column_idx_);
  const int64_t N = columns_.count();
  for (int64_t i = 0; i < N; ++i) {
    columns_.at(i).second = 0;
  }  // end for
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

int32_t ObHTableExplicitColumnTracker::get_cur_version()
{
  int32_t cur_version = 0;
  if (OB_NOT_NULL(curr_column_)) {
    cur_version = curr_column_->second;
  }
  return cur_version;
}

////////////////////////////////////////////////////////////////
ObHTableWildcardColumnTracker::ObHTableWildcardColumnTracker()
    :allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     current_qualifier_(),
     current_count_(0)
{}

int ObHTableWildcardColumnTracker::init(const table::ObHTableFilter &htable_filter)
{
  int ret = OB_SUCCESS;
  max_versions_ = htable_filter.get_max_versions();
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
    cmp_ret = ObHTableUtils::compare_qualifier(cell.get_qualifier(), current_qualifier_);
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

int ObHTableWildcardColumnTracker::get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  UNUSED(cell);
  match_code = ObHTableMatchCode::SEEK_NEXT_COL;
  return OB_SUCCESS;
}

void ObHTableWildcardColumnTracker::reset()
{
  LOG_DEBUG("reset qualifier");
  column_has_expired_ = false;
  current_qualifier_.reset();
}

int32_t ObHTableWildcardColumnTracker::get_cur_version()
{
  return current_count_;
}

////////////////////////////////////////////////////////////////
ObHTableScanMatcher::ObHTableScanMatcher(const table::ObHTableFilter &htable_filter,
                                         ObHTableColumnTracker *column_tracker /*= nullptr*/)
    :time_range_(-htable_filter.get_max_stamp(), -htable_filter.get_min_stamp()),
     column_tracker_(column_tracker),
     hfilter_(NULL),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     curr_row_(),
     need_verify_cell_ttl_(false),
     now_(ObHTableUtils::current_time_millis())
{}

int ObHTableScanMatcher::is_cell_ttl_expired(const ObHTableCell &cell, bool &is_expired)
{
  int ret = OB_SUCCESS;
  is_expired = false;
  int64_t ttl_time = INT64_MAX;
  int64_t cell_ts = -cell.get_timestamp();
  if (OB_FAIL(cell.get_ttl(ttl_time))) {
    LOG_WARN("failed to get ttl", K(ret), K(cell));
  } else if (now_ - cell_ts > ttl_time) {
    is_expired = true;
  }
  return ret;
}

int ObHTableScanMatcher::pre_check(const ObHTableCell &cell, ObHTableMatchCode &match_code, bool &need_match_column)
{
  int ret = OB_SUCCESS;
  need_match_column = false;
  bool is_expired = false;
  if (is_curr_row_empty()) {
    // Since the curCell is null it means we are already sure that we have moved over to the next
    // row
    match_code = ObHTableMatchCode::DONE;
  } else if (0 != ObHTableUtils::compare_rowkey(curr_row_, cell)) {
    // if row key is changed, then we know that we have moved over to the next row
    // WildcardColumnTracker will come to this branch
    match_code = ObHTableMatchCode::DONE;
    LOG_DEBUG("row changed", K(match_code), K(cell));
  } else if (column_tracker_->done()) {
    match_code = ObHTableMatchCode::SEEK_NEXT_ROW;
  }
  // check for early out based on timestamp alone
  else if (column_tracker_->is_done(cell.get_timestamp())) {
    if (OB_FAIL(column_tracker_->get_next_column_or_row(cell, match_code))) {
      LOG_WARN("failed to get next column or row", K(ret), K(cell));
    }
  } else if (need_verify_cell_ttl_ && OB_FAIL(is_cell_ttl_expired(cell, is_expired))) {
    LOG_WARN("failed to check cell ttl expired", K(ret), K(cell));
  } else if (is_expired) {
    match_code = ObHTableMatchCode::SKIP;
  } else {
    // continue
    need_match_column = true;
  }
  LOG_DEBUG("pre_check", K(ret), K(match_code), K(need_match_column));
  return ret;
}

int ObHTableScanMatcher::match_column(const ObHTableCell &cell, ObHTableMatchCode &match_code)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = cell.get_timestamp();
  int cmp_tr = time_range_.compare(timestamp);
  LOG_DEBUG("compare time range", K(timestamp), K(cmp_tr), K_(time_range));
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
      switch (match_code) {
        case ObHTableMatchCode::SKIP:
        case ObHTableMatchCode::SEEK_NEXT_COL:
          break;
        default: {
          if (NULL != hfilter_) {
            hfilter::Filter::ReturnCode filter_rc = hfilter::Filter::ReturnCode::INCLUDE;
            if (OB_FAIL(hfilter_->filter_cell(cell, filter_rc))) {
              LOG_WARN("failed to filter cell", K(ret));
            } else {
              ObHTableMatchCode orig_code = match_code;
              match_code = merge_filter_return_code(cell, match_code, filter_rc);
              LOG_DEBUG("filter cell", K(filter_rc), K(orig_code), K(match_code));
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
  UNUSED(cell);
  UNUSED(match_code);
  UNUSED(filter_rc);
  switch (filter_rc) {
    case Filter::ReturnCode::INCLUDE:
      break;
    case Filter::ReturnCode::INCLUDE_AND_NEXT_COL:
      if (ObHTableMatchCode::INCLUDE == match_code) {
        ret_code = ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL;
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
      if (match_code == ObHTableMatchCode::INCLUDE || match_code == ObHTableMatchCode::INCLUDE_AND_SEEK_NEXT_COL) {
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
      LOG_WARN("failed to match column", K(ret), K(cell), K(match_code));
    }
  }
  return ret;
}

const ObHTableCell *ObHTableScanMatcher::get_curr_row() const
{
  const ObHTableCell *p = NULL;
  if (NULL != curr_row_.get_ob_row()) {
    p = &curr_row_;
  }
  return p;
}

int ObHTableScanMatcher::set_to_new_row(const ObHTableCell &arg_curr_row)
{
  int ret = OB_SUCCESS;
  const ObHTableCellEntity &curr_row = dynamic_cast<const ObHTableCellEntity &>(arg_curr_row);
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

int ObHTableScanMatcher::get_next_cell_hint(const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  if (hfilter_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter is null", K(ret));
  } else if (OB_FAIL(hfilter_->get_next_cell_hint(allocator_, cell, new_cell))) {
    LOG_WARN("failed to get next hint cell of filter", K(ret), K(cell));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObHTableRowIterator::ObHTableRowIterator(const ObTableQuery &query)
    : ObTableQueryResultIterator(&query),
      allocator_("HtbRowItAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      forward_range_alloc_("HtbForwRanAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      child_op_(NULL),
      curr_cell_(),
      scan_order_(query.get_scan_order()),
      matcher_(NULL),
      has_more_cells_(true),
      is_inited_(false),
      need_verify_cell_ttl_(false),
      htable_filter_(query.get_htable_filter()),
      hfilter_(NULL),
      limit_per_row_per_cf_(htable_filter_.get_max_results_per_column_family()),
      offset_per_row_per_cf_(htable_filter_.get_row_offset_per_column_family()),
      max_result_size_(query.get_max_result_size()),
      batch_size_(query.get_batch()),
      time_to_live_(0),
      max_version_(0),
      column_tracker_(NULL),
      column_tracker_wildcard_(),
      column_tracker_explicit_(),
      matcher_impl_(htable_filter_),
      cell_count_(0),
      count_per_row_(0),
      is_first_result_(true),
      is_table_group_inited_(false),
      is_table_group_req_(false),
      family_name_(),
      is_cur_row_expired_(false),
      allow_partial_results_(false),
      is_cache_block_(true),
      scanner_context_(NULL)
{
  if (query.get_scan_ranges().count() != 0) {
    start_row_key_ = query.get_scan_ranges().at(0).start_key_;
    stop_row_key_ = query.get_scan_ranges().at(0).end_key_;
  }
}

int ObHTableRowIterator::next_cell()
{
  ObNewRow *ob_row = NULL;
  int ret = child_op_->get_next_row(ob_row);
  if (OB_SUCCESS == ret) {
    curr_cell_.set_ob_row(ob_row);
    if (OB_FAIL(try_record_expired_rowkey(curr_cell_))) {
      LOG_WARN("failed to record expired rowkey", K(ret));
    }
    LOG_DEBUG("[yzfdebug] fetch next cell", K_(curr_cell));
  } else if (OB_ITER_END == ret) {
    has_more_cells_ = false;
    curr_cell_.set_ob_row(NULL);
    matcher_->clear_curr_row();
    LOG_DEBUG("iterator end", K_(has_more_cells));
  }
  return ret;
}

int ObHTableRowIterator::get_next_cell_hint()
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

int ObHTableRowIterator::get_next_result(ObTableQueryDListResult *&out_result)
{
  int ret = OB_SUCCESS;
  out_result = &one_iterable_hbase_row_;
  one_iterable_hbase_row_.reset();
  if (OB_FAIL(get_next_result_internal(out_result))) {
    LOG_WARN("fail to get next result", K(ret));
  }
  return ret;
}

int ObHTableRowIterator::get_next_result(ObTableQueryResult *&out_result)
{
  int ret = OB_SUCCESS;
  out_result = &one_hbase_row_;
  one_hbase_row_.reset();
  if (OB_FAIL(get_next_result_internal(out_result))) {
    LOG_WARN("fail to get next result", K(ret));
  }
  return ret;
}

template <typename ResultType>
int ObHTableRowIterator::get_next_result_internal(ResultType*& result)
{
  int ret = OB_SUCCESS;
  ObHTableMatchCode match_code = ObHTableMatchCode::DONE_SCAN;  // initialize
  bool has_filter_row = (NULL != hfilter_) && (hfilter_->has_filter_row());
  if (scanner_context_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scanner_context_ meet unexecpted nullptr", K(ret));
  }
  if (OB_SUCC(ret) && NULL == column_tracker_) {
    // first iteration
    if (htable_filter_.get_columns().count() <= 0) {
      column_tracker_ = &column_tracker_wildcard_;
    } else {
      column_tracker_ = &column_tracker_explicit_;
    }
    if (OB_FAIL(column_tracker_->init(htable_filter_))) {
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
  if (OB_SUCC(ret)) {
    if (has_filter_row) {
      scanner_context_->limits_.set_size_scope(LimitScope::Scope::BETWEEN_ROWS);
      scanner_context_->limits_.set_time_scope(LimitScope::Scope::BETWEEN_ROWS);
    } else if (allow_partial_results_) {
      scanner_context_->limits_.set_size_scope(LimitScope::Scope::BETWEEN_CELLS);
      scanner_context_->limits_.set_time_scope(LimitScope::Scope::BETWEEN_CELLS);
    }
  }
  bool loop = true;
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
        // loop = false;
        ret = seek_or_skip_to_next_row(curr_cell_);
      }
    }
  }
  while (OB_SUCC(ret) && loop) {
    match_code = ObHTableMatchCode::DONE_SCAN;  // initialize
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
            // do what SEEK_NEXT_ROW does.
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
              if (OB_FAIL(add_new_row(*(curr_cell_.get_ob_row()), result))) {
                LOG_WARN("failed to add row to result", K(ret));
              } else {
                scanner_context_->increment_batch_progress(1);
                scanner_context_->increment_size_progress(curr_cell_.get_ob_row()->get_serialize_size());
                ++cell_count_;
                LOG_DEBUG(
                    "[yzfdebug] add cell", K_(cell_count), K_(curr_cell), K_(count_per_row), K_(offset_per_row_per_cf));
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
  return ret;
}

void ObHTableRowIterator::init_table_group_value()
{
  if (is_table_group_inited_) {
    // has inited, do nothing
  } else {
    is_table_group_inited_ = true;
    is_table_group_req_ = child_op_->get_scan_executor()->get_table_ctx().is_tablegroup_req();
    // get family name
    ObString table_name = child_op_->get_scan_executor()->get_table_ctx().get_table_name();
    family_name_ = table_name.after('$');
  }
}

int ObHTableRowIterator::add_new_row(const ObNewRow &row, ObTableQueryDListResult *&out_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_result->add_row(row, family_name_))) {
    LOG_WARN("failed to add row to result", K(ret));
  }
  return ret;
}

int ObHTableRowIterator::add_new_row(const ObNewRow &row, ObTableQueryResult *&out_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_result->add_row(row))) {
    LOG_WARN("failed to add row to result", K(ret));
  }
  return ret;
}

/// Seek the scanner at or after the specified KeyValue.
/// return the skipped cell count
int ObHTableRowIterator::seek(ObHTableCell &key, int32_t &skipped_count)
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
int ObHTableRowIterator::rescan_and_get_next_row(table::ObTableApiScanRowIterator *tb_op, ObNewRow *&ob_next_row)
{
  int ret = OB_SUCCESS;
  ObNewRow *tmp_next_row = nullptr;
  if (OB_ISNULL(tb_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tb_op is null", K(ret));
  } else if (OB_FAIL(tb_op->get_scan_executor()->rescan())) {
    LOG_WARN("failed to rescan executor", K(ret));
  } else {
    if (OB_FAIL(tb_op->get_next_row(tmp_next_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else {
      ob_next_row = tmp_next_row;
    }
  }
  return ret;
}

int ObHTableRowIterator::seek_first_cell_on_hint(const ObNewRow *ob_row)
{
  int ret = OB_SUCCESS;
  ObTableApiScanRowIterator *forward_child_op = get_forward_child_op();
  ObTableCtx &forward_tb_ctx = forward_child_op->get_scan_executor()->get_table_ctx();
  ObNewRange &forward_range = forward_tb_ctx.get_key_ranges().at(0);
  forward_tb_ctx.set_limit(-1);
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
    if (OB_FAIL(rescan_and_get_next_row(forward_child_op, first_cell_on_row))) {
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


int ObHTableRowIterator::seek_or_skip_to_next_row(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  int32_t cur_version = column_tracker_->get_cur_version();
  ObHTableCell *next_cell = nullptr;

  bool enter_next_key = false;
  while (OB_SUCC(ret) && !enter_next_key) {
    if (OB_FAIL(ObHTableUtils::create_last_cell_on_row_col(allocator_, curr_cell_, next_cell))) {
      LOG_WARN("failed to create last cell", K(ret));
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

int ObHTableRowIterator::seek_or_skip_to_next_col(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ObHTableCell *next_cell = NULL;
  if (OB_FAIL(ObHTableUtils::create_last_cell_on_row_col(allocator_, cell, next_cell))) {
    LOG_WARN("failed to create next cell", K(ret));
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

void ObHTableRowIterator::set_need_verify_cell_ttl(bool need_verify_cell_ttl) {
  need_verify_cell_ttl_ = need_verify_cell_ttl;
  matcher_impl_.set_need_verify_cell_ttl(need_verify_cell_ttl);
}

int ObHTableRowIterator::try_record_expired_rowkey(const ObHTableCellEntity &cell)
{
  int ret = OB_SUCCESS;
  if (!is_cur_row_expired_) {
    if (need_verify_cell_ttl_ && OB_NOT_NULL(matcher_) && OB_FAIL(matcher_->is_cell_ttl_expired(cell, is_cur_row_expired_))) {
      LOG_WARN("failed to is cell ttl expired", K(ret));
    } else if (is_cur_row_expired_ || (time_to_live_ > 0 && OB_NOT_NULL(column_tracker_) &&
                                          column_tracker_->is_expired(cell.get_timestamp()))) {
      is_cur_row_expired_ = true;
      if (OB_NOT_NULL(child_op_) && OB_NOT_NULL(child_op_->get_scan_executor())) {
        ObTableCtx &tb_ctx = child_op_->get_scan_executor()->get_table_ctx();
        if (!tb_ctx.is_index_scan()) {
          MTL(ObHTableRowkeyMgr *)
              ->record_htable_rowkey(
                  tb_ctx.get_ls_id(), tb_ctx.get_table_id(), tb_ctx.get_tablet_id(), cell.get_rowkey());
        }
      }
    }
  }
  return ret;
}

void ObHTableRowIterator::try_record_expired_rowkey(const int32_t versions, const ObString &rowkey)
{
  if (max_version_ > 0 && !is_cur_row_expired_ && versions > max_version_) {
    is_cur_row_expired_ = true;
    if (OB_NOT_NULL(child_op_) && OB_NOT_NULL(child_op_->get_scan_executor())) {
      ObTableCtx &tb_ctx = child_op_->get_scan_executor()->get_table_ctx();
      if (!tb_ctx.is_index_scan()) {
        MTL(ObHTableRowkeyMgr*)->record_htable_rowkey(tb_ctx.get_ls_id(),
                                                      tb_ctx.get_table_id(),
                                                      tb_ctx.get_tablet_id(),
                                                      rowkey);
      }
    }
  }
}

////////////////////////////////////////////////////////////////

ObHTableReversedRowIterator::ObHTableReversedRowIterator(const ObTableQuery &query)
    : ObHTableRowIterator(query),
      iter_allocator_("HtbRevIterAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      reversed_range_alloc_("HtbRevRanAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      spec_(nullptr),
      forward_tb_ctx_(iter_allocator_),
      expr_frame_info_(iter_allocator_)
{}

ObHTableReversedRowIterator::~ObHTableReversedRowIterator()
{
  if (OB_NOT_NULL(forward_child_op_)) {
    forward_child_op_->close();
  }
  if (OB_NOT_NULL(spec_)) {
    forward_tb_ctx_.set_expr_info(nullptr);
    if (OB_NOT_NULL(forward_child_op_)) {
      spec_->destroy_executor(forward_child_op_->get_scan_executor());
    }
    if (is_query_async_) {
      spec_->~ObTableApiSpec();
    }
  }
}

int ObHTableReversedRowIterator::init()
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("reverse iter inited twice", K(ret));
  } else if (OB_ISNULL(forward_child_op_ = OB_NEWx(ObTableApiScanRowIterator, &iter_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("forward_child_op_ alloc memory error", K(ret));
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
  } else if (OB_ISNULL(child_op_->get_scan_executor())) {
    ret = OB_INIT_FAIL;
    LOG_WARN("scan_executort is NULL", K(ret));
  } else if (OB_FAIL(create_forward_child_op())) {
    LOG_WARN("create_reversed_child_op failed", K(ret));
  } else {
    // curr_cell_ needs to be initialized, and the pointer should be directed
    // to either the last `startKey` or the first cell of the preceding row.
    ObNewRow *tmp_next_row = nullptr;
    ObHTableCell *new_cell = nullptr;
    if (OB_FAIL(child_op_->get_next_row(tmp_next_row))) {
      if (OB_ITER_END == ret) {
        has_more_cells_ = false;
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
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObHTableReversedRowIterator::next_cell()
{
  int ret = OB_SUCCESS;
  ObNewRow *ob_row = NULL;
  ObHTableCellEntity cell_clone;
  if (OB_FAIL(cell_clone.deep_copy_ob_row(curr_cell_.get_ob_row(), allocator_))) {
    LOG_WARN("allocate memory for row_key failed", K(ret));
  } else if (OB_FAIL(forward_child_op_->get_next_row(ob_row))) {
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
int ObHTableReversedRowIterator::seek_or_skip_to_next_row(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  reversed_range_alloc_.reuse();
  if (OB_FAIL(ob_write_string(reversed_range_alloc_, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("allocate memory for row_key failed", K(ret));
  } else if (OB_FAIL(seek_or_skip_to_next_row_inner(rowkey_clone))) {
    LOG_WARN("failed to seek or skip to next row", K(ret));
  }
  return ret;
}

int ObHTableReversedRowIterator::seek_or_skip_to_next_row(const ObString &rowkey)
{
  int ret = OB_SUCCESS;
  reversed_range_alloc_.reuse();
  if (OB_FAIL(seek_or_skip_to_next_row_inner(rowkey))) {
    LOG_WARN("failed to seek or skip to next row", K(ret));
  }
  return ret;
}

int ObHTableReversedRowIterator::seek_or_skip_to_next_row_inner(const ObString &rowkey)
{
  int ret = OB_SUCCESS;
  ObTableCtx &reversed_tb_ctx = child_op_->get_scan_executor()->get_table_ctx();
  ObNewRange &reverse_range = reversed_tb_ctx.get_key_ranges().at(0);
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
    if (OB_FAIL(rescan_and_get_next_row(child_op_, ob_next_row))) {
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

int ObHTableReversedRowIterator::seek_or_skip_to_next_col(const ObHTableCell &cell)
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
    LOG_WARN("failed to seek_or_skip_to_next_col", K(ret));
  }
  return ret;
}

int ObHTableReversedRowIterator::seek_or_skip_to_next_col(const ObString &rowkey, const ObString &qualifier)
{
  int ret = OB_SUCCESS;
  forward_range_alloc_.reuse();
  if (OB_FAIL(seek_or_skip_to_next_col_inner(rowkey, qualifier))) {
    LOG_WARN("failed to seek_or_skip_to_next_col", K(ret));
  }
  return ret;
}

int ObHTableReversedRowIterator::seek_or_skip_to_next_col_inner(const ObString &rowkey, const ObString &qualifier)
{
  int ret = OB_SUCCESS;
  forward_tb_ctx_.set_limit(-1);
  ObNewRange &column_range = forward_tb_ctx_.get_key_ranges().at(0);
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
    if (OB_FAIL(rescan_and_get_next_row(forward_child_op_, ob_next_column_row))) {
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

int ObHTableReversedRowIterator::seek(ObHTableCell &key, int32_t &skipped_count)
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

int ObHTableReversedRowIterator::init_forward_tb_ctx()
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  if (OB_FAIL(cache_guard_.init(&forward_tb_ctx_))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard_.get_expr_info(&forward_tb_ctx_, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(forward_tb_ctx_, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(forward_tb_ctx_.init_exec_ctx())) {
    LOG_WARN("fail to init exec forward_tb_ctx_", K(ret), K(forward_tb_ctx_));
  } else {
    forward_tb_ctx_.set_init_flag(true);
    forward_tb_ctx_.set_expr_info(expr_frame_info);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cache_guard_.get_spec<TABLE_API_EXEC_SCAN>(&forward_tb_ctx_, spec_))) {
      LOG_WARN("fail to get spec from cache", K(ret));
    }
  }
  return ret;
}

int ObHTableReversedRowIterator::init_async_forward_tb_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(forward_tb_ctx_.cons_column_items_for_cg())) {
    LOG_WARN("fail to construct column items", K(ret));
  } else if (OB_FAIL(forward_tb_ctx_.generate_table_schema_for_cg())) {
    LOG_WARN("fail to generate table schema", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::generate_exprs(forward_tb_ctx_, iter_allocator_, expr_frame_info_))) {
    LOG_WARN("fail to generate exprs", K(ret), K(forward_tb_ctx_));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(forward_tb_ctx_, expr_frame_info_))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(forward_tb_ctx_.classify_scan_exprs())) {
    LOG_WARN("fail to classify scan exprs", K(ret));
  } else if (OB_FAIL(forward_tb_ctx_.init_exec_ctx())) {
    LOG_WARN("fail to init exec forward_tb_ctx_", K(ret), K(forward_tb_ctx_));
  } else {
    forward_tb_ctx_.set_init_flag(true);
    forward_tb_ctx_.set_expr_info(&expr_frame_info_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableSpecCgService::generate<TABLE_API_EXEC_SCAN>(iter_allocator_, forward_tb_ctx_, spec_))) {
      LOG_WARN("fail to generate scan spec", K(ret), K(forward_tb_ctx_));
    }
  }
  return ret;
}

int ObHTableReversedRowIterator::create_forward_child_op()
{
  int ret = OB_SUCCESS;
  table::ObTableCtx &reversed_tb_ctx = child_op_->get_scan_executor()->get_table_ctx();
  ObTableApiExecutor *api_executor = nullptr;
  forward_tb_ctx_.set_scan(true);
  forward_tb_ctx_.set_entity_type(reversed_tb_ctx.get_entity_type());
  forward_tb_ctx_.set_schema_cache_guard(reversed_tb_ctx.get_schema_cache_guard());
  forward_tb_ctx_.set_table_schema(reversed_tb_ctx.get_table_schema());
  forward_tb_ctx_.set_schema_guard(reversed_tb_ctx.get_schema_guard());
  forward_tb_ctx_.set_simple_table_schema(reversed_tb_ctx.get_simple_table_schema());
  forward_tb_ctx_.set_sess_guard(reversed_tb_ctx.get_sess_guard());
  forward_tb_ctx_.set_is_tablegroup_req(reversed_tb_ctx.is_tablegroup_req());
  forward_tb_ctx_.set_read_latest(reversed_tb_ctx.is_read_latest());

  if (forward_tb_ctx_.is_init()) {
    LOG_INFO("forward_tb_ctx_ has been inited", K_(forward_tb_ctx));
  } else if (OB_FAIL(forward_tb_ctx_.init_common(*const_cast<ObTableApiCredential *>(reversed_tb_ctx.get_credential()),
                 reversed_tb_ctx.get_tablet_id(),
                 reversed_tb_ctx.get_timeout_ts()))) {
    LOG_WARN("fail to init table forward_ctx common part", K(ret), K(reversed_tb_ctx.get_table_name()));
  } else if (OB_FAIL(forward_tb_ctx_.init_scan(*query_, reversed_tb_ctx.is_weak_read(),
                     reversed_tb_ctx.get_table_id()))) {
    LOG_WARN("fail to init table forward_ctx scan part", K(ret), K(forward_tb_ctx_));
  }
  if (OB_SUCC(ret)) {
    if (!is_query_async_) {
      ret = init_forward_tb_ctx();
    } else {
      ret = init_async_forward_tb_ctx();
    }
    if (OB_SUCC(ret)) {
      forward_tb_ctx_.set_scan_order(ObQueryFlag::Forward);
      if (OB_FAIL(forward_tb_ctx_.init_trans(reversed_tb_ctx.get_session_info().get_tx_desc(),
              reversed_tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot()))) {
        LOG_WARN("fail to init trans", K(ret), K(forward_tb_ctx_));
      } else if (OB_FAIL(spec_->create_executor(forward_tb_ctx_, api_executor))) {
        LOG_WARN("fail to generate executor", K(ret), K(forward_tb_ctx_));
      } else if (OB_FAIL(forward_child_op_->open(static_cast<ObTableApiScanExecutor *>(api_executor)))) {
        LOG_WARN("fail to open forward child op", K(ret));
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObHTableFilterOperator::ObHTableFilterOperator(const ObTableQuery &query, table::ObTableQueryResult &one_result)
    : ObTableQueryResultIterator(&query),
      row_iterator_(NULL),
      one_result_(&one_result),
      batch_size_(query.get_batch()),
      ob_kv_params_(query.get_ob_params()),
      caching_(-1),
      batch_(query.get_batch()),
      max_result_size_(std::min(query.get_max_result_size(),
                                static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024))),
      is_first_result_(true),
      check_existence_only_(false),
      scanner_context_(),
      is_inited_(false),
      row_count_(0)
{
}

ObHTableFilterOperator::ObHTableFilterOperator(const ObTableQuery &query,
                                               table::ObTableQueryIterableResult &one_result)
    : ObTableQueryResultIterator(&query),
      row_iterator_(NULL),
      iterable_result_(&one_result),
      batch_size_(query.get_batch()),
      ob_kv_params_(query.get_ob_params()),
      caching_(-1),
      batch_(query.get_batch()),
      max_result_size_(std::min(query.get_max_result_size(),
          static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024))),
      is_first_result_(true),
      check_existence_only_(false),
      scanner_context_(),
      is_inited_(false),
      row_count_(0)
{}

ObHTableFilterOperator::~ObHTableFilterOperator()
{
  if (OB_NOT_NULL(row_iterator_)) {
    row_iterator_->~ObHTableRowIterator();
  }
}

bool ObHTableFilterOperator::reach_limit()
{
  return query_->get_limit() > 0 && row_count_ >= query_->get_limit();
}

bool ObHTableFilterOperator::reach_caching_limit(int num_of_row)
{
  return caching_ >= 0 && num_of_row >= caching_;
}

// @param one_result for one batch
int ObHTableFilterOperator::get_next_result(ObTableQueryResult *&next_result)
{
  int ret = OB_SUCCESS;
  one_result_->reset_except_property();
  next_result = one_result_;

  if (OB_FAIL(get_next_result_internal(one_result_))) {
    LOG_WARN("fail to get next result", K(ret));
  }
  return ret;
}

int ObHTableFilterOperator::get_next_result(ObTableQueryIterableResult *&next_result)
{
  int ret = OB_SUCCESS;
  next_result = iterable_result_;
  if (OB_FAIL(get_next_result_internal(iterable_result_))) {
    LOG_WARN("fail to get next result", K(ret));
  }
  return ret;
}

template <typename ResultType>
int ObHTableFilterOperator::get_next_result_internal(ResultType *&next_result)
{
  int ret = OB_SUCCESS;
  bool has_filter_row = (NULL != filter_) && (filter_->has_filter_row());
  ObTableQueryDListResult *htable_row = nullptr;
// ObNewRow first_entity;
  // ObObj first_entity_cells[4];
  // first_entity.cells_ = first_entity_cells;
  // first_entity.count_ = 4;
  int num_of_row = 0;
  scanner_context_.progress_.reset();

  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obHTableFilterOperator has not init", K(ret));
  } else if (!row_iterator_->is_inited()) {
    if (OB_FAIL(row_iterator_->init())) {
      LOG_WARN("hbase iterator init error", K(ret));
    } else {
      row_iterator_->init_table_group_value();
      row_iterator_->is_inited() = true;
    }
  }
  while (OB_SUCC(ret) && row_iterator_->has_more_result() && !reach_caching_limit(num_of_row) &&
         OB_SUCC(row_iterator_->get_next_result(htable_row))) {
    LOG_DEBUG("got one row", "cells_count", htable_row->get_row_count());
    bool is_empty_row = (htable_row->get_row_count() == 0);
    if (is_empty_row) {
      if (nullptr != filter_) {
        filter_->reset();
      }
      continue;
    }
    /*
    if (NULL != filter_) {
      // for RowFilter etc. which filter according to rowkey
      if (OB_FAIL(htable_row->get_first_row(first_entity))) {
        LOG_WARN("failed to get first cell", K(ret));
      } else {
        ObHTableCellEntity first_cell_entity(&first_entity);
        if (filter_->filter_row_key(first_cell_entity)) {
          // filter out the current row and fetch the next row
          filter_->reset();
          LOG_DEBUG("skip the row", K(ret));
          continue;
        }
      }
    }
    */
    bool exclude = false;
    if (has_filter_row) {
      // FIXME @todo allows direct modification of the final list to be submitted
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

    // if only check exist, just return row_count_ as 1
    if (check_existence_only_) {
      next_result->save_row_count_only(1);
      ret = OB_ITER_END;
      break;
    }

    /* @todo check batch limit and size limit */
    // We have got one hbase row, store it to this batch
    if (OB_FAIL(next_result->add_all_row(*htable_row))) {
      LOG_WARN("failed to add cells to row", K(ret));
    }
    if (NULL != filter_) {
      filter_->reset();
    }
    if (OB_SUCC(ret)) {
      if (next_result->reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
        break;
      }
    }
    num_of_row++;
    row_count_++;
    if (reach_limit()) {
      row_iterator_->no_more_result();
    }
  }  // end while

  if (!row_iterator_->has_more_result()) {
    ret = OB_ITER_END;
  }

  if (OB_ITER_END == ret && next_result->get_row_count() > 0) {
    ret = OB_SUCCESS;
  }

  LOG_DEBUG("get_next_result", K(ret), "row_count", next_result->get_row_count());
  return ret;
}

void ObHTableFilterOperator::set_query_async()
{
  is_query_async_ = true;
  if (OB_NOT_NULL(row_iterator_)) {
    row_iterator_->set_query_async();
  }
}

int ObHTableFilterOperator::init(common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  scanner_context_.limits_.set_batch(batch_);
  const ObHBaseParams *hbase_params = nullptr;
  if (OB_ISNULL(allocator)) {
    ret = OB_INIT_FAIL;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("bbHTableFilterOperator inited twice", K(ret));
  } else {
    if (ObQueryFlag::Reverse == query_->get_scan_order()) {
      if (OB_ISNULL(row_iterator_ = OB_NEWx(ObHTableReversedRowIterator, allocator, *query_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("row_iterator_ init nullptr", K(ret));
      }
    } else {
      if (OB_ISNULL(row_iterator_ = OB_NEWx(ObHTableRowIterator, allocator, *query_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("row_iterator_ init nullptr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const ObString &hfilter_string = query_->get_htable_filter().get_filter();
      if (hfilter_string.empty()) {
        filter_ = NULL;
      } else if (OB_FAIL(filter_parser_.init(allocator))) {
        LOG_WARN("failed to init filter_parser", K(ret));
      } else if (OB_FAIL(filter_parser_.parse_filter(hfilter_string, filter_))) {
        LOG_WARN("failed to parse filter", K(ret), K(hfilter_string));
      } else {
        filter_->set_reversed(ObQueryFlag::Reverse == query_->get_scan_order());
        row_iterator_->set_hfilter(filter_);
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(ob_kv_params_.ob_params_)) {
        if (OB_FAIL(ob_kv_params_.init_ob_params_for_hfilter(hbase_params))) {
          LOG_WARN("init ob_params fail", K(ret), K(ob_kv_params_));
        } else {
          caching_ = hbase_params->caching_;
          scanner_context_.limits_.set_fields(
              max_result_size_, hbase_params->call_timeout_, LimitScope(LimitScope::Scope::BETWEEN_ROWS));
          check_existence_only_ = hbase_params->check_existence_only_;
          row_iterator_->set_allow_partial_results(hbase_params->allow_partial_results_);
          row_iterator_->set_is_cache_block(hbase_params->is_cache_block_);
          if (NULL != filter_ && !hbase_params->hbase_version_.empty()) {
            filter_->set_hbase_version(hbase_params->hbase_version_);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row_iterator_->set_scanner_context(&scanner_context_);
    is_inited_ = true;
    LOG_DEBUG("obHTableFilterOperator init success", K(ret));
  }
  return ret;
}

ScannerContext::ScannerContext()
{
  limits_.set_fields(LIMIT_DEFAULT_VALUE, LIMIT_DEFAULT_VALUE, LIMIT_DEFAULT_VALUE, LimitScope::Scope::BETWEEN_ROWS);
  progress_.set_fields(
      PROGRESS_DEFAULT_VALUE, PROGRESS_DEFAULT_VALUE, PROGRESS_DEFAULT_VALUE, LimitScope::Scope::BETWEEN_ROWS);
}

ScannerContext::ScannerContext(int32_t batch, int64_t size, int64_t time, LimitScope limit_scope)
{
  limits_.set_fields(batch, size, time, limit_scope);
}

void ScannerContext::increment_batch_progress(int32_t batch)
{
  int32_t current_batch = progress_.get_batch();
  progress_.set_batch(current_batch + batch);
}

void ScannerContext::increment_size_progress(int64_t size)
{
  int64_t current_size = progress_.get_size();
  progress_.set_size(current_size + size);
}

bool ScannerContext::check_batch_limit(LimitScope checker_scope)
{
  bool ret = false;
  if (limits_.can_enforce_batch_from_scope(checker_scope) && limits_.get_batch() > 0) {
    ret = progress_.get_batch() >= limits_.get_batch();
  }
  return ret;
}

bool ScannerContext::check_size_limit(LimitScope checker_scope)
{
  bool ret = false;
  if (limits_.can_enforce_size_from_scope(checker_scope) && limits_.get_size() > 0) {
    ret = progress_.get_size() >= limits_.get_size();
  }
  return ret;
}

bool ScannerContext::check_time_limit(LimitScope checker_scope)
{
  bool ret = false;
  if (limits_.can_enforce_time_from_scope(checker_scope) && limits_.get_time() > 0) {
    ret = progress_.get_time() >= limits_.get_time();
  }
  return ret;
}

bool ScannerContext::check_any_limit(LimitScope checker_scope)
{
  return check_batch_limit(checker_scope) || check_size_limit(checker_scope) || check_time_limit(checker_scope);
}
