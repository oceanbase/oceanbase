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

#ifndef _OB_HTABLE_FILTER_OPERATOR_H
#define _OB_HTABLE_FILTER_OPERATOR_H 1
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "share/table/ob_table.h"
#include "ob_table_filter.h"
#include "common/row/ob_row_iterator.h"
#include "ob_htable_utils.h"
#include "ob_htable_filter_parser.h"
#include "ob_htable_filters.h"
#include "ob_table_scan_executor.h"
#include <utility>
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace table
{
class ObHColumnDescriptor final
{
public:
  ObHColumnDescriptor()
      :time_to_live_(0),
       max_version_(0)
  {}
  void reset();
  int from_string(const common::ObString &kv_attributes);

  void set_time_to_live(int32_t v) { time_to_live_ = v; }
  int32_t get_time_to_live() const { return time_to_live_; }
  void set_max_version(int32_t v) { max_version_ = v; }
  int32_t get_max_version() const { return max_version_; }
private:
  int32_t time_to_live_; // Time-to-live of cell contents, in seconds.
  int32_t max_version_;
};

enum class ObHTableMatchCode
{
  INCLUDE = 0,
  INCLUDE_AND_SEEK_NEXT_COL = 1,
  INCLUDE_AND_SEEK_NEXT_ROW = 2,
  SKIP = 3,
  SEEK_NEXT_COL = 4,
  SEEK_NEXT_ROW = 5,
  SEEK_NEXT_USING_HINT = 6,
  DONE = 7,
  DONE_SCAN = 8,
  DONE_REVERSE_SCAN = 9,
};

// Interface ObHTableColumnTracker
class ObHTableColumnTracker
{
public:
  // types and constants
  typedef std::pair<common::ObString, int32_t> ColumnCount;
  class ColumnCountComparator;
  class ColumnCountReverseComparator;
public:
  ObHTableColumnTracker()
      :max_versions_(1),
       min_versions_(0),
       oldest_stamp_(0)
  {}
  virtual ~ObHTableColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter,
                   common::ObQueryFlag::ScanOrder &scan_order) = 0;
  virtual int check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code) = 0;
  virtual int check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code) = 0;
  virtual const ColumnCount *get_curr_column() const = 0;
  virtual void reset() = 0;
  virtual bool done() const = 0;
  virtual int get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code) = 0;
  virtual common::ObQueryFlag::ScanOrder get_scan_order() { return tracker_scan_order_; }
  virtual void set_scan_order(common::ObQueryFlag::ScanOrder tracker_scan_order)
  {
    tracker_scan_order_ = tracker_scan_order;
  }
  // Give the tracker a chance to declare it's done based on only the timestamp.
  bool is_done(int64_t timestamp) const;
  void set_ttl(int32_t ttl_value);
  void set_max_version(int32_t max_version);
  int32_t get_max_version() { return max_versions_; }
protected:
  int32_t max_versions_;  // default: 1
  int32_t min_versions_;  // default: 0
  int64_t oldest_stamp_;  // default: 0
  common::ObQueryFlag::ScanOrder tracker_scan_order_;
protected:
  bool is_expired(int64_t timestamp) const { return (-timestamp) < oldest_stamp_; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableColumnTracker);
};

class ObHTableExplicitColumnTracker: public ObHTableColumnTracker
{
public:
  ObHTableExplicitColumnTracker();
  virtual ~ObHTableExplicitColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter,
                   common::ObQueryFlag::ScanOrder &scan_order) override;

  virtual int check_column(const ObHTableCell &cell,
                           ObHTableMatchCode &match_code) override;
  virtual int check_versions(const ObHTableCell &cell,
                             ObHTableMatchCode &match_code) override;
  virtual int get_next_column_or_row(const ObHTableCell &cell,
                                     ObHTableMatchCode &match_code) override;
  virtual const ColumnCount *get_curr_column() const override { return curr_column_; }
  virtual void reset() override;
  virtual bool done() const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableExplicitColumnTracker);
  // function members

  void done_with_column(const ObHTableCell &cell);
  ObHTableMatchCode check_version(int64_t timestamp);
private:
  common::ObSEArray<ColumnCount, common::OB_DEFAULT_SE_ARRAY_COUNT> columns_;
  int64_t curr_column_idx_;
  ColumnCount *curr_column_;
  int32_t current_count_;
};

class ObHTableWildcardColumnTracker: public ObHTableColumnTracker
{
public:
  ObHTableWildcardColumnTracker();
  virtual ~ObHTableWildcardColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter,
                   common::ObQueryFlag::ScanOrder &scan_order) override;
  virtual int check_column(const ObHTableCell &cell,
                           ObHTableMatchCode &match_code) override;
  virtual int check_versions(const ObHTableCell &cell,
                             ObHTableMatchCode &match_code) override;
  virtual int get_next_column_or_row(const ObHTableCell &cell,
                                     ObHTableMatchCode &match_code) override;
  virtual const ColumnCount *get_curr_column() const override { return NULL; }
  virtual void reset() override;
  virtual bool done() const override { return false; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableWildcardColumnTracker);
  // function members
  int reset_cell(const ObHTableCell &cell);
  ObHTableMatchCode check_version(int64_t timestamp);
private:
  // states
  common::ObArenaAllocator allocator_;
  ObString current_qualifier_;
  int32_t current_count_;
};

class ObHTableScanMatcher
{
public:
  explicit ObHTableScanMatcher(const table::ObHTableFilter &htable_filter,
                               ObHTableColumnTracker *column_tracker = nullptr);
  virtual ~ObHTableScanMatcher() {}
  void init(ObHTableColumnTracker *tracker, table::hfilter::Filter *hfilter)
  {
    column_tracker_ = tracker;
    hfilter_ = hfilter;
  }
  void set_hfilter(table::hfilter::Filter *hfilter) { hfilter_ = hfilter; }

  int match(const ObHTableCell &cell, ObHTableMatchCode &match_code);
  int create_key_for_next_col(common::ObArenaAllocator &allocator,
                              const ObHTableCell &cell,
                              ObHTableCell *&next_cell);

  const ObHTableCell* get_curr_row() const;
  void clear_curr_row() { curr_row_.set_ob_row(NULL); }
  int set_to_new_row(const ObHTableCell &curr_row);
  bool is_curr_row_empty() const { return NULL == curr_row_.get_ob_row(); }
  ObHTableMatchCode merge_filter_return_code(const ObHTableCell &cell,
                                             const ObHTableMatchCode match_code,
                                             hfilter::Filter::ReturnCode filter_rc);
private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableScanMatcher);
  int pre_check(const ObHTableCell &cell, ObHTableMatchCode &match_code, bool &need_match_column);
  int match_column(const ObHTableCell &cell, ObHTableMatchCode &match_code);
private:
  ObNegativeTimeRange time_range_;
  ObHTableColumnTracker *column_tracker_;
  table::hfilter::Filter *hfilter_;
  common::ObArenaAllocator allocator_;
  ObHTableCellEntity curr_row_;  // the first cell of current row
  common::ObNewRow curr_ob_row_;
};

class ObHTableRowIterator: public ObTableQueryResultIterator
{
public:
  ObHTableRowIterator(const ObTableQuery &query);
  virtual ~ObHTableRowIterator();
  /// Fetch next row
  virtual int get_next_result(ObTableQueryResult *&one_row) override;

  int seek(const ObHTableCell &key);
  virtual void set_scan_result(table::ObTableApiScanRowIterator *scan_result) override
  {
    child_op_ = scan_result;
  }
  bool has_more_result() const { return has_more_cells_; }
  void set_hfilter(table::hfilter::Filter *hfilter);
  void set_ttl(int32_t ttl_value);
  int add_same_kq_to_res(ObIArray<common::ObNewRow> &same_kq_cells,
                         ObTableQueryResult *&out_result);
  ObIArray<common::ObNewRow> &get_same_kq_cells() { return same_kq_cells_; }
  void set_max_version(int32_t max_version) { max_version_ = max_version; }
private:
  int next_cell();
  int reverse_next_cell(ObIArray<common::ObNewRow> &same_kq_cells,
                        ObTableQueryResult *&out_result);
  int seek_or_skip_to_next_row(const ObHTableCell &cell);
  int seek_or_skip_to_next_col(const ObHTableCell &cell);
  bool reach_batch_limit() const;
  bool reach_size_limit() const;
private:
  table::ObTableApiScanRowIterator *child_op_;
  const table::ObHTableFilter &htable_filter_;
  table::hfilter::Filter *hfilter_;
  int32_t limit_per_row_per_cf_;
  int32_t offset_per_row_per_cf_;
  int64_t max_result_size_;
  int32_t batch_size_;
  int32_t time_to_live_; // Column family level time-to-live, in seconds.
  int32_t max_version_; // Column family max_version

  table::ObTableQueryResult one_hbase_row_;
  ObHTableCellEntity curr_cell_;
  common::ObArenaAllocator allocator_;  // used for deep copy of curr_cell_
  ObHTableColumnTracker *column_tracker_;
  ObHTableScanMatcher *matcher_;
  ObHTableWildcardColumnTracker column_tracker_wildcard_;
  ObHTableExplicitColumnTracker column_tracker_explicit_;
  ObHTableScanMatcher matcher_impl_;
  common::ObQueryFlag::ScanOrder scan_order_;
  ObSEArray<common::ObNewRow, 16> same_kq_cells_;
  int32_t cell_count_;
  int32_t count_per_row_;
  bool has_more_cells_;
  bool is_first_result_;
};

// entry class
class ObHTableFilterOperator: public ObTableQueryResultIterator
{
public:
  ObHTableFilterOperator(const ObTableQuery &query, table::ObTableQueryResult &one_result);
  virtual ~ObHTableFilterOperator() {}
  /// Fetch next batch result
  virtual int get_next_result(ObTableQueryResult *&one_result) override;
  virtual bool has_more_result() const override { return row_iterator_.has_more_result(); }
  virtual table::ObTableQueryResult *get_one_result() override { return one_result_; }
  virtual void set_one_result(ObTableQueryResult *result) override {one_result_ = result;}
  virtual void set_scan_result(table::ObTableApiScanRowIterator *scan_result) override
  {
    row_iterator_.set_scan_result(scan_result);
  }
  void set_ttl(int32_t ttl_value) { row_iterator_.set_ttl(ttl_value); }
  void set_max_version(int32_t max_version_value) { row_iterator_.set_max_version(max_version_value); }
  // parse the filter string
  int parse_filter_string(common::ObIAllocator* allocator);
  OB_INLINE table::hfilter::Filter *get_hfiter() { return hfilter_; }
private:
  ObHTableRowIterator row_iterator_;
  table::ObTableQueryResult *one_result_;
  table::ObHTableFilterParser filter_parser_;
  table::hfilter::Filter *hfilter_;
  int32_t batch_size_;
  int64_t max_result_size_;
  bool is_first_result_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_HTABLE_FILTER_OPERATOR_H */
