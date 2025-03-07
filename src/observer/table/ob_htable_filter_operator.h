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
#include "ob_table_cache.h"
#include "ob_table_cg_service.h"
#include <utility>
#include "share/schema/ob_table_schema.h"
#include "ob_table_query_async_processor.h"

namespace oceanbase
{
namespace table
{
class ObTableHbaseRowKeyDefaultCompare final : public observer::ObTableMergeFilterCompare {
public:
  ObTableHbaseRowKeyDefaultCompare() = default;
  ~ObTableHbaseRowKeyDefaultCompare() override = default;

  int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const override;
  bool operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs) override;
};

class ObTableHbaseRowKeyReverseCompare final : public observer::ObTableMergeFilterCompare {
public:
  ObTableHbaseRowKeyReverseCompare() = default;
  ~ObTableHbaseRowKeyReverseCompare() override = default;

  int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const override;
  bool operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs) override;
};

class ObHColumnDescriptor final
{
public:
  ObHColumnDescriptor()
      :time_to_live_(0),
       max_version_(0)
  {}
  void reset();
  int from_string(const common::ObString &kv_attributes);
  void from_kv_attribute(const ObKVAttr &kv_attributes);

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
       oldest_stamp_(0),
       column_has_expired_(false)
  {}
  virtual ~ObHTableColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter) = 0;
  virtual int check_column(const ObHTableCell &cell, ObHTableMatchCode &match_code) = 0;
  virtual int check_versions(const ObHTableCell &cell, ObHTableMatchCode &match_code) = 0;
  virtual const ColumnCount *get_curr_column() const = 0;
  virtual void reset() = 0;
  virtual bool done() const = 0;
  virtual int get_next_column_or_row(const ObHTableCell &cell, ObHTableMatchCode &match_code) = 0;
  // Give the tracker a chance to declare it's done based on only the timestamp.
  bool is_done(int64_t timestamp) const;
  void set_ttl(int32_t ttl_value);
  void set_max_version(int32_t max_version);
  int32_t get_max_version() { return max_versions_; }
  virtual int32_t get_cur_version() = 0;
  bool is_expired(int64_t timestamp) const { return (-timestamp) < oldest_stamp_; }
  virtual bool check_column_expired() const { return column_has_expired_; }
protected:
  int32_t max_versions_;  // default: 1
  int32_t min_versions_;  // default: 0
  int64_t oldest_stamp_;  // default: 0
  bool column_has_expired_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableColumnTracker);
};

class ObHTableExplicitColumnTracker: public ObHTableColumnTracker
{
public:
  ObHTableExplicitColumnTracker();
  virtual ~ObHTableExplicitColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter) override;

  virtual int check_column(const ObHTableCell &cell,
                           ObHTableMatchCode &match_code) override;
  virtual int check_versions(const ObHTableCell &cell,
                             ObHTableMatchCode &match_code) override;
  virtual int get_next_column_or_row(const ObHTableCell &cell,
                                     ObHTableMatchCode &match_code) override;
  virtual const ColumnCount *get_curr_column() const override { return curr_column_; }
  virtual void reset() override;
  virtual bool done() const override;
  virtual int32_t get_cur_version() override;
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
  virtual int init(const table::ObHTableFilter &htable_filter) override;
  virtual int check_column(const ObHTableCell &cell,
                           ObHTableMatchCode &match_code) override;
  virtual int check_versions(const ObHTableCell &cell,
                             ObHTableMatchCode &match_code) override;
  virtual int get_next_column_or_row(const ObHTableCell &cell,
                                     ObHTableMatchCode &match_code) override;
  virtual const ColumnCount *get_curr_column() const override { return NULL; }
  virtual void reset() override;
  virtual bool done() const override { return false; }
  virtual int32_t get_cur_version() override;
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


class LimitScope
{
public:
  enum class Scope
  {
    BETWEEN_ROWS = 0,
    BETWEEN_CELLS = 1
  };
  LimitScope() : scope_(Scope::BETWEEN_ROWS), depth_(0) {}
  LimitScope(Scope scope) : scope_(scope), depth_(static_cast<int>(scope)) {}
  ~LimitScope() {};
  OB_INLINE int depth() const { return depth_; }
  OB_INLINE void set_scope(Scope scope) {scope_ = scope; depth_ = static_cast<int>(scope);}
  OB_INLINE bool can_enforce_limit_from_scope(const LimitScope& checker_scope) const { return checker_scope.depth() <= depth_; }
  TO_STRING_KV(K_(scope), K_(depth));
private:
  Scope scope_;
  int depth_;
};

class LimitFields
{
public:
  LimitFields(): batch_(-1), size_(-1), time_(-1), size_scope_(LimitScope::Scope::BETWEEN_ROWS), time_scope_(LimitScope::Scope::BETWEEN_ROWS) {}
  LimitFields(int32_t batch, int64_t size, int64_t time, LimitScope limit_scope) { set_fields(batch, size, time, limit_scope); }
  void set_fields(int64_t size, int64_t time, LimitScope limit_scope) {
    set_size_scope(limit_scope);
    set_time_scope(limit_scope);
    set_size(size);
    set_time(time);
  }
  void set_fields(int64_t batch_size, int64_t size, int64_t time, LimitScope limit_scope) {
    set_batch(batch_size);
    set_size_scope(limit_scope);
    set_time_scope(limit_scope);
    set_size(size);
    set_time(time);
  }
  ~LimitFields() {};
  void reset() { batch_ = 0; size_ =0; time_ = 0; size_scope_.set_scope(LimitScope::Scope::BETWEEN_ROWS);}
  void set_batch(int32_t batch) { batch_ = batch; }
  void set_size(int64_t size) { size_ = size; }
  void set_time(int64_t time) { time_ = time; }
  void set_time_scope(LimitScope scope) { time_scope_ = scope; }
  void set_size_scope(LimitScope scope) { size_scope_ = scope; }
  int32_t get_batch() { return batch_; }
  int64_t get_size() { return size_; }
  int64_t get_time() { return time_; }
  LimitScope get_size_scope() { return size_scope_; }
  LimitScope get_time_scope() { return time_scope_; }
  bool can_enforce_batch_from_scope(LimitScope checker_scope) { return LimitScope(LimitScope::Scope::BETWEEN_CELLS).can_enforce_limit_from_scope(checker_scope);}
  bool can_enforce_size_from_scope(LimitScope checker_scope) { return size_scope_.can_enforce_limit_from_scope(checker_scope); }
  bool can_enforce_time_from_scope(LimitScope checker_scope) { return time_scope_.can_enforce_limit_from_scope(checker_scope); }
  TO_STRING_KV(K_(batch), K_(size), K_(time), K_(size_scope), K_(time_scope));
private:
  int32_t batch_;
  int64_t size_;
  int64_t time_;
  LimitScope size_scope_;
  LimitScope time_scope_;
};

class ScannerContext
{
public:
  ScannerContext();
  ~ScannerContext() {};
  ScannerContext(int32_t batch, int64_t size, int64_t time, LimitScope limit_scope);
  void increment_batch_progress(int32_t batch);
  void increment_size_progress(int64_t size);
  void update_time_progress() { progress_.set_time(ObTimeUtility::current_time()); }
  bool check_batch_limit(LimitScope checker_scope);
  bool check_size_limit(LimitScope checker_scope);
  bool check_time_limit(LimitScope checker_scope);
  bool check_any_limit(LimitScope checker_scope);

  LimitFields limits_;
  LimitFields progress_;
  TO_STRING_KV(K_(limits),
                K_(progress));
private:
  static const int LIMIT_DEFAULT_VALUE = -1;
  static const int PROGRESS_DEFAULT_VALUE = 0;
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
  const ObHTableCell* get_curr_row() const;
  int is_cell_ttl_expired(const ObHTableCell &cell, bool &is_expired);
  void set_need_verify_cell_ttl(bool need_verify_cell_ttl) { need_verify_cell_ttl_ = need_verify_cell_ttl; }
  void clear_curr_row() { curr_row_.set_ob_row(NULL); }
  int set_to_new_row(const ObHTableCell &curr_row);
  int get_next_cell_hint(const ObHTableCell &cell, ObHTableCell *&new_cell);
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
  bool need_verify_cell_ttl_;
  int64_t now_;
};

class ObHTableRowIterator: public ObTableQueryResultIterator
{
public:
  ObHTableRowIterator(const ObTableQuery &query);
  virtual ~ObHTableRowIterator() {
    allocator_.reset();
  };
  /// Fetch next row
  virtual int get_next_result(ObTableQueryResult *&one_row) override;
  int get_next_result(ObTableQueryDListResult *&one_result);
  virtual int seek(ObHTableCell &key, int32_t &skipped_count);
  virtual void set_scan_result(table::ObTableApiScanRowIterator *scan_result) override
  {
    child_op_ = scan_result;
  }
  bool has_more_result() const { return has_more_cells_; }
  void no_more_result() { has_more_cells_ = false; }
  void set_hfilter(table::hfilter::Filter *hfilter);
  void set_ttl(int32_t ttl_value) { time_to_live_ = ttl_value; }
  void set_need_verify_cell_ttl(bool need_verify_cell_ttl);
  virtual int init()
  {
    is_inited_ = true;
    return OB_SUCCESS;
  };
  bool &is_inited()
  {
    return is_inited_;
  }
  void set_max_version(int32_t max_version) { max_version_ = max_version; }
  void init_table_group_value();
  void set_scanner_context(ScannerContext *scanner_context) { scanner_context_ = scanner_context; }
  void set_allow_partial_results(bool allow_partial_result) { allow_partial_results_ = allow_partial_result; }
  void set_is_cache_block(bool is_cache_block) { is_cache_block_ = is_cache_block; }
private:
  template <typename ResultType>
  int get_next_result_internal(ResultType*& result);

  virtual int get_next_result(ObTableQueryIterableResult *&one_result) override { return OB_NOT_SUPPORTED; }
protected:
  virtual int rescan_and_get_next_row(table::ObTableApiScanRowIterator *tb_op, ObNewRow *&ob_next_row);
  int seek_first_cell_on_hint(const ObNewRow *ob_row);
  virtual table::ObTableApiScanRowIterator* get_forward_child_op() { return child_op_; }
  virtual int next_cell();
  virtual int seek_or_skip_to_next_row(const ObHTableCell &cell);
  virtual int seek_or_skip_to_next_col(const ObHTableCell &cell);
  virtual bool reach_batch_limit() const;
  virtual bool reach_size_limit() const;
  int get_next_cell_hint();
  virtual int add_new_row(const ObNewRow &row, ObTableQueryResult *&out_result);

  int add_new_row(const ObNewRow &row, ObTableQueryDListResult *&out_result);

protected:
  // try record expired rowkey accord cell's timestamp
  virtual int try_record_expired_rowkey(const ObHTableCellEntity &cell);
  // try record expired rowkey accord cell's versions
  virtual void try_record_expired_rowkey(const int32_t versions, const ObString &rowkey);

protected:
  common::ObArenaAllocator allocator_;  // used for deep copy of curr_cell_
  common::ObArenaAllocator forward_range_alloc_;  // used for forward range
  table::ObTableApiScanRowIterator *child_op_;
  ObHTableCellEntity curr_cell_;
  common::ObQueryFlag::ScanOrder scan_order_;
  ObHTableScanMatcher *matcher_;
  bool has_more_cells_;
  ObRowkey start_row_key_;
  ObRowkey stop_row_key_;
  bool is_inited_;
  bool need_verify_cell_ttl_;

private:
  const table::ObHTableFilter &htable_filter_;
  table::hfilter::Filter *hfilter_;
  int32_t limit_per_row_per_cf_;
  int32_t offset_per_row_per_cf_;
  int64_t max_result_size_;
  int32_t batch_size_;
  int32_t time_to_live_; // Column family level time-to-live, in seconds.
  int32_t max_version_; // Column family max_version

  table::ObTableQueryResult one_hbase_row_;
  table::ObTableQueryDListResult one_iterable_hbase_row_;
  ObHTableColumnTracker *column_tracker_;
  ObHTableWildcardColumnTracker column_tracker_wildcard_;
  ObHTableExplicitColumnTracker column_tracker_explicit_;
  ObHTableScanMatcher matcher_impl_;
  int32_t cell_count_;
  int32_t count_per_row_;
  bool is_first_result_;
  bool is_table_group_inited_;
  bool is_table_group_req_;
  ObString family_name_;
  bool is_cur_row_expired_;
  bool allow_partial_results_;
  bool is_cache_block_;
  ScannerContext *scanner_context_;
};

class ObHTableReversedRowIterator : public ObHTableRowIterator {
public:
  ObHTableReversedRowIterator(const ObTableQuery &query);
  virtual ~ObHTableReversedRowIterator();
  virtual int init() override;

private:
  virtual int next_cell() override;
  int seek(ObHTableCell &key, int32_t &skipped_count) override;
  virtual int seek_or_skip_to_next_row(const ObHTableCell &cell) override;
  int seek_or_skip_to_next_row(const ObString &rowkey);
  int seek_or_skip_to_next_row_inner(const ObString &rowkey);
  virtual int seek_or_skip_to_next_col(const ObHTableCell &cell) override;
  int seek_or_skip_to_next_col(const ObString &rowkey, const ObString &qualifier);
  int seek_or_skip_to_next_col_inner(const ObString &rowkey, const ObString &qualifier);
  int init_forward_tb_ctx();
  int init_async_forward_tb_ctx();
  virtual table::ObTableApiScanRowIterator* get_forward_child_op() { return forward_child_op_; }
  int create_forward_child_op();

private:
  common::ObArenaAllocator iter_allocator_;  // used for forward iter
  common::ObArenaAllocator reversed_range_alloc_;  // used for reversed range
  table::ObTableApiScanRowIterator *forward_child_op_;
  ObTableApiCacheGuard cache_guard_;
  ObTableApiSpec *spec_;
  table::ObTableCtx forward_tb_ctx_;
  ObExprFrameInfo expr_frame_info_;
};

// entry class
class ObHTableFilterOperator: public ObTableQueryResultIterator
{
public:
  ObHTableFilterOperator(const ObTableQuery &query, table::ObTableQueryResult &one_result);
  ObHTableFilterOperator(const ObTableQuery &query, table::ObTableQueryIterableResult &one_result);
  virtual ~ObHTableFilterOperator();
  /// Fetch next batch result
  bool reach_limit();
  bool reach_caching_limit(int num_of_row);
  virtual int get_next_result(ObTableQueryResult *&one_result) override;
  virtual int get_next_result(ObTableQueryIterableResult *&one_result) override;
  int init(common::ObIAllocator* allocator);
  virtual bool has_more_result() const override { return is_inited_ ? row_iterator_->has_more_result() : false; }
  virtual table::ObTableQueryResult *get_one_result() override { return one_result_; }
  virtual void set_one_result(ObTableQueryResult *result) override {one_result_ = result;}
  virtual void set_query_async() override;
  virtual void set_scan_result(table::ObTableApiScanRowIterator *scan_result) override
  {
    if (is_inited_) {
      row_iterator_->set_scan_result(scan_result);
    };
  }
  void set_ttl(int32_t ttl_value)
  {
    if (is_inited_) {
      row_iterator_->set_ttl(ttl_value);
    };
  }

  void set_need_verify_cell_ttl(bool need_verify_cell_ttl)
  {
    if (is_inited_) {
      row_iterator_->set_need_verify_cell_ttl(need_verify_cell_ttl);
    };
  }
  void set_max_version(int32_t max_version_value)
  {
    if (is_inited_) {
      row_iterator_->set_max_version(max_version_value);
    };
  }
  // parse the filter string
  int parse_filter_string(common::ObIAllocator* allocator);

private:
  template <typename ResultType>
  int get_next_result_internal(ResultType*& result);
private:
  ObHTableRowIterator *row_iterator_;
  table::ObTableQueryResult *one_result_;
  table::ObTableQueryIterableResult *iterable_result_;

  table::ObHTableFilterParser filter_parser_;
  int32_t batch_size_;
  const ObKVParams& ob_kv_params_;
  int32_t caching_;
  int32_t batch_;
  int time_limit_delta;
  int64_t max_result_size_;
  bool is_first_result_;
  bool check_existence_only_;
  ScannerContext scanner_context_;
  bool is_inited_;
  int32_t row_count_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_HTABLE_FILTER_OPERATOR_H */
