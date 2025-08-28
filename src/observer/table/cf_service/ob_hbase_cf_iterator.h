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

#ifndef _OB_HBASE_CF_ITERATOR_H
#define _OB_HBASE_CF_ITERATOR_H

#include "observer/table/common/ob_hbase_common_struct.h"
#include "observer/table/adapters/ob_i_adapter.h"
#include "observer/table/ob_htable_filter_operator.h"
#include "observer/table/adapters/ob_hbase_adapter_factory.h"

namespace oceanbase
{
namespace table
{

class ObHbaseExplicitColumnTracker: public ObHTableColumnTracker
{
public:
ObHbaseExplicitColumnTracker();
  virtual ~ObHbaseExplicitColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family = false) override;

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
  DISALLOW_COPY_AND_ASSIGN(ObHbaseExplicitColumnTracker);
  // function members

  void done_with_column(const ObHTableCell &cell);
  ObHTableMatchCode check_version(int64_t timestamp);
private:
  common::ObSEArray<ColumnCount, common::OB_DEFAULT_SE_ARRAY_COUNT> columns_;
  int64_t curr_column_idx_;
  ColumnCount *curr_column_;
  int32_t current_count_;
};

class ObHbaseWildcardColumnTracker: public ObHTableColumnTracker
{
public:
ObHbaseWildcardColumnTracker();
  virtual ~ObHbaseWildcardColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family = false) override;
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
  DISALLOW_COPY_AND_ASSIGN(ObHbaseWildcardColumnTracker);
  // function members
  int reset_cell(const ObHTableCell &cell);
  ObHTableMatchCode check_version(int64_t timestamp);
private:
  // states
  common::ObArenaAllocator allocator_;
  ObString current_qualifier_;
  int32_t current_count_;
};

class ObHbaseTSExplicitColumnTracker: public ObHTableColumnTracker
{
public:
  ObHbaseTSExplicitColumnTracker() {}
  virtual ~ObHbaseTSExplicitColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family = false) override;

  virtual int check_column(const ObHTableCell &cell,
                           ObHTableMatchCode &match_code) override;
  virtual int check_versions(const ObHTableCell &cell,
                             ObHTableMatchCode &match_code) override;
  virtual int get_next_column_or_row(const ObHTableCell &cell,
                                     ObHTableMatchCode &match_code) override;
  virtual const ColumnCount *get_curr_column() const override { return NULL; }
  virtual void reset() override;
  virtual bool done() const override;
  virtual int32_t get_cur_version() override;
  virtual bool is_done(int64_t timestamp) const; // check column is done by timestamp
private:
  ObHashSet<ObString> column_set_;
  DISALLOW_COPY_AND_ASSIGN(ObHbaseTSExplicitColumnTracker);
};

class ObHbaseTSWildcardColumnTracker : public ObHTableColumnTracker
{
public:
  ObHbaseTSWildcardColumnTracker() = default;
  virtual ~ObHbaseTSWildcardColumnTracker() {}
  virtual int init(const table::ObHTableFilter &htable_filter, bool qualifier_with_family = false) override;
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
  virtual bool is_done(int64_t timestamp) const; // check column is done by timestamp
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHbaseTSWildcardColumnTracker);
};

class ObHbaseRowIterator
{
public:
  ObHbaseRowIterator(const ObHbaseQuery &hbase_query, ObTableExecCtx &exec_ctx, bool is_timeseries_table);
  virtual ~ObHbaseRowIterator() 
  {
    if (OB_NOT_NULL(cell_iter_)) {
      cell_iter_->~ObHbaseICellIter();
      cell_iter_ = nullptr;
    }
    
    is_inited_ = false;
  };
  virtual int init(ScannerContext &scanner_context, const ObHBaseParams *hbase_params, hfilter::Filter *filter);
  int get_next_row(ObTableQueryResult *&one_row);
  int get_next_row(ObTableQueryDListResult *&one_result);
  bool has_more_result() const { return has_more_cells_; }
  void no_more_result() { has_more_cells_ = false; }
  virtual void close();
  virtual ObHbaseICellIter* get_child_cell_iter() { return cell_iter_; }
  virtual ObHbaseRescanParam& get_rescan_param() { return forward_rescan_param_; }
  const common::ObIArray<ObTabletID>& get_tablet_ids() const { return tablet_ids_; }
private:
  template <typename ResultType>
  int get_next_row_internal(ResultType*& result);
  template <typename ResultType>
  int get_next_row_internal_normal(ResultType *&result);
  template <typename ResultType>
  int get_next_row_internal_timeseries(ResultType *&result);

  void set_hfilter(table::hfilter::Filter *hfilter);
  void set_need_verify_cell_ttl(bool need_verify_cell_ttl);
  void init_family_name();
  virtual int seek(ObHTableCell &key, int32_t &skipped_count);
protected:
  virtual int rescan_and_get_next_row(table::ObHbaseICellIter *cell_iter, ObHbaseRescanParam &rescan_param, ObNewRow *&ob_next_row);
  int seek_first_cell_on_hint(const ObNewRow *ob_row);
  virtual table::ObHbaseICellIter* get_forward_cell_iter() { return cell_iter_; }
  virtual int next_cell();
  virtual int seek_or_skip_to_next_row(const ObHTableCell &cell);
  virtual int seek_or_skip_to_next_col(const ObHTableCell &cell);
  virtual bool reach_batch_limit() const;
  virtual bool reach_size_limit() const;
  int get_next_cell_hint();
  int add_result_cell(const ObNewRow &row, ObTableQueryResult *&out_result);
  int add_result_cell(const ObNewRow &row, ObTableQueryDListResult *&out_result);
  int get_and_init_cell_iter(common::ObIAllocator &allocator, ObTableExecCtx &ctx,
                             const ObTableQuery &query, ObHbaseICellIter *&cell_iter);

protected:
  // try record expired rowkey accord cell's timestamp
  virtual int try_record_expired_rowkey(const ObHTableCellEntity &cell);
  // try record expired rowkey accord cell's versions
  virtual void try_record_expired_rowkey(const int32_t versions, const ObString &rowkey);

protected:
  common::ObArenaAllocator allocator_;  // used for deep copy of curr_cell_
  common::ObArenaAllocator forward_range_alloc_;  // used for forward range
  const ObHbaseQuery &hbase_query_;
  table::ObHbaseICellIter *cell_iter_;
  ObHTableCellEntity curr_cell_;
  common::ObQueryFlag::ScanOrder scan_order_;
  ObHTableScanMatcher *matcher_;
  bool has_more_cells_;
  ObRowkey start_row_key_;
  ObRowkey stop_row_key_;
  bool is_inited_;
  bool need_verify_cell_ttl_;
  table::ObHbaseRescanParam forward_rescan_param_;
  ObHbaseAdapterGuard hbase_adapter_guard_;
  ObTableExecCtx &exec_ctx_;

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
  ObHbaseWildcardColumnTracker column_tracker_wildcard_;
  ObHbaseExplicitColumnTracker column_tracker_explicit_;
  ObHbaseTSWildcardColumnTracker ts_column_tracker_wildcard_;
  ObHbaseTSExplicitColumnTracker ts_column_tracker_explicit_;
  ObHTableScanMatcher matcher_impl_;
  int32_t cell_count_;
  int32_t count_per_row_;
  bool is_first_result_;
  ObString family_name_;
  bool is_cur_row_expired_;
  bool allow_partial_results_;
  bool is_cache_block_;
  ScannerContext *scanner_context_;
  ObSEArray<ObTabletID, 4> tablet_ids_;
  bool is_timeseries_table_;
};

class ObHbaseReversedRowIterator : public ObHbaseRowIterator
{
public:
  ObHbaseReversedRowIterator(const ObHbaseQuery &hbase_query, ObTableExecCtx &exec_ctx);
  virtual ~ObHbaseReversedRowIterator();
  virtual int init(ScannerContext &scanner_context, const ObHBaseParams *hbase_params, hfilter::Filter *filter) override;
  virtual void close() override;
  virtual ObHbaseICellIter* get_child_cell_iter() override { return forward_cell_iter_; }
  virtual ObHbaseRescanParam& get_rescan_param() override { return reverse_rescan_param_; }
private:
  virtual int next_cell() override;
  int seek(ObHTableCell &key, int32_t &skipped_count) override;
  virtual int seek_or_skip_to_next_row(const ObHTableCell &cell) override;
  int seek_or_skip_to_next_row(const ObString &rowkey);
  int seek_or_skip_to_next_row_inner(const ObString &rowkey);
  virtual int seek_or_skip_to_next_col(const ObHTableCell &cell) override;
  int seek_or_skip_to_next_col(const ObString &rowkey, const ObString &qualifier);
  int seek_or_skip_to_next_col_inner(const ObString &rowkey, const ObString &qualifier);
  int create_forward_cell_iter(ObTableExecCtx &exec_ctx);
  virtual table::ObHbaseICellIter* get_forward_cell_iter() { return forward_cell_iter_; }
  
private:
  common::ObArenaAllocator iter_allocator_;  // used for forward iter
  common::ObArenaAllocator reversed_range_alloc_;  // used for reversed range
  table::ObHbaseICellIter *forward_cell_iter_;
  table::ObHbaseRescanParam reverse_rescan_param_;
  ObTableQuery* forward_query_;
};

class ObHbaseCFIterator : public ObHbaseQueryResultIterator
{
public:
  ObHbaseCFIterator(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx);
  virtual ~ObHbaseCFIterator();

  virtual int init() override;
  virtual void close() override;
  virtual int get_next_result(ObTableQueryResult &hbase_wide_rows) override;
  virtual int get_next_result(ObTableQueryIterableResult &hbase_wide_rows) override;
  virtual int get_next_result(ObTableQueryIterableResult *&hbase_wide_rows) override;
  virtual hfilter::Filter *get_filter() const override { return filter_; }
  virtual bool has_more_result() const override;
  TO_STRING_KV(K_(batch_size), K_(ob_kv_params), K_(caching), K_(batch), K_(max_result_size),
               K_(is_first_result), K_(check_existence_only), K_(scanner_context),
               K_(is_inited), K_(row_count), KPC_(filter), K_(hbase_query));
private:
  template <typename ResultType>
  int get_next_result_internal(ResultType &next_result);
  bool reach_limit();
  bool reach_caching_limit(int num_of_row);
  int init_hbase_filter(ObIAllocator &allocator, const ObHBaseParams *hbase_param, hfilter::Filter *&filter);
  int init_hbase_params(const ObHBaseParams *&hbase_param);
private:
  common::ObArenaAllocator allocator_;
  const ObHbaseQuery &hbase_query_;
  ObTableExecCtx &exec_ctx_;
  ObHbaseRowIterator *row_iterator_;
  table::ObTableQueryIterableResult iterable_result_;
  table::ObHTableFilterParser filter_parser_;
  int32_t batch_size_;
  const ObKVParams& ob_kv_params_;
  int32_t caching_;
  int32_t batch_;
  int64_t max_result_size_;
  bool is_first_result_;
  bool check_existence_only_;
  ScannerContext scanner_context_;
  bool is_inited_;
  int32_t row_count_;
  hfilter::Filter *filter_;
  bool is_timeseries_table_;
};

} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_HBASE_COLUMN_FAMILY_ITERATOR_H
