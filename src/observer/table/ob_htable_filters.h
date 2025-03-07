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

#ifndef _OB_HTABLE_FILTERS_H
#define _OB_HTABLE_FILTERS_H 1
#include "ob_htable_utils.h"
#include "share/table/ob_table_rpc_struct.h"
#include "deps/oblib/src/lib/container/ob_heap.h"
namespace oceanbase
{
namespace table
{
/// hbase filters
/// @see https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/Filter.html
namespace hfilter
{
typedef table::ObTableQueryResult RowCells;
/** Interface Filter
 * Interface for row and column filters directly applied within the regionserver. A filter can expect the following call sequence:
 * + reset() : reset the filter state before filtering a new row.
 * + filterAllRemaining(): true means row scan is over; false means keep going.
 * + filterRowKey(Cell): true means drop this row; false means include.
 * + filterCell(Cell): decides whether to include or exclude this Cell. See Filter.ReturnCode.
 * + transformCell(Cell): if the Cell is included, let the filter transform the Cell.
 * + filterRowCells(List): allows direct modification of the final list to be submitted
 * + filterRow(): last chance to drop entire row based on the sequence of filter calls. Eg: filter a row if it doesn't contain a specified column.
 */
class Filter
{
public:
  enum class ReturnCode
  {
    INCLUDE = 0,
    INCLUDE_AND_NEXT_COL = 1,
    INCLUDE_AND_SEEK_NEXT_ROW = 2,
    SKIP = 3,
    NEXT_COL = 4,
    NEXT_ROW = 5,
    SEEK_NEXT_USING_HINT = 6
  };
public:
  Filter();
  virtual ~Filter();

  /// Reset the state of the filter between rows.
  virtual void reset() = 0;
  /// If this returns true, the scan will terminate.
  virtual bool filter_all_remaining() = 0;
  /// Filters a row based on the row key.
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) = 0;
  /// Will filter use hint to seek.
  virtual bool is_hinting_filter() = 0;
  /// A way to filter based on the column family, column qualifier and/or the column value.
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) = 0;
  /// Give the filter a chance to transform the passed KeyValue.
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) = 0;
  /// Chance to alter the list of Cells to be submitted.
  virtual int filter_row_cells(const RowCells &cells) = 0;
  virtual int filter_row_cells(ObTableQueryDListResult &cells) = 0;
  /// Last chance to veto row based on previous filterCell(Cell) calls.
  virtual bool filter_row() = 0;
  /// for tableApi filter
  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) = 0;
  /// Primarily used to check for conflicts with scans(such as scans that do not read a full row at a time).
  virtual bool has_filter_row() = 0;
  virtual int64_t get_format_filter_string_length() const = 0;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const = 0;
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) = 0;
  virtual void set_reversed(bool reversed) { is_reversed_ = reversed; }
  bool is_reversed() const { return is_reversed_; }
  virtual void set_hbase_version(const ObString &version) { hbase_major_version_ = version[0] - '0'; }
  int8_t get_hbase_major_version() const { return hbase_major_version_; }
  VIRTUAL_TO_STRING_KV("filter", "Filter");
protected:
  bool is_reversed_;
  int8_t hbase_major_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(Filter);
};

enum class CompareOperator
{
  EQUAL,
  GREATER,
  GREATER_OR_EQUAL,
  LESS,
  LESS_OR_EQUAL,
  NO_OP,
  NOT_EQUAL,
  IS/*table api only*/,
  IS_NOT/*table api only*/
};

class FilterUtils
{
public:
  static const char *get_compare_op_name(const CompareOperator &op);
};

class FilterBase: public Filter
{
public:
  FilterBase() {}
  virtual ~FilterBase();

  virtual void reset() override {}
  virtual bool filter_all_remaining() override { return false; }
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override
  { UNUSED(first_row_cell); if (filter_all_remaining()) filtered = true; else filtered = false; return OB_SUCCESS; }
  virtual bool is_hinting_filter() override { return false; }
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) override
  { UNUSED(cell); return common::OB_SUCCESS; }
  virtual int filter_row_cells(const RowCells &cells) override
  { UNUSED(cells); return common::OB_SUCCESS; }
  virtual int filter_row_cells(ObTableQueryDListResult &cells) override
  {UNUSED(cells); return common::OB_SUCCESS;}
  virtual bool filter_row() override { return false; }
  virtual bool has_filter_row() override { return false; }
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override
  { UNUSED(cell); UNUSED(ret_code); return common::OB_SUCCESS; }
  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) override
  { UNUSED(select_columns); UNUSED(row); UNUSED(filtered); return common::OB_SUCCESS; }
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override
  { UNUSED(allocator); UNUSED(cell); UNUSED(new_cell); return common::OB_SUCCESS; }
  static const char* compare_operator_to_string(CompareOperator cmp_op);
  virtual int64_t get_format_filter_string_length() const = 0;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(FilterBase);
};

class Comparable
{
public:
  Comparable(const ObString &comparator_value)
      :comparator_value_(comparator_value)
  {}
  virtual ~Comparable() {}
  virtual int compare_to(const ObString &b) { UNUSED(b); return common::OB_SUCCESS; }
  virtual int compare_to(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         int &cmp_ret)
  { UNUSED(select_columns); UNUSED(row); UNUSED(cmp_ret); return common::OB_SUCCESS; }
  virtual int compare_to(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         CompareOperator compare_op,
                         int &cmp_ret)
  { UNUSED(select_columns);
    UNUSED(row);
    UNUSED(compare_op);
    UNUSED(cmp_ret);
    return common::OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV("comprable", "Comprable");
protected:
  ObString comparator_value_;
private:
  DISALLOW_COPY_AND_ASSIGN(Comparable);
};

class BinaryComparator: public Comparable
{
public:
  BinaryComparator(const ObString &comparator_value)
      :Comparable(comparator_value)
  {}
  virtual ~BinaryComparator() {}
  virtual int compare_to(const ObString &b) override;
  TO_STRING_KV("comparable", "BinaryComparator");
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(BinaryComparator);
};

class BinaryPrefixComparator: public Comparable
{
public:
  BinaryPrefixComparator(const ObString &comparator_value)
      :Comparable(comparator_value)
  {}
  virtual ~BinaryPrefixComparator() {}
  virtual int compare_to(const ObString &b) override;
  TO_STRING_KV("comparable", "BinaryPrefixComparator");
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(BinaryPrefixComparator);
};

class RegexStringComparator: public Comparable
{
public:
  RegexStringComparator(const ObString &comparator_value)
      :Comparable(comparator_value)
  {}
  virtual ~RegexStringComparator() {}
  virtual int compare_to(const ObString &b) override;
  TO_STRING_KV("comparable", "RegexStringComparator");
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(RegexStringComparator);
};

class SubStringComparator: public Comparable
{
public:
  SubStringComparator(const ObString &comparator_value)
      :Comparable(comparator_value)
  {}
  virtual ~SubStringComparator() {}
  virtual int compare_to(const ObString &b) override;
  TO_STRING_KV("comparable", "SubStringComparator");
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(SubStringComparator);
};

class CompareFilter: public FilterBase
{
public:
  CompareFilter(CompareOperator cmp_op, Comparable *comparator)
      :cmp_op_(cmp_op),
       comparator_(comparator)
  {}
  virtual ~CompareFilter();
  int check_arguments() const;
  static bool compare(CompareOperator op, int cmp_ret);
  virtual int64_t get_format_filter_string_length() const = 0;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const = 0;
protected:
  bool compare_row(CompareOperator op, Comparable &comparator, const ObHTableCell &cell);
  bool compare_family(CompareOperator op, Comparable &comparator, const ObHTableCell &cell);
  bool compare_qualifier(CompareOperator op, Comparable &comparator, const ObHTableCell &cell);
  bool compare_value(CompareOperator op, Comparable &comparator, const ObHTableCell &cell);
protected:
  CompareOperator cmp_op_;
  Comparable *comparator_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(CompareFilter);
};

/// This filter is used to filter based on the key.
class RowFilter: public CompareFilter
{
public:
  RowFilter(CompareOperator cmp_op, Comparable *comparator)
      :CompareFilter(cmp_op, comparator),
      filter_out_row_(false)
  {}
  virtual ~RowFilter();
  virtual void reset() override;
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_row() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "RowFilter",
               "cmp_op", compare_operator_to_string(cmp_op_),
               "comparator", comparator_);
private:
  bool filter_out_row_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(RowFilter);
};

/// This filter is used to filter based on the key.
class QualifierFilter: public CompareFilter
{
public:
  QualifierFilter(CompareOperator cmp_op, Comparable *comparator)
      :CompareFilter(cmp_op, comparator)
  {}
  virtual ~QualifierFilter();

  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "QualifierFilter",
               "cmp_op", compare_operator_to_string(cmp_op_),
               "comparator", comparator_);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(QualifierFilter);
};

/// This filter is used to filter based on column value.
class ValueFilter: public CompareFilter
{
public:
  ValueFilter(CompareOperator cmp_op, Comparable *comparator)
      :CompareFilter(cmp_op, comparator)
  {}
  virtual ~ValueFilter();
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "ValueFilter",
               "cmp_op", compare_operator_to_string(cmp_op_),
               "comparator", comparator_);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ValueFilter);
};

// this filter is used to filter based on the target column value and cells with the same timestamp
class DependentColumnFilter: public CompareFilter
{
public:
  DependentColumnFilter(const ObString &family, const ObString &qualifier, bool drop_dependent_column)
      :CompareFilter(CompareOperator::NO_OP, NULL),
      family_(family),
      qualifier_(qualifier),
      drop_dependent_column_(drop_dependent_column),
      is_inited_(false)
  {}
  DependentColumnFilter(const ObString &family, const ObString &qualifier, bool drop_dependent_column, CompareOperator cmp_op, Comparable *comparator)
      :CompareFilter(cmp_op, comparator),
      family_(family),
      qualifier_(qualifier),
      drop_dependent_column_(drop_dependent_column),
      is_inited_(false)
  {}
  virtual ~DependentColumnFilter();

  virtual bool has_filter_row() override { return true; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "DependentColumnFilter",
               K_(family),
               K_(qualifier),
               K_(drop_dependent_column),
               "cmp_op", compare_operator_to_string(cmp_op_),
               K_(comparator));
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int filter_row_cells(ObTableQueryDListResult &cells) override;
  virtual void reset() override;
  int init();

private:
  ObString family_;
  ObString qualifier_;
  bool drop_dependent_column_; // default as false
  bool is_inited_;
  common::hash::ObHashSet<int64_t> stamp_set_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(DependentColumnFilter);
};

/// represents an ordered List of Filters which will be evaluated with a specified boolean operator
/// FilterList.Operator.MUST_PASS_ALL (AND) or FilterList.Operator.MUST_PASS_ONE (OR).
class FilterListBase: public FilterBase
{
public:
  enum class Operator
  { MUST_PASS_ALL/*AND*/, MUST_PASS_ONE/*OR*/ };
public:
  FilterListBase(Operator op)
      :op_(op)
  {}
  virtual ~FilterListBase();

  int add_filter(Filter *filter);
  Operator get_operator() const { return op_; }
  virtual void reset() override;
  virtual bool has_filter_row() override { return true; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  virtual const char *filter_name() const { return "FilterListBase"; }
  virtual void set_reversed(bool reversed) override;
  virtual void set_hbase_version(const ObString &version) override;

  TO_STRING_KV("filter", "FilterList",
               "op", operator_to_string(op_),
               "filters", filters_);
  static const char* operator_to_string(Operator op);
protected:
  Operator op_;
  ObSEArray<Filter*, 8> filters_;
  ObSEArray<bool, 8> cell_included_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(FilterListBase);
};

class FilterListAND: public FilterListBase
{
public:
  FilterListAND(Operator op)
      :FilterListBase(op)
  {}
  virtual ~FilterListAND();
  virtual void reset() override;
  virtual bool filter_all_remaining() override;
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) override;
  virtual bool filter_row() override;
  virtual const char *filter_name() const override { return "FilterListAND"; }
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
private:
  static ReturnCode merge_return_code(ReturnCode rc, ReturnCode local_rc);
  ObSEArray<Filter*, 8> seek_hint_filters_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(FilterListAND);
};

class FilterListOR: public FilterListBase
{
public:
  FilterListOR(Operator op)
      :FilterListBase(op)
  {}
  virtual ~FilterListOR();
  virtual void reset() override;
  virtual bool filter_all_remaining() override;
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) override;
  virtual bool filter_row() override;
  virtual const char *filter_name() const override { return "FilterListOR"; }
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
private:
  static ReturnCode merge_return_code(ReturnCode rc, ReturnCode local_rc);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(FilterListOR);
};

/// A wrapper filter that filters an entire row if any of the Cell checks do not pass.
class SkipFilter: public FilterBase
{
public:
  SkipFilter(Filter *filter)
      :filter_(filter),
      filter_row_(false)
  {}
  virtual ~SkipFilter();
  virtual void reset() override;
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "SkipFilter",
               "sub_filter", filter_);
private:
  Filter *filter_;
  bool filter_row_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(SkipFilter);
};

/// A wrapper filter that returns true from filterAllRemaining() as soon as the wrapped filters
/// Filter.filterRowKey(), Filter.filterCell(), Filter.filterRow() or Filter.filterAllRemaining() methods returns true.
class WhileMatchFilter: public FilterBase
{
public:
  WhileMatchFilter(Filter *filter)
      :filter_(filter),
      filter_all_remaining_(false)
  {}
  virtual ~WhileMatchFilter();

  virtual void reset() override;
  virtual bool filter_all_remaining() override;
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) override;
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }
  virtual void set_reversed(bool reversed) override { if (nullptr != filter_) { filter_->set_reversed(reversed); } }
  virtual void set_hbase_version(const ObString &version) override { filter_->set_hbase_version(version); }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;

  TO_STRING_KV("filter", "WhileMatchFilter",
               "sub_filter", filter_);
private:
  Filter *filter_;
  bool filter_all_remaining_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(WhileMatchFilter);
};

class SingleColumnValueFilter: public FilterBase
{
public:
  SingleColumnValueFilter(const ObString &family, const ObString &qualifier, CompareOperator cmp_op, Comparable *comparator)
      :family_(family),
      qualifier_(qualifier),
      cmp_op_(cmp_op),
      comparator_(comparator),
      filter_if_missing_(false),
      latest_version_only_(true),
      found_column_(false),
      matched_column_(false)
  {}
  virtual ~SingleColumnValueFilter();
  void set_filter_if_missing(bool v) { filter_if_missing_ = v; }
  void set_latest_version_only(bool v) { latest_version_only_ = v; }

  virtual void reset() override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "SingleColumnValueFilter",
               K_(family),
               K_(qualifier),
               "cmp_op", compare_operator_to_string(cmp_op_),
               K_(comparator),
               K_(filter_if_missing),
               K_(latest_version_only));
private:
  bool filter_column_value(const ObHTableCell &cell);
protected:
  bool match_family_column(const ObHTableCell &cell);
protected:
  ObString family_;
  ObString qualifier_;
  CompareOperator cmp_op_;
  Comparable *comparator_;
  bool filter_if_missing_;  // default: false
  bool latest_version_only_;  // default: true
  // state
  bool found_column_;
  bool matched_column_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(SingleColumnValueFilter);
};

// Simple filter that filters out matched cells filtered by SingleColumnValueFilter
class SingleColumnValueExcludeFilter: public SingleColumnValueFilter
{
public:
  SingleColumnValueExcludeFilter(const ObString &family, const ObString &qualifier, CompareOperator cmp_op, Comparable *comparator)
      :SingleColumnValueFilter(family, qualifier, cmp_op, comparator)
  {}
  virtual ~SingleColumnValueExcludeFilter();

  virtual int filter_row_cells(ObTableQueryDListResult &cells) override;
  virtual bool has_filter_row() override { return true; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "SingleColumnValueExcludeFilter",
               K_(family),
               K_(qualifier),
               "cmp_op", compare_operator_to_string(cmp_op_),
               K_(comparator),
               K_(filter_if_missing),
               K_(latest_version_only));
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(SingleColumnValueExcludeFilter);
};

/// Simple filter that limits results to a specific page size
class PageFilter: public FilterBase
{
public:
  PageFilter(int64_t page_size)
      : page_size_(page_size),
        rows_accepted_(0)
  {}
  virtual ~PageFilter() {}
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_all_remaining() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "PageFilter", K_(page_size), K_(rows_accepted));
private:
  int64_t page_size_;
  int64_t rows_accepted_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(PageFilter);
};

/// Simple filter that includes rows based on a chance.
class RandomRowFilter: public FilterBase
{
public:
  RandomRowFilter(float_t chance)
      : chance_(chance),
        filter_out_row_(false)
  {}
  ~RandomRowFilter() {}
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  void reset() override;
  virtual bool has_filter_row() override { return true; }
  virtual bool filter_row() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "RandomRowFilter", K_(chance));
private:
  float_t chance_;
  bool filter_out_row_;
  DISALLOW_COPY_AND_ASSIGN(RandomRowFilter);
};

/// Simple filter that limits results to a specific column size
class ColumnPaginationFilter: public FilterBase
{
public:
  ColumnPaginationFilter(int64_t limit, int64_t offset)
      : limit_(limit),
        offset_(offset),
        count_(0)
  {}
  ColumnPaginationFilter(int64_t limit, ObString column_offset)
      : limit_(limit),
        offset_(0),
        column_offset_(column_offset),
        count_(0)
  {}
  virtual ~ColumnPaginationFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual void reset() override;
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "ColumnPaginationFilter", K_(limit), K_(offset));
private:
  int64_t limit_;
  int64_t offset_;
  ObString column_offset_;
  int64_t count_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ColumnPaginationFilter);
};


/// Simple filter based on the lead portion of Column names
class ColumnPrefixFilter: public FilterBase
{
public:
  ColumnPrefixFilter(ObString prefix)
      : prefix_(prefix)
  {}
  virtual ~ColumnPrefixFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
  virtual bool is_hinting_filter() override { return true; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "ColumnPrefixFilter", K_(prefix));
private:
  ObString prefix_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ColumnPrefixFilter);
};

/// Simple filter that returns first cell on row only.
class FirstKeyOnlyFilter: public FilterBase
{
public:
  FirstKeyOnlyFilter()
      : found_(false)
  {}
  virtual ~FirstKeyOnlyFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  void reset() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "FirstKeyOnlyFilter");
private:
  bool found_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(FirstKeyOnlyFilter);
};

/// Simple filter that returns only the key component of each key-value.
class KeyOnlyFilter: public FilterBase
{
public:
  KeyOnlyFilter(bool len_as_val)
      : len_as_val_(len_as_val)
  {}
  virtual ~KeyOnlyFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "KeyOnlyFilter");
private:
  bool len_as_val_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(KeyOnlyFilter);
};

template<typename L, typename R>
struct ObPair
{
  L left_; R right_;
public:
  ObPair() : left_(), right_() {}
  ObPair(const L &l, const R &r): left_(l), right_(r) {}
  inline bool operator==(const ObPair &b) const
  {
    return b.left_ == left_ && b.right_ == right_;
  }
  TO_STRING_KV(K_(left), K_(right));
};

typedef ObPair<ObString, ObPair<ObString, ObString>> NextRowType; // <rowkey candidate, <fuzzy key, fuzzy meta>>
struct NextRowsComparator
{
  bool reversed_;

  NextRowsComparator(bool reversed) : reversed_(reversed) {}
  bool operator() (const NextRowType *le, const NextRowType *re)
  {
    bool b_ret = false;
    if (nullptr != le && nullptr != re) {
      if (reversed_) {
        b_ret = (le->left_ <= re->left_);
      } else {
        b_ret = (le->left_ >= re->left_);
      } // to build a min-root heap
    }
    return b_ret;
  }
  int get_error_code() { return OB_SUCCESS; }
};

class Order
{
public:
  virtual bool lt(char lhs, char rhs) = 0;
  virtual bool gt(char lhs, char rhs) = 0;
  virtual void inc(char &val) = 0;
  virtual bool isMax(char val) = 0;
  virtual int8_t min() const = 0;
};

class ASC : public Order
{
  virtual bool lt(char lhs, char rhs) override { return lhs < rhs; }
  virtual bool gt(char lhs, char rhs) override { return lhs > rhs; }
  virtual void inc(char &val) override { val += 1; }
  virtual bool isMax(char val) override { return val == -1; }
  virtual int8_t min() const override { return 0; }
};

class DESC : public Order
{
  virtual bool lt(char lhs, char rhs) override { return lhs > rhs; }
  virtual bool gt(char lhs, char rhs) override { return lhs < rhs; }
  virtual void inc(char &val) override { val -= 1; }
  virtual bool isMax(char val) override { return val == 0; }
  virtual int8_t min() const override { return -1; }
};

class RowTracker
{
public:
  RowTracker(ObIAllocator &allocator, common::ObIArray<ObPair<ObString, ObString>*> &fuzzy_key_data, bool is_reversed)
      : allocator_(allocator),
        is_reversed_(is_reversed),
        cmp_(is_reversed),
        next_rows_(cmp_, &allocator),
        fuzzy_key_data_(fuzzy_key_data) ,
        order_(nullptr),
        inited_(false)
  {}
  ~RowTracker() {}
  int init(const ObString &cur_row);
  int update_tracker(const ObString &cur_row, bool &done); // initialize or update next_rows usuing current_cell
  int next_row(ObString &row); // get top row from next_rows_
  TO_STRING_KV("FuzzyRowFilter", "RowTracker", K_(is_reversed), K_(next_rows), K_(fuzzy_key_data));
private:
  int update_with(const ObString &cur_row, const ObPair<ObString, ObString> &fuzzy_data);
  // compare cur_cell rowkey and the given rowkey considering scan order
  bool less_than(const ObString &cur_row, const ObString &next_row);
private:
  ObIAllocator &allocator_;
  bool is_reversed_;
  NextRowsComparator cmp_;
  ObBinaryHeap<NextRowType*, NextRowsComparator> next_rows_;
  ObIArray<ObPair<ObString, ObString>*> &fuzzy_key_data_;
  Order *order_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(RowTracker);
};

class FuzzyRowFilter: public FilterBase
{
public:
  FuzzyRowFilter(ObIAllocator &allocator, ObIArray<ObPair<ObString, ObString>*> &fuzzy_key)
      : allocator_(allocator),
        row_tracker_(nullptr),
        done_(false),
        filter_row_(false),
        included_(false),
        is_inited_(false),
        fuzzy_key_(fuzzy_key)
  {}
  virtual ~FuzzyRowFilter() {
    if (nullptr != row_tracker_) {
      row_tracker_->~RowTracker();
    }
  }
  int init(const ObHTableCell &cell);
  virtual bool filter_all_remaining() override { return done_; }
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
  virtual bool is_hinting_filter() override { return true; }
  virtual bool filter_row() override { return filter_row_; }
  void reset() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "FuzzyRowFilter", K_(row_tracker), K_(fuzzy_key), K_(is_inited));
private:
  static bool fuzzy_match(const ObString &rowkey, const ObPair<ObString, ObString> &fuzzy_key);
private:
  ObIAllocator &allocator_;
  RowTracker *row_tracker_;
  bool done_;
  bool filter_row_;
  bool included_;
  bool is_inited_;
  ObIArray<ObPair<ObString, ObString>*> &fuzzy_key_;
  DISALLOW_COPY_AND_ASSIGN(FuzzyRowFilter);
};

class ObTimestampNode
{
public:
  ObTimestampNode(): value_(0) {};
  ObTimestampNode(int64_t value): value_(value) {};
  ~ObTimestampNode() {};
  RBNODE(ObTimestampNode, rblink);
  int64_t get_value() const
  {
    return value_;
  }
  int compare(const ObTimestampNode *node) const
  {
    return common::compare(value_, node->get_value());
  }
  TO_STRING_KV("ObRbTreeNode", "ObTimestampNode", K(value_));
private:
  int64_t value_;
};

/// Simple filter that returns those key-values whose timestamps matches any of the specified timestamps.
class TimestampsFilter: public FilterBase
{
public:
  TimestampsFilter(common::ObSEArray<int64_t, 8>* origin_ts, bool can_hint)
      : max_timestamp_(ObHTableConstants::LATEST_TIMESTAMP),
        can_hint_(can_hint),
        is_inited_(false),
        origin_ts_(origin_ts),
        nodes_(),
        timestamps_()
  {}
  virtual ~TimestampsFilter()
  {
    if (OB_NOT_NULL(origin_ts_)) {
      origin_ts_->~ObSEArray();
    }
  }
  int init();
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
  virtual bool is_hinting_filter() override { return can_hint_; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "TimestampsFilter");
private:
  int64_t max_timestamp_;
  bool can_hint_;
  bool is_inited_;
  common::ObSEArray<int64_t, 8>* origin_ts_;
  common::ObSEArray<ObTimestampNode, 8> nodes_;
  container::ObRbTree<ObTimestampNode, container::ObDummyCompHelper<ObTimestampNode>> timestamps_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TimestampsFilter);
};

/// Simple filter that filter cells by column family and qualifier.
class ColumnValueFilter: public FilterBase
{
public:
  ColumnValueFilter(ObString &cf, ObString &qualifier, CompareOperator cmp_op, Comparable *comparator)
      : cf_(cf),
        qualifier_(qualifier),
        cmp_op_(cmp_op),
        comparator_(comparator),
        column_found_(false)
  {}
  ~ColumnValueFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  void reset() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "ColumnValueFilter", K_(cf), K_(qualifier), K_(cmp_op), KP_(comparator));
private:
  ObString cf_;
  ObString qualifier_;
  CompareOperator cmp_op_;
  Comparable *comparator_;
  bool column_found_;
  DISALLOW_COPY_AND_ASSIGN(ColumnValueFilter);
};

/// Simple filter that returns rows in range.
class MultiRowRangeFilter: public FilterBase
{
public:
  MultiRowRangeFilter(common::ObSEArray<KeyRange*, 8>* origin_range)
      : origin_range_(origin_range),
        inited_(false),
        nodes_(),
        ranges_(),
        range_(nullptr),
        return_code_(),
        done_(false)
  {}
  virtual ~MultiRowRangeFilter() {}
  virtual int init();
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override { UNUSED(cell); ret_code = return_code_; return OB_SUCCESS; }
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
  virtual bool is_hinting_filter() override { return true; }
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual bool filter_all_remaining() override { return done_; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "MultiRowRangeFilter");
protected:
  common::ObSEArray<KeyRange*, 8>* origin_range_;
  bool inited_;
private:
  common::ObSEArray<ObKeyRangeNode, 8> nodes_;
  ObKeyRangeTree ranges_;
  KeyRange* range_;
  ReturnCode return_code_;
  bool done_;
  DISALLOW_COPY_AND_ASSIGN(MultiRowRangeFilter);
};

/// Simple filter that returns rows before stop key.
class InclusiveStopFilter: public MultiRowRangeFilter
{
public:
  InclusiveStopFilter(common::ObSEArray<KeyRange*, 8>* origin_range, ObString stop_key)
      : MultiRowRangeFilter(origin_range),
        stop_key_(stop_key)
  {}
  ~InclusiveStopFilter() {}
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "InclusiveStopFilter");
private:
  ObString stop_key_;
  DISALLOW_COPY_AND_ASSIGN(InclusiveStopFilter);
};

/// Simple filter that returns columns in range.
class ColumnRangeFilter: public FilterBase
{
public:
  ColumnRangeFilter(common::ObSEArray<KeyRange*, 8>* origin_range)
      : origin_range_(origin_range),
        inited_(false),
        nodes_(),
        ranges_(),
        range_(nullptr)
  {}
  virtual ~ColumnRangeFilter() {}
  virtual int init();
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  void reset() override;
  virtual bool is_hinting_filter() override { return true; }
  virtual int get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "ColumnRangeFilter");
protected:
  common::ObSEArray<KeyRange*, 8>* origin_range_;
  bool inited_;
private:
  common::ObSEArray<ObKeyRangeNode, 8> nodes_;
  ObKeyRangeTree ranges_;
  KeyRange* range_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ColumnRangeFilter);
};

/// Simple filter based on the multiple lead portion of Column names
class MultipleColumnPrefixFilter: public ColumnRangeFilter
{
public:
  MultipleColumnPrefixFilter(common::ObSEArray<KeyRange*, 8>* origin_range)
      : ColumnRangeFilter(origin_range)
  {}
  ~MultipleColumnPrefixFilter() {}
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "MultipleColumnPrefixFilter");
private:
  DISALLOW_COPY_AND_ASSIGN(MultipleColumnPrefixFilter);
};

/// Simple filter that filter cells with column family.
class FamilyFilter : public CompareFilter
{
public:
  FamilyFilter(CompareOperator cmp_op, Comparable *comparator)
    : CompareFilter(cmp_op, comparator)
  {}
  virtual ~FamilyFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "FamilyFilter");
private:
  DISALLOW_COPY_AND_ASSIGN(FamilyFilter);
};

/// Simple filter that returns first N columns on row only.
class ColumnCountGetFilter: public FilterBase
{
public:
  ColumnCountGetFilter(int64_t limit)
      :limit_(limit),
      count_(0)
  {}
  virtual ~ColumnCountGetFilter() {}
  virtual void reset() override;
  virtual int filter_row_key(const ObHTableCell &first_row_cell, bool &filtered) override;
  virtual bool filter_all_remaining() override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "ColumnCountGetFilter",
               K_(limit));
private:
  int64_t limit_;
  int64_t count_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ColumnCountGetFilter);
};

/// Simple filter that returns cells before matching qualifiers.
class FirstKeyValueMatchingQualifiersFilter: public FilterBase
{
public:
  FirstKeyValueMatchingQualifiersFilter(common::ObSEArray<ObString, 8> *qualifiers)
      : found_(false),
        qualifiers_(qualifiers)
  {}
  virtual ~FirstKeyValueMatchingQualifiersFilter() {}
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  void reset() override;
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "FirstKeyValueMatchingQualifiersFilter");
private:
  bool found_;
  common::ObSEArray<ObString, 8> *qualifiers_;
  DISALLOW_COPY_AND_ASSIGN(FirstKeyValueMatchingQualifiersFilter);
};

/// CheckAndMutateFilter is used to implement the check logic of CheckAndMutate
/// @see https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Table.html#checkAndMutate-byte:A-byte:A-
class CheckAndMutateFilter: public FilterBase
{
public:
  CheckAndMutateFilter(const ObString &family, const ObString &qualifier, CompareOperator cmp_op,
                       Comparable *comparator, bool value_is_null)
      :family_(family),
       qualifier_(qualifier),
       cmp_op_(cmp_op),
       comparator_(comparator),
       value_is_null_(value_is_null),
       found_column_(false),
       matched_column_(false)
  {}
  virtual ~CheckAndMutateFilter();

  virtual void reset() override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }
  OB_INLINE bool value_is_null() { return value_is_null_; }
  virtual int64_t get_format_filter_string_length() const override;
  virtual int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const override;
  TO_STRING_KV("filter", "CheckAndMutateFilter",
               K_(family),
               K_(qualifier),
               "cmp_op", compare_operator_to_string(cmp_op_),
               K_(comparator),
               K_(value_is_null));
private:
  bool filter_column_value(const ObHTableCell &cell);
  bool match_column(const ObHTableCell &cell);
private:
  ObString family_;
  ObString qualifier_;
  CompareOperator cmp_op_;
  Comparable *comparator_;
  //  If the passed value is null, the check is for the lack of column (ie: non-existence)
  bool value_is_null_;  // default: false
  // state
  bool found_column_;
  bool matched_column_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(CheckAndMutateFilter);
};


} // end namespace hfilter

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_HTABLE_FILTERS_H */
