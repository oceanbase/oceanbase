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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) = 0;
  /// A way to filter based on the column family, column qualifier and/or the column value.
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) = 0;
  /// Give the filter a chance to transform the passed KeyValue.
  virtual int transform_cell(const ObHTableCell &cell, const ObHTableCell *&new_cell) = 0;
  /// Chance to alter the list of Cells to be submitted.
  virtual int filter_row_cells(const RowCells &cells) = 0;
  /// Last chance to veto row based on previous filterCell(Cell) calls.
  virtual bool filter_row() = 0;
  /// for tableApi filter
  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) = 0;
  /// Primarily used to check for conflicts with scans(such as scans that do not read a full row at a time).
  virtual bool has_filter_row() = 0;

  void set_reversed(bool reversed) { is_reversed_ = reversed; }
  bool is_reversed() const { return is_reversed_; }
  VIRTUAL_TO_STRING_KV("filter", "Filter");
protected:
  bool is_reversed_;
private:
  DISALLOW_COPY_AND_ASSIGN(Filter);
};

enum class CompareOperator
{
  EQUAL,
  GREATER,
  GREATER_OR_EQUAL,
  LESS, LESS_OR_EQUAL,
  NO_OP,
  NOT_EQUAL,
  IS/*table api only*/,
  IS_NOT/*table api only*/
};

class FilterBase: public Filter
{
public:
  FilterBase() {}
  virtual ~FilterBase();

  virtual void reset() override {}
  virtual bool filter_all_remaining() override { return false; }
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override
  { UNUSED(first_row_cell); if (filter_all_remaining()) return true; else return false; }
  virtual int transform_cell(const ObHTableCell &cell, const ObHTableCell *&new_cell) override
  { new_cell = &cell; return common::OB_SUCCESS; }
  virtual int filter_row_cells(const RowCells &cells) override
  { UNUSED(cells); return common::OB_SUCCESS; }
  virtual bool filter_row() override { return false; }
  virtual bool has_filter_row() override { return false; }
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override
  { UNUSED(cell); UNUSED(ret_code); return common::OB_SUCCESS; }
  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) override
  { UNUSED(select_columns); UNUSED(row); UNUSED(filtered); return common::OB_SUCCESS; }

  static const char* compare_operator_to_string(CompareOperator cmp_op);
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
protected:
  bool compare_row(CompareOperator op, Comparable &comparator, const ObHTableCell &cell);
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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_row() override;
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
  TO_STRING_KV("filter", "ValueFilter",
               "cmp_op", compare_operator_to_string(cmp_op_),
               "comparator", comparator_);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ValueFilter);
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

  TO_STRING_KV("filter", "FilterList",
               "op", operator_to_string(op_),
               "filters", filters_);
  static const char* operator_to_string(Operator op);
protected:
  Operator op_;
  ObSEArray<Filter*, 8> filters_;
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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_row() override;
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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual bool filter_row() override;
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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override;
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }
  virtual int transform_cell(const ObHTableCell &cell, const ObHTableCell *&new_cell) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  virtual int transform_cell(const ObHTableCell &cell, const ObHTableCell *&new_cell) override;
  virtual bool filter_row() override;
  virtual bool has_filter_row() override { return true; }

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
  TO_STRING_KV("filter", "SingleColumnValueFilter",
               K_(family),
               K_(qualifier),
               "cmp_op", compare_operator_to_string(cmp_op_),
               K_(comparator),
               K_(filter_if_missing),
               K_(latest_version_only));
private:
  bool filter_column_value(const ObHTableCell &cell);
  bool match_column(const ObHTableCell &cell);
private:
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
  virtual bool filter_row_key(const ObHTableCell &first_row_cell) override;
  virtual bool filter_all_remaining() override;
  virtual int filter_cell(const ObHTableCell &cell, ReturnCode &ret_code) override;
  TO_STRING_KV("filter", "ColumnCountGetFilter",
               K_(limit));
private:
  int64_t limit_;
  int64_t count_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ColumnCountGetFilter);
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
