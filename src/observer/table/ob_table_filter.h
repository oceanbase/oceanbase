/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef _OB_TABLE_FILTERS_H
#define _OB_TABLE_FILTERS_H 1

#include "ob_htable_filter_parser.h"
#include "ob_htable_filters.h"
#include "ob_table_scan_executor.h"
#include "common/row/ob_row_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "ob_table_aggregation.h"

namespace oceanbase {

namespace common
{
class ObArenaAllocator;
class ObNewRow;
} // end namespace common

namespace table
{

class ObTableComparator : public hfilter::Comparable
{
public:
  ObTableComparator(const ObString &column_name, const ObString &comparator_value)
      :Comparable(comparator_value),
       column_name_(column_name)
  {}
  virtual ~ObTableComparator() {}

  // UNUSED, cannot compare when op is is/not is
  virtual int compare_to(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         int &cmp_ret) override;
  virtual int compare_to(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         hfilter::CompareOperator compare_op,
                         int &cmp_ret) override;
  VIRTUAL_TO_STRING_KV("comprable", "ObTableComparator");
private:
  ObString column_name_;
private:
  bool is_numeric(const ObString &value);
  DISALLOW_COPY_AND_ASSIGN(ObTableComparator);
};

class ObTableCompareFilter : public hfilter::FilterBase
{
public:
  ObTableCompareFilter(hfilter::CompareOperator cmp_op, hfilter::Comparable *comparator)
      :cmp_op_(cmp_op),
       comparator_(comparator)
  {}
  virtual ~ObTableCompareFilter() {};

  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) override;
  TO_STRING_KV("filter", "ObTableCompareFilter",
               "cmp_op", hfilter::FilterBase::compare_operator_to_string(cmp_op_),
               "comparator", comparator_);
private:
  hfilter::CompareOperator cmp_op_;
  hfilter::Comparable *comparator_;
  DISALLOW_COPY_AND_ASSIGN(ObTableCompareFilter);
};

class ObTableFilterListAnd : public hfilter::FilterListBase
{
public:
  ObTableFilterListAnd(hfilter::FilterListBase::Operator op)
      : FilterListBase(op)
  {}
  virtual ~ObTableFilterListAnd() {}

  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableFilterListAnd);
};

class ObTableFilterListOr : public hfilter::FilterListBase
{
public:
  ObTableFilterListOr(hfilter::FilterListBase::Operator op)
      : FilterListBase(op)
  {}
  virtual ~ObTableFilterListOr() {}

  virtual int filter_row(const ObIArray<ObString> &select_columns,
                         const common::ObNewRow &row,
                         bool &filtered) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableFilterListOr);
};

class ObTableFilterParser : public ObHTableFilterParser
{
public:
  ObTableFilterParser()
      : ObHTableFilterParser()
  {}
  virtual ~ObTableFilterParser() {}

  virtual int create_comparator(const SimpleString &bytes,
                                hfilter::Comparable *&comparator) override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableFilterParser);
};


class ObTableQueryResultIterator
{
public:
  ObTableQueryResultIterator(const ObTableQuery *query = nullptr)
      : query_(query),
        is_query_sync_(false)
  {
  }
  virtual ~ObTableQueryResultIterator() {}
  virtual int get_next_result(ObTableQueryResult *&one_result) = 0;
  virtual bool has_more_result() const = 0;
  virtual void set_one_result(ObTableQueryResult *result){ UNUSED(result); }
  virtual void set_scan_result(table::ObTableApiScanRowIterator *scan_result) = 0;
  virtual ObTableQueryResult *get_one_result() { return nullptr; }
  virtual void set_query(const ObTableQuery *query) { query_ = query; };
  virtual void set_query_sync() { is_query_sync_ = true; }
protected:
  const ObTableQuery *query_;
  bool is_query_sync_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryResultIterator);
};

class ObNormalTableQueryResultIterator: public ObTableQueryResultIterator
{
public:
  ObNormalTableQueryResultIterator(const ObTableQuery &query, table::ObTableQueryResult &one_result)
      : ObTableQueryResultIterator(&query),
        one_result_(&one_result),
        last_row_(NULL),
        batch_size_(query.get_batch()),
        max_result_size_(std::min(query.get_max_result_size(),
                          static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024))),
        scan_result_(NULL),
        is_first_result_(true),
        has_more_rows_(true),
        agg_calculator_(query)
  {
  }
  virtual ~ObNormalTableQueryResultIterator() {}
  virtual int get_next_result(table::ObTableQueryResult *&one_result) override;
  virtual bool has_more_result() const override;
  virtual void set_scan_result(table::ObTableApiScanRowIterator *scan_result) override
  {
    scan_result_ = scan_result;
  }
  virtual void set_one_result(ObTableQueryResult *result) override {one_result_ = result;}
  virtual table::ObTableQueryResult *get_one_result() override { return one_result_; }
  ObTableAggCalculator &get_agg_calculator() { return agg_calculator_; }
  int init_aggregation();
  int get_aggregate_result(table::ObTableQueryResult *&next_result);
  int get_normal_result(table::ObTableQueryResult *&next_result);
  bool is_aggregate_query() { return agg_calculator_.is_exist(); }
private:
  table::ObTableQueryResult *one_result_;
  common::ObNewRow *last_row_;
  int32_t batch_size_;
  int64_t max_result_size_;
  table::ObTableApiScanRowIterator *scan_result_;
  bool is_first_result_;
  bool has_more_rows_;
  ObTableAggCalculator agg_calculator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObNormalTableQueryResultIterator);
};

class ObTableFilterOperator: public ObTableQueryResultIterator
{
public:
  ObTableFilterOperator(const ObTableQuery &query, table::ObTableQueryResult &one_result)
    : ObTableQueryResultIterator(&query),
      one_result_(&one_result),
      scan_result_(NULL),
      last_row_(NULL),
      tfilter_(NULL),
      batch_size_(query.get_batch()),
      max_result_size_(std::min(query.get_max_result_size(),
                        static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024))),
      is_first_result_(true),
      has_more_rows_(true),
      row_idx_(0),
      agg_calculator_(query)
  {}
  virtual ~ObTableFilterOperator() {}
  virtual int get_next_result(ObTableQueryResult *&next_result) override;
  virtual bool has_more_result() const override { return has_more_rows_; }
  virtual void set_one_result(ObTableQueryResult *result) override { one_result_ = result; }
  void set_scan_result(table::ObTableApiScanRowIterator *scan_result) { scan_result_ = scan_result; }
  int parse_filter_string(common::ObIAllocator* allocator);
  ObTableAggCalculator &get_agg_calculator() { return agg_calculator_; }
  int init_aggregation();
  int get_aggregate_result(table::ObTableQueryResult *&next_result);
  int get_normal_result(table::ObTableQueryResult *&next_result);
  bool is_aggregate_query() { return agg_calculator_.is_exist(); }
private:
  int check_limit_param();
private:
  table::ObTableQueryResult *one_result_;
  table::ObTableApiScanRowIterator *scan_result_;
  common::ObNewRow *last_row_;
  ObTableFilterParser filter_parser_;
  hfilter::Filter *tfilter_;
  int32_t batch_size_;
  int64_t max_result_size_;
  bool is_first_result_;
  bool has_more_rows_;
  int64_t row_idx_; // not filtered row index
  ObTableAggCalculator agg_calculator_;
};

} // end namespace table

} // end namespace oceanbase



#endif /* _OB_TABLE_FILTERS_H */