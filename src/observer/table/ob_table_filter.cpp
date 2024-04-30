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
#define USING_LOG_PREFIX SERVER
#include "ob_table_filter.h"
#include "ob_table_rpc_processor.h"
#include "htable_filter_tab.hxx"
#include "htable_filter_lex.hxx"
#include "ob_table_aggregation.h"

using namespace oceanbase::common;
using namespace oceanbase::table;


inline bool ObTableComparator::is_numeric(const ObString &value)
{
  int ret = true;
  char *ptr = const_cast<char *>(value.ptr());
  const int32_t length = value.length();

  // 22x ObString.is_numeric do not consider negative, so need this func
  // but 4.0 has fix it
  if (2 <= length && '-' == ptr[0]) {
    // negative
    ObString s(length-1, ptr+1);
    ret = s.is_numeric();
  } else {
    // positive
    ret = value.is_numeric();
  }

  return ret;
}

int ObTableComparator::compare_to(const ObIArray<ObString> &select_columns, const common::ObNewRow &row, int &cmp_ret)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSEDx(select_columns, row, cmp_ret);
  return OB_NOT_IMPLEMENT;
}

int ObTableComparator::compare_to(const ObIArray<ObString> &select_columns,
                                  const common::ObNewRow &row,
                                  hfilter::CompareOperator compare_op,
                                  int &cmp_ret)
{
  int ret = OB_ERR_UNEXPECTED;
  const int64_t M = row.get_count();
  const int64_t N = select_columns.count();
  int64_t idx;

  if (M != N) {
    LOG_WARN("row cell count is not equal to select columns count", K(ret), K(select_columns), K(row));
  } else {
    for (idx = 0; idx < N; idx++) {
      if (column_name_ == select_columns.at(idx)) {
        ret = OB_SUCCESS;
        break;
      }
    }

    if (OB_SUCCESS == ret) {
      const common::ObObj &cell = row.get_cell(idx);
      const common::ObObjType column_type = cell.get_type();
      const common::ObCollationType cs_type = cell.get_collation_type();
      if (compare_op == hfilter::CompareOperator::IS) {
        cmp_ret = cell.is_null() ? 0 : 1;
      } else if (compare_op == hfilter::CompareOperator::IS_NOT) {
        cmp_ret = cell.is_null() ? 1 : 0;
      } else if (ob_is_int_tc(column_type)) {
        // support int8, int16, int24, int32, int64.
        if (!is_numeric(comparator_value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("comparator value is not a number", K(ret), K_(comparator_value));
        } else {
          // has a border problem
          // if comparator_value is bigger than INT64_MAX, atol() will return INT64_MAX
          // so `< comparator_value` will filter the INT64_MAX
          // also when comparator_value = INT64_MIN
          int64_t src_v = atol(comparator_value_.ptr());
          int64_t dest_v = cell.get_int();
          cmp_ret = src_v == dest_v ? 0 :
                    (src_v > dest_v ? 1 : -1);
        }
      } else if (ob_is_uint_tc(column_type)) {
        // support uint8, uint16, uint24, uint32, uint64.
        if (!is_numeric(comparator_value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("comparator value is not a number", K(ret), K_(comparator_value));
        } else {
          // has a border problem
          // if comparator_value is bigger than UINT64_MAX, strtoul() will return ULONG_MAX(UINT64_MAX)
          // so `< comparator_value` will filter the UINT64_MAX
          // also when comparator_value = UINT64_MIN
          char *endptr = nullptr;
          uint64_t src_v = strtoul(comparator_value_.ptr(), &endptr, 10);
          if (OB_ISNULL(endptr) || *endptr != '\0') {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("comparator value is not a valid number", K(ret), K_(comparator_value));
          } else {
            uint64_t dest_v = cell.get_uint64();
            cmp_ret = src_v == dest_v ? 0 :
                      (src_v > dest_v ? 1 : -1);
          }
        }
      } else if (ob_is_string_tc(column_type)) {
        // support varchar, char, varbinary, binary
        ObObj compare_obj;
        compare_obj.set_varchar(comparator_value_.ptr());
        compare_obj.set_collation_type(cs_type);
        if (OB_FAIL(compare_obj.compare(cell, cs_type, cmp_ret))) {
          LOG_WARN("obj compare fail", K(compare_obj), K(cell), K(ret));
        }
      } else if (ob_is_null(column_type)) {
        ret = OB_ERR_NULL_VALUE;
      } else {
        // not support others
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "only for int and string, other column type");
        LOG_WARN("do not support other column type, only for int, string", K(ret), K(column_type));
      }
    } else {
      LOG_WARN("comparator does not find column", K(ret), K_(column_name), K(select_columns));
    }
  }

  return ret;
}

int ObTableCompareFilter::filter_row(const ObIArray<ObString> &select_columns, const common::ObNewRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (hfilter::CompareOperator::NO_OP == cmp_op_) {
    filtered = true;
  } else {
    int cmp_ret;
    if (OB_SUCC(comparator_->compare_to(select_columns, row, cmp_op_, cmp_ret))) {
      filtered = hfilter::CompareFilter::compare(cmp_op_, cmp_ret);
    } else if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
      filtered = true;
    } else {
      LOG_WARN("compare failed", K(ret));
    }
  }

  return ret;
}

int ObTableFilterListAnd::filter_row(const ObIArray<ObString> &select_columns, const common::ObNewRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (filters_.empty()) {
    ret = hfilter::FilterListBase::filter_row(select_columns, row, filtered);
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      Filter *filter = filters_.at(i);
      if (OB_SUCC(filter->filter_row(select_columns, row, filtered))) {
        if (filtered) {
          break;
        }
      } else {
        // do nothing
      }
    } // end for
  } // end else

  return ret;
}

int ObTableFilterListOr::filter_row(const ObIArray<ObString> &select_columns, const common::ObNewRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (filters_.empty()) {
    ret = hfilter::FilterListBase::filter_row(select_columns, row, filtered);
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      Filter *filter = filters_.at(i);
      if (OB_SUCC(filter->filter_row(select_columns, row, filtered))) {
        if (!filtered) {
          break;
        }
      } else {
        // do nothing
      }
    } // end for
  } // end else

  return ret;
}

int ObTableFilterParser::create_comparator(const SimpleString &bytes, hfilter::Comparable *&comparator)
{
  int ret = OB_SUCCESS;
  char *p = static_cast<char*>(memchr(bytes.str_, ':', bytes.len_));
  if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no `:` found in the filter string", K(ret));
  } else {
    comparator = NULL;
    int64_t len1 = p - bytes.str_;
    ObString cmp_column(len1, bytes.str_);
    ObString cmp_value(bytes.len_-len1-1, p+1);

    comparator = OB_NEWx(ObTableComparator, allocator_, cmp_column, cmp_value);
    if (NULL == comparator) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory for ObTableComparator", K(ret));
    }

    if (OB_SUCC(ret) && NULL != comparator) {
      if (OB_FAIL(comparators_.push_back(comparator))) {
        LOG_WARN("failed to add comparator", K(ret));
        comparator->~Comparable();
        comparator = NULL;
      }
    }
  } // end else

  return ret;
}

int ObNormalTableQueryResultIterator::get_next_result(table::ObTableQueryResult *&next_result)
{
  int ret = OB_SUCCESS;
  if (is_aggregate_query()) {
    if (OB_FAIL(get_aggregate_result(next_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failt to get aggregate result", K(ret));
      }
    }
  } else {
    if (OB_FAIL(get_normal_result(next_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get normal result", K(ret));
      }
    }
  }
  return ret;
}

bool ObNormalTableQueryResultIterator::has_more_result() const
{
  return has_more_rows_;
}

int ObNormalTableQueryResultIterator::init_aggregation()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_agg_calculator().init())) {
    LOG_WARN("failed to init agg_calculator", K(ret));
  }
  return ret;
}

int ObNormalTableQueryResultIterator::get_aggregate_result(table::ObTableQueryResult *&next_result) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(one_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one_result_ should not be null", K(ret));
  } else {
    ObNewRow *row = nullptr;
    while (OB_SUCC(ret) && OB_SUCC(scan_result_->get_next_row(row))) {
      if (OB_FAIL(agg_calculator_.aggregate(*row))) {
        LOG_WARN("fail to aggregate", K(ret), K(*row));
      }
    }  // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      agg_calculator_.final_aggregate(); // agg sum/svg finally
      has_more_rows_ = false;
      one_result_->reset();
      if (OB_FAIL(one_result_->deep_copy_property_names(get_agg_calculator().get_agg_columns()))) {
        LOG_WARN("fail to deep copy property names to one result", K(ret));
      } else if (OB_FAIL(one_result_->add_row(agg_calculator_.get_aggregate_results()))) {
        LOG_WARN("fail to add aggregation result", K(ret), K(agg_calculator_.get_aggregate_results()));
      } else {
        next_result = one_result_;
      }
    }
  }

  return ret;
}

int ObNormalTableQueryResultIterator::get_normal_result(table::ObTableQueryResult *&next_result) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(one_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one_result_ should not be null", K(ret));
  } else {
    one_result_->reset_except_property();
  }

  if (OB_SUCC(ret)) {
    if (NULL != last_row_) {
      if (OB_FAIL(one_result_->add_row(*last_row_))) {
        LOG_WARN("failed to add row, ", K(ret));
      } else {
        row_idx_++;
        last_row_ = NULL;
      }
    }
  }

  if (OB_SUCC(ret)) {
    const bool has_limit = (limit_ != -1);
    bool has_reach_limit = (row_idx_ >= offset_ + limit_);
    next_result = one_result_;
    ObNewRow *row = nullptr;
    while (OB_SUCC(ret) && (!has_limit || !has_reach_limit) &&
           OB_SUCC(scan_result_->get_next_row(row))) {
      LOG_DEBUG("[yzfdebug] scan result", "row", *row);
      if (has_limit && row_idx_ < offset_) {
        row_idx_++;
      } else if (OB_FAIL(one_result_->add_row(*row))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          ret = OB_SUCCESS;
          last_row_ = row;
          break;
        } else {
          LOG_WARN("failed to add row", K(ret));
        }
      } else {
        row_idx_++;
        if (one_result_->reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
          NG_TRACE(tag9);
          break;
        } else {
          LOG_DEBUG("[yzfdebug] scan return one row", "row", *row);
        }
      }
      has_reach_limit = (row_idx_ >= offset_ + limit_);
    }  // end while

    if (OB_SUCC(ret) && (has_limit && has_reach_limit)) {
      ret = OB_ITER_END;
    }

    if (OB_ITER_END == ret) {
      has_more_rows_ = false;
      if (one_result_->get_row_count() > 0) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObTableFilterOperator::parse_filter_string(common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  const ObString &filter_str = query_->get_filter_string();
  if (filter_str.empty()) {
    tfilter_ = NULL;
  } else if (NULL == allocator) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (OB_FAIL(filter_parser_.init(allocator))) {
    LOG_WARN("failed to init filter_parser_", K(ret));
  } else if (OB_FAIL(filter_parser_.parse_filter(filter_str, tfilter_))) {
    LOG_WARN("failed to parse filter", K(ret), K(filter_str));
  }

  return ret;
}

int ObTableFilterOperator::check_limit_param()
{
  int ret = OB_SUCCESS;
  const int32_t limit = query_->get_limit();
  const int32_t offset = query_->get_offset();
  if (limit != -1 && (limit < 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid limit param", K(ret), K(limit), K(offset));
  }
  return ret;
}

int ObTableFilterOperator::init_full_column_name(const ObIArray<ObString>& col_arr)
{
  int ret = OB_SUCCESS;
  bool is_select_column_empty = query_->get_select_columns().empty(); // query select column is empty when do queryAndMutate
  if (is_aggregate_query()) {
    // do nothing
  } else if (OB_FAIL(full_column_name_.assign(col_arr))) {
    LOG_WARN("fail to assign full column name", K(ret));
  } else if (!is_select_column_empty) {
    one_result_->reset_property_names();
    // why need deep copy, query_ is owned to query session when do sync query, and query session destroy before property_names serialize.
    if (OB_FAIL(one_result_->deep_copy_property_names(query_->get_select_columns()))) { // normal query should reset select column
      LOG_WARN("fail to assign query column name", K(ret));
    }
  }
  return ret;
}

int ObTableFilterOperator::add_row(table::ObTableQueryResult *next_result, ObNewRow *row)
{
  int ret = OB_SUCCESS;
  ObNewRow new_row;
  const ObIArray<ObString> &select_columns = query_->get_select_columns();
  if (!select_columns.empty()) {
    size_t new_size = select_columns.count();
    size_t old_size = full_column_name_.count();
    ObObj cell_arr[new_size];
    new_row.assign(cell_arr, new_size);
    for (size_t i = 0; i < old_size; i ++) {
      int64_t idx = -1;
      if (!has_exist_in_array(select_columns, full_column_name_.at(i), &idx)) {
        // do nothing
      } else {
        cell_arr[idx] = row->get_cell(i);
      }
    }
    if (OB_FAIL(next_result->add_row(new_row))) {
      LOG_WARN("failed to add row", K(ret));
    }
  } else { // query select column is empty when do queryAndMutate
    if (OB_FAIL(next_result->add_row(*row))) {
      LOG_WARN("failed to add row", K(ret));
    }
  }
  return ret;
}

int ObTableFilterOperator::get_next_result(ObTableQueryResult *&next_result)
{
  int ret = OB_SUCCESS;
  if (is_aggregate_query()) {
    if (OB_FAIL(get_aggregate_result(next_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failt to get aggregate result", K(ret));
      }
    }
  } else {
    if (OB_FAIL(get_normal_result(next_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get normal result", K(ret));
      }
    }
  }
  return ret;
}

int ObTableFilterOperator::init_aggregation()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_agg_calculator().init())) {
    LOG_WARN("failed to init agg_calculator", K(ret));
  }
  return ret;
}

int ObTableFilterOperator::get_aggregate_result(table::ObTableQueryResult *&next_result) {
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;
  if (OB_ISNULL(one_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one_result_ should not be null", K(ret));
  } else {
    const int32_t limit = query_->get_limit();
    const int32_t offset = query_->get_offset();
    const bool has_limit = (limit != -1);
    bool has_reach_limit = (row_idx_ >= offset + limit);
    const ObIArray<ObString> &select_columns = one_result_->get_select_columns();
    const int64_t N = select_columns.count();
    while (OB_SUCC(ret) && (!has_limit || !has_reach_limit) &&
           OB_SUCC(scan_result_->get_next_row(row))) {
      if (N != row->get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select column count is not equal to row cells count", K(ret), K(select_columns), K(*row));
        continue;
      }

      bool filtered = false;
      if (OB_FAIL(tfilter_->filter_row(select_columns, *row, filtered))) {
        LOG_WARN("filter row error", K(ret));
        continue;
      } else if (filtered) {
        continue;
      }

      if (has_limit && row_idx_ < offset) {
        row_idx_++;
      } else if (OB_FAIL(agg_calculator_.aggregate(*row))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get aggregate ", K(ret));
      } else {
        row_idx_++;
      }
      has_reach_limit = (row_idx_ >= offset + limit);
    } // end while

    if (OB_SUCC(ret) && (has_limit && has_reach_limit)) {
      ret = OB_ITER_END;
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      agg_calculator_.final_aggregate(); // agg sum/svg finally
      has_more_rows_ = false;
      one_result_->reset();
      if (OB_FAIL(one_result_->deep_copy_property_names(get_agg_calculator().get_agg_columns()))) {
        LOG_WARN("fail to deep copy property names to one result", K(ret));
      } else if (OB_FAIL(one_result_->add_row(agg_calculator_.get_aggregate_results()))) {
        LOG_WARN("fail to add aggregation result", K(ret), K(agg_calculator_.get_aggregate_results()));
      } else {
        next_result = one_result_;
      }
    }
  }
  return ret;
}

int ObTableFilterOperator::get_normal_result(table::ObTableQueryResult *&next_result) {
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_limit_param())) {
    LOG_WARN("fail to check limit param", K(ret));
  } else if (OB_ISNULL(one_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one_result_ should not be null", K(ret));
  } else {
    one_result_->reset_except_property();
  }

  if (OB_SUCC(ret)) {
    if (NULL != last_row_) {
      if (OB_FAIL(add_row(one_result_, last_row_))) {
        LOG_WARN("failed to add row", K(ret));
      } else {
        row_idx_++;
        last_row_ = NULL;
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int32_t limit = query_->get_limit();
    const int32_t offset = query_->get_offset();
    const bool has_limit = (limit != -1);
    bool has_reach_limit = (row_idx_ >= offset + limit);
    next_result = one_result_;
    ObNewRow *row = nullptr;
    const ObIArray<ObString> &select_columns = full_column_name_;
    const int64_t N = select_columns.count();

    while (OB_SUCC(ret) && (!has_limit || !has_reach_limit) &&
           OB_SUCC(scan_result_->get_next_row(row))) {
      if (N != row->get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select column count is not equal to row cells count", K(ret), K(select_columns), K(*row));
        continue;
      }

      bool filtered = false;
      if (OB_FAIL(tfilter_->filter_row(select_columns, *row, filtered))) {
        LOG_WARN("filter row error", K(ret));
        continue;
      } else if (filtered) {
        continue;
      }

      if (has_limit && row_idx_ < offset) {
        row_idx_++;
      } else if (OB_FAIL(add_row(one_result_, row))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          ret = OB_SUCCESS;
          last_row_ = row;
          break;
        } else {
          LOG_WARN("failed to add row", K(ret));
        }
      } else {
        row_idx_++;
        if (one_result_->reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
          NG_TRACE(tag9);
          break;
        } else {
          LOG_DEBUG("scan return one row", K(*row));
        }
      }
      has_reach_limit = (row_idx_ >= offset + limit);
    } // end while

    if (OB_SUCC(ret) && (has_limit && has_reach_limit)) {
      ret = OB_ITER_END;
    }

    if (OB_ITER_END == ret) {
      has_more_rows_ = false;
      if (one_result_->get_row_count() > 0) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}