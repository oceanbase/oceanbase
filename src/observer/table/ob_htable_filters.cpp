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
#include "ob_htable_filters.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::table::hfilter;
Filter::Filter()
  :is_reversed_(false)
{
}

Filter::~Filter()
{}

FilterBase::~FilterBase() {}
////////////////////////////////////////////////////////////////
CompareFilter::~CompareFilter()
{}

const char* FilterBase::compare_operator_to_string(CompareOperator cmp_op)
{
  const char* op_str = "UNKNOWN";
  switch (cmp_op) {
    case CompareOperator::EQUAL:
      op_str = "EQUAL";
      break;
    case CompareOperator::GREATER:
      op_str = "GREATER";
      break;
    case CompareOperator::GREATER_OR_EQUAL:
      op_str = "GREATER_OR_EQUAL";
      break;
    case CompareOperator::LESS:
      op_str = "LESS";
      break;
    case CompareOperator::LESS_OR_EQUAL:
      op_str = "LESS_OR_EQUAL";
      break;
    case CompareOperator::NO_OP:
      op_str = "NO_OP";
      break;
    case CompareOperator::NOT_EQUAL:
      op_str = "NOT_EQUAL";
      break;
    default:
      break;
  }
  return op_str;
}

int CompareFilter::check_arguments() const
{
  int ret = OB_SUCCESS;
  if (NULL != dynamic_cast<RegexStringComparator*>(comparator_)
      || NULL != dynamic_cast<SubStringComparator*>(comparator_)) {
    if (CompareOperator::EQUAL != cmp_op_
        && CompareOperator::NOT_EQUAL != cmp_op_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("regexstring comparator and substring comparator can only be used with EQUAL or NOT_EQUAL", K(ret));
    }
  }
  return ret;
}


// return true indicates it will be filtered
bool CompareFilter::compare(CompareOperator op, int cmp_ret)
{
  bool bret = true;
  switch (op) {
    case CompareOperator::EQUAL:
      bret = (0 != cmp_ret);
      break;
    case CompareOperator::GREATER:
      bret = (cmp_ret >= 0);
      break;
    case CompareOperator::GREATER_OR_EQUAL:
      bret = (cmp_ret > 0);
      break;
    case CompareOperator::LESS:
      bret = (cmp_ret <= 0);
      break;
    case CompareOperator::LESS_OR_EQUAL:
      bret = (cmp_ret < 0);
      break;
    case CompareOperator::NOT_EQUAL:
      bret = (cmp_ret == 0);
      break;
    case CompareOperator::IS:
      bret = (0 != cmp_ret);
      break;
    case CompareOperator::IS_NOT:
      bret = (0 != cmp_ret);
      break;
    default:
      break;
  }
  return bret;
}

bool CompareFilter::compare_row(CompareOperator op, Comparable &comparator, const ObHTableCell &cell)
{
  bool bret = false;
  if (CompareOperator::NO_OP == op) {
    bret = true;
  } else {
    int cmp_ret = comparator.compare_to(cell.get_rowkey());
    bret = compare(op, cmp_ret);
  }
  return bret;
}

bool CompareFilter::compare_qualifier(CompareOperator op, Comparable &comparator, const ObHTableCell &cell)
{
  bool bret = false;
  if (CompareOperator::NO_OP == op) {
    bret = true;
  } else {
    int cmp_ret = comparator.compare_to(cell.get_qualifier());
    bret = compare(op, cmp_ret);
  }
  return bret;
}

bool CompareFilter::compare_value(CompareOperator op, Comparable &comparator, const ObHTableCell &cell)
{
  bool bret = false;
  if (CompareOperator::NO_OP == op) {
    bret = true;
  } else {
    int cmp_ret = comparator.compare_to(cell.get_value());
    bret = compare(op, cmp_ret);
  }
  return bret;
}

////////////////////////////////////////////////////////////////
int BinaryComparator::compare_to(const ObString &b)
{
  return comparator_value_.compare(b);
}

int BinaryPrefixComparator::compare_to(const ObString &b)
{
  int cmp_ret = 0;
  if (b.length() <= comparator_value_.length()) {
    cmp_ret = comparator_value_.compare(b);
  } else {
    ObString b_prefix(comparator_value_.length(), b.ptr());
    cmp_ret = comparator_value_.compare(b_prefix);
  }
  return cmp_ret;
}

int RegexStringComparator::compare_to(const ObString &b)
{
  // @todo
  UNUSED(b);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "regexstring comparator not supported yet");
  return 0;
}

// If value_ is substring of b, return 0; otherwise return 1
int SubStringComparator::compare_to(const ObString &b)
{
  int cmp_ret = 0;
  char *a_dup = strndupa(comparator_value_.ptr(), comparator_value_.length());
  char *b_dup = strndupa(b.ptr(), b.length());
  if (NULL == a_dup || NULL == b_dup) {
    LOG_WARN_RET(common::OB_ALLOCATE_MEMORY_FAILED, "failed to dup string");
  } else {
    char* p = strcasestr(b_dup, a_dup);
    cmp_ret = (NULL == p) ? 1: 0;
  }
  return cmp_ret;
}

////////////////////////////////////////////////////////////////
RowFilter::~RowFilter()
{}

void RowFilter::reset()
{
  filter_out_row_ = false;
}

bool RowFilter::filter_row_key(const ObHTableCell &first_row_cell)
{
  filter_out_row_ = compare_row(cmp_op_, *comparator_, first_row_cell);
  return filter_out_row_;
}

int RowFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  UNUSED(cell);
  ret_code = (filter_out_row_)?(ReturnCode::NEXT_ROW):(ReturnCode::INCLUDE);
  return ret;
}

bool RowFilter::filter_row()
{
  return filter_out_row_;
}

////////////////////////////////////////////////////////////////
QualifierFilter::~QualifierFilter()
{}

int QualifierFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  ret_code = ReturnCode::INCLUDE;
  const ObString qualifier = cell.get_qualifier();
  if (qualifier.length() > 0) {
    if (compare_qualifier(cmp_op_, *comparator_, cell)) {
      ret_code = ReturnCode::SKIP;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ValueFilter::~ValueFilter() {}
int ValueFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  ret_code = ReturnCode::INCLUDE;
  if (compare_value(cmp_op_, *comparator_, cell)) {
    ret_code = ReturnCode::SKIP;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
FilterListBase::~FilterListBase()
{}

int FilterListBase::add_filter(Filter *filter)
{
  int ret = OB_SUCCESS;
  if (NULL == filter) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter is nullptr", K(ret));
  } else if (OB_FAIL(filters_.push_back(filter))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

const char* FilterListBase::operator_to_string(Operator op)
{
  const char* op_str = "UNKNOWN";
  switch(op) {
    case Operator::MUST_PASS_ALL:
      op_str = "AND";
      break;
    case Operator::MUST_PASS_ONE:
      op_str = "OR";
      break;
    default:
      break;
  }
  return op_str;
}

void FilterListBase::reset()
{
  const int64_t N = filters_.count();
  for (int64_t i = 0; i < N; ++i)
  {
    filters_.at(i)->reset();
  } // end for
}
////////////////////////////////////////////////////////////////
FilterListAND::~FilterListAND()
{}

// Maximal Step Rule
Filter::ReturnCode FilterListAND::merge_return_code(ReturnCode rc, ReturnCode local_rc)
{
  ReturnCode ret_code = local_rc;
  if (rc == ReturnCode::SEEK_NEXT_USING_HINT) {
    ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
  } else {
    switch (local_rc) {
      case ReturnCode::SEEK_NEXT_USING_HINT:
        ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
        break;
      case ReturnCode::INCLUDE:
        ret_code = rc;
        break;
      case ReturnCode::INCLUDE_AND_NEXT_COL:
        if (rc == ReturnCode::INCLUDE
            || rc == ReturnCode::INCLUDE_AND_NEXT_COL) {
          ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
        } else if (rc == ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW) {
          ret_code = ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW;
        } else if (rc == ReturnCode::SKIP
                   || rc == ReturnCode::NEXT_COL) {
          ret_code = ReturnCode::NEXT_COL;
        } else if (rc == ReturnCode::NEXT_ROW) {
          ret_code = ReturnCode::NEXT_ROW;
        }
        break;
      case ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW:
        if (rc == ReturnCode::INCLUDE
            || rc == ReturnCode::INCLUDE_AND_NEXT_COL
            || rc == ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW) {
          ret_code = ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW;
        } else if (rc == ReturnCode::SKIP
                   || rc == ReturnCode::NEXT_COL
                   || rc == ReturnCode::NEXT_ROW) {
          ret_code = ReturnCode::NEXT_ROW;
        }
        break;
      case ReturnCode::SKIP:
        if (rc == ReturnCode::INCLUDE
            || rc == ReturnCode::SKIP) {
          ret_code = ReturnCode::SKIP;
        } else if (rc == ReturnCode::INCLUDE_AND_NEXT_COL
                   || rc == ReturnCode::NEXT_COL) {
          ret_code = ReturnCode::NEXT_COL;
        } else if (rc == ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW
                   || rc == ReturnCode::NEXT_ROW) {
          ret_code = ReturnCode::NEXT_ROW;
        }
        break;
      case ReturnCode::NEXT_COL:
        if (rc == ReturnCode::INCLUDE
            || rc == ReturnCode::INCLUDE_AND_NEXT_COL
            || rc == ReturnCode::SKIP
            || rc == ReturnCode::NEXT_COL) {
          ret_code = ReturnCode::NEXT_COL;
        } else if (rc == ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW
                   || rc == ReturnCode::NEXT_ROW) {
          ret_code = ReturnCode::NEXT_ROW;
        }
        break;
      case ReturnCode::NEXT_ROW:
        ret_code = ReturnCode::NEXT_ROW;
        break;
      default:
        break;
    }  // end switch
  }
  return ret_code;
}

int FilterListAND::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (filters_.empty()) {
    ret_code = ReturnCode::INCLUDE;
  } else {
    ret_code = ReturnCode::INCLUDE;
    seek_hint_filters_.reset();
    const int64_t N = filters_.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      Filter *filter = filters_.at(i);
      if (filter->filter_all_remaining()) {
        ret_code = ReturnCode::NEXT_ROW;
        break;
      } else {
        ReturnCode local_rc;
        if (OB_FAIL(filter->filter_cell(cell, local_rc))) {
          LOG_WARN("failed to filter cell", K(ret));
        } else {
          ret_code = merge_return_code(ret_code, local_rc);
          LOG_DEBUG("[yzfdebug] AND filter cell", K(i), K(local_rc), K(ret_code));
          if (local_rc == ReturnCode::SEEK_NEXT_USING_HINT) {
            if (OB_FAIL(seek_hint_filters_.push_back(filter))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
    } // end for
    if (!seek_hint_filters_.empty()) {
      ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
    }
  }
  return ret;
}

void FilterListAND::reset()
{
  FilterListBase::reset();
  seek_hint_filters_.reset();
}

bool FilterListAND::filter_all_remaining()
{
  bool bret = false;
  if (filters_.empty()) {
    bret = FilterListBase::filter_all_remaining();
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      if (filters_.at(i)->filter_all_remaining()) {
        bret = true;
        break;
      }
    } // end for
  }
  return bret;
}

bool FilterListAND::filter_row_key(const ObHTableCell &first_row_cell)
{
  bool bret = false;
  if (filters_.empty()) {
    bret = FilterListBase::filter_row_key(first_row_cell);
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      Filter *filter = filters_.at(i);
      if (filter->filter_all_remaining() || filter->filter_row_key(first_row_cell)) {
        bret = true;
        // can not break here
      }
    } // end for
  }
  return bret;
}

bool FilterListAND::filter_row()
{
  bool bret = false;
  if (filters_.empty()) {
    bret = FilterListBase::filter_row();
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      Filter *filter = filters_.at(i);
      if (filter->filter_row()) {
        bret = true;
        break;
      }
    } // end for
  }
  return bret;
}
////////////////////////////////////////////////////////////////
FilterListOR::~FilterListOR()
{}

void FilterListOR::reset()
{
  FilterListBase::reset();
}

bool FilterListOR::filter_all_remaining()
{
  bool bret = true;
  if (filters_.empty()) {
    bret = FilterListBase::filter_all_remaining();
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      if (!filters_.at(i)->filter_all_remaining()) {
        bret = false;
        break;
      }
    } // end for
  }
  return bret;
}

bool FilterListOR::filter_row_key(const ObHTableCell &first_row_cell)
{
  bool bret = true;
  if (filters_.empty()) {
    bret = FilterListBase::filter_row_key(first_row_cell);
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      Filter *filter = filters_.at(i);
      if (!filter->filter_all_remaining() && !filter->filter_row_key(first_row_cell)) {
        bret = false;
        // can not break here
      }
    } // end for
  }
  return bret;
}

// Minimal Step Rule
Filter::ReturnCode FilterListOR::merge_return_code(ReturnCode rc, ReturnCode local_rc)
{
  ReturnCode ret_code = local_rc;
  switch (local_rc) {
    case ReturnCode::INCLUDE:
      ret_code = ReturnCode::INCLUDE;
      break;
    case ReturnCode::INCLUDE_AND_NEXT_COL:
      if (ReturnCode::INCLUDE == rc
          || ReturnCode::SKIP == rc
          || ReturnCode::SEEK_NEXT_USING_HINT == rc) {
        ret_code = ReturnCode::INCLUDE;
      } else if (ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW == rc
                 || ReturnCode::INCLUDE_AND_NEXT_COL == rc
                 || ReturnCode::NEXT_COL == rc
                 || ReturnCode::NEXT_ROW == rc) {
        ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
      }
      break;
    case ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW:
      if (ReturnCode::INCLUDE == rc
          || ReturnCode::SKIP == rc
          || ReturnCode::SEEK_NEXT_USING_HINT == rc) {
        ret_code = ReturnCode::INCLUDE;
      } else if (ReturnCode::INCLUDE_AND_NEXT_COL == rc
                 || ReturnCode::NEXT_COL == rc) {
        ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
      } else if (ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW == rc
                 || ReturnCode::NEXT_ROW == rc) {
        ret_code = ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW;
      }
      break;
    case ReturnCode::SKIP:
      if (ReturnCode::INCLUDE == rc
          || ReturnCode::INCLUDE_AND_NEXT_COL == rc
          || ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW == rc) {
        ret_code = ReturnCode::INCLUDE;
      } else {
        ret_code = ReturnCode::SKIP;
      }
      break;
    case ReturnCode::NEXT_COL:
      {
        switch (rc) {
          case ReturnCode::INCLUDE:
            ret_code = ReturnCode::INCLUDE;
            break;
          case ReturnCode::NEXT_COL:
          case ReturnCode::NEXT_ROW:
            ret_code = ReturnCode::NEXT_COL;
            break;
          case ReturnCode::INCLUDE_AND_NEXT_COL:
          case ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW:
            ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
            break;
          case ReturnCode::SKIP:
          case ReturnCode::SEEK_NEXT_USING_HINT:
            ret_code = ReturnCode::SKIP;
            break;
          default:
            LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "BUG");
            break;
        }
      }
      break;
    case ReturnCode::NEXT_ROW:
      {
        switch (rc) {
          case ReturnCode::INCLUDE:
            ret_code = ReturnCode::INCLUDE;
            break;
          case ReturnCode::NEXT_COL:
            ret_code = ReturnCode::NEXT_COL;
            break;
          case ReturnCode::NEXT_ROW:
            ret_code = ReturnCode::NEXT_ROW;
            break;
          case ReturnCode::INCLUDE_AND_NEXT_COL:
            ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
            break;
          case ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW:
            ret_code = ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW;
            break;
          case ReturnCode::SKIP:
          case ReturnCode::SEEK_NEXT_USING_HINT:
            ret_code = ReturnCode::SKIP;
            break;
          default:
            LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "BUG");
            break;
        }
      }
      break;
    case ReturnCode::SEEK_NEXT_USING_HINT:
      {
        switch (rc) {
          case ReturnCode::INCLUDE:
          case ReturnCode::INCLUDE_AND_NEXT_COL:
          case ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW:
            ret_code = ReturnCode::INCLUDE;
            break;
          case ReturnCode::SKIP:
          case ReturnCode::NEXT_COL:
          case ReturnCode::NEXT_ROW:
            ret_code = ReturnCode::SKIP;
            break;
          case ReturnCode::SEEK_NEXT_USING_HINT:
            ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
            break;
          default:
            LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "BUG", K(rc));
            break;
        }
      }
      break;
    default:
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "BUG", K(local_rc));
      break;
  }  // end switch
  return ret_code;
}

int FilterListOR::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (filters_.empty()) {
    ret_code = ReturnCode::INCLUDE;
  } else {
    bool every_filter_return_hint = true;
    ret_code = ReturnCode::SKIP;  // Each sub-filter in filter list got true for filterAllRemaining().
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      Filter *filter = filters_.at(i);
      if (filter->filter_all_remaining()) {
        continue;
      } else {
        ReturnCode local_rc;
        if (OB_FAIL(filter->filter_cell(cell, local_rc))) {
          LOG_WARN("failed to filter cell", K(ret));
        } else {
          if (ReturnCode::SEEK_NEXT_USING_HINT != local_rc) {
            every_filter_return_hint = false;
          }
          ret_code = merge_return_code(ret_code, local_rc);
          LOG_DEBUG("[yzfdebug] OR filter cell", K(i), K(local_rc), K(ret_code));
        }
      }
    } // end for
    if (every_filter_return_hint) {
      ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
    }
  }
  return ret;
}

bool FilterListOR::filter_row()
{
  bool bret = true;
  if (filters_.empty()) {
    bret = FilterListBase::filter_row();
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N; ++i)
    {
      Filter *filter = filters_.at(i);
      if (!filter->filter_row()) {
        bret = false;
        break;
      }
    } // end for
  }
  return bret;
}
////////////////////////////////////////////////////////////////
SkipFilter::~SkipFilter() {}
void SkipFilter::reset()
{
  if (NULL != filter_) {
    filter_->reset();
  }
  filter_row_ = false;
}

bool SkipFilter::filter_row_key(const ObHTableCell &first_row_cell)
{
  UNUSED(first_row_cell);
  return false;
}

// A wrapper filter that filters an entire row if any of the Cell checks do not pass.
int SkipFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filter_->filter_cell(cell, ret_code))) {
  } else {
    filter_row_ = filter_row_ || (ReturnCode::INCLUDE != ret_code);
  }
  return ret;
}

bool SkipFilter::filter_row()
{
  return filter_row_;
}

int SkipFilter::transform_cell(const ObHTableCell &cell, const ObHTableCell *&new_cell)
{
  return filter_->transform_cell(cell, new_cell);
}
////////////////////////////////////////////////////////////////
WhileMatchFilter::~WhileMatchFilter() {}

void WhileMatchFilter::reset()
{
  if (NULL != filter_) {
    filter_->reset();
  }
}

bool WhileMatchFilter::filter_all_remaining()
{
  return filter_all_remaining_ || filter_->filter_all_remaining();
}

bool WhileMatchFilter::filter_row_key(const ObHTableCell &first_row_cell)
{
  bool bret = true;
  if (filter_all_remaining()) {
    bret = true;
  } else {
    bret = filter_->filter_row_key(first_row_cell);
    filter_all_remaining_ = filter_all_remaining_ || bret;
  }
  return bret;
}

int WhileMatchFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filter_->filter_cell(cell, ret_code))) {
  } else {
    filter_all_remaining_ = filter_all_remaining_ || (ReturnCode::INCLUDE != ret_code);
  }
  return ret;
}

int WhileMatchFilter::transform_cell(const ObHTableCell &cell, const ObHTableCell *&new_cell)
{
  return filter_->transform_cell(cell, new_cell);
}

bool WhileMatchFilter::filter_row()
{
  bool bret = filter_->filter_row();
  filter_all_remaining_ = filter_all_remaining_ || bret;
  return bret;
}

////////////////////////////////////////////////////////////////
SingleColumnValueFilter::~SingleColumnValueFilter()
{}

void SingleColumnValueFilter::reset()
{
  LOG_DEBUG("[yzfdebug] reset SingleColumnValueFilter");
  found_column_ = false;
  matched_column_ = false;
}

int SingleColumnValueFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (matched_column_) {
    // already found and matched the single column
    ret_code = ReturnCode::INCLUDE;
    LOG_DEBUG("[yzfdebug] already matched column", K(ret_code));
  } else if (latest_version_only_ && found_column_) {
    // found but not matched the column
    ret_code = ReturnCode::NEXT_ROW;
    LOG_DEBUG("[yzfdebug] latest verion only but not matched", K(ret_code));
  } else if (!match_column(cell)) {
    ret_code = ReturnCode::INCLUDE;
    LOG_DEBUG("[yzfdebug] not found column yet", K(ret_code));
  } else {
    found_column_ = true;
    LOG_DEBUG("[yzfdebug] found column", K_(found_column));
    if (filter_column_value(cell)) {
      ret_code = (latest_version_only_) ? (ReturnCode::NEXT_ROW) : (ReturnCode::INCLUDE);
      LOG_DEBUG("[yzfdebug] found column but value not match", K_(latest_version_only), K(ret_code));
    } else {
      matched_column_ = true;
      ret_code = ReturnCode::INCLUDE;
      LOG_DEBUG("[yzfdebug] found column and match", K(ret_code));
    }
  }
  return ret;
}

bool SingleColumnValueFilter::match_column(const ObHTableCell &cell)
{
  return qualifier_ == cell.get_qualifier();
}

bool SingleColumnValueFilter::filter_column_value(const ObHTableCell &cell)
{
  int cmp_ret = comparator_->compare_to(cell.get_value());
  return CompareFilter::compare(cmp_op_, cmp_ret);
}

bool SingleColumnValueFilter::filter_row()
{
  // If column was found, return false if it was matched, true if it was not
  // If column not found, return true if we filter if missing, false if not
  LOG_DEBUG("[yzfdebug] filter row", K_(found_column), K_(matched_column), K_(filter_if_missing));
  return found_column_ ? (!matched_column_) : (filter_if_missing_);
}
////////////////////////////////////////////////////////////////
void ColumnCountGetFilter::reset()
{
  count_ = 0;
}

bool ColumnCountGetFilter::filter_row_key(const ObHTableCell &first_row_cell)
{
  UNUSED(first_row_cell);
  return filter_all_remaining();
}

bool ColumnCountGetFilter::filter_all_remaining()
{
  return count_ > limit_;
}

int ColumnCountGetFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  UNUSED(cell);
  ++count_;
  if (filter_all_remaining()) {
    ret_code = ReturnCode::NEXT_COL;
  } else {
    ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
  }
  return ret;
}
////////////////////////////////////////////////////////////////
CheckAndMutateFilter::~CheckAndMutateFilter()
{}

void CheckAndMutateFilter::reset()
{
  LOG_DEBUG("[yzfdebug] reset CheckAndMutateFilter");
  found_column_ = false;
  matched_column_ = false;
}

// NOTE: when value_is_null is true，cannot return other cell directly
// 判断是否 check 通过，例如当整个 column family 为空的时候，result_count = 0，但是应该是 check 通过

// NOTE: when value_is_null is true, cannot return other cells directly and determine whether check is
// passed by result_count > 0 in ObTableQueryAndMutateP::try_process.
// for example, if the whole column family is empty and result_count = 0, but check should passed.
int CheckAndMutateFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (matched_column_) {
    // already found and matched the single column
    ret_code = ReturnCode::INCLUDE;
    LOG_DEBUG("[yzfdebug] already matched column", K(ret_code));
  } else if (found_column_) {  // latest_version_only_ == true
    // found but not matched the column
    ret_code = ReturnCode::NEXT_ROW;
    LOG_DEBUG("[yzfdebug] latest verion only but not matched", K(ret_code));
  } else if (match_column(cell)) {
    found_column_ = true;
    LOG_DEBUG("[yzfdebug] found column", K_(found_column));
    if (value_is_null_ || !filter_column_value(cell)) {
      matched_column_ = true;
      ret_code = ReturnCode::INCLUDE;
      LOG_DEBUG("[yzfdebug] found column and match", K(ret_code));
    } else {
      ret_code = ReturnCode::NEXT_ROW;
      LOG_DEBUG("[yzfdebug] found column but value not match", K(ret_code));
    }
  }
  return ret;
}

bool CheckAndMutateFilter::match_column(const ObHTableCell &cell)
{
  return qualifier_ == cell.get_qualifier();
}

bool CheckAndMutateFilter::filter_column_value(const ObHTableCell &cell)
{
  int cmp_ret = comparator_->compare_to(cell.get_value());
  return CompareFilter::compare(cmp_op_, cmp_ret);
}

bool CheckAndMutateFilter::filter_row()
{
  LOG_DEBUG("[yzfdebug] filter row", K_(found_column), K_(matched_column), K_(value_is_null));
  bool bret = true;
  bret = found_column_ ? (!matched_column_) : true;
  return bret;
}
