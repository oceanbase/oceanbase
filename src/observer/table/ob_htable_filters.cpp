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
    : is_reversed_(false),
      hbase_major_version_(1)
{
}

Filter::~Filter()
{}

const char *FilterUtils::get_compare_op_name(const CompareOperator &op)
{
  const char *name = "UNKNOWN";

  switch (op) {
    case CompareOperator::EQUAL: {
      name = "EQUAL";
      break;
    }
    case CompareOperator::GREATER: {
      name = "GREATER";
      break;
    }
    case CompareOperator::GREATER_OR_EQUAL: {
      name = "GREATER_OR_EQUAL";
      break;
    }
    case CompareOperator::LESS: {
      name = "LESS";
      break;
    }
    case CompareOperator::LESS_OR_EQUAL: {
      name = "LESS_OR_EQUAL";
      break;
    }
    case CompareOperator::NO_OP: {
      name = "NO_OP";
      break;
    }
    case CompareOperator::NOT_EQUAL: {
      name = "NOT_EQUAL";
      break;
    }
    case CompareOperator::IS: {
      name = "IS";
      break;
    }
    case CompareOperator::IS_NOT: {
      name = "IS_NOT";
      break;
    }
    default: {
      name = "UNKNOWN";
    }
  }

  return name;
}

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

bool CompareFilter::compare_family(CompareOperator op, Comparable &comparator, const ObHTableCell &cell)
{
  bool bret = false;
  if (CompareOperator::NO_OP == op) {
    bret = true;
  } else {
    int cmp_ret = comparator.compare_to(cell.get_family());
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
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "regexstring comparator");
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

int RowFilter::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  filter_out_row_ = compare_row(cmp_op_, *comparator_, first_row_cell);
  filtered = filter_out_row_;
  return OB_SUCCESS;
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

// statement is "RowFilter $compare_op_name"
int64_t RowFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("RowFilter"); // "RowFilter"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int RowFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "RowFilter %s", FilterUtils::get_compare_op_name(cmp_op_));
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
    }
  }

  return ret;
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

// statement is "QualifierFilter $compare_op_name"
int64_t QualifierFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("QualifierFilter"); // "QualifierFilter"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int QualifierFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "QualifierFilter %s", FilterUtils::get_compare_op_name(cmp_op_));
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
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

// statement is "ValueFilter $compare_op_name"
int64_t ValueFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("ValueFilter"); // "ValueFilter"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int ValueFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "ValueFilter %s", FilterUtils::get_compare_op_name(cmp_op_));
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////
DependentColumnFilter::~DependentColumnFilter()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    if (stamp_set_.created()) {
      if (OB_FAIL(stamp_set_.destroy())) {
        LOG_WARN("fail to destory time_stamp_", K(ret));
      }
    }
  }
}

int DependentColumnFilter::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stamp_set_.create(10001))) {
    LOG_WARN("create timestamp set failed", KR(ret));
  }
  return ret;
}

int DependentColumnFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(init())) {
      LOG_WARN("fail to init DependentColumnFilter", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (cell.get_qualifier() != qualifier_) {
      ret_code = ReturnCode::INCLUDE;
    } else if (comparator_ != NULL && compare_value(cmp_op_, *comparator_, cell)) {
      ret_code = ReturnCode::SKIP;
    } else if (OB_FAIL(stamp_set_.set_refactored(cell.get_timestamp()))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      if (drop_dependent_column_) {
        ret_code = ReturnCode::SKIP;
      } else {
        ret_code = ReturnCode::INCLUDE;
      }
    }
  } 
  
  return ret;
}

int DependentColumnFilter::filter_row_cells(ObTableQueryDListResult &cells)
{
  int ret = OB_SUCCESS;
  common::ObDList<common::ObDLinkNode<ObHTableCellEntity*> > &cell_list = cells.get_cell_list();
  common::ObArray<common::ObDLinkNode<ObHTableCellEntity*> *> rm_list;
  DLIST_FOREACH(node, cell_list) {
    if (OB_ISNULL(node)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null pointer during filter row cells", K(ret));
    } else if (OB_ISNULL(node->get_data())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null pointer during filter row cells", K(ret));
    } else {
      int64_t real_timestamp = -node->get_data()->get_timestamp();
      if (OB_HASH_NOT_EXIST == stamp_set_.exist_refactored(real_timestamp)) {
        if (OB_FAIL(rm_list.push_back(node))) {
          LOG_WARN("fail to push back node into rm_list", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rm_list.size(); ++i) {
    common::ObDLinkNode<ObHTableCellEntity*> *node = rm_list.at(i);
    // node will not be NULL or header
    cell_list.remove(node);
    int64_t &row_count = cells.get_row_count();
    --row_count;
  }
  return ret;
}

void DependentColumnFilter::reset()
{
  stamp_set_.reuse();
}

int64_t DependentColumnFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("DependentColumnFilter"); // "DependentColumnFilter"
  len += 1; // blank
  len += family_.length(); // "$family"
  len += 1; // blank
  len += qualifier_.length(); // "$qualifier"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int DependentColumnFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "DependentColumnFilter ");
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      strncat(buf + pos, family_.ptr(), family_.length());
      pos += family_.length();
      int64_t n = snprintf(buf + pos, buf_len - pos, " ");
      if (n < 0 || n > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
      } else {
        pos += n;
        strncat(buf + pos, qualifier_.ptr(), qualifier_.length());
        pos += qualifier_.length();
        int64_t n = snprintf(buf + pos, buf_len - pos, " %s", FilterUtils::get_compare_op_name(cmp_op_));
        if (n < 0 || n > buf_len - pos) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
        } else {
          pos += n;
        }
      }
    }
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
  } else if (OB_FAIL(cell_included_.push_back(false))) {
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
  for (int64_t i = 0; i < N; ++i) {
    filters_.at(i)->reset();
    cell_included_.at(i) = false;
  } // end for
}

void FilterListBase::set_hbase_version(const ObString &version)
{
  const int64_t N = filters_.count();
  for (int64_t i = 0; i < N; ++i) {
    Filter* filter = nullptr;
    if (nullptr != (filter = filters_.at(i))) {
      filter->set_hbase_version(version);
    }
  } // end for
  Filter::set_hbase_version(version);
}

// statement is "$list_filter_name $operator $filter0, $filter1, ..., $filtern"
// eg: "FilterListAND AND ValueFilter EQUAL, ValueFilter GREATER"
int64_t FilterListBase::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen(filter_name()); // "$list_filter_name"
  len += 1; // blank
  len += strlen(operator_to_string(op_)); // "$operator"
  len += 1; // blank

  int64_t N = filters_.count();
  for (int64_t i = 0; i < N - 1; i++) {
    Filter *filter = filters_.at(i);
    if (OB_NOT_NULL(filter)) {
      len += filter->get_format_filter_string_length();
      len += 2; // ", "
    }
  }
  if (0 < N) {
    Filter *filter = filters_.at(N - 1);
    if (OB_NOT_NULL(filter)) {
      len += filter->get_format_filter_string_length();
    }
  }

  return len;
}

int FilterListBase::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "%s %s ", filter_name(), operator_to_string(op_));
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      int64_t N = filters_.count();
      for (int64_t i = 0; i < N - 1 && OB_SUCC(ret); i++) {
        Filter *filter = filters_.at(i);
        if (OB_NOT_NULL(filter) && OB_FAIL(filter->get_format_filter_string(buf, buf_len, pos))) {
          LOG_WARN("fail to get format filter string", KR(ret), K(n), K(pos), K(buf_len));
        } else {
          J_COMMA();
        }
      }
      if (OB_SUCC(ret) && 0 < N) {
        Filter *filter = filters_.at(N - 1);
        if (OB_NOT_NULL(filter) && OB_FAIL(filter->get_format_filter_string(buf, buf_len, pos))) {
          LOG_WARN("fail to get format filter string", KR(ret), K(n), K(pos), K(buf_len));
        }
      }
    }
  }

  return ret;
}

void FilterListBase::set_reversed(bool reversed)
{
  const int64_t N = filters_.count();
  for (int64_t i = 0; i < N; ++i) {
    filters_.at(i)->set_reversed(reversed);
  } // end for
}

////////////////////////////////////////////////////////////////
FilterListAND::~FilterListAND()
{}

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
  ret_code = ReturnCode::INCLUDE;
  seek_hint_filters_.reset();
  ReturnCode local_rc = ReturnCode::INCLUDE;
  const int64_t N = filters_.count();
  bool loop = true;
  int64_t i = 0;
  for (; OB_SUCCESS == ret && i < N && loop; ++i) {
    Filter *filter = filters_.at(i);
    if (filter->filter_all_remaining()) {
      ret_code = ReturnCode::NEXT_ROW;
      loop = false;
    } else {
      if (OB_FAIL(filter->filter_cell(cell, local_rc))) {
        LOG_WARN("failed to filter cell", K(ret));
        loop = false;
      } else {
        if (get_hbase_major_version() < 2) {
          ret_code = local_rc;
        } else {
          ret_code = merge_return_code(ret_code, local_rc);
        }
        switch (ret_code) {
          case ReturnCode::INCLUDE_AND_NEXT_COL:
          case ReturnCode::INCLUDE:
          case ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW: {
            break;
          }
          case ReturnCode::SEEK_NEXT_USING_HINT: {  
            if (OB_FAIL(seek_hint_filters_.push_back(filter))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
          default: {
            loop = false;
          }
        }
      }
    }
  }// end for  
  for (; i < N && ret == OB_SUCCESS; ++i) {
    Filter *filter = filters_.at(i);
    if (filter != NULL && filter->is_hinting_filter()) {
      if (OB_FAIL(filter->filter_cell(cell, local_rc))) {
        LOG_WARN("failed to filter cell", K(ret));
      } else if (local_rc == ReturnCode::SEEK_NEXT_USING_HINT) {
        if (OB_FAIL(seek_hint_filters_.push_back(filter))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !seek_hint_filters_.empty()) {
    ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
  }
  return ret;
}

int FilterListAND::transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell)
{
  int ret = OB_SUCCESS;
  const int64_t N = filters_.count();
    for (int64_t i = 0; i < N && OB_SUCC(ret); ++i) {
      Filter* filter = filters_.at(i);
      if (OB_NOT_NULL(filter) && OB_FAIL(filter->transform_cell(allocator, cell))) {
        LOG_WARN("failed to transform cell", K(ret), KP(filter), K(cell));
      }
    } // end for
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

int FilterListAND::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (filters_.empty()) {
    ret = FilterListBase::filter_row_key(first_row_cell, filtered);
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N && OB_SUCC(ret); ++i) {
      Filter *filter = filters_.at(i);
      bool cur_filtered = false;
      if (OB_ISNULL(filter)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("filter is null", K(ret));
      } else if (filter->filter_all_remaining()) {
        filtered = true;
        // can not break here
      } else if (OB_FAIL(filter->filter_row_key(first_row_cell, cur_filtered))) {
        LOG_WARN("failed to filter row key", K(ret), KP(filter));
      } else if (cur_filtered) {
        filtered = true;
        // can not break here
      }
    } // end for
  }
  return ret;
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

int FilterListAND::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObHTableCell* max_cell = NULL;
  if (!seek_hint_filters_.empty()) {
    const int64_t N = seek_hint_filters_.count();
    for (int64_t i = 0; i < N && ret == OB_SUCCESS; ++i) {
      Filter *filter = seek_hint_filters_.at(i);
      if (filter != NULL && !filter->filter_all_remaining()) {
        ObHTableCell* get_cell = NULL;
        if (OB_FAIL(filter->get_next_cell_hint(allocator, cell, get_cell))) {
          LOG_WARN("failed to get next hint cell of filter", K(ret), K(*filter));
        } else {
          if (get_cell != NULL) {
            if (max_cell == NULL) {
              max_cell = get_cell;
            } else if (ObHTableUtils::compare_cell(*max_cell, *get_cell, is_reversed()) > 0) {
              max_cell = get_cell;
            }
          }
        }
      }
    } // end for
  }
  new_cell = max_cell;
  return ret;
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
    for (int64_t i = 0; i < N; ++i) {
      if (!filters_.at(i)->filter_all_remaining()) {
        bret = false;
        break;
      }
    } // end for
  }
  return bret;
}

int FilterListOR::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (filters_.empty()) {
    ret = FilterListBase::filter_row_key(first_row_cell, filtered);
  } else {
    const int64_t N = filters_.count();
    for (int64_t i = 0; i < N && OB_SUCC(ret); ++i)
    {
      Filter *filter = filters_.at(i);
      bool cur_filtered = true;
      if (OB_ISNULL(filter)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("filter is null", K(ret));
      } else if (!filter->filter_all_remaining()) {
        if (OB_FAIL(filter->filter_row_key(first_row_cell, cur_filtered))) {
          LOG_WARN("failed to filter row key", K(ret), KP(filter));
        } else if (!cur_filtered) {
          filtered = false;
          // can not break here
        }
      }
    } // end for
  }
  return ret;
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
    for (int64_t i = 0; i < N; ++i) {
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
          if (ReturnCode::INCLUDE == local_rc 
              || ReturnCode::INCLUDE_AND_NEXT_COL == local_rc 
              || ReturnCode::INCLUDE_AND_SEEK_NEXT_ROW == local_rc) {
            cell_included_.at(i) = true;
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

int FilterListOR::transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell)
{
  int ret = OB_SUCCESS;
  const int64_t N = filters_.count();
    for (int64_t i = 0; i < N && OB_SUCC(ret); ++i) {
      bool included = cell_included_.at(i);
      if (included) {
        Filter* filter = filters_.at(i);
        if (OB_NOT_NULL(filter) && OB_FAIL(filter->transform_cell(allocator, cell))) {
          LOG_WARN("failed to transform cell", K(ret), KP(filter), K(cell));
        }
      }
    } // end for
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

int FilterListOR::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObHTableCell* min_cell = NULL;
  if (!filters_.empty()) {
    const int64_t N = filters_.count();
    bool loop = true;
    for (int64_t i = 0; i < N && ret == OB_SUCCESS && loop; ++i) {
      Filter *filter = filters_.at(i);
      if (filter != NULL && !filter->filter_all_remaining()) {
        ObHTableCell* get_cell = NULL;
        if (OB_FAIL(filter->get_next_cell_hint(allocator, cell, get_cell))) {
          LOG_WARN("failed to get next hint cell of filter", K(ret), K(*filter));
        } else {
          if (get_cell == NULL) {
            min_cell = NULL;
            loop = false;
          } else if (min_cell == NULL) {
            min_cell = get_cell;
          } else if (ObHTableUtils::compare_cell(*min_cell, *get_cell, is_reversed()) > 0) {
            min_cell = get_cell;
          }
        }
      }
    }
  }
  new_cell = min_cell;
  return ret;
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

int SkipFilter::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  UNUSED(first_row_cell);
  filtered = false;
  return OB_SUCCESS;
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

int SkipFilter::transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell)
{
  return filter_->transform_cell(allocator, cell);
}

// statement is "SkipFilter $filter"
int64_t SkipFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("SkipFilter"); // "SkipFilter"
  len += 1; // blank
  // "$filter"
  if (OB_NOT_NULL(filter_)) {
    len += filter_->get_format_filter_string_length();
  }

  return len;
}

int SkipFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "SkipFilter ");
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      if (OB_NOT_NULL(filter_) && OB_FAIL(filter_->get_format_filter_string(buf, buf_len, pos))) {
        LOG_WARN("fail to get format filter string", KR(ret), K(n), K(pos), K(buf_len));
      }
    }
  }

  return ret;
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

int WhileMatchFilter::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (filter_all_remaining()) {
    filtered = true;
  } else {
    if (OB_FAIL(filter_->filter_row_key(first_row_cell, filtered))) {
      LOG_WARN("failed to filter row key", K(ret));
    } else {
      filter_all_remaining_ = filter_all_remaining_ || filtered;
    }
  }
  return ret;
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

int WhileMatchFilter::transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell)
{
  return filter_->transform_cell(allocator, cell);
}

bool WhileMatchFilter::filter_row()
{
  bool bret = filter_->filter_row();
  filter_all_remaining_ = filter_all_remaining_ || bret;
  return bret;
}



// statement is "WhileMatchFilter $filter"
int64_t WhileMatchFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("WhileMatchFilter"); // "WhileMatchFilter"
  len += 1; // blank
  // "$filter"
  if (OB_NOT_NULL(filter_)) {
    len += filter_->get_format_filter_string_length();
  }

  return len;
}

int WhileMatchFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "WhileMatchFilter ");
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      if (OB_NOT_NULL(filter_) && OB_FAIL(filter_->get_format_filter_string(buf, buf_len, pos))) {
        LOG_WARN("fail to get format filter string", KR(ret), K(n), K(pos), K(buf_len));
      }
    }
  }

  return ret;
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
  } else if (!match_family_column(cell)) {
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

bool SingleColumnValueFilter::match_family_column(const ObHTableCell &cell)
{
  return family_ == cell.get_family() && qualifier_ == cell.get_qualifier();
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

// statement is "SingleColumnValueFilter $family $qualifier $compare_op_name"
int64_t SingleColumnValueFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("SingleColumnValueFilter"); // "WhileMatchFilter"
  len += 1; // blank
  len += family_.length(); // "$family"
  len += 1; // blank
  len += qualifier_.length(); // "$qualifier"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int SingleColumnValueFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "SingleColumnValueFilter ");
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      strncat(buf + pos, family_.ptr(), family_.length());
      pos += family_.length();
      int64_t n = snprintf(buf + pos, buf_len - pos, " ");
      if (n < 0 || n > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
      } else {
        pos += n;
        strncat(buf + pos, qualifier_.ptr(), qualifier_.length());
        pos += qualifier_.length();
        int64_t n = snprintf(buf + pos, buf_len - pos, " %s", FilterUtils::get_compare_op_name(cmp_op_));
        if (n < 0 || n > buf_len - pos) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
        } else {
          pos += n;
        }
      }
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////
SingleColumnValueExcludeFilter::~SingleColumnValueExcludeFilter()
{}

int SingleColumnValueExcludeFilter::filter_row_cells(ObTableQueryDListResult &cells)
{
  int ret = OB_SUCCESS;
  common::ObDList<common::ObDLinkNode<ObHTableCellEntity*> > &cell_list = cells.get_cell_list();
  common::ObArray<common::ObDLinkNode<ObHTableCellEntity*> *> rm_list;
  DLIST_FOREACH(node, cell_list) {
    if (OB_ISNULL(node)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null pointer during filter row cells", K(ret));
    } else if (OB_ISNULL(node->get_data())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null pointer during filter row cells", K(ret));
    } else if (match_family_column(*node->get_data())) {
      if (OB_FAIL(rm_list.push_back(node))) {
        LOG_WARN("fail to push back node into rm_list", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rm_list.size(); ++i) {
    common::ObDLinkNode<ObHTableCellEntity*> *node = rm_list.at(i);
    // node will not be NULL or header
    cell_list.remove(node);
    int64_t &row_count = cells.get_row_count();
    --row_count;
  }
  
  return ret;
}

// statement is "SingleColumnValueFilter $family $qualifier $compare_op_name"
int64_t SingleColumnValueExcludeFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("SingleColumnValueExcludeFilter"); // "SingleColumnValueExcludeFilter"
  len += 1; // blank
  len += family_.length(); // "$family"
  len += 1; // blank
  len += qualifier_.length(); // "$qualifier"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int SingleColumnValueExcludeFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "SingleColumnValueExcludeFilter ");
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      strncat(buf + pos, family_.ptr(), family_.length());
      pos += family_.length();
      int64_t n = snprintf(buf + pos, buf_len - pos, " ");
      if (n < 0 || n > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
      } else {
        pos += n;
        strncat(buf + pos, qualifier_.ptr(), qualifier_.length());
        pos += qualifier_.length();
        int64_t n = snprintf(buf + pos, buf_len - pos, " %s", FilterUtils::get_compare_op_name(cmp_op_));
        if (n < 0 || n > buf_len - pos) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
        } else {
          pos += n;
        }
      }
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////
bool PageFilter::filter_row()
{
  rows_accepted_++;
  return rows_accepted_ > page_size_;
}

int PageFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  UNUSED(cell);
  ret_code = ReturnCode::INCLUDE;
  return common::OB_SUCCESS;
}

bool PageFilter::filter_all_remaining()
{
  return rows_accepted_ >= page_size_;
}

// statement is "PageFilter"
int64_t PageFilter::get_format_filter_string_length() const
{
  return strlen("PageFilter");
}

int PageFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "PageFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int RandomRowFilter::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  UNUSED(first_row_cell);
  if (chance_ < 0) {
      filter_out_row_ = true;
  } else if (chance_ > 1) {
      filter_out_row_ = false;
  } else {
      filter_out_row_ = !((float(rand()) / (RAND_MAX + 1.0f)) < chance_);
  }
  filtered = filter_out_row_;
  return OB_SUCCESS;
}

int RandomRowFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  UNUSED(cell);
  ret_code = ReturnCode::INCLUDE;
  if(filter_out_row_) {
    ret_code = ReturnCode::NEXT_ROW;
  }
  return common::OB_SUCCESS;
}

void RandomRowFilter::reset()
{
  filter_out_row_ = false;
}

bool RandomRowFilter::filter_row()
{
  return filter_out_row_;
}

// statement is "RandomRowFilter"
int64_t RandomRowFilter::get_format_filter_string_length() const
{
  return strlen("RandomRowFilter");
}

int RandomRowFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "RandomRowFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ColumnPaginationFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (!column_offset_.empty()) {
    if (count_ >= limit_) {
      ret_code = ReturnCode::NEXT_ROW;
    } else {
      int cmp = 0;
      if (count_ == 0) {
        cmp = cell.get_qualifier().compare(column_offset_);
      }
      if (cmp < 0) {
        ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
      } else {
        count_ ++;
        ret_code = ReturnCode::INCLUDE_AND_NEXT_COL;
      }
    }
  } else {
    if (count_ >= offset_ + limit_) {
      ret_code = ReturnCode::NEXT_ROW;
    } else{
      ret_code = count_ < offset_ ? ReturnCode::NEXT_COL : ReturnCode::INCLUDE_AND_NEXT_COL;
      count_++;
    }
  }
  return ret;
}

void ColumnPaginationFilter::reset()
{
  count_ = 0;
  return;
}

// statement is "ColumnPaginationFilter"
int64_t ColumnPaginationFilter::get_format_filter_string_length() const
{
  return strlen("ColumnPaginationFilter");
}

int ColumnPaginationFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "ColumnPaginationFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

int ColumnPaginationFilter::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ret = ObHTableUtils::create_first_cell_on_row_col(allocator, cell, column_offset_, new_cell);
  return ret;
}

////////////////////////////////////////////////////////////////
int ColumnPrefixFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  ObString qualifier = cell.get_qualifier();
  if (qualifier.length() < prefix_.length()) {
    int32_t cmp = qualifier.compare(prefix_);
    if (cmp <= 0) {
      ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
    } else {
      ret_code = ReturnCode::NEXT_ROW;
    }
  } else {
    bool matched = qualifier.prefix_match(prefix_);
    if (matched) {
      ret_code = ReturnCode::INCLUDE;
    } else {
      int cmp = qualifier.compare(prefix_);
      if (cmp < 0) {
        ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
      } else {
        ret_code = ReturnCode::NEXT_ROW;
      }
    }
  }
  return ret;
}

int ColumnPrefixFilter::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::create_first_cell_on_row_col(allocator, cell, prefix_, new_cell))) {
    LOG_WARN("failed to create hint cell of filter", K(ret));
  }
  return ret;
}

// statement is "ColumnPrefixFilter"
int64_t ColumnPrefixFilter::get_format_filter_string_length() const
{
  return strlen("ColumnPrefixFilter");
}

int ColumnPrefixFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "ColumnPrefixFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int FirstKeyOnlyFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (found_) {
    ret_code = ReturnCode::NEXT_ROW;
  } else {
    found_ = true;
    ret_code = ReturnCode::INCLUDE;
  }
  return ret;
}

void FirstKeyOnlyFilter::reset()
{
  found_ = false;
}

// statement is "FirstKeyOnlyFilter"
int64_t FirstKeyOnlyFilter::get_format_filter_string_length() const
{
  return strlen("FirstKeyOnlyFilter");
}

int FirstKeyOnlyFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "FirstKeyOnlyFilter");
    if (n < 0) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////
int KeyOnlyFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  ret_code = ReturnCode::INCLUDE;
  return OB_SUCCESS;
}

int KeyOnlyFilter::transform_cell(ObIAllocator &allocator, ObHTableCellEntity &cell)
{
  int ret = OB_SUCCESS;
  ObNewRow* row = cell.get_ob_row();
  if (OB_ISNULL(row)) {
    ret = OB_NULL_CHECK_ERROR;
    LOG_WARN("ob row is null", K(ret));
  } else {
    if (len_as_val_) {
      int32_t len = cell.get_value().length();
      char* val = nullptr;
      if (OB_FAIL(ObHTableUtils::generate_hbase_bytes(allocator, len, val))) {
        LOG_WARN("fail to generate hbase bytes", K(ret));
      } else {
        cell.get_ob_row()->get_cell(ObHTableConstants::COL_IDX_V).set_varchar(val, sizeof(len));
      }
    } else {
      ObString s = ObString();
      cell.get_ob_row()->get_cell(ObHTableConstants::COL_IDX_V).set_varchar(s);
    }
  }
  return ret;
}

// statement is "KeyOnlyFilter"
int64_t KeyOnlyFilter::get_format_filter_string_length() const
{
  return strlen("KeyOnlyFilter");
}

int KeyOnlyFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "KeyOnlyFilter");
    if (n < 0) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////
int RowTracker::update_with(const ObString &cur_row, const ObPair<ObString, ObString> &fuzzy_data)
{
  int ret = OB_SUCCESS;
  ObString result;
  int padding = is_reversed_ ? -1 : 0;
  if (OB_FAIL(ob_write_string(allocator_, fuzzy_data.left_, result, OB_MAX(fuzzy_data.left_.length(), cur_row.length()), padding))) {
    LOG_WARN("failed to clone fuzzy key bytes", K(ret), K_(fuzzy_data.left));
  } else {
    bool increased = false;
    int to_inc = -1;
    ObString fuzzy_key = fuzzy_data.left_;
    ObString fuzzy_meta = fuzzy_data.right_;
    bool loop = true;
    for (int i = 0; i < result.length() && !increased && loop; i++) {
      if (i >= fuzzy_meta.length() || fuzzy_meta[i] == 0) {
        result.ptr()[i] = cur_row[i];
        if (!order_->isMax(cur_row[i])) {
          to_inc = i;
        }
      } else {
        if (order_->lt(cur_row[i], fuzzy_key[i])) {
          increased = true;
        } else if (order_->gt(cur_row[i], fuzzy_key[i])) {
          loop = false;
        }
      }
    } // end for

    if (!increased) {
      if (to_inc >= 0) {
        order_->inc(result.ptr()[to_inc]);
        for (int i = to_inc + 1; i < result.length(); i++) {
          if (i >= fuzzy_meta.length() || fuzzy_meta[i] == 0) {
            result.ptr()[i] = order_->min();
          }
        } // end for
      }
    }
    if (increased || to_inc >=0) {
      if (is_reversed_) {
        if (!result.empty()) {
          NextRowType *ptr;
          if (nullptr == (ptr = OB_NEWx(NextRowType, &allocator_, result, fuzzy_data))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("no memory", K(ret));
          } else if (OB_FAIL(next_rows_.push(ptr))) {
            LOG_WARN("failed to push to ob binary heap", K(ret));
            ptr->~NextRowType();
          }
        }
      } else {
        ObString trim_result;
        int offset = OB_MAX(fuzzy_meta.length() - 1, result.length() - 1);
        while (offset > 0 && fuzzy_meta[offset] == 0) {
          offset--;
        }
        offset = OB_MAX(offset, to_inc);
        if (OB_FAIL(ob_write_string(allocator_, result, trim_result, offset + 1))) {
          LOG_WARN("failed to clone result of row tracker", K(ret), K(result));
        } else if (!trim_result.empty()) {
          NextRowType *ptr;
          if (nullptr == (ptr = OB_NEWx(NextRowType, &allocator_, trim_result, fuzzy_data))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("no memory", K(ret));
          } else if (OB_FAIL(next_rows_.push(ptr))) {
            LOG_WARN("failed to push to ob binary heap", K(ret));
            ptr->~NextRowType();
          }
        }
      }
    }
  }
  return ret;
}

bool RowTracker::less_than(const ObString &cur_row, const ObString &next_row)
{
  int cmp = cur_row.compare(next_row);
  return is_reversed_ ? cmp > 0 : cmp < 0;
}

int RowTracker::init(const ObString &cur_row)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("row tracker has been inited", K(ret));
  } else if (nullptr == (order_ = is_reversed_ ? 
      static_cast<Order*>(OB_NEWx(DESC, &allocator_)) 
      : static_cast<Order*>(OB_NEWx(ASC, &allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  } else {
    int64_t fuzzy_key_count = fuzzy_key_data_.count();
    ARRAY_FOREACH_X(fuzzy_key_data_, idx, fuzzy_key_count, OB_SUCC(ret)) {
      ObPair<ObString, ObString> *it = fuzzy_key_data_.at(idx);
      if (nullptr == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fuzzy key data is null", K(ret));
      } else if (OB_FAIL(update_with(cur_row, *it))) {
        LOG_WARN("failed to update row tracker", K(ret), K(cur_row));
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

int RowTracker::update_tracker(const ObString &cur_row, bool &done)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("row tracker is not inited", K(ret));
  }
  while (OB_SUCC(ret) && !next_rows_.empty() && !less_than(cur_row, next_rows_.top()->left_)) {
    ObPair<ObString, ObString> &fuzzy_data = next_rows_.top()->right_;
    if (OB_FAIL(next_rows_.pop())) {
      LOG_WARN("failed to pop from binary heap", K(ret));
    } else if (OB_FAIL(update_with(cur_row, fuzzy_data))) {
      LOG_WARN("failed to update row tracker with row", K(ret));
    }
  }
  done |= next_rows_.empty();
  return ret;
}

int RowTracker::next_row(ObString &row)
{
  int ret = OB_SUCCESS;
  if (next_rows_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("next rows should not be empty", K(ret), K_(next_rows));
  } else {
    row = next_rows_.top()->left_;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int FuzzyRowFilter::init(const ObHTableCell &cell)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_NORET((fuzzy_key_), idx) {
    ObPair<ObString, ObString> *it = fuzzy_key_.at(idx);
    if (nullptr != it) {
      ObString meta = it->right_;
      for (int i = 0; i < meta.length(); i++) {
        if ('0' == meta[i]) {
          meta.ptr()[i] = -1;
        } else if ('1' == meta[i]) {
          meta.ptr()[i] = 0;
          it->left_.ptr()[i] = 0;
        }
      }
    }
  }
  if (nullptr == (row_tracker_ = OB_NEWx(RowTracker, &allocator_, allocator_, fuzzy_key_, is_reversed()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  } else if (OB_FAIL(row_tracker_->init(cell.get_rowkey()))) {
    LOG_WARN("failed to init row tracker", K(ret));
    row_tracker_->~RowTracker();
  } else {
    is_inited_ = true;
  }
  return ret;
}

int FuzzyRowFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (included_) {
    ret_code = ReturnCode::INCLUDE;
  } else if (!is_inited_ && OB_FAIL(init(cell))) {
    LOG_WARN("failed to init FuzzyRowFilter");
  } else {
    filter_row_ = true;
    ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
    ARRAY_FOREACH_X((fuzzy_key_), idx, cnt, filter_row_) {
      ObPair<ObString, ObString> *it = fuzzy_key_.at(idx);
      if (nullptr != it) {
        ObString fuzzy_meta = it->right_;
        if (fuzzy_match(cell.get_rowkey(), (*it))) {
          included_ = true;
          filter_row_ = false;
          ret_code = ReturnCode::INCLUDE;
        }
      }
    }
  }
  return ret;
}

int FuzzyRowFilter::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_tracker_->update_tracker(cell.get_rowkey(), done_))) {
    LOG_WARN("failed to update row trackser", K(ret), K_(row_tracker));
  } else if (!done_) {
    ObString new_key;
    if (OB_FAIL(row_tracker_->next_row(new_key))) {
      LOG_WARN("failed to get next row key from row tracker", K(ret), K_(row_tracker));
    } else if (OB_FAIL(ObHTableUtils::create_first_cell_on_row(allocator, new_key, new_cell))) {
      LOG_WARN("failed to create first cell on row", K(ret));
    }
  }
  return ret;
}

void FuzzyRowFilter::reset()
{
  included_ = false;
  filter_row_ = false;
}

// statement is "FuzzyRowFilter"
int64_t FuzzyRowFilter::get_format_filter_string_length() const
{
  return strlen("FuzzyRowFilter");
}

int FuzzyRowFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "FuzzyRowFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

bool FuzzyRowFilter::fuzzy_match(const ObString &rowkey, const ObPair<ObString, ObString> &fuzzy_key)
{
  bool bret = true;
  int len = OB_MIN(rowkey.length(), fuzzy_key.left_.length());
  int int_num = len / sizeof(int64_t);
  int offset_bytes = int_num << 3;
  const char* row_key_ptr = rowkey.ptr();
  const char* fuzzy_key_ptr = fuzzy_key.left_.ptr();
  const char* fuzzy_meta_ptr = fuzzy_key.right_.ptr();

  // int64_t
  for (int i = 0; i < offset_bytes && bret; i += sizeof(int64_t)) {
    int64_t row_byte = *(reinterpret_cast<const int64_t*>(row_key_ptr + i));
    int64_t fuzzy_byte = *(reinterpret_cast<const int64_t*>(fuzzy_key_ptr + i));
    int64_t meta_byte = *(reinterpret_cast<const int64_t*>(fuzzy_meta_ptr + i));
    if ((row_byte & meta_byte) != fuzzy_byte) {
      bret = false;
    }
  }

  // int32_t
  if (bret && len - offset_bytes >= sizeof(int32_t)) {
    int32_t row_byte = *(reinterpret_cast<const int32_t*>(row_key_ptr + offset_bytes));
    int32_t fuzzy_byte = *(reinterpret_cast<const int32_t*>(fuzzy_key_ptr + offset_bytes));
    int32_t meta_byte = *(reinterpret_cast<const int32_t*>(fuzzy_meta_ptr + offset_bytes));
    if ((row_byte & meta_byte) != fuzzy_byte) {
      bret = false;
    } else {
      offset_bytes += sizeof(int32_t);
    }
  }

  // int16_t
  if (bret && len - offset_bytes >= sizeof(int16_t)) {
    int16_t row_byte = *(reinterpret_cast<const int16_t*>(row_key_ptr + offset_bytes));
    int16_t fuzzy_byte = *(reinterpret_cast<const int16_t*>(fuzzy_key_ptr + offset_bytes));
    int16_t meta_byte = *(reinterpret_cast<const int16_t*>(fuzzy_meta_ptr + offset_bytes));
    if ((row_byte & meta_byte) != fuzzy_byte) {
      bret = false;
    } else {
      offset_bytes += sizeof(int16_t);
    }
  }

  // int8_t
  if (bret && len - offset_bytes >= sizeof(int8_t)) {
    int8_t row_byte = *(reinterpret_cast<const int8_t*>(row_key_ptr + offset_bytes));
    int8_t fuzzy_byte = *(reinterpret_cast<const int8_t*>(fuzzy_key_ptr + offset_bytes));
    int8_t meta_byte = *(reinterpret_cast<const int8_t*>(fuzzy_meta_ptr + offset_bytes));
    if ((row_byte & meta_byte) != fuzzy_byte) {
      bret = false;
    }
  }
  return bret;
}

////////////////////////////////////////////////////////////////
int TimestampsFilter::init()
{
  int ret = OB_SUCCESS;
  bool is_first = true;
  if (OB_NOT_NULL(origin_ts_)) {
    FOREACH_X(ts, (*origin_ts_), OB_SUCC(ret)) {
      ObTimestampNode node = ObTimestampNode(-(*ts));
      if (OB_FAIL(nodes_.push_back(node))) {
        LOG_WARN("failed to push back", K(ret), K(node));
      } else {
        ObTimestampNode* node_ptr = &*(nodes_.end() - 1);
        if (OB_FAIL(timestamps_.insert(node_ptr))) {
          LOG_WARN("failed to insert to rb tree", K(ret), KP(node_ptr));
        }
      }
    } // end for each
    ObTimestampNode* last_node = timestamps_.get_last();
    if (OB_NOT_NULL(last_node)) {
      max_timestamp_ = last_node->get_value();
    }
  }
  return ret;
}

int TimestampsFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  ObTimestampNode key_node = ObTimestampNode(cell.get_timestamp());
  ObTimestampNode* found = nullptr;

  if (!is_inited_) {
    if (OB_FAIL(init())) {
      LOG_WARN("failed to init timestamps", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(timestamps_.search(&key_node, found))) {
      OBLOG_LOG(WARN, "search node from tree failed", KR(ret), K(key_node));
    } else {
      if (OB_NOT_NULL(found)) {
        ret_code = ReturnCode::INCLUDE;
      } else if (cell.get_timestamp() > max_timestamp_) {
        ret_code = ReturnCode::NEXT_COL;
      } else {
        ret_code = can_hint_? ReturnCode::SEEK_NEXT_USING_HINT : ReturnCode::SKIP;
      }
    }
  }
  return ret;
}

int TimestampsFilter::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  if (can_hint_) {
    ObTimestampNode key_node = ObTimestampNode(cell.get_timestamp());
    ObTimestampNode* found = nullptr;
    if (OB_FAIL(timestamps_.nsearch(&key_node, found))) {
      OBLOG_LOG(WARN, "search node from tree failed", KR(ret), K(key_node));
    } else {
      if (found == NULL) {
        if (OB_FAIL(ObHTableUtils::create_last_cell_on_row_col(allocator, cell, new_cell))) {
          LOG_WARN("failed to create cell", K(ret), K(cell));
        }
      } else if (OB_FAIL(ObHTableUtils::create_first_cell_on_row_col_ts(allocator, cell, found->get_value(), new_cell))) {
        LOG_WARN("failed to create cell", K(ret), K(cell));
      }
    }
  } else {
    new_cell = nullptr;
  }
  return ret;
}

// statement is "TimestampsFilter"
int64_t TimestampsFilter::get_format_filter_string_length() const
{
  return strlen("TimestampsFilter");
}

int TimestampsFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "TimestampsFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ColumnValueFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (cell.get_qualifier() != qualifier_ || cell.get_family() != cf_) {
    ret_code = column_found_? ReturnCode::NEXT_ROW : ReturnCode::NEXT_COL;
  } else {
    column_found_ = true;
    int cmp_ret = comparator_->compare_to(cell.get_value());
    bool filtered = CompareFilter::compare(cmp_op_, cmp_ret);
    ret_code = filtered? ReturnCode::SKIP : ReturnCode::INCLUDE;
  }
  return ret;
}

void ColumnValueFilter::reset()
{
  column_found_ = false;
}

// statement is "ColumnValueFilter"
int64_t ColumnValueFilter::get_format_filter_string_length() const
{
  return strlen("ColumnValueFilter");
}

int ColumnValueFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "ColumnValueFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int MultiRowRangeFilter::init()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(origin_range_)) {
    FOREACH_X(range, (*origin_range_), OB_SUCC(ret)) {
      ObKeyRangeNode node(*range);
      if (OB_FAIL(nodes_.push_back(node))) {
        LOG_WARN("failed to push back", K(ret), K(node));
      } else {
        ObKeyRangeNode* node_ptr = &*(nodes_.end() - 1);
        if (OB_FAIL(ranges_.insert(node_ptr))) {
          LOG_WARN("failed to insert to rb tree", K(ret), KP(node_ptr));
        }
      }
    } // end for each

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObHTableUtils::merge_key_range(ranges_))) {
        LOG_WARN("failed to merge key ranges", K(ret));
      } else if (!is_reversed()) {
        range_ = ranges_.get_first()->get_value();
      } else {
        range_ = ranges_.get_last()->get_value();
      }
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int MultiRowRangeFilter::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  if (filter_all_remaining()) {
    filtered = true;
  } else if (!inited_ && OB_FAIL(init())) {
    LOG_WARN("failed to init MultiRowRangeFilter", K(ret));
  } else if (OB_NOT_NULL(range_) && range_->contain(first_row_cell.get_rowkey())) {
    return_code_ = ReturnCode::INCLUDE;
  } else {
    KeyRange key_range(first_row_cell.get_rowkey(), true, ObString(), true);
    ObKeyRangeNode key_node(&key_range);
    ObKeyRangeNode* result = nullptr;
    ranges_.psearch(&key_node, result);

    if (result == NULL) {
      if (is_reversed()) {
        done_ = true;
        return_code_ = ReturnCode::NEXT_ROW;
      } else {
        return_code_ = ReturnCode::SEEK_NEXT_USING_HINT;
      }
    } else {
      range_ = result->get_value();
      if (range_->contain(first_row_cell.get_rowkey())) {
        return_code_ = ReturnCode::INCLUDE;
      } else {
        if (!is_reversed()) {
          ObKeyRangeNode* next = nullptr;
          ranges_.get_next(result, next);
          if (OB_NOT_NULL(next)) {
            range_ = next->get_value();
          } else {
            range_ = nullptr;
            done_ = true;
          }
        }
        return_code_ = ReturnCode::SEEK_NEXT_USING_HINT;
      }
    }
  }
  return ret;
}

int MultiRowRangeFilter::get_next_cell_hint(oceanbase::common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(range_)) {
    if (OB_FAIL(FilterBase::get_next_cell_hint(allocator, cell, new_cell))) {
      LOG_WARN("failed to get next cell hint", K(ret));
    }
  } else if (is_reversed()) {
    if (OB_FAIL(ObHTableUtils::create_first_cell_on_row(allocator, range_->max(), new_cell))) {
      LOG_WARN("failed to create first cell on row", K(ret));
    }
  } else if (OB_FAIL(ObHTableUtils::create_first_cell_on_row(allocator, range_->min(), new_cell))) {
    LOG_WARN("failed to create first cell on row", K(ret));
  }
  return ret;
}

// statement is "MultiRowRangeFilter"
int64_t MultiRowRangeFilter::get_format_filter_string_length() const
{
  return strlen("MultiRowRangeFilter");
}

int MultiRowRangeFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "MultiRowRangeFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// statement is "InclusiveStopFilter"
int64_t InclusiveStopFilter::get_format_filter_string_length() const
{
  return strlen("InclusiveStopFilter");
}

int InclusiveStopFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "InclusiveStopFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ColumnRangeFilter::init()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(origin_range_)) {
    FOREACH_X(range, (*origin_range_), OB_SUCC(ret)) {
      ObKeyRangeNode node(*range);
      if (OB_FAIL(nodes_.push_back(node))) {
        LOG_WARN("failed to push back", K(ret), K(node));
      } else {
        ObKeyRangeNode* node_ptr = &*(nodes_.end() - 1);
        if (OB_FAIL(ranges_.insert(node_ptr))) {
          LOG_WARN("failed to insert to rb tree", K(ret), KP(node_ptr));
        }
      }
    } // end for each

    if (OB_SUCC(ret) && OB_FAIL(ObHTableUtils::merge_key_range(ranges_))) {
      LOG_WARN("failed to merge key ranges", K(ret));
    } else if (!ranges_.is_empty()) {
      range_ = ranges_.get_first()->get_value();
    }
  }
  inited_ = true;
  return ret;
}

int ColumnRangeFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (!inited_ && OB_FAIL(init())) {
    LOG_WARN("failed to init ColumnRangeFilter", K(ret));
  } else if (OB_NOT_NULL(range_) && range_->contain(cell.get_qualifier())) {
    ret_code = ReturnCode::INCLUDE;
  } else if (ranges_.is_empty()) {
    ret_code = ReturnCode::NEXT_ROW;
  } else {
    KeyRange key_range(cell.get_qualifier(), true, ObString(), true);
    ObKeyRangeNode key_node(&key_range);
    ObKeyRangeNode* result = nullptr;
    ranges_.psearch(&key_node, result);

    if (result == NULL) {
      ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
    } else {
      range_ = result->get_value();
      if (range_->contain(cell.get_qualifier())) {
        ret_code = ReturnCode::INCLUDE;
      } else {
        ObKeyRangeNode* next = nullptr;
        ranges_.get_next(result, next);
        if (OB_NOT_NULL(next)) {
          range_ = next->get_value();
          ret_code = ReturnCode::SEEK_NEXT_USING_HINT;
        } else {
          range_ = nullptr;
          ret_code = ReturnCode::NEXT_ROW;
        }
      }
    }
  }
  return ret;
}

void ColumnRangeFilter::reset()
{
  ObKeyRangeNode* first = ranges_.get_first();
  if (OB_NOT_NULL(first)) {
    range_ = first->get_value();
  }
}

int ColumnRangeFilter::get_next_cell_hint(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(range_)) {
    if (OB_FAIL(FilterBase::get_next_cell_hint(allocator, cell, new_cell))) {
      LOG_WARN("failed to get next cell hint", K(ret));
    }
  } else if (OB_FAIL(ObHTableUtils::create_first_cell_on_row_col(allocator, cell, range_->min(), new_cell))) {
    LOG_WARN("failed to create first cell on row", K(ret));
  }
  return ret;
}

// statement is "ColumnRangeFilter"
int64_t ColumnRangeFilter::get_format_filter_string_length() const
{
  return strlen("ColumnRangeFilter");
}

int ColumnRangeFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "ColumnRangeFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// statement is "MultipleColumnPrefixFilter"
int64_t MultipleColumnPrefixFilter::get_format_filter_string_length() const
{
  return strlen("MultipleColumnPrefixFilter");
}

int MultipleColumnPrefixFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "MultipleColumnPrefixFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int FamilyFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (!cell.get_family().empty() && compare_family(cmp_op_, *comparator_, cell)) {
    ret_code = ReturnCode::NEXT_ROW;
  } else {
    ret_code = ReturnCode::INCLUDE;
  }
  return ret;
}

// statement is "FamilyFilter"
int64_t FamilyFilter::get_format_filter_string_length() const
{
  return strlen("FamilyFilter");
}

int FamilyFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "FamilyFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
void ColumnCountGetFilter::reset()
{
  count_ = 0;
}

int ColumnCountGetFilter::filter_row_key(const ObHTableCell &first_row_cell, bool &filtered)
{
  UNUSED(first_row_cell);
  filtered = filter_all_remaining();
  return OB_SUCCESS;
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

// statement is "ColumnCountGetFilter"
int64_t ColumnCountGetFilter::get_format_filter_string_length() const
{
  return strlen("ColumnCountGetFilter");
}

int ColumnCountGetFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "ColumnCountGetFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int FirstKeyValueMatchingQualifiersFilter::filter_cell(const ObHTableCell &cell, ReturnCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (found_) {
    ret_code = ReturnCode::NEXT_ROW;
  } else {
    if (OB_NOT_NULL(qualifiers_)) {
      FOREACH_X(it, (*qualifiers_), OB_SUCC(ret) && !found_) {
        if (it->length() == cell.get_qualifier().length() && it->compare(cell.get_qualifier()) == 0) {
          found_ = true;
        }
      }
    }
    ret_code = ReturnCode::INCLUDE;
  }
  return ret;
}

void FirstKeyValueMatchingQualifiersFilter::reset()
{
  found_ = false;
}

// statement is "FirstKeyValueMatchingQualifiersFilter"
int64_t FirstKeyValueMatchingQualifiersFilter::get_format_filter_string_length() const
{
  return strlen("FirstKeyValueMatchingQualifiersFilter");
}

int FirstKeyValueMatchingQualifiersFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHTableUtils::get_format_filter_string(buf, buf_len, pos, "FirstKeyValueMatchingQualifiersFilter"))) {
    LOG_WARN("failed to format filter string", K(ret));
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
  int cmp_ret = -comparator_->compare_to(cell.get_value());
  return CompareFilter::compare(cmp_op_, cmp_ret);
}

bool CheckAndMutateFilter::filter_row()
{
  LOG_DEBUG("[yzfdebug] filter row", K_(found_column), K_(matched_column), K_(value_is_null));
  bool bret = true;
  bret = found_column_ ? (!matched_column_) : true;
  return bret;
}

// statement is "CheckAndMutateFilter $family $qualifier $compare_op_name"
int64_t CheckAndMutateFilter::get_format_filter_string_length() const
{
  int64_t len = 0;

  len += strlen("CheckAndMutateFilter"); // "WhileMatchFilter"
  len += 1; // blank
  len += family_.length(); // "$family"
  len += 1; // blank
  len += qualifier_.length(); // "$qualifier"
  len += 1; // blank
  len += strlen(FilterUtils::get_compare_op_name(cmp_op_)); // "$compare_op_name"

  return len;
}

int CheckAndMutateFilter::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "CheckAndMutateFilter ");
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      strncat(buf + pos, family_.ptr(), family_.length());
      pos += family_.length();
      int64_t n = snprintf(buf + pos, buf_len - pos, " ");
      if (n < 0 || n > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
      } else {
        pos += n;
        strncat(buf + pos, qualifier_.ptr(), qualifier_.length());
        pos += qualifier_.length();
        int64_t n = snprintf(buf + pos, buf_len - pos, " %s", FilterUtils::get_compare_op_name(cmp_op_));
        if (n < 0 || n > buf_len - pos) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
        } else {
          pos += n;
        }
      }
    }
  }

  return ret;
}
