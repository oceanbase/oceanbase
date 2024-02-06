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

// This file contains implementation support for the json base abstraction.

#define USING_LOG_PREFIX SQL
#include "ob_json_base.h"
#include "ob_json_tree.h"
#include "ob_json_bin.h"
#include "ob_json_parse.h"
#include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
#include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
#include "lib/charset/ob_dtoa.h" // ob_gcvt_opt
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "lib/charset/ob_charset.h" // for strntod
#include "common/ob_smart_var.h" // for SMART_VAR

namespace oceanbase {
namespace common {

struct ObJsonBaseCmp {
  bool operator()(const ObIJsonBase *a, const ObIJsonBase *b) {
    bool is_eq = false;
    if (a->is_tree() && b->is_tree()) {
      is_eq = (a == b);
    } else if (a->is_bin() && b->is_bin()) {
      is_eq = (a->get_data() == b->get_data());
    } else {
      LOG_WARN("unexpected type", K(OB_ERR_UNEXPECTED), K(*a), K(*b));
    }
    return is_eq;
  }
};

struct ObJsonBaseUnique {
  bool operator()(const ObIJsonBase *a, const ObIJsonBase *b) {
    bool is_eq = false;
    if (a->is_tree() && b->is_tree()) {
      is_eq = (a == b);
    } else if (a->is_bin() && b->is_bin()) {
      is_eq = (a->get_data() == b->get_data());
    }
    return is_eq;
  }
};

// only use in seek, this can not from stack memory
int ObIJsonBase::add_if_missing(ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);
  ObJsonBaseCmp cmp;
  ObJsonBaseUnique unique;
  ObJsonBaseSortedVector::iterator pos = dup.end();

  if ((OB_SUCC(dup.insert_unique(this, pos, cmp, unique)))) {
    if (OB_FAIL(res.push_back(this))) {
      LOG_WARN("fail to push_back value into result", K(ret), K(res.size()));
    }
  } else if (ret == OB_CONFLICT_VALUE) {
    ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
  }

  return ret;
}

int ObIJsonBase::find_ellipsis(const JsonPathIterator &cur_node,
                               const JsonPathIterator &last_node,
                               const JsonPathIterator &next_node, bool is_auto_wrap,
                               bool only_need_one, ObJsonBaseSortedVector &dup,
                               ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);
  bool is_done = false;

  if (OB_FAIL(find_child(next_node, last_node, is_auto_wrap, only_need_one, dup, res))) {
    LOG_WARN("fail to seek recursively", K(ret), K(is_auto_wrap), K(only_need_one));
  } else if (json_type() == ObJsonNodeType::J_ARRAY) {
    uint64_t size = element_count();
    ObIJsonBase *jb_ptr = NULL;
    for (uint32_t i = 0; i < size && !is_done; ++i) {
      jb_ptr = NULL; // reset jb_ptr to NULL
      ret = get_array_element(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get array child dom",K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(cur_node, last_node, is_auto_wrap,
          only_need_one, dup, res))) {
        LOG_WARN("fail to seek recursively",K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  } else if(json_type() == ObJsonNodeType::J_OBJECT){
    uint64_t count = element_count();
    ObIJsonBase *jb_ptr = NULL;
    for (uint32_t i = 0; i < count && !is_done; ++i) {
      jb_ptr = NULL; // reset jb_ptr to NULL
      ret = get_object_value(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get object child dom",K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(cur_node, last_node, is_auto_wrap, 
          only_need_one, dup, res))) {
        LOG_WARN("fail to seek recursively",K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  }

  return ret;
}

int ObIJsonBase::find_array_range(const JsonPathIterator &next_node,
                                  const JsonPathIterator &last_node,
                                  const ObJsonPathBasicNode* path_node, bool is_auto_wrap,
                                  bool only_need_one, ObJsonBaseSortedVector &dup,
                                  ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);

  SMART_VAR (ObArrayRange, range) {
    if (OB_FAIL(path_node->get_array_range(element_count(), range))) {
      LOG_WARN("fail to get array range", K(ret), K(element_count()));
    } else {
      bool is_done = false;
      ObIJsonBase *jb_ptr = NULL;
      for (uint32_t i = range.array_begin_; OB_SUCC(ret) && i < range.array_end_ && !is_done; ++i) {
        jb_ptr = NULL; // reset jb_ptr to NULL
        ret = get_array_element(i, jb_ptr);
        if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("fail to get array child dom", K(ret), K(i));
        } else if (OB_FAIL(jb_ptr->find_child(next_node, last_node, is_auto_wrap, 
            only_need_one, dup, res))) {
          LOG_WARN("fail to seek recursively", K(ret), K(i), K(is_auto_wrap), K(only_need_one));
        } else {
          is_done = is_seek_done(res, only_need_one);
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::find_array_cell(const JsonPathIterator &next_node,
                                 const JsonPathIterator &last_node,
                                 const ObJsonPathBasicNode* path_node, bool is_auto_wrap,
                                 bool only_need_one, ObJsonBaseSortedVector &dup,
                                 ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);

  SMART_VAR (ObJsonArrayIndex, idx) {
    if (OB_FAIL(path_node->get_first_array_index(element_count(), idx))) {
      LOG_WARN("failed to get array index.", K(ret), K(element_count()));
    } else {
      if (idx.is_within_bounds()) {
        ObIJsonBase *jb_ptr = NULL;
        ret = get_array_element(idx.get_array_index(), jb_ptr);
        if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("fail to get array child dom", K(ret), K(idx.get_array_index()));
        } else if (OB_FAIL(jb_ptr->find_child(next_node, last_node, is_auto_wrap,
            only_need_one, dup, res))) {
          LOG_WARN("fail to seek recursively", K(ret), K(is_auto_wrap), K(only_need_one));
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::find_member_wildcard(const JsonPathIterator &next_node, 
                                      const JsonPathIterator &last_node, bool is_auto_wrap,
                                      bool only_need_one, ObJsonBaseSortedVector &dup,
                                      ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);
  bool is_done = false;
  ObIJsonBase *jb_ptr = NULL;
  uint64_t count = element_count();

  for (uint64_t i = 0; i < count && OB_SUCC(ret) && !is_done; ++i) {
    jb_ptr = NULL; // reset jb_ptr to NULL
    ret = get_object_value(i, jb_ptr);
    if (OB_ISNULL(jb_ptr)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get object child dom",K(ret), K(i));
    } else if (OB_FAIL(jb_ptr->find_child(next_node, last_node, is_auto_wrap, only_need_one, dup, res))) {
      LOG_WARN("fail to seek recursively",K(ret), K(i));
    } else {
      is_done = is_seek_done(res, only_need_one);
    }
  }

  return ret;
}                                     

int ObIJsonBase::find_member(const JsonPathIterator &next_node, const JsonPathIterator &last_node,
                             const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                             bool only_need_one, ObJsonBaseSortedVector &dup,
                             ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);
  ObString key_name(path_node->get_object().len_, path_node->get_object().object_name_);
  ObIJsonBase *jb_ptr = NULL;

  ret = get_object_value(key_name, jb_ptr);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(jb_ptr->find_child(next_node, last_node, is_auto_wrap, only_need_one, dup, res))) {
      LOG_WARN("fail to seek recursively", K(ret), K(is_auto_wrap), K(only_need_one));
    }
  } else if (ret == OB_SEARCH_NOT_FOUND) {
    // not found, it is normal.
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail to get object value", K(ret), K(key_name));
  }

  return ret;
}

int ObIJsonBase::find_child(const JsonPathIterator &cur_node, const JsonPathIterator &last_node, 
                            bool is_auto_wrap, bool only_need_one, ObJsonBaseSortedVector &dup,
                            ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);

  // If the path expression is already at the end, the current DOM is the res, 
  // and it is added to the res
  if (cur_node == last_node) {
    if (OB_FAIL(add_if_missing(dup, res))) {
      LOG_WARN("fail to add node.", K(ret));
    }
  } else {
    ObJsonPathBasicNode *path_node = static_cast<ObJsonPathBasicNode *>(*cur_node);
    SMART_VAR (JsonPathIterator, next_node) {
      next_node = cur_node + 1;
      switch (path_node->get_node_type()) {
        case JPN_MEMBER: {
          if (json_type() == ObJsonNodeType::J_OBJECT) {
            if (OB_FAIL(find_member(next_node, last_node, path_node, is_auto_wrap,
                only_need_one, dup, res))) {
              LOG_WARN("fail to find member.", K(ret), K(is_auto_wrap), K(only_need_one)); 
            }
          }
          break;
        }

        case JPN_MEMBER_WILDCARD: {
          if (json_type() == ObJsonNodeType::J_OBJECT) {
            if (OB_FAIL(find_member_wildcard(next_node, last_node, is_auto_wrap,
                only_need_one, dup, res))) {
              LOG_WARN("fail to find member wildcard.", K(ret), K(is_auto_wrap), K(only_need_one));
            }
          } 
          break;
        }

        case JPN_ARRAY_CELL: {
          if (json_type() == ObJsonNodeType::J_ARRAY) {
            if (OB_FAIL(find_array_cell(next_node, last_node, path_node, is_auto_wrap,
                only_need_one, dup, res))) {
              LOG_WARN("fail to find array cell.", K(ret), K(is_auto_wrap), K(only_need_one));
            }
          } else {
            if (is_auto_wrap && path_node->is_autowrap()) {
              if(OB_FAIL(find_child(next_node, last_node, is_auto_wrap, only_need_one,dup, res))) {
                LOG_WARN("fail to seek autowrap of array cell", K(ret), K(only_need_one));
              }
            }
          }
          break;
        }

        case JPN_ARRAY_CELL_WILDCARD:
        case JPN_ARRAY_RANGE: {
          if (json_type() == ObJsonNodeType::J_ARRAY) {
            if(OB_FAIL(find_array_range(next_node, last_node, path_node, is_auto_wrap,
                only_need_one, dup, res))) {
              LOG_WARN("fail in find array range.", K(ret)); 
            }
          } else {
            if (is_auto_wrap && path_node->is_autowrap()) {
              if (OB_FAIL(find_child(next_node, last_node, is_auto_wrap, 
                  only_need_one, dup, res))) {
                LOG_WARN("fail to seek autowrap of array range", K(ret), K(only_need_one));
              }
            }
          }
          break;
        }

        case JPN_ELLIPSIS: {
          if(OB_FAIL(find_ellipsis(cur_node, last_node,next_node, is_auto_wrap,
              only_need_one, dup, res))) {
            LOG_WARN("fail to find ellipsis.", K(ret), K(is_auto_wrap), K(only_need_one));
          }
          break;
        }

        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected json path type.", K(ret), K(path_node->get_node_type()));
          break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::seek(const ObJsonPath &path, uint32_t node_cnt, bool is_auto_wrap,
                      bool only_need_one, ObJsonBaseVector &res) const
{
  INIT_SUCC(ret);
  JsonPathIterator cur_node = path.begin();
  JsonPathIterator last_node = path.begin() + node_cnt;
  ObJsonBaseSortedVector dup;

  if (OB_FAIL(find_child(cur_node, last_node, is_auto_wrap, only_need_one, dup, res))) {
    LOG_WARN("fail to seek", K(ret), K(node_cnt), K(only_need_one), K(res.size()));
  }

  return ret;
}

int ObIJsonBase::print_jtime(ObJsonBuffer &j_buf, bool is_quoted) const
{
  INIT_SUCC(ret);

  if (is_quoted) {
    if (OB_FAIL(j_buf.append("\""))) {
      LOG_WARN("fail to append \"", K(ret), K(is_quoted));
    }
  }

  // ObTimeConverter::ob_time_to_str will lose precision.
  if (OB_SUCC(ret)) {
    ObDTMode tmode;
    ObJsonNodeType j_type = json_type();
    if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type, tmode))) {
      LOG_WARN("fail to get ObDTMode by json type", K(ret), K(j_type));
    } else {
      ObTime t(tmode);
      const int64_t tmp_buf_len = DATETIME_MAX_LENGTH + 1;
      char tmp_buf[tmp_buf_len] = {0};
      int64_t pos = 0;
      const int16_t print_scale = 6;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(t, tmode, print_scale, 
                                                         tmp_buf, tmp_buf_len, pos, true))) {
        LOG_WARN("fail to change time to string", K(ret), K(j_type), K(t), K(pos));
      } else if (OB_FAIL(j_buf.append(tmp_buf))) {
        LOG_WARN("fail to append date_buf to j_buf", K(ret), K(j_type), K(tmp_buf));
      } else if (is_quoted) {
        if (OB_FAIL(j_buf.append("\""))) {
          LOG_WARN("fail to append \"", K(ret), K(is_quoted));
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::print_array(ObJsonBuffer &j_buf, uint64_t depth, bool is_pretty) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append("["))) {
    LOG_WARN("fail to append [", K(ret), K(depth), K(is_pretty));
  } else {
    uint64_t arr_size = element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < arr_size; ++i) {
      if (i > 0 && OB_FAIL(ObJsonBaseUtil::append_comma(j_buf, is_pretty))) {
        LOG_WARN("fail to append comma", K(ret), K(is_pretty));
      } else if (is_pretty && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth))) {
        LOG_WARN("fail to append newline and indent", K(ret), K(depth));
      } else {
        ObIJsonBase *jb_ptr = NULL;
        ObJsonBin j_bin(allocator_);
        if (is_bin()) {
          jb_ptr = &j_bin;
        }
        if (OB_FAIL(get_array_element(i, jb_ptr))) {
          LOG_WARN("fail to get array element", K(ret), K(depth), K(i));
        } else if (OB_FAIL(jb_ptr->print(j_buf, true, is_pretty, depth))) {
          LOG_WARN("fail to print json value to string", K(ret), K(i), K(is_pretty), K(depth));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pretty && arr_size > 0
          && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth - 1))) {
        LOG_WARN("fail to append newline and indent", K(ret), K(depth), K(arr_size));
      } else if (OB_FAIL(j_buf.append("]"))) {
        LOG_WARN("fail to append [", K(ret), K(depth), K(arr_size));
      }
    }
  }

  return ret;
}

int ObIJsonBase::print_object(ObJsonBuffer &j_buf, uint64_t depth, bool is_pretty) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append("{"))) {
    LOG_WARN("fail to append '{' to buffer", K(ret));
  } else {
    bool is_first = true;
    uint64_t count = element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (!is_first && OB_FAIL(ObJsonBaseUtil::append_comma(j_buf, is_pretty))) {
        LOG_WARN("fail to append comma", K(ret), K(is_pretty));
      } else {
        is_first = false;
        ObString key;
        if (OB_FAIL(get_key(i, key))) {
          LOG_WARN("fail to get key", K(ret), K(i));
        } else if (is_pretty && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth))) {
          LOG_WARN("fail to newline and indent", K(ret), K(depth), K(i), K(key));
        } else if (OB_FAIL(ObJsonBaseUtil::append_string(j_buf, true, key.ptr(), key.length()))) { // key
          LOG_WARN("fail to print string", K(ret), K(depth), K(i), K(key));
        } else if (OB_FAIL(j_buf.append(":"))) {
          LOG_WARN("fail to append \":\"", K(ret), K(depth), K(i), K(key));
        } else if (OB_FAIL(j_buf.append(" "))) {
          LOG_WARN("fail to append \" \"", K(ret), K(depth), K(i), K(key));
        } else {
          ObIJsonBase *jb_ptr = NULL;
          ObJsonBin j_bin(allocator_);
          if (is_bin()) {
            jb_ptr = &j_bin;
          }
          if (OB_FAIL(get_object_value(i, jb_ptr))) {
            LOG_WARN("fail to get object value", K(ret), K(i), K(is_pretty), K(depth));
          } else if (OB_FAIL(jb_ptr->print(j_buf, true, is_pretty, depth))) { // value
            LOG_WARN("fail to print json value to string", K(ret), K(i), K(is_pretty), K(depth));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pretty && count > 0
          && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth -1))) {
        LOG_WARN("fail to newline and indent", K(ret), K(is_pretty), K(depth));
      } else if (OB_FAIL(j_buf.append("}"))) {
        LOG_WARN("fail to append \"}\" to buffer", K(ret), K(is_pretty), K(depth));
      }
    }
  }

  return ret;
}

int ObIJsonBase::print_decimal(ObJsonBuffer &j_buf) const
{
  INIT_SUCC(ret);
  int64_t pos = j_buf.length();

  if (OB_FAIL(j_buf.reserve(number::ObNumber::MAX_PRINTABLE_SIZE))) {
    LOG_WARN("fail to reserve memory for buf", K(ret));     
  } else {
    ObScale scale = get_decimal_scale();
    number::ObNumber val = get_decimal_data();
    if (OB_FAIL(val.format_v2(j_buf.ptr(), j_buf.capacity(), pos, scale))) {
      LOG_WARN("fail to format decimal value", K(ret), K(val), K(j_buf.length()), K(pos));    
    } else if (OB_FAIL(j_buf.set_length(pos))){
      LOG_WARN("fail to set buf length", K(ret), K(pos), K(j_buf.length()));    
    }
  }

  return ret;
}

struct ObFindDoubleEscapeFunc {
  ObFindDoubleEscapeFunc() {}

  bool operator()(const char c)
  {
    return c == '.' || c == 'e';
  }
};

int ObIJsonBase::print_double(ObJsonBuffer &j_buf) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.reserve(DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE + 1))) {
    LOG_WARN("fail to reserve memory for j_buf", K(ret));
  } else {
    double val = get_double();
    char *start = j_buf.ptr() + j_buf.length();
    uint64_t len = ob_gcvt(val, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
        DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE, start, NULL);
    if (OB_FAIL(j_buf.set_length(j_buf.length() + len))) {
      LOG_WARN("fail to set j_buf len", K(ret), K(j_buf.length()), K(len));
    } else {
      // add ".0" in the end.
      ObFindDoubleEscapeFunc func;
      if (std::none_of(start, start + len, func)) {
        if (OB_FAIL(j_buf.append("."))) {
          LOG_WARN("fail to append '.' to buffer", K(ret), K(start), K(len));
        } else if (OB_FAIL(j_buf.append("0"))) {
          LOG_WARN("fail to append '0' to buffer", K(ret), K(start), K(len));
        }
      }
    }
  }

  return ret;
}

// only used in json opaque type.
// Notice: varchar has different mapping in mysql 5.7 (var_string)and 8.0(varchar)
static const obmysql::EMySQLFieldType opaque_ob_type_to_mysql_type[ObMaxType] =
{
  /* ObMinType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NULL,          /* ObNullType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObUTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObUSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObUMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObUInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObUInt64Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObUFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObUDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObUNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATETIME,      /* ObDateTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIMESTAMP,     /* ObTimestampType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATE,          /* ObDateType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIME,          /* ObTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_YEAR,          /* ObYearType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING,    /* ObVarcharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VARCHAR,       /* ObHexStringType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,       /* ObExtendType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObUnknownType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB,     /* ObTinyTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,          /* ObTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB,   /* ObMediumTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB,     /* ObLongTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BIT,           /* ObBitType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObEnumType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObSetType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObEnumInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObSetInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE,       /* ObTimestampTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE, /* ObTimestampLTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO,                 /* ObTimestampNanoType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_RAW,                            /* ObRawType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM,                    /* ObIntervalYMType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS,                    /* ObIntervalDSType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT,                   /* ObNumberFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2,                      /* ObNVarchar2Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NCHAR,                          /* ObNCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_UROWID,
  obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB,                          /* ObLobType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_JSON,                              /* ObJsonType */
  /* ObMaxType */
};

int ObIJsonBase::print_opaque(ObJsonBuffer &j_buf, uint64_t depth, bool is_quoted) const
{
  INIT_SUCC(ret);
  const char *data = get_data();
  uint64_t data_length = get_data_length();
  ObObjType f_type = field_type();

  if (f_type >= ObMaxType) {
    ret = OB_ERR_MAX_VALUE;
    LOG_WARN("fail to convert ob type to mysql type", K(ret), K(f_type));
  } else {
    const uint64_t need_len = ObBase64Encoder::needed_encoded_length(data_length);
    obmysql::EMySQLFieldType mysql_type = obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED;
    mysql_type = opaque_ob_type_to_mysql_type[f_type];
    char field_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
    int64_t field_len = ObFastFormatInt::format_unsigned(mysql_type, field_buf);
    ObArenaAllocator tmp_allocator;
    ObJsonBuffer base64_buf(&tmp_allocator);

    // base64::typeXX:<binary data>
    if (OB_FAIL(base64_buf.append("base64:type"))) {
      LOG_WARN("fail to append \" base64:type \"", K(ret), K(depth));
    } else if (OB_FAIL(base64_buf.append(field_buf, field_len))) {
      LOG_WARN("fail to append field type", K(ret), K(depth), K(f_type));
    } else if (OB_FAIL(base64_buf.append(":"))) {
      LOG_WARN("fail to append \":\"", K(ret), K(depth));
    } else if (OB_FAIL(base64_buf.reserve(need_len))) {
      LOG_WARN("fail to reserve memory for base64_buf", K(ret), K(depth), K(need_len));
    } else {
      int64_t pos = 0;
      const uint8_t *input = reinterpret_cast<const uint8_t *>(data);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("json string is null", K(ret));
      } else if (OB_FAIL(ObBase64Encoder::encode(input, data_length,
                                                 base64_buf.ptr() + base64_buf.length(),
                                                 need_len, pos))) {
        LOG_WARN("fail to encode", K(ret), K(depth), K(f_type), K(need_len), K(pos));
      } else if (OB_FAIL(base64_buf.set_length(base64_buf.length() + need_len))) {
        LOG_WARN("fail to set buf length", K(ret), K(depth), K(f_type), K(need_len), K(base64_buf));
      } else {
        if (is_quoted) {
          if (OB_FAIL(ObJsonBaseUtil::add_double_quote(j_buf, base64_buf.ptr(),
                                                       base64_buf.length()))) {
            LOG_WARN("fail to add double quote", K(ret), K(depth), K(f_type), K(base64_buf));
          }
        } else if (OB_FAIL(j_buf.append(base64_buf.ptr(), base64_buf.length()))) {
          LOG_WARN("fail to append base64_buf", K(ret), K(depth), K(f_type), K(base64_buf));
        }
      }
    }
    base64_buf.reset(); // free memory
  }

  return ret;
}

int ObIJsonBase::print(ObJsonBuffer &j_buf, bool is_quoted, bool is_pretty, uint64_t depth) const
{
  INIT_SUCC(ret);
  ObJsonNodeType j_type = json_type();

  // consistent with mysql 5.7
  // in mysql 8.0, varstring is handled as json string, obvarchartype is considered as varstring.
  switch (j_type) {
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP: {
      if (OB_FAIL(print_jtime(j_buf, is_quoted))) {
        LOG_WARN("fail to change jtime to string", K(ret), K(is_quoted), K(is_pretty), K(depth));
      }
      break;
    }

    case ObJsonNodeType::J_ARRAY: {
      if (ObJsonParser::is_json_doc_over_depth(++depth)) {
        ret = OB_ERR_JSON_OUT_OF_DEPTH;
        LOG_WARN("current json over depth", K(ret), K(depth), K(j_type));
      } else if (OB_FAIL(print_array(j_buf, depth, is_pretty))) {
        LOG_WARN("fail to change jarray to string", K(ret), K(is_quoted), K(is_pretty), K(depth));
      }
      break;
    }

    case ObJsonNodeType::J_OBJECT: {
      if (ObJsonParser::is_json_doc_over_depth(++depth)) {
        ret = OB_ERR_JSON_OUT_OF_DEPTH;
        LOG_WARN("current json over depth", K(ret), K(depth), K(j_type));
      } else if (OB_FAIL(print_object(j_buf, depth, is_pretty))) {
        LOG_WARN("fail to print object to string", K(ret), K(depth), K(j_type), K(is_pretty));
      }
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      if (get_boolean() ? OB_FAIL(j_buf.append("true", sizeof("true") - 1)) :
                          OB_FAIL(j_buf.append("false", sizeof("false") - 1))) {
        LOG_WARN("fail to append boolean", K(ret), K(get_boolean()), K(j_type));              
      }
      break;
    }

    case ObJsonNodeType::J_DECIMAL: {
      if (OB_FAIL(print_decimal(j_buf))) {
        LOG_WARN("fail to print decimal to string", K(ret), K(depth), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE: {
      if (OB_FAIL(print_double(j_buf))) {
        LOG_WARN("fail to print double to string", K(ret), K(depth), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_NULL: {
      if (OB_FAIL(j_buf.append("null", sizeof("null") - 1))) {
        LOG_WARN("fail to append null string to buffer", K(ret), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_OPAQUE: {
      if (OB_FAIL(print_opaque(j_buf, depth, is_quoted))) {
        LOG_WARN("fail to print opaque to string", K(ret), K(depth), K(j_type), K(is_quoted));
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t data_len = get_data_length();
      const char *data = get_data();
      if (is_quoted && data_len == 0) {
        if (OB_FAIL(j_buf.append("\"\"", 2))) {
          LOG_WARN("fail to append empty string", K(ret), K(j_type), K(is_quoted));            
        }
      } else if (OB_ISNULL(data) && data_len != 0) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret), K(data_len));
      } else if (OB_FAIL(ObJsonBaseUtil::append_string(j_buf, is_quoted, data, data_len))) {
        // if data is null, data_len is 0, it is an empty string
        LOG_WARN("fail to append string", K(ret), K(j_type), K(is_quoted));
      }
      break;
    }

    case ObJsonNodeType::J_INT: {
      char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      int64_t len = ObFastFormatInt::format_signed(get_int(), tmp_buf);
      if (OB_FAIL(j_buf.append(tmp_buf, len))) {
        LOG_WARN("fail to append json int to buffer", K(ret), K(get_int()), K(len), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_UINT: {
      char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      int64_t len = ObFastFormatInt::format_unsigned(get_uint(), tmp_buf);
      if(OB_FAIL(j_buf.append(tmp_buf, len))) {
        LOG_WARN("fail to append json uint to buffer", K(ret), K(get_uint()), K(len), K(j_type));
      }
      break;
    }
    
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undefined json node type", K(ret), K(j_type));
      break;
    }
  }

  return ret;
}

int ObIJsonBase::calc_json_hash_value(uint64_t val, hash_algo hash_func, uint64_t &res) const
{
  INIT_SUCC(ret);
  ObJsonHashValue hash_value(val, hash_func);
  ObJsonNodeType j_type = json_type();

  switch (j_type) {
    case ObJsonNodeType::J_ARRAY: {
      hash_value.calc_character(ObJsonHashValue::JSON_ARRAY_FLAG);
      uint64_t arr_res = 0;
      uint64_t size = element_count();
      ObIJsonBase *jb_ptr = NULL;
      ObJsonBin j_bin(allocator_);
      if (is_bin()) {
        jb_ptr = &j_bin;
      }
      for (uint64_t i = 0; i < size && OB_SUCC(ret); i++) {
        if (OB_FAIL(get_array_element(i, jb_ptr))) {
          LOG_WARN("fail to get this json array element", K(ret), K(i), K(size));
        } else if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("null ptr", K(ret), K(i), K(size));
        } else if (OB_FAIL(jb_ptr->calc_json_hash_value(hash_value.get_hash_value(), hash_func, arr_res))) {
          LOG_WARN("fail to calc json hash value", K(ret), K(i), K(size), K(j_type));
        } else {
          hash_value.calc_uint64(arr_res);
        }
      }
      break;
    }

    case ObJsonNodeType::J_OBJECT: {
      hash_value.calc_character(ObJsonHashValue::JSON_OBJ_FLAG);
      uint64_t obj_res = 0;
      uint64_t count = element_count();
      ObString key;
      ObIJsonBase *jb_ptr = NULL;
      ObJsonBin j_bin(allocator_);
      if (is_bin()) {
        jb_ptr = &j_bin;
      }
      for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_FAIL(get_key(i, key))) {
          LOG_WARN("failed to get key", K(ret), K(i));
        } else {
          hash_value.calc_string(key);
          if (OB_FAIL(get_object_value(i, jb_ptr))) {
            LOG_WARN("failed to get sub obj.", K(ret), K(i), K(key));
          } else if (jb_ptr->calc_json_hash_value(hash_value.get_hash_value(), hash_func, obj_res)) {
            LOG_WARN("fail to calc json hash value", K(ret), K(i), K(count), K(j_type));
          } else {
            hash_value.calc_uint64(obj_res);
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OPAQUE: {
      ObString str(get_data_length(), get_data());
      hash_value.calc_string(str);
      break;
    }

    case ObJsonNodeType::J_INT: {
      hash_value.calc_int64(get_int());
      break;
    }

    case ObJsonNodeType::J_UINT: {
      hash_value.calc_uint64(get_uint());
      break;
    }

    case ObJsonNodeType::J_DOUBLE: {
      hash_value.calc_double(get_double());
      break;
    }

    case ObJsonNodeType::J_DECIMAL: {
      hash_value.calc_num(get_decimal_data());
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      bool value_bool = get_boolean();
      hash_value.calc_character(value_bool ?
                                ObJsonHashValue::JSON_BOOL_TRUE :
                                ObJsonHashValue::JSON_BOOL_FALSE);
      break;
    }

    case ObJsonNodeType::J_DATETIME: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_DATETIME, this))) {
        LOG_WARN("fail to calc json datetime hash", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_DATETIME, this))) {
        LOG_WARN("fail to calc json timestamp hash", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_TIME: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_TIME, this))) {
        LOG_WARN("fail to calc json time hash", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DATE: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_DATE, this))) {
        LOG_WARN("fail to calc json date hash", K(ret));
      }
      break;
    }

    case ObJsonNodeType::J_NULL: {
      hash_value.calc_character(ObJsonHashValue::JSON_NULL_FLAG);
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect json type", K(ret), K(j_type));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    res = hash_value.get_hash_value();
  }

  return ret;
}

// 1. If the arrays are equal in length and each array element is equal, then the two arrays are equal.
// 2. If you run into the first element of an unequal array, 
//    the smaller element will have a smaller array, and in this example, a is smaller.
//    a[0, 1, 2, 3] vs b[0, 1, 3, 1, 1, 1, 1] ----> a < b
// 3. If the array is not equal, and the smaller array is equal to the larger array element by element, 
//    then the smaller array is smaller, and example a is smaller.
//    a[0, 1, 2] vs b[0, 1, 2, 1, 1, 1, 1] ----> a < b
int ObIJsonBase::compare_array(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_ARRAY || j_type_b != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    const uint64_t size_a = element_count();
    const uint64_t size_b = other.element_count();
    const uint64_t min_size = std::min(size_a, size_b);
    for (uint64_t i = 0; (res == 0) && OB_SUCC(ret) && i < min_size; i++) {
      ObIJsonBase *jb_a_ptr = NULL;
      ObIJsonBase *jb_b_ptr = NULL;
      ObJsonBin j_bin_a(allocator_);
      ObJsonBin j_bin_b(allocator_);
      if (is_bin()) {
        jb_a_ptr = &j_bin_a;
        jb_b_ptr = &j_bin_b;
      }
      if (OB_FAIL(get_array_element(i, jb_a_ptr))) {
        LOG_WARN("fail to get this json array element", K(ret), K(i), K(size_a));
      } else if (OB_FAIL(other.get_array_element(i, jb_b_ptr))) {
        LOG_WARN("fail to get other json array element", K(ret), K(i), K(size_b));
      } else if (OB_FAIL(jb_a_ptr->compare(*jb_b_ptr, res))) {
        LOG_WARN("fail to compare this json to other json", K(ret), K(i), K(size_a), K(size_b));
      }
    }

    // Compare the array length if all the comparisons are equal.
    if (OB_SUCC(ret) && res == 0) {
      res = ObJsonBaseUtil::compare_numbers(size_a, size_b);
    }
  }

  return ret;
}

// 1. Two objects are equal if their key-value number are equal and their keys are equal and their values are equal.
// 2. If the key-value number of two objects are not equal, then objects with fewer key-value pairs are smaller
// 3. Compare each key-value pair and return the result of the first unequal encounter.
int ObIJsonBase::compare_object(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_OBJECT || j_type_b != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    const uint64_t len_a = element_count();
    const uint64_t len_b = other.element_count();
    res = ObJsonBaseUtil::compare_numbers(len_a, len_b);
    for (uint64_t i = 0; (res == 0) && OB_SUCC(ret) && i < len_a; i++) {
      ObString key_a, key_b;
      if (OB_FAIL(get_key(i, key_a))) {
        LOG_WARN("failed to get this key", K(ret), K(i));
      } else if (OB_FAIL(other.get_key(i, key_b))) {
        LOG_WARN("failed to get other key.", K(ret), K(i));
      } else {
        // Compare keys.
        res = key_a.compare(key_b);
        if (res == 0) {
          ObIJsonBase *jb_a_ptr = NULL;
          ObIJsonBase *jb_b_ptr = NULL;
          ObJsonBin j_bin_a(allocator_);
          ObJsonBin j_bin_b(allocator_);
          if (is_bin()) {
            jb_a_ptr = &j_bin_a;
            jb_b_ptr = &j_bin_b;
          }
          // Compare value.
          if (OB_FAIL(get_object_value(i, jb_a_ptr))) {
            LOG_WARN("fail to get this json obj element", K(ret), K(i), K(len_a));
          } else if (OB_FAIL(other.get_object_value(i, jb_b_ptr))) {
            LOG_WARN("fail to get other json obj element", K(ret), K(i), K(len_b));
          } else if (OB_FAIL(jb_a_ptr->compare(*jb_b_ptr, res))) {
            LOG_WARN("fail to compare this json to other json", K(ret), K(i), K(len_a), K(len_b));
          }
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_int(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_INT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    int64_t int_a = get_int();
    switch (j_type_b) {
      case ObJsonNodeType::J_INT: {
        int64_t int_b = other.get_int();
        res = ObJsonBaseUtil::compare_numbers(int_a, int_b);
        break;
      }

      case ObJsonNodeType::J_UINT: {
        uint64_t uint_b = other.get_uint();
        res = ObJsonBaseUtil::compare_int_uint(int_a, uint_b);
        break;
      }

      case ObJsonNodeType::J_DOUBLE: {
        double double_b = other.get_double();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_int(double_b, int_a, res))) {
          LOG_WARN("fail to compare double with int", K(ret), K(double_b), K(int_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      case ObJsonNodeType::J_DECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_int(num_b, int_a, res))) {
          LOG_WARN("fail to compare decimal with int", K(ret), K(num_b), K(int_a));
        } else {
          res = -1 * res;
        }
        break;
      }
      
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_uint(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_UINT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    uint64_t uint_a = get_uint();
    switch (j_type_b) {
      case ObJsonNodeType::J_UINT: {
        uint64_t uint_b = other.get_uint();
        res = ObJsonBaseUtil::compare_numbers(uint_a, uint_b);
        break;
      }

      case ObJsonNodeType::J_INT: {
        int64_t int_b = other.get_int();
        res = -1 * ObJsonBaseUtil::compare_int_uint(int_b, uint_a);
        break;
      }

      case ObJsonNodeType::J_DOUBLE: {
        double double_b = other.get_double();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_uint(double_b, uint_a, res))) {
          LOG_WARN("fail to compare double with uint", K(ret), K(double_b), K(uint_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      case ObJsonNodeType::J_DECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_uint(num_b, uint_a, res))) {
          LOG_WARN("fail to compare decimal with uint", K(ret), K(num_b), K(uint_a));
        } else {
          res = -1 * res;
        }
        break;
      }
      
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_double(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_DOUBLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    double double_a = get_double();
    switch (j_type_b) {
      case ObJsonNodeType::J_DOUBLE: {
        double double_b = other.get_double();
        res = ObJsonBaseUtil::compare_numbers(double_a, double_b);
        break;
      }

      case ObJsonNodeType::J_INT: {
        int64_t int_b = other.get_int();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_int(double_a, int_b, res))) {
          LOG_WARN("fail to compare double with int", K(ret), K(double_a), K(int_b));
        }
        break;
      }

      case ObJsonNodeType::J_UINT: {
        uint64_t uint_b = other.get_uint();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_uint(double_a, uint_b, res))) {
          LOG_WARN("fail to compare double with uint", K(ret), K(double_a), K(uint_b));
        }
        break;
      }

      case ObJsonNodeType::J_DECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_double(num_b, double_a, res))) {
          LOG_WARN("fail to compare decimal with double", K(ret), K(double_a), K(num_b));
        } else {
          res = -1 * res;
        }
        break;
      }
      
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_decimal(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_DECIMAL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    number::ObNumber num_a = get_decimal_data();
    switch (j_type_b) {
      case ObJsonNodeType::J_DECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (num_a.is_zero() && num_b.is_zero()) {
          res = 0;
        } else {
          res = num_a.compare(num_b);
        }
        break;
      }

      case ObJsonNodeType::J_INT: {
        int64_t int_b = other.get_int();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_int(num_a, int_b, res))) {
          LOG_WARN("fail to compare decimal with int", K(ret), K(num_a), K(int_b));
        }
        break;
      }

      case ObJsonNodeType::J_UINT: {
        uint64_t uint_b = other.get_uint();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_uint(num_a, uint_b, res))) {
          LOG_WARN("fail to compare decimal with uint", K(ret), K(num_a), K(uint_b));
        }
        break;
      }

      case ObJsonNodeType::J_DOUBLE: {
        double double_b = other.get_double();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_double(num_a, double_b, res))) {
          LOG_WARN("fail to compare decimal with double", K(ret), K(num_a), K(double_b));
        }
        break;
      }
    
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_datetime(ObDTMode dt_mode_a, const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_DATETIME && j_type_a != ObJsonNodeType::J_DATE
      && j_type_a != ObJsonNodeType::J_TIME && j_type_a != ObJsonNodeType::J_TIMESTAMP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    ObDTMode dt_mode_b;
    if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type_b, dt_mode_b))) {
      LOG_WARN("fail to get dt mode by json tyoe", K(ret), K(j_type_b));
    } else {
      ObTime t_a;
      ObTime t_b;
      if (OB_FAIL(get_obtime(t_a))) {
        LOG_WARN("fail to get json obtime_a", K(ret));
      } else {
        int64_t int_a = ObTimeConverter::ob_time_to_int(t_a, dt_mode_a);
        if (OB_FAIL(other.get_obtime(t_b))) {
          LOG_WARN("fail to get json obtime_b", K(ret));
        } else {
          int64_t int_b = ObTimeConverter::ob_time_to_int(t_b, dt_mode_b);
          res = ObJsonBaseUtil::compare_numbers(int_a, int_b);
        }
      }
    }
  }

  return ret;
}

static constexpr int JSON_TYPE_NUM = static_cast<int>(ObJsonNodeType::J_ERROR) + 1;
// 0 means that the priority is the same, but it also means that types are directly comparable, 
// i.e. decimal, int, uint, and double are all comparable.
// 1 means this_type has a higher priority
// -1 means this_type has a lower priority
static constexpr int type_comparison[JSON_TYPE_NUM][JSON_TYPE_NUM] = {
  /* NULL */      {0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  /* DECIMAL */   {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  /* INT */       {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  /* UINT */      {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  /* DOUBLE */    {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  /* STRING */    {1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  /* OBJECT */    {1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1, -1},
  /* ARRAY */     {1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1},
  /* BOOLEAN */   {1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1},
  /* DATE */      {1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1},
  /* TIME */      {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1},
  /* DATETIME */  {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1, -1},
  /* TIMESTAMP */ {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1, -1},
  /* OPAQUE */    {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1},
  /* ERROR */     {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1},
};

int ObIJsonBase::compare(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();
  res = 0;

  if (j_type_a == ObJsonNodeType::J_ERROR || j_type_b == ObJsonNodeType::J_ERROR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error json type", K(j_type_a), K(j_type_b));
  } else {
    // Compare the matrix to get which json type has a higher priority, and return the result if the priority is different.
    int type_cmp = type_comparison[static_cast<int>(j_type_a)][static_cast<int>(j_type_b)];
    if (type_cmp != 0) { // Different priorities, complete comparison.
      res = type_cmp;
    } else { // Same priority.
      switch (j_type_a) {
        case ObJsonNodeType::J_ARRAY: {
          if (OB_FAIL(compare_array(other, res))) {
            LOG_WARN("fail to compare json array", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_OBJECT: {
          if (OB_FAIL(compare_object(other, res))) {
            LOG_WARN("fail to compare json object", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_STRING: {
          ObString str_a(get_data_length(), get_data());
          ObString str_b(other.get_data_length(), other.get_data());
          res = str_a.compare(str_b);
          break;
        }

        case ObJsonNodeType::J_INT: {
          if (OB_FAIL(compare_int(other, res))) {
            LOG_WARN("fail to compare json int", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_UINT: {
          if (OB_FAIL(compare_uint(other, res))) {
            LOG_WARN("fail to compare json uint", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_DOUBLE: {
          if (OB_FAIL(compare_double(other, res))) {
            LOG_WARN("fail to compare json double", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_DECIMAL: {
          if (OB_FAIL(compare_decimal(other, res))) {
            LOG_WARN("fail to compare json decimal", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_BOOLEAN: {
          res = ObJsonBaseUtil::compare_numbers(get_boolean(), other.get_boolean());
          break;
        }

        case ObJsonNodeType::J_DATETIME: {
          if(OB_FAIL(compare_datetime(DT_TYPE_DATETIME, other, res))) {
            LOG_WARN("fail to compare json datetime", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_TIMESTAMP: {
          if(OB_FAIL(compare_datetime(DT_TYPE_DATETIME, other, res))) {
            LOG_WARN("fail to compare json timestamp", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_TIME: {
          if(OB_FAIL(compare_datetime(DT_TYPE_TIME, other, res))) {
            LOG_WARN("fail to compare json time", K(ret));
          }
          break;
        }
        
        case ObJsonNodeType::J_DATE: {
          if(OB_FAIL(compare_datetime(DT_TYPE_DATE, other, res))) {
            LOG_WARN("fail to compare json date", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_OPAQUE: {
          // If Opaque has the same type and value, the two Opaque are equal.
          res = ObJsonBaseUtil::compare_numbers(field_type(), other.field_type());
          if (res == 0) {
            ObString str_a(get_data_length(), get_data());
            ObString str_b(other.get_data_length(), other.get_data());
            res = str_a.compare(str_b);
          }
          break;
        }

        case ObJsonNodeType::J_NULL: {
          if (j_type_a != j_type_b) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("both json type should be NULL", K(j_type_a), K(j_type_b));
          } else {
            res = 0;
          }
          break;
        }
        
        default:{
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect json type",K(j_type_a), K(j_type_b));
          break;
        }
      }
    }
  }
  return ret;
}


uint32_t ObIJsonBase::depth()
{
  INIT_SUCC(ret);
  uint32_t depth = 0;

  if (is_bin()) {
    ObArenaAllocator allocator;
    ObIJsonBase *j_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_TREE, j_tree))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else {
      depth = j_tree->depth();
    }
  } else {
    depth = static_cast<ObJsonNode *>(this)->depth();
  }

  return depth;
}

int ObIJsonBase::get_location(ObJsonBuffer &path)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    ObArenaAllocator allocator;
    ObIJsonBase *j_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_TREE, j_tree))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else if (OB_FAIL(static_cast<ObJsonNode *>(j_tree)->get_location(path))) {
      LOG_WARN("fail to get path location", K(ret));
    }
  } else { // is_tree
    if (OB_FAIL(static_cast<ObJsonNode *>(this)->get_location(path))) {
      LOG_WARN("fail to get path location", K(ret));
    }
  }

  return ret;
}

int ObIJsonBase::merge_tree(ObIAllocator *allocator, ObIJsonBase *other, ObIJsonBase *&result)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator) || OB_ISNULL(other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(allocator), KP(other));
  } else if (is_bin()) {
    ObIJsonBase *j_this_tree = NULL;
    ObIJsonBase *j_other_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(allocator, this, ObJsonInType::JSON_TREE,
        j_this_tree))) {
      LOG_WARN("fail to transform this to tree", K(ret));
    } else if (other->is_bin()) {
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, other, ObJsonInType::JSON_TREE,
          j_other_tree))) {
        LOG_WARN("fail to transform other to tree", K(ret));
      } 
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(static_cast<ObJsonNode *>(j_this_tree)->merge_tree(allocator,
          j_other_tree, result))) {
        LOG_WARN("fail to merge tree", K(ret), K(*j_this_tree), K(*j_other_tree));
      }
    }
  } else { // is_tree
    if (OB_FAIL(static_cast<ObJsonNode *>(this)->merge_tree(allocator, other, result))) {
      LOG_WARN("fail to merge tree", K(ret), K(*other));
    }
  }

  return ret;
}

int ObIJsonBase::get_used_size(uint64_t &size)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    size = j_bin->get_used_bytes();
  } else { // is tree
    ObArenaAllocator allocator;
    ObIJsonBase *j_bin = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else {
      size = static_cast<const ObJsonBin *>(j_bin)->get_used_bytes();
    }
  }

  return ret;
}

int ObIJsonBase::get_free_space(uint64_t &size)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    if (OB_FAIL(j_bin->get_free_space(size))) {
      LOG_WARN("fail to get json binary free space", K(ret), K(*j_bin));
    }
  } else { // is tree
    ObArenaAllocator allocator;
    ObIJsonBase *j_bin = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else if (OB_FAIL(static_cast<const ObJsonBin *>(j_bin)->get_free_space(size))) {
      LOG_WARN("fail to get json binary free space after transforming", K(ret), K(*j_bin));
    }
  }

  return ret;
}

int ObIJsonBase::get_raw_binary(common::ObString &out, ObIAllocator *allocator)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    if (OB_FAIL(j_bin->get_raw_binary(out, allocator))) {
      LOG_WARN("fail to get raw binary", K(ret), K(*j_bin));
    }
  } else { // is tree
    if(OB_ISNULL(allocator)) { // check param
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("param allocator is null", K(ret));
    } else {
      ObIJsonBase *j_bin = NULL;
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
        LOG_WARN("fail to transform to tree", K(ret));
      } else if (OB_FAIL(static_cast<const ObJsonBin *>(j_bin)->get_raw_binary(out, allocator))) {
        LOG_WARN("fail to get raw binary after transforming", K(ret), K(*j_bin));
      }
    }
  }

  return ret;
}

JsonObjectIterator ObIJsonBase::object_iterator() const
{
  return JsonObjectIterator(this);
}

int ObIJsonBase::to_int(int64_t &value, bool check_range, bool force_convert) const
{
  INIT_SUCC(ret);
  int64_t val = 0;

  switch (json_type()) {
    case ObJsonNodeType::J_UINT: {
      uint64_t ui = get_uint();
      if (ui > INT64_MAX && check_range) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("fail to cast uint to int, out of range", K(ret), K(ui));
      } else {
        val = static_cast<int64_t>(ui);
      }
      break;
    }

    case ObJsonNodeType::J_INT: {
      val = get_int();
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *start = get_data();
      if (OB_ISNULL(start)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        const char *end = start + length;
        bool valid = false;
        if (check_range && OB_UNLIKELY(NULL != memchr(start, '.', length))) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = ObFastAtoi<int64_t>::atoi(start, end, valid);
          if (valid == false) {
            ret = OB_ERR_DATA_TRUNCATED;
            LOG_WARN("fail to cast string to int", K(ret), K(length), KP(start));
          }
        }
      }
      
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      val = get_boolean();
      break;
    }

    case ObJsonNodeType::J_DECIMAL: {
      number::ObNumber nmb = get_decimal_data();
      if (OB_FAIL(nmb.extract_valid_int64_with_round(val))){
        LOG_WARN("fail to convert decimal as int", K(ret));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE: {
      double d = get_double();
      if (d <= static_cast<double>(LLONG_MIN)) {
        if (check_range) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = LLONG_MIN;
        }
      } else if (d >= static_cast<double>(LLONG_MAX)) {
        if (check_range) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = LLONG_MAX;
        }
      } else {
        val = static_cast<int64_t>(rint(d));
      }
      break;
    }  
    
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to int", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret) || force_convert) {
    value = val;
  }

  return ret;
}

int ObIJsonBase::to_uint(uint64_t &value, bool fail_on_negative, bool check_range) const
{
  INIT_SUCC(ret);
  uint64_t val = 0;

  switch (json_type()) {
    case ObJsonNodeType::J_UINT: {
      val = get_uint();
      break;
    }

    case ObJsonNodeType::J_INT: {
      int64_t signed_val = get_int();
      if (signed_val < 0 && fail_on_negative) {
        LOG_WARN("json int value is negative, can't change to uint", K(ret));
        ret = OB_OPERATE_OVERFLOW;
      } else {
        val = static_cast<uint64_t>(signed_val);
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      const char *data = get_data();
      uint64_t length = get_data_length();
      int err = 0;
      char *endptr = NULL;
      bool is_unsigned = true;
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else if (data[0] == '-') {
        is_unsigned = false;
        if (fail_on_negative) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("value is negative, can't change to uin", K(ret), KP(data));
        }
      } else if (check_range && OB_UNLIKELY(NULL != memchr(data, '.', length))) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("value contains ., can't change to uin", K(ret), KP(data));
      }

      if (OB_SUCC(ret)) {
        val = ObCharset::strntoullrnd(data, length, is_unsigned, &endptr, &err);
        if (ERANGE == err && (UINT64_MAX == val || 0 == val)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("faild to cast string to uint, cause data is out of range", K(ret), K(is_unsigned),
                                                                               KP(data), K(length));
        } else {
          // check_convert_str_err
          // 1. only one of data and endptr is null, it is invalid input.
          if ((OB_ISNULL(data) || OB_ISNULL(endptr)) && data != endptr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null pointer(s)", K(ret), KP(data), KP(endptr));
          } else
          // 2. data == endptr include NULL == NULL.
          if (OB_UNLIKELY(data == endptr) || OB_UNLIKELY(EDOM == err)) {
            ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
            LOG_WARN("wrong value", K(ret), K(err), K(length));
          } else {
            // 3. so here we are sure that both data and endptr are not NULL.
            endptr += ObCharset::scan_str(endptr, data + length, OB_SEQ_SPACES);
            if (endptr < data + length) {
              ret = OB_ERR_DATA_TRUNCATED; //1265
              LOG_DEBUG("check_convert_str_err", K(length), K(data - endptr));
            }
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      val = get_boolean();
      break;
    }

    case ObJsonNodeType::J_DECIMAL: {
      number::ObNumber num_val = get_decimal_data();
      int64_t i = 0;
      uint64_t ui = 0;
      number::ObNumber nmb;
      ObArenaAllocator allocator;
      if (num_val.is_negative() && fail_on_negative) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("the number is negative, can't change to uint", K(ret), K(num_val));
      } else if (OB_FAIL(nmb.from(num_val, allocator))) {
        LOG_WARN("deep copy failed", K(ret));
      } else if (OB_FAIL(ObJsonBaseUtil::number_to_uint(nmb, val))) {
        LOG_WARN("fail to cast number to uint", K(ret), K(nmb), K(num_val));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE: {
      double d = get_double();
      if (d < 0 && fail_on_negative) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("double value is negative", K(ret), K(fail_on_negative), K(d));
      } else {
        ret = ObJsonBaseUtil::double_to_uint(d, val, check_range);
      }
      break;
    }  
    
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to int", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = val;
  }

  return ret;
}

int ObIJsonBase::to_double(double &value) const
{
  INIT_SUCC(ret);
  double val = 0.0;

  switch (json_type()) {
    case ObJsonNodeType::J_DECIMAL: {
      number::ObNumber num_val = get_decimal_data();
      char buf[MAX_DOUBLE_PRINT_SIZE] = {0};
      int64_t pos = 0;
      if (OB_FAIL(num_val.format(buf, sizeof(buf), pos, -1))) {
        LOG_WARN("fail to format number", K(ret), K(num_val));
      } else {
        char *endptr = NULL;
        int err = 0;
        val = ObCharset::strntod(buf, pos, &endptr, &err);
        if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("faild to cast string to double, cause data is out of range",
              K(ret), K(val), K(pos), K(num_val));
        }
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        char *endptr = NULL;
        int err = 0;
        val = ObCharset::strntodv2(data, length, &endptr, &err);
        if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("faild to cast string to double, cause data is out of range", K(ret), K(length),
                                                                                 KP(data), K(val));
        } else {
          ObString tmp_str(length, data);
          ObString trimed_str = tmp_str.trim();
          if (lib::is_mysql_mode() && 0 == trimed_str.length()) {
            ret = OB_ERR_DOUBLE_TRUNCATED;
            LOG_WARN("convert string to double failed", K(ret), K(tmp_str));
          } else {
            // 1. only one of data and endptr is null, it is invalid input.
            if ((OB_ISNULL(data) || OB_ISNULL(endptr)) && data != endptr) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null pointer(s)", K(ret), KP(data), KP(endptr));
            } else if (OB_UNLIKELY(data == endptr) || OB_UNLIKELY(EDOM == err)) { // 2. data == endptr include NULL == NULL.
              ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
              LOG_WARN("wrong value", K(ret), K(length), K(val));
            } else { // 3. so here we are sure that both data and endptr are not NULL.
              endptr += ObCharset::scan_str(endptr, data + length, OB_SEQ_SPACES);
              if (endptr < data + length) {
                ret = OB_ERR_DATA_TRUNCATED; //1265
                LOG_DEBUG("check_convert_str_err", K(length), K(data - endptr));
              }
            }
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE: {
      val = get_double();
      break;
    }

    case ObJsonNodeType::J_INT: {
      val = static_cast<double>(get_int());
      break;
    }

    case ObJsonNodeType::J_UINT: {
      val = static_cast<double>(get_uint());
      break;
    }
    
    case ObJsonNodeType::J_BOOLEAN: {
      val = get_boolean();
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to double", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = val;
  }

  return ret;
}

int ObIJsonBase::to_number(ObIAllocator *allocator, number::ObNumber &number) const
{
  INIT_SUCC(ret);
  number::ObNumber num;

  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else {
    switch (json_type()) {
      case ObJsonNodeType::J_DECIMAL: {
        num = get_decimal_data();
        break;
      }

      case ObJsonNodeType::J_STRING: {
        uint64_t length = get_data_length();
        const char *data = get_data();
        if (OB_ISNULL(data)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("data is null", K(ret));
        } else if (OB_FAIL(num.from(data, length, *allocator))) {
          LOG_WARN("fail to create number from string", K(ret), KP(data));
        }
        break;
      }

      case ObJsonNodeType::J_DOUBLE: {
        double d = get_double();
        if (OB_FAIL(ObJsonBaseUtil::double_to_number(d, *allocator, num))) {
          LOG_WARN("fail to cast double to number", K(ret), K(d));
        }
        break;
      }

      case ObJsonNodeType::J_INT: {
        int64_t i = get_int();
        if (OB_FAIL(num.from(i, *allocator))) {
          LOG_WARN("fail to create number from int", K(ret), K(i));
        }
        break;
      }

      case ObJsonNodeType::J_UINT: {
        uint64_t ui = get_uint();
        if (OB_FAIL(num.from(ui, *allocator))) {
          LOG_WARN("fail to number from uint", K(ret), K(ui));
        }
        break;
      }

      case ObJsonNodeType::J_BOOLEAN: {
        bool b = get_boolean();
        if (OB_FAIL(num.from(static_cast<int64_t>(b), *allocator))) {
          LOG_WARN("fail to number from int(boolean)", K(ret), K(b));
        }
        break;
      }
      
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to cast json type to double", K(ret), K(json_type()));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    number = num;
  }

  return ret;
}

int ObIJsonBase::to_datetime(int64_t &value, ObTimeConvertCtx *cvrt_ctx_t) const
{
  INIT_SUCC(ret);
  int64_t datetime = 0;
  ObTimeConvertCtx cvrt_ctx(NULL, false);
  if (OB_NOT_NULL(cvrt_ctx_t) && cvrt_ctx_t->is_timestamp_) {
    cvrt_ctx.tz_info_ = cvrt_ctx_t->tz_info_;
    cvrt_ctx.is_timestamp_ = cvrt_ctx_t->is_timestamp_;
  }
  switch (json_type()) {
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIMESTAMP: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(t, cvrt_ctx, datetime))) {
        LOG_WARN("fail to case ObTime to datetime", K(ret), K(t));
      }
      break;
    }
    
    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, datetime))) {
          LOG_WARN("fail to cast string to datetime", K(ret), K(str));
        }
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to datetime", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = datetime;
  }

  return ret;
}

int ObIJsonBase::to_date(int32_t &value) const
{
  INIT_SUCC(ret);
  int32_t date = 0;

  switch (json_type()) {
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIMESTAMP: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        date = ObTimeConverter::ob_time_to_date(t);
      }
      break;
    }
    
    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObTimeConverter::str_to_date(str, date))) {
          LOG_WARN("fail to cast string to date", K(ret), K(str));
        }
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to date", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = date;
  }

  return ret;
}

int ObIJsonBase::to_time(int64_t &value) const
{
  INIT_SUCC(ret);
  int64_t time = 0;

  switch (json_type()) {
    case ObJsonNodeType::J_TIME: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        time = ObTimeConverter::ob_time_to_time(t);
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObTimeConverter::str_to_time(str, time))) {
          LOG_WARN("fail to cast string to time", K(ret), K(str));
        }
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to time", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = time;
  }

  return ret;
}

int ObIJsonBase::to_bit(uint64_t &value) const
{
  INIT_SUCC(ret);
  uint64_t bit = 0;
  const ObJsonNodeType j_type = json_type();
  ObDTMode dt_mode = 0;

  switch (j_type) {
    case ObJsonNodeType::J_DATETIME: {
      dt_mode = DT_TYPE_DATETIME; // set dt_node and pass through
    }

    case ObJsonNodeType::J_DATE: {
      if (dt_mode == 0) {
        dt_mode = DT_TYPE_DATE; // set dt_node and pass through
      }
    }

    case ObJsonNodeType::J_TIME: {
      if (dt_mode == 0) {
        dt_mode = DT_TYPE_TIME; // set dt_node and pass through
      }
    }

    case ObJsonNodeType::J_TIMESTAMP: {
      if (dt_mode == 0) {
        dt_mode = DT_TYPE_DATETIME; // set dt_node and pass through
      }
  
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        const int64_t length = OB_CAST_TO_VARCHAR_MAX_LENGTH + 1;
        char buf[length] = {0};
        int64_t pos = 0;
        dt_mode = (t.mode_ == 0 ? dt_mode : t.mode_);
        if (OB_FAIL(ObTimeConverter::ob_time_to_str(t, dt_mode, 0, buf, length, pos, true))) {
          LOG_WARN("fail to change time to string", K(ret), K(j_type), K(t.mode_), K(t), K(pos));
        } else {
          ObString str(sizeof(buf), strlen(buf), buf);
          if (OB_FAIL(ObJsonBaseUtil::string_to_bit(str, bit))) {
            LOG_WARN("fail to cast string to bit", K(ret), K(str));
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      bit = get_boolean();
      break;
    }

    case ObJsonNodeType::J_DECIMAL: {
      int64_t i = 0;
      uint64_t ui = 0;
      number::ObNumber nmb;
      number::ObNumber num_val = get_decimal_data();
      ObArenaAllocator allocator;
      if (OB_FAIL(nmb.from(num_val, allocator))) {
        LOG_WARN("deep copy failed", K(ret));
      } else if (OB_FAIL(ObJsonBaseUtil::number_to_uint(nmb, bit))) {
        LOG_WARN("fail to cast number to uint", K(ret), K(nmb));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE: {
      double d = get_double();
      if (OB_FAIL(ObJsonBaseUtil::double_to_uint(d, bit))) {
        LOG_WARN("fail to change double to uint", K(ret), K(d));
      }
      break;
    }

    case ObJsonNodeType::J_INT: {
      bit = static_cast<uint64_t>(get_int());
      break;
    }

    case ObJsonNodeType::J_UINT: {
      bit = get_uint();
      break;
    }

    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OPAQUE: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObJsonBaseUtil::string_to_bit(str, bit))) {
          LOG_WARN("fail to cast string to bit", K(ret), K(str));
        }
      }
      break;
    }

    case ObJsonNodeType::J_NULL: {
      bit = 0;
      break;
    }
  
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to bit", K(ret), K(j_type));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = bit;
  }

  return ret;
}

int ObJsonBaseFactory::get_json_base(ObIAllocator *allocator, const ObString &buf,
                                     ObJsonInType in_type, ObJsonInType expect_type,
                                     ObIJsonBase *&out)
{
  return get_json_base(allocator, buf.ptr(), buf.length(), in_type, expect_type, out);
}

int ObJsonBaseFactory::get_json_base(ObIAllocator *allocator, const char *ptr, uint64_t length,
                                     ObJsonInType in_type, ObJsonInType expect_type,
                                     ObIJsonBase *&out)
{
  INIT_SUCC(ret);
  void *buf = NULL;

  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator), KP(ptr));
  } else if (OB_ISNULL(ptr) || length == 0) {
    ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    LOG_WARN("param is NULL", K(ret), KP(ptr), K(length));
  } else if (in_type != ObJsonInType::JSON_TREE && in_type != ObJsonInType::JSON_BIN) { // check in_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param in_type is invalid", K(ret), K(in_type));
  } else if (expect_type != ObJsonInType::JSON_TREE && expect_type != ObJsonInType::JSON_BIN) { // check expect_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expect_type is invalid", K(ret), K(expect_type));
  } else if (in_type == ObJsonInType::JSON_TREE) {
    ObJsonNode *j_tree = NULL;
    if (OB_FAIL(ObJsonParser::get_tree(allocator, ptr, length, j_tree))) {
      LOG_WARN("fail to get json tree", K(ret), K(length), K(in_type), K(expect_type));
    } else if (expect_type == ObJsonInType::JSON_TREE) {
      out = j_tree;
    } else { // expect json bin
      if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(expect_type), K(sizeof(ObJsonBin)));
      } else {
        ObJsonBin *j_bin = new (buf) ObJsonBin(allocator);
        if (OB_FAIL(j_bin->parse_tree(j_tree))) {
          LOG_WARN("fail to parse tree", K(ret), K(in_type), K(expect_type), K(*j_tree));
        } else {
          out = j_bin;
        }
      }
    }
  } else if (in_type == ObJsonInType::JSON_BIN) {
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(expect_type), K(sizeof(ObJsonBin)));
    } else {
      ObJsonBin *j_bin = new (buf) ObJsonBin(ptr, length, allocator);
      if (OB_FAIL(j_bin->reset_iter())) {
        LOG_WARN("fail to reset iter", K(ret), K(in_type), K(expect_type));
      } else if (expect_type == ObJsonInType::JSON_BIN) {
        out = j_bin;
      } else { // expect json tree
        ObJsonNode *j_tree = NULL;
        if (OB_FAIL(j_bin->to_tree(j_tree))) {
          LOG_WARN("fail to change bin to tree", K(ret), K(in_type), K(expect_type), K(*j_bin));
        } else {
          out = j_tree;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect json type",K(in_type), K(expect_type));
  }

  return ret;
}

int ObJsonBaseFactory::transform(ObIAllocator *allocator, ObIJsonBase *src,
                                 ObJsonInType expect_type, ObIJsonBase *&out)
{
  INIT_SUCC(ret);
  void *buf = NULL;
  ObJsonInType src_type = src->get_internal_type();

  if (OB_ISNULL(allocator) || OB_ISNULL(src)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator), KP(src));
  } else if (src_type != ObJsonInType::JSON_TREE && src_type != ObJsonInType::JSON_BIN) { // check in_type
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param src_type is invalid", K(ret), K(src_type));
  } else if (expect_type != ObJsonInType::JSON_TREE && expect_type != ObJsonInType::JSON_BIN) { // check expect_type
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param expect_type is invali", K(ret), K(expect_type));
  } else if (src_type == ObJsonInType::JSON_TREE) { // input:tree
    if (expect_type == ObJsonInType::JSON_BIN) { // to bin
      if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(src_type), K(expect_type), K(sizeof(ObJsonBin)));
      } else {
        ObJsonBin *j_bin = new (buf) ObJsonBin(allocator);
        ObJsonNode *j_tree = static_cast<ObJsonNode *>(src);
        if (OB_FAIL(j_bin->parse_tree(j_tree))) {
          LOG_WARN("fail to parse tree", K(ret), K(src_type), K(expect_type), K(*j_tree));
        } else {
          out = j_bin;
        }
      }
    } else { // to tree, itself
      out = src;
    }
  } else if (src_type == ObJsonInType::JSON_BIN) { // input:bin
    if (expect_type == ObJsonInType::JSON_TREE) { // to tree
      ObJsonNode *j_tree = NULL;
      ObJsonBin *j_bin = static_cast<ObJsonBin *>(src);
      if (OB_FAIL(j_bin->to_tree(j_tree))) {
        LOG_WARN("fail to change bin to tree", K(ret), K(src_type), K(expect_type), K(*j_bin));
      } else {
        out = j_tree;
      }
    } else { // to bin, itself
      out = src;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect json type",K(src_type), K(expect_type));
  }

  return ret;  
}

int ObJsonBaseUtil::get_dt_mode_by_json_type(ObJsonNodeType j_type, ObDTMode &dt_mode)
{
  INIT_SUCC(ret);

  switch (j_type) {
    case ObJsonNodeType::J_DATETIME: {
      dt_mode = DT_TYPE_DATETIME;
      break;
    }

    case ObJsonNodeType::J_DATE: {
      dt_mode = DT_TYPE_DATE;
      break;
    }

    case ObJsonNodeType::J_TIMESTAMP: {
      dt_mode = DT_TYPE_DATETIME;
      break;
    }

    case ObJsonNodeType::J_TIME: {
      dt_mode = DT_TYPE_TIME;
      break;
    }
    
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type", K(j_type));
      break;
    }
  }

  return ret;
}

int ObJsonBaseUtil::append_comma(ObJsonBuffer &j_buf, bool is_pretty)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append(","))) {
    LOG_WARN("fail to append \",\" to buffer", K(ret), K(is_pretty));
  } else if (!is_pretty) {
    if (OB_FAIL(j_buf.append(" "))) {
      LOG_WARN("fail to append space to buffer", K(ret));
    }
  }
  
  return ret;
}

int ObJsonBaseUtil::append_newline_and_indent(ObJsonBuffer &j_buf, uint64_t level)
{
  // Append newline and two spaces per indentation level.
  INIT_SUCC(ret);

  if (level > ObJsonParser::JSON_DOCUMENT_MAX_DEPTH) {
    ret = OB_ERR_JSON_OUT_OF_DEPTH;
    LOG_WARN("indent level is too deep", K(ret), K(level));
  } else if (OB_FAIL(j_buf.append("\n"))) {
    LOG_WARN("fail to append newline to buffer", K(ret), K(level));
  } else if (OB_FAIL(j_buf.reserve(level * 2))) {
    LOG_WARN("fail to reserve memory for buffer", K(ret), K(level));
  } else {
    char str[level * 2];
    MEMSET(str, ' ', level * 2);
    if (OB_FAIL(j_buf.append(str, level * 2))) {
      LOG_WARN("fail to append space to buffer", K(ret), K(level));
    }
  }
  
  return ret;
}

int ObJsonBaseUtil::escape_character(char c, ObJsonBuffer &j_buf)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append("\\"))) {
    LOG_WARN("fail to append \"\\\" to j_buf", K(ret), K(c));
  } else {
    switch (c) {
      case '\b': {
        if (OB_FAIL(j_buf.append("b"))) {
          LOG_WARN("fail to append 'b' to j_buf", K(ret));
        }
        break;
      }

      case '\t': {
        if (OB_FAIL(j_buf.append("t"))) {
          LOG_WARN("fail to append 't' to j_buf", K(ret));
        }
        break;
      }

      case '\n': {
        if (OB_FAIL(j_buf.append("n"))) {
          LOG_WARN("fail to append 'n' to j_buf", K(ret));
        }
        break;
      }

      case '\f': {
        if (OB_FAIL(j_buf.append("f"))) {
          LOG_WARN("fail to append 'f' to j_buf", K(ret));
        }
        break;
      }

      case '\r': {
        if (OB_FAIL(j_buf.append("r"))) {
          LOG_WARN("fail to append 'r' to j_buf", K(ret));
        }
        break;
      }

      case '"':
      case '\\': {
        char str[1] = {c};
        if (OB_FAIL(j_buf.append(str, 1))) {
          LOG_WARN("fail to append c to j_buf", K(ret), K(c));
        }
        break;
      }
      
      default: {
        // Control characters that cannot be printed are printed in hexadecimal format. 
        // The meaning of the number is determined by ISO/IEC 10646
        static char _dig_vec_lower[] = "0123456789abcdefghijklmnopqrstuvwxyz";
        char high[1] = {_dig_vec_lower[(c & 0xf0) >> 4]};
        char low[1] = {_dig_vec_lower[(c & 0x0f)]};
        if (OB_FAIL(j_buf.append("u00", 3))) {
          LOG_WARN("fail to append \"u00\" to j_buf", K(ret));
        } else if (OB_FAIL(j_buf.append(high, 1))) {
          LOG_WARN("fail to append four high bits to j_buf", K(ret));
        } else if (OB_FAIL(j_buf.append(low, 1))) {
          LOG_WARN("fail to append four low bits to j_buf", K(ret));
        }
        break;
      }
    }
  }

  return ret;
}

struct ObFindEscapeFunc {
  ObFindEscapeFunc() {}

  bool operator()(const char c)
  {
    const unsigned char uc = static_cast<unsigned char>(c);
    return uc <= 0x1f || uc == '"' || uc == '\\';
  }
};

int ObJsonBaseUtil::add_double_quote(ObJsonBuffer &j_buf, const char *cptr, uint64_t length)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(cptr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param is null or length is 0", K(ret), K(cptr), K(length));
  } else if (OB_FAIL(j_buf.reserve(length + 2))) {
    LOG_WARN("fail to reserve length for j_buf", K(ret), K(length));
  } else if (OB_FAIL(j_buf.append("\""))) {
    LOG_WARN("fail to append double quote to j_buf", K(ret), K(length));
  } else {
    ObFindEscapeFunc func;
    const char *const end = cptr + length;
    while (true) {
      // Find the location of the next escape character.
      const char *next_special = std::find_if(cptr, end, func);

      // Most characters do not need to be escaped. 
      // Append the characters that do not need to be escaped
      if (OB_FAIL(j_buf.append(cptr, next_special - cptr))) {
        LOG_WARN("fail to append common segments to j_buf", K(ret), K(length),
                                                           K(next_special - cptr));
        break;
      }

      cptr = next_special;
      if (cptr == end) {
        break;
      }

      // Handle escape characters.
      if (OB_FAIL(escape_character(*cptr++, j_buf))) {
        LOG_WARN("fail to escape character", K(ret), K(length), K(*cptr));
        break;
      }
    };
  }

  if (OB_SUCC(ret) && OB_FAIL(j_buf.append("\""))) {
    LOG_WARN("fail to append end double quote to j_buf", K(ret), K(length));
  }

  return ret;
}

int ObJsonBaseUtil::append_string(ObJsonBuffer &j_buf, bool is_quoted,
                                const char *data, uint64_t length)
{
  INIT_SUCC(ret);

  if (is_quoted) {
    if (OB_FAIL(ObJsonBaseUtil::add_double_quote(j_buf, data, length))) {
      LOG_WARN("fail to add double quote", K(ret), K(length));
    }
  } else {
    if (OB_FAIL(j_buf.append(data, length))) {
      LOG_WARN("fail to append data", K(ret), K(length));
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_int_uint(int64_t a, uint64_t b)
{
  int ret = 0;

  if (a < 0) {
    ret = -1;
  } else {
    ret = compare_numbers(static_cast<uint64_t>(a), b);
  }

  return ret;
}

int ObJsonBaseUtil::compare_decimal_uint(const number::ObNumber &a, uint64_t b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_zero = (b == 0);

  if (a_is_zero) { // So if A is 0, let's deal with both cases where B is 0 or positive.
    res = b_is_zero ? 0 : -1;
  } else if (a_is_negative) { // A is negative, so it's definitely less than B.
    res = -1;
  } else { // If A is positive, let's deal with both cases where B is 0 or positive.
    if (b_is_zero) {
      res = 1;
    } else { // A and b are both positive.
      const int64_t MAX_BUF_SIZE = 256;
      char buf_alloc[MAX_BUF_SIZE];
      ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
      number::ObNumber b_num;

      if (OB_FAIL(b_num.from(b, allocator))) {
        LOG_WARN("fail to cast number from b", K(ret), K(a), K(b));
      } else {
        res = a.compare(b_num);
      }
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_decimal_int(const number::ObNumber &a, int64_t b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_negative = (b < 0);
  const bool b_is_zero = (b == 0);

  if (a_is_negative != b_is_negative) { // The two signs are different. Negative numbers are smaller.
    res = a_is_negative ? -1 : 1;
  } else if (a_is_zero) { // If a is 0, b must be either 0 or positive, otherwise the first if statement is entered.
    res = b_is_zero ? 0 : -1;
  } else if (b_is_zero) { // If b is 0, then a can only be 0 or positive, and the second if already rules out a being 0, so a is now positive.
    res = 1;
  } else { // Both a and B are positive or negative.
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber b_num;

    if (OB_FAIL(b_num.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(a), K(b));
    } else {
      res = a.compare(b_num);
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_decimal_double(const number::ObNumber &a, double b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_negative = (b < 0);
  const bool b_is_zero = (b == 0);

  if (a_is_negative != b_is_negative) { // The two signs are different. Negative numbers are smaller.
    res = a_is_negative ? -1 : 1;
  } else if (a_is_zero) { // If a is 0, b must be either 0 or positive, otherwise the first if statement is entered.
    res = b_is_zero ? 0 : -1;
  } else if (b_is_zero) { // If b is 0, then a can only be 0 or positive, and the second if already rules out a being 0, so a is now positive.
    res = 1;
  } else { // Both a and B are positive or negative.
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber b_num;
    if (OB_FAIL(ObJsonBaseUtil::double_to_number(b, allocator, b_num))) {
      if (ret == OB_NUMERIC_OVERFLOW) {
        res = a_is_negative ? 1 : -1; // They're both negative numbers. The larger the number, the smaller the number.
      } else { // Conversion error.
        LOG_WARN("fail to cast double to number", K(ret), K(b));
      }
    } else {
      res = a.compare(b_num);
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_double_int(double a, int64_t b, int &res)
{
  INIT_SUCC(ret);

  double b_double = static_cast<double>(b);
  if (a < b_double) {
    res = -1;
  } else if (a > b_double) {
    res = 1;
  } else {
    /*
      The two numbers were equal when compared as double. Since
      conversion from int64_t to double isn't lossless, they could
      still be different. Convert to decimal to compare their exact
      values.
    */
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber num_b;
    if (OB_FAIL(num_b.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(b));
    } else if (OB_FAIL(compare_decimal_double(num_b, a, res))) {
      LOG_WARN("fail to compare json decimal with double", K(num_b), K(a), K(b));
    } else {
      res = -1 * res;
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_int_json(int a, ObIJsonBase* other, int& result)
{
  INIT_SUCC(ret);
  if (other->json_type() != ObJsonNodeType::J_INT) {
    result = 1;
  } else {
    int64_t value = other->get_int();
    result = (a == value) ? 0 : (a > value ? 1 : -1);
  }
  return ret;
}

int ObJsonBaseUtil::compare_double_uint(double a, uint64_t b, int &res)
{
  INIT_SUCC(ret);

  double b_double = static_cast<double>(b);
  if (a < b_double) {
    res = -1;
  } else if (a > b_double) {
    res = 1;
  } else {
    /*
      The two numbers were equal when compared as double. Since
      conversion from uint64_t to double isn't lossless, they could
      still be different. Convert to decimal to compare their exact
      values.
    */
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber num_b;
    if (OB_FAIL(num_b.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(b));
    } else if (OB_FAIL(compare_decimal_double(num_b, a, res))) {
      LOG_WARN("fail to compare json decimal with double", K(num_b), K(a), K(b));
    } else {
      res = -1 * res;
    }
  }

  return ret;
}

template<class T>
int ObJsonBaseUtil::double_to_number(double d, T &allocator, number::ObNumber &num)
{
  INIT_SUCC(ret);
  char buf[DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE] = {0};
  uint64_t length = ob_gcvt(d, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
                            sizeof(buf) - 1, buf, NULL);
  ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
  ObPrecision res_precision = PRECISION_UNKNOWN_YET;
  ObScale res_scale = NUMBER_SCALE_UNKNOWN_YET;

  if (OB_FAIL(num.from_sci_opt(str.ptr(), str.length(), allocator, 
                                &res_precision, &res_scale))) {
    LOG_WARN("fail to create number from sci_opt", K(ret), K(d), K(length));
  }

  return ret;
}

int ObJsonBaseUtil::number_to_uint(number::ObNumber &nmb, uint64_t &value)
{
  INIT_SUCC(ret);

  int64_t i = 0;
  uint64_t ui = 0;
  uint64_t val = 0;
  
  if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(nmb.round(0)))) {
    LOG_WARN("round failed", K(ret), K(nmb));
  } else if (nmb.is_valid_int64(i)) {
    val = static_cast<uint64_t>(i);
  } else if (nmb.is_valid_uint64(ui)) {
    val = ui;
  } else {
    val = UINT64_MAX;
  }

  if (OB_SUCC(ret)) {
    value = val;
  }

  return ret;
}

int ObJsonBaseUtil::double_to_uint(double d, uint64_t &value, bool check_range)
{
  INIT_SUCC(ret);

  if (d <= LLONG_MIN) {
    if (check_range) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      value = LLONG_MIN;
    }
  } else if (d >= static_cast<double>(ULLONG_MAX)) {
    if (check_range) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      value = ULLONG_MAX;
    }
  } else {
    value = static_cast<uint64_t>(rint(d));
  }

  return ret;
}

int ObJsonBaseUtil::get_bit_len(const ObString &str, int32_t &bit_len)
{
  INIT_SUCC(ret);

  if (str.empty()) {
    bit_len = 1;
  } else {
    const char *ptr = str.ptr();
    uint32_t uneven_value = reinterpret_cast<const unsigned char&>(ptr[0]);
    if (0 == uneven_value) {
      bit_len = 1;
    } else {
      // Built-in Function: int __builtin_clz (unsigned int x).
      // Returns the number of leading 0-bits in x, starting at the most significant bit position.
      // If x is 0, the result is undefined.
      int32_t len = str.length();
      int32_t uneven_len = static_cast<int32_t>(
          sizeof(unsigned int) * 8 - __builtin_clz(uneven_value));
      bit_len = uneven_len + 8 * (len - 1);
    }
  }

  return ret;
}

uint64_t ObJsonBaseUtil::hex_to_uint64(const ObString &str)
{
  int32_t N = str.length();
  const uint8_t *p = reinterpret_cast<const uint8_t*>(str.ptr());
  uint64_t value = 0;

  if (OB_LIKELY(NULL != p)) {
    for (int32_t i = 0; i < N; ++i, ++p) {
      value = value * (UINT8_MAX + 1) + *p;
    }
  }

  return value;
}

int ObJsonBaseUtil::string_to_bit(ObString &str, uint64_t &value)
{
  INIT_SUCC(ret);

  uint64_t bit = 0;
  int32_t bit_len = 0;

  if (OB_FAIL(ObJsonBaseUtil::get_bit_len(str, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(str));
  } else if (bit_len > OB_MAX_BIT_LENGTH) {
    bit = UINT64_MAX;
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("bit len too long", K(bit_len));
  } else {
    bit = ObJsonBaseUtil::hex_to_uint64(str);
  }

  if (OB_SUCC(ret)) {
    value = bit;
  }

  return ret;
}

int ObJsonBaseUtil::sort_array_pointer(ObIJsonBase *orig, ObSortedVector<ObIJsonBase *> &vec)
{
  INIT_SUCC(ret);
  ObSortedVector<ObIJsonBase *>::iterator insert_pos = vec.end();
  for (uint64_t i = 0; OB_SUCC(ret) && i < orig->element_count(); i++) {
    ObIJsonBase *tmp = NULL;
    if (OB_FAIL(orig->get_array_element(i, tmp))) {
      LOG_WARN("fail to get_array_element from json", K(ret));
    } else {
      int tmp_ret = vec.insert_unique(tmp, insert_pos, Array_less(), Array_equal());
      if (tmp_ret != OB_SUCCESS && tmp_ret != OB_CONFLICT_VALUE) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("fail to push_back into result", K(ret));
      }
    }
  }
  return ret;
}

bool ObJsonBaseUtil::binary_search(ObSortedVector<ObIJsonBase *> &vec, ObIJsonBase *value)
{
  bool is_found = false;
  is_found = std::binary_search(vec.begin(), vec.end(),
                                value, Array_less());
  return is_found;
}

int ObJsonHashValue::calc_time(ObDTMode dt_mode, const ObIJsonBase *jb)
{
  INIT_SUCC(ret);

  ObTime time_val;
  if (OB_FAIL(jb->get_obtime(time_val))) {
    LOG_WARN("fail to get json obtime", K(ret));
  } else {
    int64_t int_time = ObTimeConverter::ob_time_to_int(time_val, dt_mode);
    calc_int64(int_time);
  }

  return ret;
}

JsonObjectIterator::JsonObjectIterator(const ObIJsonBase *json_doc)
    : curr_element_(0),
      json_object_(json_doc)
{
  // only object json_docs can construct JsonObjectIterator
  element_count_ = json_object_->element_count();
}


bool JsonObjectIterator::end() const
{
  return curr_element_ >= element_count_;
}

int JsonObjectIterator::get_elem(ObJsonObjPair &elem)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_key(curr_element_, elem.first))) {
    LOG_WARN("failed to get key", K(ret), K(curr_element_));
  } else if (OB_FAIL(json_object_->get_object_value(curr_element_, elem.second))) {
    LOG_WARN("fail to get this json obj element", K(ret), K(curr_element_));
  }
  return ret;
}

int JsonObjectIterator::get_key(ObString &key)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_key(curr_element_, key))) {
    LOG_WARN("failed to get key", K(ret), K(curr_element_));
  }
  return ret;
}

int JsonObjectIterator::get_value(ObIJsonBase *&value)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_object_value(curr_element_, value))) {
    LOG_WARN("failed to get key", K(ret), K(curr_element_));
  }
  return ret;
}

int JsonObjectIterator::get_value(ObString &key, ObIJsonBase *&value)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_object_value(key, value))) {
    LOG_WARN("failed to get key", K(ret), K(key));
  }
  return ret;
}

void JsonObjectIterator::next()
{
  curr_element_++;
}

} // namespace common
} // namespace oceanbase
