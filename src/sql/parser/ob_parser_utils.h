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

#ifndef _OB_PARSER_UTILS_H
#define _OB_PARSER_UTILS_H 1
#include "lib/utility/ob_print_utils.h"
#include "sql/parser/parse_node.h"
#include "lib/ob_name_def.h"
#include "common/ob_smart_call.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "share/ob_errno.h"
const char* get_type_name(int type);

namespace oceanbase
{
namespace sql
{
inline void databuff_print_stmt_location(char *buf, int64_t buf_len, int64_t &pos, const ObStmtLoc &obj)
{
  if (obj.first_column_ > 0 || obj.last_column_ > 0 || obj.first_line_ > 0 || obj.last_line_ > 0) {
    J_COMMA();
    int first_line = obj.first_line_;
    int last_line = obj.last_line_;
    int first_column = obj.first_column_;
    int last_column = obj.last_column_;
    J_KV(K(first_line),
        K(last_line),
        K(first_column),
        K(last_column));
  }
}

inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const ParseNode &obj)
{
  int ret  = OB_SUCCESS;
  J_OBJ_START();
  J_KV(N_TYPE, get_type_name(obj.type_),
       N_INT_VALUE, obj.value_,
       N_STR_VALUE_LEN, obj.str_len_,
       N_STR_VALUE, common::ObString(obj.str_len_, obj.str_value_));
  databuff_print_stmt_location(buf, buf_len, pos, obj.stmt_loc_);
  if (0 < obj.num_child_) {
    J_COMMA();
    J_NAME(N_CHILDREN);
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; i < obj.num_child_; ++i) {
      if (NULL == obj.children_[i]) {
        J_EMPTY_OBJ();
      } else if (OB_FAIL(SMART_CALL(databuff_print_obj(buf, buf_len, pos, *obj.children_[i])))) {
      }
      if (i != obj.num_child_ - 1) { common::databuff_printf(buf, buf_len, pos, ", "); }
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return ret;
}

inline void print_indent(char *buf, const int64_t buf_len, int64_t &pos, const int level)
{
  for (int i=0; i<level; ++i) {
    common::databuff_printf(buf, buf_len, pos, "\t");
  }
}

inline void databuff_simple_print_obj(char *buf, const int64_t buf_len, const int64_t index, int64_t &pos, const ParseNode *obj, int level)
{
  print_indent(buf, buf_len, pos, level);
  if (NULL != obj) {
    common::databuff_printf(buf,
                            buf_len, pos,
                            "|--[%ld],[%s], str_value_=[%.*s], value=[%ld]\n",
                            index,
                            get_type_name(obj->type_),
                            int32_t(obj->str_len_),
                            obj->str_value_,
                            obj->value_);
  } else {
    common::databuff_printf(buf, buf_len, pos, "|--[%ld]\n", index);
  }
  for (int64_t i = 0; i < obj->num_child_; ++i) {
    if (NULL != obj->children_[i]) {
      databuff_simple_print_obj(buf, buf_len, i, pos, obj->children_[i], level + 1);
    }
  }
}

class ObParserResultTreePrintWrapper
{
public:
  explicit ObParserResultTreePrintWrapper(const ParseNode &parse_tree)
      :parse_tree_(parse_tree)
  {}
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "\n");
    databuff_simple_print_obj(buf, buf_len, 0, pos, &parse_tree_, 0);
    return pos;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObParserResultTreePrintWrapper);
private:
  const ParseNode &parse_tree_;
};

class ObParserResultPrintWrapper
{
public:
  explicit ObParserResultPrintWrapper(const ParseNode &parse_tree)
      :parse_tree_(parse_tree)
  {}
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(SMART_CALL(databuff_print_obj(buf, buf_len, pos, parse_tree_)))) {
      pos = 0;
      J_OBJ_START();
      J_KV(N_TYPE, get_type_name(parse_tree_.type_),
          N_INT_VALUE, parse_tree_.value_,
          N_STR_VALUE_LEN, parse_tree_.str_len_,
          N_STR_VALUE, common::ObString(parse_tree_.str_len_, parse_tree_.str_value_));
      databuff_print_stmt_location(buf, buf_len, pos, parse_tree_.stmt_loc_);
      J_OBJ_END();
    }
    return pos;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObParserResultPrintWrapper);
private:
  const ParseNode &parse_tree_;
};

struct ObCharsets4Parser
{
  explicit ObCharsets4Parser() :
      string_collation_(common::CS_TYPE_UTF8MB4_GENERAL_CI),
      nls_collation_(common::CS_TYPE_INVALID)
    {}
  common::ObCollationType string_collation_; //collation type for the string to parse
  common::ObCollationType nls_collation_; //oracle database collation for validating identifiers
};

} // end namespace sql
} // end namespace oceanbase


#endif /* _OB_PARSER_UTILS_H */
