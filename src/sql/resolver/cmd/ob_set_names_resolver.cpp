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

#include "sql/resolver/cmd/ob_set_names_resolver.h"
#include "sql/resolver/cmd/ob_set_names_stmt.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{
ObSetNamesResolver::ObSetNamesResolver(ObResolverParams &params)
    :ObCMDResolver(params)
{}

ObSetNamesResolver::~ObSetNamesResolver()
{}

int ObSetNamesResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (T_SET_NAMES != parse_tree.type_ && T_SET_CHARSET != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "create stmt failed", K(ret));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "parse_tree.children_ is null.", K(ret));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "parse_tree.children_[0] is null.", K(ret));
  } else {
    ObSetNamesStmt *stmt = NULL;
    if (OB_ISNULL(stmt = create_stmt<ObSetNamesStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "create stmt failed", K(ret));
    } else {
      if (T_SET_NAMES == parse_tree.type_) {
        // SET NAMES
        stmt->set_is_set_names(true);
        if (T_DEFAULT == parse_tree.children_[0]->type_) {
          stmt->set_is_default_charset(true);
        } else {
          ObString charset;
          charset.assign_ptr(parse_tree.children_[0]->str_value_,
                             static_cast<int32_t>(parse_tree.children_[0]->str_len_));
          // 目前支持gbk，utf16和utf8mb4，只有set names utf16不支持
          // 如果后续支持更多的字符集，这里需要考虑怎么实现形式更好，
          // 最好使用函数，目前没有必要
          if (0 == charset.case_compare("utf16")) {
            ret = OB_ERR_WRONG_VALUE_FOR_VAR;
            LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
                static_cast<int>(strlen("character_set_client")), "character_set_client",
                charset.length(), charset.ptr());
          } else {
            stmt->set_charset(charset);
          }
        }
        if (OB_SUCC(ret)) {
          if (NULL == parse_tree.children_[1]) {
            // do nothing
          } else if (T_DEFAULT == parse_tree.children_[1]->type_) {
            stmt->set_is_default_collation(true);
          } else {
            ObString collation;
            collation.assign_ptr(parse_tree.children_[1]->str_value_,
                static_cast<int32_t>(parse_tree.children_[1]->str_len_));
            stmt->set_collation(collation);
          }
        }
      } else {
        // SET CHARACTER SET
        stmt->set_is_set_names(false);
        if (T_DEFAULT == parse_tree.children_[0]->type_) {
          stmt->set_is_default_charset(true);
        } else {
          ObString charset;
          charset.assign_ptr(parse_tree.children_[0]->str_value_, static_cast<int32_t>(parse_tree.children_[0]->str_len_));
          stmt->set_charset(charset);
        }
      }
    }
  }
  return ret;
}
} // sql
} // oceanbase
