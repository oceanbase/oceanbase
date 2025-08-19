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

#ifndef OCEANBASE_SQL_OB_CREATE_CCL_RULE_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_CCL_RULE_RESOLVER_H_
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateCCLRuleResolver: public ObDDLResolver
{
  static const int64_t IF_NOT_EXIST = 0;
  static const int64_t CCL_RULE_NAME = 1;
  static const int64_t CCL_AFFECT_DATABASE_TABLE = 2;
  static const int64_t CCL_AFFECT_USER_HOSTNAME = 3;
  static const int64_t CCL_AFFECT_DML = 4;
  static const int64_t CCL_FILTER_OPTION = 5;
  static const int64_t CCL_WITH_OPTION = 6;
  static const int64_t CCL_AFFECT_SCOPE = 7;
public:
  explicit ObCreateCCLRuleResolver(ObResolverParams &params);
  virtual ~ObCreateCCLRuleResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  /**
   * @brief
   * 合并多个关键字，使用指定的分隔符，并对字符串内部的特殊字符进行转义。
   *
   * @param ccl_filter_option_node ParseNode
   * @param separator 用于拼接字符串的分隔符, ';'
   * @param escape_char 用于转义特殊字符的字符, '\'
   */
  int merge_strings_with_escape(const ParseNode &ccl_filter_option_node,
                                char separator, char escape_char,
                                ObString &ccl_keyword);
  /**
   * @brief 对单个字符串进行转义，处理分隔符和转义符自身。
   *
   * @param original_string 原始ObString
   * @param separator 字符串中的分隔符, ';'
   * @param escape_char 字符串中的转义符, '\'
   */
  int escape_string(const ObString &original_string, char separator,
                    char escape_char, ObString &after_escape_string);

  DISALLOW_COPY_AND_ASSIGN(ObCreateCCLRuleResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_CCL_RULE_RESOLVER_H_*/
