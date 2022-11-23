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

#ifndef OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_outline_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObCreateOutlineStmt;
class ObCreateOutlineResolver : public ObOutlineResolver
{
public:
  explicit ObCreateOutlineResolver(ObResolverParams &params) : ObOutlineResolver(params) {}
  virtual ~ObCreateOutlineResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_sql_id(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt);
  int resolve_hint(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt);
  static const int64_t OUTLINE_CHILD_COUNT = 5;
  DISALLOW_COPY_AND_ASSIGN(ObCreateOutlineResolver);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_
