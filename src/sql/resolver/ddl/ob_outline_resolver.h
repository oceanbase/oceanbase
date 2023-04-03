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

#ifndef OCEANBASE_SQL_OB_OUTLINE_RESOLVER_H_
#define OCEANBASE_SQL_OB_OUTLINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObOutlineResolver : public ObDDLResolver
{
public:
  explicit ObOutlineResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObOutlineResolver() {}
protected:
  int resolve_outline_name(const ParseNode *node, common::ObString &db_name, common::ObString &outline_name);
  int resolve_outline_stmt(const ParseNode *node, ObStmt *&out_stmt, common::ObString &outline_sql);
  int resolve_outline_target(const ParseNode *target_node, common::ObString &outline_target);
  static const int64_t RELATION_FACTOR_CHILD_COUNT = 2;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOutlineResolver);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_OUTLINE_RESOLVER_H_
