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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_VARIALBLE_SET_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_VARIALBLE_SET_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObVariableSetResolver : public ObStmtResolver
{
public:
  explicit ObVariableSetResolver(ObResolverParams &params);
  virtual ~ObVariableSetResolver();

  virtual int resolve(const ParseNode &parse_tree);
  int resolve_value_expr(ParseNode &val_node, ObRawExpr *&value_expr);
  int resolve_subquery_info(const ObIArray<ObSubQueryInfo> &subquery_info, ObRawExpr *&value_expr);
private:
  int resolve_set_names(const ParseNode &parse_tree);
  DISALLOW_COPY_AND_ASSIGN(ObVariableSetResolver);
};

class ObAlterSessionSetResolver : public ObStmtResolver
{
public:
  explicit ObAlterSessionSetResolver(ObResolverParams &params);
  virtual ~ObAlterSessionSetResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterSessionSetResolver);
};

}
}
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_VARIALBLE_SET_RESOLVER_ */
