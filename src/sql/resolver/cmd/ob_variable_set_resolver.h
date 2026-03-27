/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
