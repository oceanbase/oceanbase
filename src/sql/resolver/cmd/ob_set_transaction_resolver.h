/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_SET_TRANSACTION_RESOLVER_H_
#define OCEANBASE_SQL_OB_SET_TRANSACTION_RESOLVER_H_
#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObSetTransactionResolver : public ObCMDResolver
{
public:
  explicit ObSetTransactionResolver(ObResolverParams &params);
  virtual ~ObSetTransactionResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int build_isolation_expr(ObRawExpr *&expr, int32_t level);
  int build_access_expr(ObRawExpr *&expr, const bool is_read_only);
  int scope_resolve(const ParseNode &parse_tree, share::ObSetVar::SetScopeType &scope);
  int access_mode_resolve(const ParseNode &parse_tree, bool &is_read_only);
  int transaction_characteristics_resolve(const ParseNode &parse_tree,
                                          bool &is_read_only,
                                          int32_t &level);
  int isolation_level_resolve(const ParseNode &parse_tree, int32_t &level);
  DISALLOW_COPY_AND_ASSIGN(ObSetTransactionResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SET_TRANSACTION_RESOLVER_H_*/
