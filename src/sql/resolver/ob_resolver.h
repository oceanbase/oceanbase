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

#ifndef OCEANBASE_SQL_RESOLVER_OB_RESOLVER
#define OCEANBASE_SQL_RESOLVER_OB_RESOLVER

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;

/// the interface of this module
class ObResolver
{
public:
  enum IsPrepared
  {
    IS_PREPARED_STMT,
    IS_NOT_PREPARED_STMT
  };

  explicit ObResolver(ObResolverParams &params);
  virtual ~ObResolver();

  virtual int resolve(IsPrepared if_prepared, const ParseNode &parse_tree, ObStmt *&stmt);
  const common::ObTimeZoneInfo *get_timezone_info();
  ObResolverParams &get_params() { return params_; }

private:
  template <typename ResolverType>
  int stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt);

  template <typename SelectResolverType>
  int select_stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt);
private:
  // data members
  ObResolverParams params_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_OB_RESOLVER */
