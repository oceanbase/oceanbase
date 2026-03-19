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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_ALTER_VIEW_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_ALTER_VIEW_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_alter_view_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterViewResolver : public ObDDLResolver
{
  static const int64_t IF_EXISTS = 0;      // if_exists node (can be NULL)
  static const int64_t VIEW_NAME = 1;      // view_name node
  static const int64_t ALTER_ACTION = 2;   // alter action node (T_ALTER_VIEW_COMPILE, etc.)
public:
  explicit ObAlterViewResolver(ObResolverParams &params);
  virtual ~ObAlterViewResolver();

  virtual int resolve(const ParseNode &parse_tree);

private:
  int resolve_alter_view(const ParseNode &parse_tree);

  DISALLOW_COPY_AND_ASSIGN(ObAlterViewResolver);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_ALTER_VIEW_RESOLVER_H_
