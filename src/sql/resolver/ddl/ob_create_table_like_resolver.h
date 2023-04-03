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

#ifndef OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_RESOLVER_
#define OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_RESOLVER_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTableLikeResolver : public ObDDLResolver
{
public:
  explicit ObCreateTableLikeResolver(ObResolverParams &params);
  virtual ~ObCreateTableLikeResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObCreateTableLikeStmt *get_create_table_like_stmt()
  {
    return static_cast<ObCreateTableLikeStmt*>(stmt_);
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableLikeResolver);
};


} //end namespace sql
} //end namespace oceanbase

#endif // OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_RESOLVER_

