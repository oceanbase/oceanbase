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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_DEALLOCATE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_DEALLOCATE_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDeallocateResolver : public ObStmtResolver
{
public:
  explicit ObDeallocateResolver(ObResolverParams &params) : ObStmtResolver(params){}
  virtual ~ObDeallocateResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
  ObDeallocateStmt *get_deallocate_stmt() { return static_cast<ObDeallocateStmt*>(stmt_); }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeallocateResolver);
};

} // namespace sql
}  // namespace oceanbase

#endif /*OCEANBASE_SQL_RESOLVER_DML_OB_DEALLOCATE_RESOLVER_H_*/
