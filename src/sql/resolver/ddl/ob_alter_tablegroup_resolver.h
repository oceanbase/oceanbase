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

#ifndef OCEANBASE_SQL_OB_ALTER_TABLEGROUP_RESOLVER_
#define OCEANBASE_SQL_OB_ALTER_TABLEGROUP_RESOLVER_

#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAlterTablegroupResolver : public ObTableGroupResolver
{
  static const int TG_NAME = 0;
  static const int TABLE_LIST = 1;
public:
  explicit ObAlterTablegroupResolver(ObResolverParams &params);
  virtual ~ObAlterTablegroupResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObAlterTablegroupStmt *get_alter_tablegroup_stmt() { return static_cast<ObAlterTablegroupStmt*>(stmt_); };

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTablegroupResolver);
};


} //end namespace sql
} //end namespace oceanbase

#endif // OCEANBASE_SQL_OB_ALTER_TABLEGROUP_RESOLVER_
