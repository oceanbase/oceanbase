/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_TABLE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_TABLE_RESOLVER_H_ 1
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropTableResolver : public ObDDLResolver
{
public:
  enum node_type {
    MATERIALIZED_NODE = 0,
    IF_EXIST_NODE,
    TABLE_LIST_NODE,
    MAX_NODE
  };
public:
  explicit ObDropTableResolver(ObResolverParams &params);
  virtual ~ObDropTableResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropTableResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_TABLE_RESOLVER_H_ */
