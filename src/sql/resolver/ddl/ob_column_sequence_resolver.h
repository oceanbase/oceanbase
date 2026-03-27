/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COLUMN_SEQUENCE_RESOLVER_
#define OCEANBASE_COLUMN_SEQUENCE_RESOLVER_ 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObColumnSequenceResolver: public ObStmtResolver
{
public:
  explicit ObColumnSequenceResolver(ObResolverParams &params);
  virtual ~ObColumnSequenceResolver();

  virtual int resolve(const ParseNode &parse_tree);

  int resolve_sequence_without_name(ObColumnSequenceStmt *&mystmt, ParseNode *&node);
private:
  // types and constants
private:
  // disallow copy
  //DISALLOW_COPY_AND_ASSIGN(ObColumnSequenceResolver);
  // function members

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_COLUMN_SEQUENCE_RESOLVER_ */
