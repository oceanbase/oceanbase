/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_DROP_SEQUENCE_RESOLVER_
#define OCEANBASE_DROP_SEQUENCE_RESOLVER_ 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropSequenceResolver: public ObStmtResolver
{
public:
  explicit ObDropSequenceResolver(ObResolverParams &params);
  virtual ~ObDropSequenceResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropSequenceResolver);
  // function members

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_DROP_SEQUENCE_RESOLVER_ */
