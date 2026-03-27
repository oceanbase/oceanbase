/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_XA_COMMIT_RESOLVER_H_
#define _OB_XA_COMMIT_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObXaCommitResolver : public ObStmtResolver
{
public:
  explicit ObXaCommitResolver(ObResolverParams &params);
  virtual ~ObXaCommitResolver();
  virtual int resolve(const ParseNode &parse_node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaCommitResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif
