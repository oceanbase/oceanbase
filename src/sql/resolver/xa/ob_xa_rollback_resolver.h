/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_XA_ROLLBACK_RESOLVER_H_
#define _OB_XA_ROLLBACK_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObXaRollBackResolver : public ObStmtResolver
{
public:
  explicit ObXaRollBackResolver(ObResolverParams &params);
  virtual ~ObXaRollBackResolver();
  virtual int resolve(const ParseNode &parse_node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaRollBackResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif
