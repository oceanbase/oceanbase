/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_XA_START_RESOLVER_
#define _OB_XA_START_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObXaStartResolver : public ObStmtResolver
{
public:
  explicit ObXaStartResolver(ObResolverParams &params);
  virtual ~ObXaStartResolver();

  virtual int resolve(const ParseNode &parse_node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaStartResolver);
};

} // end sql namespace
} // end oceanbase namspace

#endif /* OCEANBASE_RESOLVER_XA_START_RESOLVER_ */
