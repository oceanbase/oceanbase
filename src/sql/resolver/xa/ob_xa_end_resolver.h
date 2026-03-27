/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_XA_END_RESOLVER_H_
#define _OB_XA_END_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObXaEndResolver : public ObStmtResolver
{
public:
  explicit ObXaEndResolver(ObResolverParams &params);
  virtual ~ObXaEndResolver();
  virtual int resolve(const ParseNode &parse_node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaEndResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif
