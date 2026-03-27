/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_XA_PREPARE_RESOLVER_H_
#define _OB_XA_PREPARE_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObXaPrepareResolver : public ObStmtResolver
{
public:
  explicit ObXaPrepareResolver(ObResolverParams &params);
  virtual ~ObXaPrepareResolver();
  virtual int resolve(const ParseNode &parse_node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaPrepareResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif
