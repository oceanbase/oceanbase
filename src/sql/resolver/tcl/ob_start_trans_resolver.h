/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RESOLVER_TCL_START_TRANS_RESOLVER_
#define OCEANBASE_RESOLVER_TCL_START_TRANS_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/tcl/ob_tcl_resolver.h"
#include "sql/resolver/tcl/ob_start_trans_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObStartTransResolver : public ObTCLResolver
{
public:
  explicit ObStartTransResolver(ObResolverParams &params);
  virtual ~ObStartTransResolver();

  virtual int resolve(const ParseNode &parse_node);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObStartTransResolver);
};
}
}
#endif /* OCEANBASE_RESOLVER_TCL_START_TRANS_RESOLVER_ */
//// end of header file

