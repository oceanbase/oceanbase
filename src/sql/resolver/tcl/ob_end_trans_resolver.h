/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_TCL_END_TRANS_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_TCL_END_TRANS_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/tcl/ob_tcl_resolver.h"
#include "sql/resolver/tcl/ob_end_trans_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObEndTransResolver : public ObTCLResolver
{
public:
  explicit ObEndTransResolver(ObResolverParams &params);
  virtual ~ObEndTransResolver();

  virtual int resolve(const ParseNode &parse_node);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObEndTransResolver);
};
}
}
#endif /* OCEANBASE_SQL_RESOLVER_TCL_END_TRANS_RESOLVER_ */
//// end of header file

