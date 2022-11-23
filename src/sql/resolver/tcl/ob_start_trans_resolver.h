/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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

