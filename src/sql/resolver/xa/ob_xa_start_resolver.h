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
