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
