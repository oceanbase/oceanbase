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

#ifndef OCEANBASE_SQL_RESOLVER_DROP_INDEX_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DROP_INDEX_RESOLVER_

#include "lib/hash/ob_placement_hashset.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropIndexResolver : public ObDDLResolver
{
public:
  explicit ObDropIndexResolver(ObResolverParams &params);
  virtual ~ObDropIndexResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropIndexResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif
