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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_CMD_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_CMD_RESOLVER_

#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
namespace oceanbase
{
namespace sql
{
class ObCMDResolver : public ObStmtResolver
{
public:
  explicit ObCMDResolver(ObResolverParams &params) : ObStmtResolver(params) {}
  virtual ~ObCMDResolver() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObCMDResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_CMD_RESOLVER_*/
