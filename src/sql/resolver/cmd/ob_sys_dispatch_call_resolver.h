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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_H_
#define OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_H_

#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObSysDispatchCallResolver final : public ObCMDResolver
{
public:
  explicit ObSysDispatchCallResolver(ObResolverParams &params) : ObCMDResolver(params) {}
  virtual ~ObSysDispatchCallResolver() {}
  DISABLE_COPY_ASSIGN(ObSysDispatchCallResolver);

  virtual int resolve(const ParseNode &parse_tree);

private:
  int check_sys_dispatch_call_priv(const ParseNode &name_node);
  int check_supported_cluster_version() const;

  static const char *const WHITELIST[][2];
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_H_
