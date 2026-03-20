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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_SET_PROTECTION_MODE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_SET_PROTECTION_MODE_RESOLVER_H_
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObSetProtectionModeResolver : public ObSystemCmdResolver
{
public:
  ObSetProtectionModeResolver(ObResolverParams &params) : ObSystemCmdResolver(params) {}
  virtual ~ObSetProtectionModeResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_SET_PROTECTION_MODE_RESOLVER_H_