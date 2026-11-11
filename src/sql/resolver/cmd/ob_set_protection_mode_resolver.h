/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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