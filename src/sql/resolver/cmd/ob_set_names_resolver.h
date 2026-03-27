/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SET_NAMES_RESOLVER_H
#define _OB_SET_NAMES_RESOLVER_H
#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
// resolver for both SET NAMES and SET CHARSET
class ObSetNamesResolver : public ObCMDResolver
{
public:
  explicit ObSetNamesResolver(ObResolverParams &params);
  virtual ~ObSetNamesResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetNamesResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OB_SET_NAMES_RESOLVER_H
