/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_LOCATION_UTILS_RESOLVER_H
#define _OB_LOCATION_UTILS_RESOLVER_H
#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
// resolver for both SET NAMES and SET CHARSET
class ObLocationUtilsResolver : public ObCMDResolver
{
public:
  static const int64_t LOCATION_NAME = 0;
  static const int64_t SUB_PATH = 1;
  static const int64_t PATTERN = 2;
  explicit ObLocationUtilsResolver(ObResolverParams &params);
  virtual ~ObLocationUtilsResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLocationUtilsResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OB_LOCATION_UTILS_RESOLVER_H
