/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DROP_MLOG_RESOLVER_H_
#define OB_DROP_MLOG_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropMLogResolver : public ObDDLResolver
{
public:
  explicit ObDropMLogResolver(ObResolverParams &params);
  virtual ~ObDropMLogResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  enum ParameterEnum {
    ENUM_TABLE_NAME = 0,
    ENUM_TOTAL_COUNT
  };
  DISALLOW_COPY_AND_ASSIGN(ObDropMLogResolver);
};
} // sql
} // oceanbase

#endif // OB_DROP_MLOG_RESOLVER_H_