/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_ALTER_OUTLINE_RESOLVER_H_
#define OCEANBASE_SQL_OB_ALTER_OUTLINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_outline_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObAlterOutlineResolver : public ObOutlineResolver
{
public:
  explicit ObAlterOutlineResolver(ObResolverParams &params) : ObOutlineResolver(params) {}
  virtual ~ObAlterOutlineResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int64_t OUTLINE_CHILD_COUNT = 4;
  DISALLOW_COPY_AND_ASSIGN(ObAlterOutlineResolver);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_ALTER_OUTLINE_RESOLVER_H_
