/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_SYNONYM_RESOLVER_H_
#define OCEANBASE_SQL_OB_DROP_SYNONYM_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObDropSynonymResolver : public ObDDLResolver
{
public:
  explicit ObDropSynonymResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObDropSynonymResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int64_t SYNONYM_CHILD_COUNT = 4;
  DISALLOW_COPY_AND_ASSIGN(ObDropSynonymResolver);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_DROP_SYNONYM_RESOLVER_H_
