/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_CCL_RULE_RESOLVER_H_
#define OCEANBASE_SQL_OB_DROP_CCL_RULE_RESOLVER_H_
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropCCLRuleResolver: public ObDDLResolver
{
  static const int64_t IF_EXIST = 0;
  static const int64_t CCL_RULE_NAME = 1;
public:
  explicit ObDropCCLRuleResolver(ObResolverParams &params);
  virtual ~ObDropCCLRuleResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropCCLRuleResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_DROP_CCL_RULE_RESOLVER_H_*/
