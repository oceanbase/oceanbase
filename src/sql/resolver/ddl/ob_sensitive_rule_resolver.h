/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_SENSITIVE_RULE_RESOLVER_H_
#define OCEANBASE_SQL_OB_SENSITIVE_RULE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_sensitive_rule_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObSensitiveRuleResolver : public ObDDLResolver
{
public:
  explicit ObSensitiveRuleResolver(ObResolverParams &params);
  virtual ~ObSensitiveRuleResolver();
  virtual int resolve(const ParseNode &parse_tree) = 0;
protected:
  int resolve_protection_spec(const ParseNode *protection_spec_node, ObSensitiveRuleStmt &stmt);
  int resolve_rule_name(const ParseNode *rule_name_node, ObSensitiveRuleStmt &stmt);
  int resolve_sensitive_field(const ParseNode *sensitive_field_list_node, ObSensitiveRuleStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSensitiveRuleResolver);
};

class ObCreateSensitiveRuleResolver : public ObSensitiveRuleResolver
{
public:
  explicit ObCreateSensitiveRuleResolver(ObResolverParams &params);
  virtual ~ObCreateSensitiveRuleResolver();
  int resolve(const ParseNode &parse_tree) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateSensitiveRuleResolver);
};

class ObDropSensitiveRuleResolver : public ObSensitiveRuleResolver
{
public:
  explicit ObDropSensitiveRuleResolver(ObResolverParams &params);
  virtual ~ObDropSensitiveRuleResolver();
  int resolve(const ParseNode &parse_tree) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropSensitiveRuleResolver);
};

class ObAlterSensitiveRuleResolver : public ObSensitiveRuleResolver
{
public:
  explicit ObAlterSensitiveRuleResolver(ObResolverParams &params);
  virtual ~ObAlterSensitiveRuleResolver();
  int resolve(const ParseNode &parse_tree) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterSensitiveRuleResolver);
};
}
}

#endif // OCEANBASE_SQL_OB_SENSITIVE_RULE_RESOLVER_H_