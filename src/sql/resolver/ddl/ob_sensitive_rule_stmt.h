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

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "share/ob_rpc_struct.h"

#ifndef OCEANBASE_SQL_OB_SENSITIVE_RULE_STMT_H_
#define OCEANBASE_SQL_OB_SENSITIVE_RULE_STMT_H_

namespace oceanbase
{
namespace sql
{
// used for privilege check
struct ObSensitiveFieldItemWithName {
  ObString db_name_;
  ObString table_name_;
  ObArray<ObString> column_names_;

  ObSensitiveFieldItemWithName(): db_name_(), table_name_(), column_names_() {};

  TO_STRING_KV(K_(db_name), K_(table_name), K_(column_names));
};

class ObSensitiveRuleStmt : public ObDDLStmt
{
public:
  ObSensitiveRuleStmt();
  virtual ~ObSensitiveRuleStmt();
  virtual obrpc::ObSensitiveRuleDDLArg &get_ddl_arg() { return arg_; }
public:
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { arg_.schema_.set_tenant_id(tenant_id); }
  OB_INLINE void set_user_id(const uint64_t user_id) { arg_.user_id_ = user_id; }
  OB_INLINE void set_ddl_type(const share::schema::ObSchemaOperationType ddl_type) { arg_.ddl_type_ = ddl_type; }
  OB_INLINE void set_ddl_stmt_str(const common::ObString &ddl_stmt_str) { arg_.ddl_stmt_str_ = ddl_stmt_str; }
  OB_INLINE void set_protection_policy(const obrpc::ObSensitiveRuleDDLArg::ObSensitiveProtectionPolicy policy) { arg_.schema_.set_protection_policy(policy); }
  OB_INLINE void set_schema_version(const int64_t schema_version) { arg_.schema_.set_schema_version(schema_version); }
  OB_INLINE void set_enabled(const bool enabled) { arg_.schema_.set_enabled(enabled); }
  OB_INLINE int set_sensitive_rule_name(const common::ObString &rule_name)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(arg_.schema_.set_sensitive_rule_name(rule_name))) {
      LOG_WARN("fail to set rule name", K(ret), K(rule_name));
    }
    return ret;
  }
  OB_INLINE int set_method(const common::ObString &method)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(arg_.schema_.set_method(method))) {
      LOG_WARN("fail to set rule name", K(ret), K(method));
    }
    return ret;
  }
  OB_INLINE int add_sensitive_field_item(const uint64_t table_id, const uint64_t column_id)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(arg_.schema_.add_sensitive_field_item(table_id, column_id))) {
      LOG_WARN("fail to push back sensitive field item", K(ret));
    }
    return ret;
  }
  OB_INLINE bool is_duplicate_sensitive_field_item(const uint64_t table_id, const uint64_t column_id)
  {
    return has_exist_in_array(arg_.schema_.get_sensitive_field_items(), ObSensitiveFieldItem(table_id, column_id));
  }
  OB_INLINE int add_sensitive_field_item_with_name(const ObSensitiveFieldItemWithName & field)
  {
    return sensitive_field_item_names_.push_back(field);
  }
  OB_INLINE const ObIArray<ObSensitiveFieldItemWithName> &get_sensitive_field_item_names() const { return sensitive_field_item_names_; }
  OB_INLINE obrpc::ObSensitiveRuleDDLArg &get_arg() { return arg_; }

private:
  obrpc::ObSensitiveRuleDDLArg arg_;
  ObArray<ObSensitiveFieldItemWithName> sensitive_field_item_names_;
  DISALLOW_COPY_AND_ASSIGN(ObSensitiveRuleStmt);
};
}
}

#endif // OCEANBASE_SQL_OB_SENSITIVE_RULE_STMT_H_