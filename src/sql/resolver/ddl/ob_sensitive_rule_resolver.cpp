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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_sensitive_rule_resolver.h"

namespace oceanbase
{
namespace sql
{
ObSensitiveRuleResolver::ObSensitiveRuleResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObSensitiveRuleResolver::~ObSensitiveRuleResolver()
{
}

#ifdef OB_BUILD_TDE_SECURITY
int ObSensitiveRuleResolver::resolve_rule_name(const ParseNode *rule_name_node,
                                               ObSensitiveRuleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString rule_name;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(rule_name_node) || rule_name_node->type_ != T_IDENT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sensitive rule name node", K(ret), K(rule_name_node));
  } else if (FALSE_IT(rule_name.assign_ptr(rule_name_node->str_value_, static_cast<int32_t>(rule_name_node->str_len_)))) {
  } else if (rule_name.empty() || 0 == rule_name.length()) {
    ret = OB_WRONG_SENSITIVE_RULE_NAME;
    LOG_WARN("invalid sensitive rule name", K(ret), K(rule_name));
    LOG_USER_ERROR(OB_WRONG_SENSITIVE_RULE_NAME, rule_name.length(), rule_name.ptr());
  } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_,
                                                                        session_info_->get_dtc_params(),
                                                                        rule_name))) {
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (is_mysql_mode() && OB_LOWERCASE_AND_INSENSITIVE == case_mode
             && OB_FAIL(ObCharset::tolower(cs_type, rule_name, rule_name, *allocator_))) {
    LOG_WARN("failed to lower string", K(ret));
  }

  if (OB_SUCC(ret)) {
    stmt.set_sensitive_rule_name(rule_name);
  }
  return ret;
}

int ObSensitiveRuleResolver::resolve_protection_spec(const ParseNode *protection_spec_node,
                                                     ObSensitiveRuleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ParseNode *rule_policy_node = NULL;
  ParseNode *method_node = NULL;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;
  if (OB_ISNULL(protection_spec_node)
      || OB_UNLIKELY(protection_spec_node->type_ != T_SENSITIVE_PROTECTION_SPEC)
      || OB_UNLIKELY(protection_spec_node->num_child_ != 1)
      || OB_ISNULL(protection_spec_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid protection spec node", K(ret), K(protection_spec_node));
  } else if (OB_ISNULL(rule_policy_node = protection_spec_node->children_[0])
             || OB_UNLIKELY(rule_policy_node->num_child_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rule policy node", K(ret), K(rule_policy_node));
  } else {
    switch (rule_policy_node->type_) {
      case T_SENSITIVE_ENCRYPTION_SPEC:
        stmt.set_protection_policy(ObSensitiveRuleDDLArg::SENSITIVE_PROTECTION_POLICY_ENCRYPTION);
        if (FALSE_IT(method_node = rule_policy_node->children_[0])) {
        } else if (NULL == method_node) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL encryption method", K(ret));
        } else if (T_DEFAULT == method_node->type_) {
          if (OB_FAIL(stmt.set_method("aes-256"))) {
            LOG_WARN("fail to set default method", K(ret));
          }
        } else {
          int64_t encryption_mode = -1;
          ObString method_str;
          bool need_encryption = false;
          if (OB_UNLIKELY(method_node->type_ != T_VARCHAR && method_node->type_ != T_CHAR)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid sensitive rule method node", K(ret), K(method_node));
          } else if (FALSE_IT(method_str.assign_ptr(method_node->str_value_, static_cast<int32_t>(method_node->str_len_)))) {
          } else if (OB_FAIL(ObResolverUtils::check_encryption_name(method_str, need_encryption))) {
            LOG_WARN("invalid encryption method", K(ret), K(method_str));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sensitive rule encryption method");
          } else if (OB_FAIL(ObEncryptionUtil::parse_encryption_id(method_str, encryption_mode))) {
            LOG_WARN("fail to parse encryption id", K(ret), K(method_str));
          } else if (OB_FAIL(ObCharset::tolower(cs_type, method_str, method_str, *allocator_))) {
            LOG_WARN("failed to lower string", K(ret));
          } else if (OB_FAIL(stmt.set_method(method_str))) {
            LOG_WARN("fail to set method", K(ret), K(method_str));
          }
        }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported sensitive rule policy type", K(ret), K(rule_policy_node->type_));
    }
  }
  return ret;
}

int ObSensitiveRuleResolver::resolve_sensitive_field(const ParseNode *sensitive_field_list_node,
                                                     ObSensitiveRuleStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObSensitiveRuleDDLArg &arg = stmt.get_ddl_arg();
  if (OB_ISNULL(sensitive_field_list_node)
      || OB_UNLIKELY(sensitive_field_list_node->type_ != T_SENSITIVE_FIELD_LIST)
      || OB_UNLIKELY(sensitive_field_list_node->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sensitive field list node", K(ret), K(sensitive_field_list_node));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_field_list_node->num_child_; ++i) {
    ParseNode *sensitive_field_node = sensitive_field_list_node->children_[i];
    ObString database_name;
    ObString table_name;
    const ObTableSchema *tbl_schema = NULL;
    ParseNode *column_list_node = NULL;
    ObSensitiveFieldItemWithName sensitive_field_item_with_name; // used for privilege check
    if (OB_ISNULL(sensitive_field_node)
        || OB_UNLIKELY(sensitive_field_node->num_child_ != 2)
        || OB_ISNULL(sensitive_field_node->children_[0])
        || OB_ISNULL(column_list_node = sensitive_field_node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sensitive field node", K(ret), K(sensitive_field_node));
    } else if (OB_FAIL(resolve_table_relation_node(sensitive_field_node->children_[0],
                                                   table_name, database_name))) {
      LOG_WARN("fail to resolve table name", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                         database_name,
                                                         table_name,
                                                         false, // not index table
                                                         tbl_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(database_name), K(table_name));
    } else if (OB_ISNULL(tbl_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(database_name), K(table_name));
    } else if (OB_FAIL(stmt.get_ddl_arg().based_schema_object_infos_.push_back(
                ObBasedSchemaObjectInfo(tbl_schema->get_table_id(),
                                        TABLE_SCHEMA,
                                        tbl_schema->get_schema_version())))) {
      LOG_WARN("push back based schema object info failed", K(ret));
    } else {
      sensitive_field_item_with_name.db_name_ = database_name;
      sensitive_field_item_with_name.table_name_ = table_name;
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_list_node->num_child_; ++j) {
      ParseNode *col_node = column_list_node->children_[j];
      const ObColumnSchemaV2 *column_schema = NULL;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      if (OB_ISNULL(col_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column node", K(ret), K(col_node));
      } else if (OB_ISNULL(column_schema = tbl_schema->get_column_schema(col_node->str_value_))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("column schema is null", K(ret), K(col_node->str_value_));
      } else if (FALSE_IT(table_id = column_schema->get_table_id())) {
      } else if (FALSE_IT(column_id = column_schema->get_column_id())) {
      } else if (stmt.is_duplicate_sensitive_field_item(table_id, column_id)) {
        ret = OB_ERR_DUPLICATE_COLUMN_EXPRESSION_WAS_SPECIFIED;
        LOG_WARN("identical column appears in sensitive field", K(ret), K(table_id), K(column_id));
        LOG_USER_ERROR(OB_ERR_DUPLICATE_COLUMN_EXPRESSION_WAS_SPECIFIED);
      } else if (OB_FAIL(stmt.add_sensitive_field_item(table_id, column_id))) {
        LOG_WARN("fail to add sensitive field item", K(ret));
      } else if (OB_FAIL(sensitive_field_item_with_name.column_names_.push_back(column_schema->get_column_name()))) {
        LOG_WARN("fail to add column name", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(stmt.add_sensitive_field_item_with_name(sensitive_field_item_with_name))) {
      LOG_WARN("fail to add sensitive field item with name", K(ret));
    }
  }
  return ret;
}
#else
#endif // OB_BUILD_TDE_SECURITY

ObCreateSensitiveRuleResolver::ObCreateSensitiveRuleResolver(ObResolverParams &params)
  : ObSensitiveRuleResolver(params)
{
}

ObCreateSensitiveRuleResolver::~ObCreateSensitiveRuleResolver()
{
}

#ifdef OB_BUILD_TDE_SECURITY
int ObCreateSensitiveRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleStmt *stmt = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(parse_tree.type_ != T_CREATE_SENSITIVE_RULE)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if ((lib::is_mysql_mode() && !((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0) ||
                                        (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
                                        (data_version >= DATA_VERSION_4_5_1_0)))
             || (lib::is_oracle_mode() && !((data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
                                            (data_version >= DATA_VERSION_4_5_1_0)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sensitive rule not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  } else if (OB_ISNULL(stmt = create_stmt<ObSensitiveRuleStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create stmt", K(ret));
  } else if (FALSE_IT(stmt_ = stmt)) {  // never reached
  } else if (FALSE_IT(stmt->set_stmt_type(stmt::T_CREATE_SENSITIVE_RULE))) {
  } else if (FALSE_IT(stmt->set_tenant_id(tenant_id))) {
  } else if (FALSE_IT(stmt->set_user_id(session_info_->get_user_id()))) {
  } else if (FALSE_IT(stmt->set_ddl_type(OB_DDL_CREATE_SENSITIVE_RULE))) {
  } else if (FALSE_IT(stmt->set_enabled(true))) {
  } else if (OB_FAIL(resolve_rule_name(parse_tree.children_[0], *stmt))) {
    LOG_WARN("fail to resolve rule name", K(ret));
  } else if (OB_FAIL(resolve_protection_spec(parse_tree.children_[2], *stmt))) {
    LOG_WARN("fail to resolve protection policy", K(ret));
  } else if (OB_FAIL(resolve_sensitive_field(parse_tree.children_[1], *stmt))) {
    LOG_WARN("fail to resolve sensitive field", K(ret));
  }

  // Check oracle privileges
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    CK (schema_checker_ != NULL);
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            ObString(),
                                            stmt->get_stmt_type(),
                                            session_info_->get_enable_role_array()));
  }

  return ret;
}
#else
int ObCreateSensitiveRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("sensitive rule not supported", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  return ret;
}
#endif

ObDropSensitiveRuleResolver::ObDropSensitiveRuleResolver(ObResolverParams &params)
  : ObSensitiveRuleResolver(params)
{
}

ObDropSensitiveRuleResolver::~ObDropSensitiveRuleResolver()
{
}

#ifdef OB_BUILD_TDE_SECURITY
int ObDropSensitiveRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleStmt *stmt = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(parse_tree.type_ != T_DROP_SENSITIVE_RULE)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if ((lib::is_mysql_mode() && !((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0) ||
                                        (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
               (data_version >= DATA_VERSION_4_5_1_0)))
             || (lib::is_oracle_mode() && !(data_version >= MOCK_DATA_VERSION_4_4_2_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sensitive rule not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  } else if (OB_ISNULL(stmt = create_stmt<ObSensitiveRuleStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create stmt", K(ret));
  } else if (FALSE_IT(stmt_ = stmt)) {  // never reached
  } else if (FALSE_IT(stmt->set_stmt_type(stmt::T_DROP_SENSITIVE_RULE))) {
  } else if (FALSE_IT(stmt->set_tenant_id(tenant_id))) {
  } else if (FALSE_IT(stmt->set_user_id(session_info_->get_user_id()))) {
  } else if (FALSE_IT(stmt->set_ddl_type(OB_DDL_DROP_SENSITIVE_RULE))) {
  } else if (OB_FAIL(resolve_rule_name(parse_tree.children_[0], *stmt))) {
  }

  // Check oracle privileges
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    CK (schema_checker_ != NULL);
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            ObString(),
                                            stmt->get_stmt_type(),
                                            session_info_->get_enable_role_array()));
  }

  return ret;
}
#else
int ObDropSensitiveRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("sensitive rule not supported", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  return ret;
}
#endif // OB_BUILD_TDE_SECURITY

ObAlterSensitiveRuleResolver::ObAlterSensitiveRuleResolver(ObResolverParams &params)
  : ObSensitiveRuleResolver(params)
{
}

ObAlterSensitiveRuleResolver::~ObAlterSensitiveRuleResolver()
{
}

#ifdef OB_BUILD_TDE_SECURITY
int ObAlterSensitiveRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleStmt *stmt = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(parse_tree.type_ != T_ALTER_SENSITIVE_RULE)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != 2)
      || OB_ISNULL(parse_tree.children_[0])
      || OB_ISNULL(parse_tree.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if ((lib::is_mysql_mode() && !((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0) ||
                                        (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
                                        (data_version >= DATA_VERSION_4_5_1_0)))
             || (lib::is_oracle_mode() && !((data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
                                            (data_version >= DATA_VERSION_4_5_1_0)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sensitive rule not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  } else if (OB_ISNULL(stmt = create_stmt<ObSensitiveRuleStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create stmt", K(ret));
  } else if (FALSE_IT(stmt_ = stmt)) {  // never reached
  } else if (FALSE_IT(stmt->set_tenant_id(tenant_id))) {
  } else if (FALSE_IT(stmt->set_user_id(session_info_->get_user_id()))) {
  } else if (OB_FAIL(resolve_rule_name(parse_tree.children_[0], *stmt))) {
    LOG_WARN("fail to resolve rule name", K(ret));
  } else {
    switch (parse_tree.children_[1]->value_) {
      case 1: // add column
        stmt->set_ddl_type(OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN);
        if (OB_FAIL(resolve_sensitive_field(parse_tree.children_[1]->children_[0], *stmt))) {
          LOG_WARN("fail to resolve sensitive field", K(ret));
        }
        break;
      case 2: // drop column
        stmt->set_ddl_type(OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN);
        if (OB_FAIL(resolve_sensitive_field(parse_tree.children_[1]->children_[0], *stmt))) {
          LOG_WARN("fail to resolve sensitive field", K(ret));
        }
        break;
      case 3: // enable
        stmt->set_ddl_type(OB_DDL_ALTER_SENSITIVE_RULE_ENABLE);
        stmt->set_enabled(true);
        break;
      case 4: // disable
        stmt->set_ddl_type(OB_DDL_ALTER_SENSITIVE_RULE_DISABLE);
        stmt->set_enabled(false);
        break;
      case 5: // set policy
        stmt->set_ddl_type(OB_DDL_ALTER_SENSITIVE_RULE_SET_PROTECTION_SPEC);
        if (OB_FAIL(resolve_protection_spec(parse_tree.children_[1]->children_[0], *stmt))) {
          LOG_WARN("fail to resolve protection policy", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid alter sensitive rule action type", K(ret), K(parse_tree.value_));
        break;
    }
  }

  // Check oracle privileges
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    CK (schema_checker_ != NULL);
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            ObString(),
                                            stmt->get_stmt_type(),
                                            session_info_->get_enable_role_array()));
  }

  return ret;
}
#else
int ObAlterSensitiveRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("sensitive rule not supported", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  return ret;
}
#endif // OB_BUILD_TDE_SECURITY

} // end namespace sql
} // end namespace oceanbase