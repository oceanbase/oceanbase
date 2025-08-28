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
#include "sql/resolver/ddl/ob_create_ccl_rule_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_ccl_rule_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObCreateCCLRuleResolver::ObCreateCCLRuleResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCreateCCLRuleResolver::~ObCreateCCLRuleResolver()
{
}

int ObCreateCCLRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateCCLRuleStmt *create_ccl_rule_stmt = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t data_version = 0;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_CREATE_CCL_RULE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(node), K(node->children_));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_3) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ccl not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ccl");
  } else {
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    bool perserve_lettercase = false;
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
      LOG_WARN("fail to get name case mode", K(mode), K(ret));
    } else {
      perserve_lettercase = lib::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
      if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(create_ccl_rule_stmt = create_stmt<ObCreateCCLRuleStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create create_database_stmt", K(ret));
    } else {
      stmt_ = create_ccl_rule_stmt;
      create_ccl_rule_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    }

    // 1.resolve if not exists and ccl name
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      if (node->children_[IF_NOT_EXIST] != NULL) {
        if (node->children_[IF_NOT_EXIST]->type_ != T_IF_NOT_EXISTS) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parse tree", K(ret), K(node->children_[IF_NOT_EXIST]->type_));
        } else {
          create_ccl_rule_stmt->set_if_not_exists(true);
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString ccl_rule_name;
      ParseNode *ccl_rule_name_node = node->children_[CCL_RULE_NAME];
      if (OB_ISNULL(ccl_rule_name_node)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("ccl rule name parse node should not be null",
                 K(ccl_rule_name_node));
      } else if (ccl_rule_name_node->type_ != T_IDENT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree, ccl_rule_name node type should be "
                 "T_IDENT, but get: ",
                 K(node->children_[CCL_RULE_NAME]->type_), K(ret));
      } else if (OB_UNLIKELY(ccl_rule_name_node->str_len_ >
                             OB_MAX_COLUMN_NAME_LENGTH)) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_WARN("ccl rule name is too long", K(ret),
                 K(ObString(static_cast<int32_t>(ccl_rule_name_node->str_len_),
                            ccl_rule_name_node->str_value_)));
      } else {
        ccl_rule_name.assign_ptr(
            ccl_rule_name_node->str_value_,
            static_cast<int32_t>(ccl_rule_name_node->str_len_));
        create_ccl_rule_stmt->set_ccl_rule_name(ccl_rule_name);
      }
    }

    // 2. resolve affected database name & table name
    ObString database_name;
    ObString table_name;
    if (OB_SUCC(ret)) {
      // node->children_指针上面已经做了判断，这里不再做判断
      ParseNode *dbname_and_table_name_node = node->children_[CCL_AFFECT_DATABASE_TABLE];
      if (OB_ISNULL(dbname_and_table_name_node)
          || dbname_and_table_name_node->type_ != T_RELATION_FACTOR
          || OB_ISNULL(dbname_and_table_name_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree", K(ret), K(dbname_and_table_name_node));
      } else {
        // resolve databases affected by this ccl rule
        if (OB_ISNULL(dbname_and_table_name_node->children_[0])) {
          // *.table, current is not support
          create_ccl_rule_stmt->set_affect_all_databases(true);
        } else {
          database_name.assign_ptr(
            dbname_and_table_name_node->children_[0]->str_value_,
            static_cast<int32_t>(dbname_and_table_name_node->children_[0]->str_len_));
          if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase,
                                                            database_name))) {
            LOG_WARN("fail to check and convert database name", K(database_name), K(ret));
          } else {
            create_ccl_rule_stmt->set_affect_database(database_name);
          }
        }
        // resolve table affected by this ccl rule
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(dbname_and_table_name_node->children_[1])) {
          // database.*
          create_ccl_rule_stmt->set_affect_all_tables(true);
        } else {
          table_name.assign_ptr(
            dbname_and_table_name_node->children_[1]->str_value_,
            static_cast<int32_t>(dbname_and_table_name_node->children_[1]->str_len_));
          if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase,
                                                               table_name))) {
            LOG_WARN("fail to check and convert table name", K(table_name), K(ret));
          } else {
            create_ccl_rule_stmt->set_affect_table(table_name);
          }
        }
      }
    }

    // 3. resolve affected user & host
    if (OB_SUCC(ret)) {
      ObString user_name;
      ObString host_name;
      ParseNode *user_hostname_node = node->children_[CCL_AFFECT_USER_HOSTNAME];
      if (OB_ISNULL(user_hostname_node)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("The user_hostname node should not be NULL", K(ret));
      } else if (2 != user_hostname_node->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sql_parser parse ccl user and hostname error", K(ret));
      } else if (OB_ISNULL(user_hostname_node->children_[0]) || OB_ISNULL(user_hostname_node->children_[1])) {
        // 0: user, 1: hostname
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("The child of user and host node should not be NULL", K(ret));
      } else {
        user_name.assign_ptr(user_hostname_node->children_[0]->str_value_,
                             static_cast<int32_t>(user_hostname_node->children_[0]->str_len_));
        if (user_hostname_node->children_[0]->type_ != T_IDENT
            && OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                 *allocator_, session_info_->get_dtc_params(), user_name))) {
          LOG_WARN("fail to convert user name to utf8", K(ret), K(user_name),
                   KPHEX(user_name.ptr(), user_name.length()));
        } else {
          create_ccl_rule_stmt->set_affect_user(user_name);
        }

        if (OB_SUCC(ret)) {
          if (NULL == user_hostname_node->children_[1]) {
            host_name.assign_ptr(OB_DEFAULT_HOST_NAME,
                                 static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
          } else {
            host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                                 static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
          }
          create_ccl_rule_stmt->set_affect_hostname(host_name);
        }
      }
    }

    // 4. resolve affect dml
    // scope
    if(OB_SUCC(ret)) {
      ParseNode *ccl_for_affect_dml_node = node->children_[CCL_AFFECT_DML];
      if (OB_ISNULL(ccl_for_affect_dml_node)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("The ccl_for_affect_scope_node and ccl_for_affect_dml_node should not be NULL", K(ret), K(ccl_for_affect_dml_node));
      } else if (ccl_for_affect_dml_node->type_ != T_INT){
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("The type of ccl_for_affect_scope_node and ccl_for_affect_dml_node should be T_INT", K(ret), K_(ccl_for_affect_dml_node->type));
      } else {
        create_ccl_rule_stmt->set_affect_dml(static_cast<ObCCLAffectDMLType>(ccl_for_affect_dml_node->value_));
      }
    }

    // 5. resolve ccl_filter_options, ccl_filter_option_node can be NULL
    if(OB_SUCC(ret)) {
      ParseNode *ccl_filter_option_node = node->children_[CCL_FILTER_OPTION];
      if (OB_NOT_NULL(ccl_filter_option_node)) {
        ObString ccl_filter_keyword;
        if (ccl_filter_option_node->type_ == T_FILTER_KEYWORDS_LIST) {
          // constructe keywords 
          ObString ccl_keywords;
          if (OB_FAIL(merge_strings_with_escape(*ccl_filter_option_node, ';', '\\', ccl_keywords))) {
            LOG_WARN("fail to construct ccl keywords from parse node", K(ret));
          } else if (ccl_keywords.size() > OB_MAX_VARCHAR_LENGTH) {
            ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
            LOG_WARN("key word is too much or too long", K(ccl_keywords.size()), K(OB_MAX_VARCHAR_LENGTH));
          } else {
            create_ccl_rule_stmt->set_filter_keywords(ccl_keywords);
          }
        } else if (ccl_filter_option_node->type_ == T_FILTER_QUERY) {
          ObStmt *filter_sql_stmt = NULL;
          ObResolver resolver(params_);
          if (OB_FAIL(
                       resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *node, filter_sql_stmt))) {
            LOG_WARN("fail to resolve", K(ret));
          } else if (OB_ISNULL(filter_sql_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid outline_stmt is NULL", K(ret));
          } else {
            //todo: filter by sql
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unrecongized type of ccl_filter_option_node ", K(ret));
        }
      }
    }

    // 6. resolve ccl_with_options, ccl_filter_option_node can be NULL
    if(OB_SUCC(ret)) {
      ParseNode *ccl_with_option_node = node->children_[CCL_WITH_OPTION];
      if (OB_ISNULL(ccl_with_option_node) || OB_ISNULL(ccl_with_option_node->children_[0])) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("The ccl_with_option_node node and it's child should not be NULL", K(ret));
      } else {
        create_ccl_rule_stmt->set_max_concurrency(ccl_with_option_node->children_[0]->value_);
      }
    }

    // 7. resolve ccl_per_sql, it == NULL if it's rule level 
    if (OB_SUCC(ret)) {
      ParseNode *ccl_for_affect_scope_node = node->children_[CCL_AFFECT_SCOPE];
      create_ccl_rule_stmt->set_affect_scope(ccl_for_affect_scope_node == NULL ? ObCCLAffectScope::RULE_LEVEL : ObCCLAffectScope::FORMAT_SQLID_LEVEL);
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ(schema_checker_->check_ora_ddl_priv(
           session_info_->get_effective_tenant_id(), session_info_->get_priv_user_id(),
           ObString(""), stmt::T_CREATE_CCL_RULE, session_info_->get_enable_role_array()),
         session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }

  return ret;
}

int ObCreateCCLRuleResolver::escape_string(const ObString &original_string,
                                           char separator, 
                                           char escape_char,
                                           ObString &after_escape_string) {
  int ret = OB_SUCCESS;
  ObSqlString escaped_sql_string;
  for (uint64_t i = 0; OB_SUCC(ret) && i < original_string.length(); i++) {
    char c = original_string[i];
    if (c == separator) {
      if (OB_FAIL(escaped_sql_string.append(&escape_char, 1))) {
        LOG_WARN("fail to append ", K(ret), K(escape_char), K(escaped_sql_string));
      } else if (OB_FAIL(escaped_sql_string.append(&separator, 1))) {
        LOG_WARN("fail to append ", K(ret), K(separator), K(escaped_sql_string));
      }
    } else if (c == escape_char) {
      if (OB_FAIL(escaped_sql_string.append(&escape_char, 1))) {
        LOG_WARN("fail to append ", K(ret), K(escape_char), K(escaped_sql_string));
      } else if (OB_FAIL(escaped_sql_string.append(&escape_char, 1))) {
        LOG_WARN("fail to append ", K(ret), K(escape_char), K(escaped_sql_string));
      }
    } else if (OB_FAIL(escaped_sql_string.append(&c, 1))) {
      LOG_WARN("fail to append ", K(ret), K(c), K(escaped_sql_string));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(*allocator_, escaped_sql_string.string(), after_escape_string))) {
    LOG_WARN("fail to write string to ccl_keyword", K(ret));
  }

  return ret;
}

int ObCreateCCLRuleResolver::merge_strings_with_escape(const ParseNode &ccl_filter_option_node,
                                                       char separator, char escape_char, ObString & ccl_keyword) {
  int ret = OB_SUCCESS;
  ObString original_string;
  ObString after_escape_string;

  ObSqlString ccl_keywords_sql;
  for (int i = 0; i < ccl_filter_option_node.num_child_ && OB_SUCC(ret); ++i) {
    if (0 == ccl_filter_option_node.children_[i]->str_len_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ccl keyword is empty", K(ret), K(i), K(ccl_filter_option_node.children_[i]->str_value_));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ccl keyword (must all be not empty)");
    } else {
      original_string.assign_ptr(ccl_filter_option_node.children_[i]->str_value_,
                     ccl_filter_option_node.children_[i]->str_len_);
      if (OB_FAIL(escape_string(original_string, separator, escape_char, after_escape_string))) {
        LOG_WARN("fail to escape_string", K(ret), K(original_string));
      } else if (OB_FAIL(ccl_keywords_sql.append(after_escape_string))) {
        LOG_WARN("fail to append after_escape_string", K(ret), K(after_escape_string));
      } else if (i < ccl_filter_option_node.num_child_ - 1) {
        // not last string, need add separator
        ccl_keywords_sql.append(&separator, 1);
      }
    }
  }

  LOG_TRACE("ccl_keywords_sql: ", K(ccl_keywords_sql));
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(*allocator_, ccl_keywords_sql.string(), ccl_keyword))) {
    LOG_WARN("fail to write string to ccl_keyword", K(ret));
  }

  return ret;
}

} //end namespace sql
} //end namespace oceanbase
