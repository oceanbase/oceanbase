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

#ifndef OCEANBAS_SQL_RESOLVER_DCL_OB_GRANT_RESOLVER_
#define OCEANBAS_SQL_RESOLVER_DCL_OB_GRANT_RESOLVER_
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/dcl/ob_grant_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObGrantResolver: public ObDCLResolver
{
public:
  explicit ObGrantResolver(ObResolverParams &params);
  virtual ~ObGrantResolver();

  virtual int resolve(const ParseNode &parse_tree);
  
  static int resolve_grant_user(
      const ParseNode *grant_user,
      ObSQLSessionInfo *session_info,
      ObString &user_name,
      ObString &host_name);

  static int resolve_grantee_clause(
      const ParseNode *grantee_clause,
      ObSQLSessionInfo *session_info,
      ObIArray<ObString> &user_name_array,
      ObIArray<ObString> &host_name_array);

  int resolve_grant_role_to_ur(
      const ParseNode *grant_role,
      ObGrantStmt *grant_stmt);

  int resolve_grant_sys_priv_to_ur(
      const ParseNode *root,
      ObGrantStmt *grant_stmt);

  static int resolve_sys_privs(
      const ParseNode *privs_node,
      share::ObRawPrivArray &sys_privs);

  static int push_pack_sys_priv(
      share::ObRawPrivArray &sys_privs,
      int64_t priv);
      
  static int priv_exists(
      share::ObRawPrivArray &sys_privs,
      int64_t priv,
      bool &exists);

  static int obj_priv_exists(
      share::ObRawObjPrivArray &sys_privs,
      share::ObRawObjPriv priv,
      bool &exists);

  int resolve_grant_role_mysql(
      const ParseNode *grant_role,
      ObGrantStmt *grant_stmt);
  int resolve_grant_system_privileges_ora(
      const ParseNode *grant_system_privileges,
      ObGrantStmt *grant_stmt);

  int resolve_sys_oracle(
      const ParseNode *root,
      ObGrantStmt *grant_stmt);
  
  static int resolve_priv_level(
      share::schema::ObSchemaGetterGuard *guard,
      const ObSQLSessionInfo *session, 
      const ParseNode *node,
      const common::ObString &session_db,
      common::ObString &db,
      common::ObString &table,
      share::schema::ObPrivLevel &grant_level,
      ObIAllocator &allocator,
      common::ObString &catalog,
      common::ObString &sensitive_rule);

  static int resolve_priv_level_with_object_type(const ObSQLSessionInfo *session_info,
                                                 const ParseNode *priv_object_node,
                                                 ObPrivLevel &grant_level);

  template<class T>
  static int resolve_priv_object(const ParseNode *priv_object_node,
                                 T *grant_stmt,
                                 ObSchemaChecker *schema_checker_,
                                 common::ObString &db,
                                 common::ObString &table,
                                 common::ObString &catalog,
                                 common::ObString &sensitive_rule,
                                 const uint64_t tenant_id,
                                 ObIAllocator *allocator,
                                 bool is_grant = true); // revoke on object which has been deleted

  template<class T>
  static int resolve_priv_set(
      const uint64_t tenant_id,
      const ParseNode *privs_node,
      share::schema::ObPrivLevel grant_level,
      ObPrivSet &priv_set,
      T *grant_stmt,
      ObSchemaChecker *schema_checker,
      ObSQLSessionInfo *session_info,
      ObIAllocator &allocator);
  static int map_mysql_priv_type_to_ora_type(
      const ObPrivType mysql_priv_type,
      share::ObRawObjPriv &ora_obj_priv,
      bool &can_map);
  static bool is_ora_obj_priv_type(
      ObPrivType priv_type);
  static int resolve_obj_ora(
      const ParseNode *node,
      const ObString &session_db,
      ObString &db,
      ObString &table,
      share::schema::ObPrivLevel &grant_level,
      bool &is_directory,
      bool &explicit_db,
      bool &is_catalog,
      bool &is_location,
      bool &is_sensitive_rule);
  int check_user_dup(
      share::schema::ObSchemaGetterGuard *guard,
      ObIArray<ObString> &user_name_array,
      const ObGrantStmt *grant_stmt,
      const ObString& user_name,
      const ObString& host_name,
      const ObString& priv_user_name,
      bool &contain_role,
      bool &is_all_role);
  int rebuild_table_priv(
      ObGrantStmt *grant_stmt,
      bool is_owner,
      const bool is_all_role);
  int check_role_grant_option(
      const ObGrantStmt *grant_stmt,
      const bool contain_role);

  static int resolve_role_sys_obj_all_col_priv_list(const ParseNode *role_sys_list,
                                                    ObIArray<ObString> &role_name_array,
                                                    share::ObRawPrivArray &sys_priv_array);
  
private:
  int build_table_priv_arary_for_all(
      ObGrantStmt *grant_stmt,
      share::ObRawObjPrivArray &table_priv_array,
      bool is_owner,
      bool is_role);
  int check_obj_priv_valid(
      ObGrantStmt *grant_stmt,
      share::ObRawObjPriv ora_obj_priv);
  int resolve_ora(const ParseNode &parse_tree);
  int resolve_mysql(const ParseNode &parse_tree);
  int resolve_grant_obj_privileges(
      const ParseNode *node,
      ObGrantStmt *grant_stmt);
  int resolve_obj_priv_list_ora(
      ObGrantStmt *grant_stmt,
      const ParseNode *privs_node,
      share::schema::ObPrivLevel grant_level,
      bool is_owner);
  template<class T>
  static int resolve_col_names_mysql(
      T *grant_stmt,
      const ObPrivType priv_type,
      ParseNode *column_list,
      ObSchemaChecker *schema_checker,
      ObSQLSessionInfo *session_info,
      ObIAllocator &allocator);
  int resolve_col_names(
      ObGrantStmt *grant_stmt,
      share::ObRawObjPriv raw_obj_priv,
      ParseNode *opt_column_list);
  int resolve_admin_option(
      const ParseNode *admin_option,
      uint64_t &option);
  int check_duplicated_privs_with_info_ora(
      const ParseNode *privs_node,
      bool &duplicated_privs,
      bool &priv_has_execute);
  static int trans_ora_sys_priv_to_obj(ParseNode *priv_type_node);
  int check_role_sys_obj_all_col_priv_list(const ParseNode *privs_node, 
                                           const ObItemType grant_type);
  int check_role_sys_obj_all_col_priv_list_type(const ParseNode *privs_node, 
                                                const ObItemType grant_type);
  int check_duplicated_privs(const ParseNode *privs_node);
  static bool role_name_exists(const ObIArray<ObString> &role_name_array,
                               const ObString &role_name);
  bool priv_exists(const share::ObRawPrivArray &priv_array,
                   const share::ObRawPriv &priv);
  bool exec_obj_priv_exist(const ParseNode *privs_node, bool &has_exec_priv);
  int resolve_grant_role_or_sys_to_user_ora(const ParseNode *grant_system_privileges,
                                            ObGrantStmt *grant_stmt);
  int resolve_grantee_clause(const ParseNode *grantee_clause, ObGrantStmt *grant_stmt);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObGrantResolver);
};


template<class T>
int ObGrantResolver::resolve_col_names_mysql(
    T *grant_stmt,
    const ObPrivType priv_type,
    ParseNode *column_list,
    ObSchemaChecker *schema_checker,
    ObSQLSessionInfo *session_info,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  ObString db_name = grant_stmt->get_database_name();
  ObString table_name = grant_stmt->get_table_name();

  ObObjectType object_type = grant_stmt->get_object_type();
  uint64_t obj_id = grant_stmt->get_object_id();
  ObArray<ObString> column_names;
  if (OB_ISNULL(grant_stmt) || OB_ISNULL(column_list)
      || OB_ISNULL(schema_checker) || OB_ISNULL(session_info)
      || OB_ISNULL(schema_checker->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "unexpected error", K(ret));
  } else {
    ObString column_name;
    for (int32_t i = 0; OB_SUCCESS == ret && i < column_list->num_child_; ++i) {
      const ParseNode *child_node = NULL;
      if (OB_ISNULL(child_node = column_list->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "child node is null");
      } else {
        const share::schema::ObColumnSchemaV2 *column_schema = NULL;
        const ObSimpleTableSchemaV2 *table_schema = NULL;
        if (OB_FAIL(ob_write_string(allocator, ObString(static_cast<int32_t>(child_node->str_len_),
                                            const_cast<char *>(child_node->str_value_)), column_name))) {
          SQL_RESV_LOG(WARN, "ob write string failed", K(ret));
        } else if (OB_FAIL(grant_stmt->add_column_privs(column_name, priv_type))) {
          SQL_RESV_LOG(WARN, "push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

template<class T>
int ObGrantResolver::resolve_priv_set(
    const uint64_t tenant_id,
    const ParseNode *privs_node,
    ObPrivLevel grant_level,
    ObPrivSet &priv_set,
    T *grant_stmt,
    ObSchemaChecker *schema_checker,
    ObSQLSessionInfo *session_info,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(privs_node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "Invalid argument, priv_node_list should not be NULL", K(privs_node), K(ret));
  } else if (OB_PRIV_INVALID_LEVEL == grant_level) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "Invalid argument, grant_level should not be invalid", K(grant_level), K(ret));
  } else if (OB_ISNULL(grant_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "grant stmt is null", K(ret));
  } else {
    for (int i = 0; i < privs_node->num_child_ && OB_SUCCESS == ret; ++i) {
      if (OB_NOT_NULL(privs_node->children_[i]) && T_PRIV_TYPE == privs_node->children_[i]->type_) {
        const ObPrivType priv_type = privs_node->children_[i]->value_;
        if (OB_PRIV_USER_LEVEL == grant_level) {
          priv_set |= priv_type;
        } else if (OB_PRIV_DB_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            priv_set |= OB_PRIV_DB_ACC;
          } else if (priv_type & (~(OB_PRIV_DB_ACC | OB_PRIV_GRANT))) {
            ret = OB_ERR_PRIV_USAGE;
            SQL_RESV_LOG(WARN, "Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            priv_set |= priv_type;
          }
        } else if (OB_PRIV_TABLE_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            priv_set |= OB_PRIV_TABLE_ACC;
          } else if (priv_type & (~(OB_PRIV_TABLE_ACC | OB_PRIV_GRANT))) {
            ret = OB_ILLEGAL_GRANT_FOR_TABLE;
            SQL_RESV_LOG(WARN, "Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else if (privs_node->children_[i]->num_child_ == 1) {
            if (OB_FAIL(ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(tenant_id))) {
              SQL_RESV_LOG(WARN, "grant or revoke column priv is not suppported", KR(ret));
            } else if (OB_FAIL(resolve_col_names_mysql(grant_stmt, priv_type,
                                                privs_node->children_[i]->children_[0],
                                                schema_checker, session_info, allocator))) {
              SQL_RESV_LOG(WARN, "resolve col names failed", K(ret));
            }
          } else {
            priv_set |= priv_type;
          }
        } else if (OB_PRIV_ROUTINE_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            priv_set |= OB_PRIV_ROUTINE_ACC;
          } else if (priv_type & (~(OB_PRIV_ROUTINE_ACC | OB_PRIV_GRANT))) {
            ret = OB_ILLEGAL_GRANT_FOR_TABLE;
            SQL_RESV_LOG(WARN, "Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            priv_set |= priv_type;
          }
        } else if (OB_PRIV_CATALOG_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            priv_set |= OB_PRIV_CATALOG_ACC;
          } else if (priv_type & (~(OB_PRIV_CATALOG_ACC | OB_PRIV_GRANT))) {
            ret = OB_ILLEGAL_GRANT_FOR_TABLE;
            SQL_RESV_LOG(WARN, "Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            priv_set |= priv_type;
          }
        } else if (OB_PRIV_SENSITIVE_RULE_LEVEL == grant_level) {
          if (priv_type & (~(OB_PRIV_SENSITIVE_RULE_ACC | OB_PRIV_GRANT))) {
            ret = OB_ILLEGAL_GRANT_FOR_TABLE;
            SQL_RESV_LOG(WARN, "Grant/Revoke privilege than can not be used",
                         "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            priv_set |= priv_type;
          }
        } else if (OB_PRIV_OBJECT_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            priv_set |= OB_PRIV_OBJECT_ACC;
          } else if (priv_type & (~(OB_PRIV_OBJECT_ACC | OB_PRIV_GRANT))) {
            ret = OB_ILLEGAL_GRANT_FOR_TABLE;
            SQL_RESV_LOG(WARN, "Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            priv_set |= priv_type;
          }
        } else {
          //do nothing
        }
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_RESV_LOG(WARN, "sql_parser parse privileges error", K(ret));
      }
    }
  }
  return ret;
}

template<class T>
int ObGrantResolver::resolve_priv_object(const ParseNode *priv_object_node,
                                         T *grant_stmt,
                                         ObSchemaChecker *schema_checker,
                                         common::ObString &db,
                                         common::ObString &table,
                                         common::ObString &catalog,
                                         common::ObString &sensitive_rule,
                                         const uint64_t tenant_id,
                                         ObIAllocator *allocator,
                                         bool is_grant)
{
  int ret = OB_SUCCESS;
  share::schema::ObObjectType object_type = share::schema::ObObjectType::INVALID;
  uint64_t object_id = OB_INVALID_ID;
  if (OB_ISNULL(grant_stmt) || OB_ISNULL(schema_checker) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (priv_object_node != NULL) {
    if (priv_object_node->value_ == 1) {
      const share::schema::ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_checker->get_table_schema(tenant_id, db, table, false, table_schema))) {
        LOG_WARN("get table schema failed", K(ret));
        if (OB_TABLE_NOT_EXIST == ret && !is_grant) {
          ret = OB_SUCCESS;
        }
      } else if (table_schema != NULL) {
        if (table_schema->is_index_table()) {
          object_type = ObObjectType::INDEX;
        } else {
          object_type = ObObjectType::TABLE;
        }
        object_id = table_schema->get_table_id();
      }
    } else if (priv_object_node->value_ == 2 || priv_object_node->value_ == 3) {
      object_type = (priv_object_node->value_ == 2) ? ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
      uint64_t routine_id = 0;
      bool is_proc = false;
      if (OB_FAIL(schema_checker->get_routine_id(tenant_id, db, table, routine_id, is_proc))) {
        LOG_WARN("get routine id failed", K(ret));
        if (OB_ERR_SP_DOES_NOT_EXIST == ret && !is_grant) {
          ret = OB_SUCCESS;
        }
      } else {
        if (is_proc) {
          object_type = ObObjectType::PROCEDURE;
        } else {
          object_type = ObObjectType::FUNCTION;
        }
        object_id = routine_id;
      }
    } else if (priv_object_node->value_ == 4) {
      object_type = ObObjectType::CATALOG;
      if (OB_FAIL(schema_checker->get_catalog_id_name(tenant_id, catalog, object_id, allocator, !is_grant))) {
        LOG_WARN("failed to get catalog id", K(ret));
      } else {
        grant_stmt->set_catalog_name(catalog);
      }
    } else if (priv_object_node->value_ == 5) {
      object_type = ObObjectType::LOCATION;
      if (OB_FAIL(schema_checker->get_location_id(tenant_id, table, object_id))) {
        LOG_WARN("failed to get location id", K(ret));
      }
    } else if (priv_object_node->value_ == 6) {
      object_type = ObObjectType::SENSITIVE_RULE;
      if (OB_FAIL(schema_checker->get_sensitive_rule_id_name(tenant_id, sensitive_rule, object_id, allocator, !is_grant))) {
        LOG_WARN("failed to get sensitive rule id", K(ret));
      } else {
        grant_stmt->set_sensitive_rule_name(sensitive_rule);
      }
    }
  } else {
    ObString object_db_name;
    if (db.empty() || table.empty()) {
      object_type = share::schema::ObObjectType::MAX_TYPE;
    } else {
      ObSynonymChecker synonym_checker;
      (void)schema_checker->get_object_type(tenant_id, db, table, object_type, object_id,
                                            object_db_name, false, false, ObString(""),
                                            synonym_checker);
    }
  }
  grant_stmt->set_object_type(object_type);
  grant_stmt->set_object_id(object_id);
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
#endif
