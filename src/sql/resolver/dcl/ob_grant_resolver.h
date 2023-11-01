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

  int resolve_grant_system_privileges_mysql(
      const ParseNode *grant_system_privileges,
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
      share::schema::ObPrivLevel &grant_level);
  
  static int resolve_priv_set(
      const ParseNode *privs_node,
      share::schema::ObPrivLevel grant_level,
      ObPrivSet &priv_set);
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
      bool &explicit_db);
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

} // end namespace sql
} // end namespace oceanbase
#endif
