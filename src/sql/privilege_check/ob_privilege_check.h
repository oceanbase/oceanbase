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

#ifndef OCEANBASE_SQL_PRIVILEGE_CHECK_OB_PRIVILEGE_CHECK_
#define OCEANBASE_SQL_PRIVILEGE_CHECK_OB_PRIVILEGE_CHECK_

#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_stmt_type.h"
namespace oceanbase {
namespace sql {
struct ObSqlCtx;
class ObStmt;

typedef int (*ObGetStmtNeedPrivsFunc) (const share::schema::ObSessionPrivInfo &session_priv,
                                       const ObStmt *basic_stmt,
                                       common::ObIArray<share::schema::ObNeedPriv> &need_privs);

typedef int (*ObGetStmtOraNeedPrivsFunc) (const share::schema::ObSessionPrivInfo &session_priv,
                                          const ObStmt *basic_stmt,
                                          common::ObIArray<share::schema::ObOraNeedPriv> &need_privs);

class ObPrivilegeCheck {
public:
  ///Check privilege
  ///@param ctx[in] sql ctx
  ///@param basic_stmt[in] stmt
  ///@param stmt_need_priv[out] execute a query needed priv.This should be added to physical plan.
  static int check_privilege(const ObSqlCtx &ctx,
                             const ObStmt *basic_stmt,
                             share::schema::ObStmtNeedPrivs &stmt_need_priv);
  ///Check privilege
  ///@param ctx[in] sql ctx
  ///@param stmt_need_priv[in] privs needed in executing a query
  static int check_privilege(const ObSqlCtx &ctx,
                             const share::schema::ObStmtNeedPrivs &stmt_need_priv);

  static int check_privilege_new(const ObSqlCtx &ctx,
                                 const ObStmt *basic_stmt,
                                 share::schema::ObStmtNeedPrivs &stmt_need_privs,
                                 share::schema::ObStmtOraNeedPrivs &stmt_ora_need_privs);

  static int check_ora_privilege(const ObSqlCtx &ctx,
                                 const share::schema::ObStmtOraNeedPrivs &stmt_ora_need_priv);

  static int can_do_operation_on_db(const share::schema::ObSessionPrivInfo &session_priv,
                                    const common::ObString &db_name);
  static int can_do_grant_on_db_table(const share::schema::ObSessionPrivInfo &session_priv,
                                      const ObPrivSet priv_set,
                                      const common::ObString &db_name,
                                      const common::ObString &table_name);

  static int can_do_drop_operation_on_db(const share::schema::ObSessionPrivInfo &session_priv,
                                         const common::ObString &db_name);
  static bool is_mysql_org_table(const common::ObString &db_name, const common::ObString &table_name);
  static int get_stmt_ora_need_privs(
      uint64_t user_id,
      const ObSqlCtx &ctx,
      const ObStmt *basic_stmt,
      common::ObIArray<share::schema::ObOraNeedPriv> &stmt_need_priv,
      uint64_t check_flag);
	// check if password is expired according to the flag in session info
  static int check_password_expired(const ObSqlCtx &ctx, const stmt::StmtType stmt_type);
	// check if password is expired and set the flag in session info
  static int check_password_expired_on_connection(const uint64_t tenant_id,
                                                  const uint64_t user_id,
                                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                                  sql::ObSQLSessionInfo &session);
  ///Get all privilege info needed by a stmt, including sub-queries.
  ///called by generate_physical_plan
  ///@param session_priv[in]      session privileges
  ///@param basic_stmt[in]        the stmt
  ///@param stmt_need_priv[out]   priv info needed by stmt
  static int get_stmt_need_privs(const share::schema::ObSessionPrivInfo &session_priv,
                                 const ObStmt *basic_stmt,
                                 common::ObIArray<share::schema::ObNeedPriv> &stmt_need_priv);
private:
   ///Extract priv info needed by a single stmt, may be sub-query.
   ///called by recursive_stmt_need_priv
   ///@param ctx[in]               sql ctx
   ///@param basic_stmt[in]        the stmt
   ///@param stmt_need_priv[out]   priv info needed by stmt
  static int one_level_stmt_need_priv(const share::schema::ObSessionPrivInfo &session_priv,
                                      const ObStmt *basic_stmt,
                                      common::ObIArray<share::schema::ObNeedPriv> &stmt_need_priv);

  static const ObGetStmtNeedPrivsFunc priv_check_funcs_[];
  static const ObGetStmtOraNeedPrivsFunc get_ora_priv_funcs_[];
  
  static int one_level_stmt_ora_need_priv(
      uint64_t user_id,
      const ObSqlCtx &ctx,
      const ObStmt *basic_stmt,
      common::ObIArray<share::schema::ObOraNeedPriv> &stmt_need_priv,
      uint64_t check_flag);

  static int check_password_life_time_mysql(const uint64_t tenant_id,
                                            const uint64_t user_id,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            sql::ObSQLSessionInfo &session);
  enum ObPasswordLifeTime { FOREVER = 0, LIMIT};
  static bool check_password_expired_time(int64_t password_last_changed_ts,
                                          int64_t password_life_time);
  static int check_password_life_time_oracle(const uint64_t tenant_id,
                                             const uint64_t user_id,
                                             share::schema::ObSchemaGetterGuard &schema_guard,
                                             sql::ObSQLSessionInfo &session);

};

}
}

#endif // OCEANBASE_SQL_PRIVILEGE_CHECK_OB_PRIVILEGE_CHECK
