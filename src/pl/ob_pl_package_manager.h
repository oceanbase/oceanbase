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

#ifndef SRC_PL_OB_PL_PACKAGE_MANAGER_H_
#define SRC_PL_OB_PL_PACKAGE_MANAGER_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"
#include "share/schema/ob_package_info.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObString;
}

namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
class ObSqlExpression;
class ObBasicSessionInfo;
class ObSessionVariable;
class ObRawExprFactory;
class ObRawExpr;
}

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObPackageInfo;
}
}
namespace common
{
class ObMySQLProxy;
}

namespace pl
{
struct ObPLExecCtx;
class ObPLResolveCtx;
class ObUserDefinedType;
class ObPLVar;
class ObPLPackage;
class ObPLFunction;
struct ObPackageStateVersion;
class ObPLPackageState;
class ObPLCondition;
class ObPLCursor;
class ObPLCacheCtx;

struct ObSysPackageFile {
  const char *const package_name;
  const char *const package_spec_file_name;
  const char *const package_body_file_name;
};

class ObCharStream;

class ObPLPackageManager
{
public:
  ObPLPackageManager() {}
  virtual ~ObPLPackageManager() {}

  int get_package_var(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                      const common::ObString &var_name, const ObPLVar *&var, int64_t &var_idx);
  int get_package_var(const ObPLResolveCtx &resolve_ctx,
                      uint64_t package_id,
                      int64_t var_idx,
                      const ObPLVar *&var);

  int get_package_type(const ObPLResolveCtx &resolve_ctx,
                       uint64_t package_id,
                       const common::ObString &type_name,
                       const ObUserDefinedType *&user_type,
                       bool log_user_error = true);
  int get_package_type(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                       uint64_t type_id, const ObUserDefinedType *&user_type);

  int get_package_expr(const ObPLResolveCtx &resolve_ctx,
                       uint64_t package_id, int64_t expr_idx, sql::ObSqlExpression *&expr);
  int get_package_expr(const ObPLResolveCtx &resolve_ctx, sql::ObRawExprFactory &expr_factory,
                       uint64_t package_id, int64_t expr_idx, sql::ObRawExpr *&expr);

  int get_package_spec_cursor(const ObPLResolveCtx &resolve_ctx,
                              uint64_t package_id,
                              const ObString &cursor_name,
                              const ObPLCursor *&cursor,
                              int64_t &cursor_idx);

  int get_package_cursor(const ObPLResolveCtx &resolve_ctx,
                         uint64_t package_id,
                         int64_t cursor_idx,
                         const ObPLCursor *&cursor);

  int get_package_cursor(const ObPLResolveCtx &resolve_ctx,
                         uint64_t package_id,
                         const common::ObString &cursor_name,
                         const ObPLCursor *&cursor,
                         int64_t &cursor_idx);

  int get_package_condition(const ObPLResolveCtx &resolve_ctx,
                            uint64_t package_id,
                            const common::ObString &condition_name,
                            const ObPLCondition *&value);

  int get_package_routine(const ObPLResolveCtx &resolve_ctx, sql::ObExecContext &exec_ctx,
                          uint64_t package_id,
                          int64_t routine_idx, ObPLFunction *&routine);

  int get_package_var_val(const ObPLResolveCtx &resolve_ctx,
                          sql::ObExecContext &exec_ctx,
                          uint64_t package_id,
                          int64_t spec_version,
                          int64_t body_version,
                          const int64_t var_idx,
                          common::ObObj &var_val);
  int set_package_var_val(const ObPLResolveCtx &resolve_ctx,
                          sql::ObExecContext &exec_ctx,
                          uint64_t package_id,
                          const int64_t var_idx,
                          const common::ObObj &var_val,
                          bool need_deseriablze = false,
                          bool from_proxy = false);

  int load_package_spec(const ObPLResolveCtx &resolve_ctx,
                        const share::schema::ObPackageInfo &package_spec_info,
                        ObPLPackage *&package_spec);
  int load_package_body(const ObPLResolveCtx &resolve_ctx,
                        const share::schema::ObPackageInfo &package_spec_info,
                        const share::schema::ObPackageInfo &package_body_info,
                        ObPLPackage *&package_body);
  int get_package_state(const ObPLResolveCtx &resolve_ctx,
                        sql::ObExecContext &exec_ctx,
                        uint64_t package_id,
                        ObPLPackageState *&package_state,
                        bool for_static_member = false);

  static int load_sys_package(common::ObMySQLProxy &sql_proxy,
                              common::ObString &package_name,
                              ObCompatibilityMode compa_mode,
                              bool from_file);
  static int load_all_common_sys_package(common::ObMySQLProxy &sql_proxy,
                                         ObCompatibilityMode compa_mode,
                                         bool from_file);
  static int load_all_sys_package(common::ObMySQLProxy &sql_proxy);
  static int load_all_special_sys_package(common::ObMySQLProxy &sql_proxy);

  static int add_package_to_plan_cache(const ObPLResolveCtx &resolve_ctx, ObPLPackage *package);
  static int get_package_from_plan_cache(const ObPLResolveCtx &resolve_ctx, 
                uint64_t package_id, 
                ObPLPackage *&package);
  static int get_package_schema_info(share::schema::ObSchemaGetterGuard &schema_guard,
                                     uint64_t package_id,
                                     const share::schema::ObPackageInfo *&package_spec_info,
                                     const share::schema::ObPackageInfo *&package_body_info);
  static int destory_package_state(sql::ObSQLSessionInfo &session_info, uint64_t package_id);
  int check_version(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                    const ObPackageStateVersion &state_version, bool old_encode_rule, bool &match);
  int get_cached_package(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                                ObPLPackage *&package_spec,
                                ObPLPackage *&package_body,
                                bool for_static_member = false);

  static int notify_package_variable_deserialize(sql::ObBasicSessionInfo *session, const ObString &name, const sql::ObSessionVariable &value);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLPackageManager);

  static int read_package_sql(ObCharStream &stream, char* buf, int64_t buf_len, bool &eos);
  static int read_and_exec_package_sql(common::ObMySQLProxy &sql_proxy,
                                       ObCharStream &stream,
                                       ObCompatibilityMode compa_mode);
  static int get_syspack_source_file_content(const char *file_name, const char *&content);
  static int load_sys_package(ObMySQLProxy &sql_proxy,
                              const ObSysPackageFile &pack_file_info,
                              ObCompatibilityMode compa_mode,
                              bool from_file);
  static int load_sys_package_list(common::ObMySQLProxy &sql_proxy,
                                   const ObSysPackageFile *sys_package_list,
                                   int sys_package_count,
                                   ObCompatibilityMode compa_mode,
                                   bool from_file);

  int get_cached_package_spec(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                              ObPLPackage *&package_spec);

  int get_package_item_state(const ObPLResolveCtx &resolve_ctx,
                             sql::ObExecContext &exec_ctx,
                             ObPLPackage &package,
                             const ObPackageStateVersion &state_version,
                             ObPLPackageState *&package_state,
                             const ObPLPackage *package_for_verify);

  int update_special_package_status(const ObPLResolveCtx &resolve_ctx,
                                    uint64_t package_id,
                                    const ObPLVar &var,
                                    const ObObj &old_val,
                                    const ObObj &new_val);
};
} //end namespace pl
} //end namespace oceanbase
#endif /* SRC_PL_OB_PL_PACKAGE_MANAGER_H_ */
