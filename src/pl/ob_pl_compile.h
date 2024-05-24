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

#ifndef OCEANBASE_SRC_PL_OB_PL_COMPILE_H_
#define OCEANBASE_SRC_PL_OB_PL_COMPILE_H_

#include "ob_pl.h"
#include "ob_pl_stmt.h"
#include "ob_pl_persistent.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObPackageInfo;
}
}
namespace pl
{
class ObPLPackageAST;
class ObPLPackage;
class ObPLPackageGuard;
class ObPLResolver;
class ObPLVarDebugInfo;
class ObRoutinePersistentInfo;
class ObPLCompiler
{
public:
  ObPLCompiler(common::ObIAllocator &allocator,
               sql::ObSQLSessionInfo &session_info,
               share::schema::ObSchemaGetterGuard &schema_guard,
               ObPLPackageGuard &package_guard,
               common::ObMySQLProxy &sql_proxy) :
    allocator_(allocator),
    session_info_(session_info),
    schema_guard_(schema_guard),
    package_guard_(package_guard),
    sql_proxy_(sql_proxy) {}
  virtual ~ObPLCompiler() {}

  int compile(const ObStmtNodeTree *block,
              const uint64_t stmt_id,
              ObPLFunction &func,
              ParamStore *params,
              bool is_prepare_protocol); //匿名块接口


  int compile(const uint64_t id, ObPLFunction &func); //Procedure/Function接口

  int analyze_package(const ObString &source, const ObPLBlockNS *parent_ns,
                      ObPLPackageAST &package_ast, bool is_for_trigger);
  int generate_package(const ObString &exec_env, ObPLPackageAST &package_ast, ObPLPackage &package);
  int compile_package(const share::schema::ObPackageInfo &package_info, const ObPLBlockNS *parent_ns,
                      ObPLPackageAST &package_ast, ObPLPackage &package); //package
  static int compile_subprogram_table(common::ObIAllocator &allocator,
                                 sql::ObSQLSessionInfo &session_info,
                                 const sql::ObExecEnv &exec_env,
                                 ObPLRoutineTable &routine_table,
                                 ObPLCompileUnit &compile_unit,
                                 share::schema::ObSchemaGetterGuard &schema_guard);
  static int compile_type_table(const ObPLUserTypeTable &ast_type_table, ObPLCompileUnit &unit);
  static int check_dep_schema(ObSchemaGetterGuard &schema_guard,
                              const DependenyTableStore &dep_schema_objs);
  static int init_anonymous_ast(ObPLFunctionAST &func_ast,
                                common::ObIAllocator &allocator,
                                sql::ObSQLSessionInfo &session_info,
                                ObMySQLProxy &sql_proxy,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                pl::ObPLPackageGuard &package_guard,
                                const ParamStore *params,
                                bool is_prepare_protocol = true);
  int check_package_body_legal(const ObPLBlockNS *parent_ns,
                                      const ObPLPackageAST &package_ast);
  static int update_schema_object_dep_info(ObIArray<ObSchemaObjVersion> &dp_tbl,
                                           uint64_t tenant_id,
                                           uint64_t owner_id,
                                           uint64_t dep_obj_id,
                                           uint64_t schema_version,
                                           share::schema::ObObjectType dep_obj_type);
  static int init_function(share::schema::ObSchemaGetterGuard &schema_guard,
                           const sql::ObExecEnv &exec_env,
                           const ObPLRoutineInfo &routine_signature,
                           ObPLFunction &routine);
private:
  int init_function(const share::schema::ObRoutineInfo *proc, ObPLFunction &func);

  int generate_package_cursors(const ObPLPackageAST &package_ast,
                               const ObPLCursorTable &ast_cursor_table,
                               ObPLPackage &package);
  int generate_package_conditions(const ObPLConditionTable &ast_condition_table,
                                  ObPLPackage &package);
  int generate_package_vars(const ObPLPackageAST &package_ast,
                            const ObPLSymbolTable &ast_var_table,
                            ObPLPackage &package);
  int generate_package_types(const ObPLUserTypeTable &ast_type_table,
                             ObPLCompileUnit &package);
  int generate_package_routines(const ObString &exec_env,
                                ObPLRoutineTable &routine_table,
                                ObPLPackage &package);
  static int compile_types(const ObIArray<const ObUserDefinedType*> &types, ObPLCompileUnit &unit);
  static int format_object_name(share::schema::ObSchemaGetterGuard &schema_guard,
                                const uint64_t tenant_id,
                                const uint64_t db_id,
                                const uint64_t package_id,
                                ObString &database_name,
                                ObString &package_name);
  int compile(const share::schema::ObRoutineInfo &routine, ObPLFunctionAST &func_ast, ObPLFunction &func);
  int read_dll_from_disk(bool enable_persistent,
                         ObRoutinePersistentInfo &routine_storage,
                         ObPLFunctionAST &func_ast,
                         ObPLCodeGenerator &cg,
                         const ObRoutineInfo &routine,
                         ObPLFunction &func,
                         ObRoutinePersistentInfo::ObPLOperation &op);
private:
  common::ObIAllocator &allocator_;
  sql::ObSQLSessionInfo &session_info_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  ObPLPackageGuard &package_guard_;
  common::ObMySQLProxy &sql_proxy_;
};

class ObPLCompilerEnvGuard
{
public:
  ObPLCompilerEnvGuard(const ObPackageInfo &info,
                       ObSQLSessionInfo &session_info,
                       share::schema::ObSchemaGetterGuard &schema_guard,
                       int &ret,
                       const ObPLBlockNS *prarent_ns = nullptr);

  ObPLCompilerEnvGuard(const ObRoutineInfo &info,
                       ObSQLSessionInfo &session_info,
                       share::schema::ObSchemaGetterGuard &schema_guard,
                       int &ret);

  ~ObPLCompilerEnvGuard();

private:
  template<class Info>
  void init(const Info &info,
            ObSQLSessionInfo &sessionInfo,
            share::schema::ObSchemaGetterGuard &schema_guard,
            int &ret,
            const ObPLBlockNS *parent_ns = nullptr);

private:
  int &ret_;
  ObSQLSessionInfo &session_info_;
  ObExecEnv old_exec_env_;
  ObSqlString old_db_name_;
  uint64_t old_db_id_;
  bool need_reset_exec_env_;
  bool need_reset_default_database_;
  ObArenaAllocator allocator_;
};

}
}

#endif /* OCEANBASE_SRC_PL_OB_PL_COMPILE_H_ */
