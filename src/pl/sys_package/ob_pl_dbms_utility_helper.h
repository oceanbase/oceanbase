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

#ifndef OCEANBASE_SRC_PL_OB_PL_DBMS_UTILITY_HELPER_H
#define OCEANBASE_SRC_PL_OB_PL_DBMS_UTILITY_HELPER_H
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{

namespace common
{
class ObObj;
class ObSqlString;
class ObIAllocator;
template <typename T>
class ObIArray;
}

namespace share
{
namespace schema
{
class ObPackageInfo;
class ObRoutineInfo;
}
}

namespace sql
{
class ObSchemaChecker;
class ObSynonymChecker;
} // namespace sql


namespace pl
{
class ObPLContext;
class ObPLExecState;
class DbmsUtilityHelper {
public:
  DbmsUtilityHelper () {}
  virtual ~DbmsUtilityHelper() {}

  enum BTErrorType {
    STACK_INFO,
    ERROR_INFO,
    UNKNOWN_INFO,
  };

  struct BtInfo {
    ObString object_name;
    uint64_t loc;
    uint64_t handler;
    uint64_t frame_size;
    int64_t error_code;
    BTErrorType error_type; // 0:stack info, 1:error info
    void init() {
      error_type = STACK_INFO;
      object_name.reset();
    }
    uint64_t get_line_number() {
      return (loc >> 32) & 0xffffffff;
    }
    uint64_t get_column_number() {
      return loc & 0xffffffff;
    }
    void set_line_number(uint64_t line_number) {
      loc += ((line_number << 32) & 0xffffffff00000000);
    }
    TO_STRING_KV(K(object_name), K(loc), K(handler),
     K(frame_size), K(error_code), K(error_type));
  };
  struct BackTrace {
    common::ObSEArray<DbmsUtilityHelper::BtInfo *, 8> call_stack;
    common::ObSEArray<DbmsUtilityHelper::BtInfo *, 8> error_trace;
    void reset() {
      call_stack.reset();
      error_trace.reset();
      err_code = OB_SUCCESS;
    }
    int err_code; // indicate if catch error trace success or not;
  };

  enum NameResolveType {
    table = 0,
    PL_SQL,
    sequences,
    trigger,
    java_source,
    java_resource,
    java_class,
    type,
    java_shared_data,
    index,
    max_resolve_type,
  };

  enum NameResolveResultType {
    db_link_type = 0,
    index_type = 1,
    table_type = 2,
    view_type = 4,
    synonym_type = 5,
    sequence_type = 6,
    procedure_type = 7,
    function_type = 8,
    package_type = 9,
    trigger_type = 12,
    udt_type = 13,
    max_resolve_res_type,
  };

  static int format_error_trace(common::ObIArray<BtInfo *> &bts, common::ObSqlString &bt, bool is_backtrace);
  static int format_error_stack(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int format_error_backtrace(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int format_call_stack(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int name_tokenize(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_param_value(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_sql_hash(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_bit_set(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int canonicalize(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_endian(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_hash_value(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_db_version(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_port_string(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_time(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int validate(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int invalidate(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  static int get_number_value(ObObjParam &param, int64_t &result);
  static int format_frame_info(common::ObIAllocator &allocator,
                               pl::ObPLExecState *frame,
                               common::ObIArray<BtInfo*> &bts,
                               bool is_last_frame = false);
  static int get_backtrace(common::ObIAllocator &allocator,
                           pl::ObPLContext &pl_ctx, common::ObIArray<BtInfo*> &bts);

  static int check_and_get_type_if_exist(ObIAllocator &allocator,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         sql::ObSQLSessionInfo &session_info,
                                         const NameResolveType type,
                                         ObIArray<ObString> &split_names,
                                         ObString &schema_name,
                                         ObString &part1,
                                         ObString &part2,
                                         NameResolveResultType &res_type,
                                         uint64_t &object_id);
  static int name_resolve(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int split_names(ObIAllocator &allocator, ObObjParam &name,
                         ObIArray<ObString> &splitted_name,
                         ObIArray<uint64_t> &offset);
  static int check_plsql_type(ObIAllocator &allocator,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              sql::ObSQLSessionInfo &session_info,
                                        ObIArray<ObString> &split_names,
                                        ObString &schema_name,
                                        ObString &part1,
                                        ObString &part2,
                                        NameResolveResultType &res_type,
                                        uint64_t &object_id);
  static int get_synonym_object_recursively(uint64_t tenant_id,
                                            uint64_t &database_id,
                                            ObString &synon_name,
                                            sql::ObSchemaChecker &schema_checker,
                                            sql::ObSynonymChecker &synonym_checker,
                                            ObString &target_obj_name,
                                            uint64_t &target_obj_db_id);
  static int check_synonym_type(ObIAllocator &allocator,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                sql::ObSQLSessionInfo &session_info,
                                ObIArray<ObString> &split_names,
                                ObString &schema_name,
                                ObString &part1,
                                ObString &part2,
                                NameResolveResultType &res_type,
                                uint64_t &object_id);
  static int get_object_with_synonym(share::schema::ObSchemaGetterGuard &schema_guard,
                                    const uint64_t tenant_id,
                                    uint64_t db_id,
                                    ObString &synon_name,
                                    ObString &obj_name,
                                    uint64_t &object_db_id,
                                    bool &is_exist);
  static int check_dblink_type(ObIAllocator &allocator,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              sql::ObSQLSessionInfo &session_info,
                              ObIArray<ObString> &split_names,
                              ObString &schema_name,
                              ObString &part1,
                              ObString &part2,
                              NameResolveResultType &res_type,
                              uint64_t &object_id);
  static int check_table_type(ObIAllocator &allocator,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              sql::ObSQLSessionInfo &session_info,
                              ObIArray<ObString> &split_names,
                              ObString &schema_name,
                              ObString &part1,
                              ObString &part2,
                              NameResolveResultType &res_type,
                              uint64_t &object_id);

  static int check_seq_type(ObIAllocator &allocator,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            sql::ObSQLSessionInfo &session_info,
                            ObIArray<ObString> &split_names,
                            ObString &schema_name,
                            ObString &part1,
                            ObString &part2,
                            NameResolveResultType &res_type,
                            uint64_t &object_id);
  static int check_trigger_type(ObIAllocator &allocator,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                sql::ObSQLSessionInfo &session_info,
                                ObIArray<ObString> &split_names,
                                ObString &schema_name,
                                ObString &part1,
                                ObString &part2,
                                NameResolveResultType &res_type,
                                uint64_t &object_id);
  static int try_synonym_object(share::schema::ObSchemaGetterGuard &schema_guard,
                                const uint64_t tenant_id,
                                uint64_t db_id,
                                ObString &synon_name,
                                ObString &obj_name,
                                ObString &obj_db_name,
                                uint64_t &obj_db_id,
                                bool &is_synon);
  static uint64_t get_line_number(const uint64_t loc);
  // note:: backup your session' db name and db id before call this function
  static int reset_session_database(sql::ObSQLSessionInfo *session_info,
                                    const ObString &database_name,
                                    uint64_t database_id);
private:
  static ObCollationType get_default_collation_type(common::ObObjType type,
                                                    sql::ObBasicSessionInfo &session_info);
  static int set_string_collation_type(common::ObObj &obj,
                                       sql::ObExecContext &ctx);
  static int set_null_string(common::ObObj &obj, sql::ObExecContext &ctx);
  static int set_string_result(common::ObObj &obj, const ObString &res, sql::ObExecContext &ctx);
  static int check_string_valid(const ObString &str, ObCollationType coll_type,
                                const char *invalid_chars, uint64_t char_cnt);
  static int is_valid_identifier(const ObString &ident);
};

}
}

#endif
