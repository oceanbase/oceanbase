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

#ifndef _OB_RESOLVER_UTILS_H
#define _OB_RESOLVER_UTILS_H
#include "share/ob_rpc_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "common/object/ob_object.h"
#include "common/ob_accuracy.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/parser/parse_node.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_stmt.h"

#define LOG_WARN_IGNORE_COL_NOTFOUND(ret, fmt, args...) \
  do {\
    if (common::OB_ERR_BAD_FIELD_ERROR == ret) {\
      LOG_DEBUG(fmt, ##args);\
    } else {\
      LOG_WARN(fmt, ##args);\
    }\
  } while(0);

namespace oceanbase
{
namespace pl
{
class ObPLBlockNS;
class ObPLRoutineInfo;
}
namespace sql
{
class ObRawExprUtils;
class ObSynonymChecker;
class ObRoutineMatchInfo
{
public:
  struct MatchInfo
  {
    MatchInfo()
      : need_cast_(false), src_type_(ObMaxType), dest_type_(ObMaxType) {}
    MatchInfo(bool need_cast, ObObjType src_type, ObObjType dst_type)
      : need_cast_(need_cast), src_type_(src_type), dest_type_(dst_type) {}

    bool need_cast_;
    ObObjType src_type_;
    ObObjType dest_type_;

    TO_STRING_KV(K_(need_cast), K_(src_type), K_(dest_type));
  };

  bool need_cast()
  {
    bool need_cast = false;
    for (int64_t i = 0; i < match_info_.count(); ++i) {
      need_cast = match_info_.at(i).need_cast_ || need_cast;
    }
    return need_cast;
  }

  bool match_same_type()
  {
    bool same_type = true;
    for (int64_t i = 0; i < match_info_.count(); ++i) {
      same_type = match_info_.at(i).src_type_ == match_info_.at(i).dest_type_ && same_type;
    }
    return same_type;
  }

  bool has_unknow_type()
  {
    bool has_unknow_type = false;
    for (int64_t  i = 0; i < match_info_.count(); ++i) {
      has_unknow_type = has_unknow_type || ObUnknownType ==  match_info_.at(i).src_type_;
    }
    return has_unknow_type;
  }

  ObObjType get_type(int i)
  {
    return match_info_.at(i).dest_type_;
  }

public:
  const share::schema::ObIRoutineInfo *routine_info_;
  common::ObSEArray<MatchInfo, 16> match_info_;

  TO_STRING_KV(K_(match_info));
};
struct ObResolverUtils
{
  enum RangeElementsNode {
    PARTITION_NAME_NODE = 0,
    PARTITION_ELEMENT_NODE = 1
  };
  // 用于判断是否需要检查 生成列 或 函数索引 中的的系统函数或伪列。
  // 如果需要检查，那么所有 non pure 的系统函数和伪列都会被检测出来并报错，
  // 并且根据被检测对象是生成列还是函数索引返回不同的错误码
  // 默认不需要检测，为 DISABLE_CHECK
  enum PureFunctionCheckStatus
  {
    DISABLE_CHECK = 0,
    CHECK_FOR_GENERATED_COLUMN,
    CHECK_FOR_FUNCTION_INDEX,
    CHECK_FOR_CHECK_CONSTRAINT,
  };
  static const int NAMENODE = 1;
  static ObItemType item_type_;
public:
  static int get_user_type(common::ObIAllocator *allocator,
                           ObSQLSessionInfo *session_info,
                           common::ObMySQLProxy *sql_proxy,
                           share::schema::ObSchemaGetterGuard *schema_guard,
                           pl::ObPLPackageGuard &package_guard,
                           uint64_t type_id,
                           const pl::ObUserDefinedType *&user_type);
  static int get_all_function_table_column_names(const TableItem &table_item,
                                                 ObResolverParams &params,
                                                 ObIArray<ObString> &column_names);
  static int check_function_table_column_exist(const TableItem &table_item,
                                               ObResolverParams &params,
                                               const ObString &column_name);
  static int check_json_table_column_exists(const TableItem &table_item,
                                           ObResolverParams &params,
                                           const ObString &column_name,
                                           bool& exists);

  static int collect_schema_version(share::schema::ObSchemaGetterGuard &schema_guard,
                                    const ObSQLSessionInfo *session_info,
                                    ObRawExpr *expr,
                                    ObIArray<ObSchemaObjVersion> &dependency_objects,
                                    bool is_called_in_sql = false,
                                    ObIArray<uint64_t> *dep_db_array = NULL);

  static int add_dependency_synonym_object(share::schema::ObSchemaGetterGuard *schema_guard,
                                            const ObSQLSessionInfo *session_info,
                                            const ObSynonymChecker &synonym_checker,
                                            DependenyTableStore &dep_table);

  static int add_dependency_synonym_object(share::schema::ObSchemaGetterGuard *schema_guard,
                                            const ObSQLSessionInfo *session_info,
                                            const ObSynonymChecker &synonym_checker,
                                            const pl::ObPLDependencyTable &dep_table);

  static int resolve_extended_type_info(const ParseNode &str_list_node,
                                        ObIArray<ObString>& type_info_array);
  // type_infos is %ori_cs_type, need convert to %cs_type first
  static int check_extended_type_info(common::ObIAllocator &alloc,
                                      ObIArray<ObString> &type_infos,
                                      ObCollationType ori_cs_type,
                                      const ObString &col_name,
                                      ObObjType col_type,
                                      ObCollationType cs_type,
                                      ObSQLMode sql_mode);
  static int check_duplicates_in_type_infos(const ObIArray<common::ObString> &type_infos,
                                            const ObString &col_name,
                                            ObObjType col_type,
                                            ObCollationType cs_type,
                                            ObSQLMode sql_mode,
                                            int32_t &dup_cnt);
  static int check_max_val_count(common::ObObjType type,
                                 const common::ObString &col_name,
                                 int64_t val_cnt,
                                 int32_t dup_cnt);
  static int get_candidate_routines(ObSchemaChecker &schema_checker,
                              uint64_t tenant_id,
                              const ObString &current_database,
                              const ObString &db_name,
                              const ObString &package_name,
                              const ObString &routine_name,
                              const share::schema::ObRoutineType routine_type,
                              common::ObIArray<const share::schema::ObIRoutineInfo *> &routines,
                              uint64_t udt_id = OB_INVALID_ID,
                              const pl::ObPLResolveCtx *resolve_ctx = NULL,
                              ObSynonymChecker *synonym_checker = NULL);
  static int check_routine_exists(const ObSQLSessionInfo *session_info,
                                  ObSchemaChecker *schema_checker,
                                  pl::ObPLBlockNS *secondary_namespace,
                                  const ObString &db_name,
                                  const ObString &package_name,
                                  const ObString &routine_name,
                                  const share::schema::ObRoutineType routine_type,
                                  bool &exists,
                                  pl::ObProcType &proc_type,
                                  uint64_t udt_id = OB_INVALID_ID);
  static int check_routine_exists(ObSchemaChecker &schema_checker,
                                  const ObSQLSessionInfo &session_info,
                                  const ObString &db_name,
                                  const ObString &package_name,
                                  const ObString &routine_name,
                                  const share::schema::ObRoutineType routine_type,
                                  bool &exists,
                                  uint64_t udt_id = OB_INVALID_ID);
  static int match_best_routine(const common::ObIArray<const share::schema::ObRoutineInfo *> &routines,
                                const common::ObIArray<ObRawExpr *> &expr_params,
                                const share::schema::ObRoutineInfo *&routine);
  static int match_vacancy_parameters(const share::schema::ObIRoutineInfo &routine_info,
                                      ObRoutineMatchInfo &match_info);
  static int check_type_match(ObResolverParams &params,
                              ObRoutineMatchInfo::MatchInfo &match_info,
                              ObRawExpr *expr,
                              ObObjType src_type,
                              uint64_t src_type_id,
                              pl::ObPLDataType &dst_pl_type);
  static int check_type_match(const pl::ObPLResolveCtx &resolve_ctx,
                              ObRoutineMatchInfo::MatchInfo &match_info,
                              ObRawExpr *expr,
                              ObObjType src_type,
                              uint64_t src_type_id,
                              pl::ObPLDataType &dst_pl_type,
                              bool is_sys_package = false);
  static int check_type_match(const pl::ObPLResolveCtx &resolve_ctx,
                              ObRoutineMatchInfo::MatchInfo &match_info,
                              ObRawExpr *expr,
                              ObObjType src_type,
                              ObCollationType src_coll_type,
                              uint64_t src_type_id,
                              pl::ObPLDataType &dst_pl_type,
                              bool is_sys_package = false);
  static int get_type_and_type_id(ObRawExpr *expr, ObObjType &type, uint64_t &type_id);
  static int check_match(const pl::ObPLResolveCtx &resolve_ctx,
                  const common::ObIArray<sql::ObRawExpr *> &expr_params,
                  const share::schema::ObIRoutineInfo *routine_info,
                  ObRoutineMatchInfo &match_info);
  static int pick_routine(ObIArray<ObRoutineMatchInfo> &match_infos,
                   const share::schema::ObIRoutineInfo *&routine_info);
  static int pick_routine(const pl::ObPLResolveCtx &resolve_ctx,
                   const common::ObIArray<sql::ObRawExpr *> &expr_params,
                   const common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos,
                   const share::schema::ObIRoutineInfo *&routine_info);
  static int pick_routine(const pl::ObPLResolveCtx &resolve_ctx,
                   const common::ObIArray<sql::ObRawExpr *> &expr_params,
                   const common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos,
                   const pl::ObPLRoutineInfo *&routine_info);
  static int pick_routine(const pl::ObPLResolveCtx &resolve_ctx,
                   const common::ObIArray<sql::ObRawExpr *> &expr_params,
                   const common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos,
                   const share::schema::ObRoutineInfo *&routine_info);
  static int get_routine(pl::ObPLPackageGuard &package_guard,
                         ObResolverParams &params,
                         uint64_t tenant_id,
                         const ObString &current_database,
                         const ObString &db_name,
                         const ObString &package_name,
                         const ObString &routine_name,
                         const share::schema::ObRoutineType routine_type,
                         const common::ObIArray<ObRawExpr *> &expr_params,
                         const share::schema::ObRoutineInfo *&routine,
                         const ObString &dblink_name = ObString(""),
                         ObIAllocator *allocator = NULL,
                         ObSynonymChecker *synonym_checker = NULL);
  static int get_routine(const pl::ObPLResolveCtx &resolve_ctx,
                         uint64_t tenant_id,
                         const ObString &current_database,
                         const ObString &db_name,
                         const ObString &package_name,
                         const ObString &routine_name,
                         const share::schema::ObRoutineType routine_type,
                         const common::ObIArray<ObRawExpr *> &expr_params,
                         const share::schema::ObRoutineInfo *&routine,
                         ObSynonymChecker *synonym_checker = NULL);
  static int resolve_sp_access_name(ObSchemaChecker &schema_checker,
                                    ObIAllocator &allocator,
                                    uint64_t tenant_id,
                                    const ObString& current_database,
                                    const ObString& procedure_name,
                                    ObString &database_name,
                                    ObString &package_name,
                                    ObString &routine_name);
  static int resolve_sp_access_name(ObSchemaChecker &schema_checker,
                                    uint64_t tenant_id,
                                    const ObString& current_database,
                                    const ParseNode &sp_access_name_node,
                                    ObString &db_name,
                                    ObString &package_name,
                                    ObString &routine_name,
                                    ObString &dblink_name);
  static int resolve_sp_name(ObSQLSessionInfo &session_info,
                             const ParseNode &sp_name_node,
                             ObString &db_name,
                             ObString &sp_name,
                             bool need_db_name = true);
  static int resolve_synonym_object_recursively(ObSchemaChecker &schema_checker,
                                                ObSynonymChecker &synonym_checker,
                                                uint64_t tenant_id,
                                                uint64_t database_id,
                                                const ObString &synonym_name,
                                                uint64_t &object_database_id,
                                                ObString &object_name,
                                                bool &exist,
                                                bool search_public_schema = true);

  static int resolve_column_ref(const ParseNode *node, const common::ObNameCaseMode mode, ObQualifiedName& column_ref);
  static int resolve_obj_access_ref_node(ObRawExprFactory &expr_factory,
                                         const ParseNode *node,
                                         ObQualifiedName &q_name,
                                         const ObSQLSessionInfo &session_info);

  static int set_parallel_info(sql::ObSQLSessionInfo &session_info,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               ObRawExpr &expr,
                               ObQueryCtx &ctx,
                               ObIArray<ObSchemaObjVersion> &return_value_version);

  static int resolve_external_symbol(common::ObIAllocator &allocator,
                                     sql::ObRawExprFactory &expr_factory,
                                     sql::ObSQLSessionInfo &session_info,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     common::ObMySQLProxy *sql_proxy,
                                     ExternalParams *extern_param_info,
                                     pl::ObPLBlockNS *ns,
                                     ObQualifiedName &q_name,
                                     ObIArray<ObQualifiedName> &columns,
                                     ObIArray<ObRawExpr*> &real_exprs,
                                     ObRawExpr *&expr,
                                     pl::ObPLPackageGuard *package_guard,
                                     bool is_prepare_protocol = false,
                                     bool is_check_mode = false,
                                     bool is_sql_scope = false);
  static int resolve_external_param_info(ExternalParams &param_info,
                                         const ObSQLSessionInfo &session_info,
                                         ObRawExprFactory &expr_factory,
                                         int64_t &prepare_param_count,
                                         ObRawExpr *&expr);

  static int revert_external_param_info(ExternalParams &param_info,
                                        ObRawExprFactory &expr_factory,
                                        ObRawExpr *expr);

   /**
   * @brief  从parser结果中解析出语句type
   * @param [in] result      - parse结果
   * @param [out] type  - 语句类型
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   */
  static int resolve_stmt_type(const ParseResult &result, stmt::StmtType &type);
  static stmt::StmtType get_stmt_type_by_item_type(const ObItemType item_type);
  /**
   * resolve expr_const, @see sql_parser.y
   *
   * @param node [in]
   * @param allocator [in]
   * @param connection_charset [in] character_set_connection of current session
   * @param connection_collation [in] collation_connection of current session
   * @param val [out]
   *
   * @return
   */
  static int resolve_const(const ParseNode *node,
                           const stmt::StmtType stmt_type,
                           common::ObIAllocator &allocator,
                           const common::ObCollationType connection_collation,
                           const ObCollationType nchar_collation,
                           const common::ObTimeZoneInfo *tz_info,
                           common::ObObjParam &val,
                           const bool is_paramlize,
                           common::ObString &literal_prefix,
                           const ObLengthSemantics default_length_semantics,
                           const common::ObCollationType server_collation,
                           ObExprInfo *parents_expr_info,
                           const ObSQLMode mode,
                           bool enable_decimal_int_type,
                           const ObCompatType compat_type,
                           bool is_from_pl = false);

  static int set_string_val_charset(ObIAllocator &allocator,
                                    ObObjParam &val,
                                    ObString &charset,
                                    ObObj &result_val,
                                    bool is_strict_mode,
                                    bool return_ret);

  static int resolve_data_type(const ParseNode &type_node,
                               const common::ObString &ident_name,
                               common::ObDataType &data_type,
                               const int is_oracle_mode/*1:Oracle, 0:MySql */,
                               const bool is_for_pl_type,
                               const ObSessionNLSParams &nls_session_param,
                               uint64_t tenant_id,
                               const bool enable_decimal_int_type,
                               const bool convert_real_type_to_decimal = false);

  static int resolve_str_charset_info(const ParseNode &type_node,
                                      common::ObDataType &data_type,
                                      const bool is_for_pl_type);

  static int resolve_timestamp_node(const bool is_set_null,
                                    const bool is_set_default,
                                    const bool is_first_timestamp_column,
                                    ObSQLSessionInfo *session,
                                    share::schema::ObColumnSchemaV2 &column);

  static int add_column_ids_without_duplicate(const uint64_t column_id, common::ObIArray<uint64_t> *array);
  static int resolve_columns_for_const_expr(ObRawExpr *&expr, ObArray<ObQualifiedName> &columns, ObResolverParams &resolve_params);
  static int resolve_const_expr(ObResolverParams &params,
                                const ParseNode &node,
                                ObRawExpr *&const_expr,
                                common::ObIArray<ObVarInfo> *var_infos);

  //check unique indx cover partition column
  static int check_unique_index_cover_partition_column(const share::schema::ObTableSchema &table_schema,
                                                       const obrpc::ObCreateIndexArg &arg);

  //Get index columns ids from array of ObColumnSortItem
  static int get_index_column_ids(const share::schema::ObTableSchema &table_schema,
                                  const common::ObIArray<obrpc::ObColumnSortItem> &columns,
                                  common::ObIArray<uint64_t> &column_ids);

  //unique idx need cover partition columns
  static int unique_idx_covered_partition_columns(const share::schema::ObTableSchema &table_schema,
                                                  const common::ObIArray<uint64_t> &index_columns,
                                                  const common::ObPartitionKeyInfo &partition_info);

  static int get_collation_type_of_names(const ObSQLSessionInfo *session_info,
                                         const ObNameTypeClass type_class,
                                         common::ObCollationType &cs_type);
  static int name_case_cmp(const ObSQLSessionInfo *session_info,
                           const common::ObString &name,
                           const common::ObString &name_other,
                           const ObNameTypeClass type_class,
                           bool &is_equal);
  static int check_column_name(const ObSQLSessionInfo *session_info,
                               const ObQualifiedName &q_name,
                               const ObColumnRefRawExpr &col_ref,
                               bool &is_hit);
  static int resolve_partition_list_value_expr(ObResolverParams &params,
                                               const ParseNode &node,
                                               const common::ObString &part_name,
                                               const share::schema::ObPartitionFuncType part_type,
                                               const common::ObIArray<ObRawExpr *> &part_func_exprs,
                                               common::ObIArray<ObRawExpr *> &part_value_expr_array,
                                               const bool &in_tablegroup = false);
  static int resolve_partition_list_value_expr(ObResolverParams &params,
                                               const ParseNode &node,
                                               const common::ObString &part_name,
                                               const share::schema::ObPartitionFuncType part_type,
                                               int64_t &expr_num,
                                               common::ObIArray<ObRawExpr *> &part_value_expr_array,
                                               const bool &in_tablegroup = false);
  static int resolve_columns_for_partition_range_value_expr(ObRawExpr *&expr, ObArray<ObQualifiedName> &columns);
  static int resolve_partition_range_value_expr(ObResolverParams &params,
                                                const ParseNode &node,
                                                const common::ObString &part_name,
                                                const share::schema::ObPartitionFuncType part_type,
                                                const ObRawExpr &part_func_expr,
                                                ObRawExpr *&part_value_expr,
                                                const bool &in_tablegroup = false);
  static int resolve_partition_range_value_expr(ObResolverParams &params,
                                                const ParseNode &node,
                                                const common::ObString &part_name,
                                                const share::schema::ObPartitionFuncType part_type,
                                                ObRawExpr *&part_value_expr,
                                                const bool &in_tablegroup = false,
                                                const bool interval_check = false);
  static int resolve_columns_for_partition_expr(ObRawExpr *&expr,
                                                common::ObIArray<ObQualifiedName> &columns,
                                                const share::schema::ObTableSchema &tbl_schema,
                                                share::schema::ObPartitionFuncType part_func_type,
                                                int64_t partition_key_start,
                                                common::ObIArray<common::ObString> &partition_keys);
  static int resolve_partition_expr(ObResolverParams &params,
                                    const ParseNode &node,
                                    const share::schema::ObTableSchema &tbl_schema,
                                    share::schema::ObPartitionFuncType part_func_type,
                                    ObRawExpr *&part_expr,
                                    common::ObIArray<common::ObString> *part_keys);
  static int resolve_generated_column_expr(ObResolverParams &params,
                                           const common::ObString &expr_str,
                                           share::schema::ObTableSchema &tbl_schema,
                                           share::schema::ObColumnSchemaV2 &generated_column,
                                           ObRawExpr *&expr,
                                           const PureFunctionCheckStatus
                                            check_status = DISABLE_CHECK);
  static int resolve_generated_column_expr(ObResolverParams &params,
                                           const ParseNode *node,
                                           share::schema::ObTableSchema &tbl_schema,
                                           share::schema::ObColumnSchemaV2 &generated_column,
                                           ObRawExpr *&expr,
                                           const PureFunctionCheckStatus
                                             check_status = DISABLE_CHECK);
  static int resolve_generated_column_expr(ObResolverParams &params,
                                           const common::ObString &expr_str,
                                           share::schema::ObTableSchema &tbl_schema,
                                           ObIArray<share::schema::ObColumnSchemaV2 *> &resolved_cols,
                                           share::schema::ObColumnSchemaV2 &generated_column,
                                           ObRawExpr *&expr,
                                           const PureFunctionCheckStatus
                                             check_status = DISABLE_CHECK,
                                           bool coltype_not_defined = false);
  static int resolve_generated_column_expr(ObResolverParams &params,
                                           const ParseNode *node,
                                           share::schema::ObTableSchema &tbl_schema,
                                           ObIArray<share::schema::ObColumnSchemaV2 *> &resolved_cols,
                                           share::schema::ObColumnSchemaV2 &generated_column,
                                           ObRawExpr *&expr,
                                           const PureFunctionCheckStatus
                                             check_status = DISABLE_CHECK,
                                           bool coltype_not_defined = false);
  static int resolve_generated_column_info(const common::ObString &expr_str,
                                           ObIAllocator &allocator,
                                           ObItemType &root_expr_type,
                                           common::ObIArray<common::ObString> &column_names);
  static int resolve_column_info_recursively(const ParseNode *node,
                                             common::ObIArray<common::ObString> &column_names);
  static share::schema::ObColumnSchemaV2* get_column_schema_from_array(
    ObIArray<share::schema::ObColumnSchemaV2 *> &resolved_cols,
    const common::ObString &column_name);
  static int resolve_default_expr_v2_column_expr(ObResolverParams &params,
                                                 const common::ObString &expr_str,
                                                 share::schema::ObColumnSchemaV2 &default_expr_v2_column,
                                                 ObRawExpr *&expr,
                                                 bool allow_sequence);
  static int resolve_default_expr_v2_column_expr(ObResolverParams &params,
                                                 const ParseNode *node,
                                                 share::schema::ObColumnSchemaV2 &default_expr_v2_column,
                                                 ObRawExpr *&expr,
                                                 bool allow_sequence);
  static int resolve_constraint_expr(ObResolverParams &params,
                                     const common::ObString &expr_str,
                                     share::schema::ObTableSchema &tbl_schema,
                                     share::schema::ObConstraint &constraint,
                                     ObRawExpr *&expr);
  static int check_comment_length(ObSQLSessionInfo *session_info,
                                  char *str,
                                  int64_t *str_len,
                                  const int64_t max_len);
  static int check_user_variable_length(char *str, int64_t str_len);
  static int resolve_check_constraint_expr(
             ObResolverParams &params,
             const ParseNode *node,
             const share::schema::ObTableSchema &tbl_schema,
             share::schema::ObConstraint &constraint,
             ObRawExpr *&expr,
             const share::schema::ObColumnSchemaV2 *column_schema = NULL,
             ObIArray<ObQualifiedName> *res_columns = NULL);
  static int create_not_null_expr_str(const ObString &column_name, common::ObIAllocator &allocator,
                                      ObString &expr_str, const bool is_oracle_mode);
  static int build_partition_key_expr(ObResolverParams &params,
                                      const share::schema::ObTableSchema &table_schema,
                                      ObRawExpr *&partition_key_expr,
                                      common::ObIArray<ObQualifiedName> *qualified_names);
  static int create_generate_table_column(ObRawExprFactory &expr_factory,
                                          const TableItem &table_item,
                                          uint64_t column_id,
                                          ColumnItem &col_item);
  static int check_partition_value_expr_for_range(const common::ObString &part_name,
                                                  const ObRawExpr &part_func_expr,
                                                  ObRawExpr &part_value_expr,
                                                  const share::schema::ObPartitionFuncType part_type,
                                                  const bool &in_tablegroup = false);
  static int check_partition_value_expr_for_range(const common::ObString &part_name,
                                                  ObRawExpr &part_value_expr,
                                                  const share::schema::ObPartitionFuncType part_type,
                                                  const bool &in_tablegroup = false,
                                                  const bool interval_check = false);
  static int check_column_valid_for_partition(const ObRawExpr &part_expr,
                                              const share::schema::ObPartitionFuncType part_func_type,
                                              const share::schema::ObTableSchema &tbl_schema);
  static int check_expr_valid_for_partition(ObRawExpr &part_expr,
                                            ObSQLSessionInfo &session_info,
                                            const share::schema::ObPartitionFuncType part_func_type,
                                            const share::schema::ObTableSchema &tbl_schema,
                                            const bool &in_tablegroup = false);
  static int check_partition_expr_for_oracle_hash(ObRawExpr &part_expr,
                                                  const share::schema::ObPartitionFuncType part_type,
                                                  const share::schema::ObTableSchema &tbl_schema,
                                                  const bool &in_tablegroup = false);
  static bool is_valid_partition_column_type(const common::ObObjType type,
                                             const share::schema::ObPartitionFuncType part_type,
                                             const bool is_check_value);
  static bool is_valid_oracle_partition_data_type(const common::ObObjType type, const bool check_value);
  static bool is_valid_oracle_interval_data_type(
      const common::ObObjType type,
      ObItemType &item_type);
  // WARNING: is_sync_ddl_user=true means outside program won't wait ddl, which is so misleading
  static int check_sync_ddl_user(ObSQLSessionInfo *session_info, bool &is_sync_ddl_user);
  static bool is_restore_user(ObSQLSessionInfo &session_info);
  static bool is_drc_user(ObSQLSessionInfo &session_info);
  static int set_sync_ddl_id_str(ObSQLSessionInfo *session_info, common::ObString &ddl_id_str);
  static int resolve_udf_name_by_parse_node(
    const ParseNode *node, const common::ObNameCaseMode case_mode, ObUDFInfo& udf_info);
  // for create table with fk in oracle mode
  static int check_dup_foreign_keys_exist(
             const common::ObSArray<obrpc::ObCreateForeignKeyArg> &fk_args);
  // for alter table add fk in oracle mode
  static int check_dup_foreign_keys_exist(
             const common::ObIArray<share::schema::ObForeignKeyInfo> &fk_infos,
             const common::ObIArray<uint64_t> &child_column_ids,
             const common::ObIArray<uint64_t> &parent_column_ids,
             const uint64_t parent_table_id,
             const uint64_t child_table_id);
  // for create table with fk in oracle mode
  static bool is_match_columns_with_order(
              const common::ObIArray<ObString> &child_columns_1,
              const common::ObIArray<ObString> &parent_columns_1,
              const common::ObIArray<ObString> &child_columns_2,
              const common::ObIArray<ObString> &parent_columns_2);
  // for alter table add fk in oracle mode
  static bool is_match_columns_with_order(
              const common::ObIArray<uint64_t> &child_column_ids_1,
              const common::ObIArray<uint64_t> &parent_column_ids_1,
              const common::ObIArray<uint64_t> &child_column_ids_2,
              const common::ObIArray<uint64_t> &parent_column_ids_2);
  static int foreign_key_column_match_uk_pk_column(const share::schema::ObTableSchema &parent_table_schema,
                                                   ObSchemaChecker &schema_checker,
                                                   const common::ObIArray<common::ObString> &parent_columns,
                                                   const common::ObSArray<obrpc::ObCreateIndexArg> &index_arg_list,
                                                   const bool is_oracle_mode,
                                                   share::schema::ObConstraintType &ref_cst_type,
                                                   uint64_t &ref_cst_id,
                                                   bool &is_match);
  static int check_self_reference_fk_columns_satisfy(
        const obrpc::ObCreateForeignKeyArg &arg);
  static int check_foreign_key_set_null_satisfy(
        const obrpc::ObCreateForeignKeyArg &arg,
        const share::schema::ObTableSchema &child_table_schema,
        const bool is_mysql_compat_mode);
  static int check_match_columns(const common::ObIArray<ObString> &parent_columns,
                                 const common::ObIArray<ObString> &key_columns,
                                 bool &is_match);
  static int check_match_columns(const common::ObIArray<uint64_t> &parent_columns,
                                 const common::ObIArray<uint64_t> &key_columns,
                                 bool &is_match);
  static int check_match_columns_strict(const ObIArray<ObString> &columns_array_1,
                                        const ObIArray<ObString> &columns_array_2,
                                        bool &is_match);
  static int check_match_columns_strict_with_order(const share::schema::ObTableSchema *index_table_schema,
                                                   const obrpc::ObCreateIndexArg &create_index_arg,
                                                   bool &is_match);
  static int check_pk_idx_duplicate(const share::schema::ObTableSchema &table_schema,
                                    const obrpc::ObCreateIndexArg &create_index_arg,
                                    const ObIArray<ObString> &input_index_columns_name,
                                    bool &is_match);
  static int check_foreign_key_columns_type(
      const bool is_mysql_compat_mode,
      const share::schema::ObTableSchema &child_table_schema,
      const share::schema::ObTableSchema &parent_table_schema,
      const common::ObIArray<common::ObString> &child_columns,
      const common::ObIArray<common::ObString> &parent_columns,
      const share::schema::ObColumnSchemaV2 *column = NULL);
  static int get_columns_name_from_index_table_schema(const share::schema::ObTableSchema &index_table_schema,
                                                      ObIArray<ObString> &index_columns_name);
  static int transform_sys_func_to_objaccess(
    common::ObIAllocator *allocator, const ParseNode *sys_func, ParseNode *&obj_access);
  static int transform_func_sys_to_udf(common::ObIAllocator *allocator,
                                       const ParseNode *func_sys,
                                       const common::ObString &db_name,
                                       const common::ObString &pkg_name,
                                       ParseNode *&func_udf);
  static int set_direction_by_mode(const ParseNode &sort_node, OrderItem &order_item);
  static int resolve_string(const ParseNode *node, common::ObString &string);

  // check some kind of the non-updatable view, which is forbidden for all dml statement:
  // mysql:
  //    aggregate
  //    distinct
  //    set
  //    limit
  // oracle:
  //   window func
  //   set
  //   rownum
  //
  // return OB_SUCCESS, OB_ERR_NON_INSERTABLE_TABLE, OB_ERR_NON_UPDATABLE_TABLE, OB_ERR_ILLEGAL_VIEW_UPDATE
  static int uv_check_basic(ObSelectStmt &stmt, const bool is_insert);

  static int check_select_item_subquery(ObSelectStmt &stmt,
                                        bool &has_subquery,
                                        bool &has_dependent_subquery,
                                        const uint64_t base_tid,
                                        bool &ref_update_table);

  // mysql updatable view check rule:
  //
  //    Subquery in the select list:
  //    Nondependent subqueries in the select list fail for INSERT, but are okay for UPDATE, DELETE.
  //    For dependent subqueries in the select list, no data change statements are permitted.
  static int uv_check_select_item_subquery(const TableItem &table_item,
                                           bool &has_subquery,
                                           bool &has_dependent_subquery,
                                           bool &ref_update_table);

  static int check_table_referred(ObSelectStmt &stmt, const uint64_t base_tid, bool &referred);
  // mysql updatable view check rule:
  //     Subquery in the WHERE clause that refers to a table in the FROM clause
  static int uv_check_where_subquery(const TableItem &table_item, bool &ref_update_table);

  static int check_has_non_inner_join(ObSelectStmt &stmt, bool &has_non_inner_join);
  static int uv_check_has_non_inner_join(const TableItem &table_item, bool &has_non_inner_join);

  // check duplicate base column in the top view, or has none column reference select items.
  static int uv_check_dup_base_col(const TableItem &table_item,
                                   bool &has_dup,
                                   bool &has_nol_col_ref);

  static int uv_mysql_insertable_join(const TableItem &table_item, const uint64_t base_tid, bool &insertable);

  // statement has unremovable distinct is not updatable in oracle.
  static int uv_check_oracle_distinct(const TableItem &table_item,
                                      ObSQLSessionInfo &session_info,
                                      ObSchemaChecker &schema_checker,
                                      bool &has_distinct);
  // check whether view is allowed to be WITH CHECK OPTION
  static int view_with_check_option_allowed(const ObSelectStmt *stmt, bool &with_check_option);
  static void set_stmt_type(const ObItemType item_type) { ObResolverUtils::item_type_ = item_type; }

  static common::ObString get_stmt_type_string(stmt::StmtType stmt_type);

  static ParseNode *get_select_into_node(const ParseNode &node);

  static int get_select_into_node(const ParseNode &node, ParseNode* &into_node, bool top_level);

  static int get_user_var_value(const ParseNode *node,
                                ObSQLSessionInfo *session_info,
                                ObObj &value);
  static int check_duplicated_column(ObSelectStmt &select_stmt, bool can_skip = false);
  static int escape_char_for_oracle_mode(common::ObIAllocator &allocator,
                                         common::ObString &str,
                                         common::ObCollationType cs_type);
  static int check_secure_path(const common::ObString &secure_file_priv, const common::ObString &full_path);
  // get column item's default value. Use select item's expr to find the original column.
  static int resolve_default_value_and_expr_from_select_item(const SelectItem &s_item,
                                                             ColumnItem &c_item,
                                                             const ObSelectStmt *stmt);
  static int check_whether_assigned(const ObDMLStmt *stmt,
                                    const common::ObIArray<ObAssignment> &assigns,
                                    uint64_t table_id,
                                    uint64_t base_column_id,
                                    bool &exist);
  static int prune_check_constraints(const common::ObIArray<ObAssignment> &assignment,
                                     ObIArray<ObRawExpr*> &check_exprs);
  // find column item by table_item (may be generated table) and base table's column id.
  static ColumnItem *find_col_by_base_col_id(ObDMLStmt &stmt,
                                             const TableItem &table_item,
                                             const uint64_t base_column_id,
                                             const uint64_t base_table_id = OB_INVALID_ID);
  static ColumnItem *find_col_by_base_col_id(ObDMLStmt &stmt,
                                             const uint64_t table_id,
                                             const uint64_t base_column_id,
                                             const uint64_t base_table_id = OB_INVALID_ID);
  static const ColumnItem *find_col_by_base_col_id(const ObDMLStmt &stmt,
                                                   const TableItem &table_item,
                                                   const uint64_t base_column_id,
                                                   const uint64_t base_table_id = OB_INVALID_ID,
                                                   bool ignore_updatable_check = false);
  static const ColumnItem *find_col_by_base_col_id(const ObDMLStmt &stmt,
                                                   const uint64_t table_id,
                                                   const uint64_t base_column_id,
                                                   const uint64_t base_table_id = OB_INVALID_ID,
                                                   bool ignore_updatable_check = false);
  //  check column is from the base table of updatable view
  static bool in_updatable_view_path(const TableItem &table_item, const ObColumnRefRawExpr &col);
  static int check_partition_range_value_result_type(const ObPartitionFuncType part_type,
                                                     const ObExprResType &column_type,
                                                     const ObString &column_name,
                                                     ObObj &part_value);
  static ObRawExpr *find_file_column_expr(ObIArray<ObRawExpr *> &pseudo_exprs,
                                          int64_t table_id,
                                          int64_t column_idx,
                                          const ObString &expr_name);
  static int calc_file_column_idx(const ObString &column_name, uint64_t &file_column_idx);
  static int build_file_column_expr_for_csv(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const common::ObString &table_name,
    const common::ObString &column_name,
    int64_t column_idx,
    ObRawExpr *&expr,
    const ObExternalFileFormat &format);
  static int build_file_column_expr_for_partition_list_col(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const common::ObString &table_name,
    const common::ObString &column_name,
    int64_t column_idx,
    ObRawExpr *&expr,
    const ObColumnSchemaV2 *generated_column);
  static int build_file_column_expr_for_file_url(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const common::ObString &table_name,
    const common::ObString &column_name,
    ObRawExpr *&expr);

  static int build_file_row_expr_for_parquet(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const common::ObString &table_name,
    const common::ObString &column_name,
    ObRawExpr *&expr);
  static int build_file_column_expr_for_parquet(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const common::ObString &table_name,
    const common::ObString &column_name,
    ObRawExpr *get_path_expr,
    ObRawExpr *cast_expr,
    const ObColumnSchemaV2 *generated_column,
    ObRawExpr *&expr);
  //only used for DDL resolver, resolve a PSEUDO column expr for validation and printer not for execution
  static int resolve_external_table_column_def(ObRawExprFactory &expr_factory,
                                               const ObSQLSessionInfo &session_info,
                                               const ObQualifiedName &q_name,
                                               common::ObIArray<ObRawExpr*> &real_exprs,
                                               ObRawExpr *&expr,
                                               const ObColumnSchemaV2 *gen_col_schema = NULL);
  static bool is_external_file_column_name(const common::ObString &name);
  static bool is_external_pseudo_column_name(const common::ObString &name);
  static ObExternalFileFormat::FormatType resolve_external_file_column_type(const common::ObString &name);

  static int resolve_file_format_string_value(const ParseNode *node,
                                              const ObCharsetType &format_charset,
                                              ObResolverParams &params,
                                              ObString &result_value);
  static int get_generated_column_expr_temp(TableItem *table_item,
                                            ObIArray<ObRawExpr *> &gen_col_depend, ObIArray<ObString> &gen_col_names,
                                            ObIArray<ObColumnSchemaV2 *> &gen_col_schema,
                                            ObSqlSchemaGuard *schema_guard,
                                            common::ObIAllocator &allocator,
                                            ObRawExprFactory &expr_factory,
                                            ObSQLSessionInfo &session_info,
                                            ObDataTypeCastParams &dtc_params);
  static bool is_expr_can_be_used_in_table_function(const ObRawExpr &expr);
  static int resolver_param(ObPlanCacheCtx &pc_ctx,
                            ObSQLSessionInfo &session,
                            const ParamStore &phy_ctx_params,
                            const stmt::StmtType stmt_type,
                            const ObCharsetType param_charset_type,
                            const ObBitSet<> &neg_param_index,
                            const ObBitSet<> &not_param_index,
                            const ObBitSet<> &must_be_positive_idx,
                            const ObPCParam *pc_param,
                            const int64_t param_idx,
                            ObObjParam &obj_param,
                            bool &is_param,
                            const bool enable_decimal_int);
  static int check_keystore_status(const uint64_t tenant_id, ObSchemaChecker &schema_checker);
  static int check_encryption_name(common::ObString &encryption_name, bool &need_encrypt);
  static int check_not_supported_tenant_name(const common::ObString &tenant_name);
  static int check_allowed_alter_operations_for_mlog(const uint64_t tenant_id,
                                                  const obrpc::ObAlterTableArg &arg,
                                                  const share::schema::ObTableSchema &table_schema);
  static int fast_get_param_type(const ParseNode &parse_node,
                                 const ParamStore *param_store,
                                 const ObCollationType connect_collation,
                                 const ObCollationType nchar_collation,
                                 const ObCollationType server_collation,
                                 const bool enable_decimal_int,
                                 ObIAllocator &alloc,
                                 ObObjType &obj_type,
                                 ObCollationType &coll_type,
                                 ObCollationLevel &coll_level);
  static int create_values_table_query(ObSQLSessionInfo *session_info,
                                       ObIAllocator *allocator,
                                       ObRawExprFactory *expr_factory,
                                       ObQueryCtx *query_ctx,
                                       ObSelectStmt *select_stmt,
                                       ObValuesTableDef *table_def);

  static int64_t get_mysql_max_partition_num(const uint64_t tenant_id);
  static int check_schema_valid_for_mview(const share::schema::ObTableSchema &table_schema);
private:
  static int try_convert_to_unsiged(const ObExprResType restype,
                                    ObRawExpr& src_expr,
                                    bool& is_out_of_range);

  static int parse_interval_ym_type(char *cstr, ObDateUnitType &part_type);
  static int parse_interval_ds_type(char *cstr, ObDateUnitType &part_type);
  static int parse_interval_precision(char *cstr, int16_t &precision, int16_t default_precision);
  static double strntod(const char *str, size_t str_len, ObItemType type,
                        char **endptr, int *err);
  static bool is_overflow(const char *str, ObItemType type, double val);
  static bool is_synonymous_type(ObObjType type1, ObObjType type2);
private:
  static int check_part_value_result_type(const ObPartitionFuncType part_func_type,
                                          const ObObjType part_column_expr_type,
                                          const ObObjMeta part_res_type,
                                          const ObObjTypeClass expect_value_tc,
                                          bool is_const_expr,
                                          ObObj &part_value);
  static int deduce_expect_value_tc(const ObObjType part_column_expr_type,
                                    const ObPartitionFuncType part_func_type,
                                    const ObCollationType coll_type,
                                    const ObString &column_name,
                                    ObObjTypeClass &expect_value_tc);
  static int log_err_msg_for_partition_value(const ObQualifiedName &name);
  static int check_partition_range_value_result_type(const share::schema::ObPartitionFuncType part_type,
                                                     const ObColumnRefRawExpr &part_column_expr,
                                                     ObRawExpr &part_value_expr);
  static int rm_space_for_neg_num(ParseNode *param_node, ObIAllocator &allocator);
  static int handle_varchar_charset(ObCharsetType charset_type,
                                    ObIAllocator &allocator,
                                    ParseNode *&node);

  static int is_negative_ora_nmb(const common::ObObjParam &obj_param, bool &is_neg, bool &is_zero);
  static const common::ObString stmt_type_string[];

  // disallow construct
  ObResolverUtils();
  ~ObResolverUtils();
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RESOLVER_UTILS_H */
