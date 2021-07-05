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
#include "sql/parser/parse_node.h"
#include "sql/session/ob_sql_session_info.h"

#define LOG_WARN_IGNORE_COL_NOTFOUND(ret, fmt, args...) \
  do {                                                  \
    if (common::OB_ERR_BAD_FIELD_ERROR == ret) {        \
      LOG_DEBUG(fmt, ##args);                           \
    } else {                                            \
      LOG_WARN(fmt, ##args);                            \
    }                                                   \
  } while (0);

namespace oceanbase {
namespace sql {
class ObRoutineMatchInfo {
public:
  struct MatchInfo {
    MatchInfo() : need_cast_(false), src_type_(ObMaxType), dest_type_(ObMaxType)
    {}
    MatchInfo(bool need_cast, ObObjType src_type, ObObjType dst_type)
        : need_cast_(need_cast), src_type_(src_type), dest_type_(dst_type)
    {}

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
    for (int64_t i = 0; i < match_info_.count(); ++i) {
      has_unknow_type = has_unknow_type || ObUnknownType == match_info_.at(i).src_type_;
    }
    return has_unknow_type;
  }

  ObObjType get_type(int i)
  {
    return match_info_.at(i).dest_type_;
  }

public:
  common::ObSEArray<MatchInfo, 16> match_info_;

  TO_STRING_KV(K_(match_info));
};
struct ObResolverUtils {
  enum RangeElementsNode { PARTITION_NAME_NODE = 0, PARTITION_ELEMENT_NODE = 1 };
  static const int NAMENODE = 1;
  static ObItemType item_type_;

public:
  static int get_all_function_table_column_names(
      const TableItem& table_item, ObResolverParams& params, ObIArray<ObString>& column_names);
  static int check_function_table_column_exist(
      const TableItem& table_item, ObResolverParams& params, const ObString& column_name);
  static int resolve_extended_type_info(const ParseNode& str_list_node, ObIArray<ObString>& type_info_array);
  // type_infos is %ori_cs_type, need convert to %cs_type first
  static int check_extended_type_info(common::ObIAllocator& alloc, ObIArray<ObString>& type_infos,
      ObCollationType ori_cs_type, const ObString& col_name, ObObjType col_type, ObCollationType cs_type,
      ObSQLMode sql_mode);
  static int check_duplicates_in_type_infos(const ObIArray<common::ObString>& type_infos, const ObString& col_name,
      ObObjType col_type, ObCollationType cs_type, ObSQLMode sql_mode, int32_t& dup_cnt);
  static int check_max_val_count(
      common::ObObjType type, const common::ObString& col_name, int64_t val_cnt, int32_t dup_cnt);

  static int resolve_column_ref(const ParseNode* node, const common::ObNameCaseMode mode, ObQualifiedName& column_ref);
  static int resolve_obj_access_ref_node(
      ObRawExprFactory& expr_factory, const ParseNode* node, ObQualifiedName& q_name);

  static int resolve_external_param_info(ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>& param_info,
      ObRawExprFactory& expr_factory, int64_t& prepare_param_count, ObRawExpr*& expr);
  static int resolve_stmt_type(const ParseResult& result, stmt::StmtType& type);
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
  static int resolve_const(const ParseNode* node, const stmt::StmtType stmt_type, common::ObIAllocator& allocator,
      const common::ObCollationType connection_collation, const ObCollationType nchar_collation,
      const common::ObTimeZoneInfo* tz_info, common::ObObjParam& val, const bool is_paramlize,
      common::ObString& literal_prefix, const ObLengthSemantics default_length_semantics,
      const common::ObCollationType server_collation, ObExprInfo* parents_expr_info = NULL);

  static int resolve_data_type(const ParseNode& type_node, const common::ObString& ident_name,
      common::ObDataType& data_type, const int is_oracle_mode /*1:Oracle, 0:MySql */,
      const ObSessionNLSParams& nls_session_param);

  static int resolve_str_charset_info(const ParseNode& type_node, common::ObDataType& data_type);

  static int resolve_timestamp_node(const bool is_set_null, const bool is_set_default,
      const bool is_first_timestamp_column, ObSQLSessionInfo* session, share::schema::ObColumnSchemaV2& column);

  static int add_column_ids_without_duplicate(const uint64_t column_id, common::ObIArray<uint64_t>* array);
  static int resolve_columns_for_const_expr(
      ObRawExpr*& expr, ObArray<ObQualifiedName>& columns, ObResolverParams& resolve_params);
  static int resolve_const_expr(
      ObResolverParams& params, const ParseNode& node, ObRawExpr*& const_expr, common::ObIArray<ObVarInfo>* var_infos);

  // check unique indx cover partition column
  static int check_unique_index_cover_partition_column(
      const share::schema::ObTableSchema& table_schema, const obrpc::ObCreateIndexArg& arg);

  // Get index columns ids from array of ObColumnSortItem
  static int get_index_column_ids(const share::schema::ObTableSchema& table_schema,
      const common::ObIArray<obrpc::ObColumnSortItem>& columns, common::ObIArray<uint64_t>& column_ids);

  // unique idx need cover partition columns
  static int unique_idx_covered_partition_columns(const share::schema::ObTableSchema& table_schema,
      const common::ObIArray<uint64_t>& index_columns, const common::ObPartitionKeyInfo& partition_info);

  static int get_collation_type_of_names(
      const ObSQLSessionInfo* session_info, const ObNameTypeClass type_class, common::ObCollationType& cs_type);
  static int name_case_cmp(const ObSQLSessionInfo* session_info, const common::ObString& name,
      const common::ObString& name_other, const ObNameTypeClass type_class, bool& is_equal);
  static int check_column_name(const ObSQLSessionInfo* session_info, const ObQualifiedName& q_name,
      const ObColumnRefRawExpr& col_ref, bool& is_hit);
  static int resolve_partition_list_value_expr(ObResolverParams& params, const ParseNode& node,
      const common::ObString& part_name, const share::schema::ObPartitionFuncType part_type,
      const common::ObIArray<ObRawExpr*>& part_func_exprs, common::ObIArray<ObRawExpr*>& part_value_expr_array,
      const bool& in_tablegroup = false);
  static int resolve_partition_list_value_expr(ObResolverParams& params, const ParseNode& node,
      const common::ObString& part_name, const share::schema::ObPartitionFuncType part_type, int64_t& expr_num,
      common::ObIArray<ObRawExpr*>& part_value_expr_array, const bool& in_tablegroup = false);
  static int resolve_columns_for_partition_range_value_expr(ObRawExpr*& expr, ObArray<ObQualifiedName>& columns);
  static int resolve_partition_range_value_expr(ObResolverParams& params, const ParseNode& node,
      const common::ObString& part_name, const share::schema::ObPartitionFuncType part_type,
      const ObRawExpr& part_func_expr, ObRawExpr*& part_value_expr, const bool& in_tablegroup = false);
  static int resolve_partition_range_value_expr(ObResolverParams& params, const ParseNode& node,
      const common::ObString& part_name, const share::schema::ObPartitionFuncType part_type,
      ObRawExpr*& part_value_expr, const bool& in_tablegroup = false);
  static int resolve_columns_for_partition_expr(ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns,
      const share::schema::ObTableSchema& tbl_schema, share::schema::ObPartitionFuncType part_func_type,
      int64_t partition_key_start, common::ObIArray<common::ObString>& partition_keys);
  static int resolve_partition_expr(ObResolverParams& params, const ParseNode& node,
      const share::schema::ObTableSchema& tbl_schema, share::schema::ObPartitionFuncType part_func_type,
      ObRawExpr*& part_expr, common::ObIArray<common::ObString>* part_keys);
  static int resolve_generated_column_expr(ObResolverParams& params, const common::ObString& expr_str,
      share::schema::ObTableSchema& tbl_schema, share::schema::ObColumnSchemaV2& generated_column, ObRawExpr*& expr);
  static int resolve_generated_column_expr(ObResolverParams& params, const ParseNode* node,
      share::schema::ObTableSchema& tbl_schema, share::schema::ObColumnSchemaV2& generated_column, ObRawExpr*& expr);
  static int resolve_default_expr_v2_column_expr(ObResolverParams& params, const common::ObString& expr_str,
      share::schema::ObColumnSchemaV2& default_expr_v2_column, ObRawExpr*& expr);
  static int resolve_default_expr_v2_column_expr(ObResolverParams& params, const ParseNode* node,
      share::schema::ObColumnSchemaV2& default_expr_v2_column, ObRawExpr*& expr);
  static int resolve_constraint_expr(ObResolverParams& params, const common::ObString& expr_str,
      share::schema::ObTableSchema& tbl_schema, share::schema::ObConstraint& constraint, ObRawExpr*& expr);
  static int resolve_check_constraint_expr(ObResolverParams& params, const ParseNode* node,
      const share::schema::ObTableSchema& tbl_schema, share::schema::ObConstraint& constraint, ObRawExpr*& expr,
      const share::schema::ObColumnSchemaV2* column_schema = NULL);
  static int build_partition_key_expr(ObResolverParams& params, const share::schema::ObTableSchema& table_schema,
      ObRawExpr*& partition_key_expr, common::ObIArray<ObQualifiedName>* qualified_names,
      const bool is_key_implicit_v2);
  static int create_generate_table_column(
      ObRawExprFactory& expr_factory, const TableItem& table_item, uint64_t column_id, ColumnItem& col_item);
  static int check_partition_value_expr_for_range(const common::ObString& part_name, const ObRawExpr& part_func_expr,
      ObRawExpr& part_value_expr, const share::schema::ObPartitionFuncType part_type,
      const bool& in_tablegroup = false);
  static int check_partition_value_expr_for_range(const common::ObString& part_name, ObRawExpr& part_value_expr,
      const share::schema::ObPartitionFuncType part_type, const bool& in_tablegroup = false);
  static int check_valid_column_for_hash_or_range(
      const ObRawExpr& part_expr, const share::schema::ObPartitionFuncType part_func_type);
  static int check_partition_expr_for_hash_or_range(
      ObRawExpr& part_expr, const share::schema::ObPartitionFuncType part_func_type, const bool& in_tablegroup = false);
  static int check_partition_expr_for_oracle_hash(
      ObRawExpr& part_expr, const share::schema::ObPartitionFuncType part_type, const bool& in_tablegroup = false);

  // for materialized view
  static int fill_table_ids_for_mv(ObSelectStmt* select_stmt, ObSchemaChecker* schema_checker);

  static int make_columns_for_materialized_view(
      const ObMaterializedViewContext& ctx, ObSchemaChecker& schema_checker, share::schema::ObTableSchema& view_schema);
  static int make_columns_for_cte_view(const ObMaterializedViewContext& ctx, ObSchemaChecker& schema_checker,
      share::schema::ObTableSchema& view_schema, common::ObArray<ObString> cte_col_names);
  static int check_materialized_view_limitation(ObSelectStmt& stmt, ObMaterializedViewContext& ctx);
  static int find_base_and_depend_table(
      ObSchemaChecker& schema_checker, ObSelectStmt& stmt, ObMaterializedViewContext& ctx);
  static int update_materialized_view_schema(
      const ObMaterializedViewContext& ctx, ObSchemaChecker& schema_checker, share::schema::ObTableSchema& view_schema);
  static bool is_valid_partition_column_type(
      const common::ObObjType type, const share::schema::ObPartitionFuncType part_type, const bool is_check_value);
  static bool is_valid_oracle_partition_data_type(const common::ObObjType type, const bool check_value);
  static int check_sync_ddl_user(ObSQLSessionInfo* session_info, bool& is_sync_ddl_user);
  static bool is_restore_user(ObSQLSessionInfo& session_info);
  static bool is_drc_user(ObSQLSessionInfo& session_info);
  static int set_sync_ddl_id_str(ObSQLSessionInfo* session_info, common::ObString& ddl_id_str);

  // for create table with fk in oracle mode
  static int check_dup_foreign_keys_exist(const common::ObSArray<obrpc::ObCreateForeignKeyArg>& fk_args);
  // for alter table add fk in oracle mode
  static int check_dup_foreign_keys_exist(const common::ObIArray<share::schema::ObForeignKeyInfo>& fk_infos,
      const common::ObIArray<uint64_t>& child_column_ids, const common::ObIArray<uint64_t>& parent_column_ids,
      const uint64_t parent_table_id);
  // for create table with fk in oracle mode
  static bool is_match_columns_with_order(const common::ObIArray<ObString>& child_columns_1,
      const common::ObIArray<ObString>& parent_columns_1, const common::ObIArray<ObString>& child_columns_2,
      const common::ObIArray<ObString>& parent_columns_2);
  // for alter table add fk in oracle mode
  static bool is_match_columns_with_order(const common::ObIArray<uint64_t>& child_column_ids_1,
      const common::ObIArray<uint64_t>& parent_column_ids_1, const common::ObIArray<uint64_t>& child_column_ids_2,
      const common::ObIArray<uint64_t>& parent_column_ids_2);
  static int foreign_key_column_match_uk_pk_column(const share::schema::ObTableSchema& parent_table_schema,
      ObSchemaChecker& schema_checker, common::ObIArray<common::ObString>& parent_columns,
      common::ObSArray<obrpc::ObCreateIndexArg>& index_arg_list, obrpc::ObCreateForeignKeyArg& arg, bool& is_match);
  static int check_match_columns(
      const common::ObIArray<ObString>& parent_columns, const common::ObIArray<ObString>& key_columns, bool& is_match);
  static int check_match_columns_strict(
      const ObIArray<ObString>& columns_array_1, const ObIArray<ObString>& columns_array_2, bool& is_match);
  static int check_match_columns_strict_with_order(const share::schema::ObTableSchema* index_table_schema,
      const obrpc::ObCreateIndexArg& create_index_arg, bool& is_match);
  static int check_pk_idx_duplicate(const share::schema::ObTableSchema& table_schema,
      const obrpc::ObCreateIndexArg& create_index_arg, const ObIArray<ObString>& input_index_columns_name,
      bool& is_match);
  static int check_foreign_key_columns_type(const share::schema::ObTableSchema& child_table_schema,
      const share::schema::ObTableSchema& parent_table_schema, common::ObIArray<common::ObString>& child_columns,
      common::ObIArray<common::ObString>& parent_columns, const share::schema::ObColumnSchemaV2* column = NULL);
  static int get_columns_name_from_index_table_schema(
      const share::schema::ObTableSchema& index_table_schema, ObIArray<ObString>& index_columns_name);
  static int transform_func_sys_to_udf(common::ObIAllocator* allocator, const ParseNode* func_sys,
      const common::ObString& db_name, const common::ObString& pkg_name, ParseNode*& func_udf);
  static int set_direction_by_mode(const ParseNode& sort_node, OrderItem& order_item);
  static int resolve_string(const ParseNode* node, common::ObString& string);

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
  static int uv_check_basic(ObSelectStmt& stmt, const bool is_insert);

  static int check_select_item_subquery(ObSelectStmt& stmt, bool& has_subquery, bool& has_dependent_subquery,
      const uint64_t base_tid, bool& ref_update_table);

  // mysql updatable view check rule:
  //
  //    Subquery in the select list:
  //    Nondependent subqueries in the select list fail for INSERT, but are okay for UPDATE, DELETE.
  //    For dependent subqueries in the select list, no data change statements are permitted.
  static int uv_check_select_item_subquery(
      const TableItem& table_item, bool& has_subquery, bool& has_dependent_subquery, bool& ref_update_table);

  static int check_table_referred(ObSelectStmt& stmt, const uint64_t base_tid, bool& referred);
  // mysql updatable view check rule:
  //     Subquery in the WHERE clause that refers to a table in the FROM clause
  static int uv_check_where_subquery(const TableItem& table_item, bool& ref_update_table);

  static int check_has_non_inner_join(ObSelectStmt& stmt, bool& has_non_inner_join);
  static int uv_check_has_non_inner_join(const TableItem& table_item, bool& has_non_inner_join);

  // check duplicate base column in the top view, or has none column reference select items.
  static int uv_check_dup_base_col(const TableItem& table_item, bool& has_dup, bool& has_nol_col_ref);

  static int uv_mysql_insertable_join(const TableItem& table_item, const uint64_t base_tid, bool& insertable);

  // statement has unremovable distinct is not updatable in oracle.
  static int uv_check_oracle_distinct(
      const TableItem& table_item, ObSQLSessionInfo& session_info, ObSchemaChecker& schema_checker, bool& has_distinct);
  static void set_stmt_type(const ObItemType item_type)
  {
    ObResolverUtils::item_type_ = item_type;
  }

  static common::ObString get_stmt_type_string(stmt::StmtType stmt_type);

  static ParseNode* get_select_into_node(const ParseNode& node);

  static int get_user_var_value(const ParseNode* node, ObSQLSessionInfo* session_info, ObObj& value);
  static int check_duplicated_column(ObSelectStmt& select_stmt, bool can_skip = false);
  static void escape_char_for_oracle_mode(ObString& str);

private:
  static int check_and_generate_column_name(const ObMaterializedViewContext& ctx, char* buf, const int64_t buf_len,
      const uint64_t table_id, const uint64_t column_id, const ObString& col_name, ObString& new_col_name);

  static int add_column_for_schema(const share::schema::ObTableSchema& ori_table, const uint64_t ori_col_id,
      const uint64_t new_col_id, const ObString& new_col_name, const bool is_rowkey,
      share::schema::ObTableSchema& view_schema);

  static int try_convert_to_unsiged(const ObExprResType restype, ObRawExpr& src_expr, bool& is_out_of_range);

  static int parse_interval_ym_type(char* cstr, ObDateUnitType& part_type);
  static int parse_interval_ds_type(char* cstr, ObDateUnitType& part_type);
  static int parse_interval_precision(char* cstr, int16_t& precision, int16_t default_precision);
  static bool is_synonymous_type(ObObjType type1, ObObjType type2)
  {
    bool ret = false;
    if (lib::is_oracle_mode()) {
      if (type1 == ObNumberType && type2 == ObNumberFloatType) {
        ret = true;
      } else if (type1 == ObNumberFloatType && type2 == ObNumberType) {
        ret = true;
      }
    }
    return ret;
  }

private:
  static int log_err_msg_for_partition_value(const ObQualifiedName& name);
  static int check_partition_range_value_result_type(const share::schema::ObPartitionFuncType part_type,
      const ObColumnRefRawExpr& part_column_expr, ObRawExpr& part_value_expr);
  static const common::ObString stmt_type_string[];

  // disallow construct
  ObResolverUtils();
  ~ObResolverUtils();
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_RESOLVER_UTILS_H */
