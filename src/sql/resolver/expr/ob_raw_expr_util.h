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

#ifndef _OB_RAW_EXPR_UTIL_H
#define _OB_RAW_EXPR_UTIL_H 1

#include "sql/resolver/expr/ob_raw_expr.h"
#include "common/row/ob_row_desc.h"
#include "sql/parser/parse_node.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "share/schema/ob_column_schema.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/hash/ob_hashset.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase {
namespace sql {
class ObBasicSessionInfo;
class ObStmt;
class ObDMLStmt;
struct ColumnItem;
struct ObGroupbyExpr;
class ObResolverUtils;
class ObSelectIntoItem;

template <class T>
class UniqueSetWrapAllocer {
public:
  UniqueSetWrapAllocer() : allocator_(NULL)
  {}
  UniqueSetWrapAllocer(ObIAllocator& alloc) : allocator_(&alloc)
  {}

  T* alloc()
  {
    T* ret = static_cast<T*>(allocator_->alloc(sizeof(T)));
    if (NULL != ret) {
      new (ret) T();
    }

    return ret;
  }
  void free(T* data)
  {
    if (NULL == data || NULL == allocator_) {
      SQL_LOG(WARN, "invalid param null pointer", KP(data), KP(allocator_));
    } else {
      data->~T();
      allocator_->free(data);
    }
  }
  void inc_ref()
  {}
  void dec_ref()
  {}

private:
  ObIAllocator* allocator_;
};

class ObRawExprUniqueSet {
public:
  typedef UniqueSetWrapAllocer<typename common::hash::HashSetTypes<uint64_t>::AllocType> NodeAllocator;
  typedef common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode, common::hash::hash_func<uint64_t>,
      common::hash::equal_to<uint64_t>, NodeAllocator, common::hash::NormalPointer, common::ObWrapperAllocator,
      4 /*EXTEND_RATIO*/>
      ExprPtrSet;

  ObRawExprUniqueSet(ObIAllocator& alloc) : node_allocator_(alloc), bucket_allocator_(alloc)
  {}
  virtual ~ObRawExprUniqueSet()
  {}

  virtual int init(int64_t bucket_num = 128)
  {
    return expr_set_.create(bucket_num, &node_allocator_, &bucket_allocator_);
  }
  int64_t count() const
  {
    return expr_array_.count();
  }
  template <typename RawExprType>
  int append(RawExprType* expr);
  template <typename RawExprType>
  int append(const ObIArray<RawExprType*>& exprs);
  const ObIArray<ObRawExpr*>& get_expr_array() const
  {
    return expr_array_;
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObRawExprUniqueSet);

private:
  NodeAllocator node_allocator_;
  common::ObWrapperAllocator bucket_allocator_;
  ExprPtrSet expr_set_;
  ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> expr_array_;
};

template <typename RawExprType>
int ObRawExprUniqueSet::append(RawExprType* expr)
{
  int ret = expr_set_.exist_refactored(reinterpret_cast<uint64_t>(expr));
  if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
    // do nothing
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(expr_array_.push_back(expr))) {
      SQL_LOG(WARN, "fail to append expr", K(ret));
    } else if (OB_FAIL(expr_set_.set_refactored(reinterpret_cast<uint64_t>(expr)))) {
      SQL_LOG(WARN, "fail to append expr", K(ret));
    }
  } else {
    SQL_LOG(WARN, "fail to search raw expr", K(ret));
  }

  return ret;
}

template <typename RawExprType>
int ObRawExprUniqueSet::append(const ObIArray<RawExprType*>& exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(append(exprs.at(i)))) {
      SQL_LOG(WARN, "fail to append expr", K(ret));
    }
  }
  return ret;
}

class ObRawExprUtils {
public:
  /**
   * make expression from string
   *
   * @param expr_str [in] expression string
   * @param buf_len [in] expression string length
   * @param allocator [in] allocator used by parser
   * @param resolve_ctx [in] expression resolver context
   * @param expr [out] resolved expression
   * @param columns [out] columns in the expr
   * @param sys_vars [out] system variables in the expr
   * @param sub_query_info [out] subqueries in the expr
   *
   * @return
   */
  static int make_raw_expr_from_str(const char* expr_str, const int64_t buf_len, ObExprResolveContext& resolve_ctx,
      ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns, common::ObIArray<ObVarInfo>& sys_vars,
      common::ObIArray<ObSubQueryInfo>* sub_query_info, common::ObIArray<ObAggFunRawExpr*>& aggr_exprs,
      common::ObIArray<ObWinFunRawExpr*>& win_exprs);
  static int make_raw_expr_from_str(const common::ObString& expr_str, ObExprResolveContext& resolve_ctx,
      ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& column, common::ObIArray<ObVarInfo>& sys_vars,
      common::ObIArray<ObSubQueryInfo>* sub_query_info, common::ObIArray<ObAggFunRawExpr*>& aggr_exprs,
      common::ObIArray<ObWinFunRawExpr*>& win_exprs);
  static int parse_default_expr_from_str(const common::ObString& expr_str, common::ObCollationType expr_str_cs_type,
      common::ObIAllocator& allocator, const ParseNode*& node);
  static int parse_expr_node_from_str(
      const common::ObString& expr_str, common::ObIAllocator& allocator, const ParseNode*& node);
  static int parse_bool_expr_node_from_str(
      const common::ObString& expr_str, common::ObIAllocator& allocator, const ParseNode*& node);
  static int build_check_constraint_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
      const ParseNode& node, ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns);
  static int build_generated_column_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
      const ParseNode& node, ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns,
      const ObSchemaChecker* schema_checker = NULL);
  static int build_generated_column_expr(const common::ObString& expr_str, ObRawExprFactory& expr_factory,
      const ObSQLSessionInfo& session_info, ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns,
      const ObSchemaChecker* schema_checker = NULL);
  static int build_generated_column_expr(const common::ObString& expr_str, ObRawExprFactory& expr_factory,
      const ObSQLSessionInfo& session_info, const share::schema::ObTableSchema& table_schema, ObRawExpr*& expr,
      const ObSchemaChecker* schema_checker = NULL);
  static int build_generated_column_expr(const common::ObString& expr_str, ObRawExprFactory& expr_factory,
      const ObSQLSessionInfo& session_info, uint64_t table_id, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2& gen_col_schema, ObRawExpr*& expr,
      const ObSchemaChecker* schema_checker = NULL);
  static int build_raw_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info, const ParseNode& node,
      ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns, common::ObIArray<ObVarInfo>& sys_vars,
      common::ObIArray<ObAggFunRawExpr*>& aggr_exprs, common::ObIArray<ObWinFunRawExpr*>& win_exprs,
      common::ObIArray<ObSubQueryInfo>& sub_query_info, common::ObIArray<ObOpRawExpr*>& op_exprs,
      bool is_prepare_protocol = false);
  static int build_raw_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
      ObSchemaChecker* schema_checker, ObStmtScope current_scope, ObStmt* stmt, const ParamStore* param_list,
      common::ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>* external_param_info, const ParseNode& node,
      ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns, common::ObIArray<ObVarInfo>& sys_vars,
      common::ObIArray<ObAggFunRawExpr*>& aggr_exprs, common::ObIArray<ObWinFunRawExpr*>& win_exprs,
      common::ObIArray<ObSubQueryInfo>& sub_query_info, common::ObIArray<ObOpRawExpr*>& op_exprs,
      bool is_prepare_protocol /*= false*/);
  static bool is_same_raw_expr(const ObRawExpr* src, const ObRawExpr* dst);
  /// replace all `from' to `to' in the raw_expr
  static int replace_all_ref_column(ObRawExpr*& raw_expr, const common::ObIArray<ObRawExpr*>& exprs, int64_t& offset);
  // if %expr_factory is not NULL, will deep copy %to expr. default behavior is shallow copy
  static int replace_ref_column(
      ObRawExpr*& raw_expr, ObRawExpr* from, ObRawExpr* to, ObRawExprFactory* expr_factory = NULL);
  static int replace_level_column(ObRawExpr*& raw_expr, ObRawExpr* to, bool& replaced);
  static int replace_ref_column(common::ObIArray<ObRawExpr*>& exprs, ObRawExpr* from, ObRawExpr* to);
  static bool all_column_exprs(const common::ObIArray<ObRawExpr*>& exprs);
  /// extract column exprs from the raw expr
  static int extract_column_exprs(const ObRawExpr* raw_expr, common::ObIArray<ObRawExpr*>& column_exprs);
  static int extract_column_exprs(
      const common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<ObRawExpr*>& column_exprs);
  static int extract_column_ids(const ObIArray<ObRawExpr*>& exprs, common::ObIArray<uint64_t>& column_ids);
  static int extract_column_ids(const ObRawExpr* raw_expr, common::ObIArray<uint64_t>& column_ids);
  static int extract_table_ids(const ObRawExpr* raw_expr, common::ObIArray<uint64_t>& table_ids);
  static int extract_param_idxs(const ObRawExpr* expr, common::ObIArray<int64_t>& param_idxs);
  static int find_alias_expr(ObRawExpr* expr, ObAliasRefRawExpr*& alias_expr);
  template <typename T>
  static int copy_exprs(ObRawExprFactory& expr_factory, const common::ObIArray<T*>& input_exprs,
      common::ObIArray<T*>& output_exprs, const uint64_t copy_types, bool use_new_allocator = false);
  static int copy_expr(ObRawExprFactory& expr_factory, const ObRawExpr* origin, ObRawExpr*& dest,
      const uint64_t copy_types, bool use_new_allocator = false);

  // try add cast expr above %expr , set %dst_expr to &expr if no cast added.
  static int try_add_cast_expr_above(ObRawExprFactory* expr_factory, const ObSQLSessionInfo* session,
      ObRawExpr& src_expr, const ObExprResType& dst_type, ObRawExpr*& new_expr);
  static int try_add_cast_expr_above(ObRawExprFactory* expr_factory, const ObSQLSessionInfo* session, ObRawExpr& expr,
      const ObExprResType& dst_type, const ObCastMode& cm, ObRawExpr*& new_expr);
  static int copy_exprs(ObRawExprFactory& expr_factory, const ObIArray<ObGroupbyExpr>& origin,
      ObIArray<ObGroupbyExpr>& dest, const uint64_t copy_types, bool use_new_allocator = false);
  // old engine: create cast expr directly. %use_def_cm and %cm is useless.
  //             cast mode will setup in ObExprCast::calc_result2().
  // new engine: may create more cast exprs to handle non-system-collation string.
  //             e.g.: utf16->number: utf16->utf8->number (two cast expr)
  //                   utf8_bin->number: utf8->number (just one cat expr)
  //             if %use_def_cm is not true, %cm will be set as cast mode for cast exprs.
  //             NOTE: IS_INNER_ADDED_EXPR flag will be set.
  static int create_cast_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, const ObExprResType& dst_type,
      ObSysFunRawExpr*& func_expr, const ObSQLSessionInfo* session_info, bool use_def_cm = true,
      ObCastMode cm = CM_NONE);
  static void need_extra_cast(const ObExprResType& src_type, const ObExprResType& dst_type,
      bool& need_extra_cast_for_src_type, bool& need_extra_cast_for_dst_type);
  static int setup_extra_cast_utf8_type(const ObExprResType& type, ObExprResType& utf8_type);

  static int erase_inner_added_exprs(ObRawExpr* src_expr, ObRawExpr*& out_expr);

  // erase implicit cast which added for operand casting.
  static int erase_operand_implicit_cast(ObRawExpr* src, ObRawExpr*& out);

  static int create_to_type_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, const common::ObObjType& dst_type,
      ObSysFunRawExpr*& to_type, ObSQLSessionInfo* session_info);
  static int create_substr_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, ObRawExpr* first_expr,
      ObRawExpr* second_expr, ObRawExpr* third_expr, ObSysFunRawExpr*& out_expr);
  static int create_type_to_str_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, ObSysFunRawExpr*& out_expr,
      ObSQLSessionInfo* session_info, bool is_type_to_str);
  static int create_type_to_str_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, int32_t expr_level,
      ObSysFunRawExpr*& out_expr, ObSQLSessionInfo* session_info, bool is_type_to_str);
  static int create_exec_param_expr(ObStmt* stmt, ObRawExprFactory& expr_factory, ObRawExpr*& src_expr,
      ObSQLSessionInfo* session_info, std::pair<int64_t, ObRawExpr*>& init_expr);
  static int create_param_expr(
      ObRawExprFactory& expr_factory, int64_t param_idx, ObRawExpr*& expr, bool is_exec_param = false);
  static int build_trim_expr(const share::schema::ObColumnSchemaV2* column_schema, ObRawExprFactory& expr_factory,
      ObSQLSessionInfo* session_info, ObRawExpr*& expr);
  static bool need_column_conv(const ColumnItem& column, ObRawExpr& expr);
  static int build_pad_expr(ObRawExprFactory& expr_factory, bool is_char,
      const share::schema::ObColumnSchemaV2* column_schema, ObRawExpr*& expr, sql::ObSQLSessionInfo* session_info);
  static bool need_column_conv(const ObExprResType& expected_type, const ObRawExpr& expr);
  static int build_column_conv_expr(ObRawExprFactory& expr_factory,
      const share::schema::ObColumnSchemaV2* column_schema, ObRawExpr*& expr,
      const sql::ObSQLSessionInfo* session_info);
  static int build_column_conv_expr(ObRawExprFactory& expr_factory, common::ObIAllocator& allocator,
      const ObColumnRefRawExpr& col_expr, ObRawExpr*& expr, const ObSQLSessionInfo* session_info);
  static int build_column_conv_expr(const ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory,
      const common::ObObjType& type, const common::ObCollationType& collation, const int64_t& accuracy,
      const bool& is_nullable, const common::ObString* column_conv_info,
      const common::ObIArray<common::ObString>* type_infos, ObRawExpr*& expr);
  static int build_var_int_expr(ObRawExprFactory& expr_factory, ObConstRawExpr*& expr);
  static int build_default_expr(ObRawExprFactory& expr_factory, const common::ObObjType& type,
      const common::ObCollationType& collation, const int64_t& accuracy, ObRawExpr*& expr);
  static int build_const_int_expr(
      ObRawExprFactory& expr_factory, common::ObObjType type, int64_t int_value, ObConstRawExpr*& expr);
  static int build_const_number_expr(
      ObRawExprFactory& expr_factory, ObObjType type, number::ObNumber value, ObConstRawExpr*& expr);
  static int build_variable_expr(ObRawExprFactory& expr_factory, const ObExprResType& result_type, ObVarRawExpr*& expr);
  static int build_const_bool_expr(ObRawExprFactory* expr_factory, ObRawExpr*& expr, bool b_value);
  static int build_const_string_expr(ObRawExprFactory& expr_factory, common::ObObjType type,
      const common::ObString& string_value, common::ObCollationType cs_type, ObConstRawExpr*& expr);
  static int build_null_expr(ObRawExprFactory& expr_factory, ObRawExpr*& expr);
  static int build_nvl_expr(ObRawExprFactory& expr_factory, const ColumnItem* column_item, ObRawExpr*& expr);
  static int build_lnnvl_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr, ObRawExpr*& lnnvl_expr);
  static int build_equal_last_insert_id_expr(
      ObRawExprFactory& expr_factory, ObRawExpr*& expr, ObSQLSessionInfo* session);
  static int build_get_user_var(ObRawExprFactory& expr_factory, const common::ObString& var_name, ObRawExpr*& expr,
      const ObSQLSessionInfo* session_info = NULL, ObQueryCtx* query_ctx = NULL,
      common::ObIArray<ObUserVarIdentRawExpr*>* user_var_exprs = NULL);
  static int build_get_sys_var(ObRawExprFactory& expr_factory, const common::ObString& var_name,
      share::ObSetVar::SetScopeType var_scope, ObRawExpr*& expr, const ObSQLSessionInfo* session_info = NULL);
  static int build_get_package_var(ObRawExprFactory& expr_factory, uint64_t package_id, int64_t var_idx,
      ObExprResType* result_type, ObRawExpr*& expr, const ObSQLSessionInfo* session_info);
  static int build_calc_part_id_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
      uint64_t ref_table_id, share::schema::ObPartitionLevel part_level, ObRawExpr* part_expr, ObRawExpr* subpart_expr,
      ObRawExpr*& expr);
  static int build_get_subprogram_var(ObRawExprFactory& expr_factory, uint64_t package_id, uint64_t routine_id,
      int64_t var_idx, ObExprResType* result_type, ObRawExpr*& expr, const ObSQLSessionInfo* session_info);
  template <typename T>
  static bool find_expr(const common::ObIArray<T>& exprs, const ObRawExpr* expr);
  template <typename T>
  static int64_t get_expr_idx(const common::ObIArray<T>& exprs, const ObRawExpr* expr);
  static int create_equal_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, const ObRawExpr* val_ref,
      const ObRawExpr* col_ref, ObRawExpr*& expr);
  static int create_double_op_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info,
      ObItemType expr_type, ObRawExpr*& add_expr, const ObRawExpr* left_expr, const ObRawExpr* right_expr);
  static int make_set_op_expr(ObRawExprFactory& expr_factory, int64_t idx, ObItemType set_op_type,
      const ObExprResType& res_type, ObSQLSessionInfo* session_info, ObRawExpr*& out_expr);
  static int merge_variables(const common::ObIArray<ObVarInfo>& src_vars, common::ObIArray<ObVarInfo>& dst_vars);
  static int get_item_count(const ObRawExpr* expr, int64_t& count);
  static int get_array_param_index(const ObRawExpr* expr, int64_t& param_index);
  static int build_column_expr(ObRawExprFactory& expr_factory, const share::schema::ObColumnSchemaV2& column_schema,
      ObColumnRefRawExpr*& column_expr);
  static bool is_same_column_ref(const ObRawExpr* column_ref1, const ObRawExpr* column_ref2);
  static int32_t get_generalized_column_level(const ObRawExpr& generalized_column);
  static int build_alias_column_expr(
      ObRawExprFactory& expr_factory, ObRawExpr* ref_expr, int32_t alias_level, ObAliasRefRawExpr*& alias_expr);
  static int build_query_output_ref(ObRawExprFactory& expr_factory, ObQueryRefRawExpr* query_ref, int64_t project_index,
      ObAliasRefRawExpr*& alias_expr);
  static int init_column_expr(const share::schema::ObColumnSchemaV2& column_schema, ObColumnRefRawExpr& column_expr);
  static uint32_t calc_column_result_flag(const share::schema::ObColumnSchemaV2& column_schema);
  static int expr_is_order_consistent(const ObRawExpr* from, const ObRawExpr* to, bool& is_consistent);
  static int exprs_contain_subquery(const common::ObIArray<ObRawExpr*>& exprs, bool& cnt_subquery);
  static int cnt_current_level_aggr_expr(const ObRawExpr* expr, int32_t cur_level, bool& cnt_aggr);
  static int cnt_current_level_aggr_expr(const ObDMLStmt* stmt, int32_t column_level, bool& cnt_aggr);
  static int function_alias(ObRawExprFactory& expr_factory, ObSysFunRawExpr*& expr);
  // extract from const value
  static int extract_int_value(const ObRawExpr* expr, int64_t& val);
  // used for enum set type
  static int need_wrap_to_string(
      common::ObObjType param_type, common::ObObjType calc_type, const bool is_same_type_need, bool& need_wrap);
  static bool contain_id(const common::ObIArray<uint64_t>& ids, const uint64_t target);
  static int clear_exprs_flag(const common::ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag flag);

  static bool has_prefix_str_expr(
      const ObRawExpr& expr, const ObColumnRefRawExpr& orig_column_expr, const ObRawExpr*& substr_expr);
  static int build_like_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, ObRawExpr* text_expr,
      ObRawExpr* pattern_expr, ObRawExpr* escape_expr, ObOpRawExpr*& like_expr);
  static int resolve_op_expr_for_oracle_implicit_cast(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info, ObOpRawExpr*& b_expr);
  static int resolve_op_expr_implicit_cast(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info,
      ObItemType op_type, ObRawExpr*& sub_expr1, ObRawExpr*& sub_expr2);
  static inline int resolve_op_expr_add_implicit_cast(ObRawExprFactory& expr_factory,
      const ObSQLSessionInfo* session_info, ObRawExpr* src_expr, const ObExprResType& dst_type,
      ObSysFunRawExpr*& func_expr);
  static int resolve_op_exprs_for_oracle_implicit_cast(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info, common::ObIArray<ObOpRawExpr*>& op_exprs);
  static int check_composite_cast(ObRawExpr*& expr, ObSchemaChecker& schema_checker);

  static int flatten_raw_exprs(const ObIArray<ObRawExpr*>& raw_exprs, ObRawExprUniqueSet& flattened_exprs,
      std::function<bool(ObRawExpr*)> filter);

  static int flatten_raw_exprs(
      const ObRawExprUniqueSet& raw_exprs, ObRawExprUniqueSet& flattened_exprs,
      std::function<bool(ObRawExpr*)> filter = [](ObRawExpr* e) { return NULL != e; });

  static int flatten_raw_expr(
      ObRawExpr* raw_expr, ObRawExprUniqueSet& flattened_exprs,
      std::function<bool(ObRawExpr*)> filter = [](ObRawExpr* e) { return NULL != e; });

  static int try_add_bool_expr(ObOpRawExpr* parent, ObRawExprFactory& expr_factory);

  static int try_add_bool_expr(ObCaseOpRawExpr* parent, ObRawExprFactory& expr_factory);

  static int try_create_bool_expr(ObRawExpr* src_expr, ObRawExpr*& out_expr, ObRawExprFactory& expr_factory);
  static bool check_need_bool_expr(const ObRawExpr* expr, bool& need_bool_expr);

  static int get_real_expr_without_cast(
      const bool is_static_typing_engine, const ObRawExpr* expr, const ObRawExpr*& out_expr);

  // build remove_const for const expr to remove const attribute.
  static int build_remove_const_expr(
      ObRawExprFactory& factory, ObSQLSessionInfo& session_info, ObRawExpr* arg, ObRawExpr*& out);
  static bool is_pseudo_column_like_expr(const ObRawExpr& expr);
  static bool is_sharable_expr(const ObRawExpr& expr);

  static int check_need_cast_expr(
      const ObExprResType& src_res_type, const ObExprResType& dst_res_type, bool& need_cast);
  static int build_add_expr(
      ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2, ObOpRawExpr*& add_expr);
  static int build_minus_expr(
      ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2, ObOpRawExpr*& add_expr);
  static int build_date_add_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2,
      ObRawExpr* param_expr3, ObSysFunRawExpr*& date_add_expr);
  static int build_date_sub_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2,
      ObRawExpr* param_expr3, ObSysFunRawExpr*& date_sub_expr);
  static int build_common_binary_op_expr(ObRawExprFactory& expr_factory, const ObItemType expect_op_type,
      ObRawExpr* param_expr1, ObRawExpr* param_expr2, ObRawExpr*& expr);
  static int build_case_when_expr(ObRawExprFactory& expr_factory, ObRawExpr* when_expr, ObRawExpr* then_expr,
      ObRawExpr* default_expr, ObRawExpr*& case_when_expr);
  static int build_is_not_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2,
      ObRawExpr* param_expr3, ObRawExpr*& is_not_expr);

  static int build_is_not_null_expr(
      ObRawExprFactory& expr_factory, ObRawExpr* param_expr, ObRawExpr*& is_not_null_expr);

  static int process_window_complex_agg_expr(ObRawExprFactory& expr_factory, const ObItemType func_type,
      ObWinFunRawExpr* win_func, ObRawExpr*& agg_expr, common::ObIArray<ObWinFunRawExpr*>* win_exprs);
  static int build_common_aggr_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
      const ObItemType expect_op_type, ObRawExpr* param_expr, ObAggFunRawExpr*& aggr_expr);

  static int build_case_when_expr_for_limit(
      ObRawExprFactory& expr_factory, ObRawExpr* limit_expr, ObRawExpr*& case_when_expr);

  static int build_not_expr(ObRawExprFactory& expr_factory, ObRawExpr* expr, ObRawExpr*& not_expr);

  static int build_or_exprs(ObRawExprFactory& expr_factory, const ObIArray<ObRawExpr*>& exprs, ObRawExpr*& or_expr);

  static int build_and_expr(ObRawExprFactory& expr_factory, const ObIArray<ObRawExpr*>& exprs, ObRawExpr*& and_expr);

  static int new_parse_node(ParseNode*& node, ObRawExprFactory& expr_factory, ObItemType type, int num);

  static int new_parse_node(
      ParseNode*& node, ObRawExprFactory& expr_factory, ObItemType type, int num, common::ObString str_val);

  static int build_rownum_expr(ObRawExprFactory& expr_factory, ObRawExpr*& rownum_expr);
  static int build_to_outfile_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info,
      const ObSelectIntoItem* into_item, const ObIArray<ObRawExpr*>& exprs, ObRawExpr*& to_outfile_expr);

private:
  static int create_real_cast_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, const ObExprResType& dst_type,
      ObSysFunRawExpr*& func_expr, const ObSQLSessionInfo* session_info);
  ObRawExprUtils();
  virtual ~ObRawExprUtils();
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprUtils);
};

template <typename T>
bool ObRawExprUtils::find_expr(const common::ObIArray<T>& exprs, const ObRawExpr* expr)
{
  return get_expr_idx(exprs, expr) != common::OB_INVALID_INDEX;
}

template <typename T>
int64_t ObRawExprUtils::get_expr_idx(const common::ObIArray<T>& exprs, const ObRawExpr* expr)
{
  int64_t expr_idx = common::OB_INVALID_INDEX;
  int64_t N = exprs.count();
  for (int64_t i = 0; common::OB_INVALID_INDEX == expr_idx && i < N; i++) {
    if (exprs.at(i) == expr) {
      expr_idx = i;
    }
  }
  return expr_idx;
}

template <typename T>
int ObRawExprUtils::copy_exprs(ObRawExprFactory& expr_factory, const common::ObIArray<T*>& input_exprs,
    common::ObIArray<T*>& output_exprs, const uint64_t copy_types, bool use_new_allocator)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); i++) {
    ObRawExpr* temp_expr = NULL;
    if (OB_ISNULL(input_exprs.at(i))) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "null expr", K(ret));
    } else if (OB_FAIL(copy_expr(expr_factory, input_exprs.at(i), temp_expr, copy_types, use_new_allocator))) {
      SQL_LOG(WARN, "failed to copy expr", K(ret));
    } else if (OB_ISNULL(temp_expr)) {
      SQL_LOG(WARN, "null expr", K(ret));
    } else {
      T* cast_expr = static_cast<T*>(temp_expr);
      if (OB_FAIL(output_exprs.push_back(cast_expr))) {
        SQL_LOG(WARN, "failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_RAW_EXPR_UTIL_H */
