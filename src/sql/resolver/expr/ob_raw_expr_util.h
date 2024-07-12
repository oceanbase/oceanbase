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
#include "share/schema/ob_trigger_info.h"

namespace oceanbase
{
namespace sql
{
class ObBasicSessionInfo;
class ObStmt;
class ObDMLStmt;
struct ColumnItem;
struct ObGroupbyExpr;
struct ObResolverUtils;
struct ObSelectIntoItem;
class ObDMLResolver;
class ObSequenceNamespaceChecker;

enum JsonArrayaggParserOffset
{
  PARSE_JSON_ARRAYAGG_DISTINCT,
  PARSE_JSON_ARRAYAGG_EXPR,
  PARSE_JSON_ARRAYAGG_FORMAT,
  PARSE_JSON_ARRAYAGG_ORDER,
  PARSE_JSON_ARRAYAGG_ON_NULL,
  PARSE_JSON_ARRAYAGG_RETURNING,
  PARSE_JSON_ARRAYAGG_STRICT,
  PARSE_JSON_ARRAYAGG_MAX_IDX
};

enum JsonObjectaggParserOffset
{
  PARSE_JSON_OBJECTAGG_KEY,
  PARSE_JSON_OBJECTAGG_VALUE,
  PARSE_JSON_OBJECTAGG_FORMAT,
  PARSE_JSON_OBJECTAGG_ON_NULL,
  PARSE_JSON_OBJECTAGG_RETURNING,
  PARSE_JSON_OBJECTAGG_STRICT,
  PARSE_JSON_OBJECTAGG_UNIQUE_KEYS,
  PARSE_JSON_OBJECTAGG_MAX_IDX
};

enum JsonAraayaggDeduceOffset
{
  DEDUCE_JSON_ARRAYAGG_EXPR,
  DEDUCE_JSON_ARRAYAGG_FORMAT,
  DEDUCE_JSON_ARRAYAGG_ON_NULL,
  DEDUCE_JSON_ARRAYAGG_RETURNING,
  DEDUCE_JSON_ARRAYAGG_STRICT,
  DEDUCE_JSON_ARRAYAGG_MAX_IDX
};

template <class T>
class UniqueSetWrapAllocer
{
public:
  UniqueSetWrapAllocer() : allocator_(NULL) {}
  UniqueSetWrapAllocer(ObIAllocator &alloc) : allocator_(&alloc) {}

  T *alloc()
  {
    T *ret = static_cast<T *>(allocator_->alloc(sizeof(T)));
    if (NULL != ret) {
      new(ret) T();
    }

    return ret;
  }
  void free(T *data)
  {
    if (NULL == data || NULL == allocator_) {
      SQL_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid param null pointer", KP(data), KP(allocator_));
    } else {
      data->~T();
      allocator_->free(data);
    }
  }
  void inc_ref() {}
  void dec_ref() {}

private:
  ObIAllocator *allocator_;
};

class ObRawExprUniqueSet
{
public:

  ObRawExprUniqueSet(bool need_unique)
    : need_unique_(need_unique)
  {}
  virtual ~ObRawExprUniqueSet() {}

  int64_t count() const { return expr_array_.count(); }
  template<typename RawExprType>
  int append(RawExprType *expr);
  template<typename RawExprType>
  int append(const ObIArray<RawExprType *> &exprs);
  const ObIArray<ObRawExpr *> &get_expr_array() const { return expr_array_; } ;
  void reuse() { expr_array_.reuse(); }
  int flatten_and_add_raw_exprs(const ObIArray<ObRawExpr *> &raw_exprs,
                                 std::function<bool(ObRawExpr *)> filter,
                                 bool need_flatten_gen_col = true);

  int flatten_and_add_raw_exprs(const ObRawExprUniqueSet &raw_exprs,
                                bool need_flatten_gen_col = true,
                                std::function<bool(ObRawExpr *)> filter
                                  = [](ObRawExpr *e){ return NULL != e;});

  int flatten_temp_expr(ObRawExpr *raw_expr);


private:
  int flatten_and_add_raw_exprs(ObRawExpr *raw_expr,
                                bool need_flatten_gen_col = true,
                                std::function<bool(ObRawExpr *)> filter
                                  = [](ObRawExpr *e){ return NULL != e;});
  DISALLOW_COPY_AND_ASSIGN(ObRawExprUniqueSet);
private:
  ObSEArray<ObRawExpr *, 16, common::ModulePageAllocator, true> expr_array_;
  bool need_unique_;
};

template<typename RawExprType>
int ObRawExprUniqueSet::append(RawExprType *expr)
{
  int ret = OB_SUCCESS;
  if (!need_unique_) {
    if (OB_FAIL(expr_array_.push_back(expr))) {
      SQL_LOG(WARN, "fail to append expr", K(ret));
    }
  } else {
    if (expr->has_flag(IS_MARKED)) {
      // do nothing
    } else {
      if (OB_FAIL(expr_array_.push_back(expr))) {
        SQL_LOG(WARN, "fail to append expr", K(ret));
      } else if (OB_FAIL(expr->add_flag(IS_MARKED))) {
        SQL_LOG(WARN, "fail to add flag", K(ret));
      }
    }
  }

  return ret;
}

template<typename RawExprType>
int ObRawExprUniqueSet::append(const ObIArray<RawExprType *> &exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(append(exprs.at(i)))) {
      SQL_LOG(WARN, "fail to append expr", K(ret));
    }
  }
  return ret;
}

class ObRawExprUtils
{
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
  static int make_raw_expr_from_str(const char *expr_str, const int64_t buf_len,
                                    ObExprResolveContext &resolve_ctx,
                                    ObRawExpr *&expr,
                                    common::ObIArray<ObQualifiedName> &columns,
                                    common::ObIArray<ObVarInfo> &sys_vars,
                                    common::ObIArray<ObSubQueryInfo> *sub_query_info,
                                    common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                    common::ObIArray<ObWinFunRawExpr*> &win_exprs,
                                    common::ObIArray<ObUDFInfo> &udf_info);
  static int make_raw_expr_from_str(const common::ObString &expr_str,
                                    ObExprResolveContext &resolve_ctx,
                                    ObRawExpr *&expr,
                                    common::ObIArray<ObQualifiedName> &column,
                                    common::ObIArray<ObVarInfo> &sys_vars,
                                    common::ObIArray<ObSubQueryInfo> *sub_query_info,
                                    common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                    common::ObIArray<ObWinFunRawExpr*> &win_exprs,
                                    common::ObIArray<ObUDFInfo> &udf_info);
  static int parse_default_expr_from_str(const common::ObString &expr_str,
                                         ObCharsets4Parser expr_str_cs_type,
                                         common::ObIAllocator &allocator,
                                         const ParseNode *&node,
                                         bool is_for_trigger = false);
  static int parse_expr_list_node_from_str(const common::ObString &expr_str,
                                           ObCharsets4Parser expr_str_cs_type,
                                           common::ObIAllocator &allocator,
                                           const ParseNode *&node,
                                           const ObSQLMode &sql_mode);
  static int parse_expr_node_from_str(const common::ObString &expr_str,
                                      ObCharsets4Parser expr_str_cs_type,
                                      common::ObIAllocator &allocator,
                                      const ParseNode *&node,
                                      const ObSQLMode &sql_mode = 0);
  static int parse_bool_expr_node_from_str(const common::ObString &expr_str,
                                           common::ObIAllocator &allocator,
                                           const ParseNode *&node);
  static int build_check_constraint_expr(ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         const ParseNode &node,
                                         ObRawExpr *&expr,
                                         common::ObIArray<ObQualifiedName> &columns);
  static int check_deterministic(const ObRawExpr *expr,
                                 common::ObIAllocator &allocator,
                                 const ObResolverUtils::PureFunctionCheckStatus
                                   check_status = ObResolverUtils::DISABLE_CHECK);
  static int check_deterministic_single(const ObRawExpr *expr,
                                        const ObResolverUtils::PureFunctionCheckStatus
                                          check_status = ObResolverUtils::DISABLE_CHECK);
  static int build_generated_column_expr(ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         const ParseNode &node,
                                         ObRawExpr *&expr,
                                         common::ObIArray<ObQualifiedName> &columns,
                                         const ObTableSchema* new_table_schema,
                                         const bool sequence_allowed,
                                         ObDMLResolver *dml_resolver,
                                         const ObSchemaChecker *schema_checker = NULL,
                                         const ObResolverUtils::PureFunctionCheckStatus
                                           check_status = ObResolverUtils::DISABLE_CHECK,
                                         const bool need_check_simple_column = true,
                                         bool use_def_collation = false,
                                          ObCollationType connection_collation = CS_TYPE_INVALID);
  static int build_generated_column_expr(const common::ObString &expr_str,
                                         ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         ObRawExpr *&expr,
                                         common::ObIArray<ObQualifiedName> &columns,
                                         const ObTableSchema* new_table_schema,
                                         const bool sequence_allowed,
                                        ObDMLResolver *dml_resolver,
                                         const ObSchemaChecker *schema_checker = NULL,
                                         const ObResolverUtils::PureFunctionCheckStatus
                                           check_status = ObResolverUtils::DISABLE_CHECK,
                                         const bool need_check_simple_column = true);
  static int build_generated_column_expr(const common::ObString &expr_str,
                                         ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         ObSQLMode sql_mode,
                                         ObCollationType connection_collation,
                                         ObRawExpr *&expr,
                                         common::ObIArray<ObQualifiedName> &columns,
                                         const ObTableSchema* new_table_schema,
                                         const bool sequence_allowed,
                                        ObDMLResolver *dml_resolver,
                                         const ObSchemaChecker *schema_checker = NULL,
                                         const ObResolverUtils::PureFunctionCheckStatus
                                           check_status = ObResolverUtils::DISABLE_CHECK,
                                         const bool need_check_simple_column = true);
  static int build_generated_column_expr(const obrpc::ObCreateIndexArg *arg,
                                         const common::ObString &expr_str,
                                         ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         const share::schema::ObTableSchema &table_schema,
                                         ObRawExpr *&expr,
                                         const ObSchemaChecker *schema_checker = NULL,
                                         const ObResolverUtils::PureFunctionCheckStatus
                                           check_status = ObResolverUtils::DISABLE_CHECK,
                                         ObIArray<share::schema::ObColumnSchemaV2*> *resolved_cols = NULL);
  static int build_generated_column_expr(const common::ObString &expr_str,
                                         ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         uint64_t table_id,
                                         const share::schema::ObTableSchema &table_schema,
                                         const share::schema::ObColumnSchemaV2 &gen_col_schema,
                                         ObRawExpr *&expr,
                                         const bool sequence_allowed,
                                         ObDMLResolver *dml_resolver,
                                         const ObSchemaChecker *schema_checker = NULL,
                                         const ObResolverUtils::PureFunctionCheckStatus
                                           check_status = ObResolverUtils::DISABLE_CHECK);
  static int check_generated_column_expr_str(const common::ObString &expr_str,
                                             const ObSQLSessionInfo &session_info,
                                             const share::schema::ObTableSchema &table_schema);
  static int build_seq_nextval_expr(ObRawExpr *&expr,
                                    const ObSQLSessionInfo *session_info,
                                    ObRawExprFactory *expr_factory,
                                    const ObQualifiedName &q_name,
                                    uint64_t seq_id,
                                    ObDMLStmt *stmt);
  // build oracle sequence_object.currval, sequence_object.nextval expr
  static int build_seq_nextval_expr(ObRawExpr *&expr,
                                    const ObSQLSessionInfo *session_info,
                                    ObRawExprFactory *expr_factory,
                                    const ObString &database_name,
                                    const ObString &tbl_name,
                                    const ObString &col_name,
                                    uint64_t seq_id,
                                    ObDMLStmt *stmt);
  static int resolve_sequence_object(const ObQualifiedName &q_name,
                                     ObDMLResolver *dml_resolver,
                                     const ObSQLSessionInfo *session_info,
                                     ObRawExprFactory *expr_factory,
                                     ObSequenceNamespaceChecker &sequence_namespace_checker,
                                     ObRawExpr *&real_ref_expr,
                                     bool is_generated_column);
  static int build_pad_expr_recursively(ObRawExprFactory &expr_factory,
                                        const ObSQLSessionInfo &session,
                                        const share::schema::ObTableSchema &table_schema,
                                        const share::schema::ObColumnSchemaV2 &gen_col_schema,
                                        ObRawExpr *&expr,
                                        const ObLocalSessionVar *local_vars = NULL,
                                        int64_t local_var_id = OB_INVALID_INDEX_INT64);
  static int build_rls_predicate_expr(const common::ObString &expr_str,
                                      ObRawExprFactory &expr_factory,
                                      const ObSQLSessionInfo &session_info,
                                      common::ObIArray<ObQualifiedName> &columns,
                                      ObRawExpr *&expr);
  static int build_raw_expr(ObRawExprFactory &expr_factory,
                            const ObSQLSessionInfo &session_info,
                            const ParseNode &node,
                            ObRawExpr *&expr,
                            common::ObIArray<ObQualifiedName> &columns,
                            common::ObIArray<ObVarInfo> &sys_vars,
                            common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                            common::ObIArray<ObWinFunRawExpr*> &win_exprs,
                            common::ObIArray<ObSubQueryInfo> &sub_query_info,
                            common::ObIArray<ObUDFInfo> &udf_info,
                            common::ObIArray<ObOpRawExpr*> &op_exprs,
                            bool is_prepare_protocol = false);
  static int build_raw_expr(ObRawExprFactory &expr_factory,
                            const ObSQLSessionInfo &session_info,
                            ObSchemaChecker *schema_checker,
                            pl::ObPLBlockNS *ns,
                            ObStmtScope current_scope,
                            ObStmt *stmt,
                            const ParamStore *param_list,
                            ExternalParams *external_param_info,
                            const ParseNode &node,
                            ObRawExpr *&expr,
                            common::ObIArray<ObQualifiedName> &columns,
                            common::ObIArray<ObVarInfo> &sys_vars,
                            common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                            common::ObIArray<ObWinFunRawExpr*> &win_exprs,
                            common::ObIArray<ObSubQueryInfo> &sub_query_info,
                            common::ObIArray<ObUDFInfo> &udf_info,
                            common::ObIArray<ObOpRawExpr*> &op_exprs,
                            bool is_prepare_protocol/*= false*/,
                            TgTimingEvent tg_timing_event = TgTimingEvent::TG_TIMING_EVENT_INVALID,
                            bool use_def_collation = false,
                            ObCollationType def_collation = CS_TYPE_INVALID);
  static bool is_same_raw_expr(const ObRawExpr *src, const ObRawExpr *dst);
  /// replace all `from' to `to' in the raw_expr
  static int replace_all_ref_column(ObRawExpr *&raw_expr, const common::ObIArray<ObRawExpr *> &exprs, int64_t& offset);
  // if %expr_factory is not NULL, will deep copy %to expr. default behavior is shallow copy
  // if except_exprs is not NULL, will skip the expr in except_exprs
  static int replace_ref_column(ObRawExpr *&raw_expr,
                                ObRawExpr *from,
                                ObRawExpr *to,
                                const ObIArray<ObRawExpr*> *except_exprs = NULL);

  static int replace_ref_column(ObRawExpr *&raw_expr,
                                ObIArray<ObRawExpr *> &from,
                                ObIArray<ObRawExpr *> &to,
                                const ObIArray<ObRawExpr*> *except_exprs = NULL);
  static int replace_level_column(ObRawExpr *&raw_expr, ObRawExpr *to, bool &replaced);
  static int replace_ref_column(common::ObIArray<ObRawExpr *> &exprs,
                                ObRawExpr *from,
                                ObRawExpr *to,
                                const ObIArray<ObRawExpr*> *except_exprs = NULL);
  static int replace_ref_column(common::ObIArray<ObRawExpr *> &exprs,
                                ObIArray<ObRawExpr *> &from,
                                ObIArray<ObRawExpr *> &to,
                                const ObIArray<ObRawExpr*> *except_exprs = NULL);
  static int contain_virtual_generated_column(ObRawExpr *&expr,
                                  bool &is_contain_vir_gen_column);
  static int extract_virtual_generated_column_parents(
  ObRawExpr *&par_expr, ObRawExpr *&child_expr, ObIArray<ObRawExpr*> &vir_gen_par_exprs);

  static bool is_all_column_exprs(const common::ObIArray<ObRawExpr*> &exprs);
  static int extract_set_op_exprs(const ObRawExpr *raw_expr,
                                  common::ObIArray<ObRawExpr*> &set_op_exprs);
  static int extract_var_assign_exprs(const ObRawExpr *raw_expr,
                                      common::ObIArray<ObRawExpr*> &assign_exprs);
  static int extract_set_op_exprs(const ObIArray<ObRawExpr*> &exprs,
                                  common::ObIArray<ObRawExpr*> &set_op_exprs);
  /// extract column exprs from the raw expr
  static int extract_column_exprs(const ObRawExpr *raw_expr,
                                  common::ObIArray<ObRawExpr*> &column_exprs,
                                  bool need_pseudo_column = false);
  static int extract_column_exprs(const common::ObIArray<ObRawExpr*> &exprs,
                                  common::ObIArray<ObRawExpr *> &column_exprs,
                                  bool need_pseudo_column = false);
  static int extract_column_exprs(const ObRawExpr *raw_expr,
                                  int64_t table_id,
                                  common::ObIArray<ObRawExpr*> &column_exprs);
  static int extract_column_exprs(const common::ObIArray<ObRawExpr*> &exprs,
                                  int64_t table_id,
                                  common::ObIArray<ObRawExpr *> &column_exprs);
  static int extract_column_exprs(const common::ObIArray<ObRawExpr*> &exprs,
                                  const common::ObIArray<int64_t> &table_ids,
                                  common::ObIArray<ObRawExpr *> &column_exprs);
  // no need to add cast.
  static int extract_column_exprs(const ObRawExpr *expr,
                                  ObIArray<const ObRawExpr*> &column_exprs);
  static int extract_column_exprs(ObRawExpr* expr,
                                  ObRelIds &rel_ids,
                                  ObIArray<ObRawExpr*> &column_exprs);
  static int extract_column_exprs(ObIArray<ObRawExpr*> &exprs,
                                  ObRelIds &rel_ids,
                                  ObIArray<ObRawExpr*> &column_exprs);
  static int extract_contain_exprs(ObRawExpr *raw_expr,
                                   const common::ObIArray<ObRawExpr*> &src_exprs,
                                   common::ObIArray<ObRawExpr *> &contain_exprs);
  static int extract_invalid_sequence_expr(ObRawExpr *raw_expr, ObRawExpr *&sequence_expr);
  static int mark_column_explicited_reference(ObRawExpr &expr);
  static int extract_column_ids(const ObIArray<ObRawExpr*> &exprs, common::ObIArray<uint64_t> &column_ids);
  static int extract_column_ids(const ObRawExpr *raw_expr, common::ObIArray<uint64_t> &column_ids);
  static int extract_table_ids(const ObRawExpr *raw_expr, common::ObIArray<uint64_t> &table_ids);
  static int extract_table_ids_from_exprs(const common::ObIArray<ObRawExpr *> &exprs,
                                          common::ObIArray<uint64_t> &table_ids);
  static int extract_param_idxs(const ObRawExpr *expr, common::ObIArray<int64_t> &param_idxs);
  static int extract_col_aggr_exprs(ObIArray<ObRawExpr*> &exprs,
                                    ObIArray<ObRawExpr*> &column_or_aggr_exprs);
  static int extract_col_aggr_exprs(ObRawExpr* expr,
                                    ObIArray<ObRawExpr*> &column_or_aggr_exprs);
  static int extract_col_aggr_winfunc_exprs(ObIArray<ObRawExpr*> &exprs,
                                            ObIArray<ObRawExpr*> &column_aggr_winfunc_exprs);
  static int extract_col_aggr_winfunc_exprs(ObRawExpr* expr,
                                            ObIArray<ObRawExpr*> &column_aggr_winfunc_exprs);

  static int extract_metadata_filename_expr(ObRawExpr *expr, ObRawExpr *&file_name_expr);
  static int find_alias_expr(ObRawExpr *expr, ObAliasRefRawExpr *&alias_expr);
  static int find_flag(const ObRawExpr *expr, ObExprInfoFlag flag, bool &is_found);
  static int find_flag_rec(const ObRawExpr *expr, ObExprInfoFlag flag, bool &is_found);

  // try add cast expr above %expr , set %dst_expr to &expr if no cast added.
  static int try_add_cast_expr_above(ObRawExprFactory *expr_factory,
                                     const ObSQLSessionInfo *session,
                                     ObRawExpr &src_expr,
                                     const ObExprResType &dst_type,
                                     ObRawExpr *&new_expr);
  static int try_add_cast_expr_above(ObRawExprFactory *expr_factory,
                                     const ObSQLSessionInfo *session,
                                     ObRawExpr &expr,
                                     const ObExprResType &dst_type,
                                     const ObCastMode &cm,
                                     ObRawExpr *&new_expr,
                                     const ObLocalSessionVar *local_vars = NULL,
                                     int64_t local_var_id = OB_INVALID_INDEX_INT64);

  static int implict_cast_pl_udt_to_sql_udt(ObRawExprFactory *expr_factory,
                                            const ObSQLSessionInfo *session,
                                            ObRawExpr* &real_ref_expr);

  static int implict_cast_sql_udt_to_pl_udt(ObRawExprFactory *expr_factory,
                                            const ObSQLSessionInfo *session,
                                            ObRawExpr* &real_ref_expr);
  // new engine: may create more cast exprs to handle non-system-collation string.
  //             e.g.: utf16->number: utf16->utf8->number (two cast expr)
  //                   utf8_bin->number: utf8->number (just one cat expr)
  //             if %use_def_cm is not true, %cm will be set as cast mode for cast exprs.
  //             NOTE: IS_INNER_ADDED_EXPR flag will be set.
  static int create_cast_expr(ObRawExprFactory &expr_factory,
                              ObRawExpr *src_expr,
                              const ObExprResType &dst_type,
                              ObSysFunRawExpr *&func_expr,
                              const ObSQLSessionInfo *session_info,
                              bool use_def_cm = true,
                              ObCastMode cm = CM_NONE,
                              const ObLocalSessionVar *local_vars = NULL,
                              int64_t local_var_id = OB_INVALID_INDEX_INT64);
  static void need_extra_cast(const ObExprResType &src_type,
                              const ObExprResType &dst_type,
                              bool &need_extra_cast_for_src_type,
                              bool &need_extra_cast_for_dst_type);
  static int setup_extra_cast_utf8_type(const ObExprResType &type,
                                        ObExprResType &utf8_type);

  static int erase_inner_added_exprs(ObRawExpr *src_expr, ObRawExpr *&out_expr);
  static int erase_inner_cast_exprs(ObRawExpr *src_expr, ObRawExpr *&out_expr);

  // erase implicit cast which added for operand casting.
  static int erase_operand_implicit_cast(ObRawExpr *src, ObRawExpr *&out);

  static const ObRawExpr *skip_inner_added_expr(const ObRawExpr *expr);

  static ObRawExpr *skip_implicit_cast(ObRawExpr *e);

  static ObRawExpr *skip_inner_added_expr(ObRawExpr *expr);
  static const ObColumnRefRawExpr *get_column_ref_expr_recursively(const ObRawExpr *expr);
  static ObRawExpr *get_sql_udt_type_expr_recursively(ObRawExpr *expr);

  static int create_to_type_expr(ObRawExprFactory &expr_factory,
                                 ObRawExpr *src_expr,
                                 const common::ObObjType &dst_type,
                                 ObSysFunRawExpr *&to_type,
                                 ObSQLSessionInfo *session_info);
  static int create_substr_expr(ObRawExprFactory &expr_factory,
                                ObSQLSessionInfo *session_info,
                                ObRawExpr *first_expr,
                                ObRawExpr *second_expr,
                                ObRawExpr *third_expr,
                                ObSysFunRawExpr *&out_expr);
  static int create_instr_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObRawExpr *first_expr,
                                       ObRawExpr *second_expr,
                                       ObRawExpr *third_expr,
                                       ObRawExpr *fourth_expr,
                                       ObSysFunRawExpr *&out_expr);
  static int create_concat_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObRawExpr *first_expr,
                                       ObRawExpr *second_expr,
                                       ObOpRawExpr *&out_expr);
  static int create_prefix_pattern_expr(ObRawExprFactory &expr_factory,
                                        ObSQLSessionInfo *session_info,
                                        ObRawExpr *first_expr,
                                        ObRawExpr *second_expr,
                                        ObRawExpr *third_expr,
                                        ObSysFunRawExpr *&out_expr);
  static int create_type_to_str_expr(ObRawExprFactory &expr_factory,
                                     ObRawExpr *src_expr,
                                     ObSysFunRawExpr *&out_expr,
                                     ObSQLSessionInfo *session_info,
                                     bool is_type_to_str,
                                     ObObjType dst_type = ObMaxType);

  static int wrap_enum_set_for_stmt(ObRawExprFactory &expr_factory,
                                    ObSelectStmt *stmt,
                                    ObSQLSessionInfo *session_info);

  static int get_exec_param_expr(ObRawExprFactory &expr_factory,
                                 ObQueryRefRawExpr *query_ref,
                                 ObRawExpr *correlated_expr,
                                 ObRawExpr *&exec_param);

  static int get_exec_param_expr(ObRawExprFactory &expr_factory,
                                 ObIArray<ObExecParamRawExpr*> *query_ref_exec_params,
                                 ObRawExpr *correlated_expr,
                                 ObRawExpr *&exec_param);
  static int create_new_exec_param(ObQueryCtx *query_ctx,
                                   ObRawExprFactory &expr_factory,
                                   ObRawExpr *&expr,
                                   bool is_onetime = false);
  static int create_new_exec_param(ObRawExprFactory &expr_factory,
                                   ObRawExpr *ref_expr,
                                   ObExecParamRawExpr *&exec_param,
                                   bool is_onetime = false);
  static int create_param_expr(ObRawExprFactory &expr_factory, int64_t param_idx, ObRawExpr *&expr);
  static int build_trim_expr(const share::schema::ObColumnSchemaV2 *column_schema,
                             ObRawExprFactory &expr_factory,
                             const ObSQLSessionInfo *session_info,
                             ObRawExpr *&expr,
                             const ObLocalSessionVar *local_vars = NULL,
                             int64_t local_var_id = OB_INVALID_INDEX_INT64);

  static bool is_domain_expr_need_special_replace(ObRawExpr* qual_expr,
                                                  ObRawExpr *depend_expr);
  static int replace_domain_wrapper_expr(ObRawExpr *depend_expr,
                                         ObColumnRefRawExpr *col_expr,
                                         ObRawExprCopier& copier,
                                         ObRawExprFactory& factory,
                                         ObSQLSessionInfo *session_info,
                                         ObRawExpr *&qual,
                                         int64_t qual_idx,
                                         ObRawExpr *&new_qual);
  static int replace_json_wrapper_expr_if_need(ObRawExpr* qual,
                                         int64_t qual_idx,
                                         ObRawExpr *depend_expr,
                                         ObRawExprFactory &expr_factory,
                                         ObSQLSessionInfo *session_info,
                                         bool& is_done_replace);

  static int replace_qual_param_if_need(ObRawExpr* qual, int64_t qual_idx, ObColumnRefRawExpr *col_expr);

  static bool need_column_conv(const ColumnItem &column, ObRawExpr &expr);
  static int build_pad_expr(ObRawExprFactory &expr_factory,
                            bool is_char,
                            const share::schema::ObColumnSchemaV2 *column_schema,
                            ObRawExpr *&expr,
                            const sql::ObSQLSessionInfo *session_info,
                            const ObLocalSessionVar *local_vars = NULL,
                            int64_t local_var_id = OB_INVALID_INDEX_INT64);
  static bool need_column_conv(const ObExprResType &expected_type, const ObRawExpr &expr);
  static bool check_exprs_type_collation_accuracy_equal(const ObRawExpr *expr1, const ObRawExpr *expr2);
  // 此方法请谨慎使用,会丢失enum类型的 enum_set_values
  static int build_column_conv_expr(ObRawExprFactory &expr_factory,
                                    const share::schema::ObColumnSchemaV2 *column_schema,
                                    ObRawExpr *&expr,
                                    const sql::ObSQLSessionInfo *session_info,
                                    const ObLocalSessionVar *local_vars = NULL);
  static int build_column_conv_expr(ObRawExprFactory &expr_factory,
                                    common::ObIAllocator &allocator,
                                    const ObColumnRefRawExpr &col_expr,
                                    ObRawExpr *&expr,
                                    const ObSQLSessionInfo *session_info,
                                    bool is_generated_column = false,
                                    const ObLocalSessionVar *local_vars = NULL,
                                    int64_t local_var_id = OB_INVALID_INDEX_INT64);
  static int build_column_conv_expr(const ObSQLSessionInfo *session_info,
                                    ObRawExprFactory &expr_factory,
                                    const common::ObObjType &type,
                                    const common::ObCollationType &collation,
                                    const int64_t &accuracy,
                                    const bool &is_nullable,
                                    const common::ObString *column_conv_info,
                                    const common::ObIArray<common::ObString> *type_infos,
                                    ObRawExpr *&expr,
                                    bool is_in_pl = false,
                                    bool is_generated_column = false,
                                    const ObLocalSessionVar *local_vars = NULL,
                                    int64_t local_var_id = OB_INVALID_INDEX_INT64);
  static int build_var_int_expr(ObRawExprFactory &expr_factory,
                                ObConstRawExpr *&expr);
  static int build_default_expr(ObRawExprFactory &expr_factory,
                                const common::ObObjType &type,
                                const common::ObCollationType &collation,
                                const int64_t &accuracy,
                                ObRawExpr *&expr);
  static int build_const_int_expr(ObRawExprFactory &expr_factory,
                                  common::ObObjType type,
                                  int64_t int_value,
                                  ObConstRawExpr *&expr);
  static int build_const_uint_expr(ObRawExprFactory &expr_factory,
                                   common::ObObjType type,
                                   uint64_t uint_value,
                                   ObConstRawExpr *&expr);
  static int build_const_float_expr(ObRawExprFactory &expr_factory,
                                    common::ObObjType type,
                                    float value,
                                    ObConstRawExpr *&expr);
  static int build_const_double_expr(ObRawExprFactory &expr_factory,
                                    common::ObObjType type,
                                    double value,
                                    ObConstRawExpr *&expr);
  static int build_const_datetime_expr(ObRawExprFactory &expr_factory,
                                   int64_t int_value,
                                   ObConstRawExpr *&expr);
  static int build_const_date_expr(ObRawExprFactory &expr_factory,
                                   int64_t int_value,
                                   ObConstRawExpr *&expr);
  static int build_const_ym_expr(ObRawExprFactory &expr_factory,
                                 common::ObObjType type,
                                 const ObObj &obj,
                                 ObConstRawExpr *&expr);
  static int build_const_ds_expr(ObRawExprFactory &expr_factory,
                                 common::ObObjType type,
                                 ObIntervalDSValue &ds_value,
                                 ObConstRawExpr *&expr);
  static int build_const_timestampnano_expr(ObRawExprFactory &expr_factory,
                                            ObObjType type,
                                            ObOTimestampData &value,
                                            ObConstRawExpr *&expr);
  static int build_const_number_expr(ObRawExprFactory &expr_factory,
                                     ObObjType type,
                                     number::ObNumber value,
                                     ObConstRawExpr *&expr);
  static int build_const_obj_expr(ObRawExprFactory &expr_factory,
                                  const ObObj &obj,
                                  ObConstRawExpr *&expr);
  static int build_mul_expr(ObRawExprFactory &raw_expr_factory,
                            ObRawExpr *expr1,
                            ObRawExpr *expr2,
                            ObOpRawExpr *&expr_out);
  static int build_div_expr(ObRawExprFactory &raw_expr_factory,
                            ObRawExpr *expr1,
                            ObRawExpr *expr2,
                            ObOpRawExpr *&expr_out);
  static int build_add_all_expr(ObRawExprFactory &raw_expr_factory,
                                ObRawExpr *expr1,
                                ObRawExpr *expr2,
                                ObRawExpr *expr3,
                                ObRawExpr *expr4,
                                ObOpRawExpr *&sum_expr);
  static int build_second_expr_from_interval_ds(ObRawExprFactory &raw_expr_factory,
                                                ObRawExpr *interval_ds_expr,
                                                ObOpRawExpr *&second_expr);
  static int build_month_expr_from_interval_ym(ObRawExprFactory &raw_expr_factory,
                                               ObRawExpr *interval_ym_expr,
                                               ObOpRawExpr *&month_expr);
  static int build_datepart_to_second_expr(ObRawExprFactory &raw_expr_factory,
                                           ObRawExpr *interval_ds_expr,
                                           int datapart,
                                           int n,
                                           ObRawExpr *&expr_out);
  static int build_interval_ym_diff_exprs(ObRawExprFactory &raw_expr_factory,
                                          ObObj &const_val,
                                          const ObObj &transition_val,
                                          const ObObj &interval_val,
                                          ObRawExpr *&diff_1_out,
                                          ObRawExpr *&diff_2_out,
                                          ObConstRawExpr *&transition_expr,
                                          ObConstRawExpr *&interval_expr);
  static int build_interval_ds_diff_exprs(ObRawExprFactory &raw_expr_factory,
                                          ObObj &const_val,
                                          const ObObj &transition_val,
                                          const ObObj &interval_val,
                                          ObRawExpr *&diff_1_out,
                                          ObRawExpr *&diff_2_out,
                                          ObConstRawExpr *&transition_expr,
                                          ObConstRawExpr *&interval_expr);
  static int build_high_bound_raw_expr(ObRawExprFactory &raw_expr_factory,
                                       ObSQLSessionInfo* session,
                                       ObObj &const_val,
                                       const ObObj &transition_val,
                                       const ObObj &interval_val,
                                       ObRawExpr *&result_expr_out,
                                       ObRawExpr *&n_part_expr);
  static int build_common_diff_exprs(ObRawExprFactory &raw_expr_factory,
                                     ObObj &const_val,
                                     const ObObj &transition_val,
                                     const ObObj &interval_val,
                                     ObRawExpr *&diff_1_out,
                                     ObRawExpr *&diff_2_out,
                                     ObConstRawExpr *&transition_expr,
                                     ObConstRawExpr *&interval_expr);
  static int build_sign_expr(ObRawExprFactory &expr_factory,
                             ObRawExpr *param,
                             ObRawExpr *&sign_expr);
  static int build_less_than_expr(ObRawExprFactory &expr_factory,
                                  ObRawExpr *left,
                                  ObRawExpr *right,
                                  ObOpRawExpr *&less_than_expr);
  static int build_variable_expr(ObRawExprFactory &expr_factory,
                                 const ObExprResType &result_type,
                                 ObVarRawExpr *&expr);
  static int build_op_pseudo_column_expr(ObRawExprFactory &expr_factory,
                                         const ObItemType expr_type,
                                         const char *expr_name,
                                         const ObExprResType &res_type,
                                         ObOpPseudoColumnRawExpr *&expr);
  static int build_const_bool_expr(ObRawExprFactory *expr_factory,
                                   ObRawExpr *&expr,
                                   bool b_value);
  static int build_const_string_expr(ObRawExprFactory &expr_factory,
                                     common::ObObjType type,
                                     const common::ObString &string_value,
                                     common::ObCollationType cs_type,
                                     ObConstRawExpr *&expr);
  static int build_null_expr(ObRawExprFactory &expr_factory, ObRawExpr *&expr);
  static int build_nvl_expr(ObRawExprFactory &expr_factory, const ColumnItem *column_item, ObRawExpr *&expr);
  static int build_nvl_expr(ObRawExprFactory &expr_factory, const ColumnItem *column_item, ObRawExpr *&expr1, ObRawExpr *&expr2);
  static int build_nvl_expr(ObRawExprFactory &expr_factory, ObRawExpr *param_expr1, ObRawExpr *param_expr2, ObRawExpr *&expr);
  static int build_lnnvl_expr(ObRawExprFactory &expr_factory,
                              ObRawExpr *param_expr,
                              ObRawExpr *&lnnvl_expr);
  static int build_equal_last_insert_id_expr(ObRawExprFactory &expr_factory,
                                             ObRawExpr *&expr,
                                             ObSQLSessionInfo *session);
  static int build_get_user_var(ObRawExprFactory &expr_factory,
                                const common::ObString &var_name,
                                ObRawExpr *&expr,
                                const ObSQLSessionInfo *session_info = NULL,
                                ObQueryCtx *query_ctx = NULL,
                                common::ObIArray<ObUserVarIdentRawExpr*> *user_var_exprs = NULL);
  static int build_get_sys_var(ObRawExprFactory &expr_factory,
                               const common::ObString &var_name,
                               share::ObSetVar::SetScopeType var_scope,
                               ObRawExpr *&expr,
                               const ObSQLSessionInfo *session_info = NULL);
  static int get_package_var_ids(ObRawExpr *expr, uint64_t &package_id, int64_t &var_idx);
  static int set_package_var_ids(ObRawExpr *expr, uint64_t package_id, int64_t var_idx);
  static int build_get_package_var(ObRawExprFactory &expr_factory,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   uint64_t package_id,
                                   int64_t var_idx,
                                   ObExprResType *result_type,
                                   ObRawExpr *&expr,
                                   const ObSQLSessionInfo *session_info);
  static int build_calc_part_id_expr(ObRawExprFactory &expr_factory,
                                     const ObSQLSessionInfo &session,
                                     uint64_t ref_table_id,
                                     share::schema::ObPartitionLevel part_level,
                                     ObRawExpr *part_expr,
                                     ObRawExpr *subpart_expr,
                                     ObRawExpr *&expr);
  static int build_calc_tablet_id_expr(ObRawExprFactory &expr_factory,
                                       const ObSQLSessionInfo &session,
                                       uint64_t ref_table_id,
                                       ObPartitionLevel part_level,
                                       ObRawExpr *part_expr,
                                       ObRawExpr *subpart_expr,
                                       ObRawExpr *&expr);
  static int build_calc_partition_tablet_id_expr(ObRawExprFactory &expr_factory,
                                                 const ObSQLSessionInfo &session,
                                                 uint64_t ref_table_id,
                                                 ObPartitionLevel part_level,
                                                 ObRawExpr *part_expr,
                                                 ObRawExpr *subpart_expr,
                                                 ObRawExpr *&expr);
  static int build_get_subprogram_var(ObRawExprFactory &expr_factory,
                                   uint64_t package_id,
                                   uint64_t routine_id,
                                   int64_t var_idx,
                                   const ObExprResType *result_type,
                                   ObRawExpr *&expr,
                                   const ObSQLSessionInfo *session_info);
  static int build_exists_expr(ObRawExprFactory &expr_factory,
                               const ObSQLSessionInfo *session_info,
                               ObItemType type,
                               ObRawExpr *param_expr,
                               ObRawExpr *&exists_expr);
  static int build_ora_decode_expr(ObRawExprFactory *expr_factory,
                                   const ObSQLSessionInfo &session_info,
                                   ObRawExpr *&expr,
                                   ObIArray<ObRawExpr *> &param_exprs);
  template <typename T>
  static bool find_expr(const common::ObIArray<T> &exprs, const ObRawExpr* expr);
  template <typename T>
  static int64_t get_expr_idx(const common::ObIArray<T> &exprs, const ObRawExpr* expr);
  static int create_equal_expr(ObRawExprFactory &expr_factory,
                               const ObSQLSessionInfo *session_info,
                               const ObRawExpr *val_ref,
                               const ObRawExpr *col_ref,
                               ObRawExpr *&expr);
  static int create_double_op_expr(ObRawExprFactory &expr_factory,
                                   const ObSQLSessionInfo *session_info,
                                   ObItemType expr_type,
                                   ObRawExpr *&add_expr,
                                   const ObRawExpr *left_expr,
                                   const ObRawExpr *right_expr);
  static int make_set_op_expr(ObRawExprFactory &expr_factory,
                              int64_t idx,
                              ObItemType set_op_type,
                              const ObExprResType &res_type,
                              ObSQLSessionInfo *session_info,
                              ObRawExpr *&out_expr);
  static int merge_variables(const common::ObIArray<ObVarInfo> &src_vars, common::ObIArray<ObVarInfo> &dst_vars);
  static int get_item_count(const ObRawExpr *expr, int64_t &count);
  static int get_array_param_index(const ObRawExpr *expr, int64_t &param_index);
  static int build_column_expr(ObRawExprFactory &expr_factory,
                               const share::schema::ObColumnSchemaV2 &column_schema,
                               ObColumnRefRawExpr *&column_expr);
  static bool is_same_column_ref(const ObRawExpr *column_ref1, const ObRawExpr *column_ref2);
  static int build_alias_column_expr(ObRawExprFactory &expr_factory, ObRawExpr *ref_expr, int32_t alias_level, ObAliasRefRawExpr *&alias_expr);
  static int build_query_output_ref(ObRawExprFactory &expr_factory,
                                    ObQueryRefRawExpr *query_ref,
                                    int64_t project_index,
                                    ObAliasRefRawExpr *&alias_expr);
  static int init_column_expr(const share::schema::ObColumnSchemaV2 &column_schema, ObColumnRefRawExpr &column_expr);
  static ObCollationLevel get_column_collation_level(const common::ObObjType &type);
  /* 计算基本列的flag */
  static uint32_t calc_column_result_flag(const share::schema::ObColumnSchemaV2 &column_schema);
  static int expr_is_order_consistent(const ObRawExpr *from, const ObRawExpr *to, bool &is_consistent);
  static int is_expr_comparable(const ObRawExpr *expr, bool &can_be);
  static int exprs_contain_subquery(const common::ObIArray<ObRawExpr*> &exprs, bool &cnt_subquery);
  static int function_alias(ObRawExprFactory &expr_factory, ObSysFunRawExpr *&expr);
  //extract from const value
  static int extract_int_value(const ObRawExpr *expr, int64_t &val);
  //used for enum set type
  static int need_wrap_to_string(common::ObObjType param_type, common::ObObjType calc_type,
                                 const bool is_same_type_need, bool &need_wrap);
  static bool contain_id(const common::ObIArray<uint64_t> &ids, const uint64_t target);
  static int clear_exprs_flag(const common::ObIArray<ObRawExpr*> &exprs, ObExprInfoFlag flag);

  static int resolve_udf_common_info(const ObString &db_name,
                                     const ObString &package_name,
                                     int64_t udf_id,
                                     int64_t package_id,
                                     const ObIArray<int64_t> &subprogram_path,
                                     int64_t udf_schema_version,
                                     int64_t pkg_schema_version,
                                     bool is_deterministic,
                                     bool is_parallel_enable,
                                     bool is_pkg_body_udf,
                                     bool is_pl_agg,
                                     int64_t type_id,
                                     ObUDFInfo &udf_info,
                                     uint64_t dblink_id,
                                     const ObString &dblink_name);
  static int resolve_udf_param_types(const share::schema::ObIRoutineInfo* func_info,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     sql::ObSQLSessionInfo &session_info,
                                     common::ObIAllocator &allocator,
                                     common::ObMySQLProxy &sql_proxy,
                                     ObUDFInfo &udf_info,
                                     pl::ObPLDbLinkGuard &dblink_guard);
  static int resolve_udf_param_exprs(ObResolverParams &params,
                                     const share::schema::ObIRoutineInfo *func_info,
                                     ObUDFInfo &udf_info);
  static int resolve_udf_param_exprs(const share::schema::ObIRoutineInfo* func_info,
                                     pl::ObPLBlockNS &secondary_namespace_,
                                     ObSchemaChecker &schema_checker,
                                     sql::ObSQLSessionInfo &session_info,
                                     ObIAllocator &allocator,
                                     bool is_prepare_protocol,
                                     sql::ObRawExprFactory &expr_factory,
                                     common::ObMySQLProxy &sql_proxy,
                                     ExternalParams *extern_param_info,
                                     ObUDFInfo &udf_info);

  static int rebuild_expr_params(ObUDFInfo &udf_info,
                                 sql::ObRawExprFactory *expr_factory,
                                 common::ObIArray<sql::ObRawExpr*> &expr_params);
  static int resolve_udf_info(common::ObIAllocator &allocator,
                              sql::ObRawExprFactory &expr_factory,
                              sql::ObSQLSessionInfo &session_info,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              ObUDFInfo &udf_info);
  //判断expr里是否包含字符串前缀的表达式
  static bool has_prefix_str_expr(const ObRawExpr &expr,
                                  const ObColumnRefRawExpr &orig_column_expr,
                                  const ObRawExpr *&substr_expr);
  static int build_like_expr(ObRawExprFactory &expr_factory,
                             ObSQLSessionInfo *session_info,
                             ObRawExpr *text_expr,
                             ObRawExpr *pattern_expr,
                             ObRawExpr *escape_expr,
                             ObOpRawExpr *&like_expr);
  static int resolve_op_expr_for_oracle_implicit_cast(ObRawExprFactory &expr_factory,
                                                      const ObSQLSessionInfo *session_info,
                                                      ObOpRawExpr* &b_expr);
  static int resolve_op_expr_implicit_cast(ObRawExprFactory &expr_factory,
                                          const ObSQLSessionInfo *session_info,
                                          ObItemType op_type,
                                          ObRawExpr* &sub_expr1,
                                          ObRawExpr* &sub_expr2);
  static inline int resolve_op_expr_add_implicit_cast(ObRawExprFactory &expr_factory,
                                                      const ObSQLSessionInfo *session_info,
                                                      ObRawExpr *src_expr,
                                                      const ObExprResType &dst_type,
                                                      ObSysFunRawExpr *&func_expr);
  static int resolve_op_exprs_for_oracle_implicit_cast(ObRawExprFactory &expr_factory,
                                                       const ObSQLSessionInfo *session_info,
                                                       common::ObIArray<ObOpRawExpr*> &op_exprs);
  static int check_composite_cast(ObRawExpr *&expr, ObSchemaChecker &schema_checker);
  static int add_cast_to_multiset(ObRawExpr *&expr);


  // 对parent的所有子节点一次调用try_create_bool_expr，给每个前面子节点按需增加bool expr
  static int try_add_bool_expr(ObOpRawExpr *parent, ObRawExprFactory &expr_factory);

  // 针对case表达式的overload方法. 会给case expr的所有when expr前面按需增加bool expr
  static int try_add_bool_expr(ObCaseOpRawExpr *parent, ObRawExprFactory &expr_factory);

  // 判断src_epxr前面是否需要bool expr，如果需要则创建bool expr并加到src_expr前面
  // 如果不需要则out_expr被赋值为原本的src_expr
  static int try_create_bool_expr(ObRawExpr *src_expr, ObRawExpr *&out_expr,
                              ObRawExprFactory &expr_factory);
  // 根据表达式类型判断是否需要增加布尔表达式
  // 因为有些表达式的返回结果就是布尔语义的，这些表达式前面就不用再增加布尔表达式
  static int check_need_bool_expr(const ObRawExpr *expr, bool &need_bool_expr);
  static int check_is_bool_expr(const ObRawExpr *expr, bool &is_bool_expr);

  // parent_raw_expr -> child_raw_expr加入隐式cast表达式后，变为:
  // parent_raw_expr -> cast_raw_expr -> child_raw_expr
  // 该函数的作用是为了获得child_raw_expr，如果有多个cast_raw_expr，会迭代循环，一直循环到
  // 第一个非隐式cast的表达式并返回
  // deduce type阶段会调用它来忽略隐式cast expr
  static int get_real_expr_without_cast(const ObRawExpr *expr,
                                        const ObRawExpr *&out_expr);

  // build remove_const for const expr to remove const attribute.
  static int build_remove_const_expr(ObRawExprFactory &factory,
                                       ObSQLSessionInfo &session_info,
                                       ObRawExpr *arg,
                                       ObRawExpr *&out);
  static int build_wrapper_inner_expr(ObRawExprFactory &factory,
                                      ObSQLSessionInfo &session_info,
                                      ObRawExpr *arg,
                                      ObRawExpr *&out);
  static int build_dup_data_expr(ObRawExprFactory &factory,
                                 ObRawExpr *param,
                                 ObRawExpr *&new_param);
  static int build_inner_aggr_code_expr(ObRawExprFactory &factory,
                                        const ObSQLSessionInfo &session_info,
                                        ObRawExpr *&out);
  static int build_inner_wf_aggr_status_expr(ObRawExprFactory &factory,
                                             const ObSQLSessionInfo &session_info,
                                             ObOpPseudoColumnRawExpr *&out);
  static int build_pseudo_rollup_id(ObRawExprFactory &factory,
                                    const ObSQLSessionInfo &session_info,
                                    ObRawExpr *&out);
  static int build_pseudo_random(ObRawExprFactory &factory,
                                 const ObSQLSessionInfo &session_info,
                                 ObRawExpr *&out);
  static bool is_pseudo_column_like_expr(const ObRawExpr &expr);
  static bool is_sharable_expr(const ObRawExpr &expr);

  static int check_need_cast_expr(const ObExprResType &src_res_type,
                                  const ObExprResType &dst_res_type,
                                  bool &need_cast,
                                  bool &ignore_dup_cast_error);
  static int build_add_expr(ObRawExprFactory &expr_factory,
                            ObRawExpr *param_expr1,
                            ObRawExpr *param_expr2,
                            ObOpRawExpr *&add_expr);
  static int build_minus_expr(ObRawExprFactory &expr_factory,
                              ObRawExpr *param_expr1,
                              ObRawExpr *param_expr2,
                              ObOpRawExpr *&add_expr);
  static int build_date_add_expr(ObRawExprFactory &expr_factory,
                                 ObRawExpr *param_expr1,
                                 ObRawExpr *param_expr2,
                                 ObRawExpr *param_expr3,
                                 ObSysFunRawExpr *&date_add_expr);
  static int build_date_sub_expr(ObRawExprFactory &expr_factory,
                                 ObRawExpr *param_expr1,
                                 ObRawExpr *param_expr2,
                                 ObRawExpr *param_expr3,
                                 ObSysFunRawExpr *&date_sub_expr);
  static int build_common_binary_op_expr(ObRawExprFactory &expr_factory,
                                         const ObItemType expect_op_type,
                                         ObRawExpr *param_expr1,
                                         ObRawExpr *param_expr2,
                                         ObRawExpr *&expr);
  static int build_case_when_expr(ObRawExprFactory &expr_factory,
                                  ObRawExpr *when_expr,
                                  ObRawExpr *then_expr,
                                  ObRawExpr *default_expr,
                                  ObRawExpr *&case_when_expr);
  static int build_is_not_expr(ObRawExprFactory &expr_factory,
                               ObRawExpr *param_expr1,
                               ObRawExpr *param_expr2,
                               ObRawExpr *&is_not_expr);

  static int extract_metadata_fileurl_expr(ObRawExpr *expr, ObRawExpr *&file_name_expr);

  static int build_is_not_null_expr(ObRawExprFactory &expr_factory,
                                    ObRawExpr *param_expr,
                                    bool is_not_null,
                                    ObRawExpr *&is_not_null_expr);

  static int process_window_complex_agg_expr(ObRawExprFactory &expr_factory,
                                             const ObItemType func_type,
                                             ObWinFunRawExpr *win_func,
                                             ObRawExpr *&agg_expr,
                                             common::ObIArray<ObWinFunRawExpr*> *win_exprs);
  static int build_common_aggr_expr(ObRawExprFactory &expr_factory,
                                    ObSQLSessionInfo *session_info,
                                    const ObItemType expect_op_type,
                                    ObRawExpr *param_expr,
                                    ObAggFunRawExpr *&aggr_expr);

  //专用于limit_expr/offset_expr中，用于构造case when limit_expr < 0 then 0 else limit_expr end
  static int build_case_when_expr_for_limit(ObRawExprFactory &expr_factory,
                                            ObRawExpr *limit_expr,
                                            ObRawExpr *&case_when_expr);

  static int build_not_expr(ObRawExprFactory &expr_factory,
                            ObRawExpr *expr,
                            ObRawExpr* &not_expr);

  static int build_or_exprs(ObRawExprFactory &expr_factory,
                            const ObIArray<ObRawExpr*> &exprs,
                            ObRawExpr* &or_expr);

  static int build_and_expr(ObRawExprFactory &expr_factory,
                            const ObIArray<ObRawExpr*> &exprs,
                            ObRawExpr * &and_expr);

  static int new_parse_node(ParseNode *& node, ObRawExprFactory &expr_factory,
                            ObItemType type, int num);

  static int new_parse_node(ParseNode *&node, ObRawExprFactory &expr_factory,
                            ObItemType type, int num, common::ObString str_val);

  static int build_rownum_expr(ObRawExprFactory &expr_factory,
                               ObRawExpr* &rownum_expr);
  static int build_rowid_expr(const ObDMLStmt *dml_stmt,
                              ObRawExprFactory &expr_factory,
                              ObIAllocator &alloc,
                              const ObSQLSessionInfo &session_info,
                              const share::schema::ObTableSchema &table_schema,
                              const uint64_t logical_table_id,
                              const ObIArray<ObRawExpr *> &rowkey_exprs,
                              ObSysFunRawExpr *&rowid_expr);
  static int build_empty_rowid_expr(ObRawExprFactory &expr_factory,
                                    uint64_t table_id,
                                    ObRawExpr *&rowid_expr);
  static int build_to_outfile_expr(ObRawExprFactory &expr_factory,
                                   const ObSQLSessionInfo *session_info,
                                   const ObSelectIntoItem *into_item,
                                   const ObIArray<ObRawExpr*> &exprs,
                                   ObRawExpr* &to_outfile_expr);
  static int build_pack_expr(ObRawExprFactory &expr_factory,
                             const bool is_ps,
                             const ObSQLSessionInfo *session_info,
                             const ObIArray<common::ObField> *field_array,
                             const ObIArray<ObRawExpr*> &input_exprs,
                             ObRawExpr *&pack_expr);
  static int build_inner_row_cmp_expr(ObRawExprFactory &expr_factory,
                                      const ObSQLSessionInfo *session_info,
                                      ObRawExpr *cast_expr,
                                      ObRawExpr *input_expr,
                                      ObRawExpr *next_expr,
                                      const uint64_t ret_code,
                                      ObSysFunRawExpr *&new_expr);

  static int set_call_in_pl(ObRawExpr *&raw_expr);


  static int transform_query_udt_column_expr(const ObSQLSessionInfo& session,
                                              ObRawExprFactory &expr_factory,
                                              ObRawExpr *hidden_blob_expr,
                                              ObRawExpr *&new_expr);
  static int try_modify_udt_col_expr_for_gen_col_recursively(const ObSQLSessionInfo &session,
                                                             const ObTableSchema &table_schema,
                                                             ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                                             ObRawExprFactory &expr_factory,
                                                             ObRawExpr *&expr);

  static int try_modify_expr_for_gen_col_recursively(const ObSQLSessionInfo &session,
                                                 const obrpc::ObCreateIndexArg *arg,
                                                 ObRawExprFactory &expr_factory,
                                                 ObRawExpr *expr,
                                                 bool &expr_changed);
  static int try_add_to_char_on_expr(const ObSQLSessionInfo &session,
                                     const obrpc::ObCreateIndexArg *arg,
                                     ObRawExprFactory &expr_factory,
                                     ObRawExpr *expr,
                                     bool &expr_changed);
  static int actual_add_to_char_on_expr(const ObSQLSessionInfo& session,
                                        const obrpc::ObCreateIndexArg *arg,
                                        ObRawExprFactory &expr_factory,
                                        ObRawExpr &src_expr,
                                        const common::ObObjType &data_type,
                                        ObSysFunRawExpr *&to_char_expr);
  static int try_add_nls_fmt_in_to_char_expr(const ObSQLSessionInfo &session,
                                             const obrpc::ObCreateIndexArg *arg,
                                             ObRawExprFactory &expr_factory,
                                             ObRawExpr *expr,
                                             bool &expr_changed);
  static int actual_add_nls_fmt_in_to_char_expr(const ObSQLSessionInfo& session,
                                                const obrpc::ObCreateIndexArg *arg,
                                                ObRawExprFactory &expr_factory,
                                                const ObObjType &data_type,
                                                ObSysFunRawExpr *to_char_expr);
  static int get_real_expr_without_generated_column(ObRawExpr *expr, ObRawExpr *&real_expr);
  static bool is_new_old_column_ref(const ParseNode *node);
  static int mock_obj_access_ref_node(common::ObIAllocator &allocator,
                                      ParseNode *&obj_access_node,
                                      const ParseNode *col_ref_node,
                                      const TgTimingEvent tg_timing_event);
  static int build_returning_lob_expr(ObRawExprFactory &factory,
                                        ObSQLSessionInfo &session_info,
                                        ObColumnRefRawExpr *ref_expr,
                                        ObSysFunRawExpr *&out);

  static const ObRawExpr *skip_implicit_cast(const ObRawExpr *e);
  /**
   * extract all params from the input expr
   * @param expr
   * @param params
   * @return
   */
  static int extract_params(ObRawExpr* expr, common::ObIArray<ObRawExpr*> &params);

  static int resolve_gen_column_udf_expr(ObRawExpr *&udf_expr,
                                        ObQualifiedName &q_name, // udf q_name
                                        ObRawExprFactory &expr_factory,
                                        const ObSQLSessionInfo &session_info,
                                        ObSchemaChecker *schema_checker,
                                        ObIArray<ObQualifiedName> &columns, // all columns after raw expr resolver
                                        ObIArray<ObRawExpr*> &real_exprs, // all resolved exprs before this one
                                        ObDMLStmt *stmt
                                        );

  static int extract_params(common::ObIArray<ObRawExpr*> &exprs,
                            common::ObIArray<ObRawExpr*> &params);
  static int is_contain_params(const common::ObIArray<ObRawExpr*> &exprs, bool &is_contain);
  static int is_contain_params(const ObRawExpr *expr, bool &is_contain);

  static int add_calc_tablet_id_on_calc_rowid_expr(const ObDMLStmt *dml_stmt,
                                                   ObRawExprFactory &expr_factory,
                                                   const ObSQLSessionInfo &session_info,
                                                   const share::schema::ObTableSchema &table_schema,
                                                   const uint64_t logical_table_id,
                                                   ObSysFunRawExpr *&calc_rowid_expr);
  static int add_calc_partition_id_on_calc_rowid_expr(const ObDMLStmt *dml_stmt,
                                                  ObRawExprFactory &expr_factory,
                                                  const ObSQLSessionInfo &session_info,
                                                  const share::schema::ObTableSchema &table_schema,
                                                  const uint64_t logical_table_id,
                                                  ObSysFunRawExpr *&calc_rowid_expr);
  static int get_col_ref_expr_recursively(ObRawExpr *expr,
                                          ObColumnRefRawExpr *&column_expr);

  static int build_shadow_pk_expr(uint64_t table_id,
                                  uint64_t column_id,
                                  const ObDMLStmt &dml_stmt,
                                  ObRawExprFactory &expr_factory,
                                  const ObSQLSessionInfo &session_info,
                                  const share::schema::ObTableSchema &index_schema,
                                  ObColumnRefRawExpr *&spk_expr);
  static bool decimal_int_need_cast(const common::ObAccuracy &src_acc,
                                    const common::ObAccuracy &dst_acc);
  static int check_contain_case_when_exprs(const ObRawExpr *raw_expr, bool &contain);
  static int check_contain_lock_exprs(const ObRawExpr *raw_expr, bool &contain);
  static bool decimal_int_need_cast(const common::ObPrecision src_p, const common::ObScale src_s,
                                    const common::ObPrecision dst_p, const common::ObScale dst_s);
  static int transform_udt_column_value_expr(ObRawExprFactory &expr_factory, ObRawExpr *old_expr, ObRawExpr *&new_expr);

  static int create_type_expr(ObRawExprFactory &expr_factory,
                              ObConstRawExpr *&type_expr,
                              const ObExprResType &dst_type,
                              bool avoid_zero_len = false);

  static int check_is_valid_generated_col(ObRawExpr *expr, ObIAllocator &allocator);

  static bool is_column_ref_skip_implicit_cast(const ObRawExpr *expr);
  static int build_default_match_filter(ObRawExprFactory &expr_factory,
                                ObRawExpr *relevance_expr,
                                ObRawExpr *threshold,
                                ObOpRawExpr *&match_filter,
                                const ObSQLSessionInfo *session);
  static int build_bm25_expr(ObRawExprFactory &expr_factory,
                             ObRawExpr *related_doc_cnt,
                             ObRawExpr *related_token_cnt,
                             ObRawExpr *total_doc_cnt,
                             ObRawExpr *doc_token_cnt,
                             ObOpRawExpr *&bm25,
                             const ObSQLSessionInfo *session);
  static int extract_match_against_filters(const ObIArray<ObRawExpr *> &filters,
                                           ObIArray<ObRawExpr *> &other_filters,
                                           ObIArray<ObRawExpr *> &match_filters);
  static int build_dummy_count_expr(ObRawExprFactory &expr_factory,
                                    const ObSQLSessionInfo *session_info,
                                    ObAggFunRawExpr *&expr);

  static int extract_local_vars_for_gencol(ObRawExpr *expr,
                                           const ObSQLMode sql_mode,
                                           ObColumnSchemaV2 &gen_col);
  static int check_contain_op_row_expr(const ObRawExpr *raw_expr, bool &contain);

  static int copy_and_formalize(ObRawExpr *&expr,
                                ObRawExprCopier *copier,
                                ObSQLSessionInfo *session_info);
  static int copy_and_formalize(const ObIArray<ObRawExpr *> &exprs,
                                ObIArray<ObRawExpr *> &new_exprs,
                                ObRawExprCopier *copier,
                                ObSQLSessionInfo *session_info);

private :
  static int create_real_cast_expr(ObRawExprFactory &expr_factory,
                              ObRawExpr *src_expr,
                              const ObExprResType &dst_type,
                              ObSysFunRawExpr *&func_expr,
                              const ObSQLSessionInfo *session_info);
  static int ora_cmp_integer(const ObConstRawExpr &const_expr, const int64_t v, int &cmp_ret);
  ObRawExprUtils();
  virtual ~ObRawExprUtils();
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprUtils);
};

template <typename T>
bool ObRawExprUtils::find_expr(const common::ObIArray<T> &exprs, const ObRawExpr *expr)
{
  return get_expr_idx(exprs, expr) != common::OB_INVALID_INDEX;
}

template <typename T>
int64_t ObRawExprUtils::get_expr_idx(const common::ObIArray<T> &exprs, const ObRawExpr* expr)
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

}
}

#endif /* _OB_RAW_EXPR_UTIL_H */
