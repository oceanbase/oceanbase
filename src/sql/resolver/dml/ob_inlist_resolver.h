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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_INLIST_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_INLIST_RESOLVER_H_
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
class ObValuesTableDef;
class ObDMLResolver;
class ObQueryRefRawExpr;
struct DistinctObjMeta;
struct InListRewriteInfo;

struct ObInListInfo
{
  ObInListInfo() : in_list_(NULL), in_list_expr_(NULL), row_cnt_(0), column_cnt_(0), is_question_mark_(true) {}
  TO_STRING_KV(K_(in_list_expr));
  const ParseNode *in_list_;
  ObQueryRefRawExpr *in_list_expr_;
  int64_t row_cnt_;
  int64_t column_cnt_;
  bool is_question_mark_;
};

struct ObInListsResolverHelper
{
  ObIAllocator &alloc_;
  const ParamStore *param_store_;
  ObCollationType connect_collation_;
  ObCollationType nchar_collation_;
  ObCollationType server_collation_;
  bool enable_decimal_int_;
  bool is_prepare_stmt_;
  uint64_t optimizer_features_enable_version_;

  ObInListsResolverHelper(ObIAllocator &alloc,
                          const ParamStore *param_store,
                          ObCollationType connect_collation,
                          ObCollationType nchar_collation,
                          ObCollationType server_collation,
                          bool enable_decimal_int,
                          bool is_prepare_stmt,
                          uint64_t optimizer_features_enable_version)
  : alloc_(alloc),
    param_store_(param_store),
    connect_collation_(connect_collation),
    nchar_collation_(nchar_collation),
    server_collation_(server_collation),
    enable_decimal_int_(enable_decimal_int),
    is_prepare_stmt_(is_prepare_stmt),
    optimizer_features_enable_version_(optimizer_features_enable_version) {}

  TO_STRING_KV(K_(connect_collation),
               K_(nchar_collation),
               K_(server_collation),
               K_(enable_decimal_int),
               K_(is_prepare_stmt),
               K_(optimizer_features_enable_version));
};

class ObInListResolver
{
public:
  ObInListResolver(ObDMLResolver *cur_resolver)
    : cur_resolver_(cur_resolver) {}
  virtual ~ObInListResolver() {}
  static int check_inlist_rewrite_enable(const ParseNode &in_list,
                                         const ObItemType op_type,
                                         const ObRawExpr &left_expr,
                                         const ObStmtScope &scope,
                                         const bool is_root_condition,
                                         const bool is_need_print,
                                         const bool is_in_pl_prepare,
                                         const ObSQLSessionInfo *session_info,
                                         const ParamStore *param_store,
                                         const ObStmt *stmt,
                                         common::ObIAllocator &alloc,
                                         bool &is_question_mark,
                                         bool &is_enable);
  // try to merge IN nodes under root_node (which is an AND/OR node)
  // if merge does not happen, the ret_node is root_node
  // if merge happened, the merged node will be a new node with merged children, and if:
  //   1. the merged node has only one child:      ret_node is that child node
  //   2. the merged node has more then one child: ret_node is the merged_node
  static int try_merge_inlists(ObExprResolveContext &resolve_ctx,
                               const bool is_root_condition,
                               const ParseNode *root_node,
                               const ParseNode *&ret_node);
  int resolve_inlist(ObInListInfo &inlist_infos);
private:
  static int get_inlist_rewrite_info(const ParseNode &in_list,
                                     const int64_t column_cnt,
                                     int64_t col_idx,
                                     ObInListsResolverHelper &helper,
                                     InListRewriteInfo &rewrite_info);
  static int check_can_merge_inlists(const ParseNode *last_in_node,
                                     const ParseNode *cur_in_node,
                                     ObInListsResolverHelper &helper,
                                     InListRewriteInfo &last_info,
                                     bool &can_merge);
  static int merge_two_in_nodes(ObIAllocator &alloc,
                                const ParseNode *src_node,
                                ParseNode *&dst_node);
  static int do_merge_inlists(ObIAllocator &alloc,
                              ObInListsResolverHelper &helper,
                              const ParseNode *root_node,
                              const ParseNode *&ret_node);
  int resolve_values_table_from_inlist(const ParseNode *in_list,
                                       const int64_t column_cnt,
                                       const int64_t row_cnt,
                                       const bool is_question_mark,
                                       const bool is_prepare_stmt,
                                       const bool is_called_in_sql,
                                       const ParamStore *param_store,
                                       ObSQLSessionInfo *session_info,
                                       ObIAllocator *allocator,
                                       ObValuesTableDef *&table_def);
  int resolve_subquery_from_values_table(ObStmtFactory *stmt_factory,
                                         ObSQLSessionInfo *session_info,
                                         ObIAllocator *allocator,
                                         ObQueryCtx *query_ctx,
                                         ObRawExprFactory *expr_factory,
                                         ObValuesTableDef *table_def,
                                         const bool is_prepare_stmt,
                                         const int64_t column_cnt,
                                         ObQueryRefRawExpr *query_ref);
  int resolve_access_param_values_table(const ParseNode &in_list,
                                        const int64_t column_cnt,
                                        const int64_t row_cnt,
                                        const ParamStore *param_store,
                                        ObSQLSessionInfo *session_info,
                                        ObIAllocator *allocator,
                                        const bool is_called_in_sql,
                                        ObValuesTableDef &table_def);
  int resolve_access_obj_values_table(const ParseNode &in_list,
                                      const int64_t column_cnt,
                                      const int64_t row_cnt,
                                      ObSQLSessionInfo *session_info,
                                      ObIAllocator *allocator,
                                      const bool is_prepare_stage,
                                      const bool is_called_in_sql,
                                      ObValuesTableDef &table_def);
private:
  ObDMLResolver *cur_resolver_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_INLIST_RESOLVER_H_ */
