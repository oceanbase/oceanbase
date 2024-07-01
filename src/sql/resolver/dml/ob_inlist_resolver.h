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
                                         const bool is_prepare_protocol,
                                         const bool is_in_pl,
                                         const ObSQLSessionInfo *session_info,
                                         const ParamStore *param_store,
                                         const ObStmt *stmt,
                                         common::ObIAllocator &alloc,
                                         bool &is_question_mark,
                                         bool &is_enable);
  int resolve_inlist(ObInListInfo &inlist_infos);
private:
  int resolve_values_table_from_inlist(const ParseNode *in_list,
                                       const int64_t column_cnt,
                                       const int64_t row_cnt,
                                       const bool is_question_mark,
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
  static int get_const_node_types(const ParseNode *node,
                                  const ParamStore *param_store,
                                  const bool is_question_mark,
                                  const ObCollationType connect_collation,
                                  const ObCollationType nchar_collation,
                                  const ObCollationType server_collation,
                                  const bool enable_decimal_int,
                                  ObIAllocator &alloc,
                                  DistinctObjMeta &param_type,
                                  bool &is_const);
  int resolve_access_param_values_table(const ParseNode &in_list,
                                        const int64_t column_cnt,
                                        const int64_t row_cnt,
                                        const ParamStore *param_store,
                                        ObSQLSessionInfo *session_info,
                                        ObIAllocator *allocator,
                                        ObValuesTableDef &table_def);
  int resolve_access_obj_values_table(const ParseNode &in_list,
                                      const int64_t column_cnt,
                                      const int64_t row_cnt,
                                      ObSQLSessionInfo *session_info,
                                      ObIAllocator *allocator,
                                      ObValuesTableDef &table_def);
private:
  ObDMLResolver *cur_resolver_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_INLIST_RESOLVER_H_ */
