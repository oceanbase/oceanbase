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

#ifndef OCEANBASE_SQL_OB_DEFAULT_VALUE_UTILS_
#define OCEANBASE_SQL_OB_DEFAULT_VALUE_UTILS_
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{
enum ObDMLDefaultOp
{
  OB_INVALID_DEFAULT_OP = 0,
  OB_NORMAL_DEFAULT_OP = 1,
  OB_NOT_STRICT_DEFAULT_OP = 2,
  OB_GENERATED_COLUMN_DEFAULT_OP = 3,
  OB_TIMESTAMP_COLUMN_DEFAULT_OP = 4,
  OB_IDENTITY_COLUMN_DEFAULT_OP = 5
};
class ObDMLResolver;
class ObDefaultValueUtils
{
public:
  ObDefaultValueUtils(ObDMLStmt *stmt, ObResolverParams *params, ObDMLResolver *resolver)
      : stmt_(stmt),
        params_(params),
        resolver_(resolver)
  {
  }
  ~ObDefaultValueUtils() {}
  //生成insert values()中不存在列的默认值
  int generate_insert_value(const ColumnItem *column,
                            ObRawExpr* &expr,
                            bool has_instead_of_trigger = false);
  //resolve default()
  int resolve_default_function(ObRawExpr *&expr, ObStmtScope scope);
  static int resolve_default_function_static(const ObTableSchema *table_schema,
                                             const ObSQLSessionInfo &session_info,
                                             ObRawExprFactory &expr_factory,
                                             ObRawExpr *&expr,
                                             const ObResolverUtils::PureFunctionCheckStatus
                                                  check_status);
  //resolve T_DFFAULT
  int resolve_default_expr(const ColumnItem &column_item, ObRawExpr *&expr, ObStmtScope scope);
  int build_default_expr_strict(const ColumnItem *column,
                                ObRawExpr *&const_expr);
  int build_default_expr_for_identity_column(const ColumnItem &column, 
                                             ObRawExpr *&expr,
                                             ObStmtScope scope);
  int build_now_expr(const ColumnItem *column, ObRawExpr *&const_expr);
  int build_expr_default_expr(const ColumnItem *column,
                              ObRawExpr *&input_expr,
                              ObRawExpr *&const_expr);
  int resolve_column_ref_in_insert(const ColumnItem *column, ObRawExpr *&expr);
private:
  int get_default_type_for_insert(const ColumnItem *column, ObDMLDefaultOp &op);

  int get_default_type_for_default_function(const ColumnItem *column,
                                            ObDMLDefaultOp &op,
                                            ObStmtScope scope);
  int get_default_type_for_default_expr(const ColumnItem *column,
                                        ObDMLDefaultOp &op,
                                        ObStmtScope scope);
  int build_default_expr_for_timestamp(const ColumnItem *column, ObRawExpr *&expr);
  int get_default_type_for_column_expr(const ColumnItem *column, ObDMLDefaultOp &op);

  int build_default_expr_not_strict(const ColumnItem *column, ObRawExpr *&expr);

  int build_default_function_expr(const ColumnItem *column,
                                  ObRawExpr *&expr,
                                  ObStmtScope scope,
                                  const bool is_default_expr);
  int build_collation_expr(const ColumnItem *column, ObRawExpr *&expr);
  
  int build_accuracy_expr(const ColumnItem *column, ObRawExpr *&expr);
  int build_type_expr(const ColumnItem *column, ObRawExpr *&expr);
  static int build_type_expr_static(ObRawExprFactory& expr_factory, 
                                    const common::ColumnType data_type, 
                                    ObRawExpr *&expr);
  static int build_collation_expr_static(ObRawExprFactory& expr_factory, 
                                         const common::ObCollationType coll_type, 
                                         ObRawExpr *&expr);
  static int build_accuracy_expr_static(ObRawExprFactory& expr_factory, 
                                        const common::ObAccuracy& accuracy, 
                                        ObRawExpr *&expr);
  static int build_nullable_expr_static(ObRawExprFactory& expr_factory, 
                                        const bool nullable, 
                                        ObRawExpr *&expr);
  static int build_default_function_expr_static(ObRawExprFactory& expr_factory, 
                                                const ObColumnSchemaV2 *column_schema, 
                                                ObRawExpr *&expr,
                                                const ObSQLSessionInfo &session_info);
  static int build_default_expr_strict_static(ObRawExprFactory& expr_factory, 
                                              const ObColumnSchemaV2 *column_schema,
                                              ObRawExpr *&expr,
                                              const ObSQLSessionInfo &session_info);
  static int get_default_type_for_default_function_static(const ObColumnSchemaV2 *column_schema,
                                                          ObDMLDefaultOp &op);

  static int build_default_expr_not_strict_static(ObRawExprFactory& expr_factory, 
                                                  const ObColumnSchemaV2 *column_schema,
                                                  ObRawExpr *&expr,
                                                  const ObSQLSessionInfo &session_info);
  int build_nullable_expr(const ColumnItem *column, ObRawExpr *&expr);
  int build_default_expr_for_generated_column(const ColumnItem &column, ObRawExpr *&expr);
  int build_default_expr_for_gc_column_ref(const ColumnItem &column, ObRawExpr *&expr);

private:
  ObDMLStmt *stmt_;
  ObResolverParams *params_;
  ObDMLResolver *resolver_;
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_DEFAULT_VALUE_UTILS_

