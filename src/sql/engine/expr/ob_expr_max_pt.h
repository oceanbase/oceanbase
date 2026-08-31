/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_MAX_PT_H_
#define _OCEANBASE_SQL_OB_EXPR_MAX_PT_H_
#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_inner_sql_connection.h"

namespace oceanbase
{
namespace sql
{
class ObExprMaxPt : public ObFuncExprOperator
{
public:
  explicit  ObExprMaxPt(common::ObIAllocator &alloc);
  virtual ~ObExprMaxPt();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &expr) const override;
  static int eval_max_pt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int resolve_table_name_and_schema(
    const ObCollationType cs_type, const ObSQLSessionInfo *session,
    const ObString &name, ObString &database_name, ObString &table_name,
    const ObTableSchema *&table_schema);
private:
  static void upper_db_table_name(
    const ObNameCaseMode case_mode, const bool is_oracle_mode, ObString &name);
  static int check_max_pt_privilege(
    ObEvalCtx &ctx,
    const ObString &db_name,
    const ObString &table_name,
    const ObTableSchema *table_schema);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprMaxPt);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_MAX_PT_H_
