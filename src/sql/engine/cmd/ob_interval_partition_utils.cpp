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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_interval_partition_utils.h"

#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"

namespace oceanbase
{
using namespace share::schema;
namespace sql
{

int ObIntervalPartitionUtils::set_interval_value(
  ObExecContext &ctx,
  const stmt::StmtType stmt_type,
  ObTableSchema &table_schema,
  ObRawExpr *interval_expr)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  if (OB_ISNULL(interval_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("interval_expr is null", KR(ret));
  } else if (OB_FAIL(ObPartitionExecutorUtils::expr_cal_and_cast_with_check_varchar_len(
             stmt_type,
             false /*is_list_part*/,
             ctx,
             interval_expr->get_result_type(),
             interval_expr,
             value_obj))) {
    LOG_WARN("fail to cast_expr_to_obj", KR(ret));
  } else {
    ObRowkey interval_rowkey;
    interval_rowkey.assign(&value_obj, 1);

    OX (table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_INTERVAL));
    OZ (table_schema.set_interval_range(interval_rowkey));
  }
  return ret;
}

int ObIntervalPartitionUtils::check_transition_interval_valid(
  const stmt::StmtType stmt_type,
  ObExecContext &ctx,
  ObRawExpr *transition_expr,
  ObRawExpr *interval_expr)
{
  int ret = OB_SUCCESS;
  ParamStore dummy_params;
  ObObj temp_obj;
  ObRawExprFactory raw_expr_factory(ctx.get_allocator());

  CK (transition_expr != NULL);
  CK (interval_expr != NULL);
  CK (interval_expr->is_const_expr());
  OZ (ObSQLUtils::calc_simple_expr_without_row(ctx.get_my_session(),
                                   interval_expr, temp_obj, &dummy_params, ctx.get_allocator()));
  if (OB_SUCC(ret) && temp_obj.is_zero()) {
    ret = OB_ERR_INTERVAL_CANNOT_BE_ZERO;
    LOG_WARN("interval can not be zero");
  }
  if (OB_SUCC(ret) && ((transition_expr->get_data_type() == ObDateTimeType
          ||transition_expr->get_data_type() == ObTimestampNanoType)
        && (interval_expr->get_data_type() == ObIntervalYMType))) {
    ObOpRawExpr *add_expr = NULL;
    ObRawExpr *tmp_expr = transition_expr;
    /* 对于年月的间隔，最多需要加 49 次，才能判断是否合法 */
    /* e.g. transition_point: 1904-02-29, interval_range: 4 year */
    for (int i = 0; OB_SUCC(ret) && i < 49; i ++) {
      OX (add_expr = NULL);
      OZ (raw_expr_factory.create_raw_expr(T_OP_ADD, add_expr));
      CK (OB_NOT_NULL(add_expr));
      OZ (add_expr->set_param_exprs(tmp_expr, interval_expr));
      OX (tmp_expr = add_expr);
    }
    CK (OB_NOT_NULL(add_expr));
    OZ (add_expr->formalize(ctx.get_my_session()));
    OZ (ObSQLUtils::calc_simple_expr_without_row(ctx.get_my_session(),
                                  add_expr, temp_obj, &dummy_params, ctx.get_allocator()));
    if (OB_ERR_DAY_OF_MONTH_RANGE == ret) {
      ret = OB_ERR_INVALID_INTERVAL_HIGH_BOUNDS;
      LOG_WARN("fail to calc value", KR(ret), KPC(add_expr));
    }
  }
  return ret;
}

int ObIntervalPartitionUtils::check_transition_interval_consistent(
  const share::schema::ObTableSchema &table_schema,
  ObExecContext &ctx,
  ObAlterTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ParamStore dummy_params;
  ObObj interval_obj;
  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> transition_objs;
  ObDDLStmt::array_t transition_exprs;

  ObRawExprFactory raw_expr_factory(ctx.get_allocator());
  ObRawExpr *transition_expr = stmt.get_transition_expr_for_add_partition();
  ObRawExpr *interval_expr = stmt.get_interval_expr_for_add_partition();
  bool is_interval_equal = false;
  bool is_transition_equal = false;

  CK (OB_NOT_NULL(interval_expr));
  CK (OB_NOT_NULL(transition_expr));
  OZ (transition_exprs.push_back(transition_expr));
  OZ (ObSQLUtils::calc_simple_expr_without_row(ctx.get_my_session(),
                                               interval_expr,
                                               interval_obj,
                                               &dummy_params,
                                               ctx.get_allocator()));
  OZ (ObPartitionExecutorUtils::cast_expr_to_obj(ctx,
                                                 stmt::T_ALTER_TABLE,
                                                 false/*is_list_part*/,
                                                 stmt.get_part_fun_exprs(),
                                                 transition_exprs,
                                                 transition_objs));

  CK (table_schema.get_interval_range().is_valid());
  CK (table_schema.get_transition_point().is_valid());
  CK (1 == transition_objs.count());

  OZ (table_schema.get_interval_range().get_obj_ptr()[0].equal(interval_obj, is_interval_equal));
  OZ (table_schema.get_transition_point().get_obj_ptr()[0].equal(transition_objs.at(0), is_transition_equal));

  if (OB_SUCC(ret) && (!is_interval_equal || !is_transition_equal)) {
    ret = OB_ERR_INTERVAL_PARTITION_ERROR;
    LOG_WARN("interval and transition are not consistent", KR(ret), K(table_schema.get_interval_range()), K(table_schema.get_transition_point()),
             K(interval_obj), K(transition_objs.at(0)));
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
