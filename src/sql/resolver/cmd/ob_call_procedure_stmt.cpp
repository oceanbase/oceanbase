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

#define USING_LOG_PREFIX SQL_RESV

#include "ob_call_procedure_stmt.h"
#include "pl/ob_pl_type.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"

namespace oceanbase
{
namespace sql
{


int ObCallProcedureInfo::add_out_param(
  int64_t i, int64_t mode, const ObString &name,
  const pl::ObPLDataType &type,
  const ObString &out_type_name, const ObString &out_type_owner)
{
  int ret = OB_SUCCESS;
  ObString store_name;
  ObString store_out_type_name;
  ObString store_out_type_owner;
  pl::ObPLDataType pl_data_type;
  if (OB_FAIL(out_idx_.add_member(i))) {
    LOG_WARN("failed to add out index", K(i), K(name), K(type), K(ret));
  } else if (OB_FAIL(out_mode_.push_back(mode))) {
    LOG_WARN("failed to push mode", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, name, store_name))) {
    LOG_WARN("failed to deep copy name", K(ret), K(name));
  } else if (OB_FAIL(out_name_.push_back(store_name))) {
    LOG_WARN("push back error", K(i), K(name), K(type), K(ret));
  } else if (OB_FAIL(pl_data_type.deep_copy(allocator_, type))) {
    LOG_WARN("fail to deep copy pl data type", K(type), K(ret));
  } else if (OB_FAIL(out_type_.push_back(pl_data_type))) {
    LOG_WARN("push back error", K(i), K(name), K(type), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, out_type_name, store_out_type_name))) {
    LOG_WARN("failed to deep copy name", K(ret), K(name));
  } else if (OB_FAIL(out_type_name_.push_back(store_out_type_name))) {
    LOG_WARN("push back error", K(i), K(name), K(type), K(out_type_name), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, out_type_owner, store_out_type_owner))) {
    LOG_WARN("failed to deep copy name", K(ret), K(name));
  } else if (OB_FAIL(out_type_owner_.push_back(store_out_type_owner))) {
    LOG_WARN("push back error", K(i), K(name), K(ret), K(out_type_name), K(out_type_owner), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObCallProcedureInfo::prepare_expression(const common::ObIArray<sql::ObRawExpr*> &params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSqlExpression*, 16> array;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObSqlExpression *expr = NULL;
    if (OB_FAIL(sql_expression_factory_.alloc(expr))) {
      LOG_WARN("failed to alloc expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(array.push_back(expr))) {
      LOG_WARN("push back error", K(ret));
    } else { /*do nothing*/ }
  }

  OZ (expressions_.assign(array));

  return ret;
}

int ObCallProcedureInfo::final_expression(const common::ObIArray<sql::ObRawExpr*> &params,
                                          ObSQLSessionInfo *session_info,
                                          share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;

  // generate static engine expressions
  sql::ObRawExprUniqueSet raw_exprs(false);
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    OZ(raw_exprs.append(params.at(i)));
  }
  sql::ObStaticEngineExprCG se_cg(allocator_,
                                  session_info,
                                  schema_guard,
                                  0 /* original param cnt */,
                                  0/* param count*/,
                                  GET_MIN_CLUSTER_VERSION());
  se_cg.set_rt_question_mark_eval(true);
  OZ(se_cg.generate(raw_exprs, frame_info_));

  uint32_t expr_op_size = 0;
  RowDesc row_desc;
  ObExprGeneratorImpl expr_generator(expr_operator_factory_, 0, 0,
                                      &expr_op_size, row_desc);
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObRawExpr *raw_expr = params.at(i);
    ObSqlExpression *expression = static_cast<ObSqlExpression*>(expressions_.at(i));
    if (OB_ISNULL(raw_expr) || OB_ISNULL(expression)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid arguments", K(i), K(raw_expr), K(expression), K(ret));
    } else {
      if (OB_FAIL(expr_generator.generate(*raw_expr, *expression))) {
        SQL_LOG(WARN, "Generate post_expr error", K(ret), KPC(raw_expr));
      } else {
        expression->set_expr(raw_expr->rt_expr_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    expr_op_size_ = std::max(frame_info_.need_ctx_cnt_, static_cast<int64_t>(expr_op_size));
  }

  return ret;
}

void ObCallProcedureInfo::reset()
{
  pl::ObPLCacheObject::reset();
  can_direct_use_param_ = false;
  package_id_ = common::OB_INVALID_ID;
  routine_id_ = common::OB_INVALID_ID;
  param_cnt_ = 0;
  is_udt_routine_ = false;
}

int ObCallProcedureInfo::check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add)
{
  int ret = OB_SUCCESS;

  //ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  //need_real_add = pc_ctx.need_add_obj_stat_;
  need_real_add = true;

  return ret;
}

void ObCallProcedureInfo::dump_deleted_log_info(const bool is_debug_log /* = true */) const
{

}

}
}


