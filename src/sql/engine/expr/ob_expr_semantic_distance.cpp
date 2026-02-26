/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

 #define USING_LOG_PREFIX SQL_ENG
 #include "sql/engine/expr/ob_expr_semantic_distance.h"
 #include "sql/engine/expr/ob_expr_operator.h"
 #include "sql/session/ob_sql_session_info.h"
 #include "sql/engine/ob_exec_context.h"
 #include "share/vector_index/ob_vector_index_util.h"
 #include "objit/include/objit/common/ob_item_type.h"
 #include "sql/engine/expr/ob_array_expr_utils.h"
 #include "share/vector_type/ob_vector_l2_distance.h"
 #include "share/vector_type/ob_vector_cosine_distance.h"
 #include "share/vector_type/ob_vector_ip_distance.h"
 #include "share/vector_type/ob_vector_l1_distance.h"

 namespace oceanbase
 {
 namespace sql
 {

 ObExprSemanticDistance::ObExprSemanticDistance(common::ObIAllocator &alloc)
     : ObFuncExprOperator(alloc, T_FUN_SYS_SEMANTIC_DISTANCE, N_SEMANTIC_DISTANCE, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
 {
 }

 int ObExprSemanticDistance::calc_result_type2(ObExprResType &type,
                                               ObExprResType &type1,
                                               ObExprResType &type2,
                                               common::ObExprTypeCtx &type_ctx) const
 {
   int ret = OB_SUCCESS;

   UNUSED(type1);
   UNUSED(type2);

   const ObRawExpr *raw_expr = type_ctx.get_raw_expr();
   if (raw_expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN && raw_expr->get_param_expr(1)->get_expr_type() != T_REF_COLUMN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("none param of semantic_distance is col ref",
      K(raw_expr->get_param_expr(0)->get_expr_type()), K(raw_expr->get_param_expr(1)->get_expr_type()));
   } else {
    type.set_double();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
   }
   return ret;
 }

 int ObExprSemanticDistance::calc_semantic_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
 {
   // no need to support, this expr is only used for parser
   return OB_NOT_SUPPORTED;
 }

 int ObExprSemanticDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
 {
   int ret = OB_SUCCESS;

   rt_expr.eval_func_ = calc_semantic_distance;

   return ret;
 }

 ObExprSemanticVectorDistance::ObExprSemanticVectorDistance(common::ObIAllocator &alloc)
     : ObExprVectorDistance(alloc, T_FUN_SYS_SEMANTIC_VECTOR_DISTANCE, N_SEMANTIC_VECTOR_DISTANCE, TWO_OR_THREE, NOT_ROW_DIMENSION)
 {
 }

 int ObExprSemanticVectorDistance::calc_result_typeN(ObExprResType &type,
                                                     ObExprResType *types_stack,
                                                     int64_t param_num,
                                                     common::ObExprTypeCtx &type_ctx) const
 {
   int ret = OB_SUCCESS;
   if (OB_UNLIKELY(param_num > 3) || OB_UNLIKELY(param_num < 2)) {
     ObString func_name_(get_name());
     ret = OB_ERR_PARAM_SIZE;
     LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
   } else if (param_num == 2) {
     const ObRawExpr *raw_expr = type_ctx.get_raw_expr();
     if (raw_expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN && raw_expr->get_param_expr(1)->get_expr_type() != T_REF_COLUMN) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("none param of semantic_distance is col ref",
        K(raw_expr->get_param_expr(0)->get_expr_type()), K(raw_expr->get_param_expr(1)->get_expr_type()));
     } else {
       type.set_type(ObDoubleType);
       type.set_calc_type(ObDoubleType);
     }
   } else {
     type.set_type(ObDoubleType);
     type.set_calc_type(ObDoubleType);
   }
   return ret;
 }


 int ObExprSemanticVectorDistance::calc_semantic_vector_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
 {
   int ret = OB_SUCCESS;
   ObExprVectorDistance::ObVecDisType dis_type = ObExprVectorDistance::ObVecDisType::MAX_TYPE;

   if (3 != expr.arg_cnt_) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("unexpected argument count", K(ret), K(expr.arg_cnt_));
   } else {
     ObDatum *datum = NULL;
     if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
       LOG_WARN("eval distance_type failed", K(ret));
     } else if (datum->is_null()) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("distance_type is null", K(ret));
     } else {
       dis_type = static_cast<ObExprVectorDistance::ObVecDisType>(datum->get_int());
     }
   }

   if (OB_SUCC(ret)) {
     if (OB_FAIL(ObExprVectorDistance::calc_distance(expr, ctx, res_datum, dis_type))) {
      LOG_WARN("failed to calc distance", K(ret), K(dis_type));
     }
   }

   return ret;
 }

 int ObExprSemanticVectorDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
 {
   int ret = OB_SUCCESS;

   rt_expr.eval_func_ = calc_semantic_vector_distance;

   return ret;
 }

 } // namespace sql
 } // namespace oceanbase