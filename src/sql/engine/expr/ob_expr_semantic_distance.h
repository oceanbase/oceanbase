/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SQL_OB_EXPR_SEMANTIC_DISTANCE
 #define OCEANBASE_SQL_OB_EXPR_SEMANTIC_DISTANCE

 #include "sql/engine/expr/ob_expr_operator.h"
 #include "sql/engine/expr/ob_expr_vector.h"

 namespace oceanbase
 {
 namespace sql
 {

 class ObExprSemanticDistance : public ObFuncExprOperator
 {
 public:
   explicit ObExprSemanticDistance(common::ObIAllocator &alloc);
   virtual ~ObExprSemanticDistance() {};

   virtual int calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 common::ObExprTypeCtx &type_ctx) const override;

   static int calc_semantic_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

   virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

 private:
   DISALLOW_COPY_AND_ASSIGN(ObExprSemanticDistance);
 };

 class ObExprSemanticVectorDistance : public ObExprVectorDistance
 {
 public:
   explicit ObExprSemanticVectorDistance(common::ObIAllocator &alloc);
   virtual ~ObExprSemanticVectorDistance() {};

   // sematic_vector_distance(chunk_col, query_vector) - foe parser
   // sematic_vector_distance(embedded_col, query_vector, distance_type) - for calc
   virtual int calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types_stack,
                                 int64_t param_num,
                                 common::ObExprTypeCtx &type_ctx) const override;

   static int calc_semantic_vector_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

   virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

 private:
   DISALLOW_COPY_AND_ASSIGN(ObExprSemanticVectorDistance);
 };

 } // namespace sql
 } // namespace oceanbase

 #endif // OCEANBASE_SQL_OB_EXPR_SEMANTIC_DISTANCE