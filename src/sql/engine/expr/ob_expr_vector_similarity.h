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

#ifndef OCEANBASE_SQL_OB_EXPR_VECTOR_SIMILARITY
#define OCEANBASE_SQL_OB_EXPR_VECTOR_SIMILARITY

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "lib/udt/ob_array_type.h"
#include "share/vector_type/ob_vector_l2_similarity.h"
#include "share/vector_type/ob_vector_cosine_similarity.h"
#include "share/vector_type/ob_vector_ip_similarity.h"


namespace oceanbase
{
namespace sql
{
class ObExprVectorSimilarity : public ObExprVector
{
public:
  enum ObVecSimilarityType
  {
    COSINE = 0,
    DOT, // inner product
    EUCLIDEAN, // L2
    MAX_TYPE,
  };
  template <typename T = float>
  struct SimilarityFunc {
    using FuncPtrType = int (*)(const T* a, const T* b, const int64_t len, double& similarity);
    static FuncPtrType similarity_funcs[];
  };
public:
  explicit ObExprVectorSimilarity(common::ObIAllocator &alloc);
  explicit ObExprVectorSimilarity(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprVectorSimilarity() {};
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int calc_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum, ObVecSimilarityType dis_type);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorSimilarity);
};

template <typename T>
typename ObExprVectorSimilarity::SimilarityFunc<T>::FuncPtrType ObExprVectorSimilarity::SimilarityFunc<T>::similarity_funcs[] =
{
  ObVectorCosineSimilarity<T>::cosine_similarity_func,
  ObVectorIPSimilarity<T>::ip_similarity_func,
  ObVectorL2Similarity<T>::l2_similarity_func,
  nullptr,
};

class ObExprVectorL2Similarity : public ObExprVectorSimilarity
{
public:
  explicit ObExprVectorL2Similarity(common::ObIAllocator &alloc);
  virtual ~ObExprVectorL2Similarity() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_l2_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorL2Similarity);
};

class ObExprVectorCosineSimilarity : public ObExprVectorSimilarity
{
public:
  explicit ObExprVectorCosineSimilarity(common::ObIAllocator &alloc);
  virtual ~ObExprVectorCosineSimilarity() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_cosine_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorCosineSimilarity);
};

class ObExprVectorIPSimilarity : public ObExprVectorSimilarity
{
public:
  explicit ObExprVectorIPSimilarity(common::ObIAllocator &alloc);
  virtual ~ObExprVectorIPSimilarity() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_ip_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorIPSimilarity);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_VECTOR_SIMILARITY
