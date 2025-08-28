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
 * This file contains implementation for array.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_VECTOR
#define OCEANBASE_SQL_OB_EXPR_VECTOR

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_type/ob_vector_l1_distance.h"
#include "share/vector_type/ob_sparse_vector_ip_distance.h"


namespace oceanbase
{
namespace sql
{
class ObExprVector : public ObFuncExprOperator
{
public:

  static const int64_t MAX_VECTOR_DIM = 16000;
  
  struct VectorCastInfo
  {
    VectorCastInfo()
      : is_vector_(false),
        need_cast_(false),
        subschema_id_(UINT16_MAX),
        dim_cnt_(0)
    {}
    bool is_vector_;
    bool need_cast_;
    uint16_t subschema_id_;
    uint16_t dim_cnt_;
  };

public:
  explicit ObExprVector(common::ObIAllocator &alloc, ObExprOperatorType type, 
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprVector() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
private:
  int collect_vector_cast_info(ObExprResType &type, ObExecContext &exec_ctx, VectorCastInfo &info) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprVector);
};

class ObExprVectorDistance : public ObExprVector
{
public:
  enum ObVecDisType
  {
    COSINE = 0,
    DOT, // inner product
    EUCLIDEAN, // L2
    MANHATTAN, // L1
    EUCLIDEAN_SQUARED, // L2_SQUARED
    HAMMING,
    MAX_TYPE,
  };
  template <typename T = float>
  struct DisFunc {
    using FuncPtrType = int (*)(const T* a, const T* b, const int64_t len, double& distance);
    static FuncPtrType distance_funcs[];
  };
  struct SparseVectorDisFunc {
    using FuncPtrType = int (*)(const ObMapType* a, const ObMapType* b, double& distance);
    static FuncPtrType spiv_distance_funcs[];
  };
public:
  explicit ObExprVectorDistance(common::ObIAllocator &alloc);
  explicit ObExprVectorDistance(common::ObIAllocator &alloc, ObExprOperatorType type, 
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprVectorDistance() {};
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int calc_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum, ObVecDisType dis_type);
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorDistance);
};

template <typename T>
typename ObExprVectorDistance::DisFunc<T>::FuncPtrType ObExprVectorDistance::DisFunc<T>::distance_funcs[] = 
{
  ObVectorCosineDistance<T>::cosine_distance_func,
  ObVectorIpDistance<T>::ip_distance_func,
  ObVectorL2Distance<T>::l2_distance_func,
  ObVectorL1Distance<T>::l1_distance_func,
  ObVectorL2Distance<T>::l2_square_func,
  nullptr,
};

class ObExprVectorL1Distance : public ObExprVectorDistance
{
public:
  explicit ObExprVectorL1Distance(common::ObIAllocator &alloc);
  virtual ~ObExprVectorL1Distance() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_l1_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorL1Distance);
};

class ObExprVectorL2Distance : public ObExprVectorDistance
{
public:
  explicit ObExprVectorL2Distance(common::ObIAllocator &alloc);
  virtual ~ObExprVectorL2Distance() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_l2_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorL2Distance);
};

class ObExprVectorL2Squared : public ObExprVectorDistance
{
public:
  explicit ObExprVectorL2Squared(common::ObIAllocator &alloc);
  virtual ~ObExprVectorL2Squared() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_l2_squared(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorL2Squared);
};

class ObExprVectorCosineDistance : public ObExprVectorDistance
{
public:
  explicit ObExprVectorCosineDistance(common::ObIAllocator &alloc);
  virtual ~ObExprVectorCosineDistance() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_cosine_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorCosineDistance);
};

class ObExprVectorIPDistance : public ObExprVectorDistance
{
public:
  explicit ObExprVectorIPDistance(common::ObIAllocator &alloc);
  virtual ~ObExprVectorIPDistance() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_inner_product(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorIPDistance);
};

class ObExprVectorNegativeIPDistance : public ObExprVectorDistance
{
public:
  explicit ObExprVectorNegativeIPDistance(common::ObIAllocator &alloc);
  virtual ~ObExprVectorNegativeIPDistance() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_negative_inner_product(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorNegativeIPDistance);
};

class ObExprVectorDims : public ObExprVector
{
public:
  explicit ObExprVectorDims(common::ObIAllocator &alloc);
  virtual ~ObExprVectorDims() {};
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_dims(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorDims);
};

class ObExprVectorNorm : public ObExprVector
{
public:
  explicit ObExprVectorNorm(common::ObIAllocator &alloc);
  virtual ~ObExprVectorNorm() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_norm(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVectorNorm);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_VECTOR
