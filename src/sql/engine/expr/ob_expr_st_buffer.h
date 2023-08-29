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
 * This file contains implementation for st_buffer and st_buffer_strategy.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_BUFFER
#define OCEANBASE_SQL_OB_EXPR_ST_BUFFER

#include "sql/engine/expr/ob_expr_operator.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

namespace oceanbase
{
namespace sql
{

// for st_buffer_strategy and st_buffer
enum class ObGeoBufferStrategyType
{
  INVALID = 0,
  END_ROUND = 1,
  END_FLAT = 2,
  JOIN_ROUND = 3,
  JOIN_MITER = 4,
  POINT_CIRCLE = 5,
  POINT_SQUARE = 6,
};

class ObExprSTBufferStrategy : public ObFuncExprOperator
{
public:
  static constexpr int BUF_STRATEGY_RES_LENGTH = 16;
  explicit ObExprSTBufferStrategy(common::ObIAllocator &alloc);
  virtual ~ObExprSTBufferStrategy() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_st_buffer_strategy(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

public:
  static ObGeoBufferStrategyType get_strategy_type_by_name(const common::ObString &name);

private:
    DISALLOW_COPY_AND_ASSIGN(ObExprSTBufferStrategy);
};

class ObExprSTBuffer : public ObFuncExprOperator
{
public:
  explicit ObExprSTBuffer(common::ObIAllocator &alloc);
  explicit ObExprSTBuffer(common::ObIAllocator &alloc,
                          ObExprOperatorType type,
                          const char *name,
                          int32_t param_num,
                          ObValidForGeneratedColFlag valid_for_genetated_col,
                          int32_t dimension);
  virtual ~ObExprSTBuffer() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_st_buffer(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
protected:
  static bool is_valid_distance(double distance);
  static int parse_binary_strategy(const common::ObString &str, common::ObGeoBufferStrategy &strategy);
  static int parse_text_strategy(common::ObString &str, common::ObGeoBufferStrategy &strategy);
  static int init_buffer_strategy(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObIAllocator &allocator,
                                  common::ObGeoBufferStrategy &buf_strat,
                                  double distance);
  static int init_buffer_strategy(const common::ObObj *params,
                                  int64_t param_num,
                                  common::ObExprCtx &expr_ctx,
                                  common::ObGeoBufferStrategy &buf_strat,
                                  double distance);
  static int fill_proj4_params(common::ObIAllocator &allocator,
                               omt::ObSrsCacheGuard &srs_guard,
                               uint32 srid,
                               common::ObGeometry *geo,
                               const common::ObSrsItem *srs,
                               common::ObGeoBufferStrategy &buf_strat,
                               bool &is_transform_method);
private:
  // Notice: Do not change order of mask and state type of buffer strategy!
  static const uint8_t JOIN_MITER_MASK = 4;
  static const uint8_t END_FLAT_MASK = 2;
  static const uint8_t POINT_SQUARE_MASK = 1;
  DISALLOW_COPY_AND_ASSIGN(ObExprSTBuffer);
};

class ObExprPrivSTBuffer : public ObExprSTBuffer
{
public:
  explicit ObExprPrivSTBuffer(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTBuffer() {}
  virtual int calc_result_typeN(ObExprResType &type,
                              ObExprResType *types,
                              int64_t param_num,
                              common::ObExprTypeCtx &type_ctx) const;
  static int eval_priv_st_buffer(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTBuffer);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_BUFFER