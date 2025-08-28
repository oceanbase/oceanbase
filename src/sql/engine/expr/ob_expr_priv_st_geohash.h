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
 * This file contains implementation for _st_geohash.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_GEOHASH_
#define OCEANBASE_SQL_OB_EXPR_ST_GEOHASH_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTGeoHash : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTGeoHash(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTGeoHash();
  virtual int calc_result_typeN(
                  ObExprResType &type,
                  ObExprResType *types,
                  int64_t param_num,
                  ObExprTypeCtx &type_ctx) const override;
  static int eval_priv_st_geohash(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr( 
                  ObExprCGCtx &expr_cg_ctx,
                  const ObRawExpr &raw_expr,
                  ObExpr &rt_expr) const override;
private:
  static int process_input_geometry(
                const ObExpr &expr,
                ObEvalCtx &ctx,
                MultimodeAlloctor &allocator,
                bool &is_null_res,
                ObGeometry *&geo,
                int &precision);
  static int get_gbox(lib::MemoryContext &mem_ctx, ObGeometry *&geo, ObGeogBox *&gbox);
  static int calc_precision(ObGeogBox *&gbox, ObGeogBox *&bounds, int &precision);
  static int calc_geohash(ObGeogBox *&gbox, int precision, ObStringBuffer &geohash_buf);

  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTGeoHash);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_GEOHASH_