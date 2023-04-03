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
 * This file contains implementation for _st_geomfromewkb.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_GEOMFROMTEWKB
#define OCEANBASE_SQL_OB_EXPR_ST_GEOMFROMTEWKB
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"


namespace oceanbase
{
namespace sql
{
class
ObExprPrivSTGeomFromEWKB : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTGeomFromEWKB(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTGeomFromEWKB();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_st_geomfromewkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int get_header_info_from_ewkb(const common::ObString &ewkb,
                                       common::ObGeoWkbHeader &header);
  static int construct_ewkb_data(common::ObString &ewkb,
                                 common::ObString &ewkb_data);
  static int create_geo_by_ewkb(common::ObIAllocator &allocator,
                                common::ObString &ewkb,
                                const common::ObGeoWkbHeader &header,
                                const common::ObSrsItem *srs,
                                common::ObGeometry *&geo);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTGeomFromEWKB);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_GEOMFROMEWKB