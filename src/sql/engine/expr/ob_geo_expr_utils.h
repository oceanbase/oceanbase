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
 * This file contains implementation for ob_geo_expr_utils.
 */

#ifndef OCEANBASE_SQL_OB_GEO_EXPR_UTILS_H_
#define OCEANBASE_SQL_OB_GEO_EXPR_UTILS_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/geo/ob_geo.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_common.h"
#include "sql/engine/expr/ob_expr.h" // for ObExpr
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_i_sql_expression.h" // for ObExprCtx
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{

enum class ObGeoAxisOrder
{
  LONG_LAT = 0,
  LAT_LONG = 1,
  SRID_DEFINED = 2,
  INVALID = 3,
};

class ObGeoExprUtils
{
public:
  ObGeoExprUtils();
  virtual ~ObGeoExprUtils() = default;
  static int get_srs_item(uint64_t tenant_id,
                          omt::ObSrsCacheGuard &srs_guard,
                          const uint32_t srid,
                          const common::ObSrsItem *&srs);
  static int get_srs_item(ObEvalCtx &ctx,
                          omt::ObSrsCacheGuard &srs_guard,
                          const common::ObString &wkb,
                          const common::ObSrsItem *&srs,
                          bool use_little_bo = false,
                          const char *func_name = NULL);
  static int build_geometry(common::ObIAllocator &allocator,
                            const common::ObString &wkb,
                            common::ObGeometry *&geo,
                            const common::ObSrsItem *srs,
                            const char *func_name,
                            const bool need_normlize = true,
                            const bool need_check_ring = false,
                            const bool need_correct = true);
  static int construct_geometry(common::ObIAllocator &allocator,
                                const common::ObString &wkb,
                                omt::ObSrsCacheGuard &srs_guard,
                                const common::ObSrsItem *&srs,
                                common::ObGeometry *&geo,
                                const char *func_name,
                                bool has_srid = true);
  static int check_coordinate_range(const common::ObSrsItem *srs,
                                    common::ObGeometry *geo,
                                    const char *func_name,
                                    const bool is_param = false,
                                    const bool is_normalized = false);
  static int parse_axis_order(const common::ObString option_str,
                              const char *func_name,
                              ObGeoAxisOrder &axis_order);
  static int check_need_reverse(ObGeoAxisOrder axis_order,
                                bool &need_reverse);
  static int correct_coordinate_range(const common::ObSrsItem *srs_item,
                                      common::ObGeometry *geo,
                                      const char *func_name);
  static int check_empty(common::ObGeometry *geo,
                         bool &is_empty);
  static int parse_srid(const common::ObString &srid_str,
                        uint32_t &srid);
  static int get_box_bestsrid(common::ObGeogBox *geo_box1,
                              common::ObGeogBox *geo_box2,
                              int32 &bestsrid);
  static int normalize_wkb(const common::ObSrsItem *srs,
                           common::ObString &wkb,
                           common::ObArenaAllocator &allocator,
                           common::ObGeometry *&geo);
  static int normalize_wkb(common::ObString &proj4text,
                           common::ObGeometry *geo); // for st_transform
  static int denormalize_wkb(common::ObString &proj4text,
                             common::ObGeometry *geo); // for st_transform
  static int geo_to_wkb(common::ObGeometry &geo,
                        const ObExpr &expr,
                        ObEvalCtx &ctx,
                        const common::ObSrsItem *srs_item,
                        common::ObString &res_wkb,
                        uint32_t srs_id = 0);
  static int geo_to_wkb(common::ObGeometry &geo,
                        common::ObExprCtx &expr_ctx,
                        const common::ObSrsItem *srs_item,
                        common::ObString &res_wkb,
                        uint32_t srs_id = 0);
  static void geo_func_error_handle(int ret, const char* func_name);
  static int zoom_in_geos_for_relation(common::ObGeometry &geo1, common::ObGeometry &geo2);

  static int pack_geo_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const ObString &str);

};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_GEO_EXPR_UTILS_H_