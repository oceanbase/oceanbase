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

namespace common
{
class ObCachedGeom;
}
namespace sql
{
struct ObGeoUnit
{
  const char *name;
  double factor;
};

const ObGeoUnit OB_GEO_UNITS[] = {
  // order by unit s, asc
  { "British chain (Benoit 1895 A)", 20.1167824 },
  { "British chain (Benoit 1895 B)", 20.116782494375872 },
  { "British chain (Sears 1922 truncated)", 20.116756 },
  { "British chain (Sears 1922)", 20.116765121552632 },
  { "British foot (1865)", 0.30480083333333335 },
  { "British foot (1936)", 0.3048007491 },
  { "British foot (Benoit 1895 A)", 0.3047997333333333 },
  { "British foot (Benoit 1895 B)", 0.30479973476327077 },
  { "British foot (Sears 1922 truncated)", 0.30479933333333337 },
  { "British foot (Sears 1922)", 0.3047994715386762 },
  { "British link (Benoit 1895 A)", 0.201167824 },
  { "British link (Benoit 1895 B)", 0.2011678249437587 },
  { "British link (Sears 1922 truncated)", 0.20116756 },
  { "British link (Sears 1922)", 0.2011676512155263 },
  { "British yard (Benoit 1895 A)", 0.9143992 },
  { "British yard (Benoit 1895 B)", 0.9143992042898124 },
  { "British yard (Sears 1922 truncated)", 0.914398 },
  { "British yard (Sears 1922)", 0.9143984146160288 },
  { "centimetre", 0.01 },
  { "chain", 20.1168 },
  { "Clarke's chain", 20.1166195164 },
  { "Clarke's foot", 0.3047972654 },
  { "Clarke's link", 0.201166195164 },
  { "Clarke's yard", 0.9143917962 },
  { "cm", 0.01 },
  { "fathom", 1.8288 },
  { "foot", 0.3048 },
  { "German legal metre", 1.0000135965 },
  { "Gold Coast foot", 0.3047997101815088 },
  { "Indian foot", 0.30479951024814694 },
  { "Indian foot (1937)", 0.30479841 },
  { "Indian foot (1962)", 0.3047996 },
  { "Indian foot (1975)", 0.3047995 },
  { "Indian yard", 0.9143985307444408 },
  { "Indian yard (1937)", 0.91439523 },
  { "Indian yard (1962)", 0.9143988 },
  { "Indian yard (1975)", 0.9143985 },
  { "kilometre", 1000 },
  { "km", 1000 },
  { "link", 0.201168 },
  { "metre", 1 },
  { "millimetre", 0.001 },
  { "nautical mile", 1852 },
  { "Statute mile", 1609.344 },
  { "US survey chain", 20.11684023368047 },
  { "US survey foot", 0.30480060960121924 },
  { "US survey link", 0.2011684023368047 },
  { "US survey mile", 1609.3472186944375 },
  { "yard", 0.9144 }
};

class ObGeoConstParamCache;
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
                            uint8_t build_flag = ObGeoBuildFlag::GEO_DEFAULT);
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
  static int geo_to_2d_wkb(common::ObGeometry &geo,
                           const ObExpr &expr,
                           ObEvalCtx &ctx,
                           const common::ObSrsItem *srs_item,
                           common::ObString &res_wkb,
                           uint32_t srs_id = 0);
  static int geo_to_3d_wkb(common::ObGeometry &geo,
                           const ObExpr &expr,
                           ObEvalCtx &ctx,
                           const common::ObSrsItem *srs_item,
                           common::ObString &res_wkb,
                           uint32_t srs_id = 0);
  static void geo_func_error_handle(int ret, const char* func_name);
  static int zoom_in_geos_for_relation(common::ObGeometry &geo1, common::ObGeometry &geo2,
                                       bool is_geo1_cached = false, bool is_geo2_cached = false);

  static int pack_geo_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const ObString &str);
  static int reverse_coordinate(ObGeometry *geo, const char *func_name);
  static int length_unit_conversion(const ObString &unit_str, const ObSrsItem *srs, double in_num, double &out_num);
  static int get_input_geometry(ObIAllocator &allocator, ObDatum *gis_datum, ObEvalCtx &ctx, ObExpr *gis_arg,
    omt::ObSrsCacheGuard &srs_guard, const char *func_name,
    const ObSrsItem *&srs, ObGeometry *&geo);
  static int make_valid_polygon_inner(
    ObCartesianPolygon &poly, ObIAllocator &allocator, ObGeometry *&valid_poly);
  static int union_polygons(
    ObIAllocator &allocator, const ObGeometry &poly, ObGeometry *&polygons_union);
  static int make_valid_polygon(ObGeometry *poly, ObIAllocator &allocator, ObGeometry *&valid_poly);
  static int create_3D_empty_collection(ObIAllocator &allocator, uint32_t srid, bool is_3d, bool is_geog, ObGeometry *&geo);
  static ObGeoConstParamCache* get_geo_constParam_cache(const uint64_t& id, ObExecContext *exec_ctx);
  static void expr_get_const_param_cache(ObGeoConstParamCache* const_param_cache, ObGeometry *&geo, uint32_t& srid, bool& is_geo_cached, int cache_idx);
  static int expr_prepare_build_geometry(ObIAllocator &allocator, const ObDatum &datum, const ObExpr &gis_arg, ObString& wkb, ObGeoType& type, uint32_t& srid);
  static int check_box_intersects(ObGeometry &geo1, ObGeometry &geo2, ObIAllocator &allocator,
                                   ObGeoConstParamCache* const_param_cache,
                                   bool is_geo1_cached, bool is_geo2_cached, bool& box_intersects);
  static int get_intersects_res(ObGeometry &geo1, ObGeometry &geo2,
                                ObExpr *gis_arg1, ObExpr *gis_arg2,
                                ObGeoConstParamCache* const_param_cache,
                                const ObSrsItem *srs,
                                ObArenaAllocator& temp_allocator, bool& res);
private:
  static int ob_geo_find_unit(const ObGeoUnit *units, const ObString &name, double &factor);
  static int init_box_by_geo(ObGeometry &geo, ObIAllocator &allocator, ObGeogBox *&box_ptr);
  static void init_boxes_by_cache(ObGeogBox *&box_ptr1, ObGeogBox& box1,
                                  ObGeogBox *&box_ptr2, ObGeogBox& box2,
                                  ObGeoConstParamCache* const_param_cache,
                                  bool is_geo1_cached, bool is_geo2_cached);
  static void init_box_by_cache(ObGeogBox *&box_ptr, ObGeogBox& box, ObCachedGeom* cache);
};

class ObGeoConstParamCache : public ObExprOperatorCtx {

public:
  ObGeoConstParamCache(common::ObIAllocator *allocator) :
        ObExprOperatorCtx(),
        allocator_(allocator),
        param1_(nullptr),
        cached_param1_(nullptr),
        param2_(nullptr),
        cached_param2_(nullptr) {}
  ~ObGeoConstParamCache();

  ObGeometry *get_const_param_cache(int arg_idx);
  ObCachedGeom *get_cached_geo(int arg_idx);
  int add_const_param_cache(int arg_idx, const common::ObGeometry &cache);
  void add_cached_geo(int arg_idx, common::ObCachedGeom *cache);
  void set_allocator(common::ObIAllocator *allocator);
  common::ObIAllocator* get_allocator() { return allocator_; }

private:
  common::ObIAllocator *allocator_;
  common::ObGeometry * param1_;          // ObGeometry * param1_ 与 ObCachedGeom *cached_param1_的区别
  common::ObCachedGeom *cached_param1_;
  common::ObGeometry * param2_;
  common::ObCachedGeom *cached_param2_;
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_GEO_EXPR_UTILS_H_