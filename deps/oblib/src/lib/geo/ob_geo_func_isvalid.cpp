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
 * This file contains implementation for ob_geo_func_isvalid.
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_isvalid.h"
#include "lib/geo/ob_geo_utils.h"

using namespace oceanbase::common;
namespace bg = boost::geometry;

namespace oceanbase
{
namespace common
{

template <typename GeoType>
static int eval_isvalid_without_strategy(const ObGeometry *g,
                                         bool &result)
{
  INIT_SUCC(ret);
  const GeoType *geo_condidate = reinterpret_cast<GeoType *>(const_cast<char *>(g->val()));
  result = bg::is_valid(*geo_condidate);
  return ret;
}

template <typename GeoType>
static int eval_isvalid_with_strategy(const ObGeometry *g,
                                      const ObGeoEvalCtx &context,
                                      bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geography srs is null", K(ret), K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::intersection::geographic_segments<> m_geographic_ll_la_aa_strategy(geog_sphere);
    const GeoType *geo_condidate = reinterpret_cast<const GeoType *>(const_cast<char *>(g->val()));
    result = bg::is_valid(*geo_condidate, m_geographic_ll_la_aa_strategy);
  }
  return ret;
}

class ObGeoFuncIsValidImpl : public ObIGeoDispatcher<bool, ObGeoFuncIsValidImpl>
{
public:
  ObGeoFuncIsValidImpl();
  virtual ~ObGeoFuncIsValidImpl() = default;
  // default templates
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // template for unary
  // default cases for cartesian
  template <typename GeoType>
  struct Eval
  {
    static int eval(const ObGeometry *g, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSED(context);
      return eval_isvalid_without_strategy<GeoType>(g, result);
    }
  };

private:
  template <typename CollectonType>
  static int eval_isvalid_geometry_collection(const ObGeometry *g,
                                              const ObGeoEvalCtx &context,
                                              bool &result)
  {
    INIT_SUCC(ret);
    result = true;
    common::ObIAllocator *allocator = context.get_allocator();
    bool is_geog = (g->crs() == oceanbase::common::ObGeoCRS::Geographic);
    typename CollectonType::iterator iter;
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else {
      const CollectonType *geo = reinterpret_cast<const CollectonType *>(const_cast<char *>(g->val()));
      iter = geo->begin();
      for (; iter != geo->end() && OB_SUCC(ret) && (result != false); iter++) {
        typename CollectonType::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo->get_sub_type(sub_ptr);
        ObGeometry *sub_g = NULL;
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, is_geog, true, sub_g))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g->set_data(wkb_nosrid);
          sub_g->set_srid(g->get_srid());
        }
        if (OB_SUCC(ret)) {
          ret = eval_wkb_unary(sub_g, context, result);
        }
      }
    }
    return ret;
  }
};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeomCollection, bool)
{
  UNUSED(context);
  return eval_isvalid_geometry_collection<ObWkbGeomCollection>(g, context, result);
} OB_GEO_FUNC_END;

// geography
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogPoint, bool)
{
  return eval_isvalid_with_strategy<ObWkbGeogPoint>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogLineString, bool)
{
  return eval_isvalid_with_strategy<ObWkbGeogLineString>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogPolygon, bool)
{
  return eval_isvalid_with_strategy<ObWkbGeogPolygon>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogCollection, bool)
{
  return eval_isvalid_geometry_collection<ObWkbGeogCollection>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogMultiPoint, bool)
{
  return eval_isvalid_with_strategy<ObWkbGeogMultiPoint>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogMultiLineString, bool)
{
  return eval_isvalid_with_strategy<ObWkbGeogMultiLineString>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncIsValidImpl, ObWkbGeogMultiPolygon, bool)
{
  return eval_isvalid_with_strategy<ObWkbGeogMultiPolygon>(g, context, result);
} OB_GEO_FUNC_END;

int ObGeoFuncIsValid::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncIsValidImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase