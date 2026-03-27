/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_length.h"

using namespace oceanbase::common;
namespace bg = boost::geometry;

namespace oceanbase
{
namespace common
{

template<typename GeoType>
static int eval_length_without_strategy(const ObGeometry *g, double &result)
{
  INIT_SUCC(ret);
  const GeoType *geo_condidate = reinterpret_cast<const GeoType *>(const_cast<char *>(g->val()));
  if (OB_ISNULL(geo_condidate)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid null geometry", K(ret));
  } else {
    result = bg::length(*geo_condidate);
  }
  return ret;
}

template<typename GeoType>
static int eval_length_with_strategy(
    const ObGeometry *g, const ObGeoEvalCtx &context, double &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  const GeoType *geo_condidate = reinterpret_cast<const GeoType *>(const_cast<char *>(g->val()));
  if (OB_ISNULL(srs) || OB_ISNULL(geo_condidate)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geography srs or geometry is null", K(ret), K(srs), K(geo_condidate));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::distance::andoyer<bg::srs::spheroid<double>> m_geographic_ll_la_aa_strategy(
        geog_sphere);
    result = bg::length(*geo_condidate, m_geographic_ll_la_aa_strategy);
  }
  return ret;
}

class ObGeoFuncLengthImpl : public ObIGeoDispatcher<double, ObGeoFuncLengthImpl>
{
public:
  ObGeoFuncLengthImpl();
  virtual ~ObGeoFuncLengthImpl() = default;
  // default templates
  OB_GEO_UNARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncLengthImpl, ObWkbGeomLineString, double)
{
  UNUSED(context);
  return eval_length_without_strategy<ObWkbGeomLineString>(g, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncLengthImpl, ObWkbGeomMultiLineString, double)
{
  UNUSED(context);
  return eval_length_without_strategy<ObWkbGeomMultiLineString>(g, result);
}
OB_GEO_FUNC_END;

// geography
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncLengthImpl, ObWkbGeogLineString, double)
{
  return eval_length_with_strategy<ObWkbGeogLineString>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncLengthImpl, ObWkbGeogMultiLineString, double)
{
  return eval_length_with_strategy<ObWkbGeogMultiLineString>(g, context, result);
}
OB_GEO_FUNC_END;

int ObGeoFuncLength::eval(const ObGeoEvalCtx &gis_context, double &result)
{
  return ObGeoFuncLengthImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase