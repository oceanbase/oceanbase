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
 * This file contains implementation for ob_geo_func_envelope.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_envelope.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_errno.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

class ObGeoFuncEnvelopeImpl : public ObIGeoDispatcher<ObCartesianBox, ObGeoFuncEnvelopeImpl>
{
public:
  ObGeoFuncEnvelopeImpl();
  virtual ~ObGeoFuncEnvelopeImpl() = default;

  OB_GEO_UNARY_FUNC_DEFAULT(ObCartesianBox, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObCartesianBox, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObCartesianBox, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObCartesianBox, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObCartesianBox, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObCartesianBox, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

private:
  template <typename GeometyType>
  static int eval_envelop_without_strategy(const ObGeometry *g,
                                           const ObGeoEvalCtx &context,
                                           ObCartesianBox &result)
  {
    UNUSED(context);
    const GeometyType *geo = reinterpret_cast<const GeometyType *>(g->val());
    boost::geometry::envelope(*geo, result);
    return OB_SUCCESS;
  }


  static int eval_envelope_collection(const ObGeometry *g,
                                      const ObGeoEvalCtx &context,
                                      ObCartesianBox & result)
  {
    INIT_SUCC(ret);
    common::ObIAllocator *allocator = context.get_allocator();
    typename ObWkbGeomCollection::iterator iter;
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else if (g->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ObCartesianBox tmp_result;
      const ObWkbGeomCollection *geo = reinterpret_cast<const ObWkbGeomCollection *>(g->val());
      iter = geo->begin();
      // if result is true, the two geometries are intersects no need to process remaining sub geometries
      for (; iter != geo->end() && OB_SUCC(ret); iter++) {
        typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo->get_sub_type(sub_ptr);
        ObGeometry *sub_g = NULL;
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, false, true, sub_g))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g->set_data(wkb_nosrid);
          sub_g->set_srid(g->get_srid());
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(eval_envelope_collection(sub_g, context, tmp_result))) {
          LOG_WARN("failed to eval sub geo from collection", K(sub_type), K(ret));
        } else {
          if (result.is_empty() && !tmp_result.is_empty()) {
            result = tmp_result; // first non-empty result
          } else if (!tmp_result.is_empty()) {
            boost::geometry::expand(result, tmp_result); // other non-empty results
          } else { /* do nothing */ }
        }
      }
    } else {
      // none of the two geometries are collection type
      ret = eval_wkb_unary(g, context, result);
    }
    return ret;
  };
};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomPoint, ObCartesianBox)
{
  return eval_envelop_without_strategy<ObWkbGeomPoint>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomLineString, ObCartesianBox)
{
  return eval_envelop_without_strategy<ObWkbGeomLineString>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomPolygon, ObCartesianBox)
{
  return eval_envelop_without_strategy<ObWkbGeomPolygon>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomMultiPoint, ObCartesianBox)
{
  return eval_envelop_without_strategy<ObWkbGeomMultiPoint>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomMultiLineString, ObCartesianBox)
{
  return eval_envelop_without_strategy<ObWkbGeomMultiLineString>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomMultiPolygon, ObCartesianBox)
{
  return eval_envelop_without_strategy<ObWkbGeomMultiPolygon>(g, context, result);
} OB_GEO_FUNC_END;

// cartisain collection
// inputs: (const ObGeometry *g, const ObGeoEvalCtx &context, double &result)
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncEnvelopeImpl, ObWkbGeomCollection, ObCartesianBox)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomCollection *geo = reinterpret_cast<const ObWkbGeomCollection *>(g->val());
  return eval_envelope_collection(g, context, result);
} OB_GEO_FUNC_END;

int ObGeoFuncEnvelope::eval(const ObGeoEvalCtx &gis_context, ObCartesianBox &result)
{
  return ObGeoFuncEnvelopeImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase