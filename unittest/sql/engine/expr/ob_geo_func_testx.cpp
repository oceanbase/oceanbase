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
 * This file contains implementation for geo_func_mockx.
 */

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_ibin.h"
#include "ob_geo_func_testx.h"
#include "ob_geo_func_testy.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
// unittest codes
class ObGeoFuncTypeXImpl : public ObIGeoDispatcher<int, ObGeoFuncTypeXImpl>
{
public:
  ObGeoFuncTypeXImpl();
  virtual ~ObGeoFuncTypeXImpl() = default;

  // template for unary
  template <typename GeometyType>
  struct Eval {
    static int eval(const ObGeometry *g, const ObGeoEvalCtx &context, int &result)
    {
      UNUSED(context);
      result = static_cast<int>(g->type());
      return OB_SUCCESS;
    }
  };

  template <typename GeometyType>
  struct EvalTree {
    static int eval(const ObGeometry *g, const ObGeoEvalCtx &context, int &result)
    {
      UNUSED(context);
      result = static_cast<int>(g->type());
      return OB_SUCCESS;
    }
  };

  // templates for binary
  template <typename GeometyType1, typename GeometyType2>
  struct EvalWkbBi {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
    {
      UNUSED(context);
      result = 0;
      result += static_cast<int>(g1->type());
      result += static_cast<int>(g2->type());
      return OB_SUCCESS;
    }
  };

  template <typename GeometyType1, typename GeometyType2>
  struct EvalWkbBiGeog {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
    {
      UNUSED(context);
      result = 0;
      result += static_cast<int>(g1->type());
      result += static_cast<int>(g2->type());
      return OB_SUCCESS;
    }
  };

  OB_GEO_CART_TREE_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // unary specialization for geographic geometrycollection
  template <>
  struct Eval<ObWkbGeogCollection> {
    static int eval(const ObGeometry *g, const ObGeoEvalCtx &context, int &result)
    {
      UNUSED(context);
      result = static_cast<int>(g->type());
      return OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
    }
  };

  // binary specialization for geographic geometrycollection and geographic geometrycollection
  template <>
  struct EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogCollection> {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
    {
      UNUSED(context);
      result = 0;
      result += static_cast<int>(g1->type());
      result += static_cast<int>(g2->type());
      return OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
    }
  };

  // binary specialization for geographic multipoint and geographic multipoint
  template <>
  struct EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint> {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
    {
      UNUSEDx(context, g1, g2);
      ObGeoEvalCtx gis_context(context.get_allocator(), context.get_srs());
      ObString dumy_wkb(4, 4, "dumy");
      ObIWkbGeogPoint gmp1;
      ObIWkbGeogPoint gmp2;
      gmp1.set_data(dumy_wkb);
      gmp2.set_data(dumy_wkb);
      gis_context.append_geo_arg(&gmp1);
      gis_context.append_geo_arg(&gmp2);
      return ObGeoFuncTypeX::eval(gis_context, result);
    }
  };

  // binary partial specialization for geographic point
  template <typename GeometyType2>
  struct EvalWkbBiGeog<ObWkbGeogPoint, GeometyType2> {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
    {
      UNUSEDx(context, g1, g2);
      result = 100;
      return OB_SUCCESS;
    }
  };

};

template <typename GeometyType1>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<GeometyType1, ObWkbGeogPoint> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    UNUSEDx(context, g1, g2);
    result = -100;
    return OB_SUCCESS;
  }
};

// binary specializations
template <>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogPoint> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    INIT_SUCC(ret);
    UNUSEDx(context, g1, g2);
    result = 300;
    int tmp_result;
    ret = ObGeoFuncMockY::eval(context, tmp_result);
    result += tmp_result;
    return OB_SUCCESS;
  }
};

template <>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogPoint> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    UNUSEDx(context, g1, g2);
    result = 400;
    return OB_SUCCESS;
  }
};

template <>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogPolygon> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    UNUSEDx(context, g1, g2);
    result = -400;
    return OB_SUCCESS;
  }
};

template <>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogPolygon> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    UNUSEDx(context, g1, g2);
    result = 800;
    return OB_SUCCESS;
  }
};

// binary partial specialization for geographic polygon
template<typename GeometryType2>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<ObWkbGeogPolygon, GeometryType2> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    UNUSEDx(context, g1, g2);
    result = 777;
    throw boost::geometry::centroid_exception();
    return OB_SUCCESS;
  }
};

template<typename GeometryType1>
struct ObGeoFuncTypeXImpl::EvalWkbBiGeog<GeometryType1, ObWkbGeogPolygon> {
  static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, int &result)
  {
    UNUSEDx(context, g1, g2);
    result = 666;
    throw boost::geometry::centroid_exception();
    return OB_SUCCESS;
  }
};

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncTypeX::eval(const ObGeoEvalCtx &gis_context, int &result)
{
  return ObGeoFuncTypeXImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase
