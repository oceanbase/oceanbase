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
 * This file contains implementation for ob_geo_func_testy.
 */

#include "lib/geo/ob_geo_dispatcher.h"
#include "ob_geo_func_testy.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
// unittest codes

constexpr int max_crs_type = 3;
constexpr int max_geo_type = 8;
int g_test_geo_unary_func_result[max_crs_type][max_geo_type];
int g_test_geo_binary_func_result[max_crs_type][max_geo_type][max_crs_type][max_geo_type];

#if 0
  ObIWkbGeomPoint cp;
  ObIWkbGeomLineString cl;
  ObIWkbGeomPolygon ca;
  ObIWkbGeomMultiPoint cmp;
  ObIWkbGeomMultiLineString cml;
  ObIWkbGeomMultiPolygon cma;
  ObIWkbGeomCollection cc;

  ObIWkbGeogPoint gp;
  ObIWkbGeogLineString gl;
  ObIWkbGeogPolygon ga;
  ObIWkbGeogMultiPoint gmp;
  ObIWkbGeogMultiLineString gml;
  ObIWkbGeogMultiPolygon gma;
  ObIWkbGeogCollection gc;
#endif

template<typename CHECK_TYPE>
bool check_geo_type(const ObGeometry *g) {
  bool bool_ret = false;
  switch(g->crs()) {
    case ObGeoCRS::Cartesian :{
      switch(g->type()) {
        case ObGeoType::POINT: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomPoint));
          break;
        }
        case ObGeoType::LINESTRING: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomLineString));
          break;
        }
        case ObGeoType::POLYGON: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomPolygon));
          break;
        }
        case ObGeoType::MULTIPOINT: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomMultiPoint));
          break;
        }
        case ObGeoType::MULTILINESTRING: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomMultiLineString));
          break;
        }
        case ObGeoType::MULTIPOLYGON: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomMultiPolygon));
          break;
        }
        case ObGeoType::GEOMETRYCOLLECTION: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeomCollection));
          break;
        }
        default:{
          break;
        }
      }
      break;
    }
    case ObGeoCRS::Geographic :{
      switch(g->type()) {
        case ObGeoType::POINT: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogPoint));
          break;
        }
        case ObGeoType::LINESTRING: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogLineString));
          break;
        }
        case ObGeoType::POLYGON: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogPolygon));
          break;
        }
        case ObGeoType::MULTIPOINT: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogMultiPoint));
          break;
        }
        case ObGeoType::MULTILINESTRING: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogMultiLineString));
          break;
        }
        case ObGeoType::MULTIPOLYGON: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogMultiPolygon));
          break;
        }
        case ObGeoType::GEOMETRYCOLLECTION: {
          bool_ret = (typeid(CHECK_TYPE) == typeid(ObWkbGeogCollection));
          break;
        }
        default:{
          break;
        }
      }
      break;
    }
    default:{
      break;
    }
  }
  return bool_ret;
}

#define OB_GEO_UNARY_TEST_FUNC_DEFINE(GEO_TYPE)                                              \
  template <>                                                                                \
  struct Eval<GEO_TYPE> {                                                                    \
    static int eval(const ObGeometry *g, const ObGeoEvalCtx &context, int &result) {         \
      UNUSED(context);                                                                       \
      if (check_geo_type<GEO_TYPE>(g) == false) {                                            \
        return OB_INVALID_ARGUMENT;                                                          \
      }                                                                                      \
      result = g_test_geo_unary_func_result[static_cast<int>(g->crs())][static_cast<int>(g->type())]; \
      return OB_SUCCESS;                                                                              \
    }                                                                                                 \
  }

#define OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(GEO_TYPE1, GEO_TYPE2)                           \
template <>                                                                                 \
  struct EvalWkbBi<GEO_TYPE1, GEO_TYPE2> {                                                 \
    static int eval(const ObGeometry *g1, const ObGeometry *g2,                             \
                    const ObGeoEvalCtx &context, int &result)                               \
    {                                                                                       \
      UNUSED(context);                                                                      \
      if (check_geo_type<GEO_TYPE1>(g1) == false) {                                         \
        return OB_INVALID_ARGUMENT;                                                         \
      }                                                                                     \
      if (check_geo_type<GEO_TYPE2>(g2) == false) {                                         \
        return OB_INVALID_ARGUMENT;                                                         \
      }                                                                                     \
      result = g_test_geo_binary_func_result[static_cast<int>(g1->crs())][static_cast<int>(g1->type())][static_cast<int>(g2->crs())][static_cast<int>(g2->type())]; \
      return OB_SUCCESS;                                                                    \
    }                                                                                       \
  }


#define OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(GEO_TYPE1, GEO_TYPE2)                           \
template <>                                                                                 \
  struct EvalWkbBiGeog<GEO_TYPE1, GEO_TYPE2> {                                       \
    static int eval(const ObGeometry *g1, const ObGeometry *g2,                             \
                    const ObGeoEvalCtx &context, int &result)                               \
    {                                                                                       \
      UNUSED(context);                                                                      \
      if (check_geo_type<GEO_TYPE1>(g1) == false) {                                         \
        return OB_INVALID_ARGUMENT;                                                         \
      }                                                                                     \
      if (check_geo_type<GEO_TYPE2>(g2) == false) {                                         \
        return OB_INVALID_ARGUMENT;                                                         \
      }                                                                                     \
      result = g_test_geo_binary_func_result[static_cast<int>(g1->crs())][static_cast<int>(g1->type())][static_cast<int>(g2->crs())][static_cast<int>(g2->type())]; \
      return OB_SUCCESS;                                                                    \
    }                                                                                       \
  }

int ob_test_init_geo_dispatcher_result() {
  for (int crs_type = 1; crs_type < static_cast<int>(ObGeoCRS::Geographic) + 1; crs_type++) {
    for (int g1_type = 0; g1_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g1_type++) {
      g_test_geo_unary_func_result[crs_type][g1_type] = (crs_type << 8) + g1_type;
    }
  }

  for (int crs_type = 1; crs_type < static_cast<int>(ObGeoCRS::Geographic) + 1; crs_type++) {
    for (int g1_type = 0; g1_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g1_type++) {
      for (int g2_type = 0; g2_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g2_type++) {
        g_test_geo_binary_func_result[crs_type][g1_type][crs_type][g2_type] = (crs_type << 8) + g1_type + g2_type;
      }
    }
  }
  return OB_SUCCESS;
}

int ob_test_init_geo_dispatcher_ret = ob_test_init_geo_dispatcher_result();

class ObGeoFuncMockyImpl : public ObIGeoDispatcher<int, ObGeoFuncMockyImpl>
{
public:
  ObGeoFuncMockyImpl();
  virtual ~ObGeoFuncMockyImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(int, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(int, OB_ERR_GIS_INVALID_DATA);

  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomPoint);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomLineString);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeomCollection);

  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogPoint);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogLineString);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon);
  OB_GEO_UNARY_TEST_FUNC_DEFINE(ObWkbGeogCollection);


  // template for binary
  OB_GEO_CART_BINARY_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);


  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPoint, ObWkbGeomCollection);

  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomLineString, ObWkbGeomCollection);

  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomPolygon, ObWkbGeomCollection);

  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPoint, ObWkbGeomCollection);

  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiLineString, ObWkbGeomCollection);

  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomMultiPolygon, ObWkbGeomCollection);

  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomMultiPoint);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomMultiLineString);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomMultiPolygon);
  OB_GEO_CART_BINARY_TEST_FUNC_DEFINE(ObWkbGeomCollection, ObWkbGeomCollection);


  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPoint, ObWkbGeogCollection);

  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogLineString, ObWkbGeogCollection);

  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogPolygon, ObWkbGeogCollection);

  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPoint, ObWkbGeogCollection);

  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiLineString, ObWkbGeogCollection);

  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogMultiPolygon, ObWkbGeogCollection);

  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogMultiPoint);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogMultiLineString);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogMultiPolygon);
  OB_GEO_GEOG_BINARY_TEST_FUNC_DEFINE(ObWkbGeogCollection, ObWkbGeogCollection);

  OB_GEO_CART_TREE_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
};

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncMockY::eval(const ObGeoEvalCtx &gis_context, int &result)
{
  return ObGeoFuncMockyImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase
