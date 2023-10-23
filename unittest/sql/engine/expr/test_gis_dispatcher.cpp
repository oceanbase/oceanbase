/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#include <sys/time.h>
#include "lib/geo/ob_geo_func_register.h"
#include "ob_geo_func_testx.h"
#include "ob_geo_func_testy.h"
#include "lib/json_type/ob_json_common.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

class TestGisDispatcher : public ::testing::Test {
public:
  TestGisDispatcher()
  {}
  ~TestGisDispatcher()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
  static void SetUpTestCase()
  {}
  static void TearDownTestCase()
  {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestGisDispatcher);
};

// X will call functor Y
enum class ObGeoFuncTestType
{
  NotImplemented = 0,
  TypeX = 1,
  TypeY = 2
};

template <ObGeoFuncTestType func_type>
struct ObGisTestFunc {
  typedef ObGeoFuncNotImplemented geo_func;
};

template <>
struct ObGisTestFunc<ObGeoFuncTestType::TypeX> {
  typedef ObGeoFuncTypeX geo_func;
};

template <>
struct ObGisTestFunc<ObGeoFuncTestType::TypeY> {
  typedef ObGeoFuncMockY geo_func;
};

TEST_F(TestGisDispatcher, not_impl)
{
  ObIWkbGeomPoint cp;
  ObIWkbGeogPoint gp;
  int ret = OB_SUCCESS;
  int result;
  ObGeoEvalCtx gis_context;
  ret = ObGisTestFunc<ObGeoFuncTestType::NotImplemented>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_ERR_GIS_INVALID_DATA);

  ret = OB_SUCCESS;
  ret = ObGeoFunc<ObGeoFuncType::NotImplemented>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_ERR_GIS_INVALID_DATA);
}

TEST_F(TestGisDispatcher, dispatcher)
{
  ObString dumy_wkb(4, 4, "dumy");
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

  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem srs(NULL);
  ObGeoEvalCtx gis_context(&allocator, &srs);
  ObGeometry *gis_args[3][8] = {{0}};

  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::POINT)] = &cp;
  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::LINESTRING)] = &cl;
  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::POLYGON)] = &ca;
  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::MULTIPOINT)] = &cmp;
  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::MULTILINESTRING)] = &cml;
  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::MULTIPOLYGON)] = &cma;
  gis_args[static_cast<int>(ObGeoCRS::Cartesian)][static_cast<int>(ObGeoType::GEOMETRYCOLLECTION)] = &cc;

  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::POINT)] = &gp;
  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::LINESTRING)] = &gl;
  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::POLYGON)] = &ga;
  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::MULTIPOINT)] = &gmp;
  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::MULTILINESTRING)] = &gml;
  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::MULTIPOLYGON)] = &gma;
  gis_args[static_cast<int>(ObGeoCRS::Geographic)][static_cast<int>(ObGeoType::GEOMETRYCOLLECTION)] = &gc;

  int result = 0;
  int ret = OB_SUCCESS;

  for (int crs_type = 1; crs_type < static_cast<int>(ObGeoCRS::Geographic) + 1; crs_type++) {
    for (int g1_type = 1; g1_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g1_type++) {
      gis_args[crs_type][g1_type]->set_data(dumy_wkb);
    }
  }

  for (int crs_type = 1; crs_type < static_cast<int>(ObGeoCRS::Geographic) + 1; crs_type++) {
    for (int g1_type = 1; g1_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g1_type++) {
      gis_context.ut_set_geo_count(1);
      gis_context.ut_set_geo_arg(0, gis_args[crs_type][g1_type]);
      ret = ObGisTestFunc<ObGeoFuncTestType::TypeY>::geo_func::eval(gis_context, result);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(result, g_test_geo_unary_func_result[crs_type][g1_type]);
    }
  }

  for (int crs_type = 1; crs_type < static_cast<int>(ObGeoCRS::Geographic) + 1; crs_type++) {
    for (int g1_type = 1; g1_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g1_type++) {
      for (int g2_type = 1; g2_type < static_cast<int>(ObGeoType::GEOMETRYCOLLECTION) + 1; g2_type++) {
        gis_context.ut_set_geo_count(2);
        gis_context.ut_set_geo_arg(0, gis_args[crs_type][g1_type]);
        gis_context.ut_set_geo_arg(1, gis_args[crs_type][g2_type]);
        ret = ObGisTestFunc<ObGeoFuncTestType::TypeY>::geo_func::eval(gis_context, result);
        ASSERT_EQ(ret, OB_SUCCESS);
        ASSERT_EQ(result, g_test_geo_binary_func_result[crs_type][g1_type][crs_type][g2_type]);
      }
    }
  }
}

TEST_F(TestGisDispatcher, specialization_and_exception_test)
{
  ObIWkbGeomPoint cp;
  ObIWkbGeomPolygon ca;
  ObIWkbGeogPoint gp;
  ObIWkbGeogLineString gl;
  ObIWkbGeogPolygon ga;
  ObIWkbGeogCollection gm;
  ObString dumy_wkb(4, 4, "dumy");
  cp.set_data(dumy_wkb);
  ca.set_data(dumy_wkb);
  gp.set_data(dumy_wkb);
  gl.set_data(dumy_wkb);
  ga.set_data(dumy_wkb);
  gm.set_data(dumy_wkb);

  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem srs(NULL);
  ObGeoEvalCtx gis_context(&allocator, &srs);
  ret = gis_context.append_geo_arg(&cp);
  ASSERT_EQ(ret, OB_SUCCESS);

  int result = 0;
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 1);

  gis_context.ut_set_geo_arg(0, &ca);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 3);

  gis_context.ut_set_geo_arg(0, &gp);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 1);

  gis_context.ut_set_geo_arg(0, &ga);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 3);

  gis_context.ut_set_geo_arg(0, &gm);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  ASSERT_EQ(result, 7);

  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &ca);
  gis_context.ut_set_geo_arg(1, &ca);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 6);

  gis_context.ut_set_geo_arg(0, &ga);
  gis_context.ut_set_geo_arg(1, &ga);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 800);

  gis_context.ut_set_geo_arg(0, &gm);
  gis_context.ut_set_geo_arg(1, &gm);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  ASSERT_EQ(result, 14);

  // test "functor" call "functor"
  ObIWkbGeogMultiPoint gmp;
  gmp.set_data(dumy_wkb);
  gis_context.ut_set_geo_arg(0, &gmp);
  gis_context.ut_set_geo_arg(1, &gmp);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 814);

  gis_context.ut_set_geo_arg(0, &gp);
  gis_context.ut_set_geo_arg(1, &gmp);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 100);

  gis_context.ut_set_geo_arg(0, &gmp);
  gis_context.ut_set_geo_arg(1, &gp);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, -100);

  gis_context.ut_set_geo_arg(0, &gp);
  gis_context.ut_set_geo_arg(1, &ga);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, -400);

  gis_context.ut_set_geo_arg(0, &ga);
  gis_context.ut_set_geo_arg(1, &gp);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(result, 400);

  // exception test
  gis_context.ut_set_geo_arg(0, &ga);
  gis_context.ut_set_geo_arg(1, &gl);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_ERR_BOOST_GEOMETRY_CENTROID_EXCEPTION);
  ASSERT_EQ(result, 777);

  gis_context.ut_set_geo_arg(0, &gl);
  gis_context.ut_set_geo_arg(1, &ga);
  ret = ObGisTestFunc<ObGeoFuncTestType::TypeX>::geo_func::eval(gis_context, result);
  ASSERT_EQ(ret, OB_ERR_BOOST_GEOMETRY_CENTROID_EXCEPTION);
  ASSERT_EQ(result, 666);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_gis_dispatcher.log");
  OB_LOGGER.set_file_name("test_json_bin.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}