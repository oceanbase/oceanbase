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
 */

#include <gtest/gtest.h>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/foreach.hpp>
#define private public
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_tree_traits.h"
#include "lib/geo/ob_geo_bin_traits.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_func_box.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/random/ob_random.h"
#undef private

#include <sys/time.h>
#include <stdexcept>
#include <exception>
#include <typeinfo>

namespace bg = boost::geometry;
using namespace oceanbase::common;

namespace oceanbase
{

namespace common
{

class TestGeoFuncBox : public ::testing::Test
{
public:
  TestGeoFuncBox()
  {}
  ~TestGeoFuncBox()
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
  DISALLOW_COPY_AND_ASSIGN(TestGeoFuncBox);
};

int append_bo(ObJsonBuffer &data, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
  uint8_t sbo = static_cast<uint8_t>(bo);
  return data.append(reinterpret_cast<char *>(&sbo), sizeof(uint8_t));
}

int append_type(ObJsonBuffer &data, ObGeoType type, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
  INIT_SUCC(ret);
  uint32_t stype = static_cast<uint32_t>(type);
  if (OB_FAIL(data.reserve(sizeof(uint32_t)))) {
  } else {
    char *ptr = data.ptr() + data.length();
    ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, stype, bo);
    ret = data.set_length(data.length() + sizeof(uint32_t));
  }
  return ret;
}

int append_uint32(ObJsonBuffer &data, uint32_t val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
  INIT_SUCC(ret);
  if (OB_FAIL(data.reserve(sizeof(uint32_t)))) {
  } else {
    char *ptr = data.ptr() + data.length();
    ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, val, bo);
    ret = data.set_length(data.length() + sizeof(uint32_t));
  }
  return ret;
}

int append_double(ObJsonBuffer &data, double val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
  INIT_SUCC(ret);
  if (OB_FAIL(data.reserve(sizeof(double)))) {
  } else {
    char *ptr = data.ptr() + data.length();
    ObGeoWkbByteOrderUtil::write<double>(ptr, val, bo);
    ret = data.set_length(data.length() + sizeof(double));
  }
  return ret;
}

void create_point(ObJsonBuffer &data, double x, double y)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
  ASSERT_EQ(OB_SUCCESS, append_double(data, x));
  ASSERT_EQ(OB_SUCCESS, append_double(data, y));
}

void create_line(ObJsonBuffer &data, std::vector<std::pair<double, double> > &value)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, value.size()));
  for (auto &p : value) {
    ASSERT_EQ(OB_SUCCESS, append_double(data, p.first));
    ASSERT_EQ(OB_SUCCESS, append_double(data, p.second));
  }
}

void create_polygon(ObJsonBuffer &data, int lnum, int pnum, std::vector<std::pair<double, double> > &value)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
  int i = 0;
  for (int l = 0; l < lnum; l++) {
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
    for (int p = 0; p < pnum; p++) {
      ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].first));
      ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].second));
      i++;
    }
  }
}

void create_multipoint(ObJsonBuffer &data, std::vector<std::pair<double, double> > &value)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, value.size()));
  for (auto &p : value) {
    create_point(data, p.first, p.second);
  }
}

void create_multiline(ObJsonBuffer &data, int lnum, int pnum, std::vector<std::pair<double, double> > &value)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
  int i = 0;
  for (int l = 0; l < lnum; l++) {
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
    for (int p = 0; p < pnum; p++) {
      ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].first));
      ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].second));
      i++;
    }
  }
}

// anum: polygon num; rnum: ring num per polygon; pnum: point num per ring
void create_multipolygon(
    ObJsonBuffer &data, int anum, int rnum, int pnum, std::vector<std::pair<double, double> > &value)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, anum));
  int i = 0;
  for (int a = 0; a < anum; a++) {
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, rnum));
    for (int r = 0; r < rnum; r++) {
      ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
      for (int p = 0; p < pnum; p++) {
        ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].first));
        ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].second));
        i++;
      }
    }
  }
}

template<typename GEO1, typename GEO2>
bool is_geo_equal(GEO1 &geo1, GEO2 &geo2)
{
  std::stringstream left;
  std::stringstream right;
  left << bg::dsv(geo1);
  right << bg::dsv(geo2);
  if (left.str() == right.str()) {
    return true;
  }
  std::cout << left.str() << std::endl;
  std::cout << right.str() << std::endl;
  return false;
}

typedef bg::strategy::within::geographic_winding<ObWkbGeogPoint> ObPlPaStrategy;
typedef bg::strategy::intersection::geographic_segments<> ObLlLaAaStrategy;

typedef bg::model::d2::point_xy<double> point_geom_t;
typedef bg::model::multi_point<point_geom_t> mpoint_geom_t;
typedef bg::model::linestring<point_geom_t> line_geom_t;
typedef bg::model::multi_linestring<line_geom_t> mline_geom_t;
typedef bg::model::polygon<point_geom_t, false> polygon_geom_t;
typedef bg::model::multi_polygon<polygon_geom_t> mpolygon_geom_t;

typedef bg::model::d2::point_xy<double, bg::cs::geographic<bg::radian> > point_geog_t;
typedef bg::model::multi_point<point_geog_t> mpoint_geog_t;
typedef bg::model::linestring<point_geog_t> line_geog_t;
typedef bg::model::multi_linestring<line_geog_t> mline_geog_t;
typedef bg::model::polygon<point_geog_t, false> polygon_geog_t;
typedef bg::model::multi_polygon<polygon_geog_t> mpolygon_geog_t;

TEST_F(TestGeoFuncBox, geom_point)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  double x = 1.2;
  double y = 3.5;
  create_point(data, x, y);
  ObIWkbGeomPoint p;
  p.set_data(data.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &p);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(x, result->xmax);
  ASSERT_EQ(x, result->xmin);
  ASSERT_EQ(y, result->ymax);
  ASSERT_EQ(y, result->ymin);
}

TEST_F(TestGeoFuncBox, geom_linestring)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::pair<double, double> > val;
  val.push_back(std::make_pair(1.5, 1.2));
  val.push_back(std::make_pair(2.0, 4.0));
  create_line(data, val);
  ObIWkbGeomLineString line;
  line.set_data(data.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &line);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(2.0, result->xmax);
  ASSERT_EQ(1.5, result->xmin);
  ASSERT_EQ(4.0, result->ymax);
  ASSERT_EQ(1.2, result->ymin);
}

TEST_F(TestGeoFuncBox, geom_polygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(1, 1));
  pol_val.push_back(std::make_pair(1, 3));
  pol_val.push_back(std::make_pair(3, 3));
  pol_val.push_back(std::make_pair(3, 1));
  pol_val.push_back(std::make_pair(1, 1));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeomPolygon poly;
  poly.set_data(data.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &poly);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(1.0, result->xmin);
  ASSERT_EQ(3.0, result->xmax);
  ASSERT_EQ(1.0, result->ymin);
  ASSERT_EQ(3.0, result->ymax);
}

TEST_F(TestGeoFuncBox, geom_multipoint)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObJsonBuffer data1(&allocator);
  std::vector<std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 1.0));
  val.push_back(std::make_pair(1.0, 0.0));
  val.push_back(std::make_pair(1.0, 2.0));
  create_multipoint(data1, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &multi_point);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(1.0, result->xmin);
  ASSERT_EQ(1.0, result->xmax);
  ASSERT_EQ(0.0, result->ymin);
  ASSERT_EQ(2.0, result->ymax);
}

TEST_F(TestGeoFuncBox, geom_multilinestring)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObJsonBuffer data1(&allocator);
  std::vector<std::pair<double, double> > val;
  val.push_back(std::make_pair(0.0, 0.0));
  val.push_back(std::make_pair(1.0, 1.0));
  val.push_back(std::make_pair(1.0, 1.0));
  val.push_back(std::make_pair(2.0, 2.0));
  val.push_back(std::make_pair(2.0, 2.0));
  val.push_back(std::make_pair(3.0, 3.0));
  create_multiline(data1, 3, 2, val);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &multi_line);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(0.0, result->xmin);
  ASSERT_EQ(3.0, result->xmax);
  ASSERT_EQ(0.0, result->ymin);
  ASSERT_EQ(3.0, result->ymax);
}

TEST_F(TestGeoFuncBox, geom_multipolygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObJsonBuffer data1(&allocator);
  std::vector<std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 1.0));
  val.push_back(std::make_pair(1.0, 3.0));
  val.push_back(std::make_pair(3.0, 3.0));
  val.push_back(std::make_pair(3.0, 1.0));
  val.push_back(std::make_pair(1.0, 1.0));
  val.push_back(std::make_pair(4.0, 4.0));
  val.push_back(std::make_pair(4.0, 6.0));
  val.push_back(std::make_pair(6.0, 6.0));
  val.push_back(std::make_pair(6.0, 4.0));
  val.push_back(std::make_pair(4.0, 4.0));
  create_multipolygon(data1, 2, 1, 5, val);
  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &multi_poly);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(1.0, result->xmin);
  ASSERT_EQ(6.0, result->xmax);
  ASSERT_EQ(1.0, result->ymin);
  ASSERT_EQ(6.0, result->ymax);
}

TEST_F(TestGeoFuncBox, geom_collection)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 6));

  create_point(data, 1.2, 3.5);

  std::vector<std::pair<double, double> > line_val;
  line_val.push_back(std::make_pair(1.5, 1.2));
  line_val.push_back(std::make_pair(2.0, 4.0));
  create_line(data, line_val);

  std::vector<std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(1, 1));
  pol_val.push_back(std::make_pair(1, 3));
  pol_val.push_back(std::make_pair(3, 3));
  pol_val.push_back(std::make_pair(3, 1));
  pol_val.push_back(std::make_pair(1, 1));
  create_polygon(data, 1, 5, pol_val);

  std::vector<std::pair<double, double> > mpt_val;
  mpt_val.push_back(std::make_pair(1.0, 1.0));
  mpt_val.push_back(std::make_pair(1.0, 0.0));
  mpt_val.push_back(std::make_pair(1.0, 2.0));
  create_multipoint(data, mpt_val);

  std::vector<std::pair<double, double> > mpl_val;
  mpl_val.push_back(std::make_pair(0.0, 0.0));
  mpl_val.push_back(std::make_pair(1.0, 1.0));
  mpl_val.push_back(std::make_pair(1.0, 1.0));
  mpl_val.push_back(std::make_pair(2.0, 2.0));
  mpl_val.push_back(std::make_pair(2.0, 2.0));
  mpl_val.push_back(std::make_pair(3.0, 3.0));
  create_multiline(data, 3, 2, mpl_val);

  std::vector<std::pair<double, double> > mpy_val;
  mpy_val.push_back(std::make_pair(1.0, 1.0));
  mpy_val.push_back(std::make_pair(1.0, 3.0));
  mpy_val.push_back(std::make_pair(3.0, 3.0));
  mpy_val.push_back(std::make_pair(3.0, 1.0));
  mpy_val.push_back(std::make_pair(1.0, 1.0));
  mpy_val.push_back(std::make_pair(4.0, 4.0));
  mpy_val.push_back(std::make_pair(4.0, 6.0));
  mpy_val.push_back(std::make_pair(6.0, 6.0));
  mpy_val.push_back(std::make_pair(6.0, 4.0));
  mpy_val.push_back(std::make_pair(4.0, 4.0));
  create_multipolygon(data, 2, 1, 5, mpy_val);

  ObIWkbGeomCollection collection;
  collection.set_data(data.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(1);
  gis_context.ut_set_geo_arg(0, &collection);
  ObGeogBox *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(0.0, result->xmin);
  ASSERT_EQ(6.0, result->xmax);
  ASSERT_EQ(0.0, result->ymin);
  ASSERT_EQ(6.0, result->ymax);
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_geo_func_box.log");
  // OB_LOGGER.set_file_name("test_geo_func_box.log");
  // OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}