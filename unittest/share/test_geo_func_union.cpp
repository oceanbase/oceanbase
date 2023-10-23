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
#include "lib/geo/ob_geo_func_union.h"
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

namespace oceanbase {

namespace common {

class TestGeoFuncUnion : public ::testing::Test {
public:
  TestGeoFuncUnion()
  {}
  ~TestGeoFuncUnion()
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
  DISALLOW_COPY_AND_ASSIGN(TestGeoFuncUnion);

};

int append_bo(ObJsonBuffer& data, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
    uint8_t sbo = static_cast<uint8_t>(bo);
    return data.append(reinterpret_cast<char*>(&sbo), sizeof(uint8_t));
}

int append_type(ObJsonBuffer& data, ObGeoType type, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
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

int append_uint32(ObJsonBuffer& data, uint32_t val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
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

int append_double(ObJsonBuffer& data, double val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
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

void create_line(ObJsonBuffer &data, std::vector< std::pair<double, double> > &value)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, value.size()));
    for (auto & p : value) {
      ASSERT_EQ(OB_SUCCESS, append_double(data, p.first));
      ASSERT_EQ(OB_SUCCESS, append_double(data, p.second));
    }
}

void create_polygon(ObJsonBuffer &data, int lnum, int pnum, std::vector< std::pair<double, double> > &value)
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

void create_multipoint(ObJsonBuffer &data, std::vector< std::pair<double, double> > &value)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, value.size()));
    for (auto & p : value) {
      create_point(data, p.first, p.second);
    }
}

void create_multiline(ObJsonBuffer &data, int lnum, int pnum, std::vector< std::pair<double, double> > &value)
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


TEST_F(TestGeoFuncUnion, point_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    create_point(data1, 3.2, 5.5);
    ObIWkbGeomPoint q;
    q.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &q);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 2);
    ASSERT_EQ(1.2, (*res)[0].get<0>());
    ASSERT_EQ(3.5, (*res)[0].get<1>());
    ASSERT_EQ(3.2, (*res)[1].get<0>());
    ASSERT_EQ(5.5, (*res)[1].get<1>());
}

TEST_F(TestGeoFuncUnion, point_point_overlap)
{
    point_geom_t p1_bg(3.2, 5.5);
    point_geom_t p2_bg(3.2, 5.5);
    mpoint_geom_t mp_res;
    bg::union_(p1_bg, p2_bg, mp_res);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 3.2, 5.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    create_point(data1, 3.2, 5.5);
    ObIWkbGeomPoint q;
    q.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &q);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), mp_res.size());
    int i = 0;
    for (auto & p : mp_res) {
      ASSERT_EQ(p.x(), (*res)[i].get<0>());
      ASSERT_EQ(p.y(), (*res)[i].get<1>());
      i++;
    }
}

TEST_F(TestGeoFuncUnion, point_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.0, 3.0);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.5, 1.2));
    val.push_back(std::make_pair(2.0, 4.0));
    create_line(data1, val);

    ObIWkbGeomLineString line;
    line.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 2);
    ASSERT_EQ((*res)[0].type(), ObGeoType::LINESTRING);
    ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
    ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[0]);
    ASSERT_EQ(1.5, (*c_l)[0].get<0>());
    ASSERT_EQ(1.2, (*c_l)[0].get<1>());
    ASSERT_EQ(2.0, (*c_l)[1].get<0>());
    ASSERT_EQ(4.0, (*c_l)[1].get<1>());
    ASSERT_EQ((*res)[1].type(), ObGeoType::POINT);
    ASSERT_EQ((*res)[1].crs(), ObGeoCRS::Cartesian);
    ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[1]);
    ASSERT_EQ(1.0, (*c_p).get<0>());
    ASSERT_EQ(3.0, (*c_p).get<1>());
}

TEST_F(TestGeoFuncUnion, point_line_overlap)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.0, 3.0);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0));
    val.push_back(std::make_pair(2.0, 6.0));
    create_line(data1, val);

    ObIWkbGeomLineString line;
    line.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    ASSERT_EQ((*res)[0].type(), ObGeoType::LINESTRING);
    ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
    ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[0]);
    ASSERT_EQ(0.0, (*c_l)[0].get<0>());
    ASSERT_EQ(0.0, (*c_l)[0].get<1>());
    ASSERT_EQ(2.0, (*c_l)[1].get<0>());
    ASSERT_EQ(6.0, (*c_l)[1].get<1>());
}

TEST_F(TestGeoFuncUnion, point_polygon)
{
    int i = 0;
    for (; i < 2; i++) {
      ObArenaAllocator allocator(ObModIds::TEST);
      ObJsonBuffer data(&allocator);
      create_point(data, 1.0, 3.0 + 2 * i);
      ObIWkbGeomPoint p;
      p.set_data(data.string());

      ObJsonBuffer data1(&allocator);
      std::vector< std::pair<double, double> > pol_val;
      pol_val.push_back(std::make_pair(0.0, 0.0));
      pol_val.push_back(std::make_pair(1.0, 0.0));
      pol_val.push_back(std::make_pair(1.0, 4.0));
      pol_val.push_back(std::make_pair(0.0, 4.0));
      pol_val.push_back(std::make_pair(0.0, 0.0));
      create_polygon(data1, 1, 5, pol_val);
      ObIWkbGeomPolygon poly;
      poly.set_data(data1.string());

      ObGeoEvalCtx gis_context(&allocator);
      gis_context.ut_set_geo_count(2);
      gis_context.ut_set_geo_arg(0, &p);
      gis_context.ut_set_geo_arg(1, &poly);
      ObGeometry *result = NULL;
      int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(result != NULL);
      ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
      ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

      ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
      ASSERT_EQ(res->empty(), false);
      {
        ASSERT_EQ((*res)[0].type(), ObGeoType::POLYGON);
        ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
        ObCartesianPolygon *c_p = static_cast<ObCartesianPolygon *>(&(*res)[0]);
        auto & ring = c_p->exterior_ring();
        ASSERT_EQ(ring.size(), 5);
        int i = 0;
        for (auto & p : ring) {
          ASSERT_EQ(p.get<0>(), pol_val[i].first);
          ASSERT_EQ(p.get<1>(), pol_val[i].second);
          i++;
        }
      }
      if (i > 0) {
        ASSERT_EQ(res->size(), 2);
        ASSERT_EQ((*res)[1].type(), ObGeoType::POINT);
        ASSERT_EQ((*res)[1].crs(), ObGeoCRS::Cartesian);
        ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[0]);
        ASSERT_EQ(p.x(), c_p->x());
        ASSERT_EQ(p.y(),  c_p->y());
      }
    }
}

TEST_F(TestGeoFuncUnion, point_multipoint)
{
    int i = 0;
    for (; i < 2; i++) {
      ObArenaAllocator allocator(ObModIds::TEST);
      ObJsonBuffer data(&allocator);
      create_point(data, 5.0, 0.0 + 2 * i);
      ObIWkbGeomPoint p;
      p.set_data(data.string());

      ObJsonBuffer data1(&allocator);
      std::vector< std::pair<double, double> > val;
      val.push_back(std::make_pair(5.0, 0.0));
      val.push_back(std::make_pair(15.0, 0.0));
      val.push_back(std::make_pair(15.0, 10.0));
      val.push_back(std::make_pair(5.0, 10.0));
      val.push_back(std::make_pair(5.0, 1.0));
      create_multipoint(data1, val);
      ObIWkbGeomMultiPoint multi_point;
      multi_point.set_data(data1.string());

      ObGeoEvalCtx gis_context(&allocator);
      gis_context.ut_set_geo_count(2);
      gis_context.ut_set_geo_arg(0, &p);
      gis_context.ut_set_geo_arg(1, &multi_point);
      ObGeometry *result = NULL;
      int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(result != NULL);
      ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
      ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

      ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
      ASSERT_EQ(res->empty(), false);
      ASSERT_EQ(res->size(), 5 + i);
      if ( i == 0) {
        int j = 0;
        for (auto & p : *res) {
          ASSERT_EQ(p.get<0>(), val[j].first);
          ASSERT_EQ(p.get<1>(), val[j].second);
          j++;
        }
      } else {
        ASSERT_EQ((*res)[0].get<0>(), p.x());
        ASSERT_EQ((*res)[0].get<1>(), p.y());
        int32_t j = 1;
        for (auto & po : val) {
          ASSERT_EQ((*res)[j].get<0>(), po.first);
          ASSERT_EQ((*res)[j].get<1>(), po.second);
          j++;
        }
      }
    }
}

TEST_F(TestGeoFuncUnion, point_multiline)
{
    int loop = 0;
    for (; loop < 2; loop++) {
      ObArenaAllocator allocator(ObModIds::TEST);
      ObJsonBuffer data(&allocator);
      create_point(data, 5.0, 0.0 + 2 * loop);
      ObIWkbGeomPoint p;
      p.set_data(data.string());

      ObJsonBuffer data1(&allocator);
      ASSERT_EQ(OB_SUCCESS, append_bo(data1));
      ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
      uint32_t line_num = 2;
      ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
      for (uint32_t i = 0; i < line_num; i++) {
        std::vector< std::pair<double, double> > val1;
        val1.push_back(std::make_pair(0.0 + i, 0.0 + i));
        val1.push_back(std::make_pair(5.0 + i, 0.0 + i));
        create_line(data1, val1);
      }
      ObIWkbGeomMultiLineString multi_line;
      multi_line.set_data(data1.string());

      ObGeoEvalCtx gis_context(&allocator);
      gis_context.ut_set_geo_count(2);
      gis_context.ut_set_geo_arg(0, &p);
      gis_context.ut_set_geo_arg(1, &multi_line);
      ObGeometry *result = NULL;
      int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(result != NULL);
      ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
      ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

      ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
      ASSERT_EQ(res->empty(), false);
      if (loop == 0) {
        ASSERT_EQ(res->size(), 2);
        ASSERT_EQ((*res)[0].type(), ObGeoType::LINESTRING);
        ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
        ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[0]);
        ASSERT_EQ((*c_l)[0].get<0>(), 0.0);
        ASSERT_EQ((*c_l)[0].get<1>(), 0.0);
        ASSERT_EQ((*c_l)[1].get<0>(), 5.0);
        ASSERT_EQ((*c_l)[1].get<1>(), 0.0);
        ASSERT_EQ((*res)[1].type(), ObGeoType::LINESTRING);
        ASSERT_EQ((*res)[1].crs(), ObGeoCRS::Cartesian);
        ObCartesianLineString *c_l1 = static_cast<ObCartesianLineString *>(&(*res)[1]);
        ASSERT_EQ((*c_l1)[0].get<0>(), 1.0);
        ASSERT_EQ((*c_l1)[0].get<1>(), 1.0);
        ASSERT_EQ((*c_l1)[1].get<0>(), 6.0);
        ASSERT_EQ((*c_l1)[1].get<1>(), 1.0);
      } else {
        ASSERT_EQ(res->size(), 3);
        ASSERT_EQ((*res)[0].type(), ObGeoType::LINESTRING);
        ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
        ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[0]);
        ASSERT_EQ((*c_l)[0].get<0>(), 0.0);
        ASSERT_EQ((*c_l)[0].get<1>(), 0.0);
        ASSERT_EQ((*c_l)[1].get<0>(), 5.0);
        ASSERT_EQ((*c_l)[1].get<1>(), 0.0);
        ASSERT_EQ((*res)[1].type(), ObGeoType::LINESTRING);
        ASSERT_EQ((*res)[1].crs(), ObGeoCRS::Cartesian);
        ObCartesianLineString *c_l1 = static_cast<ObCartesianLineString *>(&(*res)[1]);
        ASSERT_EQ((*c_l1)[0].get<0>(), 1.0);
        ASSERT_EQ((*c_l1)[0].get<1>(), 1.0);
        ASSERT_EQ((*c_l1)[1].get<0>(), 6.0);
        ASSERT_EQ((*c_l1)[1].get<1>(), 1.0);
        ASSERT_EQ((*res)[2].type(), ObGeoType::POINT);
        ASSERT_EQ((*res)[2].crs(), ObGeoCRS::Cartesian);
        ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[2]);
        ASSERT_EQ((*c_p).get<0>(), 5.0);
        ASSERT_EQ((*c_p).get<1>(), 2.0);
      }
    }
}

TEST_F(TestGeoFuncUnion, point_multipolygon)
{
    int loop = 0;
    for (; loop < 2; loop++) {
      ObArenaAllocator allocator(ObModIds::TEST);
      ObJsonBuffer data(&allocator);
      create_point(data, 1.0, 2.5 + 2 * loop);
      ObIWkbGeomPoint p;
      p.set_data(data.string());

      ObJsonBuffer data1(&allocator);
      ASSERT_EQ(OB_SUCCESS, append_bo(data1));
      ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
      uint32_t polygon_num = 2;
      ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
      for (uint32_t i = 0; i < polygon_num; i++) {
        std::vector< std::pair<double, double> > val;
        val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
        val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
        val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
        val.push_back(std::make_pair(0.0, 1.0 + 2 * i));
        val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
        create_polygon(data1, 1, 5, val);
      }
      ObIWkbGeomMultiPolygon multi_poly;
      multi_poly.set_data(data1.string());

      ObGeoEvalCtx gis_context(&allocator);
      gis_context.ut_set_geo_count(2);
      gis_context.ut_set_geo_arg(0, &p);
      gis_context.ut_set_geo_arg(1, &multi_poly);
      ObGeometry *result = NULL;
      int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(result != NULL);
      ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
      ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

      ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
      const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(multi_poly.val());
      ASSERT_EQ(res->empty(), false);
      if (loop == 0) {
        ASSERT_EQ(res->size(), 2);
        int i = 0;
        for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
          ASSERT_EQ((*res)[i].type(), ObGeoType::POLYGON);
          ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
          ObCartesianPolygon *c_p = static_cast<ObCartesianPolygon *>(&(*res)[i]);
          ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
          i++;
        }
      } else {
        ASSERT_EQ(res->size(), 3);
        int i = 0;
        for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
          ASSERT_EQ((*res)[i].type(), ObGeoType::POLYGON);
          ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
          ObCartesianPolygon *c_p = static_cast<ObCartesianPolygon *>(&(*res)[i]);
          ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
          i++;
        }
        ASSERT_EQ((*res)[2].type(), ObGeoType::POINT);
        ASSERT_EQ((*res)[2].crs(), ObGeoCRS::Cartesian);
        ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[2]);
        ASSERT_EQ((*c_p).get<0>(), 1.0);
        ASSERT_EQ((*c_p).get<1>(), 4.5);
      }
    }
}

TEST_F(TestGeoFuncUnion, line_line)
{
  line_geom_t line_bg1, line_bg2;
  mline_geom_t ml;
  bg::read_wkt("LINESTRING(1.0 2.0, 2.0 4.0)", line_bg1);
  bg::read_wkt("LINESTRING(1.5 3.0, 4.0 8.0)", line_bg2);
  bg::union_(line_bg1, line_bg2, ml);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 2.0));
  val1.push_back(std::make_pair(2.0, 4.0));
  create_line(data, val1);
  ObIWkbGeomLineString line;
  line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.5, 3.0));
  val.push_back(std::make_pair(4.0, 8.0));
  create_line(data1, val);

  ObIWkbGeomLineString line1;
  line1.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &line);
  gis_context.ut_set_geo_arg(1, &line1);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

  ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(true, is_geo_equal(ml, *res));
}

TEST_F(TestGeoFuncUnion, line_polygon)
{
  line_geom_t line_bg;
  bg::read_wkt("LINESTRING(1.0 4.0, 1.0 5.0)", line_bg);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 2.0));
  val1.push_back(std::make_pair(1.0, 5.0));
  create_line(data, val1);
  ObIWkbGeomLineString line;
  line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(0.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 0.0));
  create_polygon(data1, 1, 5, pol_val);
  ObIWkbGeomPolygon poly;
  poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &line);
  gis_context.ut_set_geo_arg(1, &poly);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 2);
  ASSERT_EQ((*res)[0].type(), ObGeoType::POLYGON);
  ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
  ObCartesianPolygon *c_pl = static_cast<ObCartesianPolygon *>(&(*res)[0]);
  const ObWkbGeomPolygon *geo2 = reinterpret_cast<const ObWkbGeomPolygon *>(poly.val());
  ASSERT_EQ(true, is_geo_equal(*geo2, *c_pl));
  ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[1]);
  ASSERT_EQ(true, is_geo_equal(line_bg, *c_l));
}

TEST_F(TestGeoFuncUnion, line_multipoint)
{
  mpoint_geom_t mp_bg{{{15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 2.0));
  val1.push_back(std::make_pair(1.0, 5.0));
  create_line(data, val1);
  ObIWkbGeomLineString line;
  line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 3.0));
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(15.0, 10.0));
  val.push_back(std::make_pair(5.0, 10.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data1, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &line);
  gis_context.ut_set_geo_arg(1, &multi_point);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 5);
  ASSERT_EQ((*res)[0].type(), ObGeoType::LINESTRING);
  ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
  ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[0]);
  const ObWkbGeomLineString *geo2 = reinterpret_cast<const ObWkbGeomLineString *>(line.val());
  ASSERT_EQ(true, is_geo_equal(*geo2, *c_l));
  for (int i = 0; i < 4; i++) {
    ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[i + 1]);
    ASSERT_EQ(true, is_geo_equal(mp_bg[i], *c_p));
  }
}

TEST_F(TestGeoFuncUnion, line_multiline)
{
  line_geom_t line_bg{{1.0, 0.0}, {2.0, 0.0}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}};
  mline_geom_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  mline_geom_t mls_res_bg;
  bg::union_(line_bg, mls_bg, mls_res_bg);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 0.0));
  val1.push_back(std::make_pair(2.0, 0.0));
  val1.push_back(std::make_pair(15.0, 0.0));
  val1.push_back(std::make_pair(15.0, 10.0));
  val1.push_back(std::make_pair(5.0, 10.0));
  val1.push_back(std::make_pair(5.0, 1.0));
  create_line(data, val1);
  ObIWkbGeomLineString line;
  line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &line);
  gis_context.ut_set_geo_arg(1, &multi_line);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(true, is_geo_equal(*res, mls_res_bg));
}

TEST_F(TestGeoFuncUnion, line_multipolygon)
{
  line_geom_t line_bg{{1.0, 0.0}, {2.0, 0.0}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}};
  mpolygon_geom_t mpol_bg;
  boost::geometry::read_wkt("MULTIPOLYGON(((0.0 0.0, 2.0 0.0, 2.0 1.0, 0.0 1.0, 0.0 0.0)), ((0.0 2.0, 2.0 2.0, 2.0 3.0, 0.0 3.0, 0.0 2.0)))", mpol_bg);
  mline_geom_t mline_bg;
  bg::difference(line_bg, mpol_bg, mline_bg);
  // std::cout << bg::dsv(mline_bg) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 0.0));
  val1.push_back(std::make_pair(2.0, 0.0));
  val1.push_back(std::make_pair(15.0, 0.0));
  val1.push_back(std::make_pair(15.0, 10.0));
  val1.push_back(std::make_pair(5.0, 10.0));
  val1.push_back(std::make_pair(5.0, 1.0));
  create_line(data, val1);
  ObIWkbGeomLineString line;
  line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data1, 1, 5, val);
  }

  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &line);
  gis_context.ut_set_geo_arg(1, &multi_poly);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 3);
  const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(multi_poly.val());
  int i = 0;
  for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
    ASSERT_EQ((*res)[i].type(), ObGeoType::POLYGON);
    ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
    ObCartesianPolygon *c_p = static_cast<ObCartesianPolygon *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
    i++;
  }

  ASSERT_EQ((*res)[i].type(), ObGeoType::LINESTRING);
  ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
  ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[i]);
  ASSERT_EQ(true, is_geo_equal(mline_bg[0], *c_l));
}

TEST_F(TestGeoFuncUnion, polygon_multipoint)
{
  mpoint_geom_t mp_bg{{{1.0, 5.0}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};

  ObArenaAllocator allocator(ObModIds::TEST);

  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(0.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 0.0));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeomPolygon poly;
  poly.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 3.0));
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(15.0, 10.0));
  val.push_back(std::make_pair(5.0, 10.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data1, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &poly);
  gis_context.ut_set_geo_arg(1, &multi_point);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 6);
  ASSERT_EQ((*res)[0].type(), ObGeoType::POLYGON);
  ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
  ObCartesianPolygon *c_pol = static_cast<ObCartesianPolygon *>(&(*res)[0]);
  const ObWkbGeomPolygon *geo2 = reinterpret_cast<const ObWkbGeomPolygon *>(poly.val());
  ASSERT_EQ(true, is_geo_equal(*geo2, *c_pol));
  for (int i = 0; i < 5; i++) {
    ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[i + 1]);
    ASSERT_EQ(true, is_geo_equal(mp_bg[i], *c_p));
  }
}

TEST_F(TestGeoFuncUnion, polygon_multiline)
{
  mline_geom_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  polygon_geom_t pol_bg;
  boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 4.0, 0.0 4.0, 0.0 0.0))", pol_bg);
  mline_geom_t mls_res;
  bg::difference(mls_bg, pol_bg, mls_res);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(0.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 0.0));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeomPolygon poly;
  poly.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &poly);
  gis_context.ut_set_geo_arg(1, &multi_line);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 3);
  ASSERT_EQ((*res)[0].type(), ObGeoType::POLYGON);
  ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Cartesian);
  ObCartesianPolygon *c_pol = static_cast<ObCartesianPolygon *>(&(*res)[0]);
  const ObWkbGeomPolygon *geo2 = reinterpret_cast<const ObWkbGeomPolygon *>(poly.val());
  ASSERT_EQ(true, is_geo_equal(*geo2, *c_pol));
  int i = 0;
  for (auto iter = mls_res.begin(); iter != mls_res.end(); iter++) {
    ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[i + 1]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_l));
    i++;
  }
}

TEST_F(TestGeoFuncUnion, polygon_multipolygon)
{
  polygon_geom_t pol_bg;
  boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 4.0, 0.0 4.0, 0.0 0.0))", pol_bg);
  mpolygon_geom_t mpol_bg;
  boost::geometry::read_wkt("MULTIPOLYGON(((0.0 0.0, 2.0 0.0, 2.0 1.0, 0.0 1.0, 0.0 0.0)), ((0.0 2.0, 2.0 2.0, 2.0 3.0, 0.0 3.0, 0.0 2.0)))", mpol_bg);
  mpolygon_geom_t mpol_res_bg;
  bg::union_(pol_bg, mpol_bg, mpol_res_bg);
  // std::cout << bg::dsv(mline_bg) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(0.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 0.0));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeomPolygon poly;
  poly.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data1, 1, 5, val);
  }
  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &poly);
  gis_context.ut_set_geo_arg(1, &multi_poly);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultipolygon *res = reinterpret_cast<ObCartesianMultipolygon *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(mpol_res_bg, *res));
  // std::cout << bg::dsv(*res) << std::endl;

  ObGeoToTreeVisitor poly_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, poly.do_visit(poly_visitor));
  ASSERT_EQ(poly_visitor.get_geometry()->type(), ObGeoType::POLYGON);
  ASSERT_EQ(poly_visitor.get_geometry()->crs(), ObGeoCRS::Cartesian);
  ObCartesianPolygon *poly_tree1 = static_cast<ObCartesianPolygon *>(poly_visitor.get_geometry());
  ObGeoToTreeVisitor poly_visitor1(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_poly.do_visit(poly_visitor1));
  ASSERT_EQ(poly_visitor1.get_geometry()->type(), ObGeoType::MULTIPOLYGON);
  ASSERT_EQ(poly_visitor1.get_geometry()->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultipolygon *poly_tree2 = static_cast<ObCartesianMultipolygon *>(poly_visitor1.get_geometry());

  ObGeoEvalCtx gis_tree_context(&allocator);
  gis_tree_context.ut_set_geo_count(2);
  gis_tree_context.ut_set_geo_arg(0, poly_tree1);
  gis_tree_context.ut_set_geo_arg(1, poly_tree2);
  ObGeometry *result_tree = NULL;
  ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_tree_context, result_tree);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result_tree != NULL);

  ObCartesianMultipolygon *res_tree = reinterpret_cast<ObCartesianMultipolygon *>(result_tree);
  ASSERT_EQ(res_tree->empty(), false);
  ASSERT_EQ(res_tree->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(*res_tree, mpol_res_bg));
}

TEST_F(TestGeoFuncUnion, multipoint_multipoint)
{
  mpoint_geom_t mp_bg1{{{1.0, 5.0}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};
  mpoint_geom_t mp_bg2{{{2.0, 5.0}, {15.0, 7.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 12.0}}};
  mpoint_geom_t mp_bg_res;
  bg::union_(mp_bg1, mp_bg2, mp_bg_res);
  // std::cout << bg::dsv(mp_bg_res) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(15.0, 10.0));
  val.push_back(std::make_pair(5.0, 10.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(2.0, 5.0));
  val1.push_back(std::make_pair(15.0, 7.0));
  val1.push_back(std::make_pair(15.0, 10.0));
  val1.push_back(std::make_pair(5.0, 10.0));
  val1.push_back(std::make_pair(5.0, 12.0));
  create_multipoint(data1, val1);
  ObIWkbGeomMultiPoint multi_point1;
  multi_point1.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_point);
  gis_context.ut_set_geo_arg(1, &multi_point1);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mp_bg_res.size());
  ASSERT_EQ(true, is_geo_equal(mp_bg_res, *res));
  // std::cout << bg::dsv(*res) << std::endl;
}

TEST_F(TestGeoFuncUnion, multipoint_multiline)
{
  mpoint_geom_t mp_bg{{{0.0, 0.5}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};
  mline_geom_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {20.0, 0.0}}};
  mpoint_geom_t mp_bg_res;
  bg::difference(mp_bg, mls_bg, mp_bg_res);
  // std::cout << bg::dsv(mp_bg_res) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(0.0, 0.5));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(15.0, 10.0));
  val.push_back(std::make_pair(5.0, 10.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(20.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_point);
  gis_context.ut_set_geo_arg(1, &multi_line);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 5);
  const ObWkbGeomMultiLineString *geo2 = reinterpret_cast<const ObWkbGeomMultiLineString *>(multi_line.val());
  int i = 0;
  for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
    ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_l));
    i++;
  }

  for (auto iter = mp_bg_res.begin(); iter != mp_bg_res.end(); iter++) {
    ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
    i++;
  }
}

TEST_F(TestGeoFuncUnion, multipoint_multipolygon)
{
  mpoint_geom_t mp_bg{{{1.0, 0.0}, {2.0, 0.0}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};
  mpolygon_geom_t mpol_bg;
  boost::geometry::read_wkt("MULTIPOLYGON(((0.0 0.0, 2.0 0.0, 2.0 1.0, 0.0 1.0, 0.0 0.0)), ((0.0 2.0, 2.0 2.0, 2.0 3.0, 0.0 3.0, 0.0 2.0)))", mpol_bg);
  mpoint_geom_t mp_bg_res;
  bg::difference(mp_bg, mpol_bg, mp_bg_res);
  // std::cout << bg::dsv(mline_bg) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 0.0));
  val1.push_back(std::make_pair(2.0, 0.0));
  val1.push_back(std::make_pair(15.0, 0.0));
  val1.push_back(std::make_pair(15.0, 10.0));
  val1.push_back(std::make_pair(5.0, 10.0));
  val1.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data, val1);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data1, 1, 5, val);
  }

  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_point);
  gis_context.ut_set_geo_arg(1, &multi_poly);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mp_bg_res.size() + polygon_num);
  const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(multi_poly.val());
  int i = 0;
  for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
    ASSERT_EQ((*res)[i].type(), ObGeoType::POLYGON);
    ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
    ObCartesianPolygon *c_p = static_cast<ObCartesianPolygon *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
    i++;
  }

  for (auto iter = mp_bg_res.begin(); iter != mp_bg_res.end(); iter++) {
    ASSERT_EQ((*res)[i].type(), ObGeoType::POINT);
    ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
    ObCartesianPoint *c_p = static_cast<ObCartesianPoint *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
    i++;
  }
}

TEST_F(TestGeoFuncUnion, multiline_multiline)
{
  mline_geom_t mls1_bg{{{1.0, 0.0}, {2.0, 0.0}, {15.0, 0.0}},
                        {{15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};
  mline_geom_t mls2_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  mline_geom_t mls_res_bg;
  bg::union_(mls1_bg, mls2_bg, mls_res_bg);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, line_num));
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 0.0));
  val1.push_back(std::make_pair(2.0, 0.0));
  val1.push_back(std::make_pair(15.0, 0.0));
  create_line(data, val1);
  std::vector< std::pair<double, double> > val4;
  val4.push_back(std::make_pair(15.0, 10.0));
  val4.push_back(std::make_pair(5.0, 10.0));
  val4.push_back(std::make_pair(5.0, 1.0));
  create_line(data, val4);
  ObIWkbGeomMultiLineString multi_line1;
  multi_line1.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeomMultiLineString multi_line2;
  multi_line2.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_line1);
  gis_context.ut_set_geo_arg(1, &multi_line2);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mls_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(*res, mls_res_bg));
}

TEST_F(TestGeoFuncUnion, multiline_multipolygon)
{
  mline_geom_t mls_bg{{{1.0, 0.0}, {2.0, 0.0}, {15.0, 0.0}},
                      {{15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};
  mpolygon_geom_t mpol_bg;
  boost::geometry::read_wkt("MULTIPOLYGON(((0.0 0.0, 2.0 0.0, 2.0 1.0, 0.0 1.0, 0.0 0.0)), ((0.0 2.0, 2.0 2.0, 2.0 3.0, 0.0 3.0, 0.0 2.0)))", mpol_bg);
  mline_geom_t mline_bg;
  bg::difference(mls_bg, mpol_bg, mline_bg);
  // std::cout << bg::dsv(mline_bg) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, line_num));
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 0.0));
  val1.push_back(std::make_pair(2.0, 0.0));
  val1.push_back(std::make_pair(15.0, 0.0));
  create_line(data, val1);
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(15.0, 10.0));
  val2.push_back(std::make_pair(5.0, 10.0));
  val2.push_back(std::make_pair(5.0, 1.0));
  create_line(data, val2);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data1, 1, 5, val);
  }

  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_line);
  gis_context.ut_set_geo_arg(1, &multi_poly);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianGeometrycollection *res = reinterpret_cast<ObCartesianGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), polygon_num + mline_bg.size());
  const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(multi_poly.val());
  int i = 0;
  for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
    ASSERT_EQ((*res)[i].type(), ObGeoType::POLYGON);
    ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
    ObCartesianPolygon *c_p = static_cast<ObCartesianPolygon *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
    i++;
  }

  for (auto iter = mline_bg.begin(); iter != mline_bg.end(); iter++) {
    ASSERT_EQ((*res)[i].type(), ObGeoType::LINESTRING);
    ASSERT_EQ((*res)[i].crs(), ObGeoCRS::Cartesian);
    ObCartesianLineString *c_l = static_cast<ObCartesianLineString *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_l));
    i++;
  }
}

TEST_F(TestGeoFuncUnion, multipolygon_multipolygon)
{
  mpolygon_geom_t mpol1_bg;
  boost::geometry::read_wkt(
        "MULTIPOLYGON(((0.0 0.0, 1.0 0.0, 1.0 4.0, 0.0 4.0, 0.0 0.0)), ((0.0 2.0, 1.0 2.0, 1.0 6.0, 0.0 6.0, 0.0 2.0)))", mpol1_bg);
  mpolygon_geom_t mpol2_bg;
  boost::geometry::read_wkt("MULTIPOLYGON(((0.0 0.0, 2.0 0.0, 2.0 1.0, 0.0 1.0, 0.0 0.0)), ((0.0 2.0, 2.0 2.0, 2.0 3.0, 0.0 3.0, 0.0 2.0)))", mpol2_bg);
  mpolygon_geom_t mpol_res_bg;
  bg::union_(mpol1_bg, mpol2_bg, mpol_res_bg);
  // std::cout << bg::dsv(mline_bg) << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > pol_val;
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    pol_val.push_back(std::make_pair(1.0, 0.0 + 2 * i));
    pol_val.push_back(std::make_pair(1.0, 4.0 + 2 * i));
    pol_val.push_back(std::make_pair(0.0, 4.0 + 2 * i));
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data, 1, 5, pol_val);
  }
  ObIWkbGeomMultiPolygon multi_poly1;
  multi_poly1.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data1, 1, 5, val);
  }
  ObIWkbGeomMultiPolygon multi_poly2;
  multi_poly2.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_poly1);
  gis_context.ut_set_geo_arg(1, &multi_poly2);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultipolygon *res = reinterpret_cast<ObCartesianMultipolygon *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(mpol_res_bg, *res));
  // std::cout << bg::dsv(*res) << std::endl;
}

int mock_get_tenant_srs_item(ObIAllocator &allocator, uint64_t srs_id, const ObSrsItem *&srs_item)
{
    int ret = OB_SUCCESS;
    ObGeographicRs rs;
    rs.rs_name.assign_ptr("ED50", strlen("ED50"));
    rs.datum_info.name.assign_ptr("European Datum 1950", strlen("European Datum 1950"));
    rs.datum_info.spheroid.name.assign_ptr("International 1924", strlen("International 1924"));
    rs.datum_info.spheroid.inverse_flattening = 297;
    rs.datum_info.spheroid.semi_major_axis = 6378388;
    rs.primem.longtitude = 0;
    rs.unit.conversion_factor = 0.017453292519943278;
    rs.axis.x.direction = ObAxisDirection::NORTH;
    rs.axis.y.direction = ObAxisDirection::EAST;
    rs.datum_info.towgs84.value[0] = -157.89;
    rs.datum_info.towgs84.value[1] = -17.16;
    rs.datum_info.towgs84.value[2] = -78.41;
    rs.datum_info.towgs84.value[3] = 2.118;
    rs.datum_info.towgs84.value[4] = 2.697;
    rs.datum_info.towgs84.value[5] = -1.434;
    rs.datum_info.towgs84.value[6] = -5.38;
    rs.authority.is_valid = false;

    ObSpatialReferenceSystemBase *srs_info;
    if (OB_FAIL(ObSpatialReferenceSystemBase::create_geographic_srs(&allocator, srs_id, &rs, srs_info))) {
        printf("faild to create geographical srs, ret=%d", ret);
    }
    ObSrsItem *tmp_srs_item = NULL;
    if (OB_ISNULL(tmp_srs_item = OB_NEWx(ObSrsItem, (&allocator), srs_info))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        printf("fail to alloc memory for srs item, ret=%d", ret);
    } else {
        srs_item = tmp_srs_item;
    }
    return ret;
}

// union LL strategy
TEST_F(TestGeoFuncUnion, line_line_geog)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObSrsItem *srs = NULL;
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
  line_geog_t line_bg1, line_bg2;
  mline_geog_t ml;
  bg::read_wkt("LINESTRING(1.0 2.0, 2.0 4.0)", line_bg1);
  bg::read_wkt("LINESTRING(1.5 3.0, 4.0 8.0)", line_bg2);
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObLlLaAaStrategy line_strategy(geog_sphere);
  bg::union_(line_bg1, line_bg2, ml, line_strategy);

  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val1;
  val1.push_back(std::make_pair(1.0, 2.0));
  val1.push_back(std::make_pair(2.0, 4.0));
  create_line(data, val1);
  ObIWkbGeogLineString line;
  line.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.5, 3.0));
  val.push_back(std::make_pair(4.0, 8.0));
  create_line(data1, val);

  ObIWkbGeogLineString line1;
  line1.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator, srs);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &line);
  gis_context.ut_set_geo_arg(1, &line1);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
  ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

  ObGeographMultilinestring *res = reinterpret_cast<ObGeographMultilinestring *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(true, is_geo_equal(ml, *res));
  std::cout << bg::dsv(*res) << std::endl;
}

// union LL strategy
TEST_F(TestGeoFuncUnion, polygon_multipolygon_geog)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObSrsItem *srs = NULL;
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
  polygon_geog_t pol_bg;
  boost::geometry::read_wkt(
        "POLYGON((1.0 0.5, 2.0 0.5, 2.0 4.0, 1.0 4.0, 1.0 0.5))", pol_bg);
  mpolygon_geog_t mpol_bg;
  boost::geometry::read_wkt("MULTIPOLYGON(((1.0 0.5, 3.0 0.5, 3.0 1.0, 1.0 1.0, 1.0 0.5)), ((1.0 2.5, 3.0 2.5, 3.0 3.0, 1.0 3.0, 1.0 2.5)))", mpol_bg);
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObLlLaAaStrategy line_strategy(geog_sphere);
  mpolygon_geog_t mpol_res_bg;
  bg::union_(pol_bg, mpol_bg, mpol_res_bg, line_strategy);
  std::cout << bg::dsv(mpol_res_bg) << std::endl;

  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(1.0, 0.5));
  pol_val.push_back(std::make_pair(2.0, 0.5));
  pol_val.push_back(std::make_pair(2.0, 4.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(1.0, 0.5));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeogPolygon poly;
  poly.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.0, 0.5 + 2 * i));
    val.push_back(std::make_pair(3.0, 0.5 + 2 * i));
    val.push_back(std::make_pair(3.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(1.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(1.0, 0.5 + 2 * i));
    create_polygon(data1, 1, 5, val);
  }
  ObIWkbGeogMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator, srs);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &poly);
  gis_context.ut_set_geo_arg(1, &multi_poly);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
  ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
  ObGeographMultipolygon *res = reinterpret_cast<ObGeographMultipolygon *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(mpol_res_bg, *res));
  // std::cout << bg::dsv(*res) << std::endl;

  ObGeoToTreeVisitor poly_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, poly.do_visit(poly_visitor));
  ASSERT_EQ(poly_visitor.get_geometry()->type(), ObGeoType::POLYGON);
  ASSERT_EQ(poly_visitor.get_geometry()->crs(), ObGeoCRS::Geographic);
  ObGeographPolygon *poly_tree1 = static_cast<ObGeographPolygon *>(poly_visitor.get_geometry());
  ObGeoToTreeVisitor poly_visitor1(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_poly.do_visit(poly_visitor1));
  ASSERT_EQ(poly_visitor1.get_geometry()->type(), ObGeoType::MULTIPOLYGON);
  ASSERT_EQ(poly_visitor1.get_geometry()->crs(), ObGeoCRS::Geographic);
  ObGeographMultipolygon *poly_tree2 = static_cast<ObGeographMultipolygon *>(poly_visitor1.get_geometry());

  ObGeoEvalCtx gis_tree_context(&allocator, srs);
  gis_tree_context.ut_set_geo_count(2);
  gis_tree_context.ut_set_geo_arg(0, poly_tree1);
  gis_tree_context.ut_set_geo_arg(1, poly_tree2);
  ObGeometry *result_tree = NULL;
  ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_tree_context, result_tree);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result_tree != NULL);

  ObGeographMultipolygon *res_tree = reinterpret_cast<ObGeographMultipolygon *>(result_tree);
  ASSERT_EQ(res_tree->empty(), false);
  ASSERT_EQ(res_tree->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(*res_tree, mpol_res_bg));

}

// difference PL strategy
TEST_F(TestGeoFuncUnion, multipoint_multiline_geog)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObSrsItem *srs = NULL;
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
  mpoint_geom_t mp_bg{{{0.0, 0.5}, {15.0, 0.0}, {15.0, 10.0}, {5.0, 10.0}, {5.0, 1.0}}};
  mline_geom_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {20.0, 0.0}}};
  mpoint_geom_t mp_bg_res;

  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObPlPaStrategy point_strategy(geog_sphere);
  bg::difference(mp_bg, mls_bg, mp_bg_res, point_strategy);
  std::cout << bg::dsv(mp_bg_res) << std::endl;

  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(0.0, 0.5));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(15.0, 10.0));
  val.push_back(std::make_pair(5.0, 10.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data, val);
  ObIWkbGeogMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(20.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeogMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator, srs);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &multi_point);
  gis_context.ut_set_geo_arg(1, &multi_line);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
  ObGeographGeometrycollection *res = reinterpret_cast<ObGeographGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), line_num + mp_bg_res.size());
  const ObWkbGeogMultiLineString *geo2 = reinterpret_cast<const ObWkbGeogMultiLineString *>(multi_line.val());
  int i = 0;
  for (auto iter = geo2->begin(); iter != geo2->end(); iter++) {
    ObGeographLineString *c_l = static_cast<ObGeographLineString *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_l));
    i++;
  }

  for (auto iter = mp_bg_res.begin(); iter != mp_bg_res.end(); iter++) {
    ObGeographPoint *c_p = static_cast<ObGeographPoint *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_p));
    i++;
  }
}

// difference LL strategy
TEST_F(TestGeoFuncUnion, polygon_multiline_geog)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObSrsItem *srs = NULL;
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

  mline_geog_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  polygon_geog_t pol_bg;
  boost::geometry::read_wkt(
        "POLYGON((1.0 1.0, 2.0 1.0, 2.0 4.0, 1.0 4.0, 1.0 1.0))", pol_bg);
  mline_geog_t mls_res;
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObLlLaAaStrategy line_strategy(geog_sphere);
  bg::difference(mls_bg, pol_bg, mls_res, line_strategy);
  std::cout << bg::dsv(mls_res) << std::endl;

  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(1.0, 1.0));
  pol_val.push_back(std::make_pair(2.0, 1.0));
  pol_val.push_back(std::make_pair(2.0, 4.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(1.0, 1.0));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeogPolygon poly;
  poly.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeogMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator, srs);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &poly);
  gis_context.ut_set_geo_arg(1, &multi_line);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
  ObGeographGeometrycollection *res = reinterpret_cast<ObGeographGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mls_res.size() + 1);
  ASSERT_EQ((*res)[0].type(), ObGeoType::POLYGON);
  ASSERT_EQ((*res)[0].crs(), ObGeoCRS::Geographic);
  ObGeographPolygon *c_pol = static_cast<ObGeographPolygon *>(&(*res)[0]);
  const ObWkbGeogPolygon *geo2 = reinterpret_cast<const ObWkbGeogPolygon *>(poly.val());
  ASSERT_EQ(true, is_geo_equal(*geo2, *c_pol));
  int i = 0;
  for (auto iter = mls_res.begin(); iter != mls_res.end(); iter++) {
    ObGeographLineString *c_l = static_cast<ObGeographLineString *>(&(*res)[i + 1]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_l));
    i++;
  }
}

// disjoint pl strategy
TEST_F(TestGeoFuncUnion, point_multiline_geog)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObSrsItem *srs = NULL;
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

  mline_geog_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  point_geog_t p_bg(1.0, 1.0);
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObPlPaStrategy point_strategy(geog_sphere);
  ASSERT_EQ(true, bg::disjoint(p_bg, mls_bg, point_strategy));

  ObJsonBuffer data(&allocator);
  create_point(data, 1.0, 1.0);
  ObIWkbGeogPoint p;
  p.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeogMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator, srs);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &p);
  gis_context.ut_set_geo_arg(1, &multi_line);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::GEOMETRYCOLLECTION);
  ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
  ObGeographGeometrycollection *res = reinterpret_cast<ObGeographGeometrycollection *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), 3);
  int i = 0;
  for (auto iter = mls_bg.begin(); iter != mls_bg.end(); iter++) {
    ObGeographLineString *c_l = static_cast<ObGeographLineString *>(&(*res)[i]);
    ASSERT_EQ(true, is_geo_equal(*iter, *c_l));
    i++;
  }
  ObGeographPoint *c_p = static_cast<ObGeographPoint *>(&(*res)[i]);
  ASSERT_EQ(true, is_geo_equal(p_bg, *c_p));
}

TEST_F(TestGeoFuncUnion, polygon_polygon)
{
  polygon_geom_t pol_bg1;
  boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 4.0, 0.0 4.0, 0.0 0.0))", pol_bg1);
  polygon_geom_t pol_bg2;
  boost::geometry::read_wkt("POLYGON((0.0 0.0, 2.0 0.0, 2.0 1.0, 0.0 1.0, 0.0 0.0))", pol_bg2);
  mpolygon_geom_t mpol_res_bg;
  bg::union_(pol_bg1, pol_bg2, mpol_res_bg);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > pol_val;
  pol_val.push_back(std::make_pair(0.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 0.0));
  pol_val.push_back(std::make_pair(1.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 4.0));
  pol_val.push_back(std::make_pair(0.0, 0.0));
  create_polygon(data, 1, 5, pol_val);
  ObIWkbGeomPolygon poly;
  poly.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  std::vector< std::pair<double, double> > pol_val1;
  pol_val1.push_back(std::make_pair(0.0, 0.0));
  pol_val1.push_back(std::make_pair(2.0, 0.0));
  pol_val1.push_back(std::make_pair(2.0, 1.0));
  pol_val1.push_back(std::make_pair(0.0, 1.0));
  pol_val1.push_back(std::make_pair(0.0, 0.0));
  create_polygon(data1, 1, 5, pol_val1);
  ObIWkbGeomPolygon poly1;
  poly1.set_data(data1.string());

  ObGeoEvalCtx gis_context(&allocator);
  gis_context.ut_set_geo_count(2);
  gis_context.ut_set_geo_arg(0, &poly);
  gis_context.ut_set_geo_arg(1, &poly1);
  ObGeometry *result = NULL;
  int ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
  ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
  ObCartesianMultipolygon *res = reinterpret_cast<ObCartesianMultipolygon *>(result);
  ASSERT_EQ(res->empty(), false);
  ASSERT_EQ(res->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(*res, mpol_res_bg));

  ObGeoToTreeVisitor poly_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, poly.do_visit(poly_visitor));
  ASSERT_EQ(poly_visitor.get_geometry()->type(), ObGeoType::POLYGON);
  ASSERT_EQ(poly_visitor.get_geometry()->crs(), ObGeoCRS::Cartesian);
  ObCartesianPolygon *poly_tree1 = static_cast<ObCartesianPolygon *>(poly_visitor.get_geometry());
  ObGeoToTreeVisitor poly_visitor1(&allocator);
  ASSERT_EQ(OB_SUCCESS, poly1.do_visit(poly_visitor1));
  ASSERT_EQ(poly_visitor1.get_geometry()->type(), ObGeoType::POLYGON);
  ASSERT_EQ(poly_visitor1.get_geometry()->crs(), ObGeoCRS::Cartesian);
  ObCartesianPolygon *poly_tree2 = static_cast<ObCartesianPolygon *>(poly_visitor1.get_geometry());

  ObGeoEvalCtx gis_tree_context(&allocator);
  gis_tree_context.ut_set_geo_count(2);
  gis_tree_context.ut_set_geo_arg(0, poly_tree1);
  gis_tree_context.ut_set_geo_arg(1, poly_tree2);
  ObGeometry *result_tree = NULL;
  ret = ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_tree_context, result_tree);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result_tree != NULL);

  ObCartesianMultipolygon *res_tree = reinterpret_cast<ObCartesianMultipolygon *>(result_tree);
  ASSERT_EQ(res_tree->empty(), false);
  ASSERT_EQ(res_tree->size(), mpol_res_bg.size());
  ASSERT_EQ(true, is_geo_equal(*res_tree, mpol_res_bg));
}

TEST_F(TestGeoFuncUnion, gc_union)
{
  mpolygon_geom_t mpol_bg;

  boost::geometry::read_wkt(
        "MULTIPOLYGON(((0.0 0.0, 1.0 0.0, 1.0 4.0, 0.0 4.0, 0.0 0.0)), ((0.0 2.0, 1.0 2.0, 1.0 6.0, 0.0 6.0, 0.0 2.0)))", mpol_bg);

  mline_geom_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  mpoint_geom_t mp_bg{{{1.0, 5.0}, {15.0, 0.0}, {0.5, 2.6}, {1.5, 0.0}, {5.0, 1.0}}};
  mpolygon_geom_t mpol_bg_tmp;
  mpolygon_geom_t *mpol_bg_res = &mpol_bg_tmp;
  mline_geom_t mls_bg_res;
  mpoint_geom_t mp_bg_res;
  for (auto & poly : mpol_bg) {
    mpolygon_geom_t tmp_res;
    bg::union_(poly, *mpol_bg_res, tmp_res);
    *mpol_bg_res = tmp_res;
  }

  bg::difference(mls_bg, *mpol_bg_res, mls_bg_res);
  mpoint_geom_t mp_bg_tmp;
  bg::difference(mp_bg, mls_bg_res, mp_bg_tmp);
  bg::difference(mp_bg_tmp, *mpol_bg_res, mp_bg_res);

  std::cout << "muli_poly: " << bg::dsv(*mpol_bg_res) << std::endl;
  std::cout << "muli_line: " << bg::dsv(mls_bg_res) << std::endl;
  std::cout << "muli_point: " << bg::dsv(mp_bg_res) << std::endl;


  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(0.5, 2.6));
  val.push_back(std::make_pair(1.5, 0.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObJsonBuffer data3(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data3));
  ASSERT_EQ(OB_SUCCESS, append_type(data3, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data3, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > pol_val;
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    pol_val.push_back(std::make_pair(1.0, 0.0 + 2 * i));
    pol_val.push_back(std::make_pair(1.0, 4.0 + 2 * i));
    pol_val.push_back(std::make_pair(0.0, 4.0 + 2 * i));
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data3, 1, 5, pol_val);
  }
  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data3.string());

  ObGeoToTreeVisitor point_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_point.do_visit(point_visitor));
  ObCartesianMultipoint *multi_point_tree = static_cast<ObCartesianMultipoint *>(point_visitor.get_geometry());
  ObGeoToTreeVisitor line_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_line.do_visit(line_visitor));
  ObCartesianMultilinestring *multi_line_tree = static_cast<ObCartesianMultilinestring *>(line_visitor.get_geometry());

  ObGeoToTreeVisitor poly_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_poly.do_visit(poly_visitor));
  ObCartesianMultipolygon *multi_poly_tree = static_cast<ObCartesianMultipolygon *>(poly_visitor.get_geometry());

  const ObSrsItem *srs = NULL;
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
  int ret = ObGeoFuncUtils::ob_geo_gc_union<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon>(allocator, *srs, multi_point_tree, multi_line_tree, multi_poly_tree);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_geo_equal(mp_bg_res, *multi_point_tree));
  ASSERT_EQ(true, is_geo_equal(mls_bg_res, *multi_line_tree));
  ASSERT_EQ(true, is_geo_equal(*mpol_bg_res, *multi_poly_tree));
}

TEST_F(TestGeoFuncUnion, remove_duplicate_geo)
{
  mpoint_geom_t mp_bg{{{1.0, 5.0}, {15.0, 0.0}, {0.5, 2.6}, {1.5, 0.0}, {5.0, 1.0}}};
  mline_geom_t mls_bg{{{0.0, 0.0}, {0.0, 1.0}, {2.0, 1.0}},
                       {{1.0, 0.0}, {2.0, 0.0}}};
  mpolygon_geom_t mpol_bg;
  boost::geometry::read_wkt(
        "MULTIPOLYGON(((0.0 0.0, 1.0 0.0, 1.0 4.0, 0.0 4.0, 0.0 0.0)), ((0.0 2.0, 1.0 2.0, 1.0 6.0, 0.0 6.0, 0.0 2.0)))", mpol_bg);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(0.5, 2.6));
  val.push_back(std::make_pair(1.5, 0.0));
  val.push_back(std::make_pair(5.0, 1.0));
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(0.5, 2.6));
  create_multipoint(data, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObGeoToTreeVisitor point_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_point.do_visit(point_visitor));
  ObGeometry *multi_point_tree = point_visitor.get_geometry();
  int ret = OB_SUCCESS;

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 3;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  std::vector< std::pair<double, double> > val4;
  val4.push_back(std::make_pair(1.0, 0.0));
  val4.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val4);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObGeoToTreeVisitor line_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_line.do_visit(line_visitor));
  ObGeometry *multi_line_tree = line_visitor.get_geometry();

  ObJsonBuffer data3(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data3));
  ASSERT_EQ(OB_SUCCESS, append_type(data3, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 3;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data3, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > pol_val;
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * (i % 2)));
    pol_val.push_back(std::make_pair(1.0, 0.0 + 2 * (i % 2)));
    pol_val.push_back(std::make_pair(1.0, 4.0 + 2 * (i % 2)));
    pol_val.push_back(std::make_pair(0.0, 4.0 + 2 * (i % 2)));
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * (i % 2)));
    create_polygon(data3, 1, 5, pol_val);
  }
  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data3.string());

  ObGeoToTreeVisitor poly_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_poly.do_visit(poly_visitor));
  ObGeometry *multi_poly_tree = poly_visitor.get_geometry();
}

TEST_F(TestGeoFuncUnion, gc_split)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector< std::pair<double, double> > val;
  val.push_back(std::make_pair(1.0, 5.0));
  val.push_back(std::make_pair(15.0, 0.0));
  val.push_back(std::make_pair(0.5, 2.6));
  val.push_back(std::make_pair(1.5, 0.0));
  val.push_back(std::make_pair(5.0, 1.0));
  create_multipoint(data, val);
  ObIWkbGeomMultiPoint multi_point;
  multi_point.set_data(data.string());

  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
  uint32_t line_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
  std::vector< std::pair<double, double> > val2;
  val2.push_back(std::make_pair(0.0, 0.0));
  val2.push_back(std::make_pair(0.0, 1.0));
  val2.push_back(std::make_pair(2.0, 1.0));
  create_line(data1, val2);
  std::vector< std::pair<double, double> > val3;
  val3.push_back(std::make_pair(1.0, 0.0));
  val3.push_back(std::make_pair(2.0, 0.0));
  create_line(data1, val3);
  ObIWkbGeomMultiLineString multi_line;
  multi_line.set_data(data1.string());

  ObJsonBuffer data3(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data3));
  ASSERT_EQ(OB_SUCCESS, append_type(data3, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data3, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > pol_val;
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    pol_val.push_back(std::make_pair(1.0, 0.0 + 2 * i));
    pol_val.push_back(std::make_pair(1.0, 4.0 + 2 * i));
    pol_val.push_back(std::make_pair(0.0, 4.0 + 2 * i));
    pol_val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    create_polygon(data3, 1, 5, pol_val);
  }
  ObIWkbGeomMultiPolygon multi_poly;
  multi_poly.set_data(data3.string());

  ObGeoToTreeVisitor point_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_point.do_visit(point_visitor));
  ObCartesianMultipoint *multi_point_tree = static_cast<ObCartesianMultipoint *>(point_visitor.get_geometry());

  ObGeoToTreeVisitor line_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_line.do_visit(line_visitor));
  ObCartesianMultilinestring *multi_line_tree = static_cast<ObCartesianMultilinestring *>(line_visitor.get_geometry());

  ObGeoToTreeVisitor poly_visitor(&allocator);
  ASSERT_EQ(OB_SUCCESS, multi_poly.do_visit(poly_visitor));
  ObCartesianMultipolygon *multi_poly_tree = static_cast<ObCartesianMultipolygon *>(poly_visitor.get_geometry());

  ObCartesianGeometrycollection gc(0, allocator);
  gc.push_back(*multi_point_tree);
  gc.push_back(*multi_line_tree);
  gc.push_back(*multi_poly_tree);

  ObCartesianMultipoint *res_multi_point = NULL;
  ObCartesianMultilinestring * res_multi_line = NULL;
  ObCartesianMultipolygon *res_multi_poly = NULL;

  int ret = ObGeoFuncUtils::ob_geo_gc_split(allocator, gc, res_multi_point, res_multi_line, res_multi_poly);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_geo_equal(*res_multi_point, *multi_point_tree));
  ASSERT_EQ(true, is_geo_equal(*res_multi_line, *multi_line_tree));
  ASSERT_EQ(true, is_geo_equal(*res_multi_poly, *multi_poly_tree));
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}