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
#include "lib/geo/ob_geo_tree_traits.h"
#include "lib/geo/ob_geo_bin_traits.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_func_difference.h"
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

class TestGeoFuncDifference : public ::testing::Test {
public:
  TestGeoFuncDifference()
  {}
  ~TestGeoFuncDifference()
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
  DISALLOW_COPY_AND_ASSIGN(TestGeoFuncDifference);

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

TEST_F(TestGeoFuncDifference, point_point)
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
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.4, 2.3));
    val.push_back(std::make_pair(3.5, 5.5));
    create_line(data1, val);
    ObIWkbGeomLineString line;
    line.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_polygon(data1, 1, 5, val);
    ObIWkbGeomPolygon poly;
    poly.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &poly);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_multipoint)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_multipoint(data1, val);
    ObIWkbGeomMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &multi_point);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_multiline)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(0.0, 0.0));
    create_multiline(data1, 2, 3, val);
    ObIWkbGeomMultiLineString multi_line;
    multi_line.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &multi_line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_multipolygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    int polynum = 2;
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polynum));
    for (int i = 0; i < polynum; i++) {
      std::vector< std::pair<double, double> > val;
      val.push_back(std::make_pair(5.0 + i, 0.0 + i));
      val.push_back(std::make_pair(15.0 + i, 0.0 + i));
      val.push_back(std::make_pair(15.0 + i, 10.0 + i));
      val.push_back(std::make_pair(5.0 + i, 10.0 + i));
      val.push_back(std::make_pair(5.0 + i, 0.0 + i));
      create_polygon(data1, 1, 5, val);
    }
    ObIWkbGeomMultiPolygon multi_polygon;
    multi_polygon.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &multi_polygon);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, line_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.4, 2.3));
    val.push_back(std::make_pair(3.5, 5.5));
    create_line(data, val);

    ObIWkbGeomLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    create_point(data1, 3.2, 5.5);
    ObIWkbGeomPoint q;
    q.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &q);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::LINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianLineString *res = reinterpret_cast<ObCartesianLineString *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 2);
    uint i = 0;
    for (auto &point : *res) {
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

TEST_F(TestGeoFuncDifference, line_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0));
    val.push_back(std::make_pair(1.0, 1.0));
    create_line(data, val);
    ObIWkbGeomLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(0.0, 0.0));
    val1.push_back(std::make_pair(0.5, 0.5));
    create_line(data1, val1);
    ObIWkbGeomLineString line1;
    line1.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &line1);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), 0.5);
      ASSERT_EQ(line[0].get<1>(), 0.5);
      ASSERT_EQ(line[1].get<0>(), 1.0);
      ASSERT_EQ(line[1].get<1>(), 1.0);
    }
/*
    typedef boost::geometry::model::linestring<boost::geometry::model::d2::point_xy<double> > line_bg;
    line_bg green, blue;
    boost::geometry::read_wkt(
        "LINESTRING(0 0, 1.0 1.0)", green);

    boost::geometry::read_wkt(
        "LINESTRING(0.0 0.0, 0.5 0.5)", blue);

    std::list<line_bg> output;
    boost::geometry::difference(green, blue, output);

    int i = 0;
    std::cout << "green - blue:" << std::endl;
    BOOST_FOREACH(line_bg const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
        }
    }*/
}

TEST_F(TestGeoFuncDifference, line_polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0));
    val.push_back(std::make_pair(1.0, 1.0));
    create_line(data, val);
    ObIWkbGeomLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::POLYGON));
    uint32_t ring_num = 1;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, ring_num));
    for (uint32_t i = 0; i < ring_num; i++) {
        ASSERT_EQ(OB_SUCCESS, append_uint32(data1, 5));
        /*ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 15.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        */
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 15.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
    }

    ObIWkbGeomPolygon poly;
    poly.set_data(data1.string());

    ObWkbGeomPolygon *geo_condidate = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(poly.val()));
    ObWkbGeomLineString *line_condidate = reinterpret_cast<ObWkbGeomLineString *>(const_cast<char *>(line.val()));
    //boost::geometry::correct(*geo_condidate);
    //boost::geometry::correct(*line_condidate);

    std::cout << boost::geometry::dsv(*geo_condidate) << std::endl;
    std::cout << boost::geometry::area(*geo_condidate) << std::endl;

/*
    ASSERT_EQ(res_bg.size(), 1);
    for (auto & line : res_bg) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), 0.5);
      ASSERT_EQ(line[0].get<1>(), 0.5);
      ASSERT_EQ(line[1].get<0>(), 1.0);
      ASSERT_EQ(line[1].get<1>(), 1.0);
    }
*/


    ObGeoEvalCtx gis_context(&allocator);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &poly);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
    ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
    ASSERT_EQ(res->size(), 1);
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), 0.5);
      ASSERT_EQ(line[0].get<1>(), 0.5);
      ASSERT_EQ(line[1].get<0>(), 1.0);
      ASSERT_EQ(line[1].get<1>(), 1.0);
    }

    /*
    typedef boost::geometry::model::linestring<boost::geometry::model::d2::point_xy<double> > line_bg;
    typedef boost::geometry::model::polygon<boost::geometry::model::d2::point_xy<double> > polygon_bg;
    line_bg green;
    polygon_bg blue;
    boost::geometry::read_wkt(
        "LINESTRING(0 0, 1.0 1.0)", green);

    boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 0.5 0.5, 15.0 10.0, 5.0 10.0, 0.0 0.0))", blue);

    //boost::geometry::correct(blue);
    std::list<line_bg> output;
    boost::geometry::difference(green, blue, output);
    std::cout << boost::geometry::dsv(blue) << std::endl;
    std::cout << boost::geometry::area(blue) << std::endl;

    int i = 0;
    std::cout << "green - blue:" << std::endl;
    BOOST_FOREACH(line_bg const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
        }
    }

    ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), 0.5);
      ASSERT_EQ(line[0].get<1>(), 0.5);
      ASSERT_EQ(line[1].get<0>(), 1.0);
      ASSERT_EQ(line[1].get<1>(), 1.0);
    }
    ObCartesianMultipoint multi_p(0, &allocator);
    ObCartesianMultilinestring multi_l(0, &allocator);
    ObCartesianMultipolygon output_ob(0, &allocator);
    boost::geometry::difference(multi_p, multi_l, output_ob);*/
}

TEST_F(TestGeoFuncDifference, line_multiline)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0));
    val.push_back(std::make_pair(2.0, 2.0));
    create_line(data, val);
    ObIWkbGeomLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
    uint32_t line_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
    for (uint32_t i = 0; i < line_num; i++) {
      std::vector< std::pair<double, double> > val1;
      val1.push_back(std::make_pair(0.0 + i, 0.0 + i));
      val1.push_back(std::make_pair(0.5 + i, 0.5 + i));
      create_line(data1, val1);
    }

    ObIWkbGeomMultiLineString multi_line;
    multi_line.set_data(data1.string());
    ObGeoEvalCtx gis_context(&allocator);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &multi_line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
    ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
    EXPECT_EQ(res->size(), 2);
    int i = 0;
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), 0.5 + i);
      ASSERT_EQ(line[0].get<1>(), 0.5 + i);
      ASSERT_EQ(line[1].get<0>(), 1.0 + i);
      ASSERT_EQ(line[1].get<1>(), 1.0 + i);
      i++;
      std::cout << "(" << line[0].get<0>() << ", " << line[0].get<1>() << ")";
      std::cout << "(" << line[1].get<0>() << ", " << line[1].get<1>() << ")" << std::endl;

    }
/*
    typedef boost::geometry::model::linestring<boost::geometry::model::d2::point_xy<double> > linestring_t;
    typedef boost::geometry::model::multi_linestring<linestring_t> mlinestring_t;
    linestring_t ls;
    boost::geometry::read_wkt(
        "LINESTRING(0 0, 2.0 2.0)", ls);
    mlinestring_t mls{{{0.0, 0.0}, {0.5, 0.5}},
                       {{1.0, 1.0}, {1.5, 1.5}}};

    std::list<linestring_t> output;
    boost::geometry::difference(ls, mls, output);
    ASSERT_EQ(output.size(), 2);
    int i = 0;
    BOOST_FOREACH(linestring_t const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
        }
    }
*/
}

TEST_F(TestGeoFuncDifference, line_multipolygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.0, 0.0));
    val.push_back(std::make_pair(1.0, 4.0));
    create_line(data, val);
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
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
    ObCartesianMultilinestring *res = reinterpret_cast<ObCartesianMultilinestring *>(result);
    EXPECT_EQ(res->size(), 2);
    int i = 0;
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), 1.0);
      ASSERT_EQ(line[0].get<1>(), 1.0 + 2 * i);
      ASSERT_EQ(line[1].get<0>(), 1.0);
      ASSERT_EQ(line[1].get<1>(), 2.0 + 2 * i);
      i++;
      std::cout << "(" << line[0].get<0>() << ", " << line[0].get<1>() << ")";
      std::cout << "(" << line[1].get<0>() << ", " << line[1].get<1>() << ")" << std::endl;

    }
}

TEST_F(TestGeoFuncDifference, polygon_polygon)
{
    typedef boost::geometry::model::polygon<boost::geometry::model::d2::point_xy<double>, false> polygon;

    polygon green, blue;

    boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0))", green);

    boost::geometry::read_wkt(
        "POLYGON((5.0 0.0, 15.0 0.0, 15.0 10.0, 5.0 10.0, 5.0 0.0))", blue);

    std::deque<polygon> output;
    boost::geometry::difference(green, blue, output);
    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(polygon const& l, output)
    {
      auto & outer = l.outer();
      std::cout << "outer : ";
      for (auto & p : outer) {
        std::cout << "(" << p.x() << ", "<< p.y() << ")";
        res_val.push_back(std::make_pair(p.x(), p.y()));
      }
      std::cout << std::endl;
      for (auto & inner : l.inners()) {
        std::cout << "inner : " << i++;
        for (auto & p : inner) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
        std::cout << std::endl;
      }
    }

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t lnum = 1;
    uint32_t pnum = 5;
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
    for (int j = 0; j < lnum; j++) {
      ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
    }

    ObIWkbGeomPolygon poly1;
    poly1.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, lnum));
    for (int j = 0; j < lnum; j++) {
      ASSERT_EQ(OB_SUCCESS, append_uint32(data1, pnum));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 15.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 15.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
    }
    ObIWkbGeomPolygon poly2;
    poly2.set_data(data1.string());
    ObGeoEvalCtx gis_context(&allocator);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &poly1);
    gis_context.ut_set_geo_arg(1, &poly2);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipolygon *res = reinterpret_cast<ObCartesianMultipolygon *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), output.size());

    for (auto &polygon : *res) {
        auto & outer = polygon.exterior_ring();
        for (auto & p : outer) {
          std::cout << "(" << p.get<0>() << ", " << p.get<1>() << ")" << std::endl;
          ASSERT_EQ(p.get<0>(), res_val[i].first);
          ASSERT_EQ(p.get<1>(), res_val[i].second);
          i++;
        }
    }

    for (auto &polygon : *res) {
        ASSERT_EQ(50, boost::geometry::area(polygon));
    }
}

TEST_F(TestGeoFuncDifference, polygon_multipolygon)
{
    typedef bg::model::d2::point_xy<double> point_t;
    typedef bg::model::polygon<point_t> polygon_t;
    typedef bg::model::multi_polygon<polygon_t> mpolygon_t;
    polygon_t pol;

    boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 0.0 4.0, 1.0 4.0, 1.0 0.0, 0.0 0.0))", pol);
    /*
    boost::geometry::read_wkt(
        "POLYGON((1.21681 -1.07095, 5.0 10.0, 1.85841 -1.15838, 1.21681 -1.07095))", pol);
    */
    mpolygon_t mpoly1;
    boost::geometry::read_wkt(
        "MULTIPOLYGON(((0.0 0.0, 0.0 1.0, 2.0 1.0, 2.0 0.0, 0.0 0.0)), ((0.0 2.0, 0.0 3.0, 2.0 3.0, 2.0 2.0, 0.0 2.0)))", mpoly1);

    ASSERT_EQ(mpoly1.size(), 2);
    std::cout << "bg::mpoly1" << boost::geometry::dsv(mpoly1) << std::endl;

    std::list<polygon_t> output;
    boost::geometry::difference(pol, mpoly1, output);
    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(polygon_t const& l, output)
    {
      auto & outer = l.outer();
      std::cout << "outer : ";
      for (auto & p : outer) {
        std::cout << "(" << p.x() << ", "<< p.y() << ")";
        res_val.push_back(std::make_pair(p.x(), p.y()));
      }
      std::cout << std::endl;
      for (auto & inner : l.inners()) {
        std::cout << "inner : " << i++;
        for (auto & p : inner) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
        std::cout << std::endl;
      }
    }



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
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);
    ObCartesianMultipolygon *res = reinterpret_cast<ObCartesianMultipolygon *>(result);
    EXPECT_EQ(res->size(), 2);
    i = 0;
    for (auto & pol : *res) {
      auto & ring = pol.exterior_ring();
      ASSERT_EQ(ring.size(), 5);

      ASSERT_EQ(ring[0].get<0>(), 0.0);
      ASSERT_EQ(ring[0].get<1>(), 1.0 + 2 * i);
      ASSERT_EQ(ring[1].get<0>(), 1.0);
      ASSERT_EQ(ring[1].get<1>(), 1.0 + 2 * i);
      ASSERT_EQ(ring[2].get<0>(), 1.0);
      ASSERT_EQ(ring[2].get<1>(), 2.0 + 2 * i);
      ASSERT_EQ(ring[3].get<0>(), 0.0);
      ASSERT_EQ(ring[3].get<1>(), 2.0 + 2 * i);
      ASSERT_EQ(ring[4].get<0>(), 0.0);
      ASSERT_EQ(ring[4].get<1>(), 1.0 + 2 * i);
      i++;
    }
}

TEST_F(TestGeoFuncDifference, multipoint_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    create_point(data, 15.0, 10.0);
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_multipoint(data1, val);
    ObIWkbGeomMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &multi_point);
    gis_context.ut_set_geo_arg(1, &p);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 4);
    int i = 0;
    for (auto &point : *res) {
        if (i == 2) i++;
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

TEST_F(TestGeoFuncDifference, multipoint_linestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(6.0, 10.0));
    val1.push_back(std::make_pair(20.0, 10.0));
    create_line(data, val1);

    ObIWkbGeomLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_multipoint(data1, val);
    ObIWkbGeomMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &multi_point);
    gis_context.ut_set_geo_arg(1, &line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 4);
    int i = 0;
    for (auto &point : *res) {
        if (i == 2) i++;
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

TEST_F(TestGeoFuncDifference, multipoint_polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(0.0, 0.0));
    val1.push_back(std::make_pair(5.0, 0.0));
    val1.push_back(std::make_pair(5.0, 10.0));
    val1.push_back(std::make_pair(0.0, 10.0));
    val1.push_back(std::make_pair(0.0, 0.0));
    create_polygon(data, 1, 5, val1);

    ObIWkbGeomPolygon pol;
    pol.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_multipoint(data1, val);
    ObIWkbGeomMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &multi_point);
    gis_context.ut_set_geo_arg(1, &pol);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 2);
    int i = 0;
    for (auto &point : *res) {
        while (i == 0 || i == 3 || i == 4) i++;
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

TEST_F(TestGeoFuncDifference, multipoint_multipoint)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(0.0, 0.0));
    val1.push_back(std::make_pair(5.0, 0.0));
    val1.push_back(std::make_pair(5.0, 10.0));
    val1.push_back(std::make_pair(0.0, 10.0));
    val1.push_back(std::make_pair(0.0, 0.0));
    create_multipoint(data, val1);
    ObIWkbGeomMultiPoint multi_point1;
    multi_point1.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_multipoint(data1, val);
    ObIWkbGeomMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &multi_point);
    gis_context.ut_set_geo_arg(1, &multi_point1);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 2);
    int i = 0;
    for (auto &point : *res) {
        while (i == 0 || i == 3 || i == 4) i++;
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

TEST_F(TestGeoFuncDifference, multipoint_multilinestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
    uint32_t line_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, line_num));
    for (uint32_t i = 0; i < line_num; i++) {
      std::vector< std::pair<double, double> > val1;
      val1.push_back(std::make_pair(5.0 + i, 0.0 + i));
      val1.push_back(std::make_pair(5.5 + i, 0.5 + i));
      create_line(data, val1);
    }

    ObIWkbGeomMultiLineString multi_line;
    multi_line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_multipoint(data1, val);
    ObIWkbGeomMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &multi_point);
    gis_context.ut_set_geo_arg(1, &multi_line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Cartesian);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 3);
    int i = 0;
    for (auto &point : *res) {
        while (i == 0 || i == 4) i++;
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

template <typename GeometryType1, typename GeometryType2, typename GeometryRes>
void apply_bg_diff_line_test(GeometryType1 &geo1, GeometryType2 &geo2, const ObSrsItem *srs,
                        GeometryRes &res)
{
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    boost::geometry::difference(geo1, geo2, res, line_strategy);
}

template <typename GeometryType1, typename GeometryType2, typename GeometryRes>
void apply_bg_diff_point_test(GeometryType1 &geo1, GeometryType2 &geo2, const ObSrsItem *srs,
                        GeometryRes &res)
{
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy point_strategy(geog_sphere);
    boost::geometry::difference(geo1, geo2, res, point_strategy);
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

TEST_F(TestGeoFuncDifference, point_point_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeogPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    create_point(data1, 3.2, 5.5);
    ObIWkbGeogPoint q;
    q.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &q);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObCartesianMultipoint *res = reinterpret_cast<ObCartesianMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_line_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    create_point(data, 1.2, 3.5);
    ObIWkbGeogPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.2, 3.5));
    val.push_back(std::make_pair(3.5, 5.5));
    create_line(data1, val);
    ObIWkbGeogLineString line;
    line.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultipoint *res = reinterpret_cast<ObGeographMultipoint *>(result);
    ASSERT_EQ(res->empty(), true);
    ASSERT_EQ(res->size(), 0);
}

TEST_F(TestGeoFuncDifference, point_polygon_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeogPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_polygon(data1, 1, 5, val);
    ObIWkbGeogPolygon poly;
    poly.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &poly);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultipoint *res = reinterpret_cast<ObGeographMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_multipoint_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeogPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(1.2, 3.5));
    create_multipoint(data1, val);
    ObIWkbGeogMultiPoint multi_point;
    multi_point.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &multi_point);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultipoint *res = reinterpret_cast<ObGeographMultipoint *>(result);
    ASSERT_EQ(res->empty(), true);
    ASSERT_EQ(res->size(), 0);
}

TEST_F(TestGeoFuncDifference, point_multiline_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeogPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(0.0, 0.0));
    create_multiline(data1, 2, 3, val);
    ObIWkbGeogMultiLineString multi_line;
    multi_line.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &multi_line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultipoint *res = reinterpret_cast<ObGeographMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, point_multipolygon_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    create_point(data, 1.2, 3.5);
    ObIWkbGeogPoint p;
    p.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    int polynum = 2;
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polynum));
    for (int i = 0; i < polynum; i++) {
      std::vector< std::pair<double, double> > val;
      val.push_back(std::make_pair(5.0 + i, 0.0 + i));
      val.push_back(std::make_pair(15.0 + i, 0.0 + i));
      val.push_back(std::make_pair(15.0 + i, 10.0 + i));
      val.push_back(std::make_pair(5.0 + i, 10.0 + i));
      val.push_back(std::make_pair(5.0 + i, 0.0 + i));
      create_polygon(data1, 1, 5, val);
    }
    ObIWkbGeogMultiPolygon multi_polygon;
    multi_polygon.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &p);
    gis_context.ut_set_geo_arg(1, &multi_polygon);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOINT);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultipoint *res = reinterpret_cast<ObGeographMultipoint *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    for (auto &point : *res) {
        ASSERT_EQ(1.2, point.get<0>());
        ASSERT_EQ(3.5, point.get<1>());
    }
}

TEST_F(TestGeoFuncDifference, line_point_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.4, 2.3));
    val.push_back(std::make_pair(3.5, 5.5));
    create_line(data, val);

    ObIWkbGeogLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    create_point(data1, 3.2, 5.5);
    ObIWkbGeogPoint q;
    q.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &q);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::LINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographLineString *res = reinterpret_cast<ObGeographLineString *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 2);
    uint i = 0;
    for (auto &point : *res) {
        ASSERT_EQ(val[i].first, point.get<0>());
        ASSERT_EQ(val[i].second, point.get<1>());
        i++;
    }
}

TEST_F(TestGeoFuncDifference, line_line_geog)
{
    typedef boost::geometry::model::linestring<boost::geometry::model::d2::point_xy<double, boost::geometry::cs::geographic<boost::geometry::radian>> > line_bg;
    line_bg green, blue;
    boost::geometry::read_wkt(
        "LINESTRING(0 0, 1.0 1.0)", green);

    boost::geometry::read_wkt(
        "LINESTRING(0.0 0.0, 0.5 0.5)", blue);

    std::list<line_bg> output;
    boost::geometry::difference(green, blue, output);

    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(line_bg const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
    }

    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0));
    val.push_back(std::make_pair(1.0, 1.0));
    create_line(data, val);
    ObIWkbGeogLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(0.0, 0.0));
    val1.push_back(std::make_pair(0.5, 0.5));
    create_line(data1, val1);
    ObIWkbGeogLineString line1;
    line1.set_data(data1.string());

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &line1);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultilinestring *res = reinterpret_cast<ObGeographMultilinestring *>(result);
    ASSERT_EQ(res->empty(), false);
    ASSERT_EQ(res->size(), 1);
    i = 0;
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), res_val[i].first);
      ASSERT_EQ(line[0].get<1>(), res_val[i].second);
      ASSERT_EQ(line[1].get<0>(), res_val[i + 1].first);
      ASSERT_EQ(line[1].get<1>(), res_val[i + 1].second);
      std::cout << "(" << line[0].get<0>() << ", " << line[0].get<1>() << ")";
      std::cout << "(" << line[1].get<0>() << ", " << line[1].get<1>() << ")" << std::endl;
      i += 2;
    }
}

typedef bg::model::d2::point_xy<double, bg::cs::geographic<bg::radian> > point_geog_t;
typedef bg::model::multi_point<point_geog_t> mpoint_geog_t;
typedef bg::model::linestring<point_geog_t> line_geog_t;
typedef bg::model::multi_linestring<line_geog_t> mline_geog_t;
typedef bg::model::polygon<point_geog_t, false> polygon_geog_t;
typedef bg::model::multi_polygon<polygon_geog_t> mpolygon_geog_t;


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


TEST_F(TestGeoFuncDifference, line_polygon_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

    line_geog_t green;
    polygon_geog_t blue;
    boost::geometry::read_wkt(
        "LINESTRING(1.0 1.0, 4.0 4.0)", green);
    boost::geometry::read_wkt(
        "POLYGON((0.5 0.5, 1.5 1.5, 2.0 2.5, 1.5 0.5, 0.5 0.5))", blue);

    mline_geog_t output;

    apply_bg_diff_line_test(green, blue, srs, output);
    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(line_geog_t const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
    }

    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.0, 1.0));
    val.push_back(std::make_pair(4.0, 4.0));
    create_line(data, val);
    ObIWkbGeogLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::POLYGON));
    uint32_t ring_num = 1;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, ring_num));
    for (uint32_t i = 0; i < ring_num; i++) {
        ASSERT_EQ(OB_SUCCESS, append_uint32(data1, 5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 1.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 1.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 2.0));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 2.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 1.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
        ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.5));
    }

    ObIWkbGeogPolygon poly;
    poly.set_data(data1.string());
    ObGeoEvalCtx gis_context(&allocator, srs);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &poly);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
    ObGeographMultilinestring *res = reinterpret_cast<ObGeographMultilinestring *>(result);
    ASSERT_EQ(res->size(), 1);
    ASSERT_EQ(true, is_geo_equal(output, *res));
}

TEST_F(TestGeoFuncDifference, line_multiline_geog)
{
    typedef bg::model::linestring<bg::model::d2::point_xy<double, bg::cs::geographic<bg::radian>> > linestring_t;
    typedef bg::model::multi_linestring<linestring_t> mlinestring_t;
    linestring_t ls;
    boost::geometry::read_wkt(
        "LINESTRING(0 0, 2.0 2.0)", ls);
    mlinestring_t mls{{{0.0, 0.0}, {0.5, 0.5}},
                       {{1.0, 1.0}, {1.5, 1.5}}};

    std::list<linestring_t> output;
    boost::geometry::difference(ls, mls, output);
    ASSERT_EQ(output.size(), 1);
    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(linestring_t const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
    }

    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0));
    val.push_back(std::make_pair(2.0, 2.0));
    create_line(data, val);
    ObIWkbGeogLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTILINESTRING));
    uint32_t line_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, line_num));
    for (uint32_t i = 0; i < line_num; i++) {
      std::vector< std::pair<double, double> > val1;
      val1.push_back(std::make_pair(0.0 + i, 0.0 + i));
      val1.push_back(std::make_pair(0.5 + i, 0.5 + i));
      create_line(data1, val1);
    }

    ObIWkbGeogMultiLineString multi_line;
    multi_line.set_data(data1.string());
    ObGeoEvalCtx gis_context(&allocator, srs);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &multi_line);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
    ObGeographMultilinestring *res = reinterpret_cast<ObGeographMultilinestring *>(result);
    EXPECT_EQ(res->size(), output.size());
    i = 0;
    for (auto & line : *res) {
      ASSERT_EQ(line.size(), 2);
      ASSERT_EQ(line[0].get<0>(), res_val[i].first);
      ASSERT_EQ(line[0].get<1>(), res_val[i].second);
      ASSERT_EQ(line[1].get<0>(), res_val[i + 1].first);
      ASSERT_EQ(line[1].get<1>(), res_val[i + 1].second);
      i += 2;
      std::cout << "(" << line[0].get<0>() << ", " << line[0].get<1>() << ")";
      std::cout << "(" << line[1].get<0>() << ", " << line[1].get<1>() << ")" << std::endl;

    }
}

TEST_F(TestGeoFuncDifference, line_multipolygon_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

    line_geog_t ls;
    boost::geometry::read_wkt(
        "LINESTRING(1.0 1.0, 4.0 4.0)", ls);
    mpolygon_geog_t mpols{{{{0.5, 0.5}, {1.5, 1.5}, {2.0, 2.5}, {1.5, 0.5}, {0.5, 0.5}}},
                       {{{0.5, 1.5}, {1.5, 2.5}, {2.0, 3.5}, {1.5, 1.5}, {0.5, 1.5}}}};

    mline_geog_t output;
    apply_bg_diff_line_test(ls, mpols, srs, output);
    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(line_geog_t const& l, output)
    {
        std::cout << i++ << ": " << l.size() << std::endl;
        for (auto & p : l) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
    }

    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(1.0, 1.0));
    val.push_back(std::make_pair(4.0, 4.0));
    create_line(data, val);
    ObIWkbGeogLineString line;
    line.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
    uint32_t polygon_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
    for (uint32_t i = 0; i < polygon_num; i++) {
      std::vector< std::pair<double, double> > val;
      val.push_back(std::make_pair(0.5, 0.5 + i));
      val.push_back(std::make_pair(1.5, 1.5 + i));
      val.push_back(std::make_pair(2.0, 2.5 + i));
      val.push_back(std::make_pair(1.5, 0.5 + i));
      val.push_back(std::make_pair(0.5, 0.5 + i));
      create_polygon(data1, 1, 5, val);
    }

    ObIWkbGeogMultiPolygon multi_poly;
    multi_poly.set_data(data1.string());
    ObGeoEvalCtx gis_context(&allocator, srs);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &line);
    gis_context.ut_set_geo_arg(1, &multi_poly);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTILINESTRING);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
    ObGeographMultilinestring *res = reinterpret_cast<ObGeographMultilinestring *>(result);
    EXPECT_EQ(res->size(), output.size());
    ASSERT_EQ(true, is_geo_equal(*res, output));
}

TEST_F(TestGeoFuncDifference, polygon_polygon_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

    polygon_geog_t green, blue;

    boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 0.0 10.0, 10.0 10.0, 10.0 0.0, 0.0 0.0))", green);

    boost::geometry::read_wkt(
        "POLYGON((5.0 0.0, 5.0 10.0, 15.0 10.0, 15.0 0.0, 5.0 0.0))", blue);

    mpolygon_geog_t output;
    apply_bg_diff_line_test(green, blue, srs, output);
    int i = 0;
    std::cout << "green && blue:" << std::endl;

    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(polygon_geog_t const& l, output)
    {
      auto & outer = l.outer();
      std::cout << "outer : ";
      for (auto & p : outer) {
        std::cout << "(" << p.x() << ", "<< p.y() << ")";
        res_val.push_back(std::make_pair(p.x(), p.y()));
      }
      std::cout << std::endl;
      for (auto & inner : l.inners()) {
        std::cout << "inner : " << i++;
        for (auto & p : inner) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
        std::cout << std::endl;
      }
    }

    ObJsonBuffer data(&allocator);
    uint32_t lnum = 1;
    uint32_t pnum = 5;
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
    for (int j = 0; j < lnum; j++) {
      ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
    }

    ObIWkbGeogPolygon poly1;
    poly1.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, lnum));
    for (int j = 0; j < lnum; j++) {
      ASSERT_EQ(OB_SUCCESS, append_uint32(data1, pnum));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 15.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 10.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 15.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 5.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data1, 0.0));
    }
    ObIWkbGeogPolygon poly2;
    poly2.set_data(data1.string());
    ObGeoEvalCtx gis_context(&allocator, srs);

    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &poly1);
    gis_context.ut_set_geo_arg(1, &poly2);
    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);

    ObGeographMultipolygon *res = reinterpret_cast<ObGeographMultipolygon *>(result);
    ASSERT_EQ(true, is_geo_equal(*res, output));
}

TEST_F(TestGeoFuncDifference, polygon_multipolygon_geog)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObSrsItem *srs = NULL;
    ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs));

    polygon_geog_t pol;
    boost::geometry::read_wkt(
        "POLYGON((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0))", pol);
    mpolygon_geog_t mpoly1;
    boost::geometry::read_wkt(
        "MULTIPOLYGON(((5.0 0.0, 15.0 0.0, 15.0 10.0, 5.0 10.0, 5.0 0.0)), ((0.0 5.0, 5.0 5.0, 5.0 8.0, 0.0 8.0, 0.0 5.0)))", mpoly1);

    mpolygon_geog_t output;
    apply_bg_diff_line_test(pol, mpoly1, srs, output);
    int i = 0;
    std::vector< std::pair<double, double> > res_val;
    BOOST_FOREACH(polygon_geog_t const& l, output)
    {
      auto & outer = l.outer();
      std::cout << "outer : ";
      for (auto & p : outer) {
        std::cout << "(" << p.x() << ", "<< p.y() << ")";
        res_val.push_back(std::make_pair(p.x(), p.y()));
      }
      std::cout << std::endl;
      for (auto & inner : l.inners()) {
        std::cout << "inner : " << i++;
        for (auto & p : inner) {
          std::cout << "(" << p.x() << ", "<< p.y() << ")" << std::endl;
          res_val.push_back(std::make_pair(p.x(), p.y()));
        }
        std::cout << std::endl;
      }
    }
    ObJsonBuffer data(&allocator);
    std::vector< std::pair<double, double> > pol_val;
    pol_val.push_back(std::make_pair(0.0, 0.0));
    pol_val.push_back(std::make_pair(10.0, 0.0));
    pol_val.push_back(std::make_pair(10.0, 10.0));
    pol_val.push_back(std::make_pair(0.0, 10.0));
    pol_val.push_back(std::make_pair(0.0, 0.0));
    create_polygon(data, 1, 5, pol_val);
    ObIWkbGeogPolygon poly;
    poly.set_data(data.string());

    ObJsonBuffer data1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data1));
    ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
    uint32_t polygon_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));

    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(5.0, 0.0));
    val.push_back(std::make_pair(15.0, 0.0));
    val.push_back(std::make_pair(15.0, 10.0));
    val.push_back(std::make_pair(5.0, 10.0));
    val.push_back(std::make_pair(5.0, 0.0));
    create_polygon(data1, 1, 5, val);
    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(0.0, 5.0));
    val1.push_back(std::make_pair(5.0, 5.0));
    val1.push_back(std::make_pair(5.0, 8.0));
    val1.push_back(std::make_pair(0.0, 8.0));
    val1.push_back(std::make_pair(0.0, 5.0));
    create_polygon(data1, 1, 5, val1);

    ObIWkbGeogMultiPolygon multi_poly;
    multi_poly.set_data(data1.string());

    const ObWkbGeogMultiPolygon *multi_p = reinterpret_cast<const ObWkbGeogMultiPolygon*>(multi_poly.val());
    ASSERT_EQ(multi_p->size(), 2);
    std::cout << "ob::mpoly1" << boost::geometry::dsv(*multi_p) << std::endl;

    ObGeoEvalCtx gis_context(&allocator, srs);
    gis_context.ut_set_geo_count(2);
    gis_context.ut_set_geo_arg(0, &poly);
    gis_context.ut_set_geo_arg(1, &multi_poly);

    ObGeometry *result = NULL;
    int ret = ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(result != NULL);
    ASSERT_EQ(result->type(), ObGeoType::MULTIPOLYGON);
    ASSERT_EQ(result->crs(), ObGeoCRS::Geographic);
    ObGeographMultipolygon *res = reinterpret_cast<ObGeographMultipolygon *>(result);
    EXPECT_EQ(res->size(), output.size());
    ASSERT_EQ(true, is_geo_equal(*res, output));
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
