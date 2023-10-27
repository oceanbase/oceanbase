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
#define private public
#include "lib/json_type/ob_json_common.h"
#include "lib/geo/ob_s2adapter.h"
#include "lib/utility/ob_test_util.h"

#include <vector>
#include <iostream>
#undef private

namespace oceanbase {
namespace common {

class TestS2Adapter : public ::testing::Test {
public:
  TestS2Adapter()
  {}
  ~TestS2Adapter()
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
  DISALLOW_COPY_AND_ASSIGN(TestS2Adapter);
};

int append_srid(ObJsonBuffer& data, uint32_t srid = 0)
{
  return data.append(reinterpret_cast<char*>(&srid), sizeof(srid));
}

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

void append_double_point(ObJsonBuffer& data, double &x, double &y)
{
  ASSERT_EQ(OB_SUCCESS, append_double(data, x));
  ASSERT_EQ(OB_SUCCESS, append_double(data, y));
}

void append_point(ObJsonBuffer& data, double x, double y)
{
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
  append_double_point(data, x, y);
}

void append_inner_point(ObJsonBuffer& data, double x, double y)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
  append_double_point(data, x, y);
}

void append_ring(ObJsonBuffer& data, std::vector<double>& xv, std::vector<double>& yv)
{
  uint32_t pnum = xv.size();
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
  for (int k = 0; k < pnum; k++) {
    append_double_point(data, xv[k], yv[k]);
  }
}

void append_line(ObJsonBuffer& data, std::vector<double>& xv, std::vector<double>& yv)
{
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
  append_ring(data, xv, yv);
}

void append_inner_line(ObJsonBuffer& data, std::vector<double>& xv, std::vector<double>& yv)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
  append_ring(data, xv, yv);
}

void append_inner_multi_point(ObJsonBuffer& data, std::vector<double>& xv, std::vector<double>& yv)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, xv.size()));
  for (int i = 0; i < xv.size(); i++) {
    append_inner_point(data, xv[i], yv[i]);
  }
}

void append_multi_point(ObJsonBuffer& data, std::vector<double>& xv, std::vector<double>& yv)
{
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, xv.size()));
  for (int i = 0; i < xv.size(); i++) {
    append_inner_point(data, xv[i], yv[i]);
  }
}

void append_inner_multi_line(ObJsonBuffer& data, std::vector<std::vector<double>>& xv, std::vector<std::vector<double>>& yv)
{
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, xv.size()));
  for (int i = 0; i < xv.size(); i++) {
    append_inner_line(data, xv[i], yv[i]);
  }
}

void append_multi_line(ObJsonBuffer& data, std::vector<std::vector<double>>& xv, std::vector<std::vector<double>>& yv)
{
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, xv.size()));
  for (int i = 0; i < xv.size(); i++) {
    append_inner_line(data, xv[i], yv[i]);
  }
}

void append_poly(ObJsonBuffer& data, std::vector<std::vector<double>>& xv, std::vector<std::vector<double>>& yv)
{
  uint32 lnum = xv.size();
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
  // push rings
  for (int j = 0; j < lnum; j++) {
    append_ring(data, xv[j], yv[j]);
  }
}

void append_inner_poly(ObJsonBuffer& data, std::vector<std::vector<double>>& xv, std::vector<std::vector<double>>& yv)
{
  uint32 lnum = xv.size();
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
  // push rings
  for (int j = 0; j < lnum; j++) {
    append_ring(data, xv[j], yv[j]);
  }
}

void append_multi_poly(ObJsonBuffer& data, std::vector<std::vector<std::vector<double>>>& xv, std::vector<std::vector<std::vector<double>>>& yv)
{
  uint32 pNum = xv.size();
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, pNum));
  // push poly
  for (int j = 0; j < pNum; j++) {
    append_inner_poly(data, xv[j], yv[j]);
  }
}

void append_inner_multi_poly(ObJsonBuffer& data, std::vector<std::vector<std::vector<double>>>& xv, std::vector<std::vector<std::vector<double>>>& yv)
{
  uint32 pNum = xv.size();
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, pNum));
  // push poly
  for (int j = 0; j < pNum; j++) {
    append_inner_poly(data, xv[j], yv[j]);
  }
}

void printCellid(ObS2Cellids &cells) {
  for (auto it : cells) {
    S2CellId tmp(it);
    std::cout << "F" << tmp.face();
    std::cout << "/L" << tmp.level() << "/";
    for (int level = 1; level <= tmp.level(); level++) {
      std::cout << tmp.child_position(level);
    }
    std::cout << std::endl;
  }
}

void printMbr(ObSpatialMBR &mbr) {
  std::cout << "BoundingBox: ";
  std::cout << "lo_x:" << mbr.get_xmin() << " ";
  std::cout << "hi_x:" << mbr.get_xmax() << " ";
  std::cout << "lo_y:" << mbr.get_ymin() << " ";
  std::cout << "hi_y:" << mbr.get_ymax() << " ";
  std::cout << std::endl;
}

TEST_F(TestS2Adapter, s2point)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  append_point(data, 1, 5);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObS2Cellids cells_with_ancestor;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  OK(s2object.get_cellids(cells_with_ancestor, true));
  printCellid(cells);
  printMbr(mbr);
  printCellid(cells_with_ancestor);
}

TEST_F(TestS2Adapter, s2polyline)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<double> xv{-86.609955, -86.609836};
  std::vector<double> yv{32.703682, 32.703659};
  append_line(data, xv, yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

TEST_F(TestS2Adapter, s2polygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::vector<double>> xv;
  std::vector<std::vector<double>> yv;
  std::vector<double> x1{-79,-77,-78,-79};
  std::vector<double> y1{42,44,41,42};
  xv.push_back(x1);
  yv.push_back(y1);
  append_poly(data, xv, yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

TEST_F(TestS2Adapter, complexPolygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::vector<double>> xv;
  std::vector<std::vector<double>> yv;
  std::vector<double> x1{-74.0126927094665,-74.0127064547618,-74.0141319802258,-74.0145335611922,-74.0145358849763,-74.0145399285567,-74.014541648353,-74.0145797021934,-74.0145806405216,-74.0145817746666,-74.0145819704835,-74.0145789529984,-74.0145775882448,-74.0145730223663,-74.0145698212414,-74.0144997485308,-74.0144993922351,-74.0144986708037,-74.014498305668,-74.0136647221644,-74.0134542384902,-74.0134540575192,-74.0134536934255,-74.013453510303,-74.0128195031462,-74.012816235274,-74.0128095254169,-74.0128060834319,-74.0127019801943,-74.012700315079,-74.0126970944194,-74.012695538875,-74.011062576148,-74.0110624213581,-74.0127057329214,-74.0127094072378,-74.0127040330277,-74.0126927094665};
  std::vector<double> y1{40.6600480732464,40.6600447465578,40.6609147092529,40.6611344232761,40.6611361779097,40.6611402822267,40.6611426319101,40.6612147162775,40.6612173067132,40.661222645325,40.6612253935011,40.6612703205022,40.6612747317321,40.6612824707693,40.6612857985766,40.6613308932157,40.6613311121095,40.6613315348099,40.6613317386166,40.6617744175344,40.6618992353571,40.6618993401319,40.6618995458745,40.6618996468423,40.6622406226638,40.6622417071219,40.6622427033898,40.6622426151996,40.6622216604417,40.6622211716785,40.6622199136429,40.6622191443704,40.6612266089193,40.6612218066015,40.6600718294846,40.6600602461869,40.6600453326751,40.6600480732464};
  xv.push_back(x1);
  yv.push_back(y1);
  append_poly(data, xv, yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

TEST_F(TestS2Adapter, complexPolygons)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::vector<double>> xv;
  std::vector<std::vector<double>> yv;
  std::vector<double> x1{20,10,10,30,45,20};
  std::vector<double> y1{35,30,10,5,20,35};
  xv.push_back(x1);
  yv.push_back(y1);
  std::vector<double> x2{30,20,20,30};
  std::vector<double> y2{20,15,25,20};
  xv.push_back(x2);
  yv.push_back(y2);
  append_poly(data, xv, yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}


TEST_F(TestS2Adapter, multiPoint)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);

  std::vector<double> xv{1,3};
  std::vector<double> yv{5,4};
  append_multi_point(data, xv, yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

TEST_F(TestS2Adapter, multiLine)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::vector<double>> xv;
  std::vector<std::vector<double>> yv;
  std::vector<double> xy1{0,1};
  std::vector<double> xy2{2,3};
  xv.push_back(xy1);
  yv.push_back(xy1);
  xv.push_back(xy2);
  yv.push_back(xy2);
  append_multi_line(data, xv, yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

TEST_F(TestS2Adapter, multiPolygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  std::vector<std::vector<std::vector<double>>> multi_poly_xv;
  std::vector<std::vector<std::vector<double>>> multi_poly_yv;
  std::vector<std::vector<double>> poly1_xv;
  std::vector<std::vector<double>> poly1_yv;
  std::vector<double> x1{20,10,10,30,45,20};
  std::vector<double> y1{35,30,10,5,20,35};
  poly1_xv.push_back(x1);
  poly1_yv.push_back(y1);
  std::vector<double> x2{30,20,20,30};
  std::vector<double> y2{20,15,25,20};
  poly1_xv.push_back(x2);
  poly1_yv.push_back(y2);
  multi_poly_xv.push_back(poly1_xv);
  multi_poly_yv.push_back(poly1_yv);

  std::vector<std::vector<double>> poly2_xv;
  std::vector<std::vector<double>> poly2_yv;
  std::vector<double> x3{40,20,45,40};
  std::vector<double> y3{40,45,30,40};
  poly2_xv.push_back(x3);
  poly2_yv.push_back(y3);
  multi_poly_xv.push_back(poly2_xv);
  multi_poly_yv.push_back(poly2_yv);
  append_multi_poly(data, multi_poly_xv, multi_poly_yv);
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

TEST_F(TestS2Adapter, collection)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_srid(data, 0));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
  // point
  append_inner_point(data, 40, 10);
  // line
  std::vector<double> xv{10,20,10};
  std::vector<double> yv{10,20,40};
  append_inner_line(data, xv, yv);
  // polygon
  std::vector<std::vector<double>> xvv,yvv;
  std::vector<double> x1{40,20,45,40};
  std::vector<double> y1{40,45,30,40};
  xvv.push_back(x1);
  yvv.push_back(y1);
  append_inner_poly(data, xvv, yvv);
  // multi point
  std::vector<double> multiPointXv{1,3};
  std::vector<double> multiPointYv{5,4};
  append_inner_multi_point(data, multiPointXv, multiPointYv);
  // multi line
  std::vector<std::vector<double>> multiLineXv;
  std::vector<std::vector<double>> multiLineYv;
  multiLineXv.push_back({0,1});
  multiLineYv.push_back({0,1});
  multiLineXv.push_back({2,3});
  multiLineYv.push_back({2,3});
  append_inner_multi_line(data, multiLineXv, multiLineYv);
  // multi polygon
  std::vector<std::vector<std::vector<double>>> multi_poly_xv;
  std::vector<std::vector<std::vector<double>>> multi_poly_yv;
  std::vector<std::vector<double>> poly1_xv;
  std::vector<std::vector<double>> poly1_yv;
  poly1_xv.push_back({20,10,10,30,45,20});
  poly1_yv.push_back({35,30,10,5,20,35});
  poly1_xv.push_back({30,20,20,30});
  poly1_yv.push_back({20,15,25,20});
  multi_poly_xv.push_back(poly1_xv);
  multi_poly_yv.push_back(poly1_yv);
  std::vector<std::vector<double>> poly2_xv;
  std::vector<std::vector<double>> poly2_yv;
  poly2_xv.push_back({40,20,45,40});
  poly2_yv.push_back({40,45,30,40});
  multi_poly_xv.push_back(poly2_xv);
  multi_poly_yv.push_back(poly2_yv);
  append_inner_multi_poly(data, multi_poly_xv, multi_poly_yv);
  // geometry
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));
  ObString wkb(data.length(), data.ptr());
  ObS2Adapter s2object(&allocator, true);
  ObS2Cellids cells;
  ObSpatialMBR mbr;
  OK(s2object.init(wkb));
  OK(s2object.get_cellids(cells, false));
  OK(s2object.get_mbr(mbr));
  printCellid(cells);
  printMbr(mbr);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}