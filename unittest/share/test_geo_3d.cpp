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
#define private public
#define protected public
#include "lib/geo/ob_geo_3d.h"
#include "lib/geo/ob_wkt_parser.h"
#undef private
#undef protected

#include <iostream>

namespace oceanbase {
namespace common {
class TestGeo3D : public ::testing::Test
{
public:
  TestGeo3D()
  {}
  ~TestGeo3D()
  {}

  ObString to_hex(const ObString &str) {
    uint64_t out_str_len = str.length() * 2;
    int64_t pos = 0;
    char *data = static_cast<char *>(allocator_.alloc(out_str_len));
    hex_print(str.ptr(), str.length(), data, out_str_len, pos);
    return ObString(out_str_len, data);
  }

  ObString mock_to_wkb(ObGeometry *geo) {
    ObString wkb;
    if (OB_NOT_NULL(geo) && !geo->is_tree()) {
      ObIWkbGeometry *geo_bin = reinterpret_cast<ObIWkbGeometry *>(geo);
      wkb = geo_bin->data_;
    }
    return wkb;
  }

  void compare_3d_to_2d_result(ObGeoType geo_type, ObString &wkb_3d, ObString &wkb_2d);
  void compare_to_3d_wkt_result(ObGeoType geo_type, ObString &wkt_3d, ObString &wkt_3d_res);
  void check_wkb_is_valid_3d(ObString &wkt, bool is_valid);
  void check_reserver_coordinate(ObString &wkt);
  // construct data
  template<typename T>
  int append_val(ObStringBuffer &buf, T t, ObGeoWkbByteOrder bo);
  int append_pointz(ObStringBuffer &buf, double x, double y, double z, ObGeoWkbByteOrder bo);
  int mock_get_tenant_srs_item(ObIAllocator &allocator, uint64_t srs_id, const ObSrsItem *&srs_item);
private:
  ObArenaAllocator allocator_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestGeo3D);
};

template<typename T>
int TestGeo3D::append_val(ObStringBuffer &buf, T t, ObGeoWkbByteOrder bo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(buf.reserve(sizeof(T)))) {
  } else {
    char *ptr = buf.ptr() + buf.length();
    ObGeoWkbByteOrderUtil::write<T>(ptr, t, bo);
    ret = buf.set_length(buf.length() + sizeof(T));
  }
  return ret;
}

template<>
int TestGeo3D::append_val(ObStringBuffer &buf, uint8_t t, ObGeoWkbByteOrder bo)
{
  return buf.append(reinterpret_cast<char*>(&t), sizeof(uint8_t));
}

int TestGeo3D::append_pointz(ObStringBuffer &buf, double x, double y, double z, ObGeoWkbByteOrder bo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(append_val<double>(buf, x, bo))) {
  } else if (OB_FAIL(append_val<double>(buf, y, bo))) {
  } else if (OB_FAIL(append_val<double>(buf, z, bo))) {
  }
  return ret;
}

void TestGeo3D::compare_3d_to_2d_result(ObGeoType geo_type, ObString &wkt_3d, ObString &wkb_2d)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo_3d = NULL;
  ObGeometry *geo_2d = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, wkt_3d, geo_3d, true, false));
  ASSERT_TRUE(NULL != geo_3d);
  ASSERT_EQ(geo_3d->type(), geo_type);
  ASSERT_EQ(OB_SUCCESS, static_cast<ObGeometry3D *>(geo_3d)->to_2d_geo(allocator, geo_2d));
  ASSERT_TRUE(NULL != geo_2d);
  ASSERT_EQ(geo_2d->type(), static_cast<ObGeoType>(static_cast<uint32_t>(geo_type) - 1000));
  ObString wkb_2d_res = to_hex(mock_to_wkb(geo_2d));
  ASSERT_EQ(wkb_2d_res, wkb_2d);
}

void TestGeo3D::compare_to_3d_wkt_result(ObGeoType geo_type, ObString &wkt_3d, ObString &wkt_3d_res)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo_3d = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, wkt_3d, geo_3d, true, false));
  ASSERT_TRUE(NULL != geo_3d);
  ASSERT_EQ(geo_3d->type(), geo_type);
  ObString wkt;
  ASSERT_EQ(OB_SUCCESS, static_cast<ObGeometry3D *>(geo_3d)->to_wkt(allocator, wkt));
  std::cout<<std::string(wkt.ptr(), wkt.length())<<std::endl;
  std::cout<<std::string(wkt_3d_res.ptr(), wkt_3d_res.length())<<std::endl;
  ASSERT_EQ(wkt_3d_res, wkt);
}

void TestGeo3D::check_wkb_is_valid_3d(ObString &wkt, bool expected_valid)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ObString wkb(geo->length(), geo->val());
  ObGeometry3D geo_3d;
  geo_3d.set_data(wkb);
  bool is_valid = false;
  if (OB_SUCCESS == geo_3d.check_wkb_valid()) {
    is_valid = true;
  } else {
    is_valid = false;
  }
  // std::cout<<std::string(wkt.ptr(), wkt.length())<<std::endl;
  ASSERT_EQ(is_valid, expected_valid);
}

void TestGeo3D::check_reserver_coordinate(ObString &wkt)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  ObString res_wkt;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ObString wkb(geo->length(), geo->val());
  ObString wkb_copy;
  ob_write_string(allocator, wkb, wkb_copy);
  ObGeometry3D geo_3d;
  geo_3d.set_data(wkb);
  ASSERT_EQ(OB_SUCCESS, geo_3d.reverse_coordinate());
  ASSERT_NE(0, MEMCMP(geo_3d.val(), wkb_copy.ptr(), wkb.length()));
  // ASSERT_EQ(OB_SUCCESS, geo_3d.to_wkt(allocator,res_wkt));
  // std::cout<<"wkt: "<<std::string(wkt.ptr(), wkt.length())<<std::endl;
  // std::cout<<"reserver wkt: "<<std::string(res_wkt.ptr(), res_wkt.length())<<std::endl;
  ASSERT_EQ(OB_SUCCESS, geo_3d.reverse_coordinate());
  // ASSERT_EQ(OB_SUCCESS, geo_3d.to_wkt(allocator,res_wkt));
  // ASSERT_EQ(wkt, res_wkt);
  ASSERT_EQ(0, MEMCMP(geo_3d.val(), wkb_copy.ptr(), wkb.length()));

}

int TestGeo3D::mock_get_tenant_srs_item(ObIAllocator &allocator, uint64_t srs_id, const ObSrsItem *&srs_item)
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

TEST_F(TestGeo3D, test_3d_to_2d)
{
  // point
  ObString wkt_3d = ObString::make_string("POINT Z (1 1 1)");
  ObString wkb_2d = ObString::make_string("0101000000000000000000F03F000000000000F03F");
  compare_3d_to_2d_result(ObGeoType::POINTZ, wkt_3d, wkb_2d);
  // linestring
  wkt_3d = ObString::make_string("LINESTRING Z (1 1 1,1 2 1)");
  wkb_2d = ObString::make_string("010200000002000000000000000000F03F000000000000F03F000000000000F03F0000000000000040");
  compare_3d_to_2d_result(ObGeoType::LINESTRINGZ, wkt_3d, wkb_2d);
  // polygon
  wkt_3d = ObString::make_string("POLYGONZ((0 0 1,10 0 2 ,10 10 2,0 10 2,0 0 1),(2 2 5 ,2 5 4,5 5 3,5 2 3,2 2 5))");
  wkb_2d = ObString::make_string("0103000000020000000500000000000000000000000000000000000000000000000000244000000000"
                                "00000000000000000000244000000000000024400000000000000000000000000000244000000000000"
                                "00000000000000000000005000000000000000000004000000000000000400000000000000040000000"
                                "00000014400000000000001440000000000000144000000000000014400000000000000040000000000"
                                "00000400000000000000040");
  compare_3d_to_2d_result(ObGeoType::POLYGONZ, wkt_3d, wkb_2d);
  // multipoint
  wkt_3d = ObString::make_string("MULTIPOINTZ((0 0 0), (2 0 1))");
  wkb_2d = ObString::make_string("010400000002000000010100000000000000000000000000000000000000010100000000000000000000400000000000000000");
  compare_3d_to_2d_result(ObGeoType::MULTIPOINTZ, wkt_3d, wkb_2d);
  // multilinestring
  wkt_3d = ObString::make_string("MULTILINESTRINGZ((0 0 1, 2 0 2), (1 1 3, 2 2 4))");
  wkb_2d = ObString::make_string("0105000000020000000102000000020000000000000000000000000000000000000000000000000000"
                                 "400000000000000000010200000002000000000000000000F03F000000000000F03F000000000000004"
                                 "00000000000000040");
  compare_3d_to_2d_result(ObGeoType::MULTILINESTRINGZ, wkt_3d, wkb_2d);
  // multipolygon
  wkt_3d = ObString::make_string("MULTIPOLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3)),((2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))");
  wkb_2d = ObString::make_string("0106000000020000000103000000010000000500000000000000000000000000000000000000000000"
                                 "00000024400000000000000000000000000000244000000000000024400000000000000000000000000"
                                 "00024400000000000000000000000000000000001030000000100000005000000000000000000004000"
                                 "00000000000040000000000000004000000000000014400000000000001440000000000000144000000"
                                 "00000001440000000000000004000000000000000400000000000000040");
  compare_3d_to_2d_result(ObGeoType::MULTIPOLYGONZ, wkt_3d, wkb_2d);
  // geometrycollection
  wkt_3d = ObString::make_string("GEOMETRYCOLLECTION Z (POINT Z(1 1 1), LINESTRINGZ (0 0 2, 1 1 3), POLYGON Z((0 0 1,10 0 2 ,10 10 2,0 10 2,0 0 1),(2 2 5 ,2 5 4,5 5 3,5 2 3,2 2 5)), GEOMETRYCOLLECTIONZ(MULTIPOINTZ((0 0 0), (2 0 1)), MULTILINESTRINGZ((0 0 1, 2 0 2), (1 1 3, 2 2 4)), MULTIPOLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3),(2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))))");
  wkb_2d = ObString::make_string("0107000000040000000101000000000000000000F03F000000000000F03F0102000000020000000000"
                                "0000000000000000000000000000000000000000F03F000000000000F03F01030000000200000005000"
                                "00000000000000000000000000000000000000000000000244000000000000000000000000000002440"
                                "00000000000024400000000000000000000000000000244000000000000000000000000000000000050"
                                "00000000000000000004000000000000000400000000000000040000000000000144000000000000014"
                                "40000000000000144000000000000014400000000000000040000000000000004000000000000000400"
                                "10700000003000000010400000002000000010100000000000000000000000000000000000000010100"
                                "00000000000000000040000000000000000001050000000200000001020000000200000000000000000"
                                "00000000000000000000000000000000000400000000000000000010200000002000000000000000000"
                                "F03F000000000000F03F000000000000004000000000000000400106000000010000000103000000020"
                                "00000050000000000000000000000000000000000000000000000000024400000000000000000000000"
                                "00000024400000000000002440000000000000000000000000000024400000000000000000000000000"
                                "00000000500000000000000000000400000000000000040000000000000004000000000000014400000"
                                "00000000144000000000000014400000000000001440000000000000004000000000000000400000000"
                                "000000040");
  compare_3d_to_2d_result(ObGeoType::GEOMETRYCOLLECTIONZ, wkt_3d, wkb_2d);

}

TEST_F(TestGeo3D, test_to_3d_wkt)
{
  ObString wkt_3d = ObString::make_string("POINT Z (1 1 1)");
  compare_to_3d_wkt_result(ObGeoType::POINTZ, wkt_3d, wkt_3d);

  wkt_3d = ObString::make_string("LINESTRING Z (1 1 1,1 2 1)");
  compare_to_3d_wkt_result(ObGeoType::LINESTRINGZ, wkt_3d, wkt_3d);

  wkt_3d = ObString::make_string("POLYGON Z ((0 0 1,10 0 2,10 10 2,0 10 2,0 0 1),(2 2 5,2 5 4,5 5 3,5 2 3,2 2 5))");
  compare_to_3d_wkt_result(ObGeoType::POLYGONZ, wkt_3d, wkt_3d);

  wkt_3d = ObString::make_string("MULTIPOINT Z ((0 0 0),(2 0 1))");
  compare_to_3d_wkt_result(ObGeoType::MULTIPOINTZ, wkt_3d, wkt_3d);

  wkt_3d = ObString::make_string("MULTILINESTRING Z ((0 0 1,2 0 2),(1 1 3,2 2 4))");
  compare_to_3d_wkt_result(ObGeoType::MULTILINESTRINGZ, wkt_3d, wkt_3d);

  wkt_3d = ObString::make_string("MULTIPOLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3)),((2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))");
  compare_to_3d_wkt_result(ObGeoType::MULTIPOLYGONZ, wkt_3d, wkt_3d);


  wkt_3d = ObString::make_string("GEOMETRYCOLLECTION Z (POINT Z (1 1 1),LINESTRING Z (0 0 2,1 1 3),POLYGON Z ((0 0 1"
                                ",10 0 2,10 10 2,0 10 2,0 0 1),(2 2 5,2 5 4,5 5 3,5 2 3,2 2 5)),GEOMETRYCOLLECTION Z"
                                " (MULTIPOINT Z ((0 0 0),(2 0 1)),MULTILINESTRING Z ((0 0 1,2 0 2),(1 1 3,2 2 4)),MULTIP"
                                "OLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3),(2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))))");
  compare_to_3d_wkt_result(ObGeoType::GEOMETRYCOLLECTIONZ, wkt_3d, wkt_3d);
  wkt_3d = ObString::make_string("GEOMETRYCOLLECTION Z ()");
  ObString wkt_3d_res = ObString::make_string("GEOMETRYCOLLECTION Z EMPTY");
  compare_to_3d_wkt_result(ObGeoType::GEOMETRYCOLLECTIONZ, wkt_3d, wkt_3d_res);
  wkt_3d = ObString::make_string("GEOMETRYCOLLECTION Z EMPTY");
  compare_to_3d_wkt_result(ObGeoType::GEOMETRYCOLLECTIONZ, wkt_3d, wkt_3d);

}

TEST_F(TestGeo3D, test_check_wkb)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObStringBuffer buf(&allocator);
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  ObGeometry3D geo_3d;
  // linestring
  ASSERT_EQ(OB_SUCCESS, append_val<uint8_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, static_cast<uint32_t>(ObGeoType::LINESTRINGZ), bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 1, 1, bo));
  geo_3d.set_data(buf.string());
  ASSERT_NE(OB_SUCCESS, geo_3d.check_wkb_valid());

  // linestring
  ASSERT_EQ(OB_SUCCESS, append_val<uint8_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, static_cast<uint32_t>(ObGeoType::LINESTRINGZ), bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, 2, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 1, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 2, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 3, 1, bo));
  geo_3d.set_data(buf.string());
  ASSERT_NE(OB_SUCCESS, geo_3d.check_wkb_valid());

  // polygon
  buf.reset();
  ASSERT_EQ(OB_SUCCESS, append_val<uint8_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, static_cast<uint32_t>(ObGeoType::POLYGONZ), bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, 3, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 1, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 2, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 1, 3, 1, bo));
  geo_3d.set_data(buf.string());
  ASSERT_NE(OB_SUCCESS, geo_3d.check_wkb_valid());
}

TEST_F(TestGeo3D, test_check_wkb_1)
{
  // point
  ObString wkt_2d = ObString::make_string("POINT(1 1)");
  check_wkb_is_valid_3d(wkt_2d, false);
  ObString wkt_3d = ObString::make_string("POINT Z (1 1 1)");
  check_wkb_is_valid_3d(wkt_3d, true);
  // linestring
  wkt_2d = ObString::make_string("LINESTRING (1 1,1 2)");
  check_wkb_is_valid_3d(wkt_2d, false);
  wkt_3d = ObString::make_string("LINESTRING Z (1 1 1,1 2 1)");
  check_wkb_is_valid_3d(wkt_3d, true);
  // polygon
  wkt_3d = ObString::make_string("POLYGONZ((0 0 1,10 0 2 ,10 10 2,0 10 2,0 0 1))");
  check_wkb_is_valid_3d(wkt_3d, true);
  wkt_2d = ObString::make_string("POLYGON((0 0,10 0 ,10 10,0 10,0 0))");
  check_wkb_is_valid_3d(wkt_2d, false);
  // multi point
  wkt_3d = ObString::make_string("MULTIPOINTZ((0 0 0), (2 0 1))");
  check_wkb_is_valid_3d(wkt_3d, true);
  wkt_2d = ObString::make_string("MULTIPOINT((0 0), (2 0))");
  check_wkb_is_valid_3d(wkt_2d, false);
  // multi linestring
  wkt_3d = ObString::make_string("MULTILINESTRING Z ((0 0 1,2 0 2),(1 1 3,2 2 4))");
  check_wkb_is_valid_3d(wkt_3d, true);
  wkt_2d = ObString::make_string("MULTILINESTRING((0 0,2 0),(1 1,2 2))");
  check_wkb_is_valid_3d(wkt_2d, false);
  // multi polygon
  wkt_3d = ObString::make_string("MULTIPOLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3)),((2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))");
  check_wkb_is_valid_3d(wkt_3d, true);
  wkt_2d = ObString::make_string("MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((2 2,2 5,5 5,5 2,2 2)))");
  check_wkb_is_valid_3d(wkt_2d, false);
  // collectionz
  wkt_3d = ObString::make_string("GEOMETRYCOLLECTION Z (POINT Z (1 1 1),LINESTRING Z (0 0 2,1 1 3),POLYGON Z ((0 0 1"
                                ",10 0 2,10 10 2,0 10 2,0 0 1),(2 2 5,2 5 4,5 5 3,5 2 3,2 2 5)),GEOMETRYCOLLECTION Z"
                                " (MULTIPOINT Z ((0 0 0),(2 0 1)),MULTILINESTRING Z ((0 0 1,2 0 2),(1 1 3,2 2 4)),MULTIP"
                                "OLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3),(2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))))");
  check_wkb_is_valid_3d(wkt_3d, true);
  wkt_2d = ObString::make_string("GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1),POLYGON((0 0"
                                ",10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2)),GEOMETRYCOLLECTION"
                                " (MULTIPOINT(0 0,2 0),MULTILINESTRING((0 0,2 0),(1 1,2 2)),MULTIP"
                                "OLYGON(((0 0,10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2)))))");
  check_wkb_is_valid_3d(wkt_2d, false);
}

TEST_F(TestGeo3D, test_reserve_coordinate)
{
  ObString wkt_3d = ObString::make_string("POINT Z (1 2 1)");
  check_reserver_coordinate(wkt_3d);
  wkt_3d = ObString::make_string("LINESTRING Z (1 2 1,3 4 1)");
  check_reserver_coordinate(wkt_3d);
  wkt_3d = ObString::make_string("POLYGONZ((0 1 1,10 0 2 ,10 5 2,0 10 2,0 1 1))");
  check_reserver_coordinate(wkt_3d);
  wkt_3d = ObString::make_string("MULTIPOINTZ((0 1 0), (2 0 1))");
  check_reserver_coordinate(wkt_3d);
  wkt_3d = ObString::make_string("MULTILINESTRING Z ((0 1 1,2 0 2),(1 2 3,2 3 4))");
  check_reserver_coordinate(wkt_3d);
  wkt_3d = ObString::make_string("MULTIPOLYGON Z (((0 1 3,10 0 3,10 5 3,0 10 3,0 1 3)),((2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))");
  check_reserver_coordinate(wkt_3d);
  wkt_3d = ObString::make_string("GEOMETRYCOLLECTION Z (POINT Z (1 2 1),LINESTRING Z (0 2 2,1 3 3),POLYGON Z ((0 1 1"
                                  ",10 0 2,10 5 2,0 10 2,0 1 1),(2 1 5,2 5 4,5 4 3,5 2 3,2 1 5)),GEOMETRYCOLLECTION Z"
                                  " (MULTIPOINT Z (0 1 0,2 0 1),MULTILINESTRING Z ((0 1 1,2 0 2),(1 2 3,2 3 4)),MULTIP"
                                  "OLYGON Z (((0 1 3,10 0 3,10 5 3,0 10 3,0 1 3),(2 1 3,2 5 3,5 10 3,5 2 3,2 1 3)))))");
  check_reserver_coordinate(wkt_3d);
}

TEST_F(TestGeo3D, test_coordinate_range)
{
  const ObSrsItem *srs_item = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, 4326, srs_item));
  ObStringBuffer buf(&allocator);
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  ObGeometry3D geo_3d;
  // point valid
  ASSERT_EQ(OB_SUCCESS, append_val<uint8_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, static_cast<uint32_t>(ObGeoType::POINTZ), bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 180 * srs_item->angular_unit(), 89 * srs_item->angular_unit(), 1, bo));
  geo_3d.set_data(buf.string());
  ObGeoCoordRangeResult result;
  ASSERT_EQ(OB_SUCCESS, geo_3d.check_3d_coordinate_range(srs_item, true, result));
  ASSERT_EQ(result.is_lati_out_range_, false);
  ASSERT_EQ(result.is_long_out_range_, false);
  // point invalid
  buf.reset();
  ASSERT_EQ(OB_SUCCESS, append_val<uint8_t>(buf, 1, bo));
  ASSERT_EQ(OB_SUCCESS, append_val<uint32_t>(buf, static_cast<uint32_t>(ObGeoType::POINTZ), bo));
  ASSERT_EQ(OB_SUCCESS, append_pointz(buf, 181 * srs_item->angular_unit(), 89 * srs_item->angular_unit(), 1, bo));
  geo_3d.set_data(buf.string());
  ObGeoCoordRangeResult result1;
  ASSERT_EQ(OB_SUCCESS, geo_3d.check_3d_coordinate_range(srs_item, true, result));
  ASSERT_EQ(result.is_lati_out_range_, false);
  ASSERT_EQ(result.is_long_out_range_, true);
  ASSERT_TRUE(std::abs(result.value_out_range_ - 181) < 0.001);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_geo_3d.log");
  OB_LOGGER.set_file_name("test_geo_3d.log");
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}