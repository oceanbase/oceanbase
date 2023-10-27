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
#define protected public
#include "lib/geo/ob_srs_info.h"
#include "lib/geo/ob_srs_wkt_parser.h"
#include "observer/omt/ob_tenant_srs.h"
#undef protected
#undef private

namespace oceanbase {

using namespace omt;
namespace common {

int mock_create_srs_item(ObIAllocator &allocator, ObSpatialReferenceSystemBase *srs_info, ObSrsItem *&srs_item) {
  int ret = OB_SUCCESS;
  ObSrsItem *tmp_srs_item = NULL;
  if (OB_ISNULL(srs_info)) {
    ret = OB_ERR_NULL_VALUE;
  } else if (FALSE_IT(tmp_srs_item = OB_NEWx(ObSrsItem, (&allocator), srs_info))){
  } else if (OB_ISNULL(tmp_srs_item)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    printf("fail to alloc memory for srs item, ret = %d", ret);
  } else {
    srs_item = tmp_srs_item;
  }
  return ret;
}

class TestGeoSrsParser : public ::testing::Test {
public:
  TestGeoSrsParser()
  {}
  ~TestGeoSrsParser()
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
  DISALLOW_COPY_AND_ASSIGN(TestGeoSrsParser);

};

TEST_F(TestGeoSrsParser, parse_geog_srs_def)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObString srs3819_def = "GEOGCS[\"HD1909\",\
                            DATUM[\"Hungarian Datum 1909\",\
                              SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],\
                              TOWGS84[595.48,121.69,515.35,4.115,-2.9383,0.853,-3.408],\
                              AUTHORITY[\"EPSG\",\"1024\"]],\
                            PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                            UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                            AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                            AUTHORITY[\"EPSG\",\"3819\"]]";
  ObGeographicRs srs3819;
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_geog_srs_wkt(allocator, srs3819_def, srs3819));
  ASSERT_TRUE(srs3819.rs_name == "HD1909");
  ASSERT_TRUE(srs3819.datum_info.name == "Hungarian Datum 1909");
  ASSERT_TRUE(srs3819.datum_info.spheroid.name == "Bessel 1841");
  ASSERT_EQ(srs3819.datum_info.spheroid.semi_major_axis, 6377397.155);
  ASSERT_EQ(srs3819.datum_info.spheroid.inverse_flattening, 299.1528128);
  ASSERT_TRUE(srs3819.datum_info.spheroid.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3819.datum_info.spheroid.authority.org_code == "7004");
  double srs_3819_towgs84[7] = {595.48, 121.69, 515.35, 4.115, -2.9383, 0.853, -3.408};
  for (int i = 0; i < 7; i++) {
    ASSERT_EQ(srs3819.datum_info.towgs84.value[i], srs_3819_towgs84[i]);
  }
  ASSERT_TRUE(srs3819.datum_info.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3819.datum_info.authority.org_code == "1024");
  ASSERT_TRUE(srs3819.primem.name == "Greenwich");
  ASSERT_EQ(srs3819.primem.longtitude, 0);
  ASSERT_TRUE(srs3819.primem.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3819.primem.authority.org_code == "8901");
  ASSERT_TRUE(srs3819.unit.type == "degree");
  ASSERT_EQ(srs3819.unit.conversion_factor, 0.017453292519943278);
  ASSERT_TRUE(srs3819.unit.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3819.unit.authority.org_code == "9122");
  ASSERT_TRUE(srs3819.axis.x.name == "Lat");
  ASSERT_TRUE(srs3819.axis.x.direction == ObAxisDirection::NORTH);
  ASSERT_TRUE(srs3819.axis.y.name == "Lon");
  ASSERT_TRUE(srs3819.axis.y.direction == ObAxisDirection::EAST);
  ASSERT_TRUE(srs3819.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3819.authority.org_code == "3819");

  ObString srs3824_def = "GEOGCS[\"TWD97\",\
                            DATUM[\"Taiwan Datum 1997\",\
                              SPHEROID[\"GRS 1980\",6378137,298.257222101,AUTHORITY[\"EPSG\",\"7019\"]],\
                              TOWGS84[0,0,0,0,0,0,0],\
                              AUTHORITY[\"EPSG\",\"1026\"]],\
                            PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                            UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                            AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                            AUTHORITY[\"EPSG\",\"3824\"]]";

  ObGeographicRs srs3824;
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_geog_srs_wkt(allocator, srs3824_def, srs3824));
  ASSERT_TRUE(srs3824.rs_name == "TWD97");
  ASSERT_TRUE(srs3824.datum_info.name == "Taiwan Datum 1997");
  ASSERT_TRUE(srs3824.datum_info.spheroid.name == "GRS 1980");
  ASSERT_EQ(srs3824.datum_info.spheroid.semi_major_axis, 6378137);
  ASSERT_EQ(srs3824.datum_info.spheroid.inverse_flattening, 298.257222101);
  ASSERT_TRUE(srs3824.datum_info.spheroid.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3824.datum_info.spheroid.authority.org_code == "7019");
  double srs3824_towgs84[7] = {0, 0, 0, 0, 0, 0, 0};
  for (int i = 0; i < 7; i++) {
    ASSERT_EQ(srs3824.datum_info.towgs84.value[i], srs3824_towgs84[i]);
  }
  ASSERT_TRUE(srs3824.datum_info.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3824.datum_info.authority.org_code == "1026");
  ASSERT_TRUE(srs3824.primem.name == "Greenwich");
  ASSERT_EQ(srs3824.primem.longtitude, 0);
  ASSERT_TRUE(srs3824.primem.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3824.primem.authority.org_code == "8901");
  ASSERT_TRUE(srs3824.unit.type == "degree");
  ASSERT_EQ(srs3824.unit.conversion_factor, 0.017453292519943278);
  ASSERT_TRUE(srs3824.unit.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3824.unit.authority.org_code == "9122");
  ASSERT_TRUE(srs3824.axis.x.name == "Lat");
  ASSERT_TRUE(srs3824.axis.x.direction == ObAxisDirection::NORTH);
  ASSERT_TRUE(srs3824.axis.y.name == "Lon");
  ASSERT_TRUE(srs3824.axis.y.direction == ObAxisDirection::EAST);
  ASSERT_TRUE(srs3824.authority.org_name == "EPSG");
  ASSERT_TRUE(srs3824.authority.org_code == "3824");
}


TEST_F(TestGeoSrsParser, parse_proj_srs_def)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObString srs2027_def = "PROJCS[\"NAD27(76) / UTM zone 15N\",\
                            GEOGCS[\"NAD27(76)\",\
                              DATUM[\"North American Datum 1927 (1976)\",\
                                SPHEROID[\"Clarke 1866\",6378206.4,294.9786982138982,AUTHORITY[\"EPSG\",\"7008\"]],\
                                AUTHORITY[\"EPSG\",\"6608\"]],\
                              PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                              UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                              AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                              AUTHORITY[\"EPSG\",\"4608\"]],\
                            PROJECTION[\"Transverse Mercator\",AUTHORITY[\"EPSG\",\"9807\"]],\
                            PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],\
                            PARAMETER[\"Longitude of natural origin\",-93,AUTHORITY[\"EPSG\",\"8802\"]],\
                            PARAMETER[\"Scale factor at natural origin\",0.9996,AUTHORITY[\"EPSG\",\"8805\"]],\
                            PARAMETER[\"False easting\",500000,AUTHORITY[\"EPSG\",\"8806\"]],\
                            PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],\
                            UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\
                            AXIS[\"E\",EAST],AXIS[\"N\",NORTH],\
                            AUTHORITY[\"EPSG\",\"2027\"]]";
  ObProjectionRs srs2027;
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_proj_srs_wkt(allocator, srs2027_def, srs2027));
  ASSERT_TRUE(srs2027.rs_name == "NAD27(76) / UTM zone 15N");
  ASSERT_TRUE(srs2027.projected_rs.rs_name == "NAD27(76)");
  ASSERT_TRUE(srs2027.projected_rs.datum_info.name == "North American Datum 1927 (1976)");
  ASSERT_TRUE(srs2027.projected_rs.datum_info.spheroid.name == "Clarke 1866");
  ASSERT_EQ(srs2027.projected_rs.datum_info.spheroid.semi_major_axis, 6378206.4);
  ASSERT_EQ(srs2027.projected_rs.datum_info.spheroid.inverse_flattening, 294.9786982138982);
  ASSERT_TRUE(srs2027.projected_rs.datum_info.spheroid.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.projected_rs.datum_info.spheroid.authority.org_code == "7008");
  double srs2027_towgs84[7] = {0, 0, 0, 0, 0, 0, 0};
  for (int i = 0; i < 7; i++) {
    ASSERT_EQ(srs2027.projected_rs.datum_info.towgs84.value[i], srs2027_towgs84[i]);
  }
  ASSERT_TRUE(srs2027.projected_rs.datum_info.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.projected_rs.datum_info.authority.org_code == "6608");
  ASSERT_TRUE(srs2027.projected_rs.primem.name == "Greenwich");
  ASSERT_EQ(srs2027.projected_rs.primem.longtitude, 0);
  ASSERT_TRUE(srs2027.projected_rs.primem.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.projected_rs.primem.authority.org_code == "8901");
  ASSERT_TRUE(srs2027.projected_rs.unit.type == "degree");
  ASSERT_EQ(srs2027.projected_rs.unit.conversion_factor, 0.017453292519943278);
  ASSERT_TRUE(srs2027.projected_rs.unit.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.projected_rs.unit.authority.org_code == "9122");
  ASSERT_TRUE(srs2027.projected_rs.axis.x.name == "Lat");
  ASSERT_TRUE(srs2027.projected_rs.axis.x.direction == ObAxisDirection::NORTH);
  ASSERT_TRUE(srs2027.projected_rs.axis.y.name == "Lon");
  ASSERT_TRUE(srs2027.projected_rs.axis.y.direction == ObAxisDirection::EAST);
  ASSERT_TRUE(srs2027.projected_rs.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.projected_rs.authority.org_code == "4608");
  ASSERT_TRUE(srs2027.projection.name == "Transverse Mercator");
  ASSERT_TRUE(srs2027.projection.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.projection.authority.org_code == "9807");
  ASSERT_EQ(srs2027.proj_params.vals.size(), 5);
  ASSERT_TRUE(srs2027.proj_params.vals[0].name == "Latitude of natural origin");
  ASSERT_EQ(srs2027.proj_params.vals[0].value, 0);
  ASSERT_TRUE(srs2027.proj_params.vals[0].authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.proj_params.vals[0].authority.org_code == "8801");
  ASSERT_TRUE(srs2027.proj_params.vals[1].name == "Longitude of natural origin");
  ASSERT_EQ(srs2027.proj_params.vals[1].value, -93);
  ASSERT_TRUE(srs2027.proj_params.vals[1].authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.proj_params.vals[1].authority.org_code == "8802");
  ASSERT_TRUE(srs2027.proj_params.vals[2].name == "Scale factor at natural origin");
  ASSERT_EQ(srs2027.proj_params.vals[2].value, 0.9996);
  ASSERT_TRUE(srs2027.proj_params.vals[2].authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.proj_params.vals[2].authority.org_code == "8805");
  ASSERT_TRUE(srs2027.proj_params.vals[3].name == "False easting");
  ASSERT_EQ(srs2027.proj_params.vals[3].value, 500000);
  ASSERT_TRUE(srs2027.proj_params.vals[3].authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.proj_params.vals[3].authority.org_code == "8806");
  ASSERT_TRUE(srs2027.proj_params.vals[4].name == "False northing");
  ASSERT_EQ(srs2027.proj_params.vals[4].value, 0);
  ASSERT_TRUE(srs2027.proj_params.vals[4].authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.proj_params.vals[4].authority.org_code == "8807");
  ASSERT_TRUE(srs2027.unit.type == "metre");
  ASSERT_EQ(srs2027.unit.conversion_factor, 1);
  ASSERT_TRUE(srs2027.unit.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.unit.authority.org_code == "9001");
  ASSERT_TRUE(srs2027.axis.x.name == "E");
  ASSERT_TRUE(srs2027.axis.x.direction == ObAxisDirection::EAST);
  ASSERT_TRUE(srs2027.axis.y.name == "N");
  ASSERT_TRUE(srs2027.axis.y.direction == ObAxisDirection::NORTH);
  ASSERT_TRUE(srs2027.authority.org_name == "EPSG");
  ASSERT_TRUE(srs2027.authority.org_code == "2027");

  ObString srs4768_def = "PROJCS[\"New Beijing / 3-degree Gauss-Kruger zone 32\",\
                            GEOGCS[\"New Beijing\",\
                              DATUM[\"New Beijing\",\
                                SPHEROID[\"Krassowsky 1940\",6378245,298.3,AUTHORITY[\"EPSG\",\"7024\"]],\
                                AUTHORITY[\"EPSG\",\"1045\"]],\
                              PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                              UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                              AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                              AUTHORITY[\"EPSG\",\"4555\"]],\
                            PROJECTION[\"Transverse Mercator\",AUTHORITY[\"EPSG\",\"9807\"]],\
                            PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],\
                            PARAMETeR[\"Longitude of natural origin\",96,AUTHORITY[\"EPSG\",\"8802\"]],\
                            PARAMETER[\"Scale factor at natural origin\",1,AUTHORITY[\"EPSG\",\"8805\"]],\
                            PARAMETER[\"False easting\",32500000,AUTHORITY[\"EPSG\",\"8806\"]],\
                            PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],\
                            UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\
                            AXIS[\"X\",NORTH],AXIS[\"Y\",EAST],\
                            AUTHORITY[\"EPSG\",\"4768\"]]";
  ObProjectionRs srs4768;
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_proj_srs_wkt(allocator, srs4768_def, srs4768));
  ASSERT_TRUE(srs4768.rs_name == "New Beijing / 3-degree Gauss-Kruger zone 32");
  ASSERT_TRUE(srs4768.projected_rs.rs_name == "New Beijing");
  ASSERT_TRUE(srs4768.projected_rs.datum_info.name == "New Beijing");
  ASSERT_TRUE(srs4768.projected_rs.datum_info.spheroid.name == "Krassowsky 1940");
  ASSERT_EQ(srs4768.projected_rs.datum_info.spheroid.semi_major_axis, 6378245);
  ASSERT_EQ(srs4768.projected_rs.datum_info.spheroid.inverse_flattening, 298.3);
  ASSERT_TRUE(srs4768.projected_rs.datum_info.spheroid.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.projected_rs.datum_info.spheroid.authority.org_code == "7024");
  double srs4768_towgs84[7] = {0, 0, 0, 0, 0, 0, 0};
  for (int i = 0; i < 7; i++) {
    ASSERT_EQ(srs4768.projected_rs.datum_info.towgs84.value[i], srs4768_towgs84[i]);
  }
  ASSERT_TRUE(srs4768.projected_rs.datum_info.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.projected_rs.datum_info.authority.org_code == "1045");
  ASSERT_TRUE(srs4768.projected_rs.primem.name == "Greenwich");
  ASSERT_EQ(srs4768.projected_rs.primem.longtitude, 0);
  ASSERT_TRUE(srs4768.projected_rs.primem.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.projected_rs.primem.authority.org_code == "8901");
  ASSERT_TRUE(srs4768.projected_rs.unit.type == "degree");
  ASSERT_EQ(srs4768.projected_rs.unit.conversion_factor, 0.017453292519943278);
  ASSERT_TRUE(srs4768.projected_rs.unit.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.projected_rs.unit.authority.org_code == "9122");
  ASSERT_TRUE(srs4768.projected_rs.axis.x.name == "Lat");
  ASSERT_TRUE(srs4768.projected_rs.axis.x.direction == ObAxisDirection::NORTH);
  ASSERT_TRUE(srs4768.projected_rs.axis.y.name == "Lon");
  ASSERT_TRUE(srs4768.projected_rs.axis.y.direction == ObAxisDirection::EAST);
  ASSERT_TRUE(srs4768.projected_rs.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.projected_rs.authority.org_code == "4555");
  ASSERT_TRUE(srs4768.projection.name == "Transverse Mercator");
  ASSERT_TRUE(srs4768.projection.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.projection.authority.org_code == "9807");
  ASSERT_EQ(srs4768.proj_params.vals.size(), 5);
  ASSERT_TRUE(srs4768.proj_params.vals[0].name == "Latitude of natural origin");
  ASSERT_EQ(srs4768.proj_params.vals[0].value, 0);
  ASSERT_TRUE(srs4768.proj_params.vals[0].authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.proj_params.vals[0].authority.org_code == "8801");
  ASSERT_TRUE(srs4768.proj_params.vals[1].name == "Longitude of natural origin");
  ASSERT_EQ(srs4768.proj_params.vals[1].value, 96);
  ASSERT_TRUE(srs4768.proj_params.vals[1].authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.proj_params.vals[1].authority.org_code == "8802");
  ASSERT_TRUE(srs4768.proj_params.vals[2].name == "Scale factor at natural origin");
  ASSERT_EQ(srs4768.proj_params.vals[2].value, 1);
  ASSERT_TRUE(srs4768.proj_params.vals[2].authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.proj_params.vals[2].authority.org_code == "8805");
  ASSERT_TRUE(srs4768.proj_params.vals[3].name == "False easting");
  ASSERT_EQ(srs4768.proj_params.vals[3].value, 32500000);
  ASSERT_TRUE(srs4768.proj_params.vals[3].authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.proj_params.vals[3].authority.org_code == "8806");
  ASSERT_TRUE(srs4768.proj_params.vals[4].name == "False northing");
  ASSERT_EQ(srs4768.proj_params.vals[4].value, 0);
  ASSERT_TRUE(srs4768.proj_params.vals[4].authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.proj_params.vals[4].authority.org_code == "8807");
  ASSERT_TRUE(srs4768.unit.type == "metre");
  ASSERT_EQ(srs4768.unit.conversion_factor, 1);
  ASSERT_TRUE(srs4768.unit.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.unit.authority.org_code == "9001");
  ASSERT_TRUE(srs4768.axis.x.name == "X");
  ASSERT_TRUE(srs4768.axis.x.direction == ObAxisDirection::NORTH);
  ASSERT_TRUE(srs4768.axis.y.name == "Y");
  ASSERT_TRUE(srs4768.axis.y.direction == ObAxisDirection::EAST);
  ASSERT_TRUE(srs4768.authority.org_name == "EPSG");
  ASSERT_TRUE(srs4768.authority.org_code == "4768");
}

TEST_F(TestGeoSrsParser, parse_wrong_srs_def)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  // more than 7 towgs84 pram
  ObString w_srs3819_def_1 = "GEOGCS[\"HD1909\",\
                            DATUM[\"Hungarian Datum 1909\",\
                              SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],\
                              TOWGS84[595.48,121.69,515.35,4.115,-2.9383,0.853,-3.408, -100.1],\
                              AUTHORITY[\"EPSG\",\"1024\"]],\
                            PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                            UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                            AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                            AUTHORITY[\"EPSG\",\"3819\"]]";

  // more than one right bracket
  ObString w_srs3819_def_2 = "GEOGCS[\"HD1909\",\
                            DATUM[\"Hungarian Datum 1909\",\
                              SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],\
                              TOWGS84[595.48,121.69,515.35,4.115,-2.9383,0.853,-3.408, -100.1],\
                              AUTHORITY[\"EPSG\",\"1024\"]],\
                            PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                            UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                            AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                            AUTHORITY[\"EPSG\",\"3819\"]]]]";

  // without prime
  ObString w_srs3819_def_3 = "GEOGCS[\"HD1909\",\
                            DATUM[\"Hungarian Datum 1909\",\
                              SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],\
                              TOWGS84[595.48,121.69,515.35,4.115,-2.9383,0.853,-3.408, -100.1],\
                              AUTHORITY[\"EPSG\",\"1024\"]],\
                            UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                            AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                            AUTHORITY[\"EPSG\",\"3819\"]]";

  // only axis
  ObString w_srs3819_def_4 = "GEOGCS[\"HD1909\",\
                            DATUM[\"Hungarian Datum 1909\",\
                              SPHEROID[\"Bessel 1841\",6377397.155,299.1528128,AUTHORITY[\"EPSG\",\"7004\"]],\
                              TOWGS84[595.48,121.69,515.35,4.115,-2.9383,0.853,-3.408, -100.1],\
                              AUTHORITY[\"EPSG\",\"1024\"]],\
                            UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                            AXIS[\"Lat\",NORTH]\
                            AUTHORITY[\"EPSG\",\"3819\"]]";
  ObGeographicRs w_srs3819;
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_geog_srs_wkt(allocator, w_srs3819_def_1, w_srs3819));
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_geog_srs_wkt(allocator, w_srs3819_def_2, w_srs3819));
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_geog_srs_wkt(allocator, w_srs3819_def_3, w_srs3819));
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_geog_srs_wkt(allocator, w_srs3819_def_4, w_srs3819));

  // without projection
  ObString w_srs4768_def_1 = "PROJCS[\"New Beijing / 3-degree Gauss-Kruger zone 32\",\
                            GEOGCS[\"New Beijing\",\
                              DATUM[\"New Beijing\",\
                                SPHEROID[\"Krassowsky 1940\",6378245,298.3,AUTHORITY[\"EPSG\",\"7024\"]],\
                                AUTHORITY[\"EPSG\",\"1045\"]],\
                              PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                              UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                              AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                              AUTHORITY[\"EPSG\",\"4555\"]],\
                            PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],\
                            PARAMETER[\"Longitude of natural origin\",96,AUTHORITY[\"EPSG\",\"8802\"]],\
                            PARAMETER[\"Scale factor at natural origin\",1,AUTHORITY[\"EPSG\",\"8805\"]],\
                            PARAMETER[\"False easting\",32500000,AUTHORITY[\"EPSG\",\"8806\"]],\
                            PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],\
                            UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\
                            AXIS[\"X\",NORTH],AXIS[\"Y\",EAST],\
                            AUTHORITY[\"EPSG\",\"4768\"]]";

  // wrong axis direction
  ObString w_srs4768_def_2 = "PROJCS[\"New Beijing / 3-degree Gauss-Kruger zone 32\",\
                            GEOGCS[\"New Beijing\",\
                              DATUM[\"New Beijing\",\
                                SPHEROID[\"Krassowsky 1940\",6378245,298.3,AUTHORITY[\"EPSG\",\"7024\"]],\
                                AUTHORITY[\"EPSG\",\"1045\"]],\
                              PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                              UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                              AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                              AUTHORITY[\"EPSG\",\"4555\"]],\
                            PROJECTION[\"Transverse Mercator\",AUTHORITY[\"EPSG\",\"9807\"]],\
                            PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],\
                            PARAMETER[\"Longitude of natural origin\",96,AUTHORITY[\"EPSG\",\"8802\"]],\
                            PARAMETER[\"Scale factor at natural origin\",1,AUTHORITY[\"EPSG\",\"8805\"]],\
                            PARAMETER[\"False easting\",32500000,AUTHORITY[\"EPSG\",\"8806\"]],\
                            PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],\
                            UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\
                            AXIS[\"X\",HELLO],AXIS[\"Y\",EAST],\
                            AUTHORITY[\"EPSG\",\"4768\"]]";

  // without geography
  ObString w_srs4768_def_3 = "PROJCS[\"New Beijing / 3-degree Gauss-Kruger zone 32\",\
                            PROJECTION[\"Transverse Mercator\",AUTHORITY[\"EPSG\",\"9807\"]],\
                            PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],\
                            PARAMETER[\"Longitude of natural origin\",96,AUTHORITY[\"EPSG\",\"8802\"]],\
                            PARAMETER[\"Scale factor at natural origin\",1,AUTHORITY[\"EPSG\",\"8805\"]],\
                            PARAMETER[\"False easting\",32500000,AUTHORITY[\"EPSG\",\"8806\"]],\
                            PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],\
                            UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\
                            AXIS[\"X\",NORTH],AXIS[\"Y\",EAST],\
                            AUTHORITY[\"EPSG\",\"4768\"]]";

  // orderless
  ObString w_srs4768_def_4 = "PROJCS[\"New Beijing / 3-degree Gauss-Kruger zone 32\",\
                            GEOGCS[\"New Beijing\",\
                              DATUM[\"New Beijing\",\
                                SPHEROID[\"Krassowsky 1940\",6378245,298.3,AUTHORITY[\"EPSG\",\"7024\"]],\
                                AUTHORITY[\"EPSG\",\"1045\"]],\
                              PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\
                              UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],\
                              AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],\
                              AUTHORITY[\"EPSG\",\"4555\"]],\
                            PROJECTION[\"Transverse Mercator\",AUTHORITY[\"EPSG\",\"9807\"]],\
                            UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\
                            PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],\
                            PARAMETER[\"Longitude of natural origin\",96,AUTHORITY[\"EPSG\",\"8802\"]],\
                            PARAMETER[\"Scale factor at natural origin\",1,AUTHORITY[\"EPSG\",\"8805\"]],\
                            PARAMETER[\"False easting\",32500000,AUTHORITY[\"EPSG\",\"8806\"]],\
                            PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],\
                            AXIS[\"X\",NORTH],AXIS[\"Y\",EAST],\
                            AUTHORITY[\"EPSG\",\"4768\"]]";
  ObProjectionRs srs4768;
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_proj_srs_wkt(allocator, w_srs4768_def_1, srs4768));
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_proj_srs_wkt(allocator, w_srs4768_def_2, srs4768));
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_proj_srs_wkt(allocator, w_srs4768_def_3, srs4768));
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObSrsWktParser::parse_proj_srs_wkt(allocator, w_srs4768_def_4, srs4768));
}

TEST_F(TestGeoSrsParser, parse_srs4236_wkt)
{
  ObSpatialReferenceSystemBase *srs_info = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem *srs_item = NULL;

  ObString srs_def = R"(GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 4236, srs_def, srs_info));
  ASSERT_EQ(OB_SUCCESS, mock_create_srs_item(allocator, srs_info, srs_item));

  ASSERT_EQ(srs_item->is_wgs84(), true);
  ASSERT_EQ(srs_item->srs_type(), ObSrsType::GEOGRAPHIC_SRS);
  ASSERT_EQ(srs_item->is_lat_long_order(), true);
  ASSERT_EQ(srs_item->is_latitude_north(), true);
  ASSERT_EQ(srs_item->is_longtitude_east(), true);

  ASSERT_EQ(srs_item->prime_meridian(), 0);
  ASSERT_EQ(srs_item->linear_uint(), 1.0);
  ASSERT_EQ(srs_item->angular_unit(), 0.017453292519943278);
  ASSERT_EQ(srs_item->semi_major_axis(), 6378137);
  ASSERT_TRUE(std::abs(srs_item->semi_minor_axis() - 6356752.314245) < 0.001);

  double orig_long = 15;
  double orig_lat = 15;
  double to_res = 0.0;
  double from_res = 0.0;

  ASSERT_EQ(OB_SUCCESS, srs_item->longtitude_convert_to_radians(orig_long, to_res));
  ASSERT_EQ(OB_SUCCESS, srs_item->longtitude_convert_from_radians(to_res, from_res));
  ASSERT_EQ(from_res, orig_long);

  ASSERT_EQ(OB_SUCCESS, srs_item->latitude_convert_to_radians(orig_lat, to_res));
  ASSERT_EQ(OB_SUCCESS, srs_item->latitude_convert_from_radians(to_res, from_res));
  ASSERT_EQ(from_res, orig_lat);
}

TEST_F(TestGeoSrsParser, parse_srs7037_wkt)
{
  ObSpatialReferenceSystemBase *srs_info = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem *srs_item = NULL;

  ObString srs_def = R"--(GEOGCS["RGR92 (lon-lat)",DATUM["Reseau Geodesique de la Reunion 1992",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6627"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lon",EAST],AXIS["Lat",NORTH],AUTHORITY["EPSG","7037"]])--";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 7037, srs_def, srs_info));
  ASSERT_EQ(OB_SUCCESS, mock_create_srs_item(allocator, srs_info, srs_item));

  ASSERT_EQ(srs_item->is_wgs84(), false);
  ASSERT_EQ(srs_item->srs_type(), ObSrsType::GEOGRAPHIC_SRS);
  ASSERT_EQ(srs_item->is_lat_long_order(), false);
  ASSERT_EQ(srs_item->is_latitude_north(), true);
  ASSERT_EQ(srs_item->is_longtitude_east(), true);

  ASSERT_EQ(srs_item->prime_meridian(), 0);
  ASSERT_EQ(srs_item->linear_uint(), 1.0);
  ASSERT_EQ(srs_item->angular_unit(), 0.017453292519943278);
  ASSERT_EQ(srs_item->semi_major_axis(), 6378137);
  ASSERT_TRUE(std::abs(srs_item->semi_minor_axis() - 6356752.31424518) < 0.001);

  double orig_long = 15;
  double orig_lat = 15;
  double to_res = 0.0;
  double from_res = 0.0;

  ASSERT_EQ(OB_SUCCESS, srs_item->longtitude_convert_to_radians(orig_long, to_res));
  ASSERT_EQ(OB_SUCCESS, srs_item->longtitude_convert_from_radians(to_res, from_res));
  ASSERT_EQ(from_res, orig_long);

  ASSERT_EQ(OB_SUCCESS, srs_item->latitude_convert_to_radians(orig_lat, to_res));
  ASSERT_EQ(OB_SUCCESS, srs_item->latitude_convert_from_radians(to_res, from_res));
  ASSERT_EQ(from_res, orig_lat);
}

TEST_F(TestGeoSrsParser, parse_srs3857_wkt)
{
  ObSpatialReferenceSystemBase *srs_info = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem *srs_item = NULL;

  ObString srs_def = R"(PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Popular Visualisation Pseudo Mercator",AUTHORITY["EPSG","1024"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","3857"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 3857, srs_def, srs_info));
  ObProjectedSrs * proj_srs_info = dynamic_cast<ObProjectedSrs *>(srs_info);
  ASSERT_TRUE(proj_srs_info != NULL);
  ASSERT_EQ(proj_srs_info->get_projection_type(), ObProjectionType::POPULAR_VISUAL_PSEUDO_MERCATOR);
  ObPopularVisualPseudoMercatorSrs * vps_proj_srs_info = dynamic_cast<ObPopularVisualPseudoMercatorSrs *>(srs_info);
  ASSERT_TRUE(vps_proj_srs_info != NULL);
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(vps_proj_srs_info->simple_proj_prams_[i].value_, 0);
  }

  ASSERT_EQ(OB_SUCCESS, mock_create_srs_item(allocator, srs_info, srs_item));
  ASSERT_EQ(srs_item->is_wgs84(), true);
  ASSERT_EQ(srs_item->srs_type(), ObSrsType::PROJECTED_SRS);
  ASSERT_EQ(srs_item->is_lat_long_order(), false);
  ASSERT_EQ(srs_item->is_latitude_north(), true);
  ASSERT_EQ(srs_item->is_longtitude_east(), true);

  ASSERT_EQ(srs_item->prime_meridian(), 0);
  ASSERT_EQ(srs_item->linear_uint(), 1.0);
  ASSERT_EQ(srs_item->angular_unit(), 0.017453292519943278);
  ASSERT_EQ(srs_item->semi_major_axis(), 0.0);
  ASSERT_EQ(srs_item->semi_minor_axis(), 0.0);

  double input = 15;
  double res = 0.0;

  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->longtitude_convert_to_radians(input, res));
  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->longtitude_convert_from_radians(input, res));

  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->latitude_convert_to_radians(input, res));
  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->latitude_convert_from_radians(input, res));
}

TEST_F(TestGeoSrsParser, parse_srs32644_wkt)
{
  ObSpatialReferenceSystemBase *srs_info = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem *srs_item = NULL;
  ObString srs_def = R"(PROJCS["WGS 84 / UTM zone 44N",GEOGCS["WGS 84", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",81,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32644"]])";

  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, srs_def, srs_info));
  ObTransverseMercatorSrs * proj_srs_info = dynamic_cast<ObTransverseMercatorSrs *>(srs_info);
  ASSERT_TRUE(proj_srs_info != NULL);
  ASSERT_EQ(proj_srs_info->get_projection_type(), ObProjectionType::TRANSVERSE_MERCATOR);

  ASSERT_EQ(proj_srs_info->simple_proj_prams_[0].epsg_code_, 8801);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[1].epsg_code_, 8802);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[2].epsg_code_, 8805);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[3].epsg_code_, 8806);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[4].epsg_code_, 8807);

  ASSERT_EQ(proj_srs_info->simple_proj_prams_[0].value_, 0);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[1].value_, 81);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[2].value_, 0.9996);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[3].value_, 500000);
  ASSERT_EQ(proj_srs_info->simple_proj_prams_[4].value_, 0);

  ASSERT_EQ(OB_SUCCESS, mock_create_srs_item(allocator, srs_info, srs_item));
  ASSERT_EQ(srs_item->is_wgs84(), true);
  ASSERT_EQ(srs_item->srs_type(), ObSrsType::PROJECTED_SRS);
  ASSERT_EQ(srs_item->is_lat_long_order(), false);
  ASSERT_EQ(srs_item->is_latitude_north(), true);
  ASSERT_EQ(srs_item->is_longtitude_east(), true);

  ASSERT_EQ(srs_item->prime_meridian(), 0);
  ASSERT_EQ(srs_item->linear_uint(), 1.0);
  ASSERT_EQ(srs_item->angular_unit(), 0.017453292519943278);
  ASSERT_EQ(srs_item->semi_major_axis(), 0);
  ASSERT_EQ(srs_item->semi_minor_axis(), 0);

  double input = 15;
  double res = 0.0;

  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->longtitude_convert_to_radians(input, res));
  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->longtitude_convert_from_radians(input, res));

  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->latitude_convert_to_radians(input, res));
  ASSERT_EQ(OB_ERR_UNEXPECTED, srs_item->latitude_convert_from_radians(input, res));
}

TEST_F(TestGeoSrsParser, parse_pg_wkt)
{
  ObSpatialReferenceSystemBase *srs_info = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSrsItem *srs_item = NULL;
  // ObString srs_def = R"(PROJCS["WGS 84 / UTM zone 44N",GEOGCS["WGS 84", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of standard parallel",71,AUTHORITY["EPSG",8832]],PARAMETER["Longitude of origin",0,AUTHORITY["EPSG",8832]],PARAMETER["False easting",0,AUTHORITY["EPSG",8806]],PARAMETER["False northing",0, AUTHORITY["EPSG",8807]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",SOUTH],AXIS["N",SOUTH],AUTHORITY["EPSG",9122]])";

  // '+proj=stere +lat_0=90 +lat_ts=71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs'
  ObString NORTH_STEREO = R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Polar Stereopraphic",AUTHORITY["EPSG","9829"]],PARAMETER["Latitude of standard parallel",71,AUTHORITY["EPSG","8832"]],PARAMETER["Longitude of origin",0,AUTHORITY["EPSG","8833"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",SOUTH],AXIS["N",SOUTH],AUTHORITY["EPSG","9122"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, NORTH_STEREO, srs_info));

  // +proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
  ObString WORLD_MERCATOR = R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator",AUTHORITY["EPSG","9804"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",1,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, WORLD_MERCATOR, srs_info));

  // +proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
  ObString SOUTH_LAMBERT = R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert Azimuthal Equal Area",AUTHORITY["EPSG","9820"]],PARAMETER["Latitude of natural origin",-90,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",NORTH],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, SOUTH_LAMBERT, srs_info));

  // +proj=laea +lat_0=90 +lon_0=-40 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
  ObString NORTH_LAMBERT = R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert Azimuthal Equal Area",AUTHORITY["EPSG","9820"]],PARAMETER["Latitude of natural origin",90,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",-40,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",NORTH],AXIS["N",SOUTH],AUTHORITY["EPSG","9122"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, NORTH_LAMBERT, srs_info));

  int SOUTH_UTM_START = 999101;
  int SOUTH_UTM_END = 999160;
  // "+proj=utm +zone=%d +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
  for (int id = SOUTH_UTM_START; id <= SOUTH_UTM_END; id++) {
    const uint32_t MAX_WKT_LEN = 4096;
    char wkt_buf[MAX_WKT_LEN] = {0};
    int longitude = -177 + ((id - SOUTH_UTM_START) * 6);
    snprintf(wkt_buf, MAX_WKT_LEN, R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",%d,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])", longitude);
    ObString LAEA_START_str = ObString::make_string(wkt_buf);
    ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, LAEA_START_str, srs_info));
  }

  int NORTH_UTM_START = 999001;
  int NORTH_UTM_END = 999060;
  for (int id = NORTH_UTM_START; id <= NORTH_UTM_END; id++) {
    const uint32_t MAX_WKT_LEN = 4096;
    char wkt_buf[MAX_WKT_LEN] = {0};
    int longitude = -177 + ((id - NORTH_UTM_START) * 6);
    snprintf(wkt_buf, MAX_WKT_LEN, R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",%d,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])", longitude);
    ObString LAEA_START_str = ObString::make_string(wkt_buf);
    ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, LAEA_START_str, srs_info));
  }
  ObString NORTH_UTM_STRAT = R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",-177,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])";
  ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, NORTH_UTM_STRAT, srs_info));

  int LAEA_START = 999163;
  int LAEA_END = 999283;

  for (int id = LAEA_START; id <= LAEA_END; id++) {
    int zone = id - LAEA_START;
    int xzone = zone % 20;
    int yzone = zone / 20;
    double lat_0 = 30.0 * (yzone - 3) + 15.0;
    double lon_0 = 0.0;

    /* The number of xzones is variable depending on yzone */
    if  ( yzone == 2 || yzone == 3 )
      lon_0 = 30.0 * (xzone - 6) + 15.0;
    else if ( yzone == 1 || yzone == 4 )
      lon_0 = 45.0 * (xzone - 4) + 22.5;
    else if ( yzone == 0 || yzone == 5 )
      lon_0 = 90.0 * (xzone - 2) + 45.0;
    else
      std::cout << "Unknown yzone encountered!" << std::endl;

    while (lon_0 > 180) {
      lon_0 -= 360;
    }
    while (lon_0 < -180) {
      lon_0 += 360;
    }

    const uint32_t MAX_WKT_LEN = 4096;
    char wkt_buf[MAX_WKT_LEN] = {0};
    snprintf(wkt_buf, MAX_WKT_LEN, R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert Azimuthal Equal Area",AUTHORITY["EPSG","9820"]],PARAMETER["Latitude of natural origin",%g,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",%g,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",NORTH],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])", lat_0, lon_0);
    ObString LAEA_START_str = ObString::make_string(wkt_buf);
    ASSERT_EQ(OB_SUCCESS, ObSrsWktParser::parse_srs_wkt(allocator, 32644, LAEA_START_str, srs_info));
  }
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_bin.log");
  OB_LOGGER.set_file_name("test_geo_srs_parser.log");
  OB_LOGGER.set_log_level("WARN");
  */
  return RUN_ALL_TESTS();
}