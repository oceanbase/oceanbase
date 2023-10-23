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
#define private public
#define protected public
#include "lib/geo/ob_wkt_parser.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/geo/ob_geo_ibin.h"
#undef private
#undef protected

namespace oceanbase {
namespace common {
class TestWktParser : public ::testing::Test
{
public:
  TestWktParser()
  {}
  ~TestWktParser()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
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

private:
  ObArenaAllocator allocator_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestWktParser);
};

TEST_F(TestWktParser, test_parse_point)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  // DBL_MAX
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("point(1.79769313486231470e+308 -1.79769313486231470e+308)"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("0101000000FAFFFFFFFFFFEF7FFAFFFFFFFFFFEFFF"), to_hex(mock_to_wkb(geo)));

  // number with sign
  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" poinT (+12345678910 -9876543210)              "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("01010000000000F0E1E0FE0642000050B7806502C2"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("pOiNt  (+1.123e100 -1e5) "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("0101000000347898C58589B45400000000006AF8C0"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("pOiNt  (+01234.56789 -98765.43210) "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("0101000000E7C6F484454A93408AB0E1E9D61CF8C0"), to_hex(mock_to_wkb(geo)));

  // begin or end with float point
  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("point(234400000000000000000000000000000000000000000000000000000000000000. .24)"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010100000055F12CD35BCE814DB81E85EB51B8CE3F"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("point(+.1234 -.5678)"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("0101000000F38E53742497BF3FCF66D5E76A2BE2BF"), to_hex(mock_to_wkb(geo)));

  // large than DBL_MAX
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point(1.79769313486231581e+308 1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // wrong wkt format
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point(1 )"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point(1 , 1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("p oint(1  1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("p oint(1. . 1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("p oint(+-1  1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point(1 1, 1 1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point((1 1), (1 2))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point((1 1))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point(1 1))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("point((1 1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);
}

TEST_F(TestWktParser, test_parse_linestring)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" linestring(+12 -34, -5.6 +78.9)"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010200000002000000000000000000284000000000000041C066666666666616C09A99999999B95340"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("lIneStrIng(1 999999,   45 1e100) "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010200000002000000000000000000F03F000000007E842E4100000000008046407DC39425AD49B254"), to_hex(mock_to_wkb(geo)));

  // at least two points
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("linestring(1 1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // wrong wkt format
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("linestring(  ), "), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("linestring( . ), "), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("linestring(1 2, 1 2), "), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("linestring((1 2), (3 4))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // large number of point inside linestring
  // single alloc size cannot exceed 4G
  uint64_t num_points = 10000000;

  char *buf = static_cast<char *>(allocator.alloc(num_points * 4 + 64));
  ObString wkt(num_points * 8 + 64, 0, buf);
  ASSERT_EQ(strlen("linestring("), wkt.write("linestring(", strlen("linestring(")));

  for (int i = 0; i < num_points; i++) {
    ASSERT_EQ(strlen("1 1,"), wkt.write("1 1,", strlen("1 1,")));
  }
  ASSERT_EQ(wkt.length() - 1, wkt.set_length(wkt.length() - 1));
  ASSERT_EQ(1, wkt.write(")", 1));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ASSERT_TRUE(NULL != geo);
  uint64_t pos = 0;
  const char *wkb = (to_hex(mock_to_wkb(geo))).ptr();
  ASSERT_EQ(0, MEMCMP(wkb + pos, "0102000000", strlen("0102000000")));

  ObString hex_num_points = ObString(sizeof(uint32_t), reinterpret_cast<char *>(&num_points));
  pos += strlen("0102000000");
  ASSERT_EQ(0, MEMCMP(wkb + pos, to_hex(hex_num_points).ptr(), sizeof(uint32_t) * 2));
  pos += sizeof(uint32_t) * 2;

  for(int i = 0; i < num_points; ++i) {
    ASSERT_EQ(0, MEMCMP(wkb + pos, "000000000000F03F", strlen("000000000000F03F")));
    pos += strlen("000000000000F03F");
  }
}

TEST_F(TestWktParser, test_parse_polygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" polygon((12 34, 56 78 , 99 10, 12 34), (13 34, 54 45, 78 98, 13 34)   ) "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("01030000000200000004000000000000000000284000000000000041400000000000004C4000000000008053400000000000C05840000000000000244000000000000028400000000000004140040000000000000000002A4000000000000041400000000000004B400000000000804640000000000080534000000000008058400000000000002A400000000000004140"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" polygon((12 34, 56 78 , 99 10, 12 34), (13 34, 54 45, 78 98, 13 34), (1 2, 3 4, 5 6, 1 2)) "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("01030000000300000004000000000000000000284000000000000041400000000000004C4000000000008053400000000000C05840000000000000244000000000000028400000000000004140040000000000000000002A4000000000000041400000000000004B400000000000804640000000000080534000000000008058400000000000002A40000000000000414004000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840000000000000F03F0000000000000040"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" polygon((12 34, 56 78 , 99 10, 12 34), (13 34, 54 45, 78 98, 13 34), (1 2, 3 4, 5 6, 1 2), (1 2, 3 4, 5 6, 76 8, 1 2, 1 2, 1 2))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("01030000000400000004000000000000000000284000000000000041400000000000004C4000000000008053400000000000C05840000000000000244000000000000028400000000000004140040000000000000000002A4000000000000041400000000000004B400000000000804640000000000080534000000000008058400000000000002A40000000000000414004000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840000000000000F03F000000000000004007000000000000000000F03F0000000000000040000000000000084000000000000010400000000000001440000000000000184000000000000053400000000000002040000000000000F03F0000000000000040000000000000F03F0000000000000040000000000000F03F0000000000000040"), to_hex(mock_to_wkb(geo)));

  // linstring of polygon must be a ring
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon((12 34, 56 78 , 99 10, 12 34), (13 34, 54 45, 78 98, 13 35), (1 2, 3 4, 5 6, 1 2))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // a ring must have at least four points
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon(())"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon((12 34))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon((12 34, 12 34))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon((12 34, 56 78 , 12 34)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // wront wkt format
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon(12 34, 56 78 , 910 1011, 12 34)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon((12 34), (56 78) , (910 1011), (12 34))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("polygon(((12 34), (56 78) , (910 1011), (12 34)))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);
}

TEST_F(TestWktParser, test_parse_multipoint)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" MULTIPOINT(-47 307,-768 -425,-3 167,-170 30,-784 721,951 146,407 790,37 850,-466 738)"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010400000009000000010100000000000000008047C00000000000307340010100000000000000000088C00000000000907AC0010100000000000000000008C00000000000E06440010100000000000000004065C00000000000003E40010100000000000000008088C0000000000088864001010000000000000000B88D400000000000406240010100000000000000007079400000000000B08840010100000000000000008042400000000000908A4001010000000000000000207DC00000000000108740"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("MULTIPOINT((0 0), (1 1), (2 2), (3 3), (4 4), (5 5), (6 6), (0 0), (1 1), (2 2), (3 3), (4 4), (5 5), (6 6))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("01040000000E0000000101000000000000000000000000000000000000000101000000000000000000F03F000000000000F03F0101000000000000000000004000000000000000400101000000000000000000084000000000000008400101000000000000000000104000000000000010400101000000000000000000144000000000000014400101000000000000000000184000000000000018400101000000000000000000000000000000000000000101000000000000000000F03F000000000000F03F010100000000000000000000400000000000000040010100000000000000000008400000000000000840010100000000000000000010400000000000001040010100000000000000000014400000000000001440010100000000000000000018400000000000001840"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("MULTIPOINT((12 34))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010400000001000000010100000000000000000028400000000000004140"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("MULTIPOINT(12 34)"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010400000001000000010100000000000000000028400000000000004140"), to_hex(mock_to_wkb(geo)));

  // cannot mix point without brackets and with brackets
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multipoint(1 1, (2 2))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // wront wkt format
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multipoint( )"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multipoint(1)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multipoint(1, 2)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);
}

TEST_F(TestWktParser, test_multilinestring)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" MultiLineString((0 0,0 1)) "), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010500000001000000010200000002000000000000000000000000000000000000000000000000000000000000000000F03F"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("MultiLineString((0 0,0 1),(3 0,3 1), (1 3, 4 5), (34 9, 3 -23 ) )"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010500000004000000010200000002000000000000000000000000000000000000000000000000000000000000000000F03F010200000002000000000000000000084000000000000000000000000000000840000000000000F03F010200000002000000000000000000F03F00000000000008400000000000001040000000000000144001020000000200000000000000000041400000000000002240000000000000084000000000000037C0"), to_hex(mock_to_wkb(geo)));

  // wront wkt foramt
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MultiLineString((0 0,0 1),(3 0,3 1), (1 3, 4 5), (34 \0, 3 -2))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multilinestring()"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multilinestring((1 2))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("multilinestring(((1 2), (3 4)))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);
}

TEST_F(TestWktParser, test_multipolygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;

  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((104 113,688 423,-859 602,272 978, 104 113)),((981 -394,189 -400,649 -325,-977 371,30 859,590 318,329 -894,-51 262,197 952,-846 -139,-920 399, 981 -394)),((-236 -759,834 757,857 747,437 -146,194 913,316 862,976 -491,-745 933,610 687,-149 -164,-803 -565,451 -275, -236 -759)),((572 96,-160 -607,529 930,-544 -132,458 294, 572 96)))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010600000004000000010300000001000000050000000000000000005A400000000000405C4000000000008085400000000000707A400000000000D88AC00000000000D0824000000000000071400000000000908E400000000000005A400000000000405C400103000000010000000C0000000000000000A88E400000000000A078C00000000000A0674000000000000079C0000000000048844000000000005074C00000000000888EC000000000003077400000000000003E400000000000D88A4000000000007082400000000000E0734000000000009074400000000000F08BC000000000008049C000000000006070400000000000A068400000000000C08D400000000000708AC000000000006061C00000000000C08CC00000000000F078400000000000A88E400000000000A078C00103000000010000000D0000000000000000806DC00000000000B887C00000000000108A400000000000A887400000000000C88A4000000000005887400000000000507B4000000000004062C000000000004068400000000000888C400000000000C073400000000000F08A400000000000808E400000000000B07EC000000000004887C00000000000288D40000000000010834000000000007885400000000000A062C000000000008064C000000000001889C00000000000A881C00000000000307C4000000000003071C00000000000806DC00000000000B887C0010300000001000000060000000000000000E08140000000000000584000000000000064C00000000000F882C000000000008880400000000000108D4000000000000081C000000000008060C00000000000A07C4000000000006072400000000000E081400000000000005840"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((3 5,2 5,2 4,3 4,3 5)),((2 2,2 8,8 8,8 2,2 2),(4 4,4 6,6 6,6 4,4 4)),((0 5,3 5,3 2,1 2,1 1,3 1,3 0,0 0,0 3,2 3,2 4,0 4,0 5)))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("0106000000030000000103000000010000000500000000000000000008400000000000001440000000000000004000000000000014400000000000000040000000000000104000000000000008400000000000001040000000000000084000000000000014400103000000020000000500000000000000000000400000000000000040000000000000004000000000000020400000000000002040000000000000204000000000000020400000000000000040000000000000004000000000000000400500000000000000000010400000000000001040000000000000104000000000000018400000000000001840000000000000184000000000000018400000000000001040000000000000104000000000000010400103000000010000000D000000000000000000000000000000000014400000000000000840000000000000144000000000000008400000000000000040000000000000F03F0000000000000040000000000000F03F000000000000F03F0000000000000840000000000000F03F00000000000008400000000000000000000000000000000000000000000000000000000000000000000000000000084000000000000000400000000000000840000000000000004000000000000010400000000000000000000000000000104000000000000000000000000000001440"), to_hex(mock_to_wkb(geo)));

  // must be a ring
  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((104 113,688 423,-859 602,272 978, 104 113)),((981 -394,189 -400,649 -325,-977 371,30 859,590 318,329 -894,-51 262,197 952,-846 -139,-920 399, 981 -394)),((-236 -759,834 757,857 747,437 -146,194 913,316 862,976 -491,-745 933,610 617,-149 -164,-803 -565,451 -275, -236 -759.0001)),((572 96,-160 -607,529 930,-544 -132,458 294, 572 96)))  "), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON ()"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((1 2, 3 4, 5 6)))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((1 2, 3 4, 1 2)))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(1 2, 3 4, 5 6, 1 2)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(1 2, 3 4, 5 6, 1 2)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2),))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2)),)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2))),"), geo, true, false));
  ASSERT_TRUE(NULL == geo);
}

TEST_F(TestWktParser, test_geometrycollection)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometry *geo = NULL;

  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("geometrycollection()"), geo, true, false));
  ASSERT_EQ(ObString("010700000000000000"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("geometrycollection empty"), geo, true, false));
  ASSERT_EQ(ObString("010700000000000000"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)),GEOMETRYCOLLECTION(LINESTRING(0 0,10 10)),\n\n\nGEOMETRYCOLLECTION(POLYGON((0 0,0 10,10 10,10 0,0 0))),GEOMETRYCOLLECTION(MULTIPOINT((0 0),(2 2),(4 4),(6 6),(8 8),(10 10))),GEOMETRYCOLLECTION(MULTILINESTRING((0 0,10 10),(0 10,10 0))),GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)),((5 5,5 10,10 10,10 5,5 5)))), GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,10 10),POLYGON((0 0,0 10,10 10,10 0,0 0)),MULTIPOINT((0 0),(2 2),(4 4),(6 6),(8 8),(10 10)),MULTILINESTRING((0 0,10 10),(0 10,10 0)),MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)),((5 5,5 10,10 10,10 5,5 5))), GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,10 10)\t\t \n,POLYGON((0 0,0 10,10 10,10 0,0 0)),MULTIPOINT((0 0),(2 2),(4 4),(6 6),(8 8),(10 10)),MULTILINESTRING((0 0,10 10),(0 10,10 0)),MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)),((5 5,5 10,10 10,10 5,5 5))))))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("010700000007000000010700000001000000010100000000000000000000000000000000000000010700000001000000010200000002000000000000000000000000000000000000000000000000002440000000000000244001070000000100000001030000000100000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000001070000000100000001040000000600000001010000000000000000000000000000000000000001010000000000000000000040000000000000004001010000000000000000001040000000000000104001010000000000000000001840000000000000184001010000000000000000002040000000000000204001010000000000000000002440000000000000244001070000000100000001050000000200000001020000000200000000000000000000000000000000000000000000000000244000000000000024400102000000020000000000000000000000000000000000244000000000000024400000000000000000010700000001000000010600000002000000010300000001000000050000000000000000000000000000000000000000000000000000000000000000001440000000000000144000000000000014400000000000001440000000000000000000000000000000000000000000000000010300000001000000050000000000000000001440000000000000144000000000000014400000000000002440000000000000244000000000000024400000000000002440000000000000144000000000000014400000000000001440010700000007000000010100000000000000000000000000000000000000010200000002000000000000000000000000000000000000000000000000002440000000000000244001030000000100000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000001040000000600000001010000000000000000000000000000000000000001010000000000000000000040000000000000004001010000000000000000001040000000000000104001010000000000000000001840000000000000184001010000000000000000002040000000000000204001010000000000000000002440000000000000244001050000000200000001020000000200000000000000000000000000000000000000000000000000244000000000000024400102000000020000000000000000000000000000000000244000000000000024400000000000000000010600000002000000010300000001000000050000000000000000000000000000000000000000000000000000000000000000001440000000000000144000000000000014400000000000001440000000000000000000000000000000000000000000000000010300000001000000050000000000000000001440000000000000144000000000000014400000000000002440000000000000244000000000000024400000000000002440000000000000144000000000000014400000000000001440010700000006000000010100000000000000000000000000000000000000010200000002000000000000000000000000000000000000000000000000002440000000000000244001030000000100000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000001040000000600000001010000000000000000000000000000000000000001010000000000000000000040000000000000004001010000000000000000001040000000000000104001010000000000000000001840000000000000184001010000000000000000002040000000000000204001010000000000000000002440000000000000244001050000000200000001020000000200000000000000000000000000000000000000000000000000244000000000000024400102000000020000000000000000000000000000000000244000000000000024400000000000000000010600000002000000010300000001000000050000000000000000000000000000000000000000000000000000000000000000001440000000000000144000000000000014400000000000001440000000000000000000000000000000000000000000000000010300000001000000050000000000000000001440000000000000144000000000000014400000000000002440000000000000244000000000000024400000000000002440000000000000144000000000000014400000000000001440"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, ObString("geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(geometrycollection(point(1 1)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))"), geo, true, false));
  ASSERT_TRUE(NULL != geo);
  ASSERT_EQ(ObString("0107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000107000000010000000101000000000000000000F03F000000000000F03F"), to_hex(mock_to_wkb(geo)));

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString("geometrycollection(point(4.297374e+307,8.433875e+307), point(1e308, 1e308)) "), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" geometrycollection (empty)"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  geo = NULL;
  ASSERT_TRUE(OB_SUCCESS != ObWktParser::parse_wkt(allocator, ObString(" geometrycollection (geometrycollection(()))"), geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // large number of geometrycollection inside geometrycollection
  // recursive geometrycollection(geometrycollection(...))
  // the parse stack should not exceed 10M, or OB_SIZE_OVERFLOW is returned.
  uint64_t num_geoms = 10000;
  char *buf = static_cast<char *>(allocator.alloc(num_geoms * strlen("geometrycollection()")));
  ObString wkt(num_geoms * strlen("geometrycollection()"), 0, buf);

  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen("geometrycollection("), wkt.write("geometrycollection(", strlen("geometrycollection(")));
  }
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen(")"), wkt.write((")"), strlen(")")));
  }
  geo = NULL;
  ASSERT_EQ(OB_SUCCESS, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ASSERT_TRUE(NULL != geo);
  uint64_t pos = 0;
  const char *wkb = (to_hex(mock_to_wkb(geo))).ptr();
  for(int i = 0; i < num_geoms - 1; ++i) {
    ASSERT_EQ(0, MEMCMP(wkb + pos, "010700000001000000", strlen("010700000001000000")));
    pos += strlen("010700000001000000");
  }
  ASSERT_EQ(0, MEMCMP(wkb + pos, "010700000000000000", strlen("010700000000000000")));

  num_geoms = 1000000;
  allocator.free(buf);
  buf = static_cast<char *>(allocator.alloc(num_geoms * strlen("geometrycollection()")));
  wkt = ObString(num_geoms * strlen("geometrycollection()"), 0, buf);
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen("geometrycollection("), wkt.write("geometrycollection(", strlen("geometrycollection(")));
  }
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen(")"), wkt.write((")"), strlen(")")));
  }
  geo = NULL;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ASSERT_TRUE(NULL == geo);

  num_geoms = 10000000;
  allocator.free(buf);
  buf = static_cast<char *>(allocator.alloc(num_geoms * strlen("geometrycollection()")));
  wkt = ObString(num_geoms * strlen("geometrycollection()"), 0, buf);
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen("geometrycollection("), wkt.write("geometrycollection(", strlen("geometrycollection(")));
  }
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen(")"), wkt.write((")"), strlen(")")));
  }
  geo = NULL;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ASSERT_TRUE(NULL == geo);

  // single alloc size cannot exceed 4G
  num_geoms = 100000000;
  allocator.free(buf);
  buf = static_cast<char *>(allocator.alloc(num_geoms * strlen("geometrycollection()")));
  wkt = ObString(num_geoms * strlen("geometrycollection()"), 0, buf);
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen("geometrycollection("), wkt.write("geometrycollection(", strlen("geometrycollection(")));
  }
  for (int i = 0; i < num_geoms; i++) {
    ASSERT_EQ(strlen(")"), wkt.write((")"), strlen(")")));
  }
  geo = NULL;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObWktParser::parse_wkt(allocator, wkt, geo, true, false));
  ASSERT_TRUE(NULL == geo);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_wkt_parser.log");
  OB_LOGGER.set_file_name("test_wkt_parser.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}