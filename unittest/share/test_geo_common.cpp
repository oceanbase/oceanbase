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
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/json_type/ob_json_common.h"
#define private public
#undef private

namespace oceanbase {
namespace common {
class TestGeoCommon : public ::testing::Test
{
public:
  TestGeoCommon()
  {}
  ~TestGeoCommon()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}
  int append_point(ObJsonBuffer &data, double x, double y, uint32_t srid);
private:
  int append_srid(ObJsonBuffer &data, uint32_t srid = 0);
  int append_bo(ObJsonBuffer &data, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian);
  int append_type(ObJsonBuffer &data, ObGeoType type, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian);
  int append_double(ObJsonBuffer &data, double val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian);
  int append_double_point(ObJsonBuffer &data, double &x, double &y);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestGeoCommon);
};


int TestGeoCommon::append_srid(ObJsonBuffer &data, uint32_t srid /* = 0*/)
{
  return data.append(reinterpret_cast<char*>(&srid), sizeof(srid));
}

int TestGeoCommon::append_bo(ObJsonBuffer &data, ObGeoWkbByteOrder bo /* = ObGeoWkbByteOrder::LittleEndian */)
{
  uint8_t sbo = static_cast<uint8_t>(bo);
  return data.append(reinterpret_cast<char*>(&sbo), sizeof(uint8_t));
}

int TestGeoCommon::append_type(ObJsonBuffer &data, ObGeoType type, ObGeoWkbByteOrder bo /* = ObGeoWkbByteOrder::LittleEndian */)
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

int TestGeoCommon::append_double(ObJsonBuffer &data, double val, ObGeoWkbByteOrder bo /* = ObGeoWkbByteOrder::LittleEndian */)
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

int TestGeoCommon::append_double_point(ObJsonBuffer &data, double &x, double &y)
{
  INIT_SUCC(ret);
  if (OB_FAIL(append_double(data, x))) {
  } else if (OB_FAIL(append_double(data, y))) {
  }
  return ret;
}

int TestGeoCommon::append_point(ObJsonBuffer &data, double x, double y, uint32_t srid)
{
  INIT_SUCC(ret);
  if (OB_FAIL(append_srid(data, srid))) {
  } else if (OB_FAIL(append_bo(data))) {
  } else if (OB_FAIL(append_type(data, ObGeoType::POINT))) {
  }
  return ret;
}

TEST_F(TestGeoCommon, test_check_geo_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  append_point(data, 1, 5, 4326);
  ObString wkb(data.length(), data.ptr());
  ObString empty_wkb(0, NULL);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObGeoTypeUtil::check_geo_type(ObGeoType::POINT, empty_wkb));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObGeoTypeUtil::check_geo_type(ObGeoType::GEOTYPEMAX, wkb));
  ASSERT_EQ(OB_SUCCESS, ObGeoTypeUtil::check_geo_type(ObGeoType::GEOMETRY, wkb));
  ASSERT_EQ(OB_SUCCESS, ObGeoTypeUtil::check_geo_type(ObGeoType::POINT, wkb));
}

TEST_F(TestGeoCommon, test_wkb_byte_order_util)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ASSERT_EQ(4, sizeof(uint32_t));
  ASSERT_EQ(8, sizeof(double));
  char *data = static_cast<char *>(allocator.alloc(2 * 8));
  ASSERT_TRUE(NULL != data);

  double d = 1234.56789;
  ObGeoWkbByteOrderUtil::write<double>(data, d, ObGeoWkbByteOrder::LittleEndian);
  ASSERT_EQ(d, ObGeoWkbByteOrderUtil::read<double>(data, ObGeoWkbByteOrder::LittleEndian));
  ObGeoWkbByteOrderUtil::write<double>(data + 8, d, ObGeoWkbByteOrder::BigEndian);
  ASSERT_EQ(d, ObGeoWkbByteOrderUtil::read<double>(data + 8, ObGeoWkbByteOrder::BigEndian));
  for (int i = 0; i < 8; i++) {
    ASSERT_TRUE(0 == MEMCMP(data + i, data + 15 - i, 1));
  }

  uint32_t n = 123456789;
  ObGeoWkbByteOrderUtil::write<uint32_t>(data, n, ObGeoWkbByteOrder::LittleEndian);
  ASSERT_EQ(n, ObGeoWkbByteOrderUtil::read<uint32_t>(data, ObGeoWkbByteOrder::LittleEndian));
  ObGeoWkbByteOrderUtil::write<uint32_t>(data + 4, n, ObGeoWkbByteOrder::BigEndian);
  ASSERT_EQ(n, ObGeoWkbByteOrderUtil::read<uint32_t>(data + 4, ObGeoWkbByteOrder::BigEndian));
  for (int i = 0; i < 4; i++) {
    ASSERT_TRUE(0 == MEMCMP(data + i, data + 7 - i, 1));
  }
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_tree.log");
  OB_LOGGER.set_file_name("test_json_tree.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}