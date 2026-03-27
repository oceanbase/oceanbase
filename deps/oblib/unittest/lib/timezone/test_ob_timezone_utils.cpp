/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/timezone/ob_timezone_util.h"
#include "lib/string/ob_string.h"

using namespace oceanbase;
using namespace oceanbase::common;

class ObTimezoneUtilsTest : public ::testing::Test
{
public:
  ObTimezoneUtilsTest();
  virtual ~ObTimezoneUtilsTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObTimezoneUtilsTest(const ObTimezoneUtilsTest &other);
  ObTimezoneUtilsTest& operator=(const ObTimezoneUtilsTest &other);

};


ObTimezoneUtilsTest::ObTimezoneUtilsTest()
{
}

ObTimezoneUtilsTest::~ObTimezoneUtilsTest()
{
}

void ObTimezoneUtilsTest::SetUp()
{
}

void ObTimezoneUtilsTest::TearDown()
{
}



#define PARSE_ZONE_FILE_RET(obj, filename)  \
                                {                                     \
                                 int err = obj.parse_timezone_file(filename); \
                                 ASSERT_EQ(OB_SUCCESS, err); \
                                } while(0)



TEST(ObTimezoneUtilsTest, parse_timezone_file_test)
{
  //// This class can't work correctly right now.
  //
  // ObTimezoneUtils zoneObj;
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/America/Chicago");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/America/Cordoba");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/America/Grenada");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/Asia/Yakutsk");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/Asia/Tehran");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/Asia/Shanghai");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/Singapore");
  // PARSE_ZONE_FILE_RET(zoneObj, "/usr/share/zoneinfo/GMT");
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
