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

#define USING_LOG_PREFIX SQL_EXE
#include <gtest/gtest.h>
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/ob_sql_init.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class ObPxDopAutoScalingTest : public ::testing::Test
{
public:

  ObPxDopAutoScalingTest() = default;
  virtual ~ObPxDopAutoScalingTest() = default;
  virtual void SetUp() {};
  virtual void TearDown() {};

private:
  // disallow copy
  ObPxDopAutoScalingTest(const ObPxDopAutoScalingTest &other);
  ObPxDopAutoScalingTest& operator=(const ObPxDopAutoScalingTest &other);
};

#define CHECK_DOP_SCALING(LOW, HIGH, CUR, DOP, RESULT)  \
  tmp = ObPxSqcHandler::dop_scaling_by_load(DOP, LOW, HIGH, CUR);

TEST_F(ObPxDopAutoScalingTest, test1) {
  { int64_t tmp = 0;
    // fixed low, high and cur. result increases as dop increases.
                    //LOW, HI, CUR,DOP,RES
    CHECK_DOP_SCALING(100, 200, 0, 10, 10);
    CHECK_DOP_SCALING(100, 200, 0, 20, 20);
    CHECK_DOP_SCALING(100, 200, 0, 30, 30);
    CHECK_DOP_SCALING(100, 200, 0, 40, 40);
    CHECK_DOP_SCALING(100, 200, 0, 50, 50);
    CHECK_DOP_SCALING(100, 200, 0, 64, 64);
    CHECK_DOP_SCALING(100, 200, 0, 70, 70);
    CHECK_DOP_SCALING(100, 200, 0, 80, 80);
    CHECK_DOP_SCALING(100, 200, 0, 90, 90);
    CHECK_DOP_SCALING(100, 200, 0, 100, 100);
    // start to decrease when cur + dop > low_water.
    CHECK_DOP_SCALING(100, 200, 0, 128, 124);
    CHECK_DOP_SCALING(100, 200, 0, 150, 138);
    CHECK_DOP_SCALING(100, 200, 0, 180, 148);
    CHECK_DOP_SCALING(100, 200, 0, 200, 150);
    // no more increase when cur + dop > high_water.
    CHECK_DOP_SCALING(100, 200, 0, 256, 150);
    CHECK_DOP_SCALING(100, 200, 0, 512, 150);

    CHECK_DOP_SCALING(100, 200, 50, 10, 10);
    CHECK_DOP_SCALING(100, 200, 50, 20, 20);
    CHECK_DOP_SCALING(100, 200, 50, 30, 30);
    CHECK_DOP_SCALING(100, 200, 50, 40, 40);
    CHECK_DOP_SCALING(100, 200, 50, 50, 50);
    // start to decrease when cur + dop > low_water.
    CHECK_DOP_SCALING(100, 200, 50, 64, 63);
    CHECK_DOP_SCALING(100, 200, 50, 80, 76);
    CHECK_DOP_SCALING(100, 200, 50, 100, 88);
    CHECK_DOP_SCALING(100, 200, 50, 128, 98);
    CHECK_DOP_SCALING(100, 200, 50, 150, 100);
    // no more increase when cur + dop > high_water.
    CHECK_DOP_SCALING(100, 200, 50, 160, 100);
    CHECK_DOP_SCALING(100, 200, 50, 200, 100);

    CHECK_DOP_SCALING(100, 200, 100, 10, 10);
    CHECK_DOP_SCALING(100, 200, 100, 20, 18);
    CHECK_DOP_SCALING(100, 200, 100, 30, 26);
    CHECK_DOP_SCALING(100, 200, 100, 40, 32);
    CHECK_DOP_SCALING(100, 200, 100, 50, 38);
    CHECK_DOP_SCALING(100, 200, 100, 64, 44);
    CHECK_DOP_SCALING(100, 200, 100, 80, 48);
    CHECK_DOP_SCALING(100, 200, 100, 100, 50);
    // no more increase when cur + dop > high_water.
    CHECK_DOP_SCALING(100, 200, 100, 128, 50);
    CHECK_DOP_SCALING(100, 200, 100, 150, 50);


    CHECK_DOP_SCALING(150, 200, 50, 10, 10);
    CHECK_DOP_SCALING(150, 200, 50, 50, 50);
    CHECK_DOP_SCALING(150, 200, 50, 75, 75);
    CHECK_DOP_SCALING(150, 200, 50, 80, 80);
    CHECK_DOP_SCALING(150, 200, 50, 100, 100);
    // start to decrease when cur + dop > low_water.
    CHECK_DOP_SCALING(150, 200, 50, 128, 120);
    CHECK_DOP_SCALING(150, 200, 50, 150, 125);
    // no more increase when cur + dop > high_water.
    CHECK_DOP_SCALING(150, 200, 50, 200, 125);
  }
}

TEST_F(ObPxDopAutoScalingTest, test2) {
  { int64_t tmp = 0;
    // decrease to 1 when cur > high_water
                    //LOW, HI, CUR,DOP,RES
    CHECK_DOP_SCALING(10, 20, 20, 2, 1);
    CHECK_DOP_SCALING(10, 20, 20, 8, 1);
    CHECK_DOP_SCALING(10, 20, 20, 16, 1);
    CHECK_DOP_SCALING(10, 20, 20, 32, 1);
    CHECK_DOP_SCALING(10, 20, 32, 2, 1);
    CHECK_DOP_SCALING(10, 20, 32, 8, 1);
    CHECK_DOP_SCALING(10, 20, 32, 16, 1);
    CHECK_DOP_SCALING(10, 20, 32, 32, 1);

    CHECK_DOP_SCALING(32, 32, 32, 2, 1);
    CHECK_DOP_SCALING(32, 32, 32, 4, 1);
    CHECK_DOP_SCALING(32, 32, 32, 8, 1);
    CHECK_DOP_SCALING(32, 32, 32, 16, 1);
    CHECK_DOP_SCALING(32, 32, 64, 2, 1);
    CHECK_DOP_SCALING(32, 32, 64, 4, 1);
    CHECK_DOP_SCALING(32, 32, 64, 8, 1);
    CHECK_DOP_SCALING(32, 32, 64, 16, 1);

    CHECK_DOP_SCALING(128, 1024, 1024, 2, 1);
    CHECK_DOP_SCALING(128, 1024, 1024, 4, 1);
    CHECK_DOP_SCALING(128, 1024, 1024, 8, 1);
    CHECK_DOP_SCALING(128, 1024, 1024, 16, 1);
    CHECK_DOP_SCALING(128, 1024, 2048, 2, 1);
    CHECK_DOP_SCALING(128, 1024, 2048, 4, 1);
    CHECK_DOP_SCALING(128, 1024, 2048, 8, 1);
    CHECK_DOP_SCALING(128, 1024, 2048, 16, 1);
  }
}

TEST_F(ObPxDopAutoScalingTest, test3) {
  { int64_t tmp = 0;
                    //LOW, HI, CUR,DOP,RES
    CHECK_DOP_SCALING(32, 64, 0, 16, 16);
    CHECK_DOP_SCALING(32, 64, 16, 16, 32);
    CHECK_DOP_SCALING(32, 64, 32, 16, 12);
    CHECK_DOP_SCALING(32, 64, 44, 16, 6);
    CHECK_DOP_SCALING(32, 64, 50, 16, 3);
    CHECK_DOP_SCALING(32, 64, 53, 16, 2);
    CHECK_DOP_SCALING(32, 64, 55, 16, 1);
    CHECK_DOP_SCALING(32, 64, 56, 16, 1);


    CHECK_DOP_SCALING(128, 256, 0, 64, 64);
    CHECK_DOP_SCALING(128, 256, 64, 64, 64);
    CHECK_DOP_SCALING(128, 256, 128, 64, 48);
    CHECK_DOP_SCALING(128, 256, 176, 64, 24);
    CHECK_DOP_SCALING(128, 256, 200, 64, 12);
    CHECK_DOP_SCALING(128, 256, 212, 64, 8);
    CHECK_DOP_SCALING(128, 256, 220, 64, 5);
    CHECK_DOP_SCALING(128, 256, 225, 64, 4);
    CHECK_DOP_SCALING(128, 256, 229, 64, 3);
    CHECK_DOP_SCALING(128, 256, 232, 64, 2);
    CHECK_DOP_SCALING(128, 256, 234, 64, 2);
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  //oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
