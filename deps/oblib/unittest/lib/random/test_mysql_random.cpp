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
#include "lib/random/ob_mysql_random.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

class TestMysqlRandom : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}

};

TEST_F(TestMysqlRandom, main_test)
{
  double res_double = 0;
  uint64_t res_uint = 0;
  char res_buf[21];
  //test static methods

  ObMysqlRandom random;
  ASSERT_TRUE(false == random.is_inited());
  ASSERT_TRUE(0 == random.get_double());
  ASSERT_TRUE(0 == random.get_uint64());
  ASSERT_TRUE(OB_NOT_INIT == random.create_random_string(NULL, 21));
  ASSERT_TRUE(OB_NOT_INIT == random.create_random_string(res_buf, -1));

  random.init(123, 456);
  ASSERT_TRUE(random.is_inited());
  ASSERT_TRUE(0 != random.max_value_);
  ASSERT_TRUE(0 != random.max_value_double_);
  ASSERT_TRUE(OB_INVALID_ARGUMENT == random.create_random_string(NULL, 21));
  ASSERT_TRUE(OB_INVALID_ARGUMENT == random.create_random_string(res_buf, -1));
  ASSERT_TRUE(OB_INVALID_ARGUMENT == random.create_random_string(res_buf, 0));

  for (int64_t i = 0; i < 1000; ++i) {
    res_double = random.get_double();
    LIB_LOG(INFO, "res_double", K(i), K(res_double));
    ASSERT_TRUE(res_double > 0);
    ASSERT_TRUE(res_double < 1);
  }

  for (int64_t i = 0; i < 1000; ++i) {
    res_uint = random.get_uint64();
    LIB_LOG(INFO, "res_uint", K(i), K(res_uint));
    ASSERT_TRUE(res_uint > 0);
    ASSERT_TRUE(res_uint < 0xFFFFFFFF);
  }

  for (int64_t i = 0; i < 1000; ++i) {
    random.create_random_string(res_buf, 21);
    LIB_LOG(INFO, "res_buf", K(i), K(res_buf));
    ASSERT_TRUE(20 == strlen(res_buf));
  }
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
