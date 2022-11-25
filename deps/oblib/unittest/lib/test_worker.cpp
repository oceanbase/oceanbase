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
#include "lib/worker.h"


TEST(TestWorker, CompatMode)
{
  using oceanbase::lib::Worker;

  // MySQL Mode as default.
  EXPECT_EQ(oceanbase::lib::Worker::CompatMode::MYSQL, THIS_WORKER.get_compatibility_mode());
  EXPECT_TRUE(oceanbase::lib::is_mysql_mode());
  EXPECT_FALSE(oceanbase::lib::is_oracle_mode());

  // Change to Oracle Mode if set
  THIS_WORKER.set_compatibility_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  EXPECT_EQ(oceanbase::lib::Worker::CompatMode::ORACLE, THIS_WORKER.get_compatibility_mode());
  EXPECT_FALSE(oceanbase::lib::is_mysql_mode());
  EXPECT_TRUE(oceanbase::lib::is_oracle_mode());

  // Turn back to MySQL Mode if set back.
  THIS_WORKER.set_compatibility_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  EXPECT_EQ(oceanbase::lib::Worker::CompatMode::MYSQL, THIS_WORKER.get_compatibility_mode());
  EXPECT_TRUE(oceanbase::lib::is_mysql_mode());
  EXPECT_FALSE(oceanbase::lib::is_oracle_mode());
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
