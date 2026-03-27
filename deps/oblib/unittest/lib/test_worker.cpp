/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/worker.h"


TEST(TestWorker, CompatMode)
{
  using oceanbase::lib::Worker;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
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
