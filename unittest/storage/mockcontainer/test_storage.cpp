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

#include "mock_ob_server.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::unittest;

class TestObStorage : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObStorage, test1)
{
}

int main(int argc, char **argv)
{
  int ret = EXIT_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  static ObServerOptions server_options =
    {8021, 8011, NULL, NULL, false, NULL, NULL, "127.0.0.1", "storage", "./tmp/data"};
  MockObServer ob_server(server_options);
  const char *schema_file = "./test.schema";

  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_storage.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

    STORAGE_LOG(WARN, "init memory pool error", "ret", tmp_ret);
  } else if (OB_SUCCESS != (tmp_ret = ob_server.init(schema_file))) {
    STORAGE_LOG(WARN, "init ob server error", "ret", tmp_ret, K(schema_file));
  } else {
    STORAGE_LOG(INFO, "init ob server success", K(schema_file));
  }
  if (OB_SUCCESS != tmp_ret) {
    ret = EXIT_FAILURE;
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }

  return ret;
}
