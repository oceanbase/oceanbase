/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"
#include <locale.h>

using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int err = OB_SUCCESS;
  setlocale(LC_ALL, "");
  oceanbase::common::ObLogger::get_logger().set_log_level(getenv("log_level") ? : "INFO");
  oceanbase::unittest::init_tenant_mgr();
    return err;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
