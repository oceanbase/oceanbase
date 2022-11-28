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
#define private public
#include "logservice/rcservice/ob_role_change_handler.h"
#include "logservice/ob_log_base_type.h"
#undef private

namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class MockRoleChangeHandler : public ObIRoleChangeSubHandler
{
public:
  void switch_to_follower_forcedly() override final
  {}
  int switch_to_leader() override final
  {
    return OB_SUCCESS;
  }
  int switch_to_follower_gracefully() override final
  {
    return OB_SUCCESS;
  }
  int resume_leader() override final
  {
    return OB_SUCCESS;
  }
};
TEST(TestRoleChangeHander, test_basic_func)
{
  ObRoleChangeHandler handler;
  ObLogBaseType type = ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE;
  MockRoleChangeHandler mock_handler;
  RCDiagnoseInfo unused_diagnose_info;
  handler.register_handler(type, &mock_handler);
  handler.switch_to_leader(unused_diagnose_info);
  handler.switch_to_follower_forcedly();
  handler.switch_to_follower_gracefully();
}
} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_role_change_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_role_change_handler");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
