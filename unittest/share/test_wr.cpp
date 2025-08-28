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

#define USING_LOG_PREFIX SHARE

#include "observer/mysql/ob_query_retry_ctrl.h"
#include "share/wr/ob_wr_collector.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace share
{
using namespace common;

class TestWr : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown(){}
};

TEST_F(TestWr, wr_collector_is_can_retry)
{
  observer::ObQueryRetryCtrl retry_ctrl;
  retry_ctrl.init();

  // err that cannot be retried
  ASSERT_EQ(false, ObWrCollector::is_can_retry(OB_ERR_XML_INDEX));
  ASSERT_EQ(false, ObWrCollector::is_can_retry(OB_INVALID_MASK));
  ASSERT_EQ(false, ObWrCollector::is_can_retry(OB_LS_NEED_REBUILD));

  // retry_func that is empty_proc func, cannot be retried
  ASSERT_EQ(false, ObWrCollector::is_can_retry(OB_ERR_SP_DOES_NOT_EXIST));
  ASSERT_EQ(false, ObWrCollector::is_can_retry(OB_ERR_FUNCTION_UNKNOWN));
  ASSERT_EQ(false, ObWrCollector::is_can_retry(OB_OBJECT_NAME_EXIST));

  // err that can be retried 
  ASSERT_EQ(true, ObWrCollector::is_can_retry(OB_SCHEMA_EAGAIN));
  ASSERT_EQ(true, ObWrCollector::is_can_retry(OB_LOCATION_NOT_EXIST));
  ASSERT_EQ(true, ObWrCollector::is_can_retry(OB_GTS_NOT_READY));
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
