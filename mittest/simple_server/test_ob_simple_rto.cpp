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

#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#define private public
#include "share/scn.h"
#include "env/ob_simple_cluster_test_base.h"
#include "logservice/ob_log_service.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#undef private

const std::string TEST_NAME = "rto_func";
const int64_t CLOG_HANG_TIME_THRESHOLD_US = 5 * 1000 * 1000;
int64_t mock_fatal_err_ts = OB_INVALID_TIMESTAMP;
bool mock_clog_disk_hang = false;
bool mock_disk_io_hang = false;
bool mock_clog_disk_full = false;
using namespace oceanbase;
namespace oceanbase
{
using namespace palf;
using namespace storage;
namespace common
{
int ObIOFaultDetector::get_device_health_status(ObDeviceHealthStatus &dhs,
    int64_t &device_abnormal_time)
{
  if (mock_disk_io_hang) {
    dhs = DEVICE_HEALTH_WARNING;
    device_abnormal_time = mock_fatal_err_ts - GCONF.data_storage_warning_tolerance_time;
  } else {
    dhs = DEVICE_HEALTH_NORMAL;
    device_abnormal_time = OB_INVALID_TIMESTAMP;
  }
  return OB_SUCCESS;
}
};
using namespace common;
namespace logservice
{
int ObLogService::get_io_start_time(int64_t &last_working_time)
{
  last_working_time = mock_clog_disk_hang ?
                      mock_fatal_err_ts - CLOG_HANG_TIME_THRESHOLD_US
                      : OB_INVALID_TIMESTAMP;
  return OB_SUCCESS;
}
int ObLogService::check_disk_space_enough(bool &is_disk_enough)
{
  is_disk_enough = !mock_clog_disk_full;
  return OB_SUCCESS;
}
};
using namespace logservice;
using namespace coordinator;
namespace unittest
{
class ObRTOTest : public ObSimpleClusterTestBase
{
public:
  ObRTOTest() : ObSimpleClusterTestBase("test_ob_rto_") {}
};

TEST_F(ObRTOTest, basic_rto)
{
  CLOG_LOG(INFO, "test rto begin");
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(1));
  //mock clog disk hang
  mock_fatal_err_ts = ObTimeUtility::fast_current_time();
  mock_clog_disk_hang = true;
  while (!MTL(ObFailureDetector*)->has_add_clog_hang_event_) {
    CLOG_LOG(INFO, "waiting detect clog disk hang");
    usleep(100 * 1000);
  }
  mock_clog_disk_hang = false;
  while (MTL(ObFailureDetector*)->has_add_clog_hang_event_) {
    CLOG_LOG(INFO, "waiting recover clog disk hang");
    usleep(100 * 1000);
  }

  //mock clog disk full
  mock_clog_disk_full = true;
  while (!MTL(ObFailureDetector*)->has_add_clog_full_event_) {
    CLOG_LOG(INFO, "waiting detect clog disk full");
    usleep(100 * 1000);
  }
  mock_clog_disk_full = false;
  while (MTL(ObFailureDetector*)->has_add_clog_full_event_) {
    CLOG_LOG(INFO, "waiting recover clog disk full");
    usleep(100 * 1000);
  }

  //mock disk io hang
  mock_fatal_err_ts = ObTimeUtility::fast_current_time();
  mock_disk_io_hang = true;
  while (!MTL(ObFailureDetector*)->has_add_data_disk_hang_event_) {
    CLOG_LOG(INFO, "waiting detect disk io hang");
    usleep(100 * 1000);
  }
  mock_disk_io_hang = false;
  while (MTL(ObFailureDetector*)->has_add_data_disk_hang_event_) {
    CLOG_LOG(INFO, "waiting recover disk io hang");
    usleep(100 * 1000);
  }
}
} // unitest
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF._enable_defensive_check = false;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
