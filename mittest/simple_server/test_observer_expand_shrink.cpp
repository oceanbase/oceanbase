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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_utils.h"
#include "env/ob_simple_server_restart_helper.h"

const char *TEST_FILE_NAME = "test_observer_expand_shrink";
const char *BORN_CASE_NAME= "ObserverExpandShink";
const char *RESTART_CASE_NAME = "ObserverExpandShinkRestart";
namespace oceanbase
{
using namespace logservice;
namespace unittest
{

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;
class ObserverExpandShink : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObserverExpandShink() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(ObserverExpandShink, observer_start)
{
  SERVER_LOG(INFO, "start observer success");
}

TEST_F(ObserverExpandShink, basic_func)
{
  omt::ObTenantNodeBalancer::get_instance().refresh_interval_ = 1 * 1000 * 1000;
  int64_t origin_server_in_use_size, origin_server_log_total_size;
  EXPECT_EQ(OB_SUCCESS, GCTX.log_block_mgr_->get_disk_usage(origin_server_in_use_size, origin_server_log_total_size));
  GCONF.log_disk_size = GCTX.log_block_mgr_->lower_align_(2 * origin_server_log_total_size);
  sleep(6);
  int64_t new_server_in_use_size, new_server_log_total_size;
  EXPECT_EQ(OB_SUCCESS, GCTX.log_block_mgr_->get_disk_usage(new_server_in_use_size, new_server_log_total_size));
  EXPECT_EQ(new_server_log_total_size, 2 * origin_server_log_total_size);
  LOG_INFO("first resize success");
  GCONF.log_disk_size = 0;
  sleep(3);
  EXPECT_EQ(OB_SUCCESS, GCTX.log_block_mgr_->get_disk_usage(new_server_in_use_size, new_server_log_total_size));
  EXPECT_NE(new_server_log_total_size, 0);
  LOG_INFO("second resize success");

  int64_t affected_rows = 0;
  std::string succ_sql_str = "ALTER RESOURCE UNIT sys_unit_config LOG_DISK_SIZE='3G'";
  EXPECT_EQ(OB_SUCCESS, exec_write_sql_sys(succ_sql_str.c_str(), affected_rows));
  // tenant_node_balancer 1 s 运行一次
  sleep(3);

  std::string succ_sql_str1 = "ALTER SYSTEM SET log_disk_utilization_limit_threshold = 81";
  EXPECT_EQ(OB_SUCCESS, exec_write_sql_sys(succ_sql_str1.c_str(), affected_rows));

  // 验证修改租户规格失败，报错小于clog盘下限
  std::string fail_sql_str = "ALTER RESOURCE UNIT sys_unit_config LOG_DISK_SIZE='1G'";
  EXPECT_EQ(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, exec_write_sql_sys(fail_sql_str.c_str(), affected_rows));
  std::string succ_sql_str2 = "ALTER SYSTEM SET log_disk_size = '4G'";
  EXPECT_EQ(OB_SUCCESS, exec_write_sql_sys(succ_sql_str2.c_str(), affected_rows));
  sleep(6);
  EXPECT_EQ(GCONF.log_disk_size, 4 * 1024 * 1024 * 1024ul);
  // 验证修改租户规格失败，clog盘空间不够
  std::string fail_sql_str1 = "ALTER RESOURCE UNIT sys_unit_config LOG_DISK_SIZE='100G'";
  EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, exec_write_sql_sys(fail_sql_str1.c_str(), affected_rows));

  // 验证创建租户失败，clog盘空间不够
  std::string succ_sql_str3 = "ALTER SYSTEM SET log_disk_size = '3G'";
  EXPECT_EQ(OB_SUCCESS, exec_write_sql_sys(succ_sql_str3.c_str(), affected_rows));
  sleep(6);
  EXPECT_EQ(GCONF.log_disk_size, 3 * 1024 * 1024 * 1024ul);
  EXPECT_EQ(OB_ZONE_RESOURCE_NOT_ENOUGH, create_tenant("runlin"));
  CLOG_LOG_RET(ERROR, OB_SUCCESS, "create runlin finished");
  GCONF.log_disk_size = 1024 * 1024 * 1024ul * 1024 * 100ul;
  int64_t log_disk_size = 0;
  int64_t log_disk_percentage = 0;
  EXPECT_EQ(OB_SERVER_OUTOF_DISK_SPACE, observer::ObServerUtils::get_log_disk_info_in_config(
                                                                log_disk_size,
                                                                log_disk_percentage));
}

//class ObserverExpandShinkRestart: public ObSimpleClusterTestBase
//{
//public:
//  ObserverExpandShinkRestart() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
//};
//
//TEST_F(ObserverExpandShinkRestart, create_tenant_after_restart)
//{
//  EXPECT_NE(0, GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_);
//  SERVER_LOG(INFO, "create_tenant_after_restart trace", KPC(GCTX.log_block_mgr_));
//}

TEST_F(ObserverExpandShink, direct_set_observer)
{
  GCONF.log_disk_size = 1024 * 1024 * 1024ul * 3;
  int64_t log_disk_size = 0;
  int64_t log_disk_percentage = 0;
  EXPECT_EQ(OB_SUCCESS, observer::ObServerUtils::get_log_disk_info_in_config(
                                                                log_disk_size,
                                                                log_disk_percentage));
  GCONF.log_disk_size = 10000000 * share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE;
  EXPECT_EQ(OB_SERVER_OUTOF_DISK_SPACE, observer::ObServerUtils::get_log_disk_info_in_config(
                                                                log_disk_size,
                                                                log_disk_percentage));
  GCONF.log_disk_size = 3 * share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE;
  EXPECT_EQ(false, GCTX.log_block_mgr_->check_space_is_enough_(share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE - 1));
  {
    GCONF.log_disk_size = 3 * share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE;
    sleep(6);
  }
  EXPECT_EQ(OB_SUCCESS, create_tenant("tt2"));
  EXPECT_EQ(false, GCTX.log_block_mgr_->check_space_is_enough_(2*share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE-1));
  EXPECT_EQ(false, GCTX.log_block_mgr_->check_space_is_enough_(2*share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE));
  int64_t affected_rows = 0;
  std::string succ_sql_str = "ALTER RESOURCE UNIT sys_unit_config LOG_DISK_SIZE='2G'";
  EXPECT_EQ(OB_SUCCESS, exec_write_sql_sys(succ_sql_str.c_str(), affected_rows));
  sleep(2);
  // tenant_node_balancer 1 s 运行一次
  EXPECT_EQ(true, GCTX.log_block_mgr_->check_space_is_enough_(2*share::ObUnitResource::UNIT_MIN_LOG_DISK_SIZE));
  bool tenant_exist = false;
  EXPECT_EQ(OB_SUCCESS, check_tenant_exist(tenant_exist, "tt2"));
  EXPECT_EQ(true, tenant_exist);
  EXPECT_EQ(OB_SUCCESS, delete_tenant("tt2"));
  int ret = OB_SUCCESS;
  while (true == tenant_exist && OB_SUCC(ret)) {
    if (OB_FAIL(check_tenant_exist(tenant_exist, "tt2"))) {
      SERVER_LOG(WARN, "check_tenant_exist failed", K(ret));
    }
  }
}

TEST_F(ObserverExpandShink, test_hidden_sys_tenant)
{
  omt::ObMultiTenant *omt = GCTX.omt_;
  bool remove_tenant_succ = false;
  int64_t log_disk_size_in_use = GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_;
  EXPECT_EQ(log_disk_size_in_use, GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_);

  share::TenantUnits units;
  EXPECT_EQ(OB_SUCCESS, omt->get_tenant_units(units));
  EXPECT_EQ(false, units.empty());
  bool has_sys_tenant = false;
  int64_t origin_sys_log_disk_size = 0;
  int64_t hidden_sys_log_disk_size = 0;
  for (int i = 0; i < units.count(); i++) {
    if (OB_SYS_TENANT_ID == units[i].tenant_id_) {
      origin_sys_log_disk_size = units[i].config_.log_disk_size();
      EXPECT_EQ(OB_SUCCESS, omt->convert_real_to_hidden_sys_tenant());
    }
  }

  EXPECT_EQ(OB_SUCCESS, omt->get_tenant_units(units));
  EXPECT_EQ(false, units.empty());
  ObUnitInfoGetter::ObTenantConfig sys_unit_config;
  for (int i = 0; i < units.count(); i++) {
    if (OB_SYS_TENANT_ID == units[i].tenant_id_) {
      hidden_sys_log_disk_size = units[i].config_.log_disk_size();
      sys_unit_config = units[i];
      sys_unit_config.config_.resource_.log_disk_size_ = origin_sys_log_disk_size + 512*1024*1024;
    }
  }
  CLOG_LOG(INFO, "runlin trace convert_hidden_to_real_sys_tenant", K(log_disk_size_in_use), KPC(GCTX.log_block_mgr_), K(origin_sys_log_disk_size), K(hidden_sys_log_disk_size));

  // 类型转换后，sys租户的unit规格可能会发生变化（隐藏sys租户的规格会被重新生成，具体逻辑参见gen_sys_tenant_unit_config）
  EXPECT_EQ(log_disk_size_in_use-origin_sys_log_disk_size+hidden_sys_log_disk_size,
      GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_);
  log_disk_size_in_use = GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_;
  EXPECT_EQ(OB_SUCCESS, omt->convert_hidden_to_real_sys_tenant(sys_unit_config));
  has_sys_tenant = true;
  EXPECT_EQ(log_disk_size_in_use-hidden_sys_log_disk_size+sys_unit_config.config_.log_disk_size(),
      GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_);
  log_disk_size_in_use = GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_;
  CLOG_LOG(INFO, "runlin trace after convert_hidden_to_real_sys_tenant", K(log_disk_size_in_use), KPC(GCTX.log_block_mgr_),
           K(origin_sys_log_disk_size));
  int64_t new_sys_log_disk_size = 2 *1024*1024*1024l +512*1024*1024;
  EXPECT_EQ(OB_SUCCESS, omt->update_tenant_log_disk_size(OB_SYS_TENANT_ID, new_sys_log_disk_size));
  EXPECT_EQ(true, has_sys_tenant);
  EXPECT_EQ(log_disk_size_in_use-sys_unit_config.config_.log_disk_size()+new_sys_log_disk_size,
            GCTX.log_block_mgr_->min_log_disk_size_for_all_tenants_);
  CLOG_LOG(INFO, "runlin trace after convert_real_to_hidden_sys_tenant", K(log_disk_size_in_use), KPC(GCTX.log_block_mgr_),
           K(origin_sys_log_disk_size));
}


TEST_F(ObserverExpandShink, paralle_set)
{
  omt::ObTenantNodeBalancer::get_instance().refresh_interval_ = 1000 * 1000 * 1000;
  sleep(3);
  LOG_INFO("start to test parallel_set");
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(1));
  ObLogService *log_service = MTL(ObLogService*);
  palf::PalfOptions opts;
  ASSERT_NE(nullptr, log_service);
  EXPECT_EQ(OB_SUCCESS, log_service->get_palf_options(opts));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_service->update_log_disk_usage_limit_size(1000));
  opts.disk_options_.log_disk_utilization_limit_threshold_ = 10;
  opts.disk_options_.log_disk_utilization_threshold_ = 11;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_service->palf_env_->update_options(opts));
  {
    const int64_t new_log_disk_size = opts.disk_options_.log_disk_usage_limit_size_*50/100;
    EXPECT_EQ(OB_SUCCESS, log_service->update_log_disk_usage_limit_size(new_log_disk_size));
    sleep(1);
    EXPECT_EQ(log_service->palf_env_->palf_env_impl_.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_,
        new_log_disk_size);
  }
  {
    const int64_t count1 = 10000;
    const int64_t count2 = 9000;
    std::atomic<int> new_log_disk_size(1*1024*1024*1024);
    auto update_size = [&log_service, &new_log_disk_size](int64_t count)
    {
      for (int64_t i = 0; i < count; i++) {
        log_service->update_log_disk_usage_limit_size(++new_log_disk_size);
      }
    };
    std::thread t1(update_size, count1);
    std::thread t2(update_size, count2);
    t1.join();
    t2.join();
    EXPECT_EQ(OB_SUCCESS, log_service->get_palf_options(opts));
    EXPECT_EQ(opts.disk_options_.log_disk_usage_limit_size_, 1*1024*1024*1024+count1+count2);
  }
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();
  return RUN_ALL_TESTS();
}
