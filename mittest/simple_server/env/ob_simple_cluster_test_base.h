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

#pragma once

#include <gtest/gtest.h>

#include "ob_simple_server.h"

namespace oceanbase
{
namespace unittest
{

int set_trace_id(char *buf);
void init_log_and_gtest(int argc, char **argv);
void init_gtest_output(std::string &gtest_log_name);

class ObSimpleClusterTestBase : public testing::Test
{
public:
  static const int64_t TRANS_TIMEOUT = 5 * 1000 * 1000;
  // set_bootstrap_and_create_tenant_warn_log 默认bootstrap和创建租户使用WARN日志，加速启动
  ObSimpleClusterTestBase(const std::string &env_prefix = "run_",
                          const char *log_disk_size = "10G",
                          const char *memory_limit = "16G");
  virtual ~ObSimpleClusterTestBase();

  int start();
  static int close();
  observer::ObServer& get_curr_observer() { return cluster_->get_observer(); }
  observer::ObSimpleServer& get_curr_simple_server() { return *cluster_; }

  int create_tenant(const char *tenant_name = "tt1",
                    const char *memory_size = "4G",
                    const char *log_disk_size = "4G",
                    const bool oracle_mode = false,
                    int64_t tenant_cpu = 2);
  int delete_tenant(const char *tenant_name = "tt1");
  int get_tenant_id(uint64_t &tenant_id, const char *tenant_name = "tt1");
  int exec_write_sql_sys(const char *sql_str, int64_t &affected_rows);
  int check_tenant_exist(bool &bool_ret, const char *tenant_name = "tt1");
  int batch_create_table(const uint64_t tenant_id,
                         ObMySQLProxy &sql_proxy,
                         const int64_t TOTAL_NUM,
                         ObIArray<ObTabletLSPair> &tablet_ls_pairs);
  int batch_drop_table(const uint64_t tenant_id,
                       ObMySQLProxy &sql_proxy,
                       const int64_t TOTAL_NUM);

protected:
  virtual void SetUp();
  virtual void TearDown();
  static void TearDownTestCase();

protected:
  // 因为ob_server.h 中ObServer的使用方式导致现在只能启动单台
  static std::shared_ptr<observer::ObSimpleServer> cluster_;
  static bool is_started_;
  static std::thread th_;
  static std::string env_prefix_;
  static std::string curr_dir_;
  static bool enable_env_warn_log_;
  static const char *UNIT_BASE;
  static const char *POOL_BASE;
};

} // end unittest
} // end oceanbase
