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

#include "ob_simple_replica.h"
#include <rapidjson/document.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#define MAX_ZONE_COUNT 3
#define CLUSTER_EVENT_FILE_NAME "CLUSTER_EVENT"
#define EVENT_KV_SEPARATOR " = "
#define TEST_CASE_FINSH_EVENT_PREFIX "FINISH_TEST_CASE_FOR_"

namespace oceanbase
{
namespace unittest
{

int set_trace_id(char *buf);
void init_log_and_gtest(int argc, char **argv);
void init_gtest_output(std::string &gtest_log_name);

struct RestartIndex
{
  int restart_zone_id_;
  int restart_no_;

  void reset()
  {
    restart_zone_id_ =  -1;
    restart_no_ = 0;
  }

  RestartIndex()
  {
    reset();
  }
};

class ObMultiReplicaTestBase : public testing::Test
{
public:
  static const int64_t TRANS_TIMEOUT = 5 * 1000 * 1000;
  // set_bootstrap_and_create_tenant_warn_log 默认bootstrap和创建租户使用WARN日志，加速启动
  ObMultiReplicaTestBase();
  virtual ~ObMultiReplicaTestBase();

  static int bootstrap_multi_replica(const std::string &app_name,
                                    const int restart_zone_id = -1,
                                     const int restart_no = 0,
                                     const std::string &env_prefix = "run_");
  static int wait_all_test_completed();
  static int restart_zone(const int zone_id, const int restart_no);
  static int start();
  static int close();
  observer::ObServer &get_curr_observer() { return replica_->get_observer(); }
  observer::ObSimpleServerReplica &get_curr_simple_server() { return *replica_; }

  static int read_cur_json_document_(rapidjson::Document & json_doc);
  static int wait_event_finish(const std::string &event_name,
                               std::string &event_content,
                               int64_t wait_timeout_ms,
                               int64_t retry_interval_ms = 1 * 1000);
  static int finish_event(const std::string &event_name, const std::string &event_content);

  int create_tenant(const char *tenant_name = DEFAULT_TEST_TENANT_NAME,
                    const char *memory_size = "2G",
                    const char *log_disk_size = "2G",
                    const bool oracle_mode = false);
  int delete_tenant(const char *tenant_name = DEFAULT_TEST_TENANT_NAME);
  int get_tenant_id(uint64_t &tenant_id, const char *tenant_name = DEFAULT_TEST_TENANT_NAME);
  int exec_write_sql_sys(const char *sql_str, int64_t &affected_rows);
  int check_tenant_exist(bool &bool_ret, const char *tenant_name = DEFAULT_TEST_TENANT_NAME);

  static std::string TEST_CASE_BASE_NAME;
  static std::string ZONE_TEST_CASE_NAME[MAX_ZONE_COUNT];
protected:
  static int init_replicas_();
  static int init_test_replica_(const int zone_id);

  static int is_valid_zone_id(int zone_id) { return zone_id >= 0; }

protected:
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

protected:
  // 因为ob_server.h 中ObServer的使用方式导致现在只能启动单台
  static std::shared_ptr<observer::ObSimpleServerReplica> replica_;
  static bool is_started_;
  static bool is_inited_;
  static std::thread th_;
  static std::string env_prefix_;
  static std::string app_name_;
  static std::string exec_dir_;
  static std::string event_file_path_;
  static std::string env_prefix_path_;
  static bool enable_env_warn_log_;

  static const char *log_disk_size_;
  static const char *memory_size_;

  static std::string local_ip_;

  static int cur_zone_id_;
  static int restart_zone_id_;
  static int restart_no_;
  static std::vector<int> zone_pids_;

  static std::vector<int64_t> rpc_ports_;
  static ObServerInfoList server_list_;
  static std::string rs_list_;

public:
  static bool block_msg_;
};

} // namespace unittest
} // namespace oceanbase
