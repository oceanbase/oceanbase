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

#define USING_LOG_PREFIX RPC_TEST

#include <gtest/gtest.h>
#include <string>
#include "lib/hash/ob_array_hash_map.h"         // ObArrayHashMap
#include "ob_simple_log_server.h"

namespace oceanbase
{
namespace unittest
{
#define SET_CASE_LOG_FILE(TEST_NAME, CASE_NAME) \
  const std::string log_file_name = TEST_NAME + "/" + CASE_NAME + ".log";\
  const std::string ele_log_file_name = TEST_NAME + "/" + CASE_NAME + ".election.log";\
  OB_LOGGER.set_file_name(log_file_name.c_str(),\
                          true, false, NULL, \
                          ele_log_file_name.c_str(), NULL);

#define RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME) \
  void *ptr = malloc(SIG_STACK_SIZE); \
  abort_unless(ptr != nullptr); \
  stack_t nss; \
  stack_t oss; \
  bzero(&nss, sizeof(nss)); \
  bzero(&oss, sizeof(oss)); \
  nss.ss_sp = ptr; \
  nss.ss_size = SIG_STACK_SIZE; \
  abort_unless(0 == sigaltstack(&nss, &oss)); \
  DEFER(sigaltstack(&oss, nullptr)); \
  if (OB_SUCCESS != oceanbase::observer::ObSignalHandle::change_signal_mask()) { \
  } \
  ::oceanbase::common::g_redirect_handler = true; \
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE(); \
  oceanbase::common::ObClockGenerator::init();\
  sleep(15); \
  const std::string rm_base_dir_cmd = "rm -rf " + TEST_NAME; \
  const std::string rm_log_cmd = "rm -f ./" + TEST_NAME + "*log*"; \
  const std::string mk_base_dir_cm = "mkdir " + TEST_NAME; \
  system(rm_base_dir_cmd.c_str()); \
  system(rm_log_cmd.c_str()); \
  system(mk_base_dir_cm.c_str()); \
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION); \
  ObClusterVersion::get_instance().update_cluster_version(DATA_CURRENT_VERSION); \
  const std::string log_file_name = TEST_NAME+"/"+TEST_NAME + ".log"; \
  const std::string ele_log_file_name = TEST_NAME+"/"+TEST_NAME + ".election.log"; \
  const std::string rs_log_file_name = TEST_NAME+"/"+TEST_NAME + ".rs.log"; \
  OB_LOGGER.set_file_name(log_file_name.c_str(), true, false, rs_log_file_name.c_str(), \
      ele_log_file_name.c_str(), NULL); \
  OB_LOGGER.set_log_level("INFO"); \
  OB_LOGGER.set_enable_log_limit(false); \
  OB_LOGGER.set_enable_async_log(false); \
  SERVER_LOG(INFO, "begin unittest"); \
  ::testing::InitGoogleTest(&argc, argv); \
  return RUN_ALL_TESTS();

class ObSimpleLogCluster : public testing::Test
{
public:
  ObSimpleLogCluster()
  {
    SERVER_LOG(INFO, "ObSimpleLogCluster construct");
  }
  virtual ~ObSimpleLogCluster() {}
  static int init();
  static int start();
  static int close();
  std::string &get_test_name() { return test_name_; }
  ObSimpleLogServer *get_log_server() { return log_server_; }

private:
  static int generate_sorted_server_list_(const int64_t node_cnt);

protected:
  static void SetUpTestCase();
  static void TearDownTestCase();

public:
  static constexpr int64_t RPC_PORT = 53212;

  static ObSimpleLogServer *log_server_;
  static bool is_started_;
  static std::string test_name_;
  static ObMemberList node_list_;
  static int64_t node_cnt_;
  // static std::vector<ObSimpleLogServer*> cluster_;
  // static ObMemberList member_list_;
  // static common::ObArrayHashMap<common::ObAddr, common::ObRegion> member_region_map_;
  // static int64_t member_cnt_;
  // static int64_t node_idx_base_;
  //thread to deal signals
  static char sig_buf_[sizeof(ObSignalWorker) + sizeof(observer::ObSignalHandle)];
  static ObSignalWorker *sig_worker_;
  static observer::ObSignalHandle *signal_handle_;
};

} // end unittest
} // end oceanbase