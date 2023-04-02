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
#include <stdlib.h>

#define protected public
#define private public

#include "ob_simple_server_restart_helper.h"

#include "ob_simple_cluster_test_base.h"
#include "lib/oblog/ob_log.h"
#include "share/config/ob_server_config.h"

#undef private
#undef protected

bool ObSimpleServerRestartHelper::need_restart_ = false;
bool ObSimpleServerRestartHelper::is_restart_ = false;
int ObSimpleServerRestartHelper::sleep_sec_ = 0;

// #define RUN_TEST_CASE(type)                                                                      \
//   sprintf(log_file_name, "%s_%s.log", test_file_name_, #type);                                   \
//   sprintf(rs_log_file_name, "%s_%s_rs.log", test_file_name_, #type);                             \
//   sprintf(election_log_file_name, "%s_%s_election.log", test_file_name_, #type);                 \
//   sprintf(filter_str, "%s*", type##_case_name_);                                                 \
//                                                                                                  \
//   OB_LOGGER.set_file_name(log_file_name, true, false, rs_log_file_name, election_log_file_name); \
//   OB_LOGGER.set_log_level(log_level_);                                                           \
//   ::testing::InitGoogleTest(&argc_, argv_);                                                      \
//                                                                                                  \
//   ::testing::GTEST_FLAG(filter) = filter_str;                                                    \
//   ret = RUN_ALL_TESTS();


int ObSimpleServerRestartHelper::run()
{
  need_restart_ = true;
  int ret = 0;
  char log_file_name[name_len];
  char rs_log_file_name[name_len];
  char election_log_file_name[name_len];
  char trace_log_file_name[name_len];
  char audit_log_file_name[name_len];
  char filter_str[name_len];
  memset(log_file_name, 0, name_len);
  memset(rs_log_file_name, 0, name_len);
  memset(election_log_file_name, 0, name_len);
  memset(filter_str, 0, name_len);

  GCONF._enable_defensive_check = false;
  GCONF._private_buffer_size = 1;
  auto child_pid = fork();

  if (child_pid < 0) {
    perror("fork");
    exit(EXIT_FAILURE);
  } else if (child_pid == 0) {

    sprintf(log_file_name, "%s_born.log", test_file_name_);
    sprintf(rs_log_file_name, "%s_born_rs.log", test_file_name_);
    sprintf(election_log_file_name, "%s_born_election.log", test_file_name_);
    sprintf(trace_log_file_name, "%s_born_trace.log", test_file_name_);
    sprintf(audit_log_file_name, "%s_born_audit.log", test_file_name_);
    sprintf(filter_str, "%s*", born_case_name_);

    OB_LOGGER.set_file_name(
        log_file_name, true, false, rs_log_file_name, election_log_file_name, trace_log_file_name, audit_log_file_name);
    OB_LOGGER.set_log_level(log_level_);
    ::testing::InitGoogleTest(&argc_, argv_);

    ::testing::GTEST_FLAG(filter) = filter_str;
    ret = RUN_ALL_TESTS();

    fprintf(stdout, "Child process exit ret = %d.\n", ret);

  } else {
    // parent process
    int status = 0;
    wait(&status);
    if (0 != status) {
      fprintf(stdout, "Child process exit with error code : %d\n", status);
      ret = status;
      return ret;
    } else {
      fprintf(stdout, "Child process run all test cases done. child ret = %d \n", status);
    }

    ObSimpleServerRestartHelper::is_restart_ = true;
    // RUN_TEST_CASE(restart);
    sprintf(log_file_name, "%s_restart.log", test_file_name_);
    sprintf(rs_log_file_name, "%s_restart_rs.log", test_file_name_);
    sprintf(election_log_file_name, "%s_restart_election.log", test_file_name_);
    sprintf(trace_log_file_name, "%s_restart_trace.log", test_file_name_);
    sprintf(audit_log_file_name, "%s_restart_audit.log", test_file_name_);
    sprintf(filter_str, "%s*", restart_case_name_);

    OB_LOGGER.set_file_name(
        log_file_name, true, false, rs_log_file_name, election_log_file_name, trace_log_file_name, audit_log_file_name);
    OB_LOGGER.set_log_level(log_level_);
    ::testing::InitGoogleTest(&argc_, argv_);

    ::testing::GTEST_FLAG(filter) = filter_str;
    ret = RUN_ALL_TESTS();

    fprintf(stdout, "Parent process exit ret = %d\n", ret);
  }

  return ret;
}
