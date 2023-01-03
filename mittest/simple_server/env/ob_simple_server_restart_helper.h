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

#include <string>
class ObSimpleServerRestartHelper
{
public:
  static bool is_restart_;
  static bool need_restart_;
  static int sleep_sec_;
  static const int name_len = 64;

public:
  ObSimpleServerRestartHelper(int argc,
                              char **argv,
                              const char *test_file_name,
                              const char *born_case_name,
                              const char *restart_case_name,
                              const char *log_level = "INFO")
    : argc_(argc),
      argv_(argv),
      test_file_name_(test_file_name),
      born_case_name_(born_case_name),
      restart_case_name_(restart_case_name),
      log_level_(log_level) {}

  int run();

  void set_sleep_sec(int t) { ObSimpleServerRestartHelper::sleep_sec_ = t; }

private:
  int argc_;
  char **argv_;
  const char *test_file_name_;
  const char *born_case_name_;
  const char *restart_case_name_;
  const char *log_level_;
};
