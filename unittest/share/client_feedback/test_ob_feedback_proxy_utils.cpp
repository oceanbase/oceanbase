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

#define UNITTEST_DEBUG
#define private public
#include "observer/mysql/ob_feedback_proxy_utils.h"
#undef private
#include "src/sql/session/ob_sql_session_info.h"
#include <gtest/gtest.h>
#include <string>
#include <sstream>
#include <iostream>

namespace oceanbase
{
namespace unittest
{

using namespace observer;
using namespace common;
using namespace std;

bool verifyHexString(const char *data, const size_t data_size, const std::string &hex_string)
{
  // data length mismatch
  if (data_size * 2 != hex_string.size()) {
    return false;
  }

  std::ostringstream stream;
  for (size_t i = 0; i < data_size; ++i) {
    stream << std::hex << std::setfill('0') << std::setw(2) << (0xFF & static_cast<unsigned char>(data[i]));
  }

  return stream.str() == hex_string;
}

TEST(ObFeedbackProxyInfo, test_serialize)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  bool verified = false;

  ObIsLockSessionInfo is_lock_session(ObFeedbackProxyInfoType::IS_LOCK_SESSION, '1');
  ret = is_lock_session.serialize(buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 7);

  // result in hex
  std::string hex_string = "00000100000031";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, test_serialize_from_session)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  sql::ObSQLSessionInfo session;
  bool verified = false;

  session.set_is_lock_session(true);
  ret = ObFeedbackProxyUtils::serialize_(session, buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 7);

  // result in hex
  std::string hex_string = "00000100000031";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, test_serialize_from_kv)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  sql::ObSQLSessionInfo session;
  bool verified = false;

  ObSEArray<short, 3> test_array;
  test_array.push_back(1);
  test_array.push_back(2);
  test_array.push_back(3);
  ret = ObFeedbackProxyUtils::serialize_(ObFeedbackProxyInfoType::IS_LOCK_SESSION, test_array, buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 10);

  // result in hex
  std::string hex_string = "00000400000003010203";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_feedback_proxy_utils.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_feedback_proxy_utils.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
