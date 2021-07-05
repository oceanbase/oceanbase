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

#include "storage/transaction/ob_mask_set.h"
#include "storage/transaction/ob_trans_end_trans_callback.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "sql/ob_end_trans_callback.h"

namespace oceanbase {
using namespace transaction;
using namespace common;

namespace sql {
class MockEndTransCallback : public ObExclusiveEndTransCallback {
public:
  MockEndTransCallback()
  {}
  ~MockEndTransCallback()
  {}
  virtual void callback(int cb_param)
  {
    UNUSED(cb_param);
  }
  virtual void callback(int cb_param, const transaction::ObTransID& trans_id)
  {
    UNUSED(cb_param);
    UNUSED(trans_id);
  }
  virtual const char* get_type() const
  {
    return "";
  }
  virtual ObEndTransCallbackType get_callback_type() const
  {
    return MOCK_CALLBACK_TYPE;
  }
};
}  // namespace sql

namespace unittest {
class TestObEndTransCallback : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static const int64_t PARTITION_KEY_COUNT = 16;
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;
};

using sql::MockEndTransCallback;

//////////////////////basic function test//////////////////////////////////////////
// test reset
TEST_F(TestObEndTransCallback, reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockEndTransCallback mock_cb;

  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));
  end_trans_callback.reset();
  EXPECT_EQ(NULL, end_trans_callback.get_cb());
}

// test destroy
TEST_F(TestObEndTransCallback, destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockEndTransCallback mock_cb;
  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));
  end_trans_callback.destroy();
  EXPECT_EQ(NULL, end_trans_callback.get_cb());
}

// test need_callback
TEST_F(TestObEndTransCallback, need_callback)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockEndTransCallback mock_cb;

  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));
  EXPECT_EQ(&mock_cb, end_trans_callback.get_cb());
  EXPECT_TRUE(end_trans_callback.need_callback());

  EXPECT_EQ(OB_SUCCESS, end_trans_callback.callback(OB_SUCCESS));
  EXPECT_FALSE(end_trans_callback.need_callback());

  ObEndTransCallback end_trans_callback_null;
  EXPECT_EQ(OB_INVALID_ARGUMENT, end_trans_callback.init(1000, NULL));
  EXPECT_EQ(OB_ERR_UNEXPECTED, end_trans_callback_null.callback(OB_SUCCESS));
  EXPECT_FALSE(end_trans_callback_null.need_callback());
}

// test callback
TEST_F(TestObEndTransCallback, callback)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  MockEndTransCallback mock_cb;

  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.callback(OB_SUCCESS));
}

// test to_string
TEST_F(TestObEndTransCallback, to_string)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockEndTransCallback mock_cb;
  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));

  char buf[100] = {0};
  const int64_t buf_len = 100;
  EXPECT_EQ(58, end_trans_callback.to_string(buf, buf_len));
}

//////////////////////////boundary test/////////////////////////////////////////
// test the init failure of ObEndTransCallback
TEST_F(TestObEndTransCallback, init_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObEndTransCallback end_trans_callback_null;
  EXPECT_EQ(OB_INVALID_ARGUMENT, end_trans_callback_null.init(1000, NULL));

  MockEndTransCallback mock_cb;
  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback_null.init(1000, &mock_cb));
  EXPECT_EQ(OB_INIT_TWICE, end_trans_callback_null.init(1000, &mock_cb));
}

// callback fail
TEST_F(TestObEndTransCallback, callback_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // NULL == cb_
  ObEndTransCallback end_trans_callback_null;
  EXPECT_EQ(OB_INVALID_ARGUMENT, end_trans_callback_null.init(1000, NULL));
  EXPECT_EQ(OB_ERR_UNEXPECTED, end_trans_callback_null.callback(OB_SUCCESS));

  // callback_count_ >= 1
  MockEndTransCallback mock_cb;
  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.callback(OB_SUCCESS));
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));
  EXPECT_EQ(&mock_cb, end_trans_callback.get_cb());
  EXPECT_EQ(OB_ERR_UNEXPECTED, end_trans_callback.callback(OB_SUCCESS));
}

// to_string  fail
TEST_F(TestObEndTransCallback, to_string_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockEndTransCallback mock_cb;
  ObEndTransCallback end_trans_callback;
  EXPECT_EQ(OB_SUCCESS, end_trans_callback.init(1000, &mock_cb));

  // test to_string(char *buf, const int64_t buf_len) buf = null
  char* buf_null = NULL;
  const int64_t buf_null_len = 100;
  EXPECT_EQ(0, end_trans_callback.to_string(buf_null, buf_null_len));

  // test to_string(char *buf, const int64_t buf_len) buf_len <= 0
  char buf[100] = {0};
  const int64_t invalid_buf_len = 0;
  EXPECT_EQ(0, end_trans_callback.to_string(buf, invalid_buf_len));
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_end_tran_callback.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
