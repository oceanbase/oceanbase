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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "share/ob_define.h"
#include "sql/ob_end_trans_callback.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/transaction/ob_trans_define.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::transaction;

namespace test {
class ObEndTransCallbacksTest : public ::testing::Test {
public:
  ObEndTransCallbacksTest()
  {}
  virtual ~ObEndTransCallbacksTest()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

private:
  // disallow copy
  ObEndTransCallbacksTest(const ObEndTransCallbacksTest& other);
  ObEndTransCallbacksTest& operator=(const ObEndTransCallbacksTest& other);

private:
  // data members
};
TEST_F(ObEndTransCallbacksTest, signal_wait)
{
  ObAddr observer(ObAddr::IPV4, "127.0.0.1", 8080);
  ObTransDesc trans_desc;

  ObEndTransSyncCallback callback;
  ASSERT_EQ(OB_SUCCESS, callback.init(&trans_desc, NULL));
  callback.callback(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, callback.wait());
}
}  // namespace test

int main(int argc, char** argv)
{

  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("DEBUG");
  return RUN_ALL_TESTS();
}
