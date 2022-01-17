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

#include "clog/ob_info_block_handler.h"

#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
using namespace clog;
namespace unittest {
class ObInfoBlockHandlerTest : public testing::Test {
public:
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, commit_handler_.init());
    EXPECT_EQ(OB_SUCCESS, index_handler_.init());
  }
  static void TearDownTestCase()
  {
    commit_handler_.destroy();
    index_handler_.destroy();
  }
  static ObCommitInfoBlockHandler commit_handler_;
  static ObIndexInfoBlockHandler index_handler_;
};

ObCommitInfoBlockHandler ObInfoBlockHandlerTest::commit_handler_;
ObIndexInfoBlockHandler ObInfoBlockHandlerTest::index_handler_;

TEST_F(ObInfoBlockHandlerTest, commit_info_block_handler)
{
  const int64_t MAX_BUF_LEN = 512 * 1024;
  char buf[MAX_BUF_LEN];

  common::ObPartitionKey key1(1099511627777, 1, 1);
  common::ObPartitionKey key2(1099511627778, 1, 1);
  common::ObPartitionKey key3(1099511627779, 1, 1);

  commit_handler_.update_info(key1, 10, 100);
  commit_handler_.update_info(key1, 11, 110);
  commit_handler_.update_info(key1, 100, 1000);

  commit_handler_.update_info(key2, 20, 200);
  commit_handler_.update_info(key2, 21, 210);
  commit_handler_.update_info(key2, 200, 2000);

  commit_handler_.update_info(key3, 30, 300);
  commit_handler_.update_info(key3, 31, 310);
  commit_handler_.update_info(key3, 300, 3000);

  EXPECT_EQ(3, commit_handler_.get_entry_cnt());

  uint64_t max_log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, commit_handler_.get_max_log_id(key1, max_log_id));
  EXPECT_EQ(100, max_log_id);
  int64_t max_submit_timestamp = 0;
  EXPECT_EQ(OB_SUCCESS, commit_handler_.get_max_submit_timestamp(max_submit_timestamp));
  EXPECT_EQ(3000, max_submit_timestamp);

  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, commit_handler_.build_info_block(buf, MAX_BUF_LEN, pos));
  EXPECT_NE(0, pos);

  ObCommitInfoBlockHandler tmp_handler;
  EXPECT_EQ(OB_SUCCESS, tmp_handler.init());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, tmp_handler.resolve_info_block(buf, MAX_BUF_LEN, pos));
  EXPECT_NE(0, pos);
  EXPECT_EQ(3, tmp_handler.get_entry_cnt());

  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_log_id(key1, max_log_id));
  EXPECT_EQ(100, max_log_id);
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_log_id(key2, max_log_id));
  EXPECT_EQ(200, max_log_id);
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_log_id(key3, max_log_id));
  EXPECT_EQ(300, max_log_id);

  max_submit_timestamp = 0;
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_submit_timestamp(max_submit_timestamp));
  EXPECT_EQ(3000, max_submit_timestamp);

  tmp_handler.destroy();
}

TEST_F(ObInfoBlockHandlerTest, index_info_block_handler)
{
  const int64_t MAX_BUF_LEN = 512 * 1024;
  char buf[MAX_BUF_LEN];

  common::ObPartitionKey key1(1099511627777, 1, 1);
  common::ObPartitionKey key2(1099511627778, 1, 1);
  common::ObPartitionKey key3(1099511627779, 1, 1);
  common::ObPartitionKey key4(1099511627780, 1, 1);

  uint64_t min_log_id = OB_INVALID_ID;
  uint64_t max_log_id = OB_INVALID_ID;

  index_handler_.update_info(key1, 10, 100);
  index_handler_.update_info(key1, 11, 110);
  index_handler_.update_info(key1, 100, 1000);

  index_handler_.update_info(key2, 20, 200);
  index_handler_.update_info(key2, 21, 210);
  index_handler_.update_info(key2, 200, 2000);

  index_handler_.update_info(key3, 30, 300);
  index_handler_.update_info(key3, 31, 310);
  index_handler_.update_info(key3, 300, 3000);

  EXPECT_EQ(3, index_handler_.get_entry_cnt());

  EXPECT_EQ(OB_SUCCESS, index_handler_.get_max_log_id(key1, max_log_id));
  EXPECT_EQ(100, max_log_id);

  EXPECT_EQ(OB_SUCCESS, index_handler_.get_min_log_id(key1, min_log_id));
  EXPECT_EQ(10, min_log_id);
  EXPECT_EQ(OB_SUCCESS, index_handler_.get_min_log_id(key2, min_log_id));
  EXPECT_EQ(20, min_log_id);
  EXPECT_EQ(OB_SUCCESS, index_handler_.get_min_log_id(key3, min_log_id));
  EXPECT_EQ(30, min_log_id);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, index_handler_.get_min_log_id(key4, min_log_id));

  int64_t max_submit_timestamp = 0;
  EXPECT_EQ(OB_SUCCESS, index_handler_.get_max_submit_timestamp(max_submit_timestamp));
  EXPECT_EQ(3000, max_submit_timestamp);

  ObIndexInfoBlockHandler::MinLogIdInfo min_log_id_info;
  EXPECT_EQ(OB_SUCCESS, index_handler_.get_all_min_log_id_info(min_log_id_info));
  EXPECT_EQ(3, min_log_id_info.size());
  EXPECT_EQ(OB_SUCCESS, min_log_id_info.get(key1, min_log_id));
  EXPECT_EQ(10, min_log_id);
  EXPECT_EQ(OB_SUCCESS, min_log_id_info.get(key2, min_log_id));
  EXPECT_EQ(20, min_log_id);
  EXPECT_EQ(OB_SUCCESS, min_log_id_info.get(key3, min_log_id));
  EXPECT_EQ(30, min_log_id);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, min_log_id_info.get(key4, min_log_id));

  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, index_handler_.build_info_block(buf, MAX_BUF_LEN, pos));
  EXPECT_NE(0, pos);

  ObIndexInfoBlockHandler tmp_handler;
  EXPECT_EQ(OB_SUCCESS, tmp_handler.init());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, tmp_handler.resolve_info_block(buf, MAX_BUF_LEN, pos));
  EXPECT_NE(0, pos);
  EXPECT_EQ(3, tmp_handler.get_entry_cnt());

  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_log_id(key1, max_log_id));
  EXPECT_EQ(100, max_log_id);
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_log_id(key2, max_log_id));
  EXPECT_EQ(200, max_log_id);
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_log_id(key3, max_log_id));
  EXPECT_EQ(300, max_log_id);

  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_min_log_id(key1, min_log_id));
  EXPECT_EQ(10, min_log_id);
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_min_log_id(key2, min_log_id));
  EXPECT_EQ(20, min_log_id);
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_min_log_id(key3, min_log_id));
  EXPECT_EQ(30, min_log_id);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, tmp_handler.get_min_log_id(key4, min_log_id));

  max_submit_timestamp = 0;
  EXPECT_EQ(OB_SUCCESS, tmp_handler.get_max_submit_timestamp(max_submit_timestamp));
  EXPECT_EQ(3000, max_submit_timestamp);
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_info_block_handler.log", true);
  OB_LOGGER.set_log_level("TRACE");
  CLOG_LOG(INFO, "begin unittest: test_info_block_handler");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
