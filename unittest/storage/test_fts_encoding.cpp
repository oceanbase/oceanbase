/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/oblog/ob_log.h"
#include <gtest/gtest.h>

#include "share/ob_fts_pos_list_codec.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_fts_index_builder_util.h"
#define USING_LOG_PREFIX STORAGE

#define protected public
#define private   public

namespace oceanbase
{

namespace storage
{

class TestObFtsPosListEncoding : public ::testing::Test
{
public:
  TestObFtsPosListEncoding() = default;
  ~TestObFtsPosListEncoding() = default;

  void SetUp() override
  {
    // allocator_ is initialized with default constructor
  }

  void TearDown() override
  {
    allocator_.reset();
  }

protected:
  common::ObArenaAllocator allocator_;
};

TEST_F(TestObFtsPosListEncoding, test_codec_easy_api) {
  int ret = OB_SUCCESS;

  share::ObFTSPositionListStore store;

  common::ObArray<int64_t> pos_list;
  for (int64_t i = 0; i < share::ObFTSConstants::MAX_POSITION_LIST_COUNT - 1; ++i) {
    pos_list.push_back(i);
  }

  // encode the pos list
  char *buf = nullptr;
  int64_t buf_len = 0;
  EXPECT_EQ(store.encode_and_serialize(allocator_, pos_list, buf, buf_len), OB_SUCCESS);

  // deserialize and decode the pos list
  common::ObArray<int64_t> decoded_pos_list;
  EXPECT_EQ(store.deserialize_and_decode(buf, buf_len, decoded_pos_list), OB_SUCCESS);
  ASSERT_EQ(decoded_pos_list.count(), pos_list.count());
  for (int64_t i = 0; i < pos_list.count(); ++i) {
    ASSERT_EQ(decoded_pos_list[i], pos_list[i]);
  }
  ASSERT_EQ(ret, OB_SUCCESS);


}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_fts_plugin.log");
  OB_LOGGER.set_file_name("test_fts_encoding.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
