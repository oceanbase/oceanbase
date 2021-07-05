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

#include "clog/ob_log_file_trailer.h"

#include <gtest/gtest.h>

using namespace oceanbase::clog;
using namespace oceanbase::common;

namespace oceanbase {
namespace unittest {

class TestObLogFileTrailer : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

TEST_F(TestObLogFileTrailer, test_ob_log_file_trailer)
{
  static const int64_t BUFSIZE = 1 << 9;
  ObLogFileTrailer trailer;
  offset_t start_pos = CLOG_TRAILER_OFFSET;
  offset_t start_pos2 = CLOG_TRAILER_OFFSET - 1;
  file_id_t file_id = 5;
  file_id_t file_id2 = OB_INVALID_FILE_ID;
  int64_t pos = 0;

  char buf[BUFSIZE];

  // test build_serialized_trailer failure
  EXPECT_EQ(OB_INVALID_ARGUMENT, trailer.build_serialized_trailer(buf, BUFSIZE, start_pos, file_id2, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, trailer.build_serialized_trailer(buf, BUFSIZE, -1, file_id, pos));

  // test_set
  EXPECT_EQ(OB_INVALID_ARGUMENT, trailer.set_file_id(file_id2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, trailer.set_start_pos(-1));
  EXPECT_EQ(OB_SUCCESS, trailer.set_file_id(file_id));
  EXPECT_EQ(OB_SUCCESS, trailer.set_start_pos(start_pos2));

  // test_serialize_and_deserialize
  CLOG_LOG(INFO, "before serialize", "trailer", to_cstring(trailer));
  EXPECT_EQ(OB_SUCCESS, trailer.build_serialized_trailer(buf, BUFSIZE, start_pos, file_id, pos));
  EXPECT_EQ(start_pos, trailer.get_start_pos());
  EXPECT_EQ(file_id, trailer.get_file_id());

  pos = 0;
  EXPECT_EQ(OB_SUCCESS, trailer.deserialize(buf, trailer.get_serialize_size(), pos));
  CLOG_LOG(INFO, "after deserialize", "trailer", to_cstring(trailer));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_file_trailer.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_file_trailer");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
