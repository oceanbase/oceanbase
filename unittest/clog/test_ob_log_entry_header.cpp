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
#include "clog/ob_log_entry_header.h"
#include "share/ob_proposal_id.h"

using namespace oceanbase::common;
namespace oceanbase {
using namespace clog;
namespace unittest {
class TestObLogEntryHeader : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

TEST_F(TestObLogEntryHeader, test_ob_log_entry_header)
{
  const int64_t BUFSIZE = 1 << 11;
  ObLogEntryHeader header;
  ObLogEntryHeader header2;
  ObLogType ltype = OB_LOG_SUBMIT;
  ObPartitionKey partition_key;
  uint64_t log_id = 4;
  int64_t data_len = BUFSIZE >> 1;
  int64_t gts = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t tts = ::oceanbase::common::ObTimeUtility::current_time();
  ObProposalID rts;
  rts.ts_ = ::oceanbase::common::ObTimeUtility::current_time();
  uint64_t tid = 2;
  int32_t pid = 3;
  int32_t p_count = 10;
  int64_t pos = 0;
  char data[data_len];
  char buf[BUFSIZE];

  memset(data, 'A', data_len);
  partition_key.init(tid, pid, p_count);

  // test_generate_entry_header
  EXPECT_EQ(OB_SUCCESS,
      header.generate_header(
          OB_LOG_SUBMIT, partition_key, log_id, data, data_len, gts, tts, rts, 0, ObVersion(0), true));
  header2.shallow_copy(header);
  EXPECT_EQ(header.get_data_checksum(), header2.get_data_checksum());
  EXPECT_EQ(header.get_header_checksum(), header2.get_header_checksum());
  EXPECT_TRUE(header == header2);

  // test_check
  EXPECT_TRUE(header.check_data_checksum(data, data_len));
  EXPECT_TRUE(header.check_integrity(data, data_len));

  EXPECT_FALSE(header.check_data_checksum(data, 0));
  EXPECT_FALSE(header.check_data_checksum(NULL, data_len));
  EXPECT_FALSE(header.check_integrity(data, -1));  // check data checksum fail

  // test_get_and_set
  EXPECT_EQ(header2.get_magic_num(), header.get_magic_num());
  EXPECT_EQ(header2.get_version(), header.get_version());
  EXPECT_EQ(ltype, header.get_log_type());
  EXPECT_EQ(tid, header.get_partition_key().table_id_);
  EXPECT_EQ(pid, header.get_partition_key().get_partition_id());
  EXPECT_EQ(log_id, header.get_log_id());
  EXPECT_EQ(gts, header.get_generation_timestamp());
  EXPECT_EQ(tts, header.get_epoch_id());
  EXPECT_EQ(rts, header.get_proposal_id());
  EXPECT_EQ(data_len, header.get_data_len());
  EXPECT_EQ(data_len + header.get_serialize_size(), header.get_total_len());

  int64_t ts = ::oceanbase::common::ObTimeUtility::current_time();
  rts.ts_ = ts;
  header.set_epoch_id(ts);
  header.set_proposal_id(rts);
  EXPECT_EQ(ts, header.get_epoch_id());
  EXPECT_EQ(ts, header.get_proposal_id().ts_);

  // test_serialize_and_deserialize
  CLOG_LOG(INFO, "before serialize", "header", to_cstring(header2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header2.serialize(buf, 0, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header2.serialize(NULL, 0, pos));
  EXPECT_EQ(OB_SERIALIZE_ERROR, header2.serialize(buf, 1, pos));  // buf is too small
  EXPECT_EQ(OB_SUCCESS, header2.serialize(buf, BUFSIZE, pos));
  header2.reset();
  EXPECT_TRUE(header2.check_data_checksum(NULL, 0));
  EXPECT_FALSE(header2.check_integrity(buf, data_len));  // magic = 0
  EXPECT_NE(header.get_header_checksum(), header2.get_header_checksum());

  pos = 0;
  CLOG_LOG(INFO, "after reset", "header", to_cstring(header2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header2.deserialize(NULL, data_len, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header2.deserialize(buf, -1, pos));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, header2.deserialize(buf, 1, pos));  // buf is too small
  EXPECT_EQ(OB_SUCCESS, header2.deserialize(buf, data_len, pos));
  CLOG_LOG(INFO, "after serialize", "header", to_cstring(header2));

  EXPECT_TRUE(header2.check_integrity(data, data_len));

  buf[3] = 'B';
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, header2.deserialize(buf, data_len, pos));
  EXPECT_FALSE(header2.check_integrity(data, data_len));  // data checksum fail

  header2.to_string(buf, data_len);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_entry_header.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_entry_header");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
