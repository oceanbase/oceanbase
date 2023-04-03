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

#include <cstdio>
#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h" // ObAddr
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_writer_utils.h"
#define private public
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_entry.h"
#include "share/scn.h"
#undef private

#include <gtest/gtest.h>

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

TEST(TestLogGroupEntryHeader, test_group_entry_header_wrap_checksum)
{
  const int64_t BUFSIZE = 1 << 21;
  LogGroupEntryHeader header;
  LogEntryHeader log_entry_header;
  int64_t group_entry_header_size = header.get_serialize_size();
  int64_t log_entry_header_size = log_entry_header.get_serialize_size();
  int64_t total_header_size = group_entry_header_size + log_entry_header_size;
  char buf[BUFSIZE];
  char ptr[BUFSIZE] = "helloworld";
  // 数据部分
  memcpy(buf + total_header_size, ptr, strlen(ptr));

  bool is_padding_log = false;
  const char *data = buf + total_header_size;
  int64_t data_len = strlen(ptr);
  memcpy(buf + total_header_size + data_len + log_entry_header_size, ptr, strlen(ptr));
  int64_t min_timestamp = 0;
  share::SCN max_scn = share::SCN::min_scn();
  int64_t log_id = 1;
  LSN committed_lsn;
  committed_lsn.val_ = 1;
  int64_t proposal_id = 1;
  int64_t log_checksum = 0;

  // test LogEntry and LogEntryHeader
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(data, data_len, share::SCN::base_scn()));
  int64_t tmp_pos = 0, new_pos = 0;
  EXPECT_EQ(OB_SUCCESS,
            log_entry_header.serialize(buf + group_entry_header_size, BUFSIZE, tmp_pos));
  EXPECT_EQ(tmp_pos, log_entry_header_size);
  EXPECT_EQ(OB_SUCCESS,
            log_entry_header.serialize(buf + total_header_size + data_len, BUFSIZE, new_pos));
  EXPECT_EQ(new_pos, log_entry_header_size);
  // test LogGroupEntryHeader and LogEntry
  LogWriteBuf write_buf;

  int64_t group_log_data_len = 0;
  int64_t group_log_len = group_entry_header_size + (log_entry_header_size + data_len);
  for (int64_t sub_val = 1; sub_val < group_log_len; ++sub_val) {
    write_buf.reset();
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, sub_val));
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf + sub_val,  group_log_len - sub_val));
    group_log_data_len = group_log_len - group_entry_header_size;
    PALF_LOG(INFO, "before group_header generate", K(group_log_data_len), K(write_buf), K(sub_val));
    EXPECT_EQ(OB_SUCCESS,
              header.generate(false, is_padding_log, write_buf, group_log_data_len,
                              max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  }

  is_padding_log = true;
  for (int64_t sub_val = 1; sub_val < group_log_len; ++sub_val) {
    write_buf.reset();
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, sub_val));
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf + sub_val,  group_log_len - sub_val));
    group_log_data_len = group_log_len - group_entry_header_size;
    PALF_LOG(INFO, "before group_header generate", K(group_log_data_len), K(write_buf), K(sub_val));
    EXPECT_EQ(OB_SUCCESS,
              header.generate(false, is_padding_log, write_buf, group_log_data_len,
                              max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  }

  group_log_len = group_entry_header_size + 2 * (log_entry_header_size + data_len);
  for (int64_t sub_val = 1; sub_val < group_log_len; ++sub_val) {
    write_buf.reset();
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, sub_val));
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf + sub_val,  group_log_len - sub_val));
    group_log_data_len = group_log_len - group_entry_header_size;
    PALF_LOG(INFO, "before group_header generate", K(group_log_data_len), K(write_buf), K(sub_val));
    EXPECT_EQ(OB_SUCCESS,
              header.generate(false, is_padding_log, write_buf, group_log_data_len,
                              max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  }

  is_padding_log = true;
  for (int64_t sub_val = 1; sub_val < group_log_len; ++sub_val) {
    write_buf.reset();
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, sub_val));
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf + sub_val,  group_log_len - sub_val));
    group_log_data_len = group_log_len - group_entry_header_size;
    PALF_LOG(INFO, "before group_header generate", K(group_log_data_len), K(write_buf), K(sub_val));
    EXPECT_EQ(OB_SUCCESS,
              header.generate(false, is_padding_log, write_buf, group_log_data_len,
                              max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  }

  is_padding_log = true;
  EXPECT_EQ(OB_SUCCESS,
            header.generate(false, is_padding_log, write_buf, group_log_data_len,
                            max_scn, log_id, committed_lsn, proposal_id, log_checksum));
}

TEST(TestLogGroupEntryHeader, test_log_group_entry_header)
{
  const int64_t BUFSIZE = 1 << 21;
  LogGroupEntryHeader header;
  LogEntryHeader log_entry_header;
  int64_t log_group_entry_header_size = header.get_serialize_size();
  int64_t log_entry_header_size = log_entry_header.get_serialize_size();
  int64_t header_size = log_group_entry_header_size + log_entry_header_size;
  char buf[BUFSIZE];
  char ptr[BUFSIZE] = "helloworld";
  // 数据部分
  memcpy(buf + header_size, ptr, strlen(ptr));

  LogType log_type = palf::LOG_PADDING;
  const bool is_padding_log = (LOG_PADDING == log_type) ? true : false;
  const char *data = buf + header_size;
  int64_t data_len = strlen(ptr);
  int64_t min_timestamp = 0;
  share::SCN max_scn = share::SCN::min_scn();
  int64_t log_id = 1;
  LSN committed_lsn;
  committed_lsn.val_ = 1;
  int64_t proposal_id = 1;
  int64_t log_checksum = 0;

  // test LogEntry and LogEntryHeader
  LogEntry log_entry;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_entry_header.generate_header(NULL, 0, share::SCN::base_scn()));
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(data, data_len, share::SCN::base_scn()));
  log_entry.header_ = log_entry_header;
  log_entry.buf_ = data;
  int64_t tmp_pos = 0;
  EXPECT_EQ(OB_SUCCESS,
            log_entry_header.serialize(buf + log_group_entry_header_size, BUFSIZE, tmp_pos));
  EXPECT_EQ(tmp_pos, log_entry_header_size);
  // test LogGroupEntryHeader and LogEntry
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, data_len + header_size));
  PALF_LOG(INFO, "runlin trace", K(tmp_pos), K(log_entry), K(write_buf),
           K(write_buf.get_total_size()));
  max_scn.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            header.generate(false, is_padding_log, write_buf, data_len + log_entry_header_size,
                            max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  max_scn.set_base();
  int64_t defalut_acc = 10;
  min_timestamp = 1;
  EXPECT_EQ(OB_SUCCESS,
            header.generate(false, is_padding_log, write_buf, data_len + log_entry_header_size,
                            max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  header.update_accumulated_checksum(defalut_acc);
  header.update_header_checksum();
  EXPECT_TRUE(
      header.check_integrity(buf + log_group_entry_header_size, data_len + log_entry_header_size));
  EXPECT_TRUE(header.is_valid());
  EXPECT_EQ(data_len + log_entry_header_size, header.get_data_len());
  EXPECT_EQ(max_scn, header.get_max_scn());
  EXPECT_EQ(log_id, header.get_log_id());
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, header.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, header.get_serialize_size());
  pos = 0;
  LogGroupEntryHeader header1;
  EXPECT_EQ(OB_SUCCESS, header1.deserialize(buf, BUFSIZE, pos));
  EXPECT_TRUE(
      header1.check_integrity(buf + log_group_entry_header_size, data_len + log_entry_header_size));
  EXPECT_TRUE(header1 == header);
  int64_t new_proposal_id = 1000;
  LSN new_lsn(10000);
  EXPECT_EQ(OB_SUCCESS, header1.update_log_proposal_id(new_proposal_id));
  EXPECT_EQ(OB_SUCCESS, header1.update_committed_end_lsn(new_lsn));
  EXPECT_EQ(new_proposal_id, header1.get_log_proposal_id());
  EXPECT_EQ(new_lsn, header1.get_committed_end_lsn());
  // header1.update_header_checksum();
  EXPECT_TRUE(
      header1.check_integrity(buf + log_group_entry_header_size, data_len + log_entry_header_size));

  LogGroupEntry log_group_entry, log_group_entry1, log_group_entry2;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_entry.generate(header, NULL));
  EXPECT_EQ(OB_SUCCESS, log_group_entry.generate(header, buf + log_group_entry_header_size));
  EXPECT_TRUE(log_group_entry.is_valid());
  EXPECT_EQ(OB_SUCCESS, log_group_entry1.shallow_copy(log_group_entry));
  EXPECT_EQ(log_group_entry1.get_header(), log_group_entry.get_header());
  EXPECT_EQ(log_group_entry1.get_header_size(), log_group_entry.get_header_size());
  EXPECT_EQ(data_len + log_entry_header_size, log_group_entry.get_data_len());
  EXPECT_TRUE(
      header1.check_integrity(buf + log_group_entry_header_size, data_len + log_entry_header_size));
  EXPECT_EQ(max_scn, log_group_entry.get_scn());
  EXPECT_EQ(committed_lsn, log_group_entry.get_committed_end_lsn());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_group_entry.serialize(buf, BUFSIZE, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_group_entry2.deserialize(buf, BUFSIZE, pos));
  EXPECT_TRUE(log_group_entry2.check_integrity());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_entry_and_group_entry.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_entry_and_grou_entry");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
