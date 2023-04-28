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
#include "lib/checksum/ob_crc64.h"          // ob_crc64
#define private public
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_entry.h"
#include "share/scn.h"
#include "logservice/ob_log_base_header.h"  // ObLogBaseHeader
#include "logservice/palf/log_group_buffer.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_writer_utils.h"
#include "share/rc/ob_tenant_base.h"
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

TEST(TestPaddingLogEntry, test_invalid_padding_log_entry)
{
  LogEntryHeader header;
  char buf[1024];
  EXPECT_EQ(OB_INVALID_ARGUMENT, header.generate_padding_header_(NULL, 1, 1, share::SCN::min_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header.generate_padding_header_(buf, 0, 1, share::SCN::min_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header.generate_padding_header_(buf, 1, 0, share::SCN::min_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, header.generate_padding_header_(buf, 1, 1, share::SCN::invalid_scn()));
  EXPECT_EQ(OB_SUCCESS, header.generate_padding_header_(buf, 1, 1, share::SCN::min_scn()));

  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(0, share::SCN::min_scn(), buf, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(1, share::SCN::invalid_scn(), buf, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(1, share::SCN::min_scn(), NULL, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(1, share::SCN::min_scn(), buf, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(1, share::SCN::min_scn(), buf, 2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(2, share::SCN::min_scn(), buf, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, LogEntryHeader::generate_padding_log_buf(1, share::SCN::min_scn(), buf, 1));
  logservice::ObLogBaseHeader base_header;
  const int64_t min_padding_valid_data_len = header.get_serialize_size() + base_header.get_serialize_size();
  EXPECT_EQ(OB_SUCCESS, LogEntryHeader::generate_padding_log_buf(1+min_padding_valid_data_len, share::SCN::min_scn(), buf, min_padding_valid_data_len));
}

TEST(TestPaddingLogEntry, test_padding_log_entry)
{
  PALF_LOG(INFO, "test_padding_log_entry");
  LogEntry padding_log_entry;
  const int64_t padding_data_len = MAX_LOG_BODY_SIZE;
  share::SCN padding_group_scn;
  padding_group_scn.convert_for_logservice(ObTimeUtility::current_time_ns());
  LogEntryHeader padding_log_entry_header;
  char base_header_data[1024] = {'\0'};
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::PADDING_LOG_BASE_TYPE,
                                          logservice::ObReplayBarrierType::NO_NEED_BARRIER);
  int64_t serialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, base_header.serialize(base_header_data, 1024, serialize_pos));
  // 生成padding log entry的有效数据
  EXPECT_EQ(OB_SUCCESS, padding_log_entry_header.generate_padding_header_(
      base_header_data, base_header.get_serialize_size(),
      padding_data_len-LogEntryHeader::HEADER_SER_SIZE, padding_group_scn));
  EXPECT_EQ(true, padding_log_entry_header.check_integrity(base_header_data, padding_data_len));

  // padding group log format
  // | GroupHeader | EntryHeader | BaseHeader | '\0' |
  LogGroupEntry padding_group_entry;
  LogGroupEntryHeader padding_group_entry_header;

  const int64_t padding_buffer_len = MAX_LOG_BUFFER_SIZE;
  char *padding_buffer = reinterpret_cast<char *>(ob_malloc(padding_buffer_len, "unittest"));
  ASSERT_NE(nullptr, padding_buffer);
  memset(padding_buffer, PADDING_LOG_CONTENT_CHAR, padding_buffer_len);
  {
    // 将base_data的数据拷贝到对应位置
    memcpy(padding_buffer+padding_group_entry_header.get_serialize_size() + LogEntryHeader::HEADER_SER_SIZE, base_header_data, 1024);
    padding_log_entry.header_ = padding_log_entry_header;
    padding_log_entry.buf_ = padding_buffer+padding_group_entry_header.get_serialize_size() + LogEntryHeader::HEADER_SER_SIZE;
    // 构造有效的padding_log_entry，用于后续的序列化操作
    EXPECT_EQ(true, padding_log_entry.check_integrity());
    EXPECT_EQ(true, padding_log_entry.header_.is_padding_log_());
  }
  {
    LogEntry deserialize_padding_log_entry;
    const int64_t tmp_padding_buffer_len = MAX_LOG_BUFFER_SIZE;
    char *tmp_padding_buffer = reinterpret_cast<char *>(ob_malloc(tmp_padding_buffer_len, "unittest"));
    ASSERT_NE(nullptr, tmp_padding_buffer);
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, padding_log_entry.serialize(tmp_padding_buffer, tmp_padding_buffer_len, pos));
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, deserialize_padding_log_entry.deserialize(tmp_padding_buffer, tmp_padding_buffer_len, pos));
    EXPECT_EQ(true, deserialize_padding_log_entry.check_integrity());
    EXPECT_EQ(padding_log_entry.header_.data_checksum_, deserialize_padding_log_entry.header_.data_checksum_);
    ob_free(tmp_padding_buffer);
    tmp_padding_buffer = nullptr;
  }

  serialize_pos = padding_group_entry_header.get_serialize_size();
  // 拷贝LogEntry到padding_buffer指定位置
  EXPECT_EQ(OB_SUCCESS, padding_log_entry.serialize(padding_buffer, padding_buffer_len, serialize_pos));

  LogWriteBuf write_buf;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(padding_buffer, padding_buffer_len));
  bool is_raw_write = false;
  bool is_padding_log = true;
  int64_t data_checksum = 0;
  EXPECT_EQ(OB_SUCCESS, padding_group_entry_header.generate(is_raw_write, is_padding_log, write_buf, padding_data_len,  padding_group_scn, 1, LSN(0), 1, data_checksum));
  padding_group_entry_header.update_accumulated_checksum(0);
  padding_group_entry_header.update_header_checksum();
  padding_group_entry.header_ = padding_group_entry_header;
  padding_group_entry.buf_ = padding_buffer + padding_group_entry_header.get_serialize_size();
  EXPECT_EQ(true, padding_group_entry.check_integrity());
  EXPECT_EQ(true, padding_group_entry.header_.is_padding_log());
  // 验证反序列化LogEntry
  {
    int64_t pos = 0;
    LogEntry tmp_padding_log_entry;
    EXPECT_EQ(OB_SUCCESS, tmp_padding_log_entry.deserialize(padding_group_entry.buf_, padding_group_entry.get_data_len(), pos));
    EXPECT_EQ(pos, padding_group_entry.get_data_len());
    EXPECT_EQ(true, tmp_padding_log_entry.check_integrity());
    EXPECT_EQ(true, tmp_padding_log_entry.header_.is_padding_log_());
    logservice::ObLogBaseHeader tmp_base_header;
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, tmp_base_header.deserialize(tmp_padding_log_entry.buf_, tmp_padding_log_entry.get_data_len(), pos));
    EXPECT_EQ(base_header.log_type_, logservice::ObLogBaseType::PADDING_LOG_BASE_TYPE);
  }

  char *serialize_buffer = reinterpret_cast<char *>(ob_malloc(padding_buffer_len, "unittest"));
  ASSERT_NE(nullptr, serialize_buffer);
  memset(serialize_buffer, PADDING_LOG_CONTENT_CHAR, padding_buffer_len);
  serialize_pos = 0;
  // 验证序列化的数据是否符合预期
  EXPECT_EQ(OB_SUCCESS, padding_group_entry.serialize(serialize_buffer, padding_buffer_len, serialize_pos));
  EXPECT_EQ(serialize_pos, padding_data_len+padding_group_entry_header.get_serialize_size());

  LogGroupEntry deserialize_group_entry;
  serialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, deserialize_group_entry.deserialize(serialize_buffer, padding_buffer_len, serialize_pos));
  EXPECT_EQ(true, deserialize_group_entry.check_integrity());
  EXPECT_EQ(true, deserialize_group_entry.header_.is_padding_log());
  EXPECT_EQ(padding_group_entry.header_, deserialize_group_entry.header_);

  // 验证反序列化LogEntry
  {
    int64_t pos = 0;
    LogEntry tmp_padding_log_entry;
    EXPECT_EQ(OB_SUCCESS, tmp_padding_log_entry.deserialize(deserialize_group_entry.buf_, padding_group_entry.get_data_len(), pos));
    EXPECT_EQ(pos, deserialize_group_entry.get_data_len());
    EXPECT_EQ(true, tmp_padding_log_entry.check_integrity());
    EXPECT_EQ(true, tmp_padding_log_entry.header_.is_padding_log_());
    logservice::ObLogBaseHeader tmp_base_header;
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, tmp_base_header.deserialize(tmp_padding_log_entry.buf_, tmp_padding_log_entry.get_data_len(), pos));
    EXPECT_EQ(base_header.log_type_, logservice::ObLogBaseType::PADDING_LOG_BASE_TYPE);
  }

  LogGroupBuffer group_buffer;
  LSN start_lsn(0);

  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  // init MTL
  share::ObTenantBase tbase(1001);
  share::ObTenantEnv::set_tenant(&tbase);
  EXPECT_EQ(OB_SUCCESS, group_buffer.init(start_lsn));

  const int64_t padding_valid_data_len = deserialize_group_entry.get_header().get_serialize_size() + padding_log_entry_header.get_serialize_size() + base_header.get_serialize_size();
  // 填充有效的padding日志到group buffer，验证数据是否相等
  // padding_buffer包括LogGruopEntryHeader, LogEntryHeader, ObLogBaseHeader, padding log body(is filled with '\0')
  EXPECT_EQ(OB_SUCCESS, group_buffer.fill_padding_body(start_lsn, serialize_buffer, padding_valid_data_len, padding_buffer_len));
  EXPECT_EQ(0, memcmp(group_buffer.data_buf_, serialize_buffer, deserialize_group_entry.get_serialize_size()));
  PALF_LOG(INFO, "runlin trace", K(group_buffer.data_buf_), K(serialize_buffer), K(padding_buffer_len), KP(group_buffer.data_buf_), KP(padding_buffer));
  ob_free(padding_buffer);
  padding_buffer = NULL;
  ob_free(serialize_buffer);
  serialize_buffer = NULL;

  group_buffer.destroy();
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
}

TEST(TestPaddingLogEntry, test_generate_padding_log_entry)
{
  PALF_LOG(INFO, "test_generate_padding_log_entry");
  LogEntry padding_log_entry;
  const int64_t padding_data_len = 1024;
  const share::SCN padding_scn = share::SCN::min_scn();
  const int64_t padding_log_entry_len = padding_data_len + LogEntryHeader::HEADER_SER_SIZE;
  char *out_buf = reinterpret_cast<char*>(ob_malloc(padding_log_entry_len, "unittest"));
  ASSERT_NE(nullptr, out_buf);
  LogEntryHeader padding_header;
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::PADDING_LOG_BASE_TYPE, logservice::ObReplayBarrierType::NO_NEED_BARRIER, 0);
  char base_header_buf[1024];
  memset(base_header_buf, 0, 1024);
  int64_t serialize_base_header_pos = 0;
  EXPECT_EQ(OB_SUCCESS, base_header.serialize(base_header_buf, 1024, serialize_base_header_pos));
  EXPECT_EQ(OB_SUCCESS, padding_header.generate_padding_header_(base_header_buf, base_header.get_serialize_size(), padding_data_len, padding_scn));
  EXPECT_EQ(true, padding_header.check_header_integrity());
  EXPECT_EQ(OB_SUCCESS, LogEntryHeader::generate_padding_log_buf(padding_data_len, padding_scn, out_buf, LogEntryHeader::PADDING_LOG_ENTRY_SIZE));
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, padding_log_entry.deserialize(out_buf, padding_log_entry_len, pos));
  EXPECT_EQ(true, padding_log_entry.check_integrity());
  EXPECT_EQ(true, padding_log_entry.header_.is_padding_log_());
  EXPECT_EQ(padding_log_entry.header_.data_checksum_, padding_header.data_checksum_);
  ob_free(out_buf);
  out_buf = nullptr;
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_log_entry_and_group_entry.log");
  OB_LOGGER.set_file_name("test_log_entry_and_group_entry.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_entry_and_group_entry");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
