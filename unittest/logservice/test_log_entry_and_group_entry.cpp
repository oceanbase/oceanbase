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

#include "lib/checksum/ob_parity_check.h"          // ob_crc64
#define private public
#include "logservice/palf/log_entry.h"
#include "logservice/ob_log_base_header.h"  // ObLogBaseHeader
#include "logservice/palf/log_group_buffer.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_writer_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_cluster_version.h"
#undef private

#include <gtest/gtest.h>
#include <random>

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
  header1.update_header_checksum();
  EXPECT_TRUE(header1.check_header_integrity());
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

int16_t get_log_entry_header_parity_checksum(const LogEntryHeader &header)
{
  bool bool_ret = parity_check(reinterpret_cast<const uint16_t &>(header.magic_));
  bool_ret ^= parity_check(reinterpret_cast<const uint16_t &>(header.version_));
  bool_ret ^= parity_check(reinterpret_cast<const uint32_t &>(header.log_size_));
  bool_ret ^= parity_check((header.scn_.get_val_for_logservice()));
  bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(header.data_checksum_));
  int64_t tmp_flag = (header.flag_ & ~(0x0001));
  bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(tmp_flag));
  PALF_LOG(INFO, "get_log_entry_header_parity_checksum", K(header), K(tmp_flag));
  return bool_ret ? 1 : 0;
}

int16_t get_log_group_entry_header_parity_checksum(const LogGroupEntryHeader &header)
{
  bool bool_ret = parity_check(reinterpret_cast<const uint16_t &>(header.magic_));
  bool_ret ^= parity_check(reinterpret_cast<const uint16_t &>(header.version_));
  bool_ret ^= parity_check(reinterpret_cast<const uint32_t &>(header.group_size_));
  bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(header.proposal_id_));
  bool_ret ^= parity_check(header.committed_end_lsn_.val_);
  bool_ret ^= parity_check(header.max_scn_.get_val_for_logservice());
  bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(header.accumulated_checksum_));
  bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(header.log_id_));
  int64_t tmp_flag = (header.flag_ & ~(0x1));
  bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(tmp_flag));
  return bool_ret ? 1 : 00;
}

TEST(TestUpgraedCompatibility, test_log_entry_header)
{
  PALF_LOG(INFO, "TestUpgraedCompatibility");
  {
    PALF_LOG(INFO, "case1. test_log_entry");
    {
      // 新版本解析旧版本数据
      LogEntryHeader ori_header;
      constexpr int64_t data_len = 100;
      char log_data[data_len] = "helloworld";
      EXPECT_EQ(OB_SUCCESS, ori_header.generate_header(log_data, data_len, share::SCN::min_scn()));
      EXPECT_EQ(true, ori_header.check_header_integrity());
      EXPECT_EQ(true, ori_header.check_integrity(log_data, data_len));
      ori_header.version_ = LogEntryHeader::LOG_ENTRY_HEADER_VERSION;
      ori_header.flag_ &= ~LogEntryHeader::CRC16_MASK;
      ori_header.flag_ |= get_log_entry_header_parity_checksum(ori_header);
      constexpr int64_t serialize_buf_len = data_len + sizeof(LogEntryHeader);
      char serialize_buf[serialize_buf_len];
      int64_t pos = 0;
      EXPECT_EQ(OB_SUCCESS, ori_header.serialize(serialize_buf, serialize_buf_len, pos));
      pos = 0;
      LogEntryHeader new_header;
      EXPECT_EQ(OB_SUCCESS, new_header.deserialize(serialize_buf, serialize_buf_len, pos));
      EXPECT_EQ(true, new_header.check_header_integrity());
      EXPECT_EQ(true, new_header.check_integrity(log_data, data_len));
      EXPECT_EQ(new_header.flag_, ori_header.flag_);
      EXPECT_EQ(new_header.data_checksum_, ori_header.data_checksum_);
      EXPECT_EQ(new_header.version_, ori_header.version_);

      // 验证padding日志
      ori_header.version_ = LogEntryHeader::LOG_ENTRY_HEADER_VERSION2;
      ori_header.flag_ &= ~LogEntryHeader::CRC16_MASK;
      ori_header.flag_ |= LogEntryHeader::PADDING_TYPE_MASK_VERSION2;
      ori_header.update_header_checksum_();
      ori_header.update_header_checksum_();
      EXPECT_EQ(true, ori_header.is_padding_log_());
      EXPECT_EQ(true, ori_header.check_header_integrity());
      pos = 0;
      EXPECT_EQ(OB_SUCCESS, ori_header.serialize(serialize_buf, serialize_buf_len, pos));
      pos = 0;
      new_header.reset();
      EXPECT_EQ(OB_SUCCESS, new_header.deserialize(serialize_buf, serialize_buf_len, pos));
      EXPECT_EQ(true, new_header.check_header_integrity());
      EXPECT_EQ(true, new_header.is_padding_log_());

      ori_header.version_ = LogEntryHeader::LOG_ENTRY_HEADER_VERSION;
      ori_header.flag_ &= ~LogEntryHeader::CRC16_MASK;
      ori_header.flag_ |= LogEntryHeader::PADDING_TYPE_MASK;
      ori_header.flag_ |= get_log_entry_header_parity_checksum(ori_header);

      pos = 0;
      EXPECT_EQ(OB_SUCCESS, ori_header.serialize(serialize_buf, serialize_buf_len, pos));
      new_header.reset();
      pos = 0;
      EXPECT_EQ(OB_SUCCESS, new_header.deserialize(serialize_buf, serialize_buf_len, pos));
      EXPECT_EQ(true, new_header.check_header_integrity());
      EXPECT_EQ(true, new_header.is_padding_log_());
      PALF_LOG(INFO, "print new_header", K(new_header), "is_padding", new_header.is_padding_log_());
      EXPECT_EQ(new_header.flag_, ori_header.flag_);
      EXPECT_EQ(new_header.data_checksum_, ori_header.data_checksum_);
      EXPECT_EQ(new_header.version_, ori_header.version_);
    }
    PALF_LOG(INFO, "case2. test_log_group_entry");
    {
      // 构造三条LogEntry，其version分别为2 1 2
      // | GroupHeader version x | EntryHeader version 2 | EntryHeader version1 | EntryHeader version2 |
      constexpr int64_t log_group_buf_len = 4096;
      char log_group_buf[log_group_buf_len] = {0};
      memset(log_group_buf, log_group_buf_len, 'c');
      LogEntryHeader log_entry_header1;
      constexpr int64_t data_len = 100;
      EXPECT_EQ(OB_SUCCESS, log_entry_header1.generate_header(log_group_buf + sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader), data_len, share::SCN::min_scn()));
      EXPECT_EQ(true, log_entry_header1.check_header_integrity());
      int64_t pos = 0;
      EXPECT_EQ(OB_SUCCESS, log_entry_header1.serialize(log_group_buf + sizeof(LogGroupEntryHeader), log_group_buf_len - sizeof(LogGroupEntryHeader), pos));

      PALF_LOG(INFO, "print log_entry_header1", K(log_entry_header1));
      LogEntryHeader log_entry_header2;
      pos = 0;
      EXPECT_EQ(OB_SUCCESS, log_entry_header2.deserialize(log_group_buf + sizeof(LogGroupEntryHeader), log_group_buf_len - sizeof(LogGroupEntryHeader), pos));
      EXPECT_EQ(true, log_entry_header2.check_header_integrity());
      EXPECT_EQ(true, log_entry_header2.check_integrity(log_group_buf + sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader), data_len));
      EXPECT_EQ(log_entry_header2.flag_, log_entry_header1.flag_);
      EXPECT_EQ(log_entry_header2.data_checksum_, log_entry_header1.data_checksum_);
      EXPECT_EQ(log_entry_header2.version_, log_entry_header1.version_);
      log_entry_header2.version_ = LogEntryHeader::LOG_ENTRY_HEADER_VERSION;
      log_entry_header2.flag_ = 0;
      log_entry_header2.flag_ |= get_log_entry_header_parity_checksum(log_entry_header2);
      PALF_LOG(INFO, "print log_entry_header2", K(log_entry_header2));
      EXPECT_EQ(true, log_entry_header2.check_header_integrity());
      EXPECT_EQ(true, log_entry_header2.check_integrity(log_group_buf + sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader), data_len));
      pos = sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader) + data_len;
      EXPECT_EQ(OB_SUCCESS, log_entry_header2.serialize(log_group_buf, log_group_buf_len, pos));

      LogEntryHeader log_entry_header3;
      pos = 0;
      EXPECT_EQ(OB_SUCCESS, log_entry_header3.deserialize(log_group_buf + sizeof(LogGroupEntryHeader), log_group_buf_len - sizeof(LogGroupEntryHeader), pos));
      PALF_LOG(INFO, "print log_entry_header3", K(log_entry_header3));
      EXPECT_EQ(true, log_entry_header3.check_header_integrity());
      EXPECT_EQ(true, log_entry_header3.check_integrity(log_group_buf + sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader), data_len));
      EXPECT_EQ(log_entry_header3.flag_, log_entry_header1.flag_);
      EXPECT_EQ(log_entry_header3.data_checksum_, log_entry_header1.data_checksum_);
      EXPECT_EQ(log_entry_header3.version_, log_entry_header1.version_);
      EXPECT_EQ(true, log_entry_header3.check_header_integrity());
      EXPECT_EQ(true, log_entry_header3.check_integrity(log_group_buf + sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader), data_len));
      EXPECT_EQ(true, log_entry_header3.check_header_integrity());
      pos = sizeof(LogGroupEntryHeader) + sizeof(LogEntryHeader) + data_len + sizeof(LogEntryHeader) + data_len;
      EXPECT_EQ(OB_SUCCESS, log_entry_header3.serialize(log_group_buf, log_group_buf_len, pos));

      pos = 0;
      LogGroupEntryHeader group_header;
      constexpr int64_t serialize_buf_len = 3*(data_len + sizeof(LogEntryHeader));
      LogWriteBuf write_buf;
      write_buf.push_back(log_group_buf, serialize_buf_len + sizeof(LogGroupEntryHeader));
      int64_t log_checksum = 0;
      EXPECT_EQ(OB_SUCCESS,
                group_header.generate(false, false, write_buf, serialize_buf_len,
                                share::SCN::min_scn(), 1, LSN(0), 1, log_checksum));
      group_header.update_accumulated_checksum(1024);
      group_header.update_header_checksum();
      group_header.update_header_checksum();
      EXPECT_EQ(true, group_header.check_header_integrity());
      EXPECT_EQ(true, group_header.check_integrity(log_group_buf+sizeof(LogGroupEntryHeader), serialize_buf_len));
      EXPECT_EQ(false, group_header.is_raw_write());
      EXPECT_EQ(false, group_header.is_padding_log());
      char log_group_serialize_buf[log_group_buf_len] = {0};
      pos = 0;
      EXPECT_EQ(OB_SUCCESS, group_header.serialize(log_group_serialize_buf, log_group_buf_len, pos));
      LogGroupEntryHeader new_group_header;
      pos = 0;
      EXPECT_EQ(OB_SUCCESS, new_group_header.deserialize(log_group_serialize_buf, log_group_buf_len, pos));
      EXPECT_EQ(true, new_group_header.check_header_integrity());
      EXPECT_EQ(true, new_group_header.check_integrity(log_group_buf+sizeof(LogGroupEntryHeader), serialize_buf_len));
      EXPECT_EQ(false, new_group_header.is_raw_write());
      EXPECT_EQ(false, new_group_header.is_padding_log());

      // pair <is_raw_write, is_padding_log>
      std::vector<std::pair<bool, bool>> input{{true, false}, {false, true}, {true, true}};

      for (auto elem : input) {
        LogGroupEntryHeader tmp_header = group_header;
        tmp_header.reset();
        EXPECT_EQ(OB_SUCCESS,
                  tmp_header.generate(elem.first, elem.second, write_buf, serialize_buf_len,
                                  share::SCN::min_scn(), 1, LSN(0), 1, log_checksum));
        tmp_header.update_accumulated_checksum(1023);
        tmp_header.update_header_checksum();
        tmp_header.update_header_checksum();
        EXPECT_EQ(true, tmp_header.check_header_integrity());
        EXPECT_EQ(true, tmp_header.check_integrity(log_group_buf+sizeof(LogGroupEntryHeader), serialize_buf_len));
        EXPECT_EQ(elem.first, tmp_header.is_raw_write());
        EXPECT_EQ(LogGroupEntryHeader::PADDING_TYPE_MASK_VERSION2, tmp_header.get_padding_mask_());
        PALF_LOG(INFO, "runlin print tmp_header", K(tmp_header));
        EXPECT_EQ(LogGroupEntryHeader::RAW_WRITE_MASK_VERSION2, tmp_header.get_raw_write_mask_());

        // 构造旧版本的LogGroupEntryHeader中
        tmp_header.version_ = LogGroupEntryHeader::LOG_GROUP_ENTRY_HEADER_VERSION;
        tmp_header.flag_ &= ~LogGroupEntryHeader::CRC16_MASK;
        tmp_header.update_write_mode(elem.first);
        tmp_header.flag_ |= (elem.second ? LogGroupEntryHeader::PADDING_TYPE_MASK : 0);
        tmp_header.flag_ |= get_log_group_entry_header_parity_checksum(tmp_header);
        EXPECT_EQ(elem.second, tmp_header.is_padding_log());
        EXPECT_EQ(elem.first, tmp_header.is_raw_write());
        EXPECT_EQ(true, tmp_header.check_header_integrity());
        EXPECT_EQ(true, tmp_header.check_integrity(log_group_buf+sizeof(LogGroupEntryHeader), serialize_buf_len));
        pos = 0;
        EXPECT_EQ(OB_SUCCESS, tmp_header.serialize(log_group_serialize_buf, log_group_buf_len, pos));
        EXPECT_EQ(LogGroupEntryHeader::PADDING_TYPE_MASK, tmp_header.get_padding_mask_());
        EXPECT_EQ(LogGroupEntryHeader::RAW_WRITE_MASK, tmp_header.get_raw_write_mask_());

        PALF_LOG(INFO, "test new binary parse old binary", K(tmp_header), K(elem.first), K(elem.second));
        // 新版本解析旧版本数据
        LogGroupEntryHeader new_group_header;
        pos = 0;
        EXPECT_EQ(OB_SUCCESS, new_group_header.deserialize(log_group_serialize_buf, log_group_buf_len, pos));
        EXPECT_EQ(true, new_group_header.check_header_integrity());
        EXPECT_EQ(true, new_group_header.check_integrity(log_group_buf+sizeof(LogGroupEntryHeader), serialize_buf_len));
        EXPECT_EQ(elem.first, new_group_header.is_raw_write());
        EXPECT_EQ(elem.second, new_group_header.is_padding_log());
        // 解析得到旧版本数据
        EXPECT_EQ(LogGroupEntryHeader::PADDING_TYPE_MASK, new_group_header.get_padding_mask_());
        EXPECT_EQ(LogGroupEntryHeader::RAW_WRITE_MASK, new_group_header.get_raw_write_mask_());
      }

    }
  }
}

void bit_flip(uint8_t *ptr, int len, int bit_count)
{
  // 保证magic和version不翻转
  const int arr_count = len * 8 - 32;
  std::vector<int> numbers(0, arr_count);
  numbers.resize(arr_count);
  for (int i = 0; i < arr_count; i++) {
    numbers[i] = i + 32;
  }
  std::random_device rd;
  auto rng = std::default_random_engine { rd() };
  std::shuffle(numbers.begin(), numbers.end(), rng);  // 打乱顺序

  for (int i = 0; i < bit_count; ++i) {
    int pos = numbers[i];
    uint8_t mask = (1 << (pos%8));
    *(ptr+pos/8) ^= mask;
    PALF_LOG(INFO, "runlin trace bit flip", K(pos));
  }
}

TEST(TestBitFlip, test_log_entry_header)
{
  std::srand(ObTimeUtility::current_time());
  PALF_LOG(INFO, "test_bit_flip_log_entry_header");
  LogEntryHeader log_entry_header;
  const int header_len = sizeof(LogEntryHeader);
  constexpr int data_len = 1024;
  char data[data_len]; memset(data, 'c', data_len);
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(data, data_len, share::SCN::base_scn()));
  PALF_LOG(INFO, "origin header", K(log_entry_header));
  const int count = 1 << 10;
  struct Pair {
    LogEntryHeader header;
    int bit_count;
  };
  int ret = OB_SUCCESS;
  std::vector<Pair> array;
  std::map<int, int> count_array;
  for (int i = 1; i <= 1; i++) {
    count_array.insert(std::pair<int, int>(i, 0));
    for (int j = 0; j < count; j++) {
      LogEntryHeader tmp_header = log_entry_header;
      uint8_t *ptr = reinterpret_cast<uint8_t*>(&tmp_header);
      bit_flip(ptr, header_len, i);
      bool bool_ret = tmp_header.check_header_integrity();
      EXPECT_EQ(false, bool_ret);
      if (bool_ret) {
        count_array[i] ++;
        array.push_back(Pair{tmp_header, i});
        PALF_LOG(ERROR, "print info", K(log_entry_header), K(tmp_header), K(j), K(i));
      }
    }
  }
  OB_LOGGER.set_file_name("print_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  for (auto &p : count_array) {
    PALF_LOG(INFO, "runlin trace print", "bit_flip", p.first, "count", p.second);
  }
}

TEST(TestBitFlip, test_log_group_entry_header)
{
  std::srand(ObTimeUtility::current_time());
  PALF_LOG(INFO, "test_bit_flip_log_group_entry_header");
  LogGroupEntryHeader header;
  LogWriteBuf write_buf;
  constexpr int data_len = 1024;
  char data[data_len]; memset(data, 'c', data_len);
  write_buf.push_back(data, data_len);
  share::SCN max_scn = share::SCN::min_scn();
  int64_t log_id = 1;
  LSN committed_lsn;
  committed_lsn.val_ = 1;
  int64_t proposal_id = 1;
  int64_t log_checksum = 0;
  EXPECT_EQ(OB_SUCCESS,
            header.generate(false, true, write_buf, data_len,
                            max_scn, log_id, committed_lsn, proposal_id, log_checksum));
  const int header_len = sizeof(LogGroupEntryHeader);
  header.update_header_checksum();
  PALF_LOG(INFO, "origin header", K(header));
  const int count = 1 << 10;
  for (int i = 0; i < count; i++) {
    LogGroupEntryHeader tmp_header = header;
    uint8_t *ptr = reinterpret_cast<uint8_t*>(&tmp_header);
    const int bit_count = 1;
    bit_flip(ptr, header_len, bit_count);
    PALF_LOG(INFO, "current header", K(header), K(tmp_header), K(i), K(bit_count));
    bool bool_ret = tmp_header.check_header_integrity();
    EXPECT_EQ(false, bool_ret);
    if (bool_ret) {
      assert(false);
    }
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_log_entry_and_group_entry.log");
  system("rm -f print_info*");
  OB_LOGGER.set_file_name("test_log_entry_and_group_entry.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_CURRENT_VERSION;
  oceanbase::common::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  PALF_LOG(INFO, "begin unittest::test_log_entry_and_group_entry");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
