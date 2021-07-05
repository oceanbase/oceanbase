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

#ifndef OCEANBASE_UNITTEST_MOCK_LOG_READER_H_
#define OCEANBASE_UNITTEST_MOCK_LOG_READER_H_

#include "clog/ob_log_reader_interface.h"
#include "clog/ob_log_file_pool.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
using namespace clog;
namespace clog {
class MockFilePool : public IFilePool {
public:
  virtual int get_fid_range(int64_t& min_fid, int64_t& max_fid)
  {
    UNUSED(min_fid);
    UNUSED(max_fid);
    return common::OB_SUCCESS;
  }
  virtual int get_fd(const int64_t file_id, int& fd)
  {
    UNUSED(file_id);
    UNUSED(fd);
    return common::OB_SUCCESS;
  }
};

class MockLogDirectReader : public ObILogDirectReader {
public:
  virtual int init(IFilePool* reader_pool)
  {
    UNUSED(reader_pool);
    return common::OB_SUCCESS;
  }
  virtual void destroy()
  {}
  virtual int read_data(const clog::ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res)
  {
    UNUSED(param);
    UNUSED(rbuf);
    UNUSED(res);
    return common::OB_SUCCESS;
  }
  virtual int read_log(const clog::ObReadParam& param, ObReadBuf& rbuf, ObLogEntry& entry)
  {
    UNUSED(param);
    UNUSED(rbuf);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  virtual int read_trailer(const clog::ObReadParam& param, ObReadBuf& rbuf, int64_t& start_pos, uint64_t& next_file_id)
  {
    UNUSED(param);
    UNUSED(rbuf);
    UNUSED(start_pos);
    UNUSED(next_file_id);
    return common::OB_SUCCESS;
  }
};

class MockRawLogIterator : public ObIRawLogIterator {
public:
  int init()
  {
    num_ = 0;
    param1.file_id_ = 1;
    param1.offset_ = 0;
    param2.file_id_ = 1;
    param2.offset_ = 1;
    param3.file_id_ = 1;
    param3.offset_ = 2;
    param4.file_id_ = 2;
    param4.offset_ = 1;
    param5.file_id_ = 2;
    param5.offset_ = 2;
    param6.file_id_ = 2;
    param6.offset_ = 3;

    ObLogType log_type = OB_LOG_SUBMIT;
    uint64_t log_id = 1;
    int64_t gts, tts, rts;
    gts = tts = rts = common::ObTimeUtility::current_time();
    int64_t data_len = 10;
    int tid, pid;
    tid = pid = 1;
    common::ObPartitionKey partition_key1;
    partition_key1.init(tid, pid, 1024);
    tid = pid = 2;
    common::ObPartitionKey partition_key2;
    partition_key2.init(tid, pid, 1024);
    tid = pid = 3;
    common::ObPartitionKey partition_key3;
    partition_key3.init(tid, pid, 1024);

    ObLogEntryHeader entry_header1;
    ObLogEntryHeader entry_header2;
    ObLogEntryHeader entry_header3;
    common::ObProposalID proposal_id;
    char data[10];
    entry_header1.generate_header(
        log_type, partition_key1, log_id, data, data_len, gts, tts, proposal_id, 0, 0, 0, 0, 0);
    entry_header2.generate_header(
        log_type, partition_key2, log_id, data, data_len, gts, tts, proposal_id, 0, 0, 0, 0, 0);
    entry_header3.generate_header(
        log_type, partition_key3, log_id, data, data_len, gts, tts, proposal_id, 0, 0, 0, 0, 0);
    log_entry1.generate_entry(entry_header1, data);
    log_entry2.generate_entry(entry_header2, data);
    log_entry3.generate_entry(entry_header3, data);

    return common::OB_SUCCESS;
  }
  virtual int reuse(const uint64_t file_id, const int64_t timeout)
  {
    UNUSED(file_id);
    UNUSED(timeout);
    return common::OB_SUCCESS;
  }
  virtual int set_file_id(const uint64_t file_id)
  {
    UNUSED(file_id);
    return common::OB_SUCCESS;
  }
  virtual void destroy()
  {}
  virtual bool is_inited() const
  {
    return true;
  }
  virtual int next_entry(ObLogEntry& entry, clog::ObReadParam& param)
  {
    int ret = common::OB_SUCCESS;
    if (0 == num_) {
      entry.generate_entry(log_entry1.get_header(), log_entry1.get_buf());
      param = param1;
      ++num_;
    } else if (1 == num_) {
      entry.generate_entry(log_entry1.get_header(), log_entry1.get_buf());
      param = param2;
      ++num_;
    } else if (2 == num_) {
      entry.generate_entry(log_entry1.get_header(), log_entry1.get_buf());
      param = param3;
      ++num_;
    } else if (3 == num_) {
      entry.generate_entry(log_entry2.get_header(), log_entry2.get_buf());
      param = param4;
      ++num_;
    } else if (4 == num_) {
      entry.generate_entry(log_entry2.get_header(), log_entry2.get_buf());
      param = param5;
      ++num_;
    } else if (5 == num_) {
      entry.generate_entry(log_entry3.get_header(), log_entry3.get_buf());
      param = param6;
      ++num_;
    } else {
      ret = common::OB_ITER_END;
    }
    return ret;
  }
  virtual int get_current_param(clog::ObReadParam& param) const
  {
    UNUSED(param);
    return common::OB_SUCCESS;
  }

private:
  int num_;
  ObLogEntry log_entry1;
  ObLogEntry log_entry2;
  ObLogEntry log_entry3;
  clog::ObReadParam param1;
  clog::ObReadParam param2;
  clog::ObReadParam param3;
  clog::ObReadParam param4;
  clog::ObReadParam param5;
  clog::ObReadParam param6;
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_MOCK_LOG_READER_H_
