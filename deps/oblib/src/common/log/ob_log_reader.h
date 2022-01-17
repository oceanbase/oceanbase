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

#ifndef OCEANBASE_COMMON_OB_LOG_READER_
#define OCEANBASE_COMMON_OB_LOG_READER_

#include "lib/atomic/ob_atomic.h"
#include "common/log/ob_log_entry.h"
#include "common/log/ob_single_log_reader.h"
#include "common/log/ob_log_cursor.h"

namespace oceanbase {
namespace common {
/**
 * Log reading class, starting from a specified log id, when the end of the log file is encountered, the next log file
 * is opened There are two main ways to use:
 * 1. The logs are played back when the Master starts, and the playback is complete when all the logs are read
 * 2. Slave's log playback thread usage: Slave uses one thread to synchronize the log, and when the other thread plays
 * back the log, The playback thread keeps chasing the log file. When the log is read to the end, you should wait a
 * short time before reading the log again
 */
class ObLogReader {
public:
  static const int64_t WAIT_TIME = 1000000;  // us
  static const int FAIL_TIMES = 60;

public:
  ObLogReader();
  virtual ~ObLogReader();

  int init(ObSingleLogReader* reader, const char* log_dir, const uint64_t log_file_id_start, const uint64_t log_seq,
      bool is_wait);

  /**
   * Open the next log file directly when encountering the SWITCH_LOG log
   * When opening the next log file, if the file does not exist, it may be cutting the file where the log is generated
   * Wait for 1ms and try again, up to 10 times, if it still does not exist, an error will be reported
   */
  int read_log(LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len);
  // Reopen a new file and locate the next bit in the current log
  int revise_log(const bool force = false);
  int reset_file_id(const uint64_t log_id_start, const uint64_t log_seq_start);
  inline void set_max_log_file_id(uint64_t max_log_file_id);
  inline uint64_t get_max_log_file_id() const;
  inline bool get_has_max() const;
  inline void set_has_no_max();
  inline uint64_t get_cur_log_file_id() const;
  inline uint64_t get_cur_log_file_id();
  inline uint64_t get_last_log_seq_id();
  inline uint64_t get_last_log_offset();
  inline int get_cursor(common::ObLogCursor& cursor);
  inline int get_next_cursor(common::ObLogCursor& cursor);

private:
  int seek(uint64_t log_seq);
  int open_log_(const uint64_t log_file_id, const uint64_t last_log_seq = 0);
  int read_log_(LogCommand& cmd, uint64_t& log_seq, char*& log_data, int64_t& data_len);

private:
  uint64_t cur_log_file_id_;
  uint64_t cur_log_seq_id_;
  uint64_t max_log_file_id_;
  ObSingleLogReader* log_file_reader_;
  bool is_inited_;
  bool is_wait_;
  bool has_max_;
};

inline void ObLogReader::set_max_log_file_id(uint64_t max_log_file_id)
{
  (void)ATOMIC_TAS(&max_log_file_id_, max_log_file_id);
  has_max_ = true;
}

inline uint64_t ObLogReader::get_max_log_file_id() const
{
  return max_log_file_id_;
}

inline bool ObLogReader::get_has_max() const
{
  return has_max_;
}

inline void ObLogReader::set_has_no_max()
{
  has_max_ = false;
}

inline uint64_t ObLogReader::get_cur_log_file_id() const
{
  return cur_log_file_id_;
}

inline uint64_t ObLogReader::get_cur_log_file_id()
{
  return cur_log_file_id_;
}

inline uint64_t ObLogReader::get_last_log_seq_id()
{
  return cur_log_seq_id_;
}

inline uint64_t ObLogReader::get_last_log_offset()
{
  return NULL != log_file_reader_ ? log_file_reader_->get_last_log_offset() : 0;
}

inline int ObLogReader::get_cursor(common::ObLogCursor& cursor)
{
  int ret = OB_SUCCESS;
  if (NULL == log_file_reader_) {
    ret = OB_READ_NOTHING;
  } else {
    cursor.file_id_ = cur_log_file_id_;
    cursor.log_id_ = cur_log_seq_id_;
    cursor.offset_ = log_file_reader_->get_last_log_offset();
  }
  return ret;
}

inline int ObLogReader::get_next_cursor(common::ObLogCursor& cursor)
{
  int ret = OB_SUCCESS;
  if (NULL == log_file_reader_) {
    ret = OB_READ_NOTHING;
  } else {
    cursor.file_id_ = cur_log_file_id_;
    cursor.log_id_ = cur_log_seq_id_ + 1;
    cursor.offset_ = log_file_reader_->get_last_log_offset();
  }
  return ret;
}

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_LOG_READER_
