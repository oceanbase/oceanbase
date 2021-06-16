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

#ifndef OCEANBASE_COMMON_OB_SINGLE_LOG_READER_
#define OCEANBASE_COMMON_OB_SINGLE_LOG_READER_

#include "lib/allocator/ob_malloc.h"
#include "lib/file/ob_file.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/data_buffer.h"
#include "common/log/ob_log_entry.h"

namespace oceanbase {
namespace common {
class ObSingleLogReader {
public:
  static const int64_t LOG_BUFFER_MAX_LENGTH;

public:
  ObSingleLogReader();
  virtual ~ObSingleLogReader();

  /**
   * ObSingleLogReader must first call the init function to initialize, then open and read_log can be called
   * During initialization, a read buffer of LOG_BUFFER_MAX_LENGTH length will be allocated
   * Release the read buffer in the destructor
   */
  int init(const char* log_dir);

  /**
   * The open function will open the log file
   * After calling the close function to close the log file, you can call the open function again to open other log
   * files, buffer reuse
   */
  int open(const uint64_t file_id, const uint64_t last_log_seq = 0);

  /**
   * Close the opened log file, then you can call the init function again to read other log files
   */
  int close();

  /**
   * Reset internal state, release buffer memory
   */
  int reset();

  /**
   * Read an update operation from the operation log
   */
  virtual int read_log(LogCommand& cmd, uint64_t& log_seq, char*& log_data, int64_t& data_len) = 0;
  int revise();
  bool if_file_exist(const uint64_t file_id);
  inline uint64_t get_cur_log_file_id() const
  {
    return file_id_;
  }
  inline uint64_t get_last_log_seq_id() const
  {
    return last_log_seq_;
  }
  inline uint64_t get_last_log_offset() const
  {
    return pos_;
  }

  /// @brief is log file opened
  inline bool is_opened() const
  {
    return file_.is_opened();
  }

  /// When @brief is initialized, get the maximum log file number in the current directory
  int get_max_log_file_id(uint64_t& max_log_file_id);
  int64_t get_cur_offset() const;

  inline void unset_dio()
  {
    dio_ = false;
  }

protected:
  int read_header(ObLogEntry& entry);
  int trim_last_zero_padding(int64_t header_size);
  int open_with_lucky(const uint64_t file_id, const uint64_t last_log_seq);

protected:
  /**
   * Read data from the log file to the read buffer
   */
  int read_log_();

protected:
  bool is_inited_;                           // Initialization tag
  uint64_t file_id_;                         // Log file id
  uint64_t last_log_seq_;                    // Last log (Mutator) serial number
  ObDataBuffer log_buffer_;                  // Read buffer
  char file_name_[OB_MAX_FILE_NAME_LENGTH];  // Log file name
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];    // Log directory
  int64_t pos_;
  int64_t pread_pos_;
  ObFileReader file_;
  bool dio_;
};
}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_SINGLE_LOG_READER_
