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

#ifndef OB_STORAGE_LOG_READER_H_
#define OB_STORAGE_LOG_READER_H_

#include "common/data_buffer.h"
#include "common/log/ob_log_cursor.h"
#include "common/log/ob_log_entry.h"

namespace oceanbase {
namespace common {
class ObILogFileStore;
}
namespace blocksstable {
class ObStorageLogReader {
public:
  ObStorageLogReader();
  virtual ~ObStorageLogReader();

  /**
   * Initialization
   * @param [in] log_dir: the log folder path
   * @param [in] log_file_id_start: the start log file id
   * @param [in] log_seq: the last log seq
   */
  int init(const char* log_dir, const uint64_t log_file_id_start, const uint64_t log_seq);
  void reset();

  /**
   * @brief Read logs
   * When meet SWITCH_LOG, will open the next file
   * @return OB_SUCCESS
   * @return OB_READ_NOTHING Nothing to read. It can be the end of last file or it is in mid term,
   *                         Caller need to tackle such different situation.
   * @return otherwise Fail
   */
  int read_log(common::LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len);

  // Revise the last corrupt log
  int revise_log(const bool force = false);

  int get_cursor(common::ObLogCursor& cursor) const;
  int get_next_cursor(common::ObLogCursor& cursor) const;

private:
  int open();
  int close();
  int seek(uint64_t log_seq);
  int get_next_entry(common::ObLogEntry& entry);
  int get_next_log(common::LogCommand& cmd, uint64_t& log_seq, char*& log_data, int64_t& data_len, bool& is_duplicate);
  int load_buf();
  int check_switch_file(const int get_ret, const common::LogCommand cmd);
  int check_and_update_seq_number(const common::ObLogEntry& entry, bool& is_duplicate);
  int tackle_less_header_len(const common::ObLogEntry& entry);
  int tackle_less_data_len(const common::ObLogEntry& entry);

private:
  static const int64_t LOG_FILE_MAX_SIZE = 256 << 20;

  bool is_inited_;
  uint64_t file_id_;
  uint64_t last_log_seq_;
  common::ObDataBuffer log_buffer_;
  int64_t pos_;
  int64_t pread_pos_;
  common::ObILogFileStore* file_store_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageLogReader);
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_STORAGE_LOG_READER_H_ */
