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

#ifndef OCEANBASE_LOGSERVICE_LOG_FILE_HANDLER_
#define OCEANBASE_LOGSERVICE_LOG_FILE_HANDLER_

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "log_define.h"                                // block_id_t ...

// This block contains the key class for writing a log into stable storage
// device.

namespace oceanbase
{
namespace palf
{
class LogWriteBuf;
// Only this class need to determine the storage system whether is OFS
//
class LogDIOAlignedBuf {
public:
  LogDIOAlignedBuf();
  ~LogDIOAlignedBuf();

  // NB: if 'align_size' is 0, means there should not align buf
  int init(uint32_t align_size,
           uint32_t aligned_buf_size);

  void destroy();

  // @brief this function used to align 'input' to the specified size,
  // call this function before write data to disk
  // @param[in] the data to be writted
  // @param[in] the length of 'input'
  // @param[out] the aligned data
  // @param[out] the aligned length of 'output'
  // @arapm[in&out] the block write offset
  int align_buf(const char *input,
                const int64_t input_len,
                char *&ouput,
                int64_t &output_len,
                offset_t &offset);

  // @brief this function used to truncate 'aligned_data_buf_', move
  // the tail unaligned part to head
  void truncate_buf();
  void reset_buf();

  TO_STRING_KV(K_(buf_write_offset), K_(buf_padding_size), K_(align_size), K_(aligned_buf_size),
      K_(aligned_used_ts), K_(truncate_used_ts));
private:
  DISALLOW_COPY_AND_ASSIGN(LogDIOAlignedBuf);
  inline bool need_align_() const
  {
    return 0 != align_size_;
  }

  void align_buf_();
private:
  // Used for dio
  // After align_buf, 'buf_write_offset_' is upper align by 'align_size_'
  offset_t buf_write_offset_;
  offset_t buf_padding_size_;
  uint32_t align_size_;
  uint32_t aligned_buf_size_;

  // If align_size is 0, 'aligned_data_buf_' is NULL.
  char *aligned_data_buf_;
  int64_t aligned_used_ts_;
  int64_t truncate_used_ts_;
  bool is_inited_;
};

// This class just used for writing log, truncating log
class LogBlockHandler {
public:
  LogBlockHandler();
  ~LogBlockHandler();

  int init(const int dir_fd,
           const int64_t log_block_size,
           const int64_t align_size,
           const int64_t align_buf_size);

  void destroy();

  // @brief this function used to open last block after restart
  // NB: retry until success!!
  int open(const char *block_path);

  // TODO by runlin, only open block via char*
  // int open(const char *block_name);
  // @brief this function used close current opened block
  // NB: retry until success!!
  int close();

  // @brief this function used to truncate block via specified offset
  // NB: retry until success!!
  int truncate(const offset_t offset);

  // @brief this function used to fill dio_aligned_buf_ via specified offset
  int load_data(const offset_t offset);

  // @brief this function used to create new block in process of writing log
  // NB: retry until success!!
  int switch_next_block(const char *block_path);

  // @brief this function used to write data at specified offset
  // @param[in] the logical offset
  // @param[in] the data to be written
  // @param[in] the length of data
  // NB: retry until success!!
  int pwrite(const offset_t offset,
             const char *buf,
             const int64_t buf_len);

  // NB: retry until success!!
  int writev(const offset_t offset,
             const LogWriteBuf &write_buf);

  TO_STRING_KV(K_(dio_aligned_buf), K_(log_block_size), K_(dir_fd), K_(io_fd));
private:
  // if timeout, retry until open block return an explicit error code
  // @brief block_path, the block path to be opened
  // @brief start_offset, the start logical offset for curr virtual block
  // @retval
  //    OB_SUCCESS, open success
  //    OB_ERR_UNEXPECTED, unexpected error
  int inner_open_(const char *block_path);

  int inner_close_();

  // NB: retry until suuccess
  int inner_truncate_(const offset_t offset);
  int inner_load_data_(const offset_t offset);

  int inner_write_once_(const offset_t offset,
      const char *buf,
      const int64_t buf_len);
  int inner_writev_once_(const offset_t offset,
      const LogWriteBuf &write_buf);
  int inner_write_impl_(const int fd, const char *buf, const int64_t count, const int64_t offset);
private:
  static constexpr int64_t RETRY_INTERVAL = 10 * 1000;
  LogDIOAlignedBuf dio_aligned_buf_;
  int64_t log_block_size_;
  int64_t total_write_size_;
  int64_t total_write_size_after_dio_;
  int64_t ob_pwrite_used_ts_;
  int64_t count_;
  int64_t trace_time_;
  int dir_fd_;
  int io_fd_;
  bool is_inited_;
};
} // end of logservice
} // end of oceanbase

#endif
