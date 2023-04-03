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

#ifndef OCEANBASE_COMMON_OB_LOG_DATA_WRITER_
#define OCEANBASE_COMMON_OB_LOG_DATA_WRITER_

#include "lib/ob_define.h"
#include "common/log/ob_log_cursor.h"

namespace oceanbase
{
namespace common
{

int myfallocate(int fd, int mode, off_t offset, off_t len);

class MinAvailFileIdGetter
{
public:
  MinAvailFileIdGetter() {}
  virtual ~MinAvailFileIdGetter() {}
  virtual int64_t get() = 0;
};

class ObLogDataWriter
{
public:
  static const int OPEN_FLAG = O_WRONLY | O_DIRECT;
  static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static const int CREATE_FLAG = OPEN_FLAG | O_CREAT;
  class AppendBuffer
  {
  public:
    static const int64_t DEFAULT_BUF_SIZE = 1 << 22;
    AppendBuffer();
    ~AppendBuffer();
    int write(const char *buf, int64_t len, int64_t pos);
    int flush(int fd);
    void destroy();
  private:
    int64_t file_pos_;
    char *buf_;
    int64_t buf_end_;
    int64_t buf_limit_;
  };
public:
  ObLogDataWriter();
  ~ObLogDataWriter();
  int init(const char *log_dir,
           const int64_t file_size,
           const int64_t du_percent,
           const int64_t log_sync_type,
           MinAvailFileIdGetter *first_useful_file_id_getter);
  void destroy();
  int write(const ObLogCursor &start_cursor,
            const ObLogCursor &end_cursor,
            const char *data,
            const int64_t data_len);
  int start_log(const ObLogCursor &cursor);
  int reset();
  int get_cursor(ObLogCursor &cursor) const;
  inline int64_t get_file_size() const { return file_size_; }
  int64_t to_string(char *buf, const int64_t len) const;
protected:
  int check_eof_after_log_cursor(const ObLogCursor &cursor);
  int prepare_fd(const int64_t file_id);
  int reuse(const char *pool_file, const char *fname);
  const char *select_pool_file(char *fname, const int64_t limit);
private:
  AppendBuffer write_buffer_;
  const char *log_dir_;
  int64_t file_size_;
  ObLogCursor end_cursor_;
  int64_t log_sync_type_;
  int fd_;
  int64_t cur_file_id_;
  int64_t num_file_to_add_;
  int64_t min_file_id_;
  int64_t min_avail_file_id_;
  MinAvailFileIdGetter *min_avail_file_id_getter_;
};
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_LOG_DATA_WRITER_ */
