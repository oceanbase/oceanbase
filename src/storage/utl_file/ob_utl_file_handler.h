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

#ifndef OCEANBASE_STORAGE_UTL_FILE_OB_UTL_FILE_HANDLER_H_
#define OCEANBASE_STORAGE_UTL_FILE_OB_UTL_FILE_HANDLER_H_

#include <stdint.h>
#include "storage/utl_file/ob_utl_file_constants.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
struct ObIOFd;
}
namespace storage
{
class ObUtlFileHandler
{
public:
  ObUtlFileHandler() = default;
  ~ObUtlFileHandler() = default;
public:
  static int fopen(const char *dir, const char *filename, const char *open_mode,
                   const int64_t max_line_size, int64_t &fd);
  static int fclose(const int64_t &fd);
  static int get_line(const int64_t &fd, char *buffer, const int64_t len,
                      const int64_t max_line_size, int64_t &line_size);
  static int get_line_raw(const int64_t &fd, char *buffer, const int64_t len, int64_t &read_size);
  static int put_buffer(const int64_t &fd, const char *buffer, const int64_t size,
                        const int64_t max_line_size, int64_t &write_size);
  static int fflush(const int64_t &fd);
  static int put_raw(const int64_t &fd, const char *buffer, int64_t size, bool autoflush = false);
  static int fseek(const int64_t &fd, const int64_t *abs_offset, const int64_t *rel_offset);
  static int fremove(const char *dir, const char *filename);
  static int fcopy(const char *src_dir, const char *src_filename,
                   const char *dst_dir, const char *dst_filename,
                   const int64_t start = 1, const int64_t *end_line = NULL);
  static int fgetattr(const char *dir, const char *filename,
                      bool &b_exist, int64_t &file_length, int64_t &block_size);
  static int fgetpos(const int64_t &fd, int64_t &pos);
  static int frename(const char *src_dir, const char *src_filename,
                     const char *dst_dir, const char *dst_filename, bool overwrite);
  static int fis_open(const int64_t &fd, bool &b_open);
public:
  static bool is_valid_max_line_size(const int size);
private:
  static bool is_valid_path(const char *path);
  static bool is_valid_path(const char *path, size_t &path_len);
  static bool is_valid_open_mode(const char *open_mode);
  static int convert_open_mode(const char *open_mode, int &flags);
  static int format_full_path(char *full_path, size_t len, const char *dir, const char *filename);
  static int put_impl(const int64_t &fd, const char *buffer, const int64_t size,
                      int64_t &write_size, bool autoflush = false);
  static int read_impl(const int64_t &fd, char *buffer, const int64_t len, int64_t &read_size);
  static int find_single_line(const char *buffer, const int64_t len, int64_t &pos, bool &found);
  static int find_and_copy_lines(const char *buffer, const int64_t len,
                                 const common::ObIOFd &dst_io_fd,
                                 const int64_t start_line, const int64_t end_line,
                                 int64_t &line_num);
  static int copy_lines(const common::ObIOFd &src_io_fd, const common::ObIOFd &dst_io_fd,
                        const int64_t start_line, const int64_t end_line);
  static int check_buffer(const char *buffer, const int64_t size,
                          const int64_t max_line_size, int64_t &last_valid_pos);
private:
  static constexpr const char *LINE_TERMINATOR = "\n";
  static const int LINE_TERMINATOR_LEN;
  static constexpr int LINE_RESERVED_LEN = 4;
  static constexpr int READ_FLAGS = O_RDONLY;
  static constexpr int WRITE_FLAGS = O_WRONLY | O_SYNC | O_CREAT | O_TRUNC;
  static constexpr int FILE_OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
};

OB_INLINE bool ObUtlFileHandler::is_valid_path(const char *path)
{
  size_t path_len = 0;
  return is_valid_path(path, path_len);
}

OB_INLINE bool ObUtlFileHandler::is_valid_max_line_size(const int size)
{
  return size >= ObUtlFileConstants::MAX_LINE_SIZE_LOWER_LIMIT
      && size <= ObUtlFileConstants::MAX_LINE_SIZE_UPPER_LIMIT;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_UTL_FILE_OB_UTL_FILE_HANDLER_H_
