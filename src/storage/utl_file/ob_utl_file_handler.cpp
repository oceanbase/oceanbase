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

#define USING_LOG_PREFIX STORAGE
#include <limits.h>
#include "storage/utl_file/ob_utl_file_handler.h"
#include "common/ob_smart_var.h"
#include "common/storage/ob_io_device.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
const int ObUtlFileHandler::LINE_TERMINATOR_LEN = STRLEN(ObUtlFileHandler::LINE_TERMINATOR);

int ObUtlFileHandler::fopen(const char *dir, const char *filename, const char *open_mode,
                            const int64_t max_line_size, int64_t &fd)
{
  int ret = OB_SUCCESS;
  size_t filename_len = 0;
  const char *real_filename = NULL;
  bool dir_included = false;
  if (OB_UNLIKELY(!is_valid_path(dir))) {
    ret = OB_UTL_FILE_INVALID_PATH;
    LOG_WARN("invalid dir", K(ret), K(dir));
  } else if (OB_UNLIKELY(!is_valid_path(filename, filename_len))) {
    ret = OB_UTL_FILE_INVALID_FILENAME;
    LOG_WARN("invalid filename", K(ret), K(filename));
  } else if (OB_ISNULL(open_mode)) {
    ret = OB_UTL_FILE_INVALID_MODE;
    LOG_WARN("open mode is null", K(ret), K(open_mode));
  } else if (OB_UNLIKELY(!is_valid_open_mode(open_mode))) {
    ret = OB_UTL_FILE_INVALID_MODE;
    LOG_WARN("invalid open mode", K(ret), K(open_mode));
  } else if (OB_UNLIKELY(!is_valid_max_line_size(max_line_size))) {
    ret = OB_UTL_FILE_INVALID_MAXLINESIZE;
    LOG_WARN("invalid max line size", K(ret), K(max_line_size));
  } else if ('/' == filename[filename_len - 1]) {
    // check dir and filename
    // filename ends with '/', invalid filename
    ret = OB_UTL_FILE_INVALID_FILENAME;
    LOG_WARN("invalid filename, should not end with '/'", K(ret), K(filename));
  } else {
    // check whether filename includes '/', which means filename has directory path inside it
    // should ignore directory path
    const char *last_slash = NULL;
    if (NULL != (last_slash = STRRCHR(filename, '/'))) {
      LOG_INFO("dir is part of filename, should ignore dir in full path",
          K(dir), K(filename));
      dir_included = true;
      real_filename = last_slash + 1;
    }
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], full_path) {
      int flags;
      ObIOFd io_fd;
      int p_ret = 0;
      if (dir_included) {
        p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir, real_filename);
      } else {
        p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir, filename);
      }
      if (p_ret < 0 || p_ret >= sizeof(full_path)) {
        ret = OB_UTL_FILE_INVALID_FILENAME;
        LOG_WARN("file name too long", K(ret), K(dir), K(filename));
      } else if (OB_FAIL(convert_open_mode(open_mode, flags))) {
        LOG_WARN("fail to convert open mode", K(ret), K(open_mode));
        ret = OB_UTL_FILE_INVALID_MODE;
      } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(full_path, flags,
          ObUtlFileConstants::UTL_FILE_ACCESS_MODE, io_fd))) {
        LOG_WARN("fail to open file", K(ret), K(full_path), K(flags),
            LITERAL_K(ObUtlFileConstants::UTL_FILE_ACCESS_MODE));
        ret = OB_UTL_FILE_INVALID_OPERATION;
      } else if (!io_fd.is_normal_file()) { // defense code
        ret = OB_UTL_FILE_INVALID_OPERATION;
        LOG_WARN("io fd is not normal file", K(ret), K(io_fd));
      } else {
        fd = io_fd.second_id_;
      }
    }
  }
  return ret;
}

int ObUtlFileHandler::fclose(const int64_t &fd)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  if (OB_UNLIKELY(!io_fd.is_normal_file())) {
    ret = OB_UTL_FILE_INVALID_FILEHANDLE;
    LOG_WARN("invalid handle", K(ret), K(io_fd));
  }  else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync(io_fd))) {
    LOG_WARN("failed to fsync", K(ret), K(io_fd));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.close(io_fd))) {
    LOG_WARN("failed to close fd", K(ret), K(io_fd));
  }
  return ret;
}

int ObUtlFileHandler::get_line(const int64_t &fd, char *buffer, const int64_t len,
                               const int64_t max_line_size, int64_t &line_size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t current_read_pos = 0;
  int64_t pos = 0;
  bool found = false;
  const int64_t buffer_size = max(1, max_line_size - LINE_RESERVED_LEN); // reserve 4 bytes to be compatible with oracle
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  if (OB_FAIL(LOCAL_DEVICE_INSTANCE.lseek(io_fd, 0, SEEK_CUR, current_read_pos))) {
    LOG_WARN("failed to lseek current pos", K(ret), K(io_fd));
  } else if (OB_FAIL(get_line_raw(fd, buffer, buffer_size, read_size))) {
    LOG_WARN("failed to read data", K(ret), K(fd), K(buffer_size));
  } else if (OB_FAIL(find_single_line(buffer, read_size, pos, found))) {
    LOG_WARN("failed to find single line in buffer", K(ret), K(buffer), K(read_size));
  } else {
    // reset read pos to first line end pos
    int64_t target_pos;
    if (found) {
      if (pos > len) {
        // cannot find '\n' within [0, len)
        buffer[len] = '\0';
        target_pos = current_read_pos + len;
        line_size = pos;
      } else {
        buffer[pos] = '\0';
        target_pos = current_read_pos + pos + 1;
        line_size = pos + 1;
      }
    } else {
      target_pos = current_read_pos;
      ret = OB_UTL_FILE_READ_ERROR;
      LOG_WARN("failed to get line beacuse line does not fit the buffer", K(ret), K(buffer), K(len), K(read_size));
    }
    int64_t result_offset = 0;
    int tmp_ret = LOCAL_DEVICE_INSTANCE.lseek(io_fd, target_pos, SEEK_SET, result_offset);
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to lseek", K(ret), K(tmp_ret), K(io_fd), K(target_pos));
    }
  }
  return ret;
}

int ObUtlFileHandler::get_line_raw(const int64_t &fd, char *buffer, const int64_t len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  int64_t pos = 0;
  if (OB_UNLIKELY(!io_fd.is_normal_file())) {
    ret = OB_UTL_FILE_INVALID_FILEHANDLE;
    LOG_WARN("invalid handle", K(ret), K(io_fd));
  } else if (OB_ISNULL(buffer) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buffer), K(len));
  } else if (OB_FAIL(read_impl(fd, buffer, len, read_size))) {
    LOG_WARN("fail to get line", K(ret), K(len));
  } else if (0 == read_size) {
    ret = OB_READ_NOTHING;
    LOG_WARN("read size is 0", K(ret), K(fd), K(len));
  }
  return ret;
}

int ObUtlFileHandler::put_buffer(const int64_t &fd, const char *buffer, const int64_t size,
    const int64_t max_line_size, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  int64_t last_valid_pos = -1;
  int64_t valid_buffer_size = 0;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buffer), K(size));
  } else if (OB_FAIL(check_buffer(buffer, size, max_line_size, last_valid_pos))) {
    LOG_WARN("buffer is invalid", K(ret), K(buffer), K(size), K(max_line_size), K(last_valid_pos));
  } else if (last_valid_pos < size - 1) {
    LOG_INFO("only partial buffer can be written", K(ret), K(buffer), K(last_valid_pos));
  }

  if (OB_SUCCESS == ret || OB_UTL_FILE_WRITE_ERROR == ret) {
    valid_buffer_size = last_valid_pos + 1;
    if (valid_buffer_size > 0) {
      int tmp_ret = put_impl(fd, buffer, valid_buffer_size, write_size, false/*autoflush*/);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("failed to put buffer", K(tmp_ret), K(fd), K(buffer), K(valid_buffer_size));
        if (OB_SUCCESS == ret) {
          ret = tmp_ret;
        }
      }
    } else {
      write_size = 0;
    }
  }

  return ret;
}

int ObUtlFileHandler::fflush(const int64_t &fd)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  if (OB_UNLIKELY(!io_fd.is_normal_file())) {
    ret = OB_UTL_FILE_INVALID_FILEHANDLE;
    LOG_WARN("invalid handle", K(ret), K(io_fd));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync(io_fd))) {
    LOG_WARN("fail to fsync", K(ret), K(io_fd));
  }
  return ret;
}

int ObUtlFileHandler::put_raw(const int64_t &fd, const char *buffer, const int64_t size, bool autoflush)
{
  int64_t write_size = 0;
  return put_impl(fd, buffer, size, write_size, autoflush);
}

int ObUtlFileHandler::fseek(const int64_t &fd, const int64_t *abs_offset, const int64_t *rel_offset)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  int64_t target_offset = 0;
  int64_t current_offset = 0;
  int64_t end_offset = 0;
  int64_t result_offset = 0;
  if (OB_UNLIKELY(!io_fd.is_normal_file())) {
    ret = OB_UTL_FILE_INVALID_FILEHANDLE;
    LOG_WARN("invalid handle", K(ret), K(io_fd));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.lseek(io_fd, 0, SEEK_CUR, current_offset))) {
    LOG_WARN("fail to seek current offset", K(ret), K(io_fd));
  } else {
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.lseek(io_fd, 0, SEEK_END, end_offset))) {
      LOG_WARN("fail to seek end offset", K(ret), K(io_fd));
    } else if (NULL == abs_offset && NULL == rel_offset) {
      ret = OB_UTL_FILE_INVALID_OFFSET;
      LOG_WARN("both of abs_offset and rel_offset are NULL", K(ret), K(abs_offset), K(rel_offset));
    } else if (NULL == abs_offset) {
      target_offset = current_offset + *rel_offset;
    } else {
      target_offset = *abs_offset;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (target_offset > end_offset) {
      ret = OB_UTL_FILE_INVALID_OFFSET;
      LOG_WARN("invalid offset", K(ret), K(target_offset), K(end_offset));
    } else if (target_offset < 0) {
      LOG_INFO("seek to the beginning of file", K(target_offset));
      target_offset = 0;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.lseek(io_fd, target_offset, SEEK_SET, result_offset))) {
      LOG_WARN("fail to seek target offset", K(ret), K(io_fd), K(target_offset));
      ret = OB_UTL_FILE_INVALID_OPERATION;
    }
  }

  if (OB_FAIL(ret)) {
    // roll back
    int64_t offset;
    int tmp_ret = LOCAL_DEVICE_INSTANCE.lseek(io_fd, current_offset, SEEK_SET, offset);
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("fail to roll back to previous offset", K(ret), K(io_fd), K(current_offset));
      ret = OB_UTL_FILE_INVALID_OPERATION;
    } else {
      LOG_INFO("roll back to previous offset", K(ret), K(io_fd), K(current_offset));
    }
  }
  return ret;
}

int ObUtlFileHandler::fremove(const char *dir, const char *filename)
{
  int ret = OB_SUCCESS;
  size_t filename_len = 0;
  const char *real_filename = NULL;
  bool dir_included = false;
  SMART_VAR(char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], full_path) {
    if (OB_UNLIKELY(!is_valid_path(dir))) {
      ret = OB_UTL_FILE_INVALID_PATH;
      LOG_WARN("invalid dir", K(ret), K(dir));
    } else if (OB_UNLIKELY(!is_valid_path(filename, filename_len))) {
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid filename", K(ret), K(filename));
    } else if ('/' == filename[filename_len - 1]) {
      // check dir and filename
      // filename ends with '/', invalid filename
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid filename, should not end with '/'", K(ret), K(filename));
    } else {
      // check whether filename includes '/', which means filename has directory path inside it
      // should ignore directory path
      const char *last_slash = NULL;
      if (NULL != (last_slash = STRRCHR(filename, '/'))) {
        LOG_INFO("dir is part of filename, should ignore dir in full path",
            K(dir), K(filename));
        dir_included = true;
        real_filename = last_slash + 1;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (dir_included && OB_FAIL(format_full_path(full_path, sizeof(full_path), dir, real_filename))) {
      LOG_WARN("fail to format full path", K(ret), K(dir), K(real_filename));
    } else if (!dir_included && OB_FAIL(format_full_path(full_path, sizeof(full_path), dir, filename))) {
      LOG_WARN("fail to format full path", K(ret), K(dir), K(filename));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.unlink(full_path))) {
      LOG_WARN("fail to unlink file", K(ret), K(full_path));
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_UTL_FILE_INVALID_PATH;
      }
    }
  }

  return ret;
}

int ObUtlFileHandler::fcopy(const char *src_dir, const char *src_filename,
                            const char *dst_dir, const char *dst_filename,
                            const int64_t start, const int64_t *end_line)
{
  int ret = OB_SUCCESS;
  bool b_src_path_exist = false;
  int64_t end = LLONG_MAX; // total line num should not exceed LLONG_MAX
  ObIOFd src_io_fd, dst_io_fd;
  SMART_VARS_2((char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], src_path),
               (char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], dst_path)) {
    if ((NULL != end_line) && FALSE_IT(end = *end_line)) {
      // do nothing
    } else if (OB_UNLIKELY(start < 1) || OB_UNLIKELY(end < 1) || OB_UNLIKELY(start > end)) {
      // 1 for first line
      ret = OB_UTL_FILE_INVALID_OFFSET;
      LOG_WARN("invalid start and end line", K(ret), K(start), K(end));
    } else if (OB_UNLIKELY(!is_valid_path(src_dir))) {
      ret = OB_UTL_FILE_INVALID_PATH;
      LOG_WARN("invalid src dir", K(ret), K(src_dir));
    } else if (OB_UNLIKELY(!is_valid_path(src_filename))) {
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid src filename", K(ret), K(src_filename));
    } else if (OB_UNLIKELY(!is_valid_path(dst_dir))) {
      ret = OB_UTL_FILE_INVALID_PATH;
      LOG_WARN("invalid dst dir", K(ret), K(dst_dir));
    } else if (OB_UNLIKELY(!is_valid_path(dst_filename))) {
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid dst filename", K(ret), K(dst_filename));
    } else if (OB_FAIL(format_full_path(src_path, sizeof(src_path), src_dir, src_filename))) {
      LOG_WARN("fail to format full path", K(ret), K(src_dir), K(src_filename));
      ret = OB_UTL_FILE_INVALID_PATH;
    } else if (OB_FAIL(format_full_path(dst_path, sizeof(dst_path), dst_dir, dst_filename))) {
      LOG_WARN("fail to format full path", K(ret), K(dst_dir), K(dst_filename));
      ret = OB_UTL_FILE_INVALID_PATH;
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.exist(src_path, b_src_path_exist))) {
      LOG_WARN("fail to check existence for src path", K(ret), K(src_path));
      ret = OB_UTL_FILE_INVALID_OPERATION;
    } else if (OB_UNLIKELY(!b_src_path_exist)) {
      ret = OB_UTL_FILE_INVALID_OPERATION;
      LOG_WARN("src path does not exist", K(ret), K(src_path), K(b_src_path_exist));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(src_path, READ_FLAGS,
        ObUtlFileConstants::UTL_FILE_ACCESS_MODE, src_io_fd))) {
      LOG_WARN("fail to open src path", K(ret), K(src_path), LITERAL_K(READ_FLAGS),
          LITERAL_K(ObUtlFileConstants::UTL_FILE_ACCESS_MODE));
      ret = OB_UTL_FILE_INVALID_OPERATION;
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(dst_path, WRITE_FLAGS,
        ObUtlFileConstants::UTL_FILE_ACCESS_MODE, dst_io_fd))) {
      LOG_WARN("fail to open dst path", K(ret), K(dst_path), LITERAL_K(WRITE_FLAGS),
          LITERAL_K(ObUtlFileConstants::UTL_FILE_ACCESS_MODE));
      ret = OB_UTL_FILE_INVALID_OPERATION;
    } else if (OB_FAIL(copy_lines(src_io_fd, dst_io_fd, start, end))) {
      // read data from src file, then write into dst file
      LOG_WARN("fail to copy lines", K(ret), K(src_path), K(dst_path),
          K(src_io_fd), K(dst_io_fd), K(start), K(end));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = LOCAL_DEVICE_INSTANCE.close(dst_io_fd)))) {
        LOG_WARN("failed to close dst file fd", K(tmp_ret), K(dst_io_fd));
      }
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = LOCAL_DEVICE_INSTANCE.close(src_io_fd)))) {
        LOG_WARN("failed to close src file fd", K(tmp_ret), K(src_io_fd));
      }
    }
  }

  return ret;
}

int ObUtlFileHandler::fgetattr(const char *dir, const char *filename,
                               bool &b_exist, int64_t &file_length, int64_t &block_size)
{
  int ret = OB_SUCCESS;
  ObIODFileStat statbuf;
  SMART_VAR(char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], full_path) {
    if (OB_UNLIKELY(!is_valid_path(dir))) {
      ret = OB_UTL_FILE_INVALID_PATH;
      LOG_WARN("invalid dir", K(ret), K(dir));
    } else if (OB_UNLIKELY(!is_valid_path(filename))) {
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid filename", K(ret), K(filename));
    } else if (OB_FAIL(format_full_path(full_path, sizeof(full_path), dir, filename))) {
      LOG_WARN("fail to format full path", K(ret), K(dir), K(filename));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.stat(full_path, statbuf))) {
      LOG_WARN("fail to stat", K(ret), K(full_path));
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        b_exist = false;
        ret = OB_SUCCESS; // reset ret
      }
    } else {
      b_exist = true;
      file_length = statbuf.size_;
      block_size = statbuf.block_size_;
    }
  }
  return ret;
}

int ObUtlFileHandler::fgetpos(const int64_t &fd, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  if (OB_UNLIKELY(!io_fd.is_normal_file())) {
    ret = OB_UTL_FILE_INVALID_FILEHANDLE;
    LOG_WARN("invalid handle", K(ret), K(io_fd));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.lseek(io_fd, 0, SEEK_CUR, pos))) {
    LOG_WARN("fail to fseek", K(ret), K(io_fd));
  }
  return ret;
}

int ObUtlFileHandler::frename(const char *src_dir, const char *src_filename,
                              const char *dst_dir, const char *dst_filename, bool overwrite)
{
  int ret = OB_SUCCESS;
  bool b_src_path_exist = false;
  bool b_dst_path_exist = false;
  SMART_VARS_2((char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], src_path),
               (char[ObUtlFileConstants::UTL_PATH_SIZE_LIMIT * 2], dst_path)) {
    if (OB_UNLIKELY(!is_valid_path(src_dir))) {
      ret = OB_UTL_FILE_INVALID_PATH;
      LOG_WARN("invalid src dir", K(ret), K(src_dir));
    } else if (OB_UNLIKELY(!is_valid_path(src_filename))) {
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid src filename", K(ret), K(src_filename));
    } else if (OB_UNLIKELY(!is_valid_path(dst_dir))) {
      ret = OB_UTL_FILE_INVALID_PATH;
      LOG_WARN("invalid dst dir", K(ret), K(dst_dir));
    } else if (OB_UNLIKELY(!is_valid_path(dst_filename))) {
      ret = OB_UTL_FILE_INVALID_FILENAME;
      LOG_WARN("invalid dst filename", K(ret), K(dst_filename));
    } else if (OB_FAIL(format_full_path(src_path, sizeof(src_path), src_dir, src_filename))) {
      LOG_WARN("fail to format full path", K(ret), K(src_dir), K(src_filename));
    } else if (OB_FAIL(format_full_path(dst_path, sizeof(dst_path), dst_dir, dst_filename))) {
      LOG_WARN("fail to format full path", K(ret), K(dst_dir), K(dst_filename));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.exist(src_path, b_src_path_exist))) {
      LOG_WARN("fail to check existence for src path", K(ret), K(src_path));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.exist(dst_path, b_dst_path_exist))) {
      LOG_WARN("fail to check existence for dst path", K(ret), K(dst_path));
    } else if (!b_src_path_exist) {
      ret = OB_UTL_FILE_RENAME_FAILED;
      LOG_WARN("src path does not exist", K(ret), K(src_path), K(b_src_path_exist));
    } else if (b_dst_path_exist && !overwrite) {
      ret = OB_UTL_FILE_RENAME_FAILED;
      LOG_WARN("dst path exists and overwrite is prohibited", K(ret), K(src_path),
          K(dst_path), K(b_dst_path_exist), K(overwrite));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.rename(src_path, dst_path))) {
      LOG_WARN("fail to rename file", K(ret), K(src_path), K(b_src_path_exist),
          K(dst_path), K(b_dst_path_exist), K(overwrite));
    }
  }
  return ret;
}

int ObUtlFileHandler::fis_open(const int64_t &fd, bool &b_open)
{
  int ret = OB_SUCCESS;
  b_open = (fd > 0);
  return ret;
}

bool ObUtlFileHandler::is_valid_path(const char *path, size_t &path_len)
{
  return OB_NOT_NULL(path) && ((path_len = STRLEN(path)) > 0)
      && (path_len < ObUtlFileConstants::UTL_PATH_SIZE_LIMIT);
}

bool ObUtlFileHandler::is_valid_open_mode(const char *open_mode)
{
  bool b_ret = false;
  static const char *supported_open_mode[] = { "r", "w", "a", "rb", "wb", "ab" };
  for (int i = 0; i < sizeof(supported_open_mode) / sizeof(const char *); ++i) {
    if (0 == STRCASECMP(open_mode, supported_open_mode[i])) {
      b_ret = true;
      break;
    }
  }
  return b_ret;
}

int ObUtlFileHandler::convert_open_mode(const char *open_mode, int &flags)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(open_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(open_mode));
  } else if (0 == STRCASECMP(open_mode, "r")) {
    flags = O_RDONLY;
  } else if (0 == STRCASECMP(open_mode, "w")) {
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  } else if (0 == STRCASECMP(open_mode, "a")) {
    flags = O_WRONLY | O_CREAT | O_APPEND;
  } else if (0 == STRCASECMP(open_mode, "rb")) {
    flags = O_RDONLY;
  } else if (0 == STRCASECMP(open_mode, "wb")) {
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  } else if (0 == STRCASECMP(open_mode, "ab")) {
    flags = O_WRONLY | O_CREAT | O_APPEND;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported open mode", K(ret), K(open_mode));
  }
  return ret;
}

int ObUtlFileHandler::format_full_path(char *full_path, size_t len,
    const char *dir, const char *filename)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir) || OB_UNLIKELY(0 == STRLEN(dir))
      || OB_ISNULL(filename) || OB_UNLIKELY(0 == STRLEN(filename))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(dir), K(filename));
  } else {
    int p_ret = snprintf(full_path, len, "%s/%s", dir, filename);
    if (p_ret < 0 || p_ret >= len) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", K(ret), K(dir), K(filename), K(len));
    }
  }
  return ret;
}

int ObUtlFileHandler::put_impl(const int64_t &fd, const char *buffer, const int64_t size,
                               int64_t &write_size, bool autoflush)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  if (OB_UNLIKELY(!io_fd.is_normal_file())) {
    ret = OB_UTL_FILE_INVALID_FILEHANDLE;
    LOG_WARN("invalid handle", K(ret), K(io_fd));
  } else if (OB_ISNULL(buffer) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buffer), K(size));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.write(io_fd, buffer, size, write_size))) {
    LOG_WARN("fail to write", K(ret), K(size));
  } else if (autoflush && OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync(io_fd))) {
    LOG_WARN("fail to flush", K(ret), K(io_fd));
  }
  return ret;
}

int ObUtlFileHandler::read_impl(const int64_t &fd, char *buffer, const int64_t len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd(&LOCAL_DEVICE_INSTANCE, ObIOFd::NORMAL_FILE_ID, fd);
  int retry_cnt = 0;
  int64_t size = len;
  while (OB_SUCC(ret)
      && size > 0
      && retry_cnt++ < ObUtlFileConstants::DEFAULT_IO_RETRY_CNT) {
    int64_t sz = 0;
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.read(io_fd, buffer, size, sz))) {
      LOG_WARN("fail to read", K(ret), K(size));
    } else {
      read_size += sz;
      size -= sz;
      buffer += sz;
    }
  }
  return ret;
}

int ObUtlFileHandler::find_single_line(const char *buffer, const int64_t len, int64_t &pos, bool &found)
{
  int ret = OB_SUCCESS;
  const char *p = NULL;
  found = false;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid line", K(buffer), K(len));
  } else if (NULL == (p = STRSTR(buffer, ObUtlFileHandler::LINE_TERMINATOR))) {
    // cannot find line terminator in buffer, so whole buffer is regarded as a single line
    pos = len;
    LOG_INFO("fail to find occurence of line terminator in buffer", K(ret), K(buffer), K(len),
        K(pos), K(found));
  } else {
    pos = p - buffer;
    found = true;
  }
  return ret;
}

int ObUtlFileHandler::find_and_copy_lines(const char *buffer, const int64_t len,
                                          const ObIOFd &dst_io_fd,
                                          const int64_t start_line, const int64_t end_line,
                                          int64_t &line_num)
{
  int ret = OB_SUCCESS;
  const char *write_buffer = buffer;
  int64_t write_buffer_begin = 0;
  int64_t write_buffer_end = 0;
  int64_t write_buffer_size = 0;
  int64_t write_size = 0;

  if (OB_ISNULL(buffer) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid line", K(buffer), K(len));
  } else {
    int64_t line_start_pos = 0;
    int64_t line_feed_pos = -1;
    const char *single_line = buffer;
    int64_t buffer_len = len;
    bool found = false;
    while (OB_SUCC(ret) && line_start_pos < len && line_num <= end_line) {
      if (line_num == start_line) {
        write_buffer = single_line;
        write_buffer_begin = line_start_pos;
        write_buffer_end = line_start_pos;
      }

      if (OB_FAIL(find_single_line(single_line, buffer_len, line_feed_pos, found))) {
        LOG_WARN("failed to find single line", K(ret), K(single_line), K(buffer_len));
      } else if (found) {
        if (line_num >= start_line && line_num <= end_line) {
          write_buffer_end += (line_feed_pos + 1);
        }
        single_line += (line_feed_pos + 1);
        line_start_pos += (line_feed_pos + 1);
        buffer_len = len - line_start_pos;
        ++line_num;
      } else {
        // cannot find line feed in buffer
        if (line_num >= start_line && line_num <= end_line) {
          write_buffer_end = len;
        }
        break;
      }
    }
  }

  // write buffer to dst file
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (FALSE_IT(write_buffer_size = write_buffer_end - write_buffer_begin)) {
    // do nothing
  } else if (write_buffer_size > 0
      && OB_FAIL(LOCAL_DEVICE_INSTANCE.write(dst_io_fd, write_buffer, write_buffer_size, write_size))) {
    LOG_WARN("fail to write to dst file", K(ret), K(dst_io_fd), K(write_buffer), K(write_buffer_size));
  }

  return ret;
}

int ObUtlFileHandler::copy_lines(const ObIOFd &src_io_fd, const ObIOFd &dst_io_fd,
                                 const int64_t start_line, const int64_t end_line)
{
  int ret = OB_SUCCESS;
  int64_t line_num = 1;
  int64_t offset = 0;
  int64_t read_size = 0;

  const int64_t size = ObUtlFileConstants::UTF_FILE_WRITE_BUFFER_SIZE;
  SMART_VAR(char[size], data) {
    do {
      if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(src_io_fd, offset, size, data, read_size))) {
        LOG_WARN("fail to read", K(ret), K(src_io_fd), K(offset), K(size));
        ret = OB_UTL_FILE_READ_ERROR;
      } else if (0 == read_size) {
        // do nothing
      } else if (OB_FAIL(find_and_copy_lines(data, read_size, dst_io_fd,
          start_line, end_line, line_num))) {
        LOG_WARN("fail to find lines", K(ret), K(read_size), K(dst_io_fd),
            K(start_line), K(end_line), K(line_num));
      } else {
        offset += read_size;
      }
    } while (OB_SUCC(ret) && 0 != read_size && line_num <= end_line);
  }
  if (OB_SUCC(ret) && start_line > line_num) {
    ret = OB_UTL_FILE_INVALID_OFFSET;
    LOG_WARN("invalid offset, start line is bigger than file line count", K(ret));
  }
  return ret;
}

int ObUtlFileHandler::check_buffer(const char *buffer, const int64_t size,
                                   const int64_t max_line_size, int64_t &last_valid_pos)
{
  int ret = OB_SUCCESS;
  int64_t p = 0, q = 0;
  while (p <= q && q < size) {
    if (buffer[q] == '\n') {
      int64_t single_line_size = q - p + 1;
      if (single_line_size > max_line_size) {
        ret = OB_UTL_FILE_WRITE_ERROR;
        LOG_WARN("invalid single line size", K(ret), K(buffer), K(p),
            K(single_line_size), K(max_line_size));
        break;
      } else {
        last_valid_pos = q;
        p = ++q;
      }
    } else {
      ++q;
    }
  }
  // for fflush check buffer size(need_crlf is false)
  if (OB_SUCC(ret) && q == size && '\n' != buffer[size - 1]) {
    int64_t left_size = q - p;
    if (left_size + 1 > max_line_size) {
      // 1 for '\n'
      ret = OB_UTL_FILE_WRITE_ERROR;
      LOG_WARN("invalid single line size", K(ret), K(buffer), K(p), K(q),
          K(left_size), K(max_line_size));
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
