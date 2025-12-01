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

#include "share/ob_admin_dump_helper.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObAdminLogNormalDumper::start_object()
{
  fprintf(stdout, " {");
  return OB_SUCCESS;
}

int ObAdminLogNormalDumper::end_object()
{
  fprintf(stdout, "} ");
  return OB_SUCCESS;
}

int ObAdminLogNormalDumper::dump_key(const char *key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid key is NULL", K(ret));
  } else {
    fprintf(stdout, " %s:", key);
  }
  return ret;
}

int ObAdminLogNormalDumper::dump_int64(int64_t arg)
{
  int ret = OB_SUCCESS;
  fprintf(stdout, " %ld ", arg);
  return ret;
}

int ObAdminLogNormalDumper::dump_uint64(uint64_t arg)
{
  int ret = OB_SUCCESS;
  fprintf(stdout, " %lu ", arg);
  return ret;
}

int ObAdminLogNormalDumper::dump_string(const char *str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid str is NULL", K(ret));
  } else {
    fprintf(stdout, " %s ", str);
  }
  return ret;
}

int ObAdminLogStatDumper::start_object()
{
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::end_object()
{
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_key(const char *key)
{
  UNUSED(key);
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_int64(int64_t arg)
{
  UNUSED(arg);
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_uint64(uint64_t arg)
{
  UNUSED(arg);
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_string(const char *str)
{
  UNUSED(str);
  return OB_SUCCESS;
}

int ObAdminLogJsonDumper::start_object()
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::StartObject() ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::end_object()
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::EndObject() ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_key(const char *arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::Key(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_int64(int64_t arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::Int64(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_uint64(uint64_t arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::Uint64(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_string(const char *arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::String(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

bool ObAdminLogDumpFilter::is_valid() const
{
  return (is_tx_id_valid() || is_tablet_id_valid());
}

int ObAdminLogDumpFilter::parse(const char *str)
{
  int ret = OB_SUCCESS;
  char *ptr1 = NULL;
  char *saveptr1 = NULL;
  char *token1 = NULL;
  char buf[1024];
  char tmp[128];
  const char *TABLET_ID_STR = "tablet_id";
  const char *TX_ID_STR = "tx_id";
  if (NULL != str) {
    strncpy(buf, str, sizeof(buf));
    buf[sizeof(buf) - 1] = '\0';
    for (ptr1 = buf; ;ptr1 = NULL) {
      token1 = strtok_r(ptr1, ";", &saveptr1);
      if (NULL == token1) {
        break;
      } else {
        int i = 0;
        char *ptr2 = NULL;
        char *saveptr2 = NULL;
        char *token2 = NULL;
        for (i = 1, ptr2 = token1; ;ptr2 = NULL, i++) {
          token2 = strtok_r(ptr2, "=", &saveptr2);
          if (NULL == token2) {
            break;
          } else if (1 == (i % 2)) {
            strncpy(tmp, token2, sizeof(tmp));
            tmp[sizeof(tmp) - 1] = '\0';
          } else {
            if (0 == strcmp(tmp, TABLET_ID_STR)) {
              tablet_id_ = atol(token2);
            } else if (0 == strcmp(tmp, TX_ID_STR)) {
              tx_id_ = atol(token2);
            } else {
              // do nothing
            }
          }
        }
      }
    }
  }
  return ret;
}

ObAdminLogDumpFilter &ObAdminLogDumpFilter::operator= (const ObAdminLogDumpFilter &rhs)
{

  tx_id_ = rhs.tx_id_;
  tablet_id_ = rhs.tablet_id_;
  return *this;
}

// 目录结构dir_path_ / tablet_id / trans_id / seq_no
int ObAdminLogDumpBlockHelper::generate_dump_dir(const char *dir_path,
                                                  const int64_t tablet_id,
                                                  const int64_t trans_id,
                                                  const int64_t seq_no,
                                                  char *dir_full_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir_path) || strlen(dir_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid dir_path", K(ret), KP(dir_path));
  } else {
    // Construct full directory path: tablet_id/trans_id/seq_no
    int64_t dir_path_len =
      snprintf(dir_full_path, common::MAX_PATH_SIZE, "%s/%ld/%ld/%ld", dir_path, tablet_id, trans_id, seq_no);
    if (dir_path_len < 0 || dir_path_len >= static_cast<int64_t>(common::MAX_PATH_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "directory path too long", K(ret), K(dir_full_path), K(tablet_id), K(trans_id),
                K(seq_no));
    }
  }
  return ret;
}

int ObAdminLogDumpBlockHelper::create_dir_if_not_exist(const char *dir_full_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir_full_path) || strlen(dir_full_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid dir_full_path", K(ret), KP(dir_full_path));
  } else {
    // Create directories recursively by processing path components
    // Expected format: base_path/tablet_id/trans_id/seq_no
    char temp_path[common::MAX_PATH_SIZE] = {0};
    int64_t path_len = strlen(dir_full_path);

    if (path_len <= 0 || path_len >= static_cast<int64_t>(sizeof(temp_path))) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "dir_full_path too long", K(ret), K(dir_full_path));
    } else {
      // Process path character by character to create each directory level
      for (int64_t i = 1; OB_SUCC(ret) && i <= path_len; ++i) {
        // Check if we've reached a path separator or the end of the path
        if (dir_full_path[i] == '/' || i == path_len) {
          // Copy path up to current position
          int64_t copy_len = i;
          if (copy_len > 0 && copy_len < static_cast<int64_t>(sizeof(temp_path))) {
            memcpy(temp_path, dir_full_path, copy_len);
            temp_path[copy_len] = '\0';

            // Create directory at this level
            int mkdir_ret = ::mkdir(temp_path, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
            if (0 != mkdir_ret) {
              int saved_errno = errno;
              // If directory already exists, it's not an error
              if (EEXIST != saved_errno) {
                ret = OB_IO_ERROR;
                TRANS_LOG(WARN, "failed to create directory", K(ret), K(temp_path),
                          K(saved_errno));
              }
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            TRANS_LOG(WARN, "path component too long", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// 文件名 {slice_idx}_{column_group_idx}_{data_seq}.{scn}.macro
int ObAdminLogDumpBlockHelper::write_macro_block_file(const char *dir_full_path,
                                                      int32_t start_slice_idx,
                                                      uint16_t column_group_idx, int64_t data_seq,
                                                      int64_t scn_val, const char *data_buf,
                                                      int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir_full_path) || strlen(dir_full_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid dir_full_path", K(ret), KP(dir_full_path));
  } else {
    // Construct full file path
    char file_path[common::MAX_PATH_SIZE] = {0};
    int64_t path_len =
      snprintf(file_path, sizeof(file_path), "%s/%d_%u_%ld.%ld.macro", dir_full_path,
               start_slice_idx, column_group_idx, data_seq, scn_val);
    if (path_len < 0 || path_len >= static_cast<int64_t>(sizeof(file_path))) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "file path too long", K(ret), K(dir_full_path), K(start_slice_idx),
                K(column_group_idx), K(data_seq), K(scn_val));
    } else {
      // Create and write file
      int fd =
        ::open(file_path, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      if (fd < 0) {
        ret = OB_IO_ERROR;
        TRANS_LOG(WARN, "failed to create file", K(ret), K(file_path), K(errno));
      } else {
        if (data_len > 0 && OB_NOT_NULL(data_buf)) {
          ssize_t write_size = ::write(fd, data_buf, data_len);
          if (write_size < 0) {
            ret = OB_IO_ERROR;
            TRANS_LOG(WARN, "failed to write file", K(ret), K(file_path), K(errno));
          } else if (static_cast<int64_t>(write_size) != data_len) {
            ret = OB_IO_ERROR;
            TRANS_LOG(WARN, "partial write", K(ret), K(write_size), K(data_len));
          }
        }
        if (OB_FAIL(ret)) {
          // Error occurred, close file anyway
          ::close(fd);
        } else {
          if (0 != ::close(fd)) {
            ret = OB_IO_ERROR;
            TRANS_LOG(WARN, "failed to close file", K(ret), K(file_path), K(errno));
          } else {
            TRANS_LOG(INFO, "successfully dump macro block to file", K(file_path), K(scn_val),
                      K(data_len));
          }
        }
      }
    }
  }
  return ret;
}

void ObLogStat::reset()
{
  group_entry_header_size_ = 0;
  log_entry_header_size_ = 0;
  log_base_header_size_ = 0;
  tx_block_header_size_ = 0;
  tx_log_header_size_ = 0;
  tx_redo_log_size_ = 0;
  mutator_size_ = 0;
  new_row_size_ = 0;
  old_row_size_ = 0;
  total_group_entry_count_ = 0;
  total_log_entry_count_ = 0;
  total_tx_log_count_ = 0;
  total_tx_redo_log_count_ = 0;
  normal_row_count_ = 0;
  table_lock_count_ = 0;
  ext_info_log_count_ = 0;
}

int64_t ObLogStat::total_size() const
{
  int64_t total_size = group_entry_header_size_
      + log_entry_header_size_
      + log_base_header_size_
      + tx_block_header_size_
      + tx_log_header_size_
      + tx_redo_log_size_;
  return total_size;
}
void ObAdminMutatorStringArg::reset()
{
  buf_ = nullptr;
  buf_len_ = 0;
  decompress_buf_ = NULL;
  decompress_buf_len_ = 0;
  flag_ = LogFormatFlag::NO_FORMAT;
  //pos_ = 0;
  //:w
  //:wtx_id_ = 0;
  writer_ptr_ = nullptr;
  filter_.reset();
  log_stat_ = NULL;
  dir_path_ = nullptr;
}

void ObAdminMutatorStringArg::reset_buf()
{
  MEMSET(buf_, 0,  buf_len_);
  pos_ = 0;
}

ObAdminMutatorStringArg &ObAdminMutatorStringArg::operator=(const ObAdminMutatorStringArg &rhs)
{
  buf_ = rhs.buf_;
  buf_len_ = rhs.buf_len_;
  decompress_buf_ = rhs.decompress_buf_;
  decompress_buf_len_ = rhs.decompress_buf_len_;
  pos_ = rhs.pos_;
  flag_ = rhs.flag_;
  writer_ptr_ = rhs.writer_ptr_;
  filter_ = rhs.filter_;
  log_stat_ = rhs.log_stat_;
  dir_path_ = rhs.dir_path_;
  return *this;
}

}//end of namespace share
}//end of namespace oceanbase
