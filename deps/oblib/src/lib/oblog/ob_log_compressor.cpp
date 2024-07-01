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

#define USING_LOG_PREFIX LIB
#include <dirent.h>
#include <regex.h>
#include <utime.h>
#include <sys/statfs.h>

#include "lib/oblog/ob_log_compressor.h"
#include "lib/thread/thread_mgr.h"
#include "lib/compress/ob_compressor_pool.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {

ObLogCompressor::ObLogCompressor() :
  is_inited_(false), stopped_(true), loop_interval_(OB_SYSLOG_COMPRESS_LOOP_INTERVAL),
  max_disk_size_(0), min_uncompressed_count_(0), compress_func_(NONE_COMPRESSOR),
  compressor_(NULL), next_compressor_(NULL), oldest_files_(cmp_, NULL)
{}

ObLogCompressor::~ObLogCompressor()
{
  if (is_inited_) {
    destroy();
  }
}

int ObLogCompressor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("the ObLogCompressor has been inited", K(ret));
  } else if (OB_FAIL(log_compress_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("failed to init ObThreadCond", K(ret));
  } else {
    strncpy(syslog_dir_, OB_SYSLOG_DIR, strlen(OB_SYSLOG_DIR));
    strncpy(alert_log_dir_, OB_ALERT_LOG_DIR, strlen(OB_ALERT_LOG_DIR));
    stopped_ = false;
    if (OB_FAIL(TG_SET_RUNNABLE_AND_START(TGDefIDs::SYSLOG_COMPRESS, *this))) {
      LOG_ERROR("failed to start log compression thread", K(ret));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      LOG_ERROR("syslog compressor init failed ", K(ret));
      stopped_ = true;
      log_compress_cond_.destroy();
    }
  }
  LOG_INFO("syslog compressor init finish ", K(ret));

  return ret;
}

void ObLogCompressor::stop()
{
  if (is_inited_) {
    stopped_ = true;
    TG_STOP(TGDefIDs::SYSLOG_COMPRESS);
  }
}

void ObLogCompressor::wait()
{
  if (is_inited_) {
    TG_WAIT(TGDefIDs::SYSLOG_COMPRESS);
  }
}

void ObLogCompressor::awake()
{
  ObThreadCondGuard guard(log_compress_cond_);
  log_compress_cond_.signal();
}

void ObLogCompressor::destroy()
{
  if (is_inited_) {
    {
      ObThreadCondGuard guard(log_compress_cond_);
      stopped_ = true;
      log_compress_cond_.signal();
    }
    stop();
    wait();
    log_compress_cond_.destroy();
    max_disk_size_ = 0;
    compress_func_ = NONE_COMPRESSOR;
    min_uncompressed_count_ = 0;
    compressor_ = NULL;
    next_compressor_ = NULL;
    is_inited_ = false;
    LOG_INFO("syslog compressor destroyed");
  }
}

int ObLogCompressor::set_max_disk_size(int64_t max_disk_size)
{
  int ret = OB_SUCCESS;
  if (max_disk_size < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (max_disk_size > 0 && max_disk_size < OB_MIN_SYSLOG_DISK_SIZE) {
      max_disk_size = OB_MIN_SYSLOG_DISK_SIZE;
    }
    if ((max_disk_size_ == 0 && max_disk_size > 0)
        || (max_disk_size_ > 0 && max_disk_size_ > max_disk_size)) {
      max_disk_size_ = max_disk_size;
      awake();
    } else {
      max_disk_size_ = max_disk_size;
    }
  }
  return ret;
}

int ObLogCompressor::set_compress_func(const char *compress_func_ptr)
{
  int ret = OB_SUCCESS;
  ObCompressorType new_compress_func;
  if (OB_ISNULL(compress_func_ptr)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(compress_func_ptr, new_compress_func))) {
    // do nothing
  } else if (new_compress_func == NONE_COMPRESSOR
             || new_compress_func == ZSTD_COMPRESSOR
             || new_compress_func == ZSTD_1_3_8_COMPRESSOR) {
    // to do: support ZLIB_COMPRESSOR
    if (new_compress_func != compress_func_) {
      LOG_INFO("modify log compress func", K(compress_func_), K(new_compress_func));
      if (OB_FAIL(set_next_compressor_(new_compress_func))) {
        LOG_ERROR("fail to modify log compress func", K(ret), K(compress_func_), K(new_compress_func));
      } else if (compress_func_ == NONE_COMPRESSOR && new_compress_func != NONE_COMPRESSOR) {
        compress_func_ = new_compress_func;
        // from disable to enanble, awake compress thread
        awake();
      } else {
        compress_func_ = new_compress_func;
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObLogCompressor::set_min_uncompressed_count(int64_t min_uncompressed_count)
{
  int ret = OB_SUCCESS;
  if (min_uncompressed_count < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    min_uncompressed_count_ = min_uncompressed_count;
  }
  return ret;
}

bool ObLogCompressor::is_compressed_file(const char *file)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  regex_t regex;
  if (OB_ISNULL(file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumet for file is null", K(ret));
  } else if (OB_FAIL(regcomp(&regex, OB_COMPRESSED_SYSLOG_FILE_PATTERN, REG_EXTENDED))) {
    LOG_ERROR("failed to compile regex pattern", K(ret));
  } else {
    if (regexec(&regex, file, 0, NULL, 0) == 0) {
      is_compressed = true;
    }
    regfree(&regex);
  }
  return is_compressed;
}

void ObLogCompressor::run1()
{
  lib::set_thread_name("SyslogCompress");
  LOG_INFO("syslog compress thread start");
  log_compress_loop_();
  LOG_INFO("syslog compress thread finish");
}

void ObLogCompressor::log_compress_loop_()
{
  int ret = OB_SUCCESS;
  int log_type = OB_SYSLOG_COMPRESS_TYPE_COUNT;
  const int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
  const int dest_size = OB_SYSLOG_COMPRESS_BUFFER_SIZE;
  char *src_buf = (char *)ob_malloc(src_size + dest_size, "SyslogCompress");
  char *dest_buf = src_buf + src_size;
  regex_t regex_archive;
  regex_t regex_uncompressed;

  if (OB_ISNULL(src_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to ob_malloc", K(ret));
  } else if (OB_FAIL(regcomp(&regex_archive, OB_ARCHIVED_SYSLOG_FILE_PATTERN, REG_EXTENDED))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("failed to compile archive regex pattern", K(ret));
  } else if (OB_FAIL(regcomp(&regex_uncompressed, OB_UNCOMPRESSED_SYSLOG_FILE_PATTERN, REG_EXTENDED))) {
    regfree(&regex_archive);
    ret = OB_ERR_SYS;
    LOG_ERROR("failed to compile uncompressed regex pattern", K(ret));
  } else {
    ObSyslogFile syslog_file;
    char compress_files[OB_SYSLOG_COMPRESS_TYPE_COUNT][OB_MAX_SYSLOG_FILE_NAME_SIZE] = {{0}};
    int64_t log_file_count[OB_SYSLOG_COMPRESS_TYPE_COUNT] = {0};
    int64_t log_min_time[OB_SYSLOG_COMPRESS_TYPE_COUNT] = {0};
    int64_t compressed_file_count = 0;
    int64_t deleted_file_count = 0;

    while (!stopped_) {
      // wait until stoped or needing to work
      {
        common::ObThreadCondGuard guard(log_compress_cond_);
        while (!stopped_ && !is_enable_compress() && max_disk_size_ <= 0 && OB_LOGGER.get_max_file_index() <= 0) {
          log_compress_cond_.wait_us(loop_interval_);
        }
      }

      if (!stopped_) {
        // record start time
        int64_t start_time = ObClockGenerator::getClock();
        bool enable_delete_file = max_disk_size_ > 0 || OB_LOGGER.get_max_file_index() > 0;
        ret = OB_SUCCESS;

        // check whether need to compress or delete file
        int64_t total_size = 0;
        struct dirent* entry;
        struct stat stat_info;
        DIR* dir = NULL;
        compressed_file_count = 0;
        deleted_file_count = 0;
        for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
          log_file_count[i] = 0;
          log_min_time[i] = INT64_MAX;
        }
        oldest_files_.reset();

        if (OB_ISNULL(dir = opendir(syslog_dir_))) {
          ret = OB_ERR_SYS;
          LOG_ERROR("failed to open syslog directory", K(ret), K(errno), K(syslog_dir_));
        } else {
          while (OB_SUCC(ret) && OB_NOT_NULL(entry = readdir(dir))) {
            if (strncmp(entry->d_name, ".", 1) == 0 || strncmp(entry->d_name, "..", 2) == 0) {
              continue;
            }
            snprintf(syslog_file.file_name_, OB_MAX_SYSLOG_FILE_NAME_SIZE, "%s/%s", syslog_dir_, entry->d_name);
            if (stat(syslog_file.file_name_, &stat_info) == -1) {
              ret = OB_ERR_SYS;
              LOG_WARN("failed to get file info", K(ret), K(errno), K(syslog_file.file_name_));
              continue;
            }
            if (S_ISREG(stat_info.st_mode)) {
              total_size += stat_info.st_size;
              int64_t tmp_time = stat_info.st_mtim.tv_sec * 1000000000L + stat_info.st_mtim.tv_nsec;
              syslog_file.mtime_ = tmp_time;
              if (enable_delete_file
                  && regexec(&regex_archive, entry->d_name, 0, NULL, 0) == 0
                  && OB_FAIL(oldest_files_.push(syslog_file))) {
                LOG_ERROR("failed to put file into array", K(ret), K(syslog_file.file_name_), K(tmp_time));
              }

              if (regexec(&regex_uncompressed, entry->d_name, 0, NULL, 0) == 0) {
                int log_type = get_log_type_(syslog_file.file_name_);
                if (log_type >= 0 && log_type < OB_SYSLOG_COMPRESS_TYPE_COUNT) {
                  log_file_count[log_type]++;
                  if (tmp_time < log_min_time[log_type]) {
                    strncpy(compress_files[log_type], syslog_file.file_name_, OB_MAX_SYSLOG_FILE_NAME_SIZE);
                    log_min_time[log_type] = tmp_time;
                  }
                }
              }
            }
          }
        }
        if (OB_NOT_NULL(dir)) {
          closedir(dir);
          dir = NULL;
        }
        if (OB_FAIL(ret)) {
          // skip
        } else if (OB_ISNULL(dir = opendir(alert_log_dir_))) {
          ret = OB_ERR_SYS;
          LOG_ERROR("failed to open alert log directory", K(ret), K(errno), K(alert_log_dir_));
        } else {
          while (OB_SUCC(ret) && OB_NOT_NULL(entry = readdir(dir))) {
            if (strncmp(entry->d_name, ".", 1) == 0 || strncmp(entry->d_name, "..", 2) == 0) {
              continue;
            }
            snprintf(syslog_file.file_name_, OB_MAX_SYSLOG_FILE_NAME_SIZE, "%s/%s", alert_log_dir_, entry->d_name);
            if (stat(syslog_file.file_name_, &stat_info) == -1) {
              ret = OB_ERR_SYS;
              LOG_WARN("failed to get file info", K(ret), K(errno), K(syslog_file.file_name_));
              continue;
            }
            if (S_ISREG(stat_info.st_mode)) {
              total_size += stat_info.st_size;
              int64_t tmp_time = stat_info.st_mtim.tv_sec * 1000000000L + stat_info.st_mtim.tv_nsec;
              syslog_file.mtime_ = tmp_time;
              if (enable_delete_file
                  && regexec(&regex_archive, entry->d_name, 0, NULL, 0) == 0
                  && OB_FAIL(oldest_files_.push(syslog_file))) {
                LOG_ERROR("failed to put file into array", K(ret), K(syslog_file.file_name_), K(tmp_time));
              }
            }
          }
        }
        if (OB_NOT_NULL(dir)) {
          closedir(dir);
          dir = NULL;
        }

        // get disk remaining size
        int64_t disk_remaining_size = get_disk_remaining_size_();
        disk_remaining_size = disk_remaining_size >=0 ? disk_remaining_size : INT64_MAX;
        if (max_disk_size_ > 0 && max_disk_size_ - total_size < disk_remaining_size) {
          disk_remaining_size = max_disk_size_ - total_size;
        }

        // compress syslog file if necessary
        if (OB_SUCC(ret) && !stopped_ && is_enable_compress() && disk_remaining_size < OB_SYSLOG_COMPRESS_RESERVE_SIZE) {
          if (compressor_ != next_compressor_) {
            compressor_ = next_compressor_;
          }
          if (OB_ISNULL(compressor_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected error, compressor is null", K(ret));
          } else {
            for (int i = 0; OB_SUCC(ret) && i < OB_SYSLOG_COMPRESS_TYPE_COUNT && is_enable_compress(); i++) {
              if (log_file_count[i] > min_uncompressed_count_) {
                int64_t file_size = get_file_size_(compress_files[i]);
                if (OB_FAIL(compress_single_file_(compress_files[i], src_buf, dest_buf))) {
                  LOG_ERROR("failed to compress file", K(ret), K(compress_files[i]));
                } else {
                  // estimated value
                  total_size -= file_size;
                  compressed_file_count++;
                }
                log_file_count[i]--;
              }
            }
          }
        }

        // delete oldest syslog file if necessary
        enable_delete_file = enable_delete_file && (max_disk_size_ > 0 || OB_LOGGER.get_max_file_index() > 0);
        if (OB_SUCC(ret) && !stopped_ && enable_delete_file && disk_remaining_size < OB_SYSLOG_DELETE_RESERVE_SIZE) {
          int array_size = oldest_files_.count();
          const char *delete_file = NULL;
          const ObSyslogFile *syslog_ptr = NULL;
          int64_t need_delete_size = OB_SYSLOG_DELETE_RESERVE_SIZE - disk_remaining_size;
          for (int i = 0; i < array_size && need_delete_size > 0 && OB_SUCC(ret); i++) {
            if (OB_FAIL(oldest_files_.top(syslog_ptr))) {
              break;
            } else if (OB_NOT_NULL(syslog_ptr)) {
              delete_file = syslog_ptr->file_name_;
              int64_t delete_file_size = get_file_size_(delete_file);
              if (delete_file_size >= 0) {
                LOG_DEBUG("log compressor unlink file", K(delete_file), K(need_delete_size), K(delete_file_size));
                unlink(delete_file);
                need_delete_size = need_delete_size - delete_file_size;
                disk_remaining_size += delete_file_size;
                deleted_file_count++;
              }
              delete_file = NULL;
              ret = oldest_files_.pop();
            }
          }
        }

        // record cost time, sleep
        int64_t cost_time = ObClockGenerator::getClock() - start_time;
        LOG_INFO("log compressor cycles once. ", K(ret), K(cost_time),
                 K(compressed_file_count), K(deleted_file_count), K(disk_remaining_size));
        cost_time = cost_time >= 0 ? cost_time:0;
        if (!stopped_ && cost_time < loop_interval_) {
          usleep(loop_interval_ - cost_time);
        }
      } // if (!stopped_)
    } // while (!stopped_)
    regfree(&regex_archive);
    regfree(&regex_uncompressed);
  }
  if (OB_NOT_NULL(src_buf)) {
    ob_free(src_buf);
  }
}

int ObLogCompressor::get_compressed_file_name_(const char *file_name, char compressed_file_name[])
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int size = strnlen(file_name, OB_MAX_SYSLOG_FILE_NAME_SIZE);
    int suffix_size = strlen(OB_SYSLOG_COMPRESS_ZSTD_SUFFIX);
    if (size <= 0 || size + suffix_size + 1 > OB_MAX_SYSLOG_FILE_NAME_SIZE) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      strncpy(compressed_file_name, file_name, size);
      strncpy(compressed_file_name + size, OB_SYSLOG_COMPRESS_ZSTD_SUFFIX, suffix_size);
      *(compressed_file_name + size + suffix_size) = '\0';
    }
  }
  return ret;
}


int ObLogCompressor::compress_single_block_(
    char *dest, size_t dest_size, const char *src, size_t src_size, size_t &return_size)
{
  int ret = OB_SUCCESS;
  int64_t size = -1;
  if (OB_FAIL(((ObCompressor *)compressor_)->compress(src, src_size, dest, dest_size, size))) {
    LOG_ERROR("failed to compress single block", K(ret));
  } else {
    return_size = size;
  }
  return ret;
}

int ObLogCompressor::compress_single_file_(const char *file_name, char *src_buf, char *dest_buf)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(file_name) || OB_ISNULL(src_buf) || OB_ISNULL(dest_buf)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    static const int sleep_us = 50 * 1000;  // 50ms
    int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
    int dest_size = OB_SYSLOG_COMPRESS_BUFFER_SIZE;
    char compressed_file_name[OB_MAX_SYSLOG_FILE_NAME_SIZE];
    FILE *input_file = NULL;
    FILE *output_file = NULL;
    // record file modify time
    struct stat st;
    stat(file_name, &st);
    time_t last_modified_time = st.st_mtime;

    if (OB_FAIL(get_compressed_file_name_(file_name, compressed_file_name))) {
      LOG_ERROR("failed to get compressed file name", K(ret), K(file_name));
    } else if (strlen(compressed_file_name) < strlen(file_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get compressed file name", K(ret), K(file_name));
    } else if (NULL == (input_file = fopen(file_name, "r"))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("failed to open file", K(ret), K(errno), K(file_name));
    } else if (NULL == (output_file = fopen(compressed_file_name, "w"))) {
      ret = OB_ERR_SYS;
      fclose(input_file);
      LOG_ERROR("failed to open file", K(ret), K(errno), K(compressed_file_name));
    } else {
      LOG_DEBUG("log compressor compress file ", K(file_name), K(compressed_file_name));
      size_t read_size = 0;
      size_t write_size = 0;
      while (OB_SUCC(ret) && !feof(input_file)) {
        if ((read_size = fread(src_buf, 1, src_size, input_file)) > 0) {
          if (OB_FAIL(compress_single_block_(dest_buf, dest_size, src_buf, read_size, write_size))) {
            LOG_ERROR("failed to compress syslog block", K(ret));
          } else if (write_size != fwrite(dest_buf, 1, write_size, output_file)) {
            ret = OB_ERR_SYS;
            LOG_ERROR("failed to write file", K(ret), K(errno), K(compressed_file_name));
          }
        }
        usleep(sleep_us);
      }
      fclose(input_file);
      fclose(output_file);
      if (OB_FAIL(ret)) {
        unlink(compressed_file_name);
      } else {
        unlink(file_name);
        (void)set_last_modify_time_(compressed_file_name, last_modified_time);
      }
    }
  }

  return ret;
}

int ObLogCompressor::set_last_modify_time_(const char *file_name, const time_t &newTime) {
  int ret = OB_SUCCESS;
  struct utimbuf newTimes;
  newTimes.actime = newTime;
  newTimes.modtime = newTime;

  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (utime(file_name, &newTimes) != 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("failed to set the file's last modified time", K(ret), K(file_name), K(newTime));
  }

  return ret;
}

int ObLogCompressor::set_next_compressor_(ObCompressorType compress_func) {
  int ret = OB_SUCCESS;
  ObCompressor *compressor = NULL;
  if (compress_func == NONE_COMPRESSOR) {
    next_compressor_ = NULL;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compress_func, compressor))) {
    LOG_ERROR("Fail to get_compressor", K(ret), K(compress_func));
  } else {
    next_compressor_ = compressor;
    if (OB_ISNULL(compressor_)) {
      compressor_ = compressor;
    }
  }
  return ret;
}

int ObLogCompressor::get_log_type_(const char *file_name) {
  int type = OB_SYSLOG_COMPRESS_TYPE_COUNT;
  int name_len = strnlen(file_name, OB_MAX_SYSLOG_FILE_NAME_SIZE);
  if (name_len >= strlen("trace.log")) {
    int dir_len = strlen(syslog_dir_);
    if (name_len > dir_len + 1
        && 0 == strncmp(file_name, syslog_dir_, dir_len)
        && file_name[dir_len] == '/') {
      file_name = file_name + dir_len + 1;
    }
    for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
      if (0 == strncmp(file_name, OB_SYSLOG_FILE_PREFIX[i], strlen(OB_SYSLOG_FILE_PREFIX[i]))) {
        type = i;
        break;
      }
    }
  }

  return type;
}

int64_t ObLogCompressor::get_file_size_(const char *file_name)
{
  int64_t size = -1;
  struct stat st;
  if (stat(file_name, &st) == 0) {
    size = st.st_size;
  }
  return size;
}

int64_t ObLogCompressor::get_disk_remaining_size_()
{
  int ret = OB_SUCCESS;
  int64_t remaining_size = 0;
  struct statfs file_system;
  if (statfs(syslog_dir_, &file_system) == -1) {
    remaining_size = -1;
    ret = OB_ERR_SYS;
    LOG_ERROR("fail to get disk remaining size", K(ret), K(strerror(errno)), K(syslog_dir_));
  } else {
    remaining_size = file_system.f_bsize * file_system.f_bavail;
  }
  return remaining_size;
}

}  // namespace common
}  // namespace oceanbase
