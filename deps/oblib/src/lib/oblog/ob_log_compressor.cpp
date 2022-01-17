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

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "lib/oblog/ob_log_compressor.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "lib/thread/thread_pool.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_mutex.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_async_log_struct.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {
/* Log files are divided into blocks and then compressed. The default block size is (2M - 1K).*/
static const int32_t DEFAULT_COMPRESSION_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
/* To prevent extreme cases where the files become larger after compression,
 * the size of the decompression buffer needs to be larger than the original data.
 * Specific size can refer to the ZSTD code implementation. */
static const int32_t DEFAULT_COMPRESSION_BUFFER_SIZE =
    DEFAULT_COMPRESSION_BLOCK_SIZE + DEFAULT_COMPRESSION_BLOCK_SIZE / 128 + 512 + 19;
static const int32_t DEFAULT_FILE_NAME_SIZE = ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE;
static const int32_t DEFAULT_LOG_QUEUE_DEPTH = 100000;

ObLogCompressor::ObLogCompressor() : is_inited_(false), has_stoped_(true), compressor_(NULL)
{}

ObLogCompressor::~ObLogCompressor()
{}

int ObLogCompressor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_STDERR("The ObLogCompressor has been inited.\n");
  } else if (OB_FAIL(file_list_.init(DEFAULT_LOG_QUEUE_DEPTH))) {
    ret = OB_ERR_SYS;
    LOG_STDERR("Failed to init file_list_.\n");
  } else if (OB_FAIL(log_compress_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    ret = OB_ERR_SYS;
    LOG_STDERR("Failed to init ObThreadCond.\n");
  } else {
    ObCompressor *ptr = NULL;
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, ptr))) {
      LOG_STDERR("Fail to get_compressor, err_code=%d.\n", ret);
    } else {
      compressor_ = ptr;
      has_stoped_ = false;
      if (OB_FAIL(start())) {
        ret = OB_ERR_SYS;
        LOG_STDERR("Fail to create log compression thread.\n");
      } else {
        is_inited_ = true;
        LOG_STDOUT("Success to create thread.\n");
      }
    }
    if (ret) {
      log_compress_cond_.destroy();
    }
  }
  if (!is_inited_) {
    destroy();
  }

  return ret;
}

void ObLogCompressor::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    log_compress_cond_.lock();
    file_list_.destroy();
    has_stoped_ = true;
    log_compress_cond_.signal();
    log_compress_cond_.unlock();
    wait();
    log_compress_cond_.destroy();
  }
}

ObString ObLogCompressor::get_compression_file_name(const ObString &file_name)
{
  ObString compression_file_name;
  ObString suffix_str = ".zst";
  int size = file_name.length();
  if (size && 0 == file_name[size - 1]) {
    size -= 1;
  }
  if (size > 0 && size + 1 + suffix_str.length() <= DEFAULT_FILE_NAME_SIZE) {
    const char *idx = NULL;
    if (size > 4 && NULL != (idx = file_name.reverse_find('.')) && idx != file_name.ptr() &&
        0 == file_name.after(--idx).compare(suffix_str)) {
    } else {
      char *buf = (char *)ob_malloc(size + 1 + suffix_str.length(), ObModIds::OB_LOG_COMPRESSOR);
      if (buf) {
        compression_file_name.assign_buffer(buf, DEFAULT_FILE_NAME_SIZE);
        if (size != compression_file_name.write(file_name.ptr(), size)) {
          ob_free(buf);
        } else {
          compression_file_name.write(".zst\0", 5);
        }
      }
    }
  }
  return compression_file_name;
}

ObCompressor *ObLogCompressor::get_compressor()
{
  return compressor_;
}

int ObLogCompressor::append_log(const ObString &file_name)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_STDERR("The ObLogCompressor has not been inited.\n");
  } else if (file_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char *buf = (char *)ob_malloc(file_name.length() + 1 + sizeof(ObString), ObModIds::OB_LOG_COMPRESSOR);
    if (buf) {
      ObString *file_name_ptr = (ObString *)buf;
      file_name_ptr->assign_buffer(buf + sizeof(ObString), file_name.length() + 1);
      if (file_name.length() != file_name_ptr->write(file_name.ptr(), file_name.length()) ||
          (0 != file_name[file_name.length() - 1] && 1 != file_name_ptr->write("\0", 1))) {
        ob_free(buf);
      } else {
        log_compress_cond_.lock();
        file_list_.push(file_name_ptr);
        log_compress_cond_.signal();
        log_compress_cond_.unlock();
      }
    }
  }
  return ret;
}

int ObLogCompressor::log_compress_block(
    char *dest, size_t dest_size, const char *src, size_t src_size, size_t &return_size)
{
  int ret = OB_SUCCESS;
  int64_t size = -1;
  if (OB_FAIL(((ObCompressor *)compressor_)->compress(src, src_size, dest, dest_size, size))) {
    LOG_STDERR("Failed to compress, err_code=%d.\n", ret);
  } else {
    return_size = size;
  }
  return ret;
}

void ObLogCompressor::log_compress()
{
  int ret = OB_SUCCESS;
  static int sleep_us = 100 * 1000;  // 100ms
  int src_size = DEFAULT_COMPRESSION_BLOCK_SIZE;
  int dest_size = DEFAULT_COMPRESSION_BUFFER_SIZE;
  char *src_buf = (char *)ob_malloc(src_size + dest_size, ObModIds::OB_LOG_COMPRESSOR);
  char *dest_buf = src_buf + src_size;
  if (!src_buf) {
    LOG_STDERR("Failed to ob_malloc.\n");
  } else {
    while (!has_stoped_) {
      ObString *file_name = NULL;
      log_compress_cond_.lock();
      while (0 >= file_list_.get_total() && !has_stoped_) {
        log_compress_cond_.wait(0);
      }
      if (!has_stoped_) {
        ret = file_list_.pop(file_name);
      }
      log_compress_cond_.unlock();

      if (has_stoped_ || NULL == file_name || file_name->empty() || 0 != access(file_name->ptr(), F_OK)) {
      } else {
        ObString compression_file_name = get_compression_file_name(*file_name);
        FILE *input_file = NULL;
        FILE *output_file = NULL;
        if (compression_file_name.empty()) {
          LOG_STDERR("Failed to get_compression_file_name.\n");
        } else if (NULL == (input_file = fopen(file_name->ptr(), "r"))) {
          LOG_STDERR("Failed to fopen, err_code=%d.\n", errno);
        } else if (NULL == (output_file = fopen(compression_file_name.ptr(), "w"))) {
          fclose(input_file);
          LOG_STDERR("Failed to fopen, err_code=%d.\n", errno);
        } else {
          size_t read_size = 0;
          size_t write_size = 0;
          while (OB_SUCC(ret) && !feof(input_file)) {
            if ((read_size = fread(src_buf, 1, src_size, input_file)) > 0) {
              if (OB_FAIL(log_compress_block(dest_buf, dest_size, src_buf, read_size, write_size))) {
                LOG_STDERR("Failed to log_compress_block, err_code=%d.\n", ret);
              } else if (write_size != fwrite(dest_buf, 1, write_size, output_file)) {
                ret = OB_ERR_SYS;
                LOG_STDERR("Failed to fwrite, err_code=%d.\n", errno);
              }
            }
            usleep(sleep_us);
          }
          fclose(input_file);
          fclose(output_file);
          if (0 != access(file_name->ptr(), F_OK) || OB_SUCCESS != ret) {
            unlink(compression_file_name.ptr());
          } else {
            unlink(file_name->ptr());
          }
        }
      }
    }
  }
  if (src_buf) {
    ob_free(src_buf);
  }
}

void ObLogCompressor::run1()
{
  lib::set_thread_name("syslog_compress");
  log_compress();
}

int ObLogCompressor::start()
{
  ThreadPool::start();
  return OB_SUCCESS;
}

void ObLogCompressor::stop()
{
  ThreadPool::stop();
}

void ObLogCompressor::wait()
{
  ThreadPool::wait();
}

}  // namespace common
}  // namespace oceanbase
