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

#include "ob_log_reader_interface.h"

namespace oceanbase {
using namespace common;
namespace clog {
// 1. Used to set the parameters of reading files, the purpose is
// to add parameters in the future without changing the interface
// 2. The caller sets the parameter information
// 3. It is up to user to determine which parameters are needed
//   and whether the parameters are vaild
ObReadParam::ObReadParam()
    : file_id_(OB_INVALID_FILE_ID),
      offset_(OB_INVALID_OFFSET),
      partition_key_(),
      log_id_(OB_INVALID_ID),
      read_len_(0),
      timeout_(OB_TIMEOUT)
{}

ObReadParam::~ObReadParam()
{}

void ObReadParam::reset()
{
  file_id_ = OB_INVALID_FILE_ID;
  offset_ = OB_INVALID_OFFSET;
  partition_key_.reset();
  log_id_ = OB_INVALID_ID;
  read_len_ = 0;
  timeout_ = OB_TIMEOUT;
}

void ObReadParam::shallow_copy(const ObReadParam& new_param)
{
  file_id_ = new_param.file_id_;
  offset_ = new_param.offset_;
  partition_key_ = new_param.partition_key_;
  log_id_ = new_param.log_id_;
  read_len_ = new_param.read_len_;
  timeout_ = new_param.timeout_;
}

ObReadRes::ObReadRes() : buf_(NULL), data_len_(0)
{}

ObReadRes::~ObReadRes()
{
  buf_ = NULL;
  data_len_ = 0;
}

void ObReadRes::reset()
{
  buf_ = NULL;
  data_len_ = 0;
}

int ObILogDirectReader::alloc_buf(const char* label, ObReadBuf& rbuf)
{
  int ret = OB_SUCCESS;
  const int64_t dio_align_size = CLOG_DIO_ALIGN_SIZE;
  const int64_t size = OB_MAX_LOG_BUFFER_SIZE + dio_align_size;

  rbuf.buf_ = static_cast<char*>(ob_malloc_align(dio_align_size, size, label));
  if (OB_UNLIKELY(NULL == (rbuf.buf_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "ob_malloc fail", K(ret), K(dio_align_size), K(size));
  } else {
    rbuf.buf_len_ = size;
  }
  return ret;
}

void ObILogDirectReader::free_buf(ObReadBuf& rbuf)
{
  if (NULL != rbuf.buf_) {
    ob_free_align(rbuf.buf_);
    rbuf.buf_ = NULL;
  }
  rbuf.buf_len_ = 0;
}

}  // end namespace clog
}  // end namespace oceanbase
