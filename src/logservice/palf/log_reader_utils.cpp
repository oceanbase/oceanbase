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

#include "lib/ob_errno.h"
#include "lib/utility/ob_utility.h"
#include "share/rc/ob_tenant_base.h"
#include "log_define.h"
#include "log_reader_utils.h"

namespace oceanbase
{
using namespace share;
namespace palf
{

ReadBuf::ReadBuf() : buf_(NULL), buf_len_(0)
{
}

ReadBuf::ReadBuf(char *buf, const int64_t buf_len) : buf_(buf), buf_len_(buf_len)
{
}

ReadBuf::ReadBuf(const ReadBuf &rhs)
{
  *this = rhs;
}

ReadBuf::~ReadBuf()
{
  reset();
}

void ReadBuf::reset()
{
  buf_ = NULL;
  buf_len_ = 0;
}

bool ReadBuf::operator==(const ReadBuf &rhs) const
{
  return this->buf_ == rhs.buf_ && this->buf_len_ == rhs.buf_len_;
}

bool ReadBuf::operator!=(const ReadBuf &rhs) const
{
  return !operator==(rhs);
}

ReadBuf &ReadBuf::operator=(const ReadBuf &rhs)
{
  buf_ = rhs.buf_;
  buf_len_ = rhs.buf_len_;
  return *this;
}

bool ReadBuf::is_valid() const
{
  return NULL != buf_ && 0 < buf_len_;
}

ReadBufGuard::ReadBufGuard(const char *label, const int64_t buf_len) : read_buf_()
{
  (void)alloc_read_buf(label, buf_len, read_buf_);
}

ReadBufGuard::~ReadBufGuard()
{
  free_read_buf(read_buf_);
}

int alloc_read_buf(const char *label, const int64_t buf_len, ReadBuf &read_buf)
{
  int ret = OB_SUCCESS;
  const int64_t dio_align_size = LOG_DIO_ALIGN_SIZE;
  const int64_t size = upper_align(buf_len, dio_align_size) + dio_align_size;
  if (NULL == (read_buf.buf_ = static_cast<char *>(
      mtl_malloc_align(dio_align_size, size, label)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    read_buf.buf_len_ = size;
  }
  return ret;
}

void free_read_buf(ReadBuf &read_buf)
{
  if (true == read_buf.is_valid()) {
    mtl_free_align(read_buf.buf_);
    read_buf.buf_ = NULL;
    read_buf.buf_len_ = 0;
  }
}
} // end of logservice
} // end of oceanbase
