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

#include "log_meta_entry.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"        // LOG*
#include "lib/checksum/ob_crc64.h"        // ob_crc64

namespace oceanbase
{
namespace palf
{
using namespace common;
LogMetaEntry::LogMetaEntry() : header_(),
                                   buf_(NULL)
{
}

LogMetaEntry::~LogMetaEntry()
{
  reset();
}

int LogMetaEntry::generate(const LogMetaEntryHeader &header,
                           const char *buf)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || false == header.is_valid()) {
  ret = OB_INVALID_ARGUMENT;
  } else {
    header_ = header;
    buf_ = buf;
  }
  return ret;
}

bool LogMetaEntry::is_valid() const
{
  return true == header_.is_valid()
         && NULL != buf_;
}

void LogMetaEntry::reset()
{
  header_.reset();
  buf_ = NULL;
}

int LogMetaEntry::shallow_copy(const LogMetaEntry &entry)
{
  int ret = OB_SUCCESS;
  if (false == entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
  header_ = entry.header_;
  buf_ = entry.buf_;
  }
  return ret;
}

const LogMetaEntryHeader& LogMetaEntry::get_log_meta_entry_header() const
{
  return header_;
}

const char *LogMetaEntry::get_buf() const
{
  return buf_;
}

bool LogMetaEntry::check_integrity() const
{
  int32_t data_len = header_.get_data_len();
  return header_.check_integrity(buf_, data_len);
}

DEFINE_SERIALIZE(LogMetaEntry)
{
  int ret = OB_SUCCESS;
  int64_t data_len = header_.get_data_len();
  int64_t new_pos = pos;
  if (OB_UNLIKELY((NULL == buf) || (buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(TRACE, "header serialize error", K(ret), K(buf_len), K(new_pos));
  } else if ((buf_len - new_pos) < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(TRACE, "buf not enough", K(buf_len), K(data_len), K(new_pos));
  } else if (0 == data_len) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    MEMCPY(static_cast<char *>(buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
    PALF_LOG(TRACE, "LogMetaEntry serialize", K(ret), K(buf_), K(buf), K(data_len), K(new_pos), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(LogMetaEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(TRACE, "header deserialize error", K(ret), K(data_len), K(new_pos));
  } else if (data_len - new_pos < header_.get_data_len()) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(TRACE, "buffer not enough", K(ret), K(data_len),
        K(data_len), K(new_pos));
  } else if (header_.get_data_len() <= 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    buf_ = const_cast<char *>(buf + new_pos);
    pos = new_pos + header_.get_data_len();
    PALF_LOG(TRACE, "LogMetaEntry deserialize", KP(this), K(pos), K(new_pos), K(buf), K_(buf));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogMetaEntry)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += header_.get_data_len();
  return size;
}
} // end namespace palf
} // end namespace oceanbase
