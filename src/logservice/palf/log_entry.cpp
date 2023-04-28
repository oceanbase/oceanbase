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

#include "log_entry.h"
#include "lib/oblog/ob_log_module.h"        // LOG*
#include "lib/ob_errno.h"                   // ERROR NUMBER
#include "lib/checksum/ob_crc64.h"          // ob_crc64
namespace oceanbase
{
namespace palf
{
using namespace common;
LogEntry::LogEntry() : header_(), buf_(NULL)
{
}

LogEntry::~LogEntry()
{
  reset();
}

int LogEntry::shallow_copy(const LogEntry &input)
{
  int ret = OB_SUCCESS;
  if (false == input.is_valid()) {
  } else {
  header_ = input.header_;
  buf_ = input.buf_;
  }
  return ret;
}

bool LogEntry::is_valid() const
{
  return true == header_.is_valid()
         && NULL != buf_;
}

void LogEntry::reset()
{
  header_.reset();
  buf_ = NULL;
}

bool LogEntry::check_integrity() const
{
  int64_t data_len = header_.get_data_len();
  return header_.check_integrity(buf_, data_len);
}

DEFINE_SERIALIZE(LogEntry)
{
  int ret = OB_SUCCESS;
  int64_t data_len = header_.get_data_len();
  int64_t new_pos = pos;

  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(buf),
        K(buf_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(WARN, "LogEntryHeader serialize failed",
        K(ret), K(buf_len), K(new_pos));
  } else if (buf_len  - new_pos < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(WARN, "LogEntry buffer not enough",
        K(ret), K(buf_len), K(data_len), K(new_pos));
  } else {
    MEMCPY(static_cast<char *>(buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(buf),
        K(data_len));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(WARN, "LogEntryHeader deserialize error", K(ret), K(data_len), K(new_pos));
  } else if (data_len  - new_pos < header_.get_data_len()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf_ = header_.get_data_len() > 0 ? const_cast<char *>(buf + new_pos) : NULL;
    pos = new_pos + header_.get_data_len();
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogEntry)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += header_.get_data_len();
  return size;
}
} // end namespace palf
} // end namespace oceanbase
