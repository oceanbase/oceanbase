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

#include "log_group_entry.h"
#include "log_entry_header.h"
#include "lib/oblog/ob_log_module.h"      // LOG*
#include "lib/ob_errno.h"                 // ERROR NUMBER
#include "lib/checksum/ob_crc64.h"        // ob_crc64

namespace oceanbase
{
using namespace share;
namespace palf
{
LogGroupEntry::LogGroupEntry() : header_(), buf_(NULL)
{
}

LogGroupEntry::~LogGroupEntry()
{
  reset();
}

int LogGroupEntry::generate(const LogGroupEntryHeader &header,
                            const char *buf)
{
  int ret = OB_SUCCESS;
  if (false == header.is_valid() || NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(header),
        K(buf));
  } else {
    header_ = header;
    buf_ = buf;
  }
  return ret;
}

int LogGroupEntry::shallow_copy(const LogGroupEntry &entry)
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

bool LogGroupEntry::is_valid() const
{
  return true == header_.is_valid()
         && NULL != buf_;
}

void LogGroupEntry::reset()
{
  header_.reset();
  buf_ = NULL;
}

bool LogGroupEntry::check_integrity() const
{
  int64_t unused_data_checksum = -1;
  return check_integrity(unused_data_checksum);
}

bool LogGroupEntry::check_integrity(int64_t &data_checksum) const
{
  int64_t data_len = header_.get_data_len();
  return header_.check_integrity(buf_, data_len, data_checksum);
}


int LogGroupEntry::truncate(const SCN &upper_limit_scn, const int64_t pre_accum_checksum)
{
  return header_.truncate(buf_, get_data_len(), upper_limit_scn, pre_accum_checksum);
}

DEFINE_SERIALIZE(LogGroupEntry)
{
  int ret = OB_SUCCESS;
  int64_t data_len = header_.get_data_len();
  int64_t new_pos = pos;

  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(buf),
        K(buf_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogGroupEntryHeader serialize error",
        K(ret), K(buf_len), K(new_pos));
  } else if (buf_len  - new_pos < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(ERROR, "LogGroupEntry buffer not enough",
        K(ret), K(buf_len), K(data_len), K(new_pos));
  } else {
    MEMCPY(static_cast<char *>(buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
  }
  PALF_LOG(TRACE, "LogGroupEntry serialize", K(ret), K(pos), K(header_));
  return ret;
}

DEFINE_DESERIALIZE(LogGroupEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(buf),
        K(data_len));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(TRACE, "LogGroupEntryHeader deserialize failed", K(ret), K(data_len), K(new_pos));
  } else if (data_len  - new_pos < header_.get_data_len()) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(TRACE, "LogGroupEntry buffer not enough", K(ret), K(data_len),
        K(header_.get_data_len()), K(new_pos));
  } else {
    buf_ = header_.get_data_len() >= 0 ? const_cast<char *>(buf + new_pos) : NULL;
    pos = new_pos + header_.get_data_len();
  }
  PALF_LOG(TRACE, "LogGroupEntry deserialize", K(ret), K(data_len), K(new_pos), K(header_), K(buf_));
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogGroupEntry)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += header_.get_data_len();
  return size;
}

int LogGroupEntry::get_log_min_scn(SCN &min_scn) const
{
  int ret = OB_SUCCESS;
  LogEntryHeader header;
  int64_t pos = 0;
  if (false == header_.is_padding_log() && OB_FAIL(header.deserialize(buf_, header_.get_data_len(), pos))) {
    PALF_LOG(ERROR, "deserialize failed", K(ret), K(header_), K(pos));
  } else {
    min_scn = true == header_.is_padding_log() ? header_.get_max_scn() : header.get_scn();
  }
  return ret;
}
} // end namespace palf
} // end namespace oceanbase
