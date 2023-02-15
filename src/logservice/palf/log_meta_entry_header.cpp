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

#include "log_meta_entry_header.h"
#include "lib/oblog/ob_log_module.h"      // LOG*
#include "lib/checksum/ob_crc64.h"        // ob_crc64
#include "lib/utility/utility.h"          // FALSE_IT

namespace oceanbase
{
namespace palf
{
using namespace common;
const int64_t LogMetaEntryHeader::HEADER_SER_SIZE = sizeof(LogMetaEntryHeader);

LogMetaEntryHeader::LogMetaEntryHeader() : magic_(0),
                                           version_(0),
                                           data_len_(0),
                                           data_checksum_(0),
                                           header_checksum_(0)
{
}

LogMetaEntryHeader::~LogMetaEntryHeader()
{
  reset();
}

int LogMetaEntryHeader::generate(const char *buf, int32_t data_len)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(buf), K(data_len));
  } else {
    magic_ = MAGIC;
    version_ = LOG_META_ENTRY_HEADER_VERSION;
    data_len_ = data_len;
    data_checksum_ = calc_data_checksum_(buf, data_len);
    header_checksum_ = calc_header_checksum_();
  }
  return ret;
}

bool LogMetaEntryHeader::is_valid() const
{
  return MAGIC == magic_
         && LOG_META_ENTRY_HEADER_VERSION == version_
         && 0 != data_len_
         && 0 != header_checksum_;
}

void LogMetaEntryHeader::reset()
{
  header_checksum_ = 0;
  data_checksum_ = 0;
  data_len_ = 0;
  version_ = 0;
  magic_ = 0;
}

LogMetaEntryHeader& LogMetaEntryHeader::operator=(const LogMetaEntryHeader &header)
{
  magic_ = header.magic_;
  version_ = header.version_;
  data_len_ = header.data_len_;
  data_checksum_ = header.data_checksum_;
  header_checksum_ = header.header_checksum_;
  return *this;
}

bool LogMetaEntryHeader::check_header_integrity() const
{
  return true == is_valid() && true == check_header_checksum_();
}

bool LogMetaEntryHeader::check_integrity(const char *buf, int32_t data_len) const
{
  bool bool_ret = false;
  const int16_t magic = MAGIC;
  if (magic != magic_) {
    bool_ret = false;
    PALF_LOG_RET(ERROR, OB_ERROR, "magic is different", K(magic_), K(magic));
  } else if (false == check_header_checksum_()) {
    PALF_LOG_RET(ERROR, OB_ERROR, "check header checsum failed", K(*this));
  } else if (false == check_data_checksum_(buf, data_len)) {
    PALF_LOG_RET(ERROR, OB_ERROR, "check data checksum failed", K(*buf), K(data_len),
        K(*this));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogMetaEntryHeader::operator==(const LogMetaEntryHeader &header) const
{
  return magic_ == header.magic_
         && version_ == header.version_
         && data_len_ == header.data_len_
         && data_checksum_ == header.data_checksum_
         && header_checksum_ == header.header_checksum_;
}

DEFINE_SERIALIZE(LogMetaEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))
             || OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))
             || OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, data_len_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_checksum_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, header_checksum_))) {
    PALF_LOG(ERROR, "LogMetaEntryHeader serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
    PALF_LOG(TRACE, "LogMetaEntryHeader serialize", KP(buf), K(*this), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(LogMetaEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_))
             || OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_))
             || OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &data_len_))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &data_checksum_))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &header_checksum_))) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(INFO, "LogMetaEntryHeader deserialize failed", K(ret), K(new_pos));
  } else if (false == check_header_integrity()) {
    ret = OB_INVALID_DATA;
  } else {
    PALF_LOG(TRACE, "LogMetaEntryHeader deserialize", K(*this), K(buf), K(buf+pos), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogMetaEntryHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(data_len_);
  size += serialization::encoded_length_i64(data_checksum_);
  size += serialization::encoded_length_i64(header_checksum_);
  return size;
}

int64_t LogMetaEntryHeader::calc_data_checksum_(const char *buf,
                                                  int32_t data_len) const
{
  int64_t data_checksum = 0;
  if (NULL == buf || data_len <= 0) {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(buf), K(data_len));
  } else {
    data_checksum = static_cast<int64_t>(ob_crc64(buf, data_len));
  }
  return data_checksum;
}

int64_t LogMetaEntryHeader::calc_header_checksum_() const
{
  int64_t header_checksum = 0;
  int64_t header_checksum_len = sizeof(*this) - sizeof(header_checksum_);
  header_checksum = static_cast<int64_t>(ob_crc64(this, header_checksum_len));
  return header_checksum;
}

bool LogMetaEntryHeader::check_header_checksum_() const
{
  bool bool_ret = false;
  int64_t header_checksum = calc_header_checksum_();
  if (header_checksum != header_checksum_) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogMetaEntryHeader::check_data_checksum_(const char *buf,
                                                int32_t data_len) const
{
  bool bool_ret = false;
  int64_t crc_checksum = 0;
  if (NULL == buf || 0 >= data_len || data_len != data_len_) {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(buf), K(data_len), K(data_len_));
  } else if (FALSE_IT(crc_checksum = ob_crc64(buf, data_len))) {
  } else if (crc_checksum != data_checksum_) {
    bool_ret = false;
    PALF_LOG_RET(WARN, OB_ERROR, "the data checksum recorded in header is not same \
        with data buf", K(*buf), K(data_len), K(data_len_), K(data_checksum_));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}
} // end namespace oceanbase
} // end namespace palf
