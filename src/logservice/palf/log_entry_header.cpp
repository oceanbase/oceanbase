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

#include "log_entry_header.h"
#include "lib/checksum/ob_parity_check.h"   // parity_check
#include "share/ob_cluster_version.h"       // GET_MIN_DATA_VERSION
#include "share/rc/ob_tenant_base.h"        // MTL_ID
#include "logservice/ob_log_base_header.h"  // ObLogBaseHeader

namespace oceanbase
{
using namespace share;
namespace palf
{

const int64_t LogEntryHeader::HEADER_SER_SIZE = sizeof(LogEntryHeader);
const int64_t LogEntryHeader::PADDING_LOG_ENTRY_SIZE = sizeof(LogEntryHeader) + sizeof(logservice::ObLogBaseHeader);
const int16_t LogEntryHeader::MAGIC = 0x4C48;

const int16_t LogEntryHeader::LOG_ENTRY_HEADER_VERSION = 1;
const int64_t LogEntryHeader::PADDING_TYPE_MASK = 1 << 1;

const int16_t LogEntryHeader::LOG_ENTRY_HEADER_VERSION2 = 2;
const int64_t LogEntryHeader::PADDING_TYPE_MASK_VERSION2 = 1ll << 62;
const int64_t LogEntryHeader::CRC16_MASK = 0xffff;
const int64_t LogEntryHeader::PARITY_MASK = 0x01;

LogEntryHeader::LogEntryHeader()
  : magic_(0),
    version_(0),
    log_size_(0),
    scn_(),
    data_checksum_(0),
    flag_(0)
{}

LogEntryHeader::~LogEntryHeader()
{
  reset();
}

LogEntryHeader& LogEntryHeader::operator=(const LogEntryHeader &header)
{
  magic_ = header.magic_;
  version_ = header.version_;
  log_size_ = header.log_size_;
  scn_ = header.scn_;
  data_checksum_ = header.data_checksum_;
  flag_ = header.flag_;
  return *this;
}

void LogEntryHeader::reset()
{
  magic_ = 0;
  version_ = 0;
  data_checksum_ = 0;
  log_size_ = -1;
  scn_.reset();
  flag_ = 0;
}

bool LogEntryHeader::is_valid() const
{
  return (magic_ == LogEntryHeader::MAGIC && log_size_ > 0 && scn_.is_valid());
}

uint16_t LogEntryHeader::calculate_header_checksum_() const
{
  uint16_t checksum = 0;
  if (LOG_ENTRY_HEADER_VERSION == version_) {
    bool bool_ret = parity_check(reinterpret_cast<const uint16_t &>(magic_));
    bool_ret ^= parity_check(reinterpret_cast<const uint16_t &>(version_));
    bool_ret ^= parity_check(reinterpret_cast<const uint32_t &>(log_size_));
    bool_ret ^= parity_check((scn_.get_val_for_logservice()));
    bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(data_checksum_));
    int64_t tmp_flag = (flag_ & ~PARITY_MASK);
    bool_ret ^= parity_check(reinterpret_cast<const uint64_t &>(tmp_flag));
    checksum = bool_ret ? 1 : 0;
  } else if (LOG_ENTRY_HEADER_VERSION2 == version_) {
    // NB: To avoid dealing with endianness issue, make the last two bytes of flag_ with zero.
    int64_t ori_flag = flag_;
    this->flag_ = (ori_flag & ~CRC16_MASK);
    checksum = xxhash_16(checksum, reinterpret_cast<const uint8_t*>(this), sizeof(LogEntryHeader));
    this->flag_ = ori_flag;
  } else {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected version", KPC(this));
  }
  PALF_LOG(TRACE, "update_header_checksum_", KPC(this), K(checksum));
  return checksum;
}

void LogEntryHeader::update_header_checksum_()
{
  reset_header_checksum_();
  flag_ = (flag_ | calculate_header_checksum_());
}

int LogEntryHeader::generate_header(const char *log_data,
                                    const int64_t data_len,
                                    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (NULL == log_data || data_len <= 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    magic_ = LogEntryHeader::MAGIC;
    version_ = get_version_();
    log_size_ = data_len;
    scn_ = scn;
    data_checksum_ = common::ob_crc64(log_data, data_len);
    // update header checksum after all member vars assigned
    (void) update_header_checksum_();
  }
  return ret;
}

bool LogEntryHeader::check_header_checksum_() const
{
  bool bool_ret = false;
  const uint16_t header_checksum = calculate_header_checksum_();
  if (LOG_ENTRY_HEADER_VERSION2 != version_ && LOG_ENTRY_HEADER_VERSION != version_) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "check_header_checksum_ failed, invalid version_", KPC(this));
  } else {
    int64_t mask = get_header_checksum_mask_();
    const uint16_t saved_header_checksum = (flag_ & mask);
    bool_ret = (header_checksum == saved_header_checksum);
    if (false == bool_ret) {
      PALF_LOG_RET(ERROR, OB_INVALID_DATA, "check_header_checksum_ failed", K(saved_header_checksum), K(header_checksum), KPC(this));
    }
  }
  return bool_ret;
}

bool LogEntryHeader::is_padding_log_() const
{
  return (flag_ & get_padding_mask_()) > 0;
}

// static member function
// the format of out_buf
// | LogEntryHeader | ObLogBaseHeader |
int LogEntryHeader::generate_padding_log_buf(const int64_t padding_data_len,
                                             const share::SCN &scn,
                                             char *out_buf,
                                             const int64_t padding_valid_data_len)
{
  int ret = OB_SUCCESS;
  LogEntryHeader header;
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::PADDING_LOG_BASE_TYPE,
                                          logservice::ObReplayBarrierType::NO_NEED_BARRIER,
                                          0);
  const int64_t base_header_len = base_header.get_serialize_size();
  const int64_t header_len = header.get_serialize_size();
  const int64_t serialize_len = base_header_len + header_len;
  int64_t serialize_header_pos = 0;
  int64_t serialize_base_header_pos = serialize_header_pos + header_len;
  if (padding_data_len <= 0
      || !scn.is_valid()
      || NULL == out_buf
      || padding_valid_data_len < serialize_len
      || padding_data_len < padding_valid_data_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(padding_data_len), K(scn), KP(out_buf), K(padding_valid_data_len));
  } else if(OB_FAIL(base_header.serialize(out_buf, padding_valid_data_len, serialize_base_header_pos))) {
    PALF_LOG(WARN, "serailize ObLogBaseHeader failed", K(padding_data_len), KP(out_buf), K(padding_valid_data_len),
        K(serialize_base_header_pos));
  } else if (FALSE_IT(serialize_base_header_pos = serialize_header_pos + header_len)) {
  } else if (OB_FAIL(header.generate_padding_header_(out_buf+serialize_base_header_pos,
                                                     base_header_len,
                                                     padding_data_len,
                                                     scn))) {
    PALF_LOG(WARN, "generaet LogEntryHeader failed", K(padding_data_len), K(scn), KP(out_buf), K(padding_valid_data_len));
  } else if (OB_FAIL(header.serialize(out_buf, header_len, serialize_header_pos))) {
    PALF_LOG(WARN, "serialize LogEntryHeader failed", K(padding_data_len), K(scn), KP(out_buf), K(padding_valid_data_len));
  } else {
    PALF_LOG(INFO, "generate_padding_log_buf success", K(header), K(padding_data_len), K(scn), KP(out_buf), K(padding_valid_data_len));
  }
  return ret;
}

bool LogEntryHeader::check_header_integrity() const
{
  return true == is_valid() && true == check_header_checksum_();
}

bool LogEntryHeader::check_integrity(const char *buf, const int64_t data_len) const
{
  bool bool_ret = false;
  // for padding log, only check integrity of ObLogBaseHeader
  int64_t valid_data_len = is_padding_log_() ? sizeof(logservice::ObLogBaseHeader) : data_len;
  if (NULL == buf || data_len <= 0) {
    PALF_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid arguments", KP(buf), K(data_len));
  } else if (LogEntryHeader::MAGIC != magic_) {
    bool_ret = false;
    PALF_LOG_RET(WARN, OB_ERROR, "magic is different", K_(magic));
  } else if (false == check_header_checksum_()) {
    PALF_LOG_RET(WARN, OB_ERROR, "check header checsum failed", K(*this));
  } else {
    const int64_t tmp_data_checksum = common::ob_crc64(buf, valid_data_len);
    if (data_checksum_ == tmp_data_checksum) {
      bool_ret = true;
    } else {
      bool_ret = false;
      PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "data checksum mismatch", K_(data_checksum), K(tmp_data_checksum), K(data_len),
          K(valid_data_len), KPC(this));
    }
  }
  return bool_ret;
}

int LogEntryHeader::generate_padding_header_(const char *log_data,
                                             const int64_t base_header_len,
                                             const int64_t padding_data_len,
                                             const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (NULL == log_data || base_header_len <= 0 || padding_data_len <= 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    magic_ = LogEntryHeader::MAGIC;
    version_ = get_version_();
    log_size_ = padding_data_len;
    scn_ = scn;
    data_checksum_ = common::ob_crc64(log_data, base_header_len);
    flag_ = (flag_ | get_padding_mask_());
    // update header checksum after all member vars assigned
    (void) update_header_checksum_();
    PALF_LOG(INFO, "generate_padding_header_ success", KPC(this), K(log_data), K(base_header_len), K(padding_data_len));
  }
  return ret;
}

int16_t LogEntryHeader::get_version_() const
{
  int16_t version = LOG_ENTRY_HEADER_VERSION;
  int ret = OB_SUCCESS;
  uint64_t min_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), min_data_version))) {
    PALF_LOG(WARN, "GET_MIN_DATA_VERSION failed", K(ret));
  } else if (min_data_version >= DATA_VERSION_4_3_3_0) {
    version = LOG_ENTRY_HEADER_VERSION2;
  }
  return version;
}

int64_t LogEntryHeader::get_padding_mask_() const
{
  int64_t padding_mask = 0;
  if (LOG_ENTRY_HEADER_VERSION2 == version_) {
    padding_mask = PADDING_TYPE_MASK_VERSION2;
  } else if (LOG_ENTRY_HEADER_VERSION == version_) {
    padding_mask = PADDING_TYPE_MASK;
  } else {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected version!!!", KPC(this));
  }
  return padding_mask;
}

int64_t LogEntryHeader::get_header_checksum_mask_() const
{
  int64_t header_checksum_mask = 0;
  if (LOG_ENTRY_HEADER_VERSION2 == version_) {
    header_checksum_mask = CRC16_MASK;
  } else if (LOG_ENTRY_HEADER_VERSION == version_) {
    header_checksum_mask = PARITY_MASK;
  } else {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected version!!!", KPC(this));
  }
  return header_checksum_mask;
}

void LogEntryHeader::reset_header_checksum_()
{
  if (LOG_ENTRY_HEADER_VERSION2 == version_) {
    flag_ &= (~CRC16_MASK);
  } else if (LOG_ENTRY_HEADER_VERSION == version_) {
    flag_ &= (~PARITY_MASK);
  } else {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected version_", KPC(this));
  }
}

DEFINE_SERIALIZE(LogEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))
             || OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))
             || OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, log_size_))
             || OB_FAIL(scn_.fixed_serialize(buf, buf_len, new_pos))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_checksum_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, flag_))) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_)))
              || OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_))
              || OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &log_size_))
              || OB_FAIL(scn_.fixed_deserialize(buf, data_len, new_pos))
              || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &data_checksum_))
              || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &flag_))) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (false == check_header_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(WARN, "invalid log_entry_header", KR(ret), KPC(this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogEntryHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(log_size_);
  size += scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(data_checksum_);
  size += serialization::encoded_length_i64(flag_);
  return size;
}

int16_t xxhash_16(int16_t checksum, const uint8_t* data, const int64_t data_len)
{
  return XXH64(data, data_len, checksum) & 0xffff;
}
} // namespace palf
} // namespace oceanbase
