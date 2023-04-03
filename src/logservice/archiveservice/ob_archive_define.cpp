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

#include "ob_archive_define.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include <cstdint>

namespace oceanbase
{
using namespace palf;
using namespace share;
namespace archive
{
// =================================== ObArchiveLease ================================= //
//
ObArchiveLease::ObArchiveLease() :
  lease_id_(OB_INVALID_ARCHIVE_LEASE_ID),
  lease_start_ts_(OB_INVALID_TIMESTAMP),
  lease_end_ts_(OB_INVALID_TIMESTAMP)
{}

ObArchiveLease::ObArchiveLease(const int64_t lease_id,
    const int64_t start_ts,
    const int64_t end_ts) :
  lease_id_(lease_id),
  lease_start_ts_(start_ts),
  lease_end_ts_(end_ts)
{}

ObArchiveLease::~ObArchiveLease()
{
  reset();
}

bool ObArchiveLease::is_valid() const
{
  return true;
  //TODO
  //return lease_id_ > 0 && lease_start_ts_ > 0 && lease_end_ts_ > lease_start_ts_;
}

void ObArchiveLease::reset()
{
  lease_id_ = OB_INVALID_ARCHIVE_LEASE_ID;
  lease_start_ts_ = OB_INVALID_TIMESTAMP;
  lease_end_ts_ = OB_INVALID_TIMESTAMP;
}

ObArchiveLease &ObArchiveLease::operator=(const ObArchiveLease &other)
{
  lease_id_ = other.lease_id_;
  lease_start_ts_ = other.lease_start_ts_;
  lease_end_ts_ = other.lease_end_ts_;
  return *this;
}

bool ObArchiveLease::operator==(const ObArchiveLease &other) const
{
  return lease_id_ == other.lease_id_
  && lease_start_ts_ == other.lease_start_ts_
  && lease_end_ts_ == other.lease_end_ts_;
}

// =============================== LogFileTuple  =================================== //
//
LogFileTuple::LogFileTuple() :
  offset_(),
  scn_(),
  piece_()
{}

LogFileTuple::LogFileTuple(const LSN &lsn, const SCN &scn, const ObArchivePiece &piece)
  : offset_(lsn),
    scn_(scn),
    piece_(piece)
{}

LogFileTuple::~LogFileTuple()
{
  reset();
}

bool LogFileTuple::is_valid() const
{
  return offset_.is_valid() && scn_.is_valid() && piece_.is_valid();
}

void LogFileTuple::reset()
{
  offset_.reset();
  scn_.reset();
  piece_.reset();
}

// 同一个piece, lsn和log scn都要小于
// 不同的piece, 必须piece小并且lsn和log scn小于等于
bool LogFileTuple::operator<(const LogFileTuple &other) const
{
  ObArchivePiece piece = piece_;
  return (offset_ < other.offset_&& scn_ < other.scn_)
    || (offset_ <= other.offset_&& scn_ <= other.scn_ && !(other.piece_ > (++piece)));
}

LogFileTuple &LogFileTuple::operator=(const LogFileTuple &other)
{
  offset_ = other.offset_;
  scn_ = other.scn_;
  piece_ = other.piece_;
  return *this;
}

void LogFileTuple::compensate_piece()
{
  piece_.inc();
}

ArchiveKey::ArchiveKey() :
  incarnation_(OB_INVALID_ARCHIVE_INCARNATION_ID),
  dest_id_(OB_INVALID_ARCHIVE_DEST_ID),
  round_(OB_INVALID_ARCHIVE_ROUND_ID)
{}

 ArchiveKey::~ArchiveKey()
{
  reset();
}

ArchiveKey::ArchiveKey(const int64_t incarnation, const int64_t dest_id, const int64_t round) :
  incarnation_(incarnation),
  dest_id_(dest_id),
  round_(round)
{}

void ArchiveKey::reset()
{
  incarnation_ = OB_INVALID_ARCHIVE_INCARNATION_ID;
  dest_id_ = OB_INVALID_ARCHIVE_DEST_ID;
  round_ = OB_INVALID_ARCHIVE_ROUND_ID;
}

bool ArchiveKey::is_valid() const
{
  return incarnation_ > 0 && dest_id_ > 0 && round_ > 0;
}

bool ArchiveKey::operator==(const ArchiveKey &other) const
{
  return incarnation_ == other.incarnation_
    && dest_id_ == other.dest_id_
    && round_ == other.round_;
}

bool ArchiveKey::operator!=(const ArchiveKey &other) const
{
  return !(*this == other);
}

ArchiveKey &ArchiveKey::operator=(const ArchiveKey &other)
{
  incarnation_ = other.incarnation_;
  dest_id_ = other.dest_id_;
  round_ = other.round_;
  return *this;
}

// =========================== ArchiveWorkStation ============================= //
//
ArchiveWorkStation::ArchiveWorkStation() :
  key_(),
  lease_()
{}

ArchiveWorkStation::ArchiveWorkStation(const ArchiveKey &key, const ObArchiveLease &lease) :
  key_(key),
  lease_(lease)
{}

ArchiveWorkStation::~ArchiveWorkStation()
{
  key_.reset();
  lease_.reset();
}

ArchiveWorkStation &ArchiveWorkStation::operator=(const ArchiveWorkStation &other)
{
  key_ = other.key_;
  lease_ = other.lease_;
  return *this;
}

bool ArchiveWorkStation::operator==(const ArchiveWorkStation &other) const
{
  return key_.incarnation_ == other.key_.incarnation_
    && key_.dest_id_ == other.key_.dest_id_
    && key_.round_ == other.key_.round_
    && lease_ == other.lease_;
}

bool ArchiveWorkStation::operator!=(const ArchiveWorkStation &other) const
{
  return !(*this == other);
}

bool ArchiveWorkStation::is_valid() const
{
  return key_.is_valid() && lease_.is_valid();
}

void ArchiveWorkStation::reset()
{
  key_.reset();
  lease_.reset();
}

DEFINE_SERIALIZE(ObArchiveFileHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    ARCHIVE_LOG(WARN, "failed to encode magic_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    ARCHIVE_LOG(WARN, "failed to encode version_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, flag_))) {
    ARCHIVE_LOG(WARN, "failed to encode flag_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, unit_size_))) {
    ARCHIVE_LOG(WARN, "failed to encode unit_size_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, start_lsn_))) {
    ARCHIVE_LOG(WARN, "failed to encode start_lsn_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObArchiveFileHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || 0 > data_len) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    ARCHIVE_LOG(WARN, "failed to decode magic_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    ARCHIVE_LOG(WARN, "failed to decode version_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &flag_))) {
    ARCHIVE_LOG(WARN, "failed to decode flag_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &unit_size_))) {
    ARCHIVE_LOG(WARN, "failed to decode unit_size_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &start_lsn_))) {
    ARCHIVE_LOG(WARN, "failed to decode start_lsn_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &checksum_))) {
    ARCHIVE_LOG(WARN, "failed to decode checksum_", KP(buf), K(data_len), K(pos), K(ret));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObArchiveFileHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(flag_);
  size += serialization::encoded_length_i64(unit_size_);
  size += serialization::encoded_length_i64(start_lsn_);
  size += serialization::encoded_length_i64(checksum_);
  return size;
}

bool ObArchiveFileHeader::is_valid() const
{
  return ARCHIVE_FILE_HEADER_MAGIC == magic_
    && checksum_ == ob_crc64(this, sizeof(*this) - sizeof(checksum_));
}

int ObArchiveFileHeader::generate_header(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    magic_ = ARCHIVE_FILE_HEADER_MAGIC;
    version_ = 1;
    flag_ = 0;
    unit_size_ = DEFAULT_ARCHIVE_UNIT_SIZE;
    start_lsn_ = lsn.val_;
    checksum_ = static_cast<int64_t>(ob_crc64(this, sizeof(*this) - sizeof(checksum_)));
  }
  return ret;
}

DEFINE_SERIALIZE(ObLSMetaFileHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    ARCHIVE_LOG(WARN, "failed to encode magic_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    ARCHIVE_LOG(WARN, "failed to encode version_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, place_holder_))) {
    ARCHIVE_LOG(WARN, "failed to encode place_holder_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(timestamp_.fixed_serialize(buf, buf_len, pos))) {
    ARCHIVE_LOG(WARN, "failed to encode timestamp_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, data_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode data_checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, header_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode header_checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLSMetaFileHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || 0 > data_len) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    ARCHIVE_LOG(WARN, "failed to decode magic_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    ARCHIVE_LOG(WARN, "failed to decode version_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &place_holder_))) {
    ARCHIVE_LOG(WARN, "failed to decode place_holder_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(timestamp_.fixed_deserialize(buf, data_len, pos))) {
    ARCHIVE_LOG(WARN, "failed to decode timestamp_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &data_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to decode data_checksum_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &header_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to decode header_checksum_", KP(buf), K(data_len), K(pos), K(ret));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLSMetaFileHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(place_holder_);
  size += timestamp_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(data_checksum_);
  size += serialization::encoded_length_i64(header_checksum_);
  return size;
}

bool ObLSMetaFileHeader::is_valid() const
{
  return LS_META_FILE_HEADER_MAGIC == magic_
    && header_checksum_ == ob_crc64(this, sizeof(*this) - sizeof(header_checksum_));
}

int ObLSMetaFileHeader::generate_header(const share::SCN &timestamp, const int64_t data_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!timestamp.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    magic_ = LS_META_FILE_HEADER_MAGIC;
    version_ = 1;
    place_holder_ = 0;
    timestamp_ = timestamp;
    data_checksum_ = data_checksum;
    header_checksum_ = static_cast<int64_t>(ob_crc64(this, sizeof(*this) - sizeof(header_checksum_)));
  }
  return ret;
}

const char *reason_str[] = {"UNKONWN",
  "SEND LOG TO ARCHIVE_DEST ERROR",
  "OBSERVER CLOG RECYCLED BEFORE ARCHIVED",
  "INTERNAL ERROR, ARCHIVE LOG NOT CONTINUOUS",
  "LS GC BEFORE ALL CLOG ARCHIVED",
  "MAX"};
const char *ObArchiveInterruptReason::get_str() const
{
  const char *str = NULL;
  if (factor_ >= Factor::UNKONWN && factor_ < Factor::MAX) {
    str = reason_str[static_cast<int64_t>(factor_)];
  }
  return str;
}

void ObArchiveInterruptReason::set(Factor factor, char *lbt_trace, const int ret_code)
{
  factor_ = factor;
  lbt_trace_ = lbt_trace;
  ret_code_ = ret_code;
}

} // namespace archive
} // namespace oceanbase
