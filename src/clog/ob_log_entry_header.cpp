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

#include "ob_log_entry_header.h"

#include "lib/checksum/ob_crc64.h"
#include "share/ob_define.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogEntryHeader::ObLogEntryHeader()
    : magic_(0),
      version_(0),
      log_type_(OB_LOG_UNKNOWN),
      partition_key_(),
      log_id_(OB_INVALID_ID),
      data_len_(0),
      generation_timestamp_(0),
      epoch_id_(0),
      proposal_id_(),
      submit_timestamp_(0),
      data_checksum_(0),
      freeze_version_(),
      header_checksum_(0)
{}

ObLogEntryHeader::~ObLogEntryHeader()
{
  magic_ = 0;
  version_ = 0;
  log_type_ = OB_LOG_UNKNOWN;
  partition_key_.reset();
  log_id_ = OB_INVALID_ID;
  data_len_ = 0;
  generation_timestamp_ = 0;
  epoch_id_ = 0;
  submit_timestamp_ = 0;
  data_checksum_ = 0;
  freeze_version_ = ObVersion();
  header_checksum_ = 0;
}

int ObLogEntryHeader::generate_header(const ObLogType log_type, const common::ObPartitionKey& partition_key,
    const uint64_t log_id, const char* buf, const int64_t data_len, const int64_t generation_timestamp,
    const int64_t epoch_id, const ObProposalID proposal_id, const int64_t submit_timestamp,
    const ObVersion freeze_version, const bool is_trans_log)
{
  int ret = OB_SUCCESS;
  if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else {
    magic_ = ENTRY_MAGIC;
    version_ = OB_LOG_VERSION;
    log_id_ = log_id;
    log_type_ = log_type;  // change ObLogType->int32_t
    partition_key_ = partition_key;
    data_len_ = data_len;
    generation_timestamp_ = generation_timestamp;
    epoch_id_ = epoch_id;
    proposal_id_ = proposal_id;
    submit_timestamp_ = submit_timestamp;
    freeze_version_ = freeze_version;
    if (is_trans_log) {
      set_is_trans_log_flag();
    }
    update_data_checksum(buf, data_len);
    update_header_checksum();
  }
  return ret;
}

void ObLogEntryHeader::reset()
{
  magic_ = 0;
  version_ = 0;
  log_type_ = OB_LOG_UNKNOWN;
  partition_key_.reset();
  log_id_ = OB_INVALID_ID;
  data_len_ = 0;
  generation_timestamp_ = 0;
  epoch_id_ = 0;
  proposal_id_.reset();
  submit_timestamp_ = 0;
  data_checksum_ = 0;
  freeze_version_ = ObVersion();
  header_checksum_ = 0;
}

bool ObLogEntryHeader::operator==(const ObLogEntryHeader& header) const
{
  return (magic_ == header.magic_) && (version_ == header.version_) && (log_type_ == header.log_type_) &&
         (partition_key_ == header.partition_key_) && (log_id_ == header.log_id_) && (data_len_ == header.data_len_) &&
         (generation_timestamp_ == header.generation_timestamp_) && (epoch_id_ == header.epoch_id_) &&
         (proposal_id_ == header.proposal_id_) && (submit_timestamp_ == header.submit_timestamp_) &&
         (data_checksum_ == header.data_checksum_) && (freeze_version_ == header.freeze_version_) &&
         (header_checksum_ == header.header_checksum_);
}

// copy to me
int ObLogEntryHeader::shallow_copy(const ObLogEntryHeader& header)
{
  int ret = OB_SUCCESS;
  partition_key_ = header.partition_key_;
  magic_ = header.magic_;
  version_ = header.version_;
  log_type_ = header.log_type_;
  log_id_ = header.log_id_;
  data_len_ = header.data_len_;
  generation_timestamp_ = header.generation_timestamp_;
  epoch_id_ = header.epoch_id_;
  proposal_id_ = header.proposal_id_;
  submit_timestamp_ = header.submit_timestamp_;
  data_checksum_ = header.data_checksum_;
  freeze_version_ = header.freeze_version_;
  header_checksum_ = header.header_checksum_;
  return ret;
}

bool ObLogEntryHeader::check_data_checksum(const char* buf, const int64_t data_len) const
{
  bool bool_ret = true;
  if ((NULL == buf) && (0 == data_len) && (data_len_ == data_len) && (0 == data_checksum_)) {
    // the entry with no data, do nothing
  } else if (OB_LIKELY((data_len > 0) && (NULL != buf) && (data_len_ == data_len))) {
    int64_t crc_checksum = ob_crc64(buf, data_len);
    if (crc_checksum == data_checksum_) {
      // checksum are the same, do nothing
    } else {
      bool_ret = false;
      CLOG_LOG(WARN,
          "checksum different",
          "data",
          buf,
          "data_len",
          data_len,
          "checksum",
          crc_checksum,
          "expected",
          data_checksum_);
      hex_dump(buf, data_len_, true, OB_LOG_LEVEL_ERROR);
    }
  } else {
    bool_ret = false;
    CLOG_LOG(WARN, "checksum different", "data", buf, "data_len", data_len, "checksum", data_checksum_);
  }
  return bool_ret;
}

bool ObLogEntryHeader::check_header_checksum() const
{
  bool bool_ret = true;
  int64_t calc_checksum = calc_header_checksum();
  if (header_checksum_ != calc_checksum) {
    bool_ret = false;
    CLOG_LOG(WARN,
        "header checksum fail",
        "calc_header_checksum",
        calc_checksum,
        "expected_checksum",
        header_checksum_,
        "submit_timestamp",
        get_submit_timestamp());
  }
  return bool_ret;
}

bool ObLogEntryHeader::check_integrity(const char* buf, const int64_t data_len) const
{
  bool bool_ret = true;
  int16_t magic = ENTRY_MAGIC;
  if (magic != magic_) {
    bool_ret = false;
    CLOG_LOG(WARN, "magic is different", "magic", magic_, "expected", magic);
  } else if (!check_header_checksum()) {
    bool_ret = false;
    CLOG_LOG(WARN, "check header checksum fails", "header", to_cstring(*this));
  } else if (!check_data_checksum(buf, data_len)) {
    bool_ret = false;
    CLOG_LOG(WARN, "check data checksum fails", K(buf), K(data_len), "header", to_cstring(*this));
  } else {
  }
  return bool_ret;
}

int ObLogEntryHeader::update_nop_or_truncate_submit_timestamp(const int64_t submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_LOG_NOP != static_cast<ObLogType>(log_type_) && OB_LOG_TRUNCATE != static_cast<ObLogType>(log_type_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "not nop or truncate log", K(ret), K_(log_type), K_(log_id), K_(partition_key));
  } else {
    submit_timestamp_ = submit_timestamp;
    CLOG_LOG(INFO, "nop/truncate log timestamp updated", K_(partition_key), K_(log_id), K(submit_timestamp));
  }
  return ret;
}

int ObLogEntryHeader::update_proposal_id(const common::ObProposalID& new_proposal_id)
{
  int ret = OB_SUCCESS;
  if (!new_proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(ret), K(new_proposal_id), K_(partition_key));
  } else if (proposal_id_ == new_proposal_id) {
    // no need update
  } else {
    proposal_id_ = new_proposal_id;
    update_header_checksum();
  }
  return ret;
}

int64_t ObLogEntryHeader::calc_header_checksum() const
{
  int64_t checksum = 0;
  // excludes header_checksum_
  int64_t calc_checksum_len = sizeof(*this) - sizeof(header_checksum_);
  checksum = ob_crc64(this, calc_checksum_len);
  return checksum;
}

int64_t ObLogEntryHeader::calc_data_checksum(const char* buf, const int64_t data_len) const
{
  int64_t checksum = 0;
  if ((NULL == buf) && (data_len == 0)) {
    // correct situation, checksum = 0 is expected
  } else if ((NULL != buf) && (data_len > 0)) {
    checksum = ob_crc64(buf, data_len);
  } else {
    // wrong situations, checksum = 0 is default
    CLOG_LOG(ERROR, "data checksum", KP(buf), K(data_len));
  }
  return checksum;
}

DEFINE_SERIALIZE(ObLogEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_)) ||
             OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_)) ||
             OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, log_type_)) ||
             OB_FAIL(partition_key_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, log_id_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_len_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, generation_timestamp_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, epoch_id_)) ||
             OB_FAIL(proposal_id_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, submit_timestamp_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_checksum_)) ||
             OB_FAIL(freeze_version_.fixed_length_encode(buf, buf_len, new_pos)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, header_checksum_))) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(TRACE, "ObLogEntryHeader serialize error", K(buf), K(buf_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_)) ||
             OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_)) ||
             OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &log_type_)) ||
             OB_FAIL(partition_key_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&log_id_))) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &data_len_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &generation_timestamp_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &epoch_id_)) ||
             OB_FAIL(proposal_id_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &submit_timestamp_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &data_checksum_)) ||
             OB_FAIL(freeze_version_.fixed_length_decode(buf, data_len, new_pos)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &header_checksum_))) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogEntryHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(log_type_);
  size += partition_key_.get_serialize_size();
  size += serialization::encoded_length_i64(log_id_);
  size += serialization::encoded_length_i64(data_len_);
  size += serialization::encoded_length_i64(generation_timestamp_);
  size += serialization::encoded_length_i64(epoch_id_);
  size += proposal_id_.get_serialize_size();
  size += serialization::encoded_length_i64(submit_timestamp_);
  size += serialization::encoded_length_i64(data_checksum_);
  size += freeze_version_.get_fixed_length_encoded_size();
  size += serialization::encoded_length_i64(header_checksum_);
  return size;
}

int64_t ObLogEntryHeader::get_submit_ts_serialize_pos() const
{
  return serialization::encoded_length_i16(magic_) + serialization::encoded_length_i16(version_) +
         serialization::encoded_length_i32(log_type_) + partition_key_.get_serialize_size() +
         serialization::encoded_length_i64(log_id_) + serialization::encoded_length_i64(data_len_) +
         serialization::encoded_length_i64(generation_timestamp_) + serialization::encoded_length_i64(epoch_id_) +
         proposal_id_.get_serialize_size();
}

int ObLogEntryHeader::serialize_submit_timestamp(char* buf, const int64_t buf_len, int64_t& pos)
{
  return serialization::encode_i64(buf, buf_len, pos, submit_timestamp_);
}

}  // end namespace clog
}  // end namespace oceanbase
