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

#include "ob_archive_block.h"

#include "common/ob_clock_generator.h"
#include "share/ob_define.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"

namespace oceanbase {

using namespace common;
namespace archive {
ObArchiveBlockMeta::ObArchiveBlockMeta()
{
  reset();
}

ObArchiveBlockMeta::~ObArchiveBlockMeta()
{
  reset();
}

int ObArchiveBlockMeta::generate_block_(const char* buf, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  int64_t meta_len = get_serialize_size();
  int64_t total_len = data_len + meta_len;

  magic_ = META_MAGIC;
  version_ = ARCHIVE_BLOCK_VERSION;
  total_len_ = total_len;
  timestamp_ = ObClockGenerator::getClock();
  data_checksum_ = calc_data_checksum_(buf, data_len);
  meta_checksum_ = calc_meta_checksum_();

  return ret;
}

void ObArchiveBlockMeta::reset()
{
  magic_ = META_MAGIC;
  version_ = ARCHIVE_BLOCK_VERSION;
  total_len_ = 0;
  timestamp_ = OB_INVALID_TIMESTAMP;

  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
  min_log_id_in_file_ = OB_INVALID_ID;
  min_log_ts_in_file_ = OB_INVALID_TIMESTAMP;
  max_log_id_ = OB_INVALID_ID;
  max_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  max_log_submit_ts_ = OB_INVALID_TIMESTAMP;

  input_bytes_ = 0;
  output_bytes_ = 0;

  data_checksum_ = 0;
  meta_checksum_ = 0;
}

bool ObArchiveBlockMeta::check_integrity(const char* data_buf, const int64_t data_len) const
{
  bool bool_ret = true;
  int16_t magic = META_MAGIC;
  if (magic != magic_) {
    bool_ret = false;
    ARCHIVE_LOG(WARN, "magic is different", "magic", magic_, "expected", magic);
  } else if (!check_meta_checksum()) {
    bool_ret = false;
    ARCHIVE_LOG(WARN, "check meta checksum fails", "meta", to_cstring(*this));
  } else if (!check_data_checksum_(data_buf, data_len)) {
    bool_ret = false;
    ARCHIVE_LOG(WARN, "check data checksum fails");
  } else {
    // do nothing
  }
  return bool_ret;
}

bool ObArchiveBlockMeta::check_data_checksum_(const char* buf, const int64_t data_len) const
{
  bool bool_ret = true;
  int64_t ori_data_len = get_data_len();

  // we do not allow null buf here
  if (NULL == buf || data_len <= 0) {
    bool_ret = false;
    ARCHIVE_LOG(WARN, "buf is null", K(data_len), K(*this));
  } else if (data_len == ori_data_len) {
    int64_t crc_check_sum = ob_crc64(buf, data_len);
    if (crc_check_sum == data_checksum_) {
      // checksum pass
    } else {
      bool_ret = false;
      ARCHIVE_LOG(ERROR, "checksum is not consistent", KP(buf), K(data_len), K(crc_check_sum), K(data_checksum_));
    }
  } else {
    bool_ret = false;
    ARCHIVE_LOG(ERROR, "data_len is not the same", KP(buf), K(data_len), K(ori_data_len), K(data_checksum_));
  }
  return bool_ret;
}

bool ObArchiveBlockMeta::check_meta_checksum() const
{
  bool bool_ret = true;
  int64_t calc_checksum = calc_meta_checksum_();
  if (meta_checksum_ != calc_checksum) {
    bool_ret = false;
    ARCHIVE_LOG(WARN, "failed to checkmeta checksum ", K(calc_checksum), K(meta_checksum_));
  }
  return bool_ret;
}

int64_t ObArchiveBlockMeta::calc_data_checksum_(const char* buf, const int64_t data_len) const
{
  int64_t checksum = 0;
  if ((NULL == buf) && (data_len == 0)) {
    // correct situation, checksum = 0 is expected
  } else if ((NULL != buf) && (data_len > 0)) {
    checksum = ob_crc64(buf, data_len);
  } else {
    // wrong situations, checksum = 0 is default
    ARCHIVE_LOG(ERROR, "data checksum", "buf", buf, "data_len", data_len);
  }
  return checksum;
}

int64_t ObArchiveBlockMeta::calc_meta_checksum_() const
{
  int64_t checksum = 0;
  // excludes meta_checksum_, the calculation relies on the structure of the meta
  int64_t calc_checksum_len = sizeof(*this) - sizeof(meta_checksum_);
  checksum = ob_crc64(this, calc_checksum_len);
  return checksum;
}

int ObArchiveBlockMeta::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))) {
    ARCHIVE_LOG(WARN, "failed to serialize magic_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))) {
    ARCHIVE_LOG(WARN, "failed to serialize version_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, total_len_))) {
    ARCHIVE_LOG(WARN, "failed to serialize total_len_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, timestamp_))) {
    ARCHIVE_LOG(WARN, "failed to serialize timestamp_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, clog_epoch_id_))) {
    ARCHIVE_LOG(WARN, "failed to serialize clog_epoch_id_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, accum_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to serialize accum_checksum_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, min_log_id_in_file_))) {
    ARCHIVE_LOG(WARN, "failed to serialize min_log_id_in_file_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, min_log_ts_in_file_))) {
    ARCHIVE_LOG(WARN, "failed to serialize min_log_ts_in_file_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, max_log_id_))) {
    ARCHIVE_LOG(WARN, "failed to serialize max_log_id_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, max_checkpoint_ts_))) {
    ARCHIVE_LOG(WARN, "failed to serialize max_checkpoint_ts_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, max_log_submit_ts_))) {
    ARCHIVE_LOG(WARN, "failed to serialize max_log_submit_ts_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, input_bytes_))) {
    ARCHIVE_LOG(WARN, "failed to serialize input_bytes_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, output_bytes_))) {
    ARCHIVE_LOG(WARN, "failed to serialize output_bytes_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to serialize data_checksum_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, meta_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to serialize meta_checksum_", KR(ret), K(pos), K(buf_len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObArchiveBlockMeta::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(TRACE, "invalid argument", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize magic_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize version_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &total_len_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize total_len_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &timestamp_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize timestamp_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &clog_epoch_id_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize clog_epoch_id_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &accum_checksum_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize accum_checksum_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(
                 serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&min_log_id_in_file_)))) {
    ARCHIVE_LOG(WARN, "failed to deserialize min_log_id_in_file_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &min_log_ts_in_file_))) {
    ARCHIVE_LOG(WARN, "failed to deserialize min_log_ts_in_file_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&max_log_id_)))) {
    ARCHIVE_LOG(WARN, "failed to deserialize max_log_id_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &max_checkpoint_ts_))) {
    ARCHIVE_LOG(WARN, "failed to deserialize max_checkpoint_ts_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &max_log_submit_ts_))) {
    ARCHIVE_LOG(WARN, "failed to deserialize max_log_submit_ts_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &input_bytes_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize input_bytes_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &output_bytes_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize output_bytes_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &data_checksum_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize data_checksum_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &meta_checksum_))) {
    ARCHIVE_LOG(TRACE, "failed to deserialize meta_checksum_", KR(ret), K(pos), K(data_len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObArchiveBlockMeta::get_serialize_size(void) const
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(total_len_);
  size += serialization::encoded_length_i64(timestamp_);
  size += serialization::encoded_length_i64(clog_epoch_id_);
  size += serialization::encoded_length_i64(accum_checksum_);
  size += serialization::encoded_length_i64(min_log_id_in_file_);
  size += serialization::encoded_length_i64(min_log_ts_in_file_);
  size += serialization::encoded_length_i64(max_log_id_);
  size += serialization::encoded_length_i64(max_checkpoint_ts_);
  size += serialization::encoded_length_i64(max_log_submit_ts_);
  size += serialization::encoded_length_i64(input_bytes_);
  size += serialization::encoded_length_i64(output_bytes_);
  size += serialization::encoded_length_i64(data_checksum_);
  size += serialization::encoded_length_i64(meta_checksum_);
  return size;
}

int ObArchiveBlockMeta::build_serialized_block(
    char* buf, const int64_t buf_len, const char* data_buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || (buf_len <= 0) || OB_ISNULL(data_buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(ret), KP(buf), K(buf_len), KP(data_buf), K(data_len));
  } else if (OB_FAIL(generate_block_(data_buf, data_len))) {
    ARCHIVE_LOG(ERROR, "generate block failed.", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialize(buf, buf_len, pos))) {
    ARCHIVE_LOG(ERROR, "serialize block_meta_ failed.", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

}  // namespace archive
}  // end namespace oceanbase
