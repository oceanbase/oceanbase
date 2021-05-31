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

#include "ob_log_define.h"
#include "lib/utility/serialization.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase {
using namespace common;
namespace clog {
DEFINE_SERIALIZE(ObLogCursorExt)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, file_id_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, offset_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, size_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, padding_flag_) ||
             OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, acc_cksm_) ||
             OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, submit_timestamp_)) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(ERROR, "serialize failed", K(ret), KP(buf), K(buf_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogCursorExt)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int32_t tmp_file_id = 0;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &tmp_file_id) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &offset_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &size_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &padding_flag_) ||
             OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &acc_cksm_) ||
             OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &submit_timestamp_)) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(ERROR, "deserialize failed", K(ret), KP(buf), K(data_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    file_id_ = static_cast<file_id_t>(tmp_file_id);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogCursorExt)
{
  int64_t size = 0;
  size += serialization::encoded_length_i32(file_id_);
  size += serialization::encoded_length_i32(offset_);
  size += serialization::encoded_length_i32(size_);
  size += serialization::encoded_length_i32(padding_flag_);
  size += serialization::encoded_length_i64(acc_cksm_);
  size += serialization::encoded_length_i64(submit_timestamp_);
  return size;
}

void ObPGLogArchiveStatus::reset()
{
  status_ = share::ObLogArchiveStatus::INVALID;

  round_start_ts_ = OB_INVALID_TIMESTAMP;
  round_start_log_id_ = OB_INVALID_ID;
  round_snapshot_version_ = OB_INVALID_TIMESTAMP;
  round_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  round_clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  round_accum_checksum_ = 0;

  archive_incarnation_ = 0;
  log_archive_round_ = 0;
  last_archived_log_id_ = OB_INVALID_ID;
  last_archived_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  last_archived_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
}

bool ObPGLogArchiveStatus::is_valid_for_clog_info()
{
  return (share::ObLogArchiveStatus::INVALID != status_ && archive_incarnation_ > 0 && log_archive_round_ > 0 &&
          OB_INVALID_ID != round_start_log_id_ && 0 != round_start_log_id_ &&
          OB_INVALID_TIMESTAMP != round_snapshot_version_ && OB_INVALID_TIMESTAMP != round_log_submit_ts_ &&
          0 <= round_clog_epoch_id_ && OB_INVALID_ID != last_archived_log_id_ &&
          OB_INVALID_TIMESTAMP != last_archived_log_submit_ts_ &&
          OB_INVALID_TIMESTAMP != last_archived_checkpoint_ts_ && clog_epoch_id_ >= 0);
}

bool ObPGLogArchiveStatus::is_round_start_info_valid()
{
  return (share::ObLogArchiveStatus::INVALID != status_ && OB_INVALID_TIMESTAMP != round_start_ts_ &&
          OB_INVALID_ID != round_start_log_id_ && 0 != round_start_log_id_ &&
          OB_INVALID_TIMESTAMP != round_snapshot_version_ && OB_INVALID_TIMESTAMP != round_log_submit_ts_ &&
          0 <= round_clog_epoch_id_);
}

bool ObPGLogArchiveStatus::operator<(const ObPGLogArchiveStatus& other)
{
  bool b_ret = false;
  if (archive_incarnation_ < other.archive_incarnation_) {
    b_ret = true;
  } else if (archive_incarnation_ > other.archive_incarnation_) {
    b_ret = false;
  } else if (log_archive_round_ < other.log_archive_round_) {
    b_ret = true;
  } else if (log_archive_round_ > other.log_archive_round_) {
    b_ret = false;
  } else if (is_stopped()) {
    b_ret = false;
  } else if (other.is_stopped()) {
    b_ret = true;
  } else if (is_interrupted()) {
    b_ret = false;
  } else if (other.is_interrupted()) {
    b_ret = true;
    // TODO: not consider log_submit_ts temporarily
  } else if (last_archived_log_id_ < other.last_archived_log_id_ ||
             last_archived_checkpoint_ts_ < other.last_archived_checkpoint_ts_) {
    b_ret = true;
  } else {
    b_ret = false;
  }
  return b_ret;
}

OB_SERIALIZE_MEMBER(ObPGLogArchiveStatus, status_, round_start_ts_, round_start_log_id_, round_snapshot_version_,
    round_log_submit_ts_, round_clog_epoch_id_, round_accum_checksum_, archive_incarnation_, log_archive_round_,
    last_archived_log_id_, last_archived_checkpoint_ts_, last_archived_log_submit_ts_, clog_epoch_id_, accum_checksum_);

}  // namespace clog
}  // namespace oceanbase
