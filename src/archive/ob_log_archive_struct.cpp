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

#include "ob_log_archive_struct.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "ob_archive_util.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::share;

namespace oceanbase {
namespace archive {

////================ start of structs for backup============//
//=======================start of ObPGArchiveCLogTask========================//

void ObPGArchiveCLogTask::reset()
{
  epoch_id_ = 0;
  need_update_log_ts_ = false;
  round_start_log_id_ = OB_INVALID_ID;
  round_start_ts_ = OB_INVALID_TIMESTAMP;
  round_snapshot_version_ = OB_INVALID_TIMESTAMP;
  checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  log_submit_ts_ = OB_INVALID_TIMESTAMP;
  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
  processed_log_count_ = 0;
  incarnation_ = 0;
  log_archive_round_ = 0;
  task_type_ = OB_ARCHIVE_TASK_TYPE_INVALID;
  compressor_type_ = INVALID_COMPRESSOR;
  pg_key_.reset();
  release_read_buf();
  next_ = NULL;
  clog_pos_list_.reset();
}

int ObPGArchiveCLogTask::prepare_read_buf()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(read_buf_ = op_reclaim_alloc(ObArchiveReadBuf))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "failed to allocate read_buf", "this", *this, K(ret));
  }
  return ret;
}

void ObPGArchiveCLogTask::release_read_buf()
{
  if (NULL != read_buf_) {
    op_reclaim_free(read_buf_);
    read_buf_ = NULL;
  }
}

bool ObPGArchiveCLogTask::is_valid() const
{
  bool bool_ret = (pg_key_.is_valid() && epoch_id_ > 0 && incarnation_ > 0 && log_archive_round_ > 0 &&
                   (OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT != task_type_ || clog_pos_list_.count() > 0) &&
                   (OB_ARCHIVE_TASK_TYPE_KICKOFF != task_type_ || clog_epoch_id_ >= 0));
  return bool_ret;
}

bool ObPGArchiveCLogTask::is_finished() const
{
  bool bool_ret = (processed_log_count_ == get_clog_count());
  return bool_ret;
}

uint64_t ObPGArchiveCLogTask::get_round_start_log_id() const
{
  return round_start_log_id_;
}

ObLogType ObPGArchiveCLogTask::get_log_type() const
{
  ObLogType log_type = ObLogType::OB_LOG_UNKNOWN;
  switch (task_type_) {
    case OB_ARCHIVE_TASK_TYPE_KICKOFF:
      log_type = ObLogType::OB_LOG_ARCHIVE_KICKOFF;
      break;
    case OB_ARCHIVE_TASK_TYPE_CHECKPOINT:
      log_type = ObLogType::OB_LOG_ARCHIVE_CHECKPOINT;
      break;
    default:
      log_type = ObLogType::OB_LOG_UNKNOWN;
  };
  return log_type;
}

bool ObPGArchiveCLogTask::need_compress() const
{
  return (INVALID_COMPRESSOR != compressor_type_ && NONE_COMPRESSOR != compressor_type_);
}

uint64_t ObPGArchiveCLogTask::get_last_log_id() const
{
  uint64_t log_id = OB_INVALID_ID;
  switch (task_type_) {
    case OB_ARCHIVE_TASK_TYPE_KICKOFF:
      log_id = round_start_log_id_ - 1;
      break;
    case OB_ARCHIVE_TASK_TYPE_CHECKPOINT:
      log_id = archive_checkpoint_log_id_;
      break;
    case OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT: {
      log_id = (clog_pos_list_.count() > 0 ? clog_pos_list_.at(clog_pos_list_.count() - 1).log_id_ : OB_INVALID_ID);
      break;
    }
    default:
      log_id = OB_INVALID_ID;
  };
  return log_id;
}

int ObPGArchiveCLogTask::init_clog_split_task(const ObPGKey& pg_key, const int64_t incarnation,
    const int64_t archive_round, const int64_t epoch_id, const uint64_t log_id, const int64_t generate_ts,
    ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= archive_round) ||
      OB_UNLIKELY(0 >= epoch_id) || OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(pg_key),
        K(incarnation),
        K(archive_round),
        K(epoch_id),
        K(log_id),
        K(generate_ts),
        K(compressor_type));
  } else {
    pg_key_ = pg_key;
    incarnation_ = incarnation;
    log_archive_round_ = archive_round;
    epoch_id_ = epoch_id;
    archive_base_log_id_ = log_id;
    generate_ts_ = generate_ts;
    task_type_ = OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT;
    compressor_type_ = compressor_type;

    // clog_epoch_id_ and accum_checksum_, max_archived_log_id_, max_archived_log_submit_ts,
    // and max_checkpoint_ts of archive_block_meta will be inited in sender
  }
  return ret;
}

int ObPGArchiveCLogTask::init_kickoff_task(const ObPGKey& pg_key, const int64_t incarnation,
    const int64_t archive_round, const int64_t epoch_id, const ObArchiveRoundStartInfo& round_start_info,
    const int64_t checkpoint_ts, ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= archive_round) ||
      OB_UNLIKELY(0 >= epoch_id) || OB_UNLIKELY(!round_start_info.is_valid()) ||
      OB_UNLIKELY(OB_INVALID_TIMESTAMP == checkpoint_ts)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR,
        "invalid argument",
        K(ret),
        K(pg_key),
        K(incarnation),
        K(archive_round),
        K(epoch_id),
        K(round_start_info),
        K(checkpoint_ts),
        K(compressor_type));
  } else {
    pg_key_ = pg_key;
    incarnation_ = incarnation;
    log_archive_round_ = archive_round;
    epoch_id_ = epoch_id;

    round_start_log_id_ = round_start_info.start_log_id_;
    round_start_ts_ = round_start_info.start_ts_;
    round_snapshot_version_ = round_start_info.snapshot_version_;
    clog_epoch_id_ = round_start_info.clog_epoch_id_;
    accum_checksum_ = round_start_info.accum_checksum_;
    log_submit_ts_ = round_start_info.log_submit_ts_;

    checkpoint_ts_ = checkpoint_ts;

    task_type_ = OB_ARCHIVE_TASK_TYPE_KICKOFF;
    compressor_type_ = compressor_type;
  }
  return ret;
}

int ObPGArchiveCLogTask::init_checkpoint_task(const ObPGKey& pg_key, const int64_t incarnation,
    const int64_t archive_round, const int64_t epoch_id, const uint64_t log_id, const int64_t checkpoint_ts,
    const int64_t log_submit_ts, ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= archive_round) ||
      OB_UNLIKELY(0 >= epoch_id) || OB_UNLIKELY(OB_INVALID_ID == log_id) ||
      OB_UNLIKELY(OB_INVALID_TIMESTAMP == checkpoint_ts) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == log_submit_ts)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR,
        "invalid argument",
        KR(ret),
        K(pg_key),
        K(incarnation),
        K(archive_round),
        K(epoch_id),
        K(log_id),
        K(checkpoint_ts),
        K(log_submit_ts),
        K(compressor_type));
  } else {
    pg_key_ = pg_key;
    incarnation_ = incarnation;
    log_archive_round_ = archive_round;
    epoch_id_ = epoch_id;
    archive_checkpoint_log_id_ = log_id;
    checkpoint_ts_ = checkpoint_ts;
    log_submit_ts_ = log_submit_ts;

    // clog_epoch_id_ and accum_checksum_ will be inited in sender
    task_type_ = OB_ARCHIVE_TASK_TYPE_CHECKPOINT;
    compressor_type_ = compressor_type;
  }
  return ret;
}

void ObArchiveSendTaskMeta::reset()
{
  pg_key_.reset();
  task_type_ = OB_ARCHIVE_TASK_TYPE_INVALID;
  need_update_log_ts_ = false;
  epoch_id_ = 0;
  incarnation_ = 0;
  log_archive_round_ = 0;
  start_log_id_ = OB_INVALID_ID;
  start_log_ts_ = OB_INVALID_TIMESTAMP;
  end_log_id_ = OB_INVALID_ID;
  end_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  pos_ = 0;
  block_meta_.reset();
}

int ObArchiveSendTaskMeta::assign(const ObArchiveSendTaskMeta& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", K(other), KR(ret));
  } else {
    pg_key_ = other.pg_key_;
    task_type_ = other.task_type_;
    need_update_log_ts_ = other.need_update_log_ts_;
    epoch_id_ = other.epoch_id_;
    incarnation_ = other.incarnation_;
    log_archive_round_ = other.log_archive_round_;
    start_log_id_ = other.start_log_id_;
    start_log_ts_ = other.start_log_ts_;
    end_log_id_ = other.end_log_id_;
    end_log_submit_ts_ = other.end_log_submit_ts_;
    checkpoint_ts_ = other.checkpoint_ts_;
    pos_ = other.pos_;
    block_meta_ = other.block_meta_;
  }
  return ret;
}

bool ObArchiveSendTaskMeta::is_valid() const
{
  return (pg_key_.is_valid() && OB_ARCHIVE_TASK_TYPE_INVALID != task_type_ && epoch_id_ > 0 && incarnation_ > 0 &&
          log_archive_round_ > 0);
}

int ObArchiveSendTaskMeta::set_archive_meta_info(const ObPGArchiveCLogTask& clog_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!clog_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid clog_task", KR(ret), K(clog_task));
  } else {
    pg_key_ = clog_task.pg_key_;
    task_type_ = clog_task.task_type_;
    epoch_id_ = clog_task.epoch_id_;
    incarnation_ = clog_task.incarnation_;
    log_archive_round_ = clog_task.log_archive_round_;
  }
  return ret;
}

void ObTSIArchiveReadBuf::reset_log_related_info()
{
  need_update_log_ts_ = false;
  start_log_id_ = OB_INVALID_ID;
  start_log_ts_ = OB_INVALID_TIMESTAMP;
  end_log_id_ = OB_INVALID_ID;
  end_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  pos_ = 0;
  block_meta_.reset();
}

void ObArchiveSendTask::reset()
{
  ObArchiveSendTaskMeta::reset();
  archive_data_len_ = 0;
  buf_len_ = 0;
  buf_ = NULL;
}

bool ObArchiveSendTask::has_enough_space(const int64_t data_size) const
{
  bool bool_ret = (get_buf_len() - get_data_len() >= data_size) ? true : false;
  return bool_ret;
}

int ObArchiveSendTask::assign_meta(const ObTSIArchiveReadBuf& send_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!send_buf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", K(send_buf), KR(ret));
  } else if (OB_FAIL(ObArchiveSendTaskMeta::assign(send_buf))) {
    ARCHIVE_LOG(WARN, "failed to assign", K(send_buf), KR(ret));
  } else { /*do nothing*/
  }
  return ret;
}

//=======================end  of ObArCompressedLogChunk========================//

void ObArchiveRoundStartInfo::reset()
{
  start_ts_ = OB_INVALID_TIMESTAMP;
  start_log_id_ = OB_INVALID_ID;
  snapshot_version_ = OB_INVALID_TIMESTAMP;
  log_submit_ts_ = OB_INVALID_TIMESTAMP;
  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
}

bool ObArchiveRoundStartInfo::is_valid() const
{
  return ((0 != start_log_id_ && OB_INVALID_ID != start_log_id_) && (start_ts_ >= 0) && log_submit_ts_ >= 0 &&
          snapshot_version_ >= 0 && clog_epoch_id_ >= 0);
}
//=======================start of ObArchiveIndexFileInfo======================//
DEFINE_SERIALIZE(ObArchiveIndexFileInfo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || (0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    ARCHIVE_LOG(WARN, "failed to encode magic_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, record_len_))) {
    ARCHIVE_LOG(WARN, "failed to encode record_len_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, INDEX_FILE_VERSION))) {
    ARCHIVE_LOG(WARN, "failed to encode version_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, is_valid_))) {
    ARCHIVE_LOG(WARN, "failed to encode is_valid_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, data_file_id_))) {
    ARCHIVE_LOG(WARN, "failed to encode data_file_id_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, input_bytes_))) {
    ARCHIVE_LOG(WARN, "failed to encode input_bytes_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, output_bytes_))) {
    ARCHIVE_LOG(WARN, "failed to encode output_bytes_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, clog_epoch_id_))) {
    ARCHIVE_LOG(WARN, "failed to encode clog_epoch_id_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, accum_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode accum_checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, min_log_id_))) {
    ARCHIVE_LOG(WARN, "failed to encode min_log_id_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, min_log_ts_))) {
    ARCHIVE_LOG(WARN, "failed to encode min_log_ts_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, max_log_id_))) {
    ARCHIVE_LOG(WARN, "failed to encode max_log_id_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, max_checkpoint_ts_))) {
    ARCHIVE_LOG(WARN, "failed to encode max_checkpoint_ts_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, max_log_submit_ts_))) {
    ARCHIVE_LOG(WARN, "failed to encode max_log_submit_ts_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_start_ts_))) {
    ARCHIVE_LOG(WARN, "failed to encode round_start_ts_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_start_log_id_))) {
    ARCHIVE_LOG(WARN, "failed to encode round_start_log_id_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_snapshot_version_))) {
    ARCHIVE_LOG(WARN, "failed to encode round_snapshot_version_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_log_submit_ts_))) {
    ARCHIVE_LOG(WARN, "failed to encode round_log_submit_ts_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_clog_epoch_id_))) {
    ARCHIVE_LOG(WARN, "failed to encode round_clog_epoch_id_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_accum_checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode round_accum_checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  } else { /* do nothing */
  }
  return ret;
}

DEFINE_DESERIALIZE(ObArchiveIndexFileInfo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || 0 > data_len) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    ARCHIVE_LOG(WARN, "failed to decode magic_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &record_len_))) {
    ARCHIVE_LOG(WARN, "failed to decode record_len_", KP(buf), K(data_len), K(pos), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (data_len >= record_len_) {
      if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
        ARCHIVE_LOG(WARN, "failed to decode version_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &is_valid_))) {
        ARCHIVE_LOG(WARN, "failed to decode is_valid_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&data_file_id_)))) {
        ARCHIVE_LOG(WARN, "failed to decode data_file_id_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &input_bytes_))) {
        ARCHIVE_LOG(WARN, "failed to decode input_bytes_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &output_bytes_))) {
        ARCHIVE_LOG(WARN, "failed to decode output_bytes_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &clog_epoch_id_))) {
        ARCHIVE_LOG(WARN, "failed to decode clog_epoch_id_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &accum_checksum_))) {
        ARCHIVE_LOG(WARN, "failed to decode accum_checksum_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&min_log_id_)))) {
        ARCHIVE_LOG(WARN, "failed to decode min_log_id_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &min_log_ts_))) {
        ARCHIVE_LOG(WARN, "failed to decode min_log_ts_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&max_log_id_)))) {
        ARCHIVE_LOG(WARN, "failed to decode max_log_id_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &max_checkpoint_ts_))) {
        ARCHIVE_LOG(WARN, "failed to decode max_checkpoint_ts_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &max_log_submit_ts_))) {
        ARCHIVE_LOG(WARN, "failed to decode max_log_submit_ts_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &round_start_ts_))) {
        ARCHIVE_LOG(WARN, "failed to decode round_start_ts_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(
                     serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&round_start_log_id_)))) {
        ARCHIVE_LOG(WARN, "failed to decode start_log_id_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &round_snapshot_version_))) {
        ARCHIVE_LOG(WARN, "failed to decode round_snapshot_vesion_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &round_log_submit_ts_))) {
        ARCHIVE_LOG(WARN, "failed to decode round_log_submit_ts_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &round_clog_epoch_id_))) {
        ARCHIVE_LOG(WARN, "failed to decode round_clog_epoch_id_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &round_accum_checksum_))) {
        ARCHIVE_LOG(WARN, "failed to decode round_accum_checksum_", KP(buf), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &checksum_))) {
        ARCHIVE_LOG(WARN, "failed to decode checksum_", KP(buf), K(data_len), K(pos), K(ret));
      }
    } else {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObArchiveIndexFileInfo)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(record_len_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i16(is_valid_);
  size += serialization::encoded_length_i64(data_file_id_);
  size += serialization::encoded_length_i64(input_bytes_);
  size += serialization::encoded_length_i64(output_bytes_);
  size += serialization::encoded_length_i64(accum_checksum_);
  size += serialization::encoded_length_i64(clog_epoch_id_);
  size += serialization::encoded_length_i64(min_log_id_);
  size += serialization::encoded_length_i64(min_log_ts_);
  size += serialization::encoded_length_i64(max_log_id_);
  size += serialization::encoded_length_i64(max_checkpoint_ts_);
  size += serialization::encoded_length_i64(max_log_submit_ts_);

  size += serialization::encoded_length_i64(round_start_ts_);
  size += serialization::encoded_length_i64(round_start_log_id_);
  size += serialization::encoded_length_i64(round_snapshot_version_);
  size += serialization::encoded_length_i64(round_log_submit_ts_);
  size += serialization::encoded_length_i64(round_clog_epoch_id_);
  size += serialization::encoded_length_i64(round_accum_checksum_);

  size += serialization::encoded_length_i64(checksum_);
  return size;
}

void ObArchiveIndexFileInfo::reset()
{
  magic_ = 0;
  record_len_ = 0;
  version_ = 0;
  is_valid_ = 0;
  data_file_id_ = 0;

  input_bytes_ = 0;
  output_bytes_ = 0;

  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;

  min_log_id_ = OB_INVALID_ID;
  min_log_ts_ = OB_INVALID_TIMESTAMP;
  max_log_id_ = OB_INVALID_ID;
  max_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  max_log_submit_ts_ = OB_INVALID_TIMESTAMP;

  round_start_ts_ = OB_INVALID_TIMESTAMP;
  round_start_log_id_ = OB_INVALID_ID;
  round_snapshot_version_ = OB_INVALID_TIMESTAMP;
  round_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  round_clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  round_accum_checksum_ = 0;
  checksum_ = 0;
}

bool ObArchiveIndexFileInfo::is_valid()
{
  bool bret = true;

  if (OB_UNLIKELY(MAGIC_NUM != magic_) || OB_UNLIKELY(0 == data_file_id_) || OB_UNLIKELY(0 >= version_) ||
      OB_UNLIKELY(!is_valid_) || OB_UNLIKELY(0 == round_start_log_id_ || OB_INVALID_ID == round_start_log_id_) ||
      OB_UNLIKELY(checksum_ != calc_checksum_())) {
    bret = false;
    ARCHIVE_LOG(WARN, "illegal index info", KPC(this), "new_checksum", calc_checksum_());
  }

  return bret;
}

int ObArchiveIndexFileInfo::build_valid_record(const uint64_t data_file_id, const uint64_t min_log_id,
    const int64_t min_log_submit_ts, const uint64_t max_log_id, const int64_t checkpoint_ts,
    const int64_t max_log_submit_ts, const int64_t clog_epoch_id, const int64_t accum_checksum,
    const ObArchiveRoundStartInfo& round_start_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == data_file_id ||
                  OB_INVALID_ID == min_log_id
                  // || OB_INVALID_TIMESTAMP == min_log_submit_ts(obsolete)
                  || OB_INVALID_ID == max_log_id || OB_INVALID_TIMESTAMP == max_log_submit_ts ||
                  OB_INVALID_TIMESTAMP == checkpoint_ts || 0 > clog_epoch_id || (!round_start_info.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR,
        "invalid arguemtns",
        KR(ret),
        K(data_file_id),
        K(min_log_id),
        K(min_log_submit_ts),
        K(max_log_id),
        K(checkpoint_ts),
        K(max_log_submit_ts),
        K(clog_epoch_id),
        K(accum_checksum),
        K(round_start_info));
  } else {
    magic_ = MAGIC_NUM;
    version_ = INDEX_FILE_VERSION;
    is_valid_ = 1;
    data_file_id_ = data_file_id;
    min_log_id_ = min_log_id;
    min_log_ts_ = min_log_submit_ts;
    max_log_id_ = max_log_id;
    max_checkpoint_ts_ = checkpoint_ts;
    max_log_submit_ts_ = max_log_submit_ts;
    clog_epoch_id_ = clog_epoch_id;
    accum_checksum_ = accum_checksum;

    round_start_ts_ = round_start_info.start_ts_;
    round_start_log_id_ = round_start_info.start_log_id_;
    round_snapshot_version_ = round_start_info.snapshot_version_;
    round_log_submit_ts_ = round_start_info.log_submit_ts_;
    round_clog_epoch_id_ = round_start_info.clog_epoch_id_;
    round_accum_checksum_ = round_start_info.accum_checksum_;
    record_len_ = get_serialize_size();
    checksum_ = calc_checksum_();
  }
  return ret;
}

int ObArchiveIndexFileInfo::build_invalid_record(const uint64_t data_file_id, const uint64_t max_log_id,
    const int64_t checkpoint_ts, const int64_t max_log_submit_ts, const int64_t clog_epoch_id,
    const int64_t accum_checksum, const ObArchiveRoundStartInfo& round_start_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == data_file_id || OB_INVALID_ID == max_log_id || OB_INVALID_TIMESTAMP == max_log_submit_ts ||
                  OB_INVALID_TIMESTAMP == checkpoint_ts || 0 > clog_epoch_id || (!round_start_info.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR,
        "invalid arguments",
        KR(ret),
        K(data_file_id),
        K(max_log_id),
        K(max_log_submit_ts),
        K(checkpoint_ts),
        K(clog_epoch_id),
        K(accum_checksum),
        K(round_start_info));
  } else {
    magic_ = MAGIC_NUM;
    version_ = INDEX_FILE_VERSION;
    is_valid_ = 0;
    data_file_id_ = data_file_id;
    min_log_id_ = max_log_id;
    min_log_ts_ = max_log_submit_ts;
    max_log_id_ = max_log_id;
    max_log_submit_ts_ = max_log_submit_ts;
    max_checkpoint_ts_ = checkpoint_ts;
    clog_epoch_id_ = clog_epoch_id;
    accum_checksum_ = accum_checksum;

    round_start_ts_ = round_start_info.start_ts_;
    round_start_log_id_ = round_start_info.start_log_id_;
    round_snapshot_version_ = round_start_info.snapshot_version_;
    round_log_submit_ts_ = round_start_info.log_submit_ts_;
    round_clog_epoch_id_ = round_start_info.clog_epoch_id_;
    round_accum_checksum_ = round_start_info.accum_checksum_;

    record_len_ = get_serialize_size();
    checksum_ = calc_checksum_();
  }
  return ret;
}

int ObArchiveIndexFileInfo::get_real_record_length(const char* buf, const int64_t buf_len, int64_t& record_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(buf), K(buf_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &magic_))) {
    ARCHIVE_LOG(WARN, "failed to decode magic_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &record_len_))) {
    ARCHIVE_LOG(WARN, "failed to decode record_len_", KP(buf), K(buf_len), K(pos), K(ret));
  } else {
    record_len = record_len_;
  }

  return ret;
}

int64_t ObArchiveIndexFileInfo::calc_checksum_() const
{
  // NOTE: consider compatibility if more members added
  int64_t calc_checksum_len = get_serialize_size() - sizeof(checksum_);
  return ob_crc64(this, calc_checksum_len);
}

//=======================end of ObArchiveIndexFileInfo========================//

//=======================start of MaxArchivedIndexInfo =======================//
MaxArchivedIndexInfo::MaxArchivedIndexInfo()
{
  reset();
}

MaxArchivedIndexInfo::~MaxArchivedIndexInfo()
{
  reset();
}

void MaxArchivedIndexInfo::reset()
{
  data_file_collect_ = false;
  archived_log_collect_ = false;
  round_start_info_collect_ = false;
  max_record_data_file_id_ = 0;
  max_record_log_id_ = OB_INVALID_ID;
  max_record_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  max_record_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
  round_start_info_.reset();
}

bool MaxArchivedIndexInfo::is_index_info_collected_()
{
  return data_file_collect_ && archived_log_collect_ && round_start_info_collect_;
}

void MaxArchivedIndexInfo::set_data_file_id(const uint64_t file_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == file_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file_id", K(file_id), K(ret));
  } else if (data_file_collect_) {
    // skip
  } else {
    data_file_collect_ = true;
    max_record_data_file_id_ = file_id;
  }
}

int MaxArchivedIndexInfo::set_log_info(const uint64_t max_log_id, const int64_t max_checkpoint_ts,
    const int64_t max_log_submit_ts, const int64_t clog_epoch_id, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(
          OB_INVALID_ID == max_log_id || 0 >= max_log_submit_ts || 0 > max_checkpoint_ts || 0 > clog_epoch_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "invalid arguments",
        K(max_log_id),
        K(max_log_submit_ts),
        K(max_checkpoint_ts),
        K(clog_epoch_id),
        K(accum_checksum),
        K(ret));
  } else if (archived_log_collect_) {
    // skip
  } else {
    max_record_log_id_ = max_log_id;
    max_record_checkpoint_ts_ = max_checkpoint_ts;
    max_record_log_submit_ts_ = max_log_submit_ts;
    clog_epoch_id_ = clog_epoch_id;
    accum_checksum_ = accum_checksum;
    archived_log_collect_ = true;
  }
  return ret;
}

int MaxArchivedIndexInfo::set_round_start_info(const ObArchiveIndexFileInfo& index_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == index_info.round_start_log_id_ || 0 == index_info.round_start_log_id_ ||
                  index_info.round_start_ts_ < 0 || OB_INVALID_TIMESTAMP == index_info.round_snapshot_version_ ||
                  OB_INVALID_TIMESTAMP == index_info.round_log_submit_ts_ || index_info.round_clog_epoch_id_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid arguments", K(index_info), KR(ret));
  } else if (round_start_info_collect_) {
    // skip it
  } else {
    round_start_info_collect_ = true;

    round_start_info_.start_ts_ = index_info.round_start_ts_;
    round_start_info_.start_log_id_ = index_info.round_start_log_id_;
    round_start_info_.snapshot_version_ = index_info.round_snapshot_version_;
    round_start_info_.log_submit_ts_ = index_info.round_log_submit_ts_;
    round_start_info_.clog_epoch_id_ = index_info.round_clog_epoch_id_;
    round_start_info_.accum_checksum_ = index_info.round_accum_checksum_;
  }
  return ret;
}
//=====================end of MaxArchivedIndexInfo =======================//

//=====================start of ObArchiveStartTimestamp=======================//
DEFINE_SERIALIZE(ObArchiveStartTimestamp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || (0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    ARCHIVE_LOG(WARN, "failed to encode checksum_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, archive_round_))) {
    ARCHIVE_LOG(WARN, "failed to encode archive_round_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, timestamp_))) {
    ARCHIVE_LOG(WARN, "failed to encode timestamp_", KP(buf), K(buf_len), K(pos), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

DEFINE_DESERIALIZE(ObArchiveStartTimestamp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || (0 > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &checksum_))) {
    ARCHIVE_LOG(WARN, "failed to decode checksum_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &archive_round_))) {
    ARCHIVE_LOG(WARN, "failed to decode archive_round_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &timestamp_))) {
    ARCHIVE_LOG(WARN, "failed to decode timestamp_", KP(buf), K(data_len), K(pos), K(ret));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObArchiveStartTimestamp)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(checksum_);
  size += serialization::encoded_length_i64(archive_round_);
  size += serialization::encoded_length_i64(timestamp_);
  return size;
}

int ObArchiveStartTimestamp::set(const int64_t archive_round, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= archive_round) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(archive_round), K(timestamp));
  } else {
    archive_round_ = archive_round;
    timestamp_ = timestamp;
    checksum_ = calc_checksum_();
  }
  return ret;
}

bool ObArchiveStartTimestamp::is_valid()
{
  return archive_round_ > 0 && timestamp_ != OB_INVALID_TIMESTAMP && checksum_ == calc_checksum_();
}

int64_t ObArchiveStartTimestamp::calc_checksum_() const
{
  int64_t calc_checksum_len = sizeof(*this) - sizeof(checksum_);
  return ob_crc64(this, calc_checksum_len);
}
//=====================end of ObArchiveStartTimestamp=======================//
void ObArchiveCompressedChunkHeader::reset()
{
  magic_ = 0;
  version_ = 0;
  compressor_type_ = INVALID_COMPRESSOR;
  orig_data_len_ = 0;
  compressed_data_len_ = 0;
}

bool ObArchiveCompressedChunkHeader::is_valid() const
{
  return ((ARCHIVE_COMPRESSED_CHUNK_MAGIC == magic_) && (orig_data_len_ > 0) && (compressed_data_len_ > 0) &&
          is_valid_archive_compressor_type(compressor_type_));
}

int ObArchiveCompressedChunkHeader::generate_header(
    const common::ObCompressorType compressor_type, const int32_t orig_data_len, const int32_t compressed_data_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
          0 >= orig_data_len || 0 >= compressed_data_len || (!is_valid_archive_compressor_type(compressor_type)))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(compressor_type), K(orig_data_len), K(compressed_data_len));
  } else {
    magic_ = ARCHIVE_COMPRESSED_CHUNK_MAGIC;
    version_ = ARCHIVE_COMPRESSED_CHUNK_VERSION;
    orig_data_len_ = orig_data_len;
    compressed_data_len_ = compressed_data_len;
    compressor_type_ = compressor_type;
  }
  return ret;
}

DEFINE_SERIALIZE(ObArchiveCompressedChunkHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    ARCHIVE_LOG(WARN, "failed to encode magic_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    ARCHIVE_LOG(WARN, "failed to encode version_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, compressor_type_))) {
    ARCHIVE_LOG(WARN, "failed to encode compressor_type_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, orig_data_len_))) {
    ARCHIVE_LOG(WARN, "failed to encode orig_data_len_", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, compressed_data_len_))) {
    ARCHIVE_LOG(WARN, "failed to encode compressed_data_len_", KP(buf), K(buf_len), K(pos), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

DEFINE_DESERIALIZE(ObArchiveCompressedChunkHeader)
{
  int ret = OB_SUCCESS;
  int32_t compressor_type = -1;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    ARCHIVE_LOG(WARN, "failed to decode magic_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    ARCHIVE_LOG(WARN, "failed to decode version_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &compressor_type))) {
    ARCHIVE_LOG(WARN, "failed to decode compressor_type_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &orig_data_len_))) {
    ARCHIVE_LOG(WARN, "failed to decode orig_data_len_", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &compressed_data_len_))) {
    ARCHIVE_LOG(WARN, "failed to decode compressed_data_len_", KP(buf), K(data_len), K(pos), K(ret));
  } else {
    compressor_type_ = static_cast<ObCompressorType>(compressor_type);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObArchiveCompressedChunkHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(compressor_type_);
  size += serialization::encoded_length_i32(orig_data_len_);
  size += serialization::encoded_length_i32(compressed_data_len_);
  return size;
}
//=======================end of ObArchiveCompressedChunkHeader========================//
//
//=======================start of ObArchiveCompressedChunk========================//

ObArchiveCompressedChunk::~ObArchiveCompressedChunk()
{
  header_.reset();
  buf_ = NULL;
}

DEFINE_SERIALIZE(ObArchiveCompressedChunk)
{
  int ret = OB_SUCCESS;
  int64_t data_len = header_.get_data_len();
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || (0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    ret = OB_SERIALIZE_ERROR;
    ARCHIVE_LOG(WARN, "header serialize error", K(ret), K(buf_len), K(new_pos), K(pos));
  } else if ((buf_len - new_pos) < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    ARCHIVE_LOG(WARN, "buf not enough", K(buf_len), K(data_len), K(new_pos), K(pos), KR(ret));
  } else {
    MEMCPY(static_cast<char*>(buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObArchiveCompressedChunk)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    ret = OB_DESERIALIZE_ERROR;
    ARCHIVE_LOG(WARN, "header deserialize error", K(ret), K(data_len), K(new_pos));
  } else if (data_len - new_pos < header_.get_data_len()) {
    ret = OB_DESERIALIZE_ERROR;
    ARCHIVE_LOG(WARN, "buf is not enough to deserialize", K(ret), K(data_len), K(new_pos), K(header_));
  } else if (OB_UNLIKELY(!header_.is_valid())) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(WARN, "invalid header", KR(ret), K(header_));
  } else {
    buf_ = buf + new_pos;
    pos = new_pos + header_.get_data_len();
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObArchiveCompressedChunk)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += header_.get_data_len();
  return size;
}

//================start of structs for backup============//

void ObArchiveReadParam::reset()
{
  file_id_ = 0;
  offset_ = -1;
  read_len_ = 0;
  timeout_ = OB_ARCHIVE_READ_TIMEOUT;
  pg_key_.reset();
}

bool ObArchiveReadParam::is_valid() const
{
  return file_id_ > 0 && offset_ >= 0 && read_len_ > 0 && timeout_ > 0 && pg_key_.is_valid();
}

//================  end of structs for backup============//
}  // end of namespace archive
}  // end of namespace oceanbase
