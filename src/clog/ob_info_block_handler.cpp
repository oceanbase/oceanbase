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

#include "ob_info_block_handler.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_storage.h"
#include "ob_log_define.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
namespace clog {
ObIInfoBlockHandler::ObIInfoBlockHandler()
{
  freeze_version_ = ObVersion(1, 0);
  have_info_hash_v2_ = false;
  info_hash_v2_.reset();
  max_submit_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObIndexInfoBlockHandler::InfoEntryLoader::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntry& info_entry)
{
  int tmp_ret = OB_SUCCESS;
  bool bool_ret = false;

  IndexInfoBlockEntry entry;

  // based on InfoEntry, init entry
  entry.min_log_id_ = info_entry.min_log_id_;
  entry.min_log_timestamp_ = info_entry.min_log_timestamp_;
  entry.max_log_timestamp_ = info_entry.max_log_timestamp_;

  // insert entry into map
  if (OB_SUCCESS != (tmp_ret = map_.insert(partition_key, entry))) {
    CLOG_LOG(ERROR, "insert index info block map failed", K(tmp_ret), K(partition_key), K(info_entry), K(entry));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObIndexInfoBlockHandler::InfoEntryV2Loader::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntryV2& info_entry_v2)
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  IndexInfoBlockEntry entry;

  // the corresponding log must already exists.
  if (OB_SUCCESS != (tmp_ret = map_.get(partition_key, entry))) {
    CLOG_LOG(ERROR, "get IndexInfoBlockEntry from map fail", K(tmp_ret), K(partition_key));
  } else {
    // update max_log_id
    entry.max_log_id_ = info_entry_v2.max_log_id_;

    // updat map
    if (OB_SUCCESS != (tmp_ret = map_.update(partition_key, entry))) {
      CLOG_LOG(ERROR, "update IndexInfoBlockEntry fail", K(tmp_ret), K(partition_key));
    } else {
      bool_ret = true;
    }
  }

  return bool_ret;
}

int ObIndexInfoBlockHandler::get_all_entry(IndexInfoBlockMap& map)
{
  int ret = OB_SUCCESS;
  int64_t capacity = (CLOG_INFO_BLOCK_SIZE_LIMIT) / map.item_size();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else if (OB_FAIL(map.init(ObModIds::OB_CLOG_INFO_BLK_HNDLR, capacity))) {
    CLOG_LOG(ERROR, "index info block map init failed", K(ret), K(capacity));
  } else {
    InfoEntryLoader info_entry_loader(map);
    InfoEntryV2Loader info_entry_v2_loader(map);

    // load InfoEntry
    if (OB_FAIL(info_hash_.for_each(info_entry_loader))) {
      CLOG_LOG(WARN, "for_each info hash fail", K(ret));
    }
    // load InfoEntryV2
    else if (OB_FAIL(info_hash_v2_.for_each(info_entry_v2_loader))) {
      CLOG_LOG(WARN, "for_each info hash v2 fail", K(ret));
    } else {
      // success
    }
  }
  return ret;
}

ObIndexInfoBlockHandler::AppendMinLogIdInfoFunctor::AppendMinLogIdInfoFunctor(MinLogIdInfo& min_log_id_info)
    : min_log_id_info_(min_log_id_info)
{}

bool ObIndexInfoBlockHandler::AppendMinLogIdInfoFunctor::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntry& info_entry)
{
  int tmp_ret = OB_SUCCESS;
  bool bool_ret = false;
  if (OB_SUCCESS != (tmp_ret = min_log_id_info_.insert(partition_key, info_entry.min_log_id_))) {
    CLOG_LOG(ERROR, "insert failed", K(partition_key), K(info_entry));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int ObIndexInfoBlockHandler::get_all_min_log_id_info(MinLogIdInfo& min_log_id_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else if (OB_FAIL(min_log_id_info.init(
                 ObModIds::OB_CLOG_INFO_BLK_HNDLR, (CLOG_INFO_BLOCK_SIZE_LIMIT) / min_log_id_info.item_size()))) {
    CLOG_LOG(ERROR, "min_log_id_info init failed", K(ret));
  } else {
    AppendMinLogIdInfoFunctor functor(min_log_id_info);
    ret = info_hash_.for_each(functor);
  }
  return ret;
}

ObIndexInfoBlockHandler::InfoEntrySerializeFunctor::InfoEntrySerializeFunctor(
    char* buf, const int64_t buf_len, int64_t& pos, TstampArray& max_log_tstamp_array)
    : buf_(buf), buf_len_(buf_len), pos_(pos), max_log_tstamp_array_(max_log_tstamp_array)
{}

bool ObIndexInfoBlockHandler::InfoEntrySerializeFunctor::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntry& info_entry)
{
  int tmp_ret = OB_SUCCESS;
  bool bool_ret = false;

  // to be compatible with older versions, only serialize min_log_id and min_log_ts
  // the max_log_ts serialize separately.
  if (OB_SUCCESS != (tmp_ret = partition_key.serialize(buf_, buf_len_, pos_))) {
    CLOG_LOG(ERROR, "serialization failed", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = serialization::encode_i64(buf_, buf_len_, pos_, info_entry.min_log_id_))) {
    CLOG_LOG(ERROR, "serialization failed", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = serialization::encode_i64(buf_, buf_len_, pos_, info_entry.min_log_timestamp_))) {
    CLOG_LOG(ERROR, "serialization failed", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = max_log_tstamp_array_.push_back(info_entry.max_log_timestamp_))) {
    CLOG_LOG(ERROR, "push back max log timestamp fail", K(tmp_ret), K(info_entry), K(max_log_tstamp_array_));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

ObIInfoBlockHandler::InfoEntrySerializeFunctorV2::InfoEntrySerializeFunctorV2(
    char* buf, const int64_t buf_len, int64_t& pos)
    : buf_(buf), buf_len_(buf_len), pos_(pos)
{}

bool ObIInfoBlockHandler::InfoEntrySerializeFunctorV2::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntryV2& info_entry_v2)
{
  int tmp_ret = OB_SUCCESS;
  bool bool_ret = false;
  if (OB_SUCCESS != (tmp_ret = partition_key.serialize(buf_, buf_len_, pos_))) {
    CLOG_LOG(ERROR, "serialization failed", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = serialization::encode_i64(buf_, buf_len_, pos_, info_entry_v2.max_log_id_))) {
    CLOG_LOG(ERROR, "serialization failed", K(tmp_ret));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int ObIInfoBlockHandler::get_max_log_id(const common::ObPartitionKey& partition_key, uint64_t& max_log_id)
{
  int ret = OB_SUCCESS;
  max_log_id = OB_INVALID_ID;
  if (have_info_hash_v2_) {
    InfoEntryV2 entry_v2;
    entry_v2.reset();
    if (OB_SUCC(info_hash_v2_.get(partition_key, entry_v2))) {
      max_log_id = entry_v2.max_log_id_;
    }
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(ERROR, "info_hash_v2 get failed", K(partition_key), K(ret));
    }
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObIInfoBlockHandler::update_info_hash_v2_(const common::ObPartitionKey& partition_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  InfoEntryV2 entry_v2;

  if (OB_SUCC(info_hash_v2_.get(partition_key, entry_v2))) {
    if (log_id > entry_v2.max_log_id_) {
      entry_v2.max_log_id_ = log_id;
    }
    if (OB_FAIL(info_hash_v2_.update(partition_key, entry_v2))) {
      CLOG_LOG(WARN, "info_hash_v2_ update failed", K(ret), K(partition_key), K(entry_v2));
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    entry_v2.max_log_id_ = log_id;
    if (OB_FAIL(info_hash_v2_.insert(partition_key, entry_v2))) {
      CLOG_LOG(WARN, "info_hash_v2.insert failed", K(ret), K(partition_key), K(entry_v2));
    }
  } else {
    CLOG_LOG(WARN, "info_hash_v2 get failed", K(ret), K(partition_key), K(entry_v2));
  }

  // init have_info_hash_v2_
  if (OB_SUCCESS == ret && !have_info_hash_v2_) {
    have_info_hash_v2_ = true;
  }

  CLOG_LOG(TRACE, "update_info_hash_v2_", K(ret), K(partition_key), K(log_id), K(entry_v2), K(have_info_hash_v2_));
  return ret;
}

int ObIInfoBlockHandler::resolve_info_hash_v2_(
    const char* buf, const int64_t buf_len, int64_t& pos, const int64_t expected_magic_number)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  uint64_t partition_num = 0;
  int64_t new_pos = pos;

  if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &magic_num))) {
    CLOG_LOG(WARN, "deserialization failed", K(ret), K(buf), K(buf_len), K(new_pos));
  } else if (magic_num != expected_magic_number) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(INFO, "magic number not match", K(magic_num), K(expected_magic_number));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, reinterpret_cast<int64_t*>(&partition_num)))) {
    CLOG_LOG(WARN, "deserialization failed", K(ret), K(buf), K(buf_len), K(new_pos));
  } else if (OB_FAIL(info_hash_v2_.shrink_size(partition_num + 1))) {
    CLOG_LOG(WARN, "info_hash_v2 shrink failed", K(ret), K(partition_num));
  } else {
    common::ObPartitionKey partition_key;
    InfoEntryV2 entry_v2;
    for (uint64_t i = 0; i < partition_num && OB_SUCC(ret); ++i) {
      uint64_t max_log_id = 0;
      partition_key.reset();
      if (OB_FAIL(partition_key.deserialize(buf, buf_len, new_pos))) {
        CLOG_LOG(WARN, "deserialization failed", K(ret));
      } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, reinterpret_cast<int64_t*>(&max_log_id)))) {
        CLOG_LOG(WARN, "deserialization failed", K(ret));
      } else {
        entry_v2.reset();
        entry_v2.max_log_id_ = max_log_id;
        if (OB_FAIL(info_hash_v2_.insert(partition_key, entry_v2))) {
          CLOG_LOG(WARN, "info_hash_v2_ insert failed", K(ret), K(partition_key), K(entry_v2));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    have_info_hash_v2_ = true;
  }
  pos = new_pos;

  return ret;
}

ObCommitInfoBlockHandler::ObCommitInfoBlockHandler() : is_inited_(false)
{}

int ObCommitInfoBlockHandler::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(info_hash_v2_.init(
                 ObModIds::OB_CLOG_INFO_BLK_HNDLR, (CLOG_INFO_BLOCK_SIZE_LIMIT) / info_hash_v2_.item_size()))) {
    CLOG_LOG(ERROR, "info_hash_v2_ init failed", K(ret));
  } else {
    freeze_version_ = ObVersion(1, 0);
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObCommitInfoBlockHandler::destroy()
{
  info_hash_v2_.destroy();
  have_info_hash_v2_ = false;
  is_inited_ = false;
}

int ObCommitInfoBlockHandler::build_info_block(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObCommitInfoBlockHandler is not inited", K(ret));
  } else if (NULL == buf || buf_len < 0 || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, CLOG_INFO_BLOCK_VERSION))) {
    CLOG_LOG(ERROR, "serialization failed", K(ret));
  } else if (OB_FAIL(freeze_version_.serialize(buf, buf_len, new_pos))) {
    CLOG_LOG(ERROR, "serialization failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, MAGIC_NUMBER))) {
    CLOG_LOG(ERROR, "serialization failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, info_hash_v2_.size()))) {
    CLOG_LOG(ERROR, "serialization failed", K(ret));
  } else {
    InfoEntrySerializeFunctorV2 functor_v2(buf, buf_len, new_pos);
    ret = info_hash_v2_.for_each(functor_v2);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_max_submit_timestamp_(buf, buf_len, new_pos))) {
      CLOG_LOG(ERROR, "build_max_submit_timestamp_ failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(reset())) {
      CLOG_LOG(ERROR, "switch file failed", K(ret));
    } else {
      pos = new_pos;
    }
  }
  CLOG_LOG(INFO, "clog info block version", K(ret), K(freeze_version_));
  return ret;
}

int ObCommitInfoBlockHandler::resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int16_t clog_info_block_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObCommitInfoBlockHandler is not inited", K(ret));
  } else if (NULL == buf || buf_len < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i16(buf, buf_len, new_pos, &clog_info_block_version))) {
    CLOG_LOG(ERROR, "deserialization failed", K(ret));
  } else if (clog_info_block_version != CLOG_INFO_BLOCK_VERSION) {
    CLOG_LOG(ERROR, "invalid version", K(clog_info_block_version), K(CLOG_INFO_BLOCK_VERSION));
    ret = OB_VERSION_NOT_MATCH;
  } else if (OB_FAIL(freeze_version_.deserialize(buf, buf_len, new_pos))) {
    CLOG_LOG(ERROR, "deserialization failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = resolve_info_hash_v2_(buf, buf_len, new_pos, MAGIC_NUMBER))) {
      CLOG_LOG(INFO, "resolve_info_hash_v2_ failed", K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = resolve_max_submit_timestamp_(buf, buf_len, new_pos))) {
      CLOG_LOG(INFO, "resolve_max_submit_timestamp_ failed", K(tmp_ret));
    }
  }
  pos = new_pos;
  return ret;
}

int ObCommitInfoBlockHandler::reset()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObCommitInfoBlockHandler is not inited", K(ret));
  } else {
    have_info_hash_v2_ = false;
    info_hash_v2_.reset();
  }
  return ret;
}

int ObCommitInfoBlockHandler::update_info(const int64_t max_submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObCommitInfoBlockHandler is not inited", K(ret));
  } else if (OB_INVALID_TIMESTAMP == max_submit_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(max_submit_timestamp));
  } else if (max_submit_timestamp > max_submit_timestamp_) {
    max_submit_timestamp_ = max_submit_timestamp;
  }
  return ret;
}

int ObCommitInfoBlockHandler::update_info(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObCommitInfoBlockHandler is not inited", K(ret));
  } else if (!partition_key.is_valid() || OB_INVALID_ID == log_id || OB_INVALID_TIMESTAMP == submit_timestamp ||
             0 == submit_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(partition_key), K(log_id), K(submit_timestamp));
  } else {
    if (submit_timestamp > max_submit_timestamp_) {
      max_submit_timestamp_ = submit_timestamp;
    }
    ret = update_info_hash_v2_(partition_key, log_id);
  }
  return ret;
}

ObIndexInfoBlockHandler::ObIndexInfoBlockHandler() : is_inited_(false)
{}

int ObIndexInfoBlockHandler::init(void)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(info_hash_.init(
                 ObModIds::OB_CLOG_INFO_BLK_HNDLR, (CLOG_INFO_BLOCK_SIZE_LIMIT) / info_hash_.item_size()))) {
    CLOG_LOG(ERROR, "info_hash init failed", K(ret));
  } else if (OB_FAIL(info_hash_v2_.init(
                 ObModIds::OB_CLOG_INFO_BLK_HNDLR, (CLOG_INFO_BLOCK_SIZE_LIMIT) / info_hash_v2_.item_size()))) {
    CLOG_LOG(ERROR, "info_hash_v2 init failed", K(ret));
  } else {
    freeze_version_ = ObVersion(1, 0);
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObIndexInfoBlockHandler::destroy()
{
  info_hash_.destroy();
  info_hash_v2_.destroy();
  have_info_hash_v2_ = false;
  is_inited_ = false;
}

int ObIndexInfoBlockHandler::build_info_block(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  TstampArray max_log_tstamp_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else {
    int64_t new_pos = pos;
    if (NULL == buf || buf_len < 0 || pos < 0 || pos > buf_len) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, INDEX_LOG_INFO_BLOCK_VERSION))) {
      CLOG_LOG(ERROR, "serialization failed", K(ret));
    } else if (OB_FAIL(freeze_version_.serialize(buf, buf_len, new_pos))) {
      CLOG_LOG(ERROR, "serialization failed", K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, info_hash_.size()))) {
      CLOG_LOG(ERROR, "serialization failed", K(ret));
    } else {
      InfoEntrySerializeFunctor functor(buf, buf_len, new_pos, max_log_tstamp_array);
      ret = info_hash_.for_each(functor);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, MAGIC_NUMBER))) {
        CLOG_LOG(ERROR, "serialization failed", K(ret));
      } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, info_hash_v2_.size()))) {
        CLOG_LOG(ERROR, "serialization failed", K(ret));
      } else {
        InfoEntrySerializeFunctorV2 functor_v2(buf, buf_len, new_pos);
        ret = info_hash_v2_.for_each(functor_v2);
      }
    }

    // serialize max_log_timestamp
    if (OB_SUCC(ret)) {
      int64_t part_number = info_hash_.size();
      int64_t magic_number_for_max_log_tstamp = MAGIC_NUMBER_FOR_MAX_LOG_TIMESTAMP;

      if (OB_UNLIKELY(part_number != max_log_tstamp_array.count())) {
        CLOG_LOG(WARN,
            "error unexpected, part_number does not equals to max_log_timestamp array size",
            K(part_number),
            K(max_log_tstamp_array.count()));
        ret = OB_ERR_UNEXPECTED;
      }
      // encode magic number
      else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, magic_number_for_max_log_tstamp))) {
        CLOG_LOG(
            ERROR, "encode magic number failed", K(ret), K(buf_len), K(new_pos), K(magic_number_for_max_log_tstamp));
      }
      // encode the number of partition
      else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, part_number))) {
        CLOG_LOG(ERROR, "encode partition number failed", K(ret), K(part_number), K(new_pos), K(buf_len));
      } else {
        // encode max_log_timestamp
        // NOTE: required same order with above
        for (int64_t index = 0; OB_SUCC(ret) && index < max_log_tstamp_array.count(); index++) {
          int64_t max_log_timestamp = max_log_tstamp_array.at(index);

          if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, max_log_timestamp))) {
            CLOG_LOG(
                ERROR, "encode max log timestamp fail", K(ret), K(max_log_timestamp), K(buf), K(buf_len), K(new_pos));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(build_max_submit_timestamp_(buf, buf_len, new_pos))) {
        CLOG_LOG(ERROR, "build_max_submit_timestamp_ failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(reset())) {
        CLOG_LOG(ERROR, "reset failed", K(ret));
      } else {
        pos = new_pos;
      }
    } else if (OB_EAGAIN == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  CLOG_LOG(INFO, "ilog info block version", K(ret), K(freeze_version_));
  return ret;
}

int ObIndexInfoBlockHandler::resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObPartitionKey* part_array = NULL;
  int64_t partition_num = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else {
    int64_t new_pos = pos;
    int16_t index_log_info_block_version = 0;
    if (NULL == buf || buf_len < 0 || pos < 0 || pos > buf_len) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(serialization::decode_i16(buf, buf_len, new_pos, &index_log_info_block_version))) {
      CLOG_LOG(ERROR, "deserialization failed", K(ret));
    } else if (index_log_info_block_version != INDEX_LOG_INFO_BLOCK_VERSION) {
      ret = OB_VERSION_NOT_MATCH;
      CLOG_LOG(ERROR, "invalid version", K(ret), K(index_log_info_block_version), K(INDEX_LOG_INFO_BLOCK_VERSION));
    } else if (OB_FAIL(freeze_version_.deserialize(buf, buf_len, new_pos))) {
      CLOG_LOG(ERROR, "deserialization failed", K(ret), K(buf), K(buf_len), K(new_pos));
    } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &partition_num))) {
      CLOG_LOG(ERROR, "deserialization failed", K(ret));
    } else if (OB_FAIL(info_hash_.shrink_size(partition_num + 1))) {
      CLOG_LOG(ERROR, "info_hash shrink fail", K(ret), K(partition_num));
    } else {
      InfoEntry entry;
      int64_t part_array_size = (sizeof(ObPartitionKey) * partition_num);

      // alloc a partition array, used to save partition_key during serialization
      if (partition_num > 0) {
        ObMemAttr mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_CLOG_INFO_BLK_HNDLR);
        part_array = static_cast<ObPartitionKey*>(ob_malloc(part_array_size, mem_attr));
        if (OB_ISNULL(part_array)) {
          CLOG_LOG(WARN, "allocate partition array fail", K(partition_num), K(part_array_size));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
        uint64_t min_log_id = 0;
        int64_t min_log_timestamp = 0;

        ObPartitionKey* pkey = new (part_array + i) ObPartitionKey();

        if (OB_FAIL(pkey->deserialize(buf, buf_len, new_pos))) {
          CLOG_LOG(ERROR, "deserialization failed", K(ret));
        } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, reinterpret_cast<int64_t*>(&min_log_id)))) {
          CLOG_LOG(ERROR, "deserialization failed", K(ret));
        } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &min_log_timestamp))) {
          CLOG_LOG(ERROR, "deserialization failed", K(ret));
        } else {
          entry.reset();
          entry.min_log_id_ = min_log_id;
          entry.min_log_timestamp_ = min_log_timestamp;
          ret = info_hash_.insert(*pkey, entry);
        }
      }
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      // decode max_log_id
      tmp_ret = resolve_info_hash_v2_(buf, buf_len, new_pos, MAGIC_NUMBER);
      if (OB_SUCCESS != tmp_ret) {
        CLOG_LOG(INFO, "resolve_info_hash_v2_ failed", K(tmp_ret));
      } else {
        // decode max_log_timestamp
        // ignore failure
        tmp_ret = resolve_max_log_timestamp_(buf, buf_len, new_pos, part_array, partition_num);

        if (OB_SUCCESS != tmp_ret) {
          CLOG_LOG(WARN, "resolve ilog info block max log timestamp fail", K(tmp_ret), K(buf), K(buf_len), K(new_pos));
        }
      }

      if (OB_SUCCESS == tmp_ret) {
        if (OB_SUCCESS != (tmp_ret = resolve_max_submit_timestamp_(buf, buf_len, new_pos))) {
          CLOG_LOG(INFO, "resolve_max_submit_timestamp_ failed", K(tmp_ret));
        }
      }
    }

    pos = new_pos;
  }

  // reclaime memory
  if (OB_NOT_NULL(part_array)) {
    ob_free(part_array);
    part_array = NULL;
  }
  return ret;
}

// if the number of partition is zero, part_array can be empty
int ObIndexInfoBlockHandler::resolve_max_log_timestamp_(const char* buf, const int64_t buf_len, int64_t& pos,
    const common::ObPartitionKey* part_array, const int64_t part_number)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  int64_t new_partition_num = 0;
  int64_t new_pos = pos;
  const int64_t expect_magic_num = MAGIC_NUMBER_FOR_MAX_LOG_TIMESTAMP;

  if (OB_UNLIKELY(part_number > 0 && NULL == part_array)) {
    CLOG_LOG(WARN, "invalid argument", K(part_number), K(part_array));
    ret = OB_INVALID_ARGUMENT;
  } else if (new_pos >= buf_len) {
    CLOG_LOG(WARN, "max log timestamp is not recorded in ilog info block", K(buf_len), K(pos), K(buf));
    ret = OB_SUCCESS;
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &magic_num))) {
    CLOG_LOG(WARN, "decode magic number fail", K(ret), K(buf), K(buf_len), K(new_pos));
  }
  // check magic number
  else if (OB_UNLIKELY(magic_num != expect_magic_num)) {
    CLOG_LOG(INFO, "magic number not match", K(magic_num), K(expect_magic_num));
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &new_partition_num))) {
    CLOG_LOG(WARN, "decode partition number failed", K(ret), K(buf), K(buf_len), K(new_pos));
  }
  // check the number of partition
  else if (OB_UNLIKELY(new_partition_num != part_number)) {
    CLOG_LOG(WARN, "partition number does not match", K(ret), K(new_partition_num), K(part_number));
    ret = OB_INVALID_DATA;
  } else {
    // decode max log timestamp
    for (int64_t i = 0; i < new_partition_num && OB_SUCC(ret); ++i) {
      int64_t max_log_timestamp = 0;
      InfoEntry entry;
      const ObPartitionKey& pkey = part_array[i];

      if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &max_log_timestamp))) {
        CLOG_LOG(WARN, "decode max log timestamp fail", K(ret), K(i), K(new_partition_num), K(buf_len), K(new_pos));
      } else if (OB_FAIL(info_hash_.get(pkey, entry))) {
        CLOG_LOG(WARN, "get info entry from info hash fail", K(ret), K(pkey));
      } else {
        // update max log timestamp
        entry.max_log_timestamp_ = max_log_timestamp;

        if (OB_FAIL(info_hash_.update(pkey, entry))) {
          CLOG_LOG(WARN, "update info entry fail", K(ret), K(pkey), K(entry));
        }
      }
    }
  }

  pos = new_pos;

  return ret;
}

int ObIndexInfoBlockHandler::reset()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else {
    have_info_hash_v2_ = false;
    info_hash_.reset();
    info_hash_v2_.reset();
  }
  return ret;
}

int ObIndexInfoBlockHandler::update_info(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else if (OB_UNLIKELY(!partition_key.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == log_id) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == submit_timestamp) || OB_UNLIKELY(0 == submit_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(partition_key), K(log_id), K(submit_timestamp));
  } else {
    InfoEntry entry;

    if (OB_SUCC(info_hash_.get(partition_key, entry))) {
      if (log_id <= entry.min_log_id_) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "update ilog info block with smaller log id", K(partition_key), K(log_id), K(entry.min_log_id_));
      } else if (OB_INVALID_TIMESTAMP == entry.max_log_timestamp_ || entry.max_log_timestamp_ < submit_timestamp) {
        // update max log timestamp
        entry.max_log_timestamp_ = submit_timestamp;

        // update info_hash
        if (OB_FAIL(info_hash_.update(partition_key, entry))) {
          CLOG_LOG(WARN, "update info hash fail", K(ret), K(partition_key), K(entry));
        }
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      entry.min_log_id_ = log_id;
      entry.min_log_timestamp_ = submit_timestamp;
      entry.max_log_timestamp_ = submit_timestamp;
      if (OB_FAIL(info_hash_.insert(partition_key, entry))) {
        CLOG_LOG(WARN, "info_hash_.insert fail", K(ret), K(partition_key), K(entry));
      }
    } else {
      CLOG_LOG(WARN, "info_hash get fail", K(ret), K(partition_key), K(entry));
    }

    CLOG_LOG(TRACE, "update ilog info block", K(ret), K(partition_key), K(log_id), K(submit_timestamp), K(entry));

    if (OB_SUCC(ret)) {
      ret = update_info_hash_v2_(partition_key, log_id);
    }
    if (submit_timestamp > max_submit_timestamp_) {
      max_submit_timestamp_ = submit_timestamp;
    }
  }
  if (OB_FAIL(ret)) {
    CLOG_LOG(ERROR, "update_info failed", K(ret), K(partition_key), K(log_id), K(submit_timestamp));
  }
  return ret;
}

int ObIndexInfoBlockHandler::update_info(const int64_t max_submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else if (OB_INVALID_TIMESTAMP == max_submit_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(max_submit_timestamp));
  } else if (max_submit_timestamp > max_submit_timestamp_) {
    max_submit_timestamp_ = max_submit_timestamp;
  }
  return ret;
}

int ObIndexInfoBlockHandler::get_min_log_id(const common::ObPartitionKey& partition_key, uint64_t& min_log_id)
{
  int ret = OB_SUCCESS;
  min_log_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else {
    int64_t submit_timestamp = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(get_min_log_id(partition_key, min_log_id, submit_timestamp)) && OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "get_min_log_id failed", K(ret), K(partition_key), K(min_log_id));
    }
  }
  return ret;
}

int ObIndexInfoBlockHandler::get_min_log_id(
    const common::ObPartitionKey& partition_key, uint64_t& min_log_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;
  min_log_id = OB_INVALID_ID;
  submit_timestamp = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObIndexInfoBlockHandler is not inited", K(ret));
  } else {
    InfoEntry entry;
    entry.reset();
    if (OB_SUCC(info_hash_.get(partition_key, entry))) {
      min_log_id = entry.min_log_id_;
      submit_timestamp = entry.min_log_timestamp_;
    }
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(ERROR, "info_hash get failed", K(partition_key), K(ret));
    }
  }
  return ret;
}

int ObIInfoBlockHandler::can_skip_based_on_submit_timestamp(const int64_t min_submit_timestamp, bool& can_skip)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TIMESTAMP == min_submit_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(min_submit_timestamp));
  } else if (OB_INVALID_TIMESTAMP == max_submit_timestamp_) {
    can_skip = false;
    ret = OB_SUCCESS;
    CLOG_LOG(INFO,
        "invalid max_submit_timestamp, cannot skip this file",
        K(ret),
        K(min_submit_timestamp),
        K(max_submit_timestamp_));
  } else {
    can_skip = (min_submit_timestamp > max_submit_timestamp_);
  }

  return ret;
}

int ObIInfoBlockHandler::record_need_freeze_partition(
    NeedFreezePartitionArray& partition_array, const common::ObPartitionKey& partition_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (!partition_key.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else {
    NeedFreezePartition partition;
    partition.set_partition_key(partition_key);
    partition.set_log_id(log_id);
    if (OB_FAIL(partition_array.push_back(partition))) {
      CLOG_LOG(WARN, "partition_array push_back failed", K(ret), K(partition_key), K(log_id));
    }
  }
  return ret;
}

int ObIInfoBlockHandler::get_min_submit_timestamp(
    storage::ObPartitionService* partition_service, int64_t& min_submit_timestamp)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionGroupIterator* partition_iter = NULL;
  ObPartitionKey partition_key;

  if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(partition_service));
  } else if (NULL == (partition_iter = partition_service->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "partition mgr alloc scan iter failed");
  }
  int64_t min_ts = INT64_MAX;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(partition_iter->get_next(partition)) || NULL == partition) {
      // do nothing
      if (OB_ITER_END != ret) {
        CLOG_LOG(WARN, "partition_iter get_next failed", K(ret), K(partition));
      }
    } else if (!partition->is_valid()) {
      CLOG_LOG(WARN,
          "partition is invalid while scanning",
          "partition key",
          partition->get_partition_key(),
          "is_valid",
          partition->is_valid(),
          "state",
          partition->get_partition_state());
    } else {
      uint64_t unused = 0;
      int64_t submit_timestamp = OB_INVALID_TIMESTAMP;
      partition_key = partition->get_partition_key();
      if (OB_FAIL(partition->get_saved_last_log_info(unused, submit_timestamp))) {
        CLOG_LOG(WARN, "partition get static submit_timestamp error", K(ret), K(partition_key));
      } else if (OB_INVALID_TIMESTAMP == submit_timestamp) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "partition last submit timestamp is invalid", K(ret), K(partition_key), K(submit_timestamp));
      } else if (min_ts > submit_timestamp) {
        min_ts = submit_timestamp;
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          CLOG_LOG(INFO, "get_sstore_submit_timestamp", K(partition_key), K(ret), K(submit_timestamp), K(min_ts));
        }
      }
    }
  }
  if (NULL != partition_iter) {
    partition_service->revert_pg_iter(partition_iter);
  }
  if (OB_ITER_END == ret) {
    min_submit_timestamp = min_ts;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIInfoBlockHandler::get_max_submit_timestamp(int64_t& max_submit_timestamp) const
{
  int ret = OB_SUCCESS;
  max_submit_timestamp = max_submit_timestamp_;
  return ret;
}

int ObIInfoBlockHandler::build_max_submit_timestamp_(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || buf_len < 0 || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, MAGIC_NUMBER_FOR_MAX_SUBMIT_TIMESTAMP))) {
    CLOG_LOG(ERROR, "serialization failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, max_submit_timestamp_))) {
    CLOG_LOG(ERROR, "serialization failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  CLOG_LOG(TRACE, "build_max_submit_timestamp_ finished", K(ret), K(max_submit_timestamp_));
  return ret;
}

int ObIInfoBlockHandler::resolve_max_submit_timestamp_(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t magic_num = 0;
  int64_t submit_timestamp = 0;
  const int64_t expected_magic_number = MAGIC_NUMBER_FOR_MAX_SUBMIT_TIMESTAMP;

  if (NULL == buf || buf_len < 0 || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &magic_num))) {
    CLOG_LOG(WARN, "deserialization failed", K(ret), K(buf), K(buf_len), K(new_pos));
  } else if (magic_num != expected_magic_number) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(INFO, "magic number not match", K(magic_num), "expected_magic_number", expected_magic_number);
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, new_pos, &submit_timestamp))) {
    CLOG_LOG(WARN, "deserialization failed", K(ret), K(buf), K(buf_len), K(new_pos));
  } else {
    max_submit_timestamp_ = submit_timestamp;
    pos = new_pos;
  }
  CLOG_LOG(TRACE, "resolve_max_submit_timestamp_ finished", K(ret), K(max_submit_timestamp_));
  return ret;
}

ObIInfoBlockHandler::CheckFileCanBeSkippedFunctor::CheckFileCanBeSkippedFunctor(
    storage::ObPartitionService* partition_service)
    : partition_service_(partition_service),
      check_partition_not_exist_time_(OB_INVALID_TIMESTAMP),
      can_skip_(false),
      ret_code_(OB_SUCCESS)
{}

bool ObIInfoBlockHandler::CheckFileCanBeSkippedFunctor::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntryV2& entry_v2)
{
  int ret = OB_ERR_UNEXPECTED;
  uint64_t last_replay_log_id = OB_INVALID_ID;
  int64_t unused = 0;
  ObIPartitionGroupGuard partition_guard;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* pls = NULL;
  if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "partitio_key is invalid", K(ret), K(partition_key));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "partition_service_ is nullptr", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key, partition_guard))) {
    // partition has been garbage collect, but the corresponding file still exists
    if (ret != OB_PARTITION_NOT_EXIST) {
      CLOG_LOG(WARN, "get_partition failed", K(ret), K(partition_key));
    } else {
      ret = OB_SUCCESS;
      if (partition_reach_time_interval(10 * 1000, check_partition_not_exist_time_)) {
        CLOG_LOG(INFO, "partition may be gc, but clog file still exist", K(ret), K(partition_key));
      }
    }
  } else if (OB_ISNULL(partition = partition_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_partition_group failed", K(ret), K(partition_key));
  } else if (OB_ISNULL(pls = partition->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret), K(partition_key), K(pls));
  } else if (OB_FAIL(partition->get_saved_last_log_info(last_replay_log_id, unused))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "partition get_last_replay_log_id failed", K(ret), K(partition_key));
  } else if (OB_INVALID_ID == last_replay_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "last_replay_log_id is invalid", K(ret), K(partition_key), K(last_replay_log_id));
  } else {
    ret = OB_SUCCESS;
    if (last_replay_log_id >= entry_v2.max_log_id_) {
      can_skip_ = true;
    } else {
      can_skip_ = false;
    }
  }
  ret_code_ = ret;
  // when any partition does not meet the criteria, exit the loop
  return OB_SUCCESS == ret && can_skip_ == true;
}

int ObIInfoBlockHandler::can_skip_based_on_log_id(storage::ObPartitionService* partition_service, bool& can_skip)
{
  int ret = OB_SUCCESS;
  if (false == have_info_hash_v2_) {
    can_skip = false;
  } else {
    if (OB_ISNULL(partition_service)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid partition_service", K(ret), K(partition_service));
    } else {
      CheckFileCanBeSkippedFunctor functor(partition_service);
      if (OB_FAIL(info_hash_v2_.for_each(functor))) {
        // get real error code
        ret = functor.get_ret_code();
        CLOG_LOG(WARN, "traverse info_hash_v2_ failed", K(ret));
      } else {
        can_skip = functor.can_skip();
      }
    }
  }
  return ret;
}

int ObIInfoBlockHandler::can_skip_based_on_log_id(storage::ObPartitionService* partition_service,
    NeedFreezePartitionArray& partition_array, const bool need_record, bool& can_skip)
{
  int ret = OB_SUCCESS;
  bool is_strict_recycle_mode = false;
  is_strict_recycle_mode = ObServerConfig::get_instance()._ob_enable_log_replica_strict_recycle_mode;
  if (false == have_info_hash_v2_) {
    can_skip = false;
  } else {
    if (OB_ISNULL(partition_service)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      CheckPartitionNeedFreezeFunctor functor(partition_service, partition_array, need_record, is_strict_recycle_mode);
      if (OB_FAIL(info_hash_v2_.for_each(functor))) {
        // get real error code
        ret = functor.get_ret_code();
        CLOG_LOG(ERROR, "traverse info_hash_v2_ failed", K(ret));
      } else {
        can_skip = functor.can_skip();
      }
    }
  }
  return ret;
}

ObIInfoBlockHandler::CheckPartitionNeedFreezeFunctor::CheckPartitionNeedFreezeFunctor(
    ObPartitionService* partition_service, NeedFreezePartitionArray& partition_array, const bool need_record,
    const bool is_strict_recycle_mode)
    : partition_service_(partition_service),
      partition_array_(partition_array),
      check_partition_not_exist_time_(OB_INVALID_TIMESTAMP),
      ret_code_(OB_SUCCESS),
      need_record_(need_record),
      is_strict_recycle_mode_(is_strict_recycle_mode),
      can_skip_(true),
      is_log_print_(false)
{}

bool ObIInfoBlockHandler::CheckPartitionNeedFreezeFunctor::operator()(
    const common::ObPartitionKey& partition_key, const InfoEntryV2& entry_v2)
{
  int ret = OB_SUCCESS;
  uint64_t last_replay_log_id = OB_INVALID_ID;
  int64_t unused = 0;
  ObIPartitionGroupGuard partition_guard;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* pls = NULL;
  ObReplicaType replica_type = REPLICA_TYPE_MAX;
  if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "partitio_key is invalid", K(ret), K(partition_key));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "partition_service_ is nullptr", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key, partition_guard))) {
    // partition has been garbage collect, but the corresponding file still exists.
    if (ret != OB_PARTITION_NOT_EXIST) {
      CLOG_LOG(WARN, "get_partition failed", K(ret), K(partition_key));
    } else {
      ret = OB_SUCCESS;
      if (partition_reach_time_interval(10 * 1000, check_partition_not_exist_time_)) {
        CLOG_LOG(INFO, "partition may be gc, but clog file still exist", K(ret), K(partition_key));
      }
    }
  } else if (OB_ISNULL(partition = partition_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_partition_group failed", K(ret), K(partition_key));
  } else if (!partition->is_valid() || OB_ISNULL(pls = partition->get_log_service())) {
    CLOG_LOG(WARN,
        "get_log_service failed",
        K(ret),
        K(partition_key),
        "is_valid",
        partition->is_valid(),
        "state",
        partition->get_partition_state());
  } else if (REPLICA_TYPE_MAX == (replica_type = pls->get_replica_type())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid replica_type", K(ret), K(partition_key), K(pls));
  } else if (OB_FAIL(partition->get_saved_last_log_info(last_replay_log_id, unused))) {
    CLOG_LOG(ERROR, "partition get_last_replay_log_id failed", K(ret), K(partition_key));
  } else if (OB_INVALID_ID == last_replay_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "last_replay_log_id is invalid", K(ret), K(last_replay_log_id));
  } else if (OB_FAIL(do_check_partition_need_freeze_(
                 partition, pls, partition_key, replica_type, last_replay_log_id, entry_v2.max_log_id_))) {
    CLOG_LOG(ERROR,
        "do_check_partition_need_freeze_ failed",
        K(ret),
        K(partition_key),
        K(replica_type),
        K(last_replay_log_id),
        K(entry_v2.max_log_id_));
  }
  ret_code_ = ret;
  return ret == OB_SUCCESS;
}

int ObIInfoBlockHandler::CheckPartitionNeedFreezeFunctor::do_check_partition_need_freeze_(
    storage::ObIPartitionGroup* partition, ObIPartitionLogService* pls, const common::ObPartitionKey& partition_key,
    const ObReplicaType replica_type, const int64_t last_replay_log_id, const int64_t max_log_id)
{
  int ret = OB_SUCCESS;
  if (!partition->is_valid() || OB_ISNULL(pls)) {
    CLOG_LOG(WARN,
        "do_can_skip_based_on_log_id_ invalid argument",
        K(ret),
        K(partition->is_valid()),
        K(pls),
        K(partition_key),
        K(last_replay_log_id),
        K(max_log_id));
  } else if (last_replay_log_id == OB_INVALID_ID || max_log_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "last_replay_log_id or max_log_id is invalid",
        K(ret),
        K(partition_key),
        K(last_replay_log_id),
        K(max_log_id));
  } else if (REPLICA_TYPE_LOGONLY != replica_type) {
    if (last_replay_log_id < max_log_id) {
      can_skip_ = false;
      if (!is_log_print_) {
        CLOG_LOG(INFO,
            "can_skip_based_on_log_id failed because of last_replay_log_id",
            K(partition_key),
            K(last_replay_log_id),
            K(max_log_id),
            K(replica_type),
            K(need_record_));
        is_log_print_ = true;
      }
      if (need_record_) {
        (void)record_need_freeze_partition(partition_array_, partition_key, max_log_id);
      }
    } else if (REPLICA_TYPE_FULL == replica_type) {
      if (OB_FAIL(
              do_check_full_partition_need_freeze_(partition, pls, partition_key, last_replay_log_id, max_log_id)) &&
          OB_EAGAIN != ret) {
        CLOG_LOG(ERROR,
            "do_check_full_partition_need_freeze_ failed",
            K(ret),
            K(partition_key),
            K(last_replay_log_id),
            K(max_log_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
    }
  } else if (REPLICA_TYPE_LOGONLY == replica_type) {
    if (OB_FAIL(do_check_log_only_partition_need_freeze_(pls, partition_key, last_replay_log_id, max_log_id))) {
      CLOG_LOG(ERROR,
          "do_check_log_only_partition_need_freeze_ failed",
          K(ret),
          K(partition_key),
          K(last_replay_log_id),
          K(max_log_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid replica type", K(ret), K(partition_key), K(replica_type));
  }
  return ret;
}

int ObIInfoBlockHandler::CheckPartitionNeedFreezeFunctor::do_check_full_partition_need_freeze_(
    storage::ObIPartitionGroup* partition, ObIPartitionLogService* pls, const common::ObPartitionKey& partition_key,
    const int64_t last_replay_log_id, const int64_t max_log_id)
{
  int ret = OB_SUCCESS;
  if (!partition->is_valid() || OB_ISNULL(pls) || !partition_key.is_valid() || OB_INVALID_ID == last_replay_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR,
        "do_check_full_partition_need_freeze_ invalid argument",
        K(ret),
        K(pls),
        K(partition_key),
        K(last_replay_log_id),
        K(max_log_id));
  } else if ((OB_SYS_TENANT_ID != partition_key.get_tenant_id()) && ObServerConfig::get_instance().enable_log_archive &&
             ObServerConfig::get_instance().backup_log_archive_option.is_mandatory() &&
             (!partition->get_pg_storage().is_restore())) {
    // TODO: only limit log collect for full replica, the case of read only replica revert
    // to full replica, and then do it together
    ObLogArchiveBackupInfo info;
    uint64_t last_archived_log_id = OB_INVALID_ID;
    if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
      CLOG_LOG(WARN, "failed to get_log_archive_backup_info", K(partition_key), KR(ret));
      // five states of archive
      // 1. BEGINING and DOING, means that archive has started, log file can be reclaimed need limit by
      // last_archived_log_id
      // 2. INVALID, means that archive may be had started or stopped, cann't reclaime the log file, need
      // wati next round
      // 3. STOPING and STOPED, means that archive has stopped
    } else if (ObLogArchiveStatus::STATUS::BEGINNING == info.status_.status_ ||
               ObLogArchiveStatus::STATUS::DOING == info.status_.status_) {
      if (OB_FAIL(
              pls->get_last_archived_log_id(info.status_.incarnation_, info.status_.round_, last_archived_log_id))) {
        CLOG_LOG(WARN, "failed to get_log_archive_backup_info", K(partition_key), K(info), KR(ret));
      } else if (OB_INVALID_ID == last_archived_log_id || last_archived_log_id < max_log_id) {
        can_skip_ = false;
        if (!is_log_print_) {
          CLOG_LOG(INFO,
              "can_skip_based_on_log_id failed because of last_archive_log_id",
              K(partition_key),
              K(last_replay_log_id),
              K(last_archived_log_id),
              K(info),
              K(max_log_id),
              K(need_record_));
          is_log_print_ = true;
        }
      } else {
      }
    } else if (ObLogArchiveStatus::STATUS::INVALID == info.status_.status_) {
      // if can't get ObLogArchiveStatus, closing archive by set configuration can
      // solve the problem of log file can't be reclaimed.
      can_skip_ = false;
      CLOG_LOG(INFO, "archive has enabled, may acquire stale inner table info", K(partition_key));
    } else {
    }
    if (OB_FAIL(ret)) {
      can_skip_ = false;
      CLOG_LOG(INFO,
          "can_skip_based_on_log_id failed because of last_archive_log_id",
          KR(ret),
          K(partition_key),
          K(last_replay_log_id),
          K(last_archived_log_id),
          K(max_log_id),
          K(need_record_));
      is_log_print_ = true;
    }
  } else {
  }
  return ret;
}

// description: for log only replica, can't reclaime log file when last_replay_log_id is greater than
//              max_log_id.
//
// reasons: log only replica doesn't have baseline data, when change 3f+2l to 1f+2l, data may be loose.
// assume ABC are full replicas, DE are log only replicas, ABDE set last_replay_log_id as 100, however,
// C's last_replay_log_id is 50. meanwhile, AB offline, DE's log have been recalimed, C becomes leader,
// logs between 50-100 will lose.
//
// solution: limit log collection for log replicas. when need reclaime logs on log only replica, should
// determine whether the min_confirmed_log_id of majority full replicas, is greater than or equal to
// max_log_id in the log file which need to be reclaimed.
int ObIInfoBlockHandler::CheckPartitionNeedFreezeFunctor::do_check_log_only_partition_need_freeze_(
    ObIPartitionLogService* pls, const common::ObPartitionKey& partition_key, const int64_t last_replay_log_id,
    const int64_t max_log_id)
{
  int ret = OB_SUCCESS;
  uint64_t recycable_log_id = 0;
  bool is_offline = false;
  if (OB_ISNULL(pls) || !partition_key.is_valid() || OB_INVALID_ID == last_replay_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR,
        "do_check_log_only_partition_need_freeze_ invalid argument",
        K(ret),
        K(pls),
        K(partition_key),
        K(last_replay_log_id),
        K(max_log_id));
  } else if (OB_FAIL(pls->is_offline(is_offline))) {
    CLOG_LOG(ERROR, "is_offline failed", K(ret));
  } else if (OB_FAIL(pls->get_recyclable_log_id(recycable_log_id))) {
    CLOG_LOG(ERROR, "get_recyclable_log_id failed", K(ret), K(partition_key), K(last_replay_log_id), K(max_log_id));
  } else if (last_replay_log_id >= max_log_id &&
             (is_offline || !is_strict_recycle_mode_ || recycable_log_id >= max_log_id)) {
    // do nothing
  } else {
    can_skip_ = false;
    if (!is_log_print_) {
      CLOG_LOG(INFO,
          "do_check_log_only_partition_need_freeze_ false",
          K(partition_key),
          K(last_replay_log_id),
          K(max_log_id),
          K(recycable_log_id),
          K(need_record_));
      is_log_print_ = true;
    }
    if (last_replay_log_id < max_log_id && need_record_) {
      (void)record_need_freeze_partition(partition_array_, partition_key, max_log_id);
    }
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
