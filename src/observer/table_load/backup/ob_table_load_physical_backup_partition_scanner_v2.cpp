/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/ob_table_load_physical_backup_partition_scanner_v2.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;
using namespace blocksstable;

/**
 * ObTableLoadPhysicalBackupPartScannerV2::ObLogTsRange
 */
OB_SERIALIZE_MEMBER(ObTableLoadPhysicalBackupPartScannerV2::ObLogTsRange,
                    start_log_ts_,
                    end_log_ts_,
                    max_log_ts_);

ObTableLoadPhysicalBackupPartScannerV2::ObLogTsRange::ObLogTsRange()
  : start_log_ts_(0),
    end_log_ts_(0),
    max_log_ts_(0)
{
}

bool ObTableLoadPhysicalBackupPartScannerV2::ObLogTsRange::is_valid() const
{
  return end_log_ts_ >= start_log_ts_ && max_log_ts_ >= end_log_ts_;
}

void ObTableLoadPhysicalBackupPartScannerV2::ObLogTsRange::reset()
{
  start_log_ts_ = 0;
  end_log_ts_ = 0;
  max_log_ts_ = 0;
}

/**
 * ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey
 */
DEFINE_SERIALIZE(ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, table_id_))) {
    COMMON_LOG(WARN, "serialize table_id_ failed, ", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf,
                                               buf_len,
                                               pos,
                                               part_id_))) {
    COMMON_LOG(WARN, "serialize part_id failed, ", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf,
                                               buf_len,
                                               pos,
                                               assit_id_))) {
    COMMON_LOG(WARN, "serialize sub_part_id failed, ", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos,
          reinterpret_cast<int64_t *>(&table_id_)))) {
    COMMON_LOG(WARN, "deserialize table_id failed, ", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &part_id_))) {
    COMMON_LOG(WARN, "deserialize part_id failed, ", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &assit_id_))) {
    COMMON_LOG(WARN, "deserialize sub_part_id failed, ", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(table_id_);
  size += serialization::encoded_length_i32(part_id_);
  size += serialization::encoded_length_i32(assit_id_);
  return size;
}

ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey::ObPartitionKey()
  : table_id_(OB_INVALID_ID),
    part_id_(OB_INVALID_INDEX),
    assit_id_(OB_INVALID_INDEX)
{
}

bool ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey::is_valid() const
{
  return OB_INVALID_ID != table_id_ && part_id_ >= 0 && assit_id_ >= 0;
}

void ObTableLoadPhysicalBackupPartScannerV2::ObPartitionKey::reset()
{
  table_id_ = OB_INVALID_ID;
  part_id_ = OB_INVALID_INDEX;
  assit_id_ = OB_INVALID_INDEX;
}

/**
 * ObTableLoadPhysicalBackupPartScannerV2::ObTableKey
 */
OB_SERIALIZE_MEMBER(ObTableLoadPhysicalBackupPartScannerV2::ObTableKey,
                    table_type_,
                    pkey_,
                    table_id_,
                    trans_version_range_,
                    version_,
                    log_ts_range_);

ObTableLoadPhysicalBackupPartScannerV2::ObTableKey::ObTableKey()
  : table_type_(ObTableType::MAX_TABLE_TYPE),
    pkey_(),
    table_id_(common::OB_INVALID_ID),
    trans_version_range_(),
    version_(),
    log_ts_range_()
{
}

bool ObTableLoadPhysicalBackupPartScannerV2::ObTableKey::is_valid() const
{
  return table_type_ != ObTableType::MAX_TABLE_TYPE || !pkey_.is_valid() ||
         common::OB_INVALID_ID == table_id_ || trans_version_range_.is_valid() ||
         !version_.is_valid();
}

void ObTableLoadPhysicalBackupPartScannerV2::ObTableKey::reset()
{
  table_type_ = ObTableType::MAX_TABLE_TYPE;
  pkey_.reset();
  table_id_ = common::OB_INVALID_ID;
  trans_version_range_.reset();
  version_.reset();
  log_ts_range_.reset();
}

/**
 * ObTableLoadPhysicalBackupPartScannerV2::ObTableKeyInfo
 */
OB_SERIALIZE_MEMBER(ObTableLoadPhysicalBackupPartScannerV2::ObTableKeyInfo,
                    table_key_,
                    total_macro_block_count_);

ObTableLoadPhysicalBackupPartScannerV2::ObTableKeyInfo::ObTableKeyInfo()
  : table_key_(),
    total_macro_block_count_(0)
{
}

bool ObTableLoadPhysicalBackupPartScannerV2::ObTableKeyInfo::is_valid() const
{
  return table_key_.is_valid() && total_macro_block_count_ >= 0;
}

void ObTableLoadPhysicalBackupPartScannerV2::ObTableKeyInfo::reset()
{
  table_key_.reset();
  total_macro_block_count_ = 0;
}

/**
 * ObTableLoadPhysicalBackupPartScannerV2::ObTableMacroIndex
 */
OB_SERIALIZE_MEMBER(ObTableLoadPhysicalBackupPartScannerV2::ObTableMacroIndex,
                    sstable_macro_index_,
                    data_version_,
                    data_seq_,
                    backup_set_id_,
                    sub_task_id_,
                    offset_,
                    data_length_);

ObTableLoadPhysicalBackupPartScannerV2::ObTableMacroIndex::ObTableMacroIndex()
  : sstable_macro_index_(0),
    data_version_(0),
    data_seq_(0),
    backup_set_id_(0),
    sub_task_id_(0),
    offset_(0),
    data_length_(0)
{
}

void ObTableLoadPhysicalBackupPartScannerV2::ObTableMacroIndex::reset()
{
  sstable_macro_index_ = 0;
  data_version_ = 0;
  data_seq_ = 0;
  backup_set_id_ = 0;
  sub_task_id_ = 0;
  offset_ = 0;
  data_length_ = 0;
}

bool ObTableLoadPhysicalBackupPartScannerV2::ObTableMacroIndex::is_valid() const
{
  return sstable_macro_index_ >= 0 && data_version_ >= 0 && data_seq_ >= 0 && backup_set_id_ > 0 &&
         sub_task_id_ >= 0 && offset_ >= 0 && data_length_ > 0;
}

/**
 * ObTableLoadPhysicalBackupPartScannerV2
 */
ObTableLoadPhysicalBackupPartScannerV2::ObTableLoadPhysicalBackupPartScannerV2()
{
  data_macro_block_index_.set_tenant_id(MTL_ID());
  lob_macro_block_index_.set_tenant_id(MTL_ID());
}

ObTableLoadPhysicalBackupPartScannerV2::~ObTableLoadPhysicalBackupPartScannerV2()
{
  reset();
}

void ObTableLoadPhysicalBackupPartScannerV2::reset()
{
  data_path_.reset();
  backup_set_id_.reset();
  data_macro_block_index_.reset();
  lob_macro_block_index_.reset();
}

int ObTableLoadPhysicalBackupPartScannerV2::init(
    const ObTableLoadBackupVersion &backup_version,
    const ObBackupStorageInfo &storage_info,
    const ObSchemaInfo &schema_info,
    const ObString &data_path,
    const ObString &backup_set_id,
    const int64_t backup_table_id,
    const int64_t subpart_count,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_UNLIKELY(!storage_info.is_valid() || data_path.empty() || backup_set_id.empty() || backup_table_id <= 0 ||
                         subpart_count <= 0 || subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(storage_info), K(data_path), K(backup_set_id),
        K(backup_table_id), K(subpart_count), K(subpart_idx));
  } else {
    backup_table_id_ = backup_table_id;
    if (OB_FAIL(ob_write_string(allocator_, data_path, data_path_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(data_path));
    } else if (OB_FAIL(ob_write_string(allocator_, backup_set_id, backup_set_id_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(backup_set_id));
    } else if (OB_FAIL(inner_init(backup_version, storage_info, schema_info, subpart_count, subpart_idx))) {
      LOG_WARN("fail to inner init", KR(ret), K(backup_set_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadPhysicalBackupPartScannerV2::init_macro_block_index(
    const int64_t subpart_count,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  int64_t file_length = 0;
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*smacro_block_index_%.*s",
                              data_path_.length(),
                              data_path_.ptr(),
                              backup_set_id_.length(),
                              backup_set_id_.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(buf));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::get_file_length(ObString(pos, buf), &storage_info_, file_length))) {
    LOG_WARN("fail to get_file_length", KR(ret), K(file_length), K(ObString(pos, buf)));
  } else if (file_length > 0) {
    char *file_buf = nullptr;
    int64_t read_size = 0;
    if (OB_ISNULL(file_buf = static_cast<char*>(allocator_.alloc(file_length + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(file_length));
    } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf), &storage_info_, file_buf, file_length, read_size))) {
      LOG_WARN("fail to read single file", KR(ret), K(file_length), K(ObString(pos, buf)));
    } else {
      file_buf[read_size] = '\0';
      ObBufferReader buffer_reader(file_buf, file_length);
      const ObBackupCommonHeader *common_header = nullptr;
      ObTableKeyInfo table_key_info;
      ObTableMacroIndex macro_index;
      int64_t cur_block_idx = 0;
      int64_t total_macro_block_count = 0;
      bool table_key_info_is_inited = false;
      bool table_key_info_is_matched = false;
      bool macro_block_index_is_inited = false;
      while (OB_SUCC(ret) && buffer_reader.remain() > 0 && !macro_block_index_is_inited) {
        common_header = nullptr;
        if (OB_FAIL(buffer_reader.get(common_header))) {
          LOG_WARN("read macro index common header fail", KR(ret));
        } else if (OB_ISNULL(common_header)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("macro index common header is null", KR(ret));
        } else if (OB_FAIL(common_header->check_valid())) {
          LOG_WARN("common_header is not vaild", KR(ret), K(*common_header));
        } else if (common_header->data_type_ == BACKUP_FILE_END_MARK) {
          break;
        } else if (OB_UNLIKELY(common_header->data_length_ > buffer_reader.remain())) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buffer_reader not enough", KR(ret), K(*common_header), K(buffer_reader));
        } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
          LOG_WARN("common header data checksum fail", KR(ret), K(*common_header));
        } else {
          int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
          while (OB_SUCC(ret) && buffer_reader.pos() < end_pos) {
            if (!table_key_info_is_inited) {
              if (OB_FAIL(buffer_reader.read_serialize(table_key_info))) {
                LOG_WARN("failed to read backup table key meta", KR(ret), K(*common_header), K(table_key_info));
              } else {
                cur_block_idx = 0;
                table_key_info_is_inited = true;
                total_macro_block_count = table_key_info.total_macro_block_count_;
                if (table_key_info.table_key_.table_id_ == backup_table_id_) {
                  table_key_info_is_matched = true;
                }
              }
            }
            while (OB_SUCC(ret) && buffer_reader.pos() < end_pos && cur_block_idx < total_macro_block_count) {
              macro_index.reset();
              if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
                LOG_WARN("read macro index fail", KR(ret), K(buffer_reader), K(macro_index));
              } else if (OB_UNLIKELY(!macro_index.is_valid())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("macro index is invalid", KR(ret), K(macro_index));
              } else {
                if (table_key_info_is_matched) {
                  if (is_lob_block(macro_index.data_seq_)) {
                    if (OB_FAIL(lob_macro_block_idx_map_.set_refactored(macro_index.data_seq_, lob_macro_block_index_.size(), 1))) {
                      LOG_WARN("fail to set refactored", KR(ret));
                    } else if (OB_FAIL(lob_macro_block_index_.push_back(macro_index))) {
                      LOG_WARN("fail to push back", KR(ret));
                    }
                  } else {
                    if (OB_FAIL(data_macro_block_index_.push_back(macro_index))) {
                      LOG_WARN("fail to push back", KR(ret));
                    }
                  }
                }
                cur_block_idx++;
              }
            }
            if (OB_SUCC(ret)) {
              if (cur_block_idx == total_macro_block_count) {
                if (table_key_info_is_matched) {
                  macro_block_index_is_inited = true;
                  break;
                } else {
                  table_key_info.reset();
                  table_key_info_is_inited = false;
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (common_header->align_length_ > 0) {
              if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
                LOG_WARN("buffer_reader buf not enough", K(ret), K(*common_header));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(locate_subpart_macro_block(data_macro_block_index_.count(), subpart_count, subpart_idx))) {
        LOG_WARN("fail to locate subpart macro block", KR(ret), K(data_macro_block_index_.count()), K(subpart_count), K(subpart_idx));
      } else {
        LOG_INFO("init macro block index result", KR(ret), K(block_idx_), K(block_start_idx_), K(block_end_idx_), K(data_macro_block_index_.count()), K(lob_macro_block_index_.count()));
      }
    }
  }
  return ret;
}

int ObTableLoadPhysicalBackupPartScannerV2::read_macro_block_data(
    const int64_t block_idx,
    const bool is_lob_block,
    char *&data_buf,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  const int64_t sub_task_id = is_lob_block ? lob_macro_block_index_[block_idx].sub_task_id_ : data_macro_block_index_[block_idx].sub_task_id_;
  const int64_t offset = is_lob_block ? lob_macro_block_index_[block_idx].offset_ : data_macro_block_index_[block_idx].offset_;
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*smacro_block_%.*s.%ld",
                              data_path_.length(),
                              data_path_.ptr(),
                              backup_set_id_.length(),
                              backup_set_id_.ptr(),
                              sub_task_id))) {
    LOG_WARN("fail to fill buf", KR(ret));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_part_file(ObString(pos, buf),
                                                               &storage_info_,
                                                               data_buf,
                                                               SSTABLE_BLOCK_BUF_SIZE,
                                                               offset,
                                                               read_size))) {
    LOG_WARN("fail to read_single_file", KR(ret), K(ObString(pos, buf)));
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
