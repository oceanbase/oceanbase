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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_partition_scanner.h"
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_block_sstable_struct.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "share/backup/ob_backup_struct.h"
#include <string.h>

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{
using namespace common;
using namespace share;
using namespace blocksstable;

/**
 * ObTableLoadBackupPartScanner::ObLogTsRange
 */
OB_SERIALIZE_MEMBER(
    ObTableLoadBackupPartScanner::ObLogTsRange,
    start_log_ts_,
    end_log_ts_,
    max_log_ts_);

ObTableLoadBackupPartScanner::ObLogTsRange::ObLogTsRange()
  : start_log_ts_(0),
    end_log_ts_(0),
    max_log_ts_(0)
{
}

bool ObTableLoadBackupPartScanner::ObLogTsRange::is_valid() const
{
  return end_log_ts_ >= start_log_ts_ && max_log_ts_ >= end_log_ts_;
}

void ObTableLoadBackupPartScanner::ObLogTsRange::reset()
{
  start_log_ts_ = 0;
  end_log_ts_ = 0;
  max_log_ts_ = 0;
}

/**
 * ObTableLoadBackupPartScanner::ObPartitionKey
 */
DEFINE_SERIALIZE(ObTableLoadBackupPartScanner::ObPartitionKey)
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

DEFINE_DESERIALIZE(ObTableLoadBackupPartScanner::ObPartitionKey)
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

DEFINE_GET_SERIALIZE_SIZE(ObTableLoadBackupPartScanner::ObPartitionKey)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(table_id_);
  size += serialization::encoded_length_i32(part_id_);
  size += serialization::encoded_length_i32(assit_id_);
  return size;
}

ObTableLoadBackupPartScanner::ObPartitionKey::ObPartitionKey()
  : table_id_(OB_INVALID_ID),
    part_id_(OB_INVALID_INDEX),
    assit_id_(OB_INVALID_INDEX)
{
}

bool ObTableLoadBackupPartScanner::ObPartitionKey::is_valid() const
{
  return OB_INVALID_ID != table_id_ && part_id_ >= 0 && assit_id_ >= 0;
}

void ObTableLoadBackupPartScanner::ObPartitionKey::reset()
{
  table_id_ = OB_INVALID_ID;
  part_id_ = OB_INVALID_INDEX;
  assit_id_ = OB_INVALID_INDEX;
}

/**
 * ObTableLoadBackupPartScanner::ObTableKey
 */
OB_SERIALIZE_MEMBER(
    ObTableLoadBackupPartScanner::ObTableKey,
    table_type_,
    pkey_,
    table_id_,
    trans_version_range_,
    version_,
    log_ts_range_);

ObTableLoadBackupPartScanner::ObTableKey::ObTableKey()
  : table_type_(ObTableType::MAX_TABLE_TYPE),
    pkey_(),
    table_id_(common::OB_INVALID_ID),
    trans_version_range_(),
    version_(),
    log_ts_range_()
{
}

bool ObTableLoadBackupPartScanner::ObTableKey::is_valid() const
{
  return table_type_ != ObTableType::MAX_TABLE_TYPE || !pkey_.is_valid() ||
         common::OB_INVALID_ID == table_id_ || trans_version_range_.is_valid() ||
         !version_.is_valid();
}

void ObTableLoadBackupPartScanner::ObTableKey::reset()
{
  table_type_ = ObTableType::MAX_TABLE_TYPE;
  pkey_.reset();
  table_id_ = common::OB_INVALID_ID;
  trans_version_range_.reset();
  version_.reset();
  log_ts_range_.reset();
}

/**
 * ObTableLoadBackupPartScanner::ObTableKeyInfo
 */
OB_SERIALIZE_MEMBER(
    ObTableLoadBackupPartScanner::ObTableKeyInfo,
    table_key_,
    total_macro_block_count_);

ObTableLoadBackupPartScanner::ObTableKeyInfo::ObTableKeyInfo()
  : table_key_(),
    total_macro_block_count_(0)
{
}

bool ObTableLoadBackupPartScanner::ObTableKeyInfo::is_valid() const
{
  return table_key_.is_valid() && total_macro_block_count_ >= 0;
}

void ObTableLoadBackupPartScanner::ObTableKeyInfo::reset()
{
  table_key_.reset();
  total_macro_block_count_ = 0;
}

/**
 * ObTableLoadBackupPartScanner::ObTableMacroIndex
 */
OB_SERIALIZE_MEMBER(
    ObTableLoadBackupPartScanner::ObTableMacroIndex,
    sstable_macro_index_,
    data_version_,
    data_seq_,
    backup_set_id_,
    sub_task_id_,
    offset_,
    data_length_);

ObTableLoadBackupPartScanner::ObTableMacroIndex::ObTableMacroIndex()
  : sstable_macro_index_(0),
    data_version_(0),
    data_seq_(0),
    backup_set_id_(0),
    sub_task_id_(0),
    offset_(0),
    data_length_(0)
{
}

void ObTableLoadBackupPartScanner::ObTableMacroIndex::reset()
{
  sstable_macro_index_ = 0;
  data_version_ = 0;
  data_seq_ = 0;
  backup_set_id_ = 0;
  sub_task_id_ = 0;
  offset_ = 0;
  data_length_ = 0;
}

bool ObTableLoadBackupPartScanner::ObTableMacroIndex::is_valid() const
{
  return sstable_macro_index_ >= 0 && data_version_ >= 0 && data_seq_ >= 0 && backup_set_id_ > 0 &&
         sub_task_id_ >= 0 && offset_ >= 0 && data_length_ > 0;
}

/**
 * ObTableLoadBackupPartScanner
 */
ObTableLoadBackupPartScanner::ObTableLoadBackupPartScanner()
  : allocator_("TLD_BPS"),
    macro_block_min_skip_idx_(INT64_MAX),
    buf_(nullptr),
    cur_idx_(-1),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  macro_block_index_.set_tenant_id(MTL_ID());
  lob_col_buf_.set_tenant_id(MTL_ID());
  lob_col_buf_size_.set_tenant_id(MTL_ID());
}

int ObTableLoadBackupPartScanner::init(
    const ObSchemaInfo &schema_info,
    const ObBackupStorageInfo &storage_info,
    const ObString &data_path,
    const ObString &backup_set_id,
    int64_t backup_table_id,
    int64_t subpart_count,
    int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_UNLIKELY(
      !storage_info.is_valid() || data_path.empty() || backup_set_id.empty() || backup_table_id <= 0 ||
      subpart_count <= 0 || subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(storage_info), K(data_path), K(backup_set_id), K(backup_table_id),
        K(subpart_count), K(subpart_idx));
  } else {
    const int64_t bucket_num = 1024;
    backup_table_id_ = backup_table_id;
    if (OB_FAIL(schema_info_.assign(schema_info))) {
      LOG_WARN("fail to assign schema_info_", KR(ret), K(schema_info));
    } else if (OB_FAIL(storage_info_.assign(storage_info))) {
      LOG_WARN("fail to assign storage_info_", KR(ret));
    } else if (OB_FAIL(macro_block_idx_map_.create(bucket_num, "TLD_MacroMap", "TLD_MacroMap", MTL_ID()))) {
      LOG_WARN("fail to create macro_block_idx_map_", KR(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, data_path, data_path_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(data_path));
    } else if (OB_FAIL(ob_write_string(allocator_, backup_set_id, backup_set_id_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(backup_set_id));
    } else if (OB_FAIL(parse_macro_block_index_file(subpart_count, subpart_idx))) {
      LOG_WARN("fail to parse macro block index file", KR(ret));
    } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(SSTABLE_BLOCK_BUF_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_ISNULL(lob_buf_ = static_cast<char*>(allocator_.alloc(SSTABLE_BLOCK_BUF_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schema_info_.column_desc_.count(); i++) {
        char *empty_buf = nullptr;
        if (OB_FAIL(lob_col_buf_.push_back(empty_buf))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(lob_col_buf_size_.push_back(0))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObTableLoadBackupPartScanner::reset()
{
  int ret = OB_SUCCESS;
  schema_info_.reset();
  storage_info_.reset();
  data_path_.reset();
  backup_set_id_.reset();
  macro_block_index_.reset();
  macro_block_idx_map_.destroy();
  macro_block_min_skip_idx_ = INT64_MAX;
  for (int64_t i = 0; i < lob_col_buf_.count(); i++) {
    if (lob_col_buf_[i] != nullptr) {
      ob_free(lob_col_buf_[i]);
      lob_col_buf_[i] = nullptr;
    }
  }
  lob_col_buf_.reset();
  lob_col_buf_size_.reset();
  if (buf_ != nullptr)  {
    allocator_.free(buf_);
    buf_ = nullptr;
  }
  if (lob_buf_ != nullptr)  {
    allocator_.free(lob_buf_);
    lob_buf_ = nullptr;
  }
  cur_idx_ = -1;
  allocator_.reset();
  is_inited_ = false;
}

int ObTableLoadBackupPartScanner::parse_macro_block_index_file(
    const int64_t subpart_count,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  int64_t file_length = 0;
  if (OB_FAIL(databuff_printf(
      buf, OB_MAX_URI_LENGTH, pos, "%.*smacro_block_index_%.*s",
      data_path_.length(), data_path_.ptr(),
      backup_set_id_.length(), backup_set_id_.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(buf));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::get_file_length(
      ObString(pos, buf), &storage_info_, file_length))) {
    LOG_WARN("fail to get_file_length", KR(ret), K(file_length), K(ObString(pos, buf)));
  } else if (file_length > 0) {
    char *file_buf = nullptr;
    int64_t read_size = 0;
    if (OB_ISNULL(file_buf = static_cast<char*>(allocator_.alloc(file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(file_length));
    } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(
        ObString(pos, buf), &storage_info_, file_buf, file_length, read_size))) {
      LOG_WARN("fail to read single file", KR(ret), K(file_length), K(ObString(pos, buf)));
    } else {
      ObBufferReader buffer_reader(file_buf, file_length);
      if (OB_FAIL(init_macro_block_index(buffer_reader, subpart_count, subpart_idx))) {
        LOG_WARN("fail to init macro block index", KR(ret), K(subpart_count), K(subpart_idx));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::init_macro_block_index(
    ObBufferReader &buffer_reader,
    const int64_t subpart_count,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  const ObBackupCommonHeader *common_header = nullptr;
  ObTableKeyInfo table_key_info;
  ObTableMacroIndex macro_index;
  int64_t start_block_idx = 0;
  int64_t end_block_idx = 0;
  int64_t cur_block_idx = 0;
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
      // do nothing
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
            LOG_WARN("failed to read backup table key meta", KR(ret), K(cur_block_idx), K(*common_header), K(table_key_info));
          } else {
            cur_block_idx = 0;
            table_key_info_is_inited = true;
            if (table_key_info.table_key_.table_id_ == backup_table_id_) {
              table_key_info_is_matched = true;
              if (FALSE_IT(locate_subpart_macro_block(
                table_key_info.total_macro_block_count_, subpart_count, subpart_idx, start_block_idx, end_block_idx))) {
              }
            } else {
              end_block_idx = table_key_info.total_macro_block_count_;
            }
          }
        }
        while (OB_SUCC(ret) && buffer_reader.pos() < end_pos && cur_block_idx < end_block_idx) {
          macro_index.reset();
          if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
            LOG_WARN("read macro index fail", KR(ret), K(buffer_reader), K(macro_index));
          } else if (OB_UNLIKELY(!macro_index.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("macro index is invalid", KR(ret), K(macro_index));
          } else {
            if (table_key_info_is_matched) {
              if (cur_block_idx >= start_block_idx) {
                if (OB_FAIL(macro_block_index_.push_back(macro_index))) {
                  LOG_WARN("fail to push back", KR(ret), K(macro_index));
                } else if (OB_FAIL(macro_block_idx_map_.set_refactored(macro_index.data_seq_, cur_block_idx, 1))) {
                  LOG_WARN("fail to set refactored", KR(ret), K(cur_block_idx), K(macro_index));
                }
              }
            }
            cur_block_idx++;
          }
        }
        if (OB_SUCC(ret)) {
          if (cur_block_idx == end_block_idx) {
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
  LOG_INFO("init macro block index", KR(ret), K(subpart_count), K(subpart_idx), K(start_block_idx),
      K(end_block_idx), K(table_key_info), K(macro_block_index_.count()));
  return ret;
}

int ObTableLoadBackupPartScanner::locate_subpart_macro_block(
    const int64_t total_macro_block_count,
    const int64_t subpart_count,
    const int64_t subpart_idx,
    int64_t &start_block_idx,
    int64_t &end_block_idx)
{
  int ret = OB_SUCCESS;
  start_block_idx = 0;
  end_block_idx = 0;
  if (total_macro_block_count > 0) {
    int64_t valid_subpart_count = subpart_count;
    if (total_macro_block_count < subpart_count * MIN_SUBPART_MACRO_BLOCK_COUNT) {
      // 宏块数目太少, 分不出subpart_count份, 重新计算能分出几份
      valid_subpart_count =  MAX(total_macro_block_count / MIN_SUBPART_MACRO_BLOCK_COUNT, 1);
    }
    const int64_t count_per_subpart = total_macro_block_count / valid_subpart_count;
    const int64_t remain_count = total_macro_block_count - count_per_subpart * valid_subpart_count;
    // 比如16个宏块分成5份: 4 3 3 3 3
    // count_per_subpart = 3
    // remain_count = 1
    // [0, 4) [4, 7) [7, 10) [10, 13) [13, 16)
    if (subpart_idx < valid_subpart_count) {
      if (subpart_idx < remain_count) {
        start_block_idx = subpart_idx * (count_per_subpart + 1);
        end_block_idx = start_block_idx + (count_per_subpart + 1);
      } else {
        start_block_idx = subpart_idx * count_per_subpart + remain_count;
        end_block_idx = start_block_idx + count_per_subpart;
      }
    } else {
      // invalid subpart, do nothing
    }
  } else {
    // emprt partition, do nothing
  }
  return ret;
}

int ObTableLoadBackupPartScanner::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (macro_block_index_.count() == 0) {
    ret = OB_ITER_END;
  } else {
    if (cur_idx_ == -1) {
      if (OB_FAIL(switch_next_macro_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to switch_next_macro_block", KR(ret), K(cur_idx_));
        }
      } else {
        ret = sstable_block_scanner_.get_next_row(row);
      }
    } else {
      if (OB_FAIL(sstable_block_scanner_.get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_macro_block())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch_next_macro_block", KR(ret));
            }
          } else {
            ret = sstable_block_scanner_.get_next_row(row);
          }
        }
      }
    }
    if (OB_SUCC(ret) && lob_col_buf_.count() > 0) {
      if (OB_FAIL(read_lob_data(row))) {
        LOG_WARN("fail to get lob data", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::switch_next_macro_block()
{
  int ret = OB_SUCCESS;
  if (cur_idx_ == macro_block_index_.count()) {
    ret = OB_ITER_END;
  } else if (cur_idx_ == -1) {
    cur_idx_ = 0;
  } else {
    if (cur_idx_ < macro_block_index_.count()) {
      ++cur_idx_;
    }
    if (cur_idx_ >= macro_block_index_.count()) {
      ret = OB_ITER_END;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (cur_idx_ == macro_block_min_skip_idx_) {
    ret = OB_ITER_END;
  } else {
    int64_t read_size = 0;
    if (OB_FAIL(read_macro_block_data(cur_idx_, buf_, read_size))) {
      LOG_WARN("fail to read macro block data", KR(ret), K(cur_idx_));
    } else {
      sstable_block_scanner_.reset();
      if (OB_FAIL(sstable_block_scanner_.init(buf_, read_size, schema_info_))) {
        LOG_WARN("fail to init sstable block scanner", KR(ret), K(macro_block_index_[cur_idx_]));
      } else if (sstable_block_scanner_.is_lob_block()) {
        // 遍历到lob宏块，提前退出
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::read_lob_data(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row->count_; i++) {
    ObObj &col = row->get_cell(i);
    if (col.is_lob()) {
      if (col.is_outrow()) {
        const ObBackupLobData *lob_data = reinterpret_cast<const ObBackupLobData *>(col.get_string_ptr());
        const int64_t direct_block_cnt = MIN(lob_data->get_direct_cnt(), static_cast<int64_t>(lob_data->idx_cnt_));
        int64_t pos = 0;
        if (lob_col_buf_[i] == nullptr || lob_col_buf_size_[i] < lob_data->byte_size_) {
          if (lob_col_buf_[i] != nullptr) {
            ob_free(lob_col_buf_[i]);
          }
          if (OB_ISNULL(lob_col_buf_[i] = static_cast<char *>(ob_malloc(lob_data->byte_size_, ObMemAttr(MTL_ID(), "TLD_BLob32x"))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            lob_col_buf_size_[i] = lob_data->byte_size_;
          }
        }
        // 读取直接索引，微块存储的是lob数据
        for (int64_t j = 0; OB_SUCC(ret) && j < direct_block_cnt; j++) {
          const ObBackupLobIndex &lob_index = lob_data->lob_idx_[j];
          if (OB_FAIL(fill_lob_buf(lob_index, lob_col_buf_[i], lob_col_buf_size_[i], pos))) {
            LOG_WARN("fail to fill lob buf", KR(ret), K(i), K(j));
          }
        }
        // 读取一层间接索引，微块存储的是lob_index
        char *indirect_index_buf = nullptr;
        for (int64_t j = direct_block_cnt; OB_SUCC(ret) && j < lob_data->idx_cnt_; j++) {
          const ObBackupLobIndex &lob_index = lob_data->lob_idx_[j];
          int64_t indirect_index_pos = 0;
          if (OB_ISNULL(indirect_index_buf) && OB_ISNULL(indirect_index_buf = static_cast<char *>(ob_malloc(SSTABLE_BLOCK_BUF_SIZE, ObMemAttr(MTL_ID(), "TLD_BLob32x"))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else if (OB_FAIL(fill_lob_buf(lob_index, indirect_index_buf, SSTABLE_BLOCK_BUF_SIZE, indirect_index_pos))) {
            LOG_WARN("fail to fill lob buf", KR(ret));
          } else {
            const ObBackupLobIndex *indirect_lob_index = nullptr;
            int64_t cur_pos = 0;
            while (OB_SUCC(ret) && cur_pos < indirect_index_pos) {
              if (OB_UNLIKELY(cur_pos + sizeof(ObBackupLobIndex)) > indirect_index_pos) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected second index buf", KR(ret), K(cur_pos), K(indirect_index_pos), K(sizeof(ObBackupLobIndex)));
              } else {
                indirect_lob_index = reinterpret_cast<const ObBackupLobIndex *>(indirect_index_buf + cur_pos);
                if (OB_FAIL(fill_lob_buf(*indirect_lob_index, lob_col_buf_[i], lob_col_buf_size_[i], pos))) {
                  LOG_WARN("fail to fill lob buf", KR(ret));
                } else {
                  cur_pos += sizeof(ObBackupLobIndex);
                }
              }
            }
          }
        }
        if (indirect_index_buf != nullptr) {
          ob_free(indirect_index_buf);
          indirect_index_buf = nullptr;
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(pos != lob_data->byte_size_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", KR(ret), K(lob_data->byte_size_), K(pos));
          } else {
            col.set_varchar(lob_col_buf_[i], pos);
          }
        }
      } else {
        col.set_varchar(col.v_.string_, col.val_len_);
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::fill_lob_buf(
    const ObBackupLobIndex &lob_index,
    char *&buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObBackupLogicMacroBlockId &macro_block_id = lob_index.logic_macro_id_;
  int64_t macro_block_idx;
  int64_t read_size = 0;
  ObTableLoadBackupSSTableBlockReader lob_reader;
  if (OB_FAIL(macro_block_idx_map_.get_refactored(macro_block_id.data_seq_, macro_block_idx))) {
    LOG_WARN("fail to get refactored", KR(ret), K(macro_block_id));
  } else if (OB_FAIL(read_macro_block_data(macro_block_idx, lob_buf_, read_size))) {
    LOG_WARN("fail to read macro block data", KR(ret), K(macro_block_idx));
  } else if (OB_FAIL(lob_reader.init(lob_buf_, read_size, schema_info_))) {
    LOG_WARN("fail to init sstable block reader", KR(ret), K(macro_block_index_[macro_block_idx]));
  } else {
    macro_block_min_skip_idx_ = MIN(macro_block_min_skip_idx_, macro_block_idx);
    const ObMicroBlockData *micro_block_data = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(lob_reader.get_next_micro_block(micro_block_data))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("fail to get next micro block", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        int64_t data_size = micro_block_data->get_buf_size() - sizeof(ObLobMicroBlockHeader);
        if (OB_UNLIKELY(buf_size - pos < data_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected buf size", KR(ret), K(pos), K(buf_size), K(data_size));
        } else {
          std::memcpy(buf + pos, micro_block_data->get_buf() + sizeof(ObLobMicroBlockHeader), data_size);
          pos += data_size;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::read_macro_block_data(int64_t block_idx, char *&data_buf, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  if (OB_FAIL(databuff_printf(
      buf, OB_MAX_URI_LENGTH, pos, "%.*smacro_block_%.*s.%ld",
      data_path_.length(), data_path_.ptr(),
      backup_set_id_.length(), backup_set_id_.ptr(),
      macro_block_index_[block_idx].sub_task_id_))) {
    LOG_WARN("fail to fill buf", KR(ret));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_part_file(
      ObString(pos, buf),
      &storage_info_,
      data_buf,
      SSTABLE_BLOCK_BUF_SIZE,
      macro_block_index_[block_idx].offset_,
      read_size))) {
    LOG_WARN("fail to read_single_file", KR(ret), K(ObString(pos, buf)));
  }
  return ret;
}

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
