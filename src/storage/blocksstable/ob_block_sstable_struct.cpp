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

#define USING_LOG_PREFIX STORAGE
#include "ob_block_sstable_struct.h"
#include "common/cell/ob_cell_writer.h"
#include "common/log/ob_log_entry.h"
#include "common/row/ob_row.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/utility/serialization.h"
#include "lib/utility/utility.h"
#include "share/scn.h"
#include "ob_block_manager.h"
#include "ob_data_buffer.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace storage;
namespace blocksstable
{

const char *BLOCK_SSTBALE_DIR_NAME = "sstable";
const char *BLOCK_SSTBALE_FILE_NAME = "block_file";

const bool ObMicroBlockEncoderOpt::ENCODINGS_DEFAULT[ObColumnHeader::MAX_TYPE] = {true, true, true, true, true, true, true, true, true, true};
const bool ObMicroBlockEncoderOpt::ENCODINGS_NONE[ObColumnHeader::MAX_TYPE] = {false, false, false, false, false, false, false, false, false, false};
const bool ObMicroBlockEncoderOpt::ENCODINGS_FOR_PERFORMANCE[ObColumnHeader::MAX_TYPE] = {true, true, false, true, false, false, false, false, false, false};

//================================ObStorageEnv======================================
bool ObStorageEnv::is_valid() const
{
  return NULL != data_dir_
      && default_block_size_ > 0
      && log_spec_.is_valid()
      && NULL != clog_dir_
      && index_block_cache_priority_ > 0
      && user_block_cache_priority_ > 0
      && user_row_cache_priority_ > 0
      && fuse_row_cache_priority_ > 0
      && bf_cache_priority_ > 0
      && tablet_ls_cache_priority_ > 0
      && ethernet_speed_ > 0;
}

ObMicroBlockId::ObMicroBlockId()
  : macro_id_(), offset_(0), size_(0)
{
}

ObMicroBlockId::ObMicroBlockId(
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size)
    : macro_id_(macro_id),
      offset_(offset),
      size_(size)
{
}

bool ObMicroBlockEncodingCtx::is_valid() const
{
  return macro_block_size_ > 0 && micro_block_size_ > 0 && rowkey_column_cnt_ > 0
      && column_cnt_ >= rowkey_column_cnt_ && NULL != col_descs_
      && encoder_opt_.is_valid() && major_working_cluster_version_ >= 0
      && (ENCODING_ROW_STORE == row_store_type_ || SELECTIVE_ENCODING_ROW_STORE == row_store_type_);
}

//======================ObPreviousEncodingArray========================
template<int64_t max_size>
int ObPreviousEncodingArray<max_size>::put(const ObPreviousEncoding &prev)
{
  int ret = OB_SUCCESS;
  if (0 == size_ || prev != prev_encodings_[last_pos_]) {
    if (max_size == size_) {
      int64_t pos = contain(prev);
      if (-1 == pos) {
        last_pos_ = (last_pos_ == max_size - 1) ? 0 : last_pos_ + 1;
        prev_encodings_[last_pos_] = prev;
      } else {
        last_pos_ = pos;
      }
    } else if (max_size > size_) {
      int64_t pos = contain(prev);
      if (-1 == pos) {
        last_pos_ = size_;
        prev_encodings_[last_pos_] = prev;
        ++size_;
      } else {
        last_pos_ = pos;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected size", K_(size));
    }
  }
  return ret;
}

template<int64_t max_size>
int64_t ObPreviousEncodingArray<max_size>::contain(const ObPreviousEncoding &prev)
{
  int64_t ret = -1;
  for (int64_t i = 0; i < size_; ++i) {
    if (prev_encodings_[i] == prev) {
      ret = i;
      break;
    }
  }
  return ret;
}

ObMacroBlockSchemaInfo::ObMacroBlockSchemaInfo()
  : column_number_(0), rowkey_column_number_(0), schema_version_(0), schema_rowkey_col_cnt_(0), compressor_(nullptr),
    column_id_array_(nullptr), column_type_array_(nullptr), column_order_array_(nullptr)
{
}

bool ObMacroBlockSchemaInfo::operator==(const ObMacroBlockSchemaInfo &other) const
{
  bool bret = false;
  bret = column_number_ == other.column_number_ && rowkey_column_number_ == other.rowkey_column_number_
      && schema_version_ == other.schema_version_ && schema_rowkey_col_cnt_ == other.schema_rowkey_col_cnt_;

  if (nullptr == compressor_ && nullptr == other.compressor_) {
  } else if (nullptr != compressor_ && nullptr != other.compressor_) {
    bret = bret && (0 == STRCMP(compressor_, other.compressor_));
  } else {
    bret = false;
  }

  if (nullptr == column_id_array_ && nullptr == other.column_id_array_) {
  } else if (nullptr != column_id_array_ && nullptr != other.column_id_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_id_array_[i] == other.column_id_array_[i];
    }
  } else {
    bret = false;
  }

  if (nullptr == column_type_array_ && nullptr == other.column_type_array_) {
  } else if (nullptr != column_type_array_ && nullptr != other.column_type_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_type_array_[i] == other.column_type_array_[i];
    }
  } else {
    bret = false;
  }

  if (nullptr == column_order_array_ && nullptr == other.column_order_array_) {
  } else if (nullptr != column_order_array_ && nullptr != other.column_order_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_order_array_[i] == other.column_order_array_[i];
    }
  } else {
    bret = false;
  }

  return bret;
}

int ObMacroBlockSchemaInfo::serialize(char *buf, int64_t data_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObBufferWriter buffer_writer(buf, data_len, pos);
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = MACRO_BLOCK_SCHEMA_INFO_HEADER_VERSION;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_writer.write(header_size))) {
    LOG_WARN("serialization header_size error.", K(header_size), K(ret),
                K(buffer_writer.capacity()), K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(header_version))) {
    LOG_WARN("serialization header_version error.", K(header_version), K(ret),
                K(buffer_writer.capacity()), K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(column_number_))) {
    LOG_WARN("fail to write column number", K(ret));
  } else if (OB_FAIL(buffer_writer.write(rowkey_column_number_))) {
    LOG_WARN("fail to write rowkey column number", K(ret));
  } else if (OB_FAIL(buffer_writer.write(schema_version_))) {
    LOG_WARN("fail to write schema version", K(ret));
  } else if (OB_FAIL(buffer_writer.write(schema_rowkey_col_cnt_))) {
    LOG_WARN("fail to write schema rowkey column cnt", K(ret));
  } else if (OB_FAIL(buffer_writer.append_fmt("%s", compressor_))) {
    LOG_WARN("fail to append fmt", K(ret));
  } else if (column_number_ > 0) {
    for (int32_t i = 0;
        i < column_number_ && nullptr != column_id_array_ && OB_SUCC(ret);
        ++i) {
      if (OB_FAIL(buffer_writer.write(column_id_array_[i]))) {
        LOG_WARN("serialization column_id_array_ error.", K(column_id_array_[i]),
            K(buffer_writer.capacity()), K(buffer_writer.pos()), K(ret));
      }
    }
    for (int32_t i = 0;
        i < column_number_ && nullptr != column_type_array_ && OB_SUCC(ret);
        ++i) {
      if (OB_FAIL(buffer_writer.write(column_type_array_[i]))) {
        LOG_WARN("serialization column_type_array_ error.", K(column_type_array_[i]),
            K(buffer_writer.capacity()), K(buffer_writer.pos()), K(ret));
      }
    }

    for (int32_t i = 0;
        i < column_number_ && nullptr != column_order_array_ && OB_SUCC(ret);
        ++i) {
      if (OB_FAIL(buffer_writer.write(column_order_array_[i]))) {
        LOG_WARN("serialization column_order_array_ error.", K(column_order_array_[i]),
            K(buffer_writer.capacity()), K(buffer_writer.pos()), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int32_t size = static_cast<int32_t>(buffer_writer.pos() - start_pos);
    char *size_ptr = buffer_writer.data() + start_pos;
    *reinterpret_cast<int32_t *>(size_ptr) = size;
    pos = buffer_writer.pos();
  }

  return ret;
}

int ObMacroBlockSchemaInfo::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("fail to read header size", K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("fail to read header version", K(ret));
  } else if (OB_FAIL(buffer_reader.read(column_number_))) {
    LOG_WARN("fail to read column number", K(ret));
  } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
    LOG_WARN("fail to read rowkey column number", K(ret));
  } else if (OB_FAIL(buffer_reader.read(schema_version_))) {
    LOG_WARN("fail to read schema version", K(ret));
  } else if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
    LOG_WARN("fail to read schema rowkey column count", K(ret));
  } else if (OB_FAIL(buffer_reader.read_cstr(compressor_))) {
    LOG_WARN("fail to deserialize compressor", K(ret), K_(compressor),
        K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (column_number_ > 0) {
    char *column_array = buffer_reader.current();
    const int64_t column_id_size = column_number_ * sizeof(uint16_t);
    const int64_t column_type_size = column_number_ * sizeof(ObObjMeta);
    const int64_t column_order_size = column_number_ * sizeof(ObOrderType);
    const int64_t total_array_size = column_id_size + column_type_size + column_order_size;
    column_id_array_ = reinterpret_cast<uint16_t *>(column_array);
    column_array += column_id_size;
    column_type_array_ = reinterpret_cast<ObObjMeta *>(column_array);
    column_array += column_type_size;
    column_order_array_ = reinterpret_cast<ObOrderType *>(column_array);
    if (OB_FAIL(buffer_reader.advance(total_array_size))) {
      LOG_WARN("fail to advance buffer reader", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos = buffer_reader.pos();
  }
  return ret;
}

int64_t ObMacroBlockSchemaInfo::get_serialize_size() const
{
  int64_t serialize_size = 0;
  int32_t header_size = 0;
  int32_t header_version = 0;
  serialize_size += sizeof(header_size);
  serialize_size += sizeof(header_version);
  serialize_size += sizeof(column_number_);
  serialize_size += sizeof(rowkey_column_number_);
  serialize_size += sizeof(schema_version_);
  serialize_size += sizeof(schema_rowkey_col_cnt_);
  if (nullptr != compressor_) {
    serialize_size += strlen(compressor_) + 1;
  }
  if (column_number_ > 0) {
    const int64_t column_id_size = column_number_ * sizeof(uint16_t);
    const int64_t column_type_size = column_number_ * sizeof(ObObjMeta);
    const int64_t column_order_size = column_number_ * sizeof(ObOrderType);
    const int64_t total_array_size = column_id_size + column_type_size + column_order_size;
    serialize_size += total_array_size;
  }
  return serialize_size;
}

int64_t ObMacroBlockSchemaInfo::get_deep_copy_size() const
{
  int64_t deep_copy_size = 0;
  deep_copy_size = sizeof(ObMacroBlockSchemaInfo);
  if (nullptr != compressor_) {
    deep_copy_size += strlen(compressor_) + 1;
  }
  if (column_number_ > 0) {
    const int64_t column_id_size = column_number_ * sizeof(uint16_t);
    const int64_t column_type_size = column_number_ * sizeof(ObObjMeta);
    const int64_t column_order_size = column_number_ * sizeof(ObOrderType);
    const int64_t total_array_size = column_id_size + column_type_size + column_order_size;
    deep_copy_size += total_array_size;
  }
  return deep_copy_size;
}

int ObMacroBlockSchemaInfo::deep_copy(ObMacroBlockSchemaInfo *&new_schema_info, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const int64_t size = get_deep_copy_size();
  new_schema_info = nullptr;
  char *buf = nullptr;
  int64_t pos = 0;
  int64_t item_size = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size), K(*this));
  } else {
    new_schema_info = new (buf) ObMacroBlockSchemaInfo();
    new_schema_info->column_number_ = column_number_;
    new_schema_info->rowkey_column_number_ = rowkey_column_number_;
    new_schema_info->schema_version_ = schema_version_;
    new_schema_info->schema_rowkey_col_cnt_ = schema_rowkey_col_cnt_;
    pos += sizeof(ObMacroBlockSchemaInfo);
    if (OB_SUCC(ret) && nullptr != compressor_) {
      item_size = strlen(compressor_) + 1;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.",
            K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->compressor_ = buf + pos;
        MEMCPY(new_schema_info->compressor_, compressor_, item_size - 1);
        new_schema_info->compressor_[item_size - 1] = '\0';
        pos += item_size;
      }
    }

    if (OB_SUCC(ret) && NULL != column_id_array_) {
      item_size = sizeof(uint16_t) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.",
                    K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->column_id_array_ = reinterpret_cast<uint16_t *>(buf + pos);
        MEMCPY(new_schema_info->column_id_array_, column_id_array_, item_size);
        pos += item_size;
      }
    }

    if (OB_SUCC(ret) && nullptr != column_type_array_) {
      item_size = sizeof(ObObjMeta) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.",
                    K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->column_type_array_ = reinterpret_cast<ObObjMeta *>(buf + pos);
        MEMCPY(new_schema_info->column_type_array_, column_type_array_, item_size);
        pos += item_size;
      }
    }

    if (OB_SUCC(ret) && NULL != column_order_array_) {
      item_size = sizeof(ObOrderType) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.",
                    K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->column_order_array_ = reinterpret_cast<ObOrderType *>(buf + pos);
        MEMCPY(new_schema_info->column_order_array_, column_order_array_, item_size);
        pos += item_size;
      }
    }
  }
  return ret;;
}

int64_t ObMacroBlockSchemaInfo::to_string(char *buf, const int64_t buf_len) const
{
 int64_t pos = 0;
  J_KV(
      K_(column_number),
      K_(rowkey_column_number),
      K_(schema_version),
      K_(schema_rowkey_col_cnt),
      K_(compressor)
  );
  J_COMMA();
  if (NULL != column_id_array_ && column_number_ > 0) {
    J_KV("column_id_array", ObArrayWrap<uint16_t>(column_id_array_, column_number_));
  }
  if (NULL != column_type_array_ && column_number_ > 0) {
    J_KV("column_type_array", ObArrayWrap<ObObjMeta>(column_type_array_, column_number_));
  }
  if (NULL != column_order_array_ && column_number_ > 0) {
    J_KV("column_order_array", ObArrayWrap<ObOrderType>(column_order_array_, column_number_));
  }
  return pos;
}


ObSSTableColumnMeta::ObSSTableColumnMeta()
  : column_id_(0),
    column_default_checksum_(0),
    column_checksum_(0)
{
}


ObSSTableColumnMeta::~ObSSTableColumnMeta()
{
}

bool ObSSTableColumnMeta::operator==(const ObSSTableColumnMeta &other) const
{
  return column_id_ == other.column_id_
        && column_default_checksum_ == other.column_default_checksum_
        && column_checksum_ == other.column_checksum_;
}

bool ObSSTableColumnMeta::operator!=(const ObSSTableColumnMeta &other) const
{
  return !(*this == other);
}

void ObSSTableColumnMeta::reset()
{
  column_id_ = 0;
  column_default_checksum_ = 0;
  column_checksum_ = 0;
}

OB_SERIALIZE_MEMBER(ObSSTableColumnMeta,
    column_id_,
    column_default_checksum_,
    column_checksum_);

int write_compact_rowkey(ObBufferWriter &buffer_writer,
                         const common::ObObj *endkey,
                         const int64_t count,
                         const ObRowStoreType row_store_type)
{
  UNUSEDx(buffer_writer, endkey, count, row_store_type);
  return OB_NOT_SUPPORTED;
}

int read_compact_rowkey(ObBufferReader &buffer_reader,
                        const common::ObObjMeta *column_type_array,
                        common::ObObj *endkey,
                        const int64_t count,
                        const ObRowStoreType row_store_type)
{
  UNUSEDx(buffer_reader, column_type_array, endkey, count, row_store_type);
  return OB_NOT_SUPPORTED;
}

OB_SERIALIZE_MEMBER(ObSSTablePair, data_version_, data_seq_);

ObSimpleMacroBlockInfo::ObSimpleMacroBlockInfo()
  : macro_id_(),
    last_access_time_(INT64_MAX),
    ref_cnt_(0)
{
}

void ObSimpleMacroBlockInfo::reset()
{
  macro_id_.reset();
  last_access_time_ = INT64_MAX;
  ref_cnt_ = 0;
}

ObMacroBlockMarkerStatus::ObMacroBlockMarkerStatus()
  : total_block_count_(0),
    reserved_block_count_(0),
    linked_block_count_(0),
    tmp_file_count_(0),
    data_block_count_(0),
    shared_data_block_count_(0),
    index_block_count_(0),
    ids_block_count_(0),
    disk_block_count_(0),
    bloomfiter_count_(0),
    hold_count_(0),
    pending_free_count_(0),
    free_count_(0),
    shared_meta_block_count_(0),
    mark_cost_time_(0),
    sweep_cost_time_(0),
    start_time_(0),
    last_end_time_(0),
    mark_finished_(false),
    hold_info_()
{
}

void ObMacroBlockMarkerStatus::reset()
{
  total_block_count_ = 0;
  reuse();
}

void ObMacroBlockMarkerStatus::fill_comment(char *buf, const int32_t buf_len) const
{
  const int64_t now = ObTimeUtility::current_time();
  if (NULL != buf && buf_len > 0) {
    if (!hold_info_.macro_id_.is_valid()) {
      buf[0] = '\0';
    } else {
      int64_t pos = 0;
      J_NAME("the earliest hold macro block info");
      J_COLON();
      BUF_PRINTO(hold_info_);
      J_COMMA();
      BUF_PRINTF("last access interval = %lds", (now - hold_info_.last_access_time_) / 1000 / 1000);
    }
  }
}

void ObMacroBlockMarkerStatus::reuse()
{
  reserved_block_count_ = 0;
  linked_block_count_ = 0;
  tmp_file_count_ = 0;
  data_block_count_ = 0;
  shared_data_block_count_ = 0;
  index_block_count_ = 0;
  ids_block_count_ = 0;
  disk_block_count_ = 0;
  bloomfiter_count_ = 0;
  hold_count_ = 0;
  pending_free_count_ = 0;
  free_count_ = 0;
  shared_meta_block_count_ = 0;
  mark_cost_time_ = 0;
  sweep_cost_time_ = 0;
  start_time_ = 0;
  last_end_time_ = 0;
  mark_finished_ = false;
  hold_info_.reset();
}

ObRecordHeaderV3::ObRecordHeaderV3()
  : magic_(0), header_length_(0), version_(0), header_checksum_(0), reserved16_(0),
    data_length_(0), data_zlength_(0), data_checksum_(0), data_encoding_length_(0),
    row_count_(0), column_cnt_(0), column_checksums_()
{
}

void ObRecordHeaderV3::set_header_checksum()
{
  int16_t checksum = 0;
  header_checksum_ = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  format_i64(data_encoding_length_, checksum);
  format_i64(row_count_, checksum);
  format_i64(column_cnt_, checksum);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    for (int64_t i = 0; i < column_cnt_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }
  header_checksum_ = checksum;
}

int ObRecordHeaderV3::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ header_checksum_;
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  format_i64(data_encoding_length_, checksum);
  format_i64(row_count_, checksum);
  format_i64(column_cnt_, checksum);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    for (int64_t i = 0; i < column_cnt_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }

  if (0 != checksum) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "record check checksum failed", K(ret), K(*this));
  }
  return ret;
}

int ObRecordHeaderV3::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || len < 0 || data_zlength_ != len
      || (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len), K(data_zlength_),
        K(data_length_), K(data_checksum_));
  } else {
    const int64_t data_checksum = ob_crc64_sse42(buf, len);
    if (data_checksum != data_checksum_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "checksum error", K(ret), K(data_checksum_), K(data_checksum));
    }
  }
  return ret;
}

int ObRecordHeaderV3::deserialize_and_check_record(
    const char *ptr, const int64_t size,
    const int16_t magic, const char *&payload_ptr, int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  ObRecordHeaderV3 header;
  int64_t pos = 0;
  if (NULL == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(size), K(magic));
  } else if (OB_FAIL(header.deserialize(ptr, size, pos))) {
    LOG_WARN("fail to deserialize header", K(ret));
  } else if (OB_FAIL(header.check_and_get_record(ptr, size, magic, payload_ptr, payload_size))) {
    LOG_WARN("fail to check and get record", K(ret));
  }

  return ret;
}

int ObRecordHeaderV3::check_and_get_record(const char *ptr, const int64_t size, const int16_t magic,
    const char *&payload_ptr, int64_t &payload_size) const
{
  int ret = OB_SUCCESS;
  if (nullptr == ptr || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr));
  } else if (magic != magic_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("record header magic is not match", K(ret), K(magic), K(magic_));
  } else if (OB_FAIL(check_header_checksum())) {
    LOG_WARN("fail to check header checksum", K(ret));
  } else {
    const int64_t header_size = get_serialize_size();
    if (size < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer not enough", K(ret), K(size), K(header_size));
    } else {
      payload_ptr = ptr + header_size;
      payload_size = size - header_size;
      if (OB_FAIL(check_payload_checksum(payload_ptr, payload_size))) {
        LOG_WARN("fail to check payload checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObRecordHeaderV3::check_record(const char *ptr, const int64_t size, const int16_t magic) const
{
  int ret = OB_SUCCESS;
  const char *payload_ptr = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(size), K(magic));
  } else if (OB_FAIL(check_and_get_record(ptr, size, magic, payload_ptr, payload_size))) {
    LOG_WARN("fail to check record", K(ret), KP(ptr), K(size), K(magic));
  }
  return ret;
}

int ObRecordHeaderV3::deserialize_and_check_record(const char *ptr, const int64_t size, const int16_t magic)
{
  int ret = OB_SUCCESS;
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(magic));
  } else if (OB_FAIL(deserialize_and_check_record(ptr, size, magic, payload_buf, payload_size))) {
    LOG_WARN("fail to check record", K(ret));
  }
  return ret;
}

int64_t ObRecordHeaderV3::get_serialize_size() const
{
  int64_t size = 0;
  size += sizeof(ObRecordCommonHeader);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    size += column_cnt_ * sizeof(int64_t);
  }
  return size;
}

int ObRecordHeaderV3::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    const int64_t serialize_size = get_serialize_size();
    int64_t pos_orig = pos;
    buf += pos_orig;
    pos = 0;
    if (serialize_size + pos_orig > buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer not enough", K(ret), K(serialize_size), K(buf_len), K(pos_orig));
    } else {
      ObRecordCommonHeader *common_header = reinterpret_cast<ObRecordCommonHeader *>(buf + pos);
      common_header->magic_ = magic_;
      common_header->header_length_ = header_length_;
      common_header->version_ = version_;
      common_header->header_checksum_ = header_checksum_;
      common_header->reserved16_ = reserved16_;
      common_header->data_length_ = data_length_;
      common_header->data_zlength_ = data_zlength_;
      common_header->data_checksum_ = data_checksum_;
      common_header->data_encoding_length_ = data_encoding_length_;
      common_header->row_count_ = row_count_;
      common_header->column_cnt_ = column_cnt_;
      pos += sizeof(ObRecordCommonHeader);
      if (RECORD_HEADER_VERSION_V3 == version_) {
        MEMCPY(buf + pos, column_checksums_, column_cnt_ * sizeof(int64_t));
        pos += column_cnt_ * sizeof(int64_t);
      }
    }
    pos += pos_orig;
  }
  return ret;
}

int ObRecordHeaderV3::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len < 8) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos_orig = pos;
    const ObRecordCommonHeader *common_header = reinterpret_cast<const ObRecordCommonHeader *>(buf);
    pos = 0;
    buf += pos_orig;
    magic_ = common_header->magic_;
    header_length_ = common_header->header_length_;
    version_ = common_header->version_;
    header_checksum_ = common_header->header_checksum_;
    reserved16_ = common_header->reserved16_;
    data_length_ = common_header->data_length_;
    data_zlength_ = common_header->data_zlength_;
    data_checksum_ = common_header->data_checksum_;
    data_encoding_length_ = common_header->data_encoding_length_;
    row_count_ = common_header->row_count_;
    column_cnt_ = common_header->column_cnt_;
    pos += sizeof(ObRecordCommonHeader);
    if (RECORD_HEADER_VERSION_V3 == version_) {
      const int64_t *column_checksums = nullptr;
      column_checksums = reinterpret_cast<const int64_t *>(buf + pos);
      column_checksums_ = const_cast<int64_t *>(column_checksums);
      pos += column_cnt_ * sizeof(int64_t);
    }
    pos += pos_orig;
  }
  return ret;
}

ObDDLMacroBlockRedoInfo::ObDDLMacroBlockRedoInfo()
  : table_key_(), data_buffer_(), block_type_(ObDDLMacroBlockType::DDL_MB_INVALID_TYPE), start_scn_(SCN::min_scn())
{
}

bool ObDDLMacroBlockRedoInfo::is_valid() const
{
  return table_key_.is_valid() && data_buffer_.ptr() != nullptr && block_type_ != ObDDLMacroBlockType::DDL_MB_INVALID_TYPE
         && logic_id_.is_valid() && start_scn_.is_valid_and_not_min();
}

OB_SERIALIZE_MEMBER(ObDDLMacroBlockRedoInfo, table_key_, data_buffer_, block_type_, logic_id_, start_scn_);

constexpr uint8_t ObColClusterInfoMask::BYTES_TYPE_TO_LEN[];

}
}
