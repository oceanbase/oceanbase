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
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"
#include "observer/table_load/backup/ob_table_load_backup_flat_row_reader_v1.h"
#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "common/row/ob_row.h"
#include "common/ob_record_header.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace blocksstable;

int read_compact_rowkey(const ObTableLoadBackupVersion &backup_version,
                        ObBufferReader &buffer_reader,
                        const common::ObObjMeta *column_type_array,
                        common::ObObj *endkey,
                        const int64_t count)
{
  int ret = OB_SUCCESS;
  common::ObNewRow row;
  row.cells_ = endkey;
  row.count_ = count;
  ObFlatRowReaderV1 reader;
  int64_t pos = buffer_reader.pos();
  common::ObArenaAllocator allocator("TLD_readrowkey");
  allocator.set_tenant_id(MTL_ID());
  if (OB_UNLIKELY(buffer_reader.data() == nullptr || buffer_reader.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(buffer_reader.data()), K(buffer_reader.length()));
  } else if (OB_UNLIKELY(column_type_array == nullptr || endkey == nullptr || count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(column_type_array), KP(endkey), K(count));
  } else if (OB_FAIL(reader.read_compact_rowkey(column_type_array,
                                                count,
                                                backup_version,
                                                allocator,
                                                buffer_reader.data(),
                                                buffer_reader.capacity(),
                                                pos,
                                                row))) {
    LOG_WARN("read compact rowkey failed", KR(ret));
  } else if (OB_FAIL(buffer_reader.set_pos(pos))) {
    LOG_WARN("set pos on buffer reader failed", KR(ret));
  }
  return ret;
}

/**
 * ObSchemaColumnInfo
 */
ObSchemaColumnInfo::ObSchemaColumnInfo()
  : partkey_idx_(-1),
    is_partkey_(false)
{
}

ObSchemaColumnInfo::ObSchemaColumnInfo(int64_t partkey_idx, bool is_partkey)
  : partkey_idx_(partkey_idx),
    is_partkey_(is_partkey)
{
}

void ObSchemaColumnInfo::reset()
{
  partkey_idx_ = -1;
  is_partkey_ = false;
}

int ObSchemaColumnInfo::assign(const ObSchemaColumnInfo &other)
{
  int ret = OB_SUCCESS;
  partkey_idx_ = other.partkey_idx_;
  is_partkey_ = other.is_partkey_;
  return ret;
}

/**
 * ObSchemaInfo
 */
ObSchemaInfo::ObSchemaInfo()
{
  column_desc_.set_tenant_id(MTL_ID());
  column_info_.set_tenant_id(MTL_ID());
  partkey_count_ = 0;
  is_heap_table_ = false;
}

void ObSchemaInfo::reset()
{
  column_desc_.reset();
  column_info_.reset();
  partkey_count_ = 0;
  is_heap_table_ = false;
}

int ObSchemaInfo::assign(const ObSchemaInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_desc_.assign(other.column_desc_))) {
    LOG_WARN("fail to assign", KR(ret), K(other));
  } else if (OB_FAIL(column_info_.assign(other.column_info_))) {
    LOG_WARN("fail to assign", KR(ret), K(other));
  } else {
    partkey_count_ = other.partkey_count_;
    is_heap_table_ = other.is_heap_table_;
  }
  return ret;
}

/**
 * ObBackupLogicMacroBlockId
 */
ObBackupLogicMacroBlockId::ObBackupLogicMacroBlockId()
  : data_seq_(0),
    data_version_(0)
{
}

ObBackupLogicMacroBlockId::ObBackupLogicMacroBlockId(const int64_t data_seq, const uint64_t data_version)
  : data_seq_(data_seq),
    data_version_(data_version)
{
}

int64_t ObBackupLogicMacroBlockId::hash() const
{
  int64_t hash_val = 0;
  hash_val = common::murmurhash(&data_seq_, sizeof(data_seq_), hash_val);
  hash_val = common::murmurhash(&data_version_, sizeof(data_version_), hash_val);
  return hash_val;
}

bool ObBackupLogicMacroBlockId::operator==(const ObBackupLogicMacroBlockId &other) const
{
  return data_seq_ == other.data_seq_ && data_version_ == other.data_version_;
}

bool ObBackupLogicMacroBlockId::operator!=(const ObBackupLogicMacroBlockId &other) const
{
  return !(operator==(other));
}

OB_SERIALIZE_MEMBER(ObBackupLogicMacroBlockId, data_seq_, data_version_);

/**
 * ObBackupLobIndex
 */
ObBackupLobIndex::ObBackupLobIndex()
  : version_(LOB_INDEX_VERSION),
    reserved_(0),
    logic_macro_id_(),
    byte_size_(0),
    char_size_(0)
{
}

bool ObBackupLobIndex::operator==(const ObBackupLobIndex &other) const
{
  return version_ == other.version_ && logic_macro_id_ == other.logic_macro_id_
      && byte_size_ == other.byte_size_ && char_size_ == other.char_size_;
}

bool ObBackupLobIndex::operator!=(const ObBackupLobIndex &other) const
{
  return !(operator==(other));
}

OB_SERIALIZE_MEMBER(ObBackupLobIndex, version_, logic_macro_id_, byte_size_, char_size_);

/**
 * ObBackupLobData
 */
ObBackupLobData::ObBackupLobData()
  : version_(LOB_DATA_VERSION),
  idx_cnt_(0),
  byte_size_(0),
  char_size_(0)
{
}

void ObBackupLobData::reset()
{
  version_ = LOB_DATA_VERSION;
  byte_size_ = 0;
  char_size_ = 0;
  idx_cnt_ = 0;
}

bool ObBackupLobData::operator==(const ObBackupLobData &other) const
{
  bool bret = version_ == other.version_ &&
                          byte_size_ == other.byte_size_ &&
                          char_size_ == other.char_size_ &&
                          idx_cnt_ == other.idx_cnt_;
  for (int64_t i = 0; i < idx_cnt_ && bret; ++i) {
    bret = lob_idx_[i] == other.lob_idx_[i];
  }
  return bret;
}

bool ObBackupLobData::operator!=(const ObBackupLobData &other) const
{
  return !(operator==(other));
}

int64_t ObBackupLobData::get_serialize_size() const
{
  int64_t serialize_size = 0;
  serialize_size += serialization::encoded_length_i32(version_);
  serialize_size += serialization::encoded_length_i32(idx_cnt_);
  serialize_size += serialization::encoded_length_i64(byte_size_);
  serialize_size += serialization::encoded_length_i64(char_size_);
  for (int64_t i = 0; i < idx_cnt_; ++i) {
    serialize_size += lob_idx_[i].get_serialize_size();
  }
  return serialize_size;
}

int ObBackupLobData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t request_size = get_serialize_size();
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0 || pos + request_size > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(pos), K(request_size), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to encode version", KR(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, idx_cnt_))) {
    LOG_WARN("fail to encode idx_cnt", KR(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, byte_size_))) {
    LOG_WARN("fail to encode byte_size", KR(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, char_size_))) {
    LOG_WARN("fail to encode char_size", KR(ret), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(idx_cnt_ > TOTAL_INDEX_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx cnt", KR(ret), K(idx_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < idx_cnt_; ++i) {
      if (OB_FAIL(lob_idx_[i].serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize lob index", KR(ret), K(buf_len), K(pos));
      }
    }
  }
  return ret;
}

int ObBackupLobData::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, buf_len, pos, reinterpret_cast<int32_t *>(&version_)))) {
    LOG_WARN("fail to decode version", KR(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, buf_len, pos, reinterpret_cast<int32_t *>(&idx_cnt_)))) {
    LOG_WARN("fail to decode idx_cnt", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t *>(&byte_size_)))) {
    LOG_WARN("fail to decode byte_size", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t *>(&char_size_)))) {
    LOG_WARN("fail to decode char_size", KR(ret));
  } else if (OB_UNLIKELY(idx_cnt_ > TOTAL_INDEX_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx cnt", KR(ret), K(idx_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < idx_cnt_; ++i) {
      if (OB_FAIL(lob_idx_[i].deserialize(buf, buf_len, pos))) {
        LOG_WARN("fail to deseriaze lob index", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObMacroBlockCommonHeader
 */
ObMacroBlockCommonHeader::ObMacroBlockCommonHeader()
{
  reset();
}

void ObMacroBlockCommonHeader::reset()
{
  header_size_ = (int32_t)get_serialize_size();
  version_ = MACRO_BLOCK_COMMON_HEADER_VERSION;
  magic_ = MACRO_BLOCK_COMMON_HEADER_MAGIC;
  attr_ = 0;
  data_version_ = 0;
  reserved_ = 0;
}

int ObMacroBlockCommonHeader::check_integrity()
{
  int ret =OB_SUCCESS;
  if (OB_UNLIKELY(
      header_size_ != get_serialize_size() ||
      version_ != MACRO_BLOCK_COMMON_HEADER_VERSION ||
      magic_ != MACRO_BLOCK_COMMON_HEADER_MAGIC)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid common header", KR(ret), K(*this));
  }
  return ret;
}

bool ObMacroBlockCommonHeader::is_valid() const
{
  bool b_ret = header_size_ > 0 &&
               version_ >= MACRO_BLOCK_COMMON_HEADER_VERSION &&
               magic_ == MACRO_BLOCK_COMMON_HEADER_MAGIC &&
               attr_ >= 0 &&
               attr_ < MaxMacroType;
  if (b_ret) {
    if ((PartitionMeta == attr_ || MacroMeta == attr_) && previous_block_index_ < -1) {
      b_ret = false;
    }
  }
  return b_ret;
}

int ObMacroBlockCommonHeader::deserialize(
    const char *buf,
    const int64_t data_len,
    int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObMacroBlockCommonHeader *ptr = reinterpret_cast<const ObMacroBlockCommonHeader*>(buf + pos);
    header_size_ = ptr->header_size_;
    version_ = ptr->version_;
    magic_ = ptr->magic_;
    attr_ = ptr->attr_;
    data_version_ = ptr->data_version_;
    reserved_ = ptr->reserved_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_ERROR("deserialize error", KR(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

int64_t ObMacroBlockCommonHeader::get_serialize_size(void) const
{
  return sizeof(ObMacroBlockCommonHeader);
}

/**
 * ObSSTableMacroBlockHeader
 */
ObSSTableMacroBlockHeader::ObSSTableMacroBlockHeader()
{
  reset();
}

void ObSSTableMacroBlockHeader::reset()
{
  memset(this, 0, sizeof(ObSSTableMacroBlockHeader));
}

bool ObSSTableMacroBlockHeader::is_valid() const
{
  return header_size_ > 0 &&
         version_ >= ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_VERSION &&
         SSTABLE_DATA_HEADER_MAGIC == magic_ &&
         attr_ >= 0 &&
         OB_INVALID_ID != table_id_ &&
         data_version_ >= 0 &&
         column_count_ >= rowkey_column_count_ &&
         rowkey_column_count_ > 0 &&
         column_index_scale_ >= 0 &&
         row_store_type_ >= 0 &&
         row_count_ > 0 &&
         occupy_size_ > 0 &&
         micro_block_count_ > 0 &&
         micro_block_size_ > 0 &&
         micro_block_data_offset_ > 0 &&
         micro_block_data_size_ > 0 &&
         micro_block_index_offset_ >= micro_block_data_offset_ &&
         micro_block_index_size_ > 0 &&
         micro_block_endkey_offset_ >= micro_block_index_offset_ &&
         micro_block_endkey_size_ > 0 &&
         data_checksum_ >= 0 &&
         partition_id_ >= -1 &&
         encrypt_id_ >= 0 &&
         master_key_id_ >= -1;
}

/**
 * ObLobMacroBlockHeader
 */
ObLobMacroBlockHeader::ObLobMacroBlockHeader()
  : ObSSTableMacroBlockHeader(),
    micro_block_size_array_offset_(0),
    micro_block_size_array_size_()
{
}

void ObLobMacroBlockHeader::reset()
{
  memset(this, 0, sizeof(ObLobMacroBlockHeader));
}

bool ObLobMacroBlockHeader::is_valid() const
{
  return header_size_ > 0 &&
         version_ >= LOB_MACRO_BLOCK_HEADER_VERSION_V1 &&
         LOB_MACRO_BLOCK_HEADER_MAGIC == magic_ &&
         common::OB_INVALID_ID != table_id_ &&
         data_version_ >= 0 &&
         column_count_ >= rowkey_column_count_ &&
         rowkey_column_count_ > 0 &&
         column_index_scale_ >= 0 &&
         row_store_type_ >= 0 &&
         row_count_ > 0 &&
         occupy_size_ > 0 &&
         micro_block_count_ > 0 &&
         micro_block_size_ > 0 &&
         micro_block_data_offset_ > 0 &&
         micro_block_data_size_ > 0 &&
         micro_block_index_offset_ >= micro_block_data_offset_ &&
         micro_block_index_size_ > 0 &&
         data_checksum_ >= 0 &&
         partition_id_ >= -1 &&
         (ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_ || 0 == attr_ /*temporarily for 147 bug*/);
}

/**
 * ObMacroBlockMeta
 */
int ObMacroBlockMeta::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);

  if (OB_UNLIKELY(endkey_ == nullptr)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The endkey is nullptr, can not deserialize");
  } else if (OB_UNLIKELY(buf == nullptr || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error", KR(ret), K(header_size),
              K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error", KR(ret), K(header_version),
              K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(attr_))) {
    LOG_WARN("deserialization attr_ error", KR(ret), K_(attr), KR(ret),
              K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(data_version_))) {
    LOG_WARN("deserialization data_version_ error", KR(ret), K_(data_version),
              K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (attr_ == ObTableLoadBackupMacroBlockType::SSTableData ||
             attr_ == ObTableLoadBackupMacroBlockType::LobData ||
             attr_ == ObTableLoadBackupMacroBlockType::LobIndex) {
    if (OB_FAIL(buffer_reader.read(column_number_))) {
      LOG_WARN("deserialization column_number_ error", KR(ret), K_(column_number),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
      LOG_WARN("deserialization rowkey_column_number_ error", KR(ret), K_(rowkey_column_number),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(column_index_scale_))) {
      LOG_WARN("deserialization column_index_scale_ error", KR(ret), K_(column_index_scale),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(row_store_type_))) {
      LOG_WARN("deserialization flag_ error", KR(ret), K_(row_store_type),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(row_count_))) {
      LOG_WARN("deserialization row_count_ error", KR(ret), K_(row_count),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(occupy_size_))) {
      LOG_WARN("deserialization occupy_size_ error", KR(ret), K_(occupy_size),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(data_checksum_))) {
      LOG_WARN("deserialization data_checksum_ error", KR(ret), K_(data_checksum),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_count_))) {
      LOG_WARN("deserialization micro_block_count_ error", KR(ret), K_(micro_block_count),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_data_offset_))) {
      LOG_WARN("deserialization micro_block_data_offset_ error", KR(ret), K_(micro_block_data_offset),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_index_offset_))) {
      LOG_WARN("deserialization micro_block_index_offset_ error", KR(ret), K_(micro_block_index_offset),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_endkey_offset_))) {
      LOG_WARN("deserialization micro_block_endkey_offset_ error", KR(ret), K_(micro_block_endkey_offset),
                K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read_cstr(compressor_))) {
      LOG_WARN("deserialization compressor_ error", KR(ret), K_(compressor), K(buffer_reader.capacity()),
                K(buffer_reader.pos()));
    }

    if (OB_SUCC(ret) && column_number_ > 0) {
      int64_t cip = 0;
      int64_t ctp = cip + column_number_ * sizeof(uint16_t); // column type array start position
      int64_t ckp = ctp + column_number_ * sizeof(ObObjMeta); // column checksum arrray start position
      int64_t ekp = ckp + column_number_ * sizeof(int64_t); // endkey start position
      char *column_array_start = buffer_reader.current();

      if (OB_ISNULL(column_array_start)) {
        ret = OB_ERR_SYS;
        LOG_WARN("column array is nullptr", KR(ret));
      } else if (OB_FAIL(buffer_reader.advance(ekp))) {
        LOG_WARN("remain buffer length not enough for column array", KR(ret), K(ekp), K(buffer_reader.remain()));
      } else {
        column_id_array_ = reinterpret_cast<uint16_t *>(column_array_start + cip);
        column_type_array_ = reinterpret_cast<ObObjMeta *>(column_array_start + ctp);
        column_checksum_ = reinterpret_cast<int64_t *>(column_array_start + ckp);
      }
    }

    if (OB_SUCC(ret)) {
      // deserialize rowkey;
      if (OB_FAIL(read_compact_rowkey(ObTableLoadBackupVersion::V_1_4, buffer_reader, column_type_array_, endkey_, rowkey_column_number_))) {
        LOG_WARN("read compact rowkey failed", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(table_id_))) {
          LOG_WARN("deserialization table_id_ error", KR(ret), K_(table_id),
                    K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(data_seq_))) {
          LOG_WARN("deserialization data_seq_ error", KR(ret), K_(data_seq),
                    K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as -1;
        data_seq_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_version_))) {
          LOG_WARN("deserialization schema_version_ error", KR(ret), K_(schema_version),
                    K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as 0;
        schema_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos > header_size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("elapsed buffer size larger than object", KR(ret), K(header_size),
                  K(buffer_reader.pos()), K(start_pos));
      }
    }

  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_reader.set_pos(start_pos + header_size))) {
      LOG_WARN("set pos on buffer reader failed", KR(ret));
    } else {
      pos = buffer_reader.pos();
    }
  }
  return ret;
}

/**
 * ObMacroBlockMetaV2
 */
ObMacroBlockMetaV2::ObMacroBlockMetaV2()
  : attr_(0),
    data_version_(0),
    column_number_(0),
    rowkey_column_number_(0),
    column_index_scale_(0),
    row_store_type_(0),
    row_count_(0),
    occupy_size_(0),
    data_checksum_(0),
    micro_block_count_(0),
    micro_block_data_offset_(0),
    micro_block_index_offset_(0),
    micro_block_endkey_offset_(0),
    compressor_(NULL),
    column_id_array_(NULL),
    column_type_array_(NULL),
    column_checksum_(NULL),
    endkey_(NULL),
    table_id_(0),
    data_seq_(-1),
    schema_version_(0),
    snapshot_version_(0),
    schema_rowkey_col_cnt_(0),
    column_order_array_(NULL),
    row_count_delta_(0),
    micro_block_mark_deletion_offset_(0),
    macro_block_deletion_flag_(false),
    micro_block_delta_offset_(0),
    partition_id_(-1),
    column_checksum_method_(ObColumnChecksumMethod::CCM_UNKOWN),
    progressive_merge_round_(0),
    write_seq_(0),
    bf_flag_(0),
    create_timestamp_(0),
    retire_timestamp_(0),
    collation_free_endkey_(NULL),
    encrypt_id_(0),
    master_key_id_(0)
{
  encrypt_key_[0] = '\0';
}

bool ObMacroBlockMetaV2::is_valid() const
{
  bool ret = true;
  if (!(attr_ >= ObMacroBlockCommonHeader::Free && attr_ < ObMacroBlockCommonHeader::MaxMacroType)) {
    ret = false;
  } else if (is_normal_data_block()) {
    if (rowkey_column_number_ <= 0 || column_number_ < rowkey_column_number_ || schema_rowkey_col_cnt_ < 0) {
      ret = false;
    } else if (column_index_scale_ < 0 || row_store_type_ < 0 || row_count_ <= 0 || occupy_size_ <= 0 || data_checksum_ < 0 ||
        micro_block_count_ <= 0 || micro_block_data_offset_ < 0 || micro_block_index_offset_ < micro_block_data_offset_
        || micro_block_endkey_offset_ < micro_block_index_offset_ || micro_block_mark_deletion_offset_ < 0 || micro_block_delta_offset_ < 0) {
      ret = false;
    } else if (0 != column_number_) {
      if (NULL == column_id_array_ || NULL == column_type_array_ || NULL == column_checksum_ || NULL == endkey_) {
        ret = false;
      }
    } else if ((0 == micro_block_mark_deletion_offset_ && 0 < micro_block_delta_offset_) || (0 == micro_block_delta_offset_ && 0 < micro_block_mark_deletion_offset_)) {
      ret = false;
    } else if (partition_id_ < -1) {
      ret = false;
    } else if (column_checksum_method_ < CCM_UNKOWN || column_checksum_method_ >= CCM_MAX) {
      ret = false;
    } else if (progressive_merge_round_ < 0) {
      ret = false;
    }
  }
  return ret;
}

int ObMacroBlockMetaV2::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);

  if (NULL == endkey_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The endkey is NULL, can not deserialize, ", K(ret));
  } else if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error.", K(header_size), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error.", K(header_version), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
  } else if (OB_FAIL(buffer_reader.read(attr_))) {
    LOG_WARN("deserialization attr_ error.", K_(attr), K(ret), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
  } else if (OB_FAIL(buffer_reader.read(data_version_))) {
    LOG_WARN("deserialization data_version_ error.", K_(data_version), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
  } else if (is_data_block()) {
    if (OB_FAIL(buffer_reader.read(column_number_))) {
      LOG_WARN("deserialization column_number_ error.", K_(column_number), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
      LOG_WARN("deserialization rowkey_column_number_ error.", K_(rowkey_column_number), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(column_index_scale_))) {
      LOG_WARN("deserialization column_index_scale_ error.", K_(column_index_scale), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(row_store_type_))) {
      LOG_WARN("deserialization flag_ error.", K_(row_store_type), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(row_count_))) {
      LOG_WARN("deserialization row_count_ error.", K_(row_count), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(occupy_size_))) {
      LOG_WARN("deserialization occupy_size_ error.", K_(occupy_size), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(data_checksum_))) {
      LOG_WARN("deserialization data_checksum_ error.", K_(data_checksum), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_count_))) {
      LOG_WARN("deserialization micro_block_count_ error.", K_(micro_block_count), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_data_offset_))) {
      LOG_WARN("deserialization micro_block_data_offset_ error.", K_(micro_block_data_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_index_offset_))) {
      LOG_WARN("deserialization micro_block_index_offset_ error.", K_(micro_block_index_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_endkey_offset_))) {
      LOG_WARN("deserialization micro_block_endkey_offset_ error.", K_(micro_block_endkey_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    } else if (OB_FAIL(buffer_reader.read_cstr(compressor_))) {
      LOG_WARN("deserialization compressor_ error.", K_(compressor), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
    }

    if (OB_SUCCESS == ret && column_number_ > 0) {
      int64_t cip = 0;
      int64_t ctp = cip + column_number_ * sizeof(uint16_t); // column type array start position
      int64_t ckp = ctp + column_number_ * sizeof(ObObjMeta); // column checksum array start position
      int64_t ekp = ckp + column_number_ * sizeof(int64_t); // endkey start position
      char *column_array_start = buffer_reader.current();

      if (OB_ISNULL(column_array_start)) {
        ret = OB_ERR_SYS;
        LOG_WARN("column array is NULL.", K(ret));
      } else if (OB_FAIL(buffer_reader.advance(ekp))) {
        LOG_WARN("remain buffer length not enough for column array.", K(ekp), K(buffer_reader.remain()), K(ret));
      } else {
        column_id_array_ = reinterpret_cast<uint16_t *>(column_array_start + cip);
        column_type_array_ = reinterpret_cast<ObObjMeta *>(column_array_start + ctp);
        column_checksum_ = reinterpret_cast<int64_t *>(column_array_start + ckp);
      }
    }

    if (OB_SUCC(ret) && is_normal_data_block()) {
      // deserialize rowkey;
      if (OB_FAIL(read_compact_rowkey(ObTableLoadBackupVersion::V_2_X_LOG, buffer_reader, column_type_array_, endkey_, rowkey_column_number_))) {
        LOG_WARN("read compact rowkey failed.", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(table_id_))) {
          LOG_WARN("deserialization table_id_ error.", K_(table_id), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(data_seq_))) {
          LOG_WARN("deserialization data_seq_ error.", K_(data_seq), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as -1;
        data_seq_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_version_))) {
          LOG_WARN("deserialization schema_version_ error.", K_(schema_version), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as 0;
        schema_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(snapshot_version_))) {
          LOG_WARN("deserialization snapshot_version_ error.", K_(snapshot_version), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as -1;
        snapshot_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
          LOG_WARN("deserialization schema_rowkey_col_cnt error.", K_(schema_rowkey_col_cnt), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as 0;
        schema_rowkey_col_cnt_ = 0;
      }
    }

    if (OB_SUCC(ret) && column_number_ > 0) {
      if (buffer_reader.pos() - start_pos < header_size) {
        int64_t order_array_size = column_number_ * sizeof(ObOrderType);
        char *column_order_array_start = buffer_reader.current();

        if (OB_ISNULL(column_order_array_start)) {
          ret = OB_ERR_SYS;
          LOG_WARN("column order array is NULL.", K(ret));
        } else if (OB_FAIL(buffer_reader.advance(order_array_size))) {
          LOG_WARN("remain buffer length not enough for column order array.", K(order_array_size), K(buffer_reader.remain()), K(ret));
        } else {
          column_order_array_ = reinterpret_cast<ObOrderType *>(column_order_array_start);
        }
      } else {
        // older version, with no order_array info in the header
        column_order_array_ = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(row_count_delta_))) {
          LOG_WARN("deserialization row_count_delta error.", K_(row_count_delta), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as 0;
        row_count_delta_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_mark_deletion_offset_))) {
          LOG_WARN("deserialization micro_block_mark_deletion_offset_ error.", K_(micro_block_mark_deletion_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as 0;
        micro_block_mark_deletion_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(macro_block_deletion_flag_))) {
          LOG_WARN("deserialization macro_block_deletion_flag_ error.", K_(macro_block_deletion_flag), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        // set default value as false;
        macro_block_deletion_flag_ = false;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_delta_offset_))) {
          LOG_WARN("deserialization micro_block_delta_offset error.", K_(micro_block_delta_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
        }
      } else {
        micro_block_delta_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(partition_id_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        partition_id_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(column_checksum_method_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        column_checksum_method_ = CCM_UNKOWN;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(progressive_merge_round_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        progressive_merge_round_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_id_))) {
          LOG_WARN("deserialization encrypt_id error", K(ret), K(buffer_reader));
        }
      } else {
        encrypt_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(master_key_id_))) {
          LOG_WARN("deserialization encrypt_id error", K(ret), K(buffer_reader));
        }
      } else {
        master_key_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_key_, sizeof(encrypt_key_)))) {
          LOG_WARN("deserialization encrypt_key_ error", K(ret), K(buffer_reader));
        }
      } else {
        encrypt_key_[0] = '\0';
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos > header_size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("elapsed buffer size larger than object.", K(header_size), K(buffer_reader.pos()), K(start_pos), K(ret));
      }
    }

  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_reader.set_pos(start_pos + header_size))) {
      LOG_WARN("set pos on buffer reader failed.", K(ret));
    } else {
      pos = buffer_reader.pos();
    }
  }
  return ret;
}

// 和实际serialize() func,用的size不一致，可能会偏大。因此不能使用OB_SERIALIZE_MEMBER这套宏。
// 如果错用OB_SERIALIZE_MEMBER，会导致反序列的时候报len不够
int64_t ObMacroBlockMetaV2::get_serialize_size() const
{
  return get_meta_content_serialize_size() + sizeof(int32_t) * 2;
}

int64_t ObMacroBlockMetaV2::get_meta_content_serialize_size() const
{
  int64_t size = sizeof(ObMacroBlockMetaV2); // struct self
  if (is_data_block()) {
    if (NULL != compressor_) {
      size += strlen(compressor_) + 1;                   // compressor_ string length.
    }
    if (NULL != column_id_array_) {
      size += sizeof(int16_t) * column_number_;     // column ids array
    }
    if (NULL != column_type_array_) {
      size += sizeof(ObObjMeta) * column_number_;     // column type array
    }
    if (NULL != column_checksum_) {
      size += sizeof(int64_t) * column_number_;     // column_checksum_ array
    }
    if (NULL != endkey_) {
      //FIXME-yangsuli: Not sure if we can simply use ObRowkey deep_copy_size
      // or if we need to add the type_ size
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      size += rowkey.get_deep_copy_size();                // rowkey object array
    }
    // regardless of whether column_order_array is NULL
    // because we may need to fill the column_order_array with default value (for compatibility)
    size += sizeof(ObOrderType) * column_number_;
  }
  return size;
}

/**
 * ObMacroBlockMetaV3
 */
ObMacroBlockMetaV3::ObMacroBlockMetaV3()
  : attr_(0),
    data_version_(0),
    column_number_(0),
    rowkey_column_number_(0),
    column_index_scale_(0),
    row_store_type_(0),
    row_count_(0),
    occupy_size_(0),
    data_checksum_(0),
    micro_block_count_(0),
    micro_block_data_offset_(0),
    micro_block_index_offset_(0),
    micro_block_endkey_offset_(0),
    column_checksum_(nullptr),
    endkey_(nullptr),
    table_id_(0),
    data_seq_(-1),
    schema_version_(0),
    snapshot_version_(0),
    schema_rowkey_col_cnt_(0),
    row_count_delta_(0),
    micro_block_mark_deletion_offset_(0),
    macro_block_deletion_flag_(false),
    micro_block_delta_offset_(0),
    partition_id_(-1),
    column_checksum_method_(ObColumnChecksumMethod::CCM_UNKOWN),
    progressive_merge_round_(0),
    write_seq_(0),
    bf_flag_(0),
    create_timestamp_(0),
    retire_timestamp_(0),
    collation_free_endkey_(nullptr),
    encrypt_id_(0),
    master_key_id_(0),
    contain_uncommitted_row_(false),
    max_merged_trans_version_(0)
{
  encrypt_key_[0] = '\0';
}

bool ObMacroBlockMetaV3::is_valid() const
{
  bool ret = true;
  if (!(attr_ >= ObMacroBlockCommonHeader::Free && attr_ < ObMacroBlockCommonHeader::MaxMacroType)) {
    ret = false;
  } else if (is_normal_data_block()) {
    if (rowkey_column_number_ <= 0 || column_number_ < rowkey_column_number_ || schema_rowkey_col_cnt_ < 0) {
      ret = false;
    } else if (column_index_scale_ < 0 || row_store_type_ < 0 || row_count_ <= 0 || occupy_size_ <= 0 || data_checksum_ < 0 ||
        micro_block_count_ <= 0 || micro_block_data_offset_ < 0 || micro_block_index_offset_ < micro_block_data_offset_ ||
        micro_block_endkey_offset_ < micro_block_index_offset_ || micro_block_mark_deletion_offset_ < 0 || micro_block_delta_offset_ < 0) {
      ret = false;
    } else if (0 != column_number_) {
      if (nullptr == column_checksum_ || nullptr == endkey_) {
        ret = false;
      }
    } else if ((0 == micro_block_mark_deletion_offset_ && 0 < micro_block_delta_offset_) || (0 == micro_block_delta_offset_ && 0 < micro_block_mark_deletion_offset_)) {
      ret = false;
    } else if (partition_id_ < -1) {
      ret = false;
    } else if (column_checksum_method_ < CCM_UNKOWN || column_checksum_method_ >= CCM_MAX) {
      ret = false;
    } else if (progressive_merge_round_ < 0) {
      ret = false;
    } else if (max_merged_trans_version_ < 0 || (!contain_uncommitted_row_ && 0 == max_merged_trans_version_)) {
      ret = false;
    }
  }
  return ret;
}

int ObMacroBlockMetaV3::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);

  if (nullptr == endkey_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The endkey is nullptr, can not deserialize", KR(ret));
  } else if (nullptr == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error.", KR(ret), K(header_size), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error.", KR(ret), K(header_version), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(attr_))) {
    LOG_WARN("deserialization attr_ error.", KR(ret), K_(attr), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(data_version_))) {
    LOG_WARN("deserialization data_version_ error.", KR(ret), K_(data_version), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (is_data_block()) {
    if (OB_FAIL(buffer_reader.read(column_number_))) {
      LOG_WARN("deserialization column_number_ error.", KR(ret), K_(column_number), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
      LOG_WARN("deserialization rowkey_column_number_ error.", KR(ret), K_(rowkey_column_number), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(column_index_scale_))) {
      LOG_WARN("deserialization column_index_scale_ error.", KR(ret), K_(column_index_scale), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(row_store_type_))) {
      LOG_WARN("deserialization flag_ error.", KR(ret), K_(row_store_type), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(row_count_))) {
      LOG_WARN("deserialization row_count_ error.", KR(ret), K_(row_count), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(occupy_size_))) {
      LOG_WARN("deserialization occupy_size_ error.", KR(ret), K_(occupy_size), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(data_checksum_))) {
      LOG_WARN("deserialization data_checksum_ error.", KR(ret), K_(data_checksum), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_count_))) {
      LOG_WARN("deserialization micro_block_count_ error.", KR(ret), K_(micro_block_count),
                  K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_data_offset_))) {
      LOG_WARN("deserialization micro_block_data_offset_ error.", KR(ret), K_(micro_block_data_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_index_offset_))) {
      LOG_WARN("deserialization micro_block_index_offset_ error.", KR(ret), K_(micro_block_index_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    } else if (OB_FAIL(buffer_reader.read(micro_block_endkey_offset_))) {
      LOG_WARN("deserialization micro_block_endkey_offset_ error.", KR(ret), K_(micro_block_endkey_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()));
    }

    if (OB_SUCCESS == ret && column_number_ > 0) {
      int64_t ekp = column_number_ * sizeof(int64_t); // endkey start position
      char *column_array_start = buffer_reader.current();

      if (OB_ISNULL(column_array_start)) {
        ret = OB_ERR_SYS;
        LOG_WARN("column array is nullptr.", KR(ret));
      } else if (OB_FAIL(buffer_reader.advance(ekp))) {
        LOG_WARN("remain buffer length not enough for column array.", KR(ret), K(ekp), K(buffer_reader.remain()));
      } else {
        column_checksum_ = reinterpret_cast<int64_t *>(column_array_start);
      }
    }

    if (OB_SUCC(ret) && is_normal_data_block()) {
      // deserialize rowkey;
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      if (OB_FAIL(buffer_reader.read(rowkey))) {
        LOG_WARN("fail to read rowkey", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(table_id_))) {
          LOG_WARN("deserialization table_id_ error.", KR(ret), K_(table_id), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(data_seq_))) {
          LOG_WARN("deserialization data_seq_ error.", KR(ret), K_(data_seq), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as -1;
        data_seq_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_version_))) {
          LOG_WARN("deserialization schema_version_ error.", KR(ret), K_(schema_version), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as 0;
        schema_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(snapshot_version_))) {
          LOG_WARN("deserialization snapshot_version_ error.", KR(ret), K_(snapshot_version), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as -1;
        snapshot_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
          LOG_WARN("deserialization schema_rowkey_col_cnt error.", KR(ret), K_(schema_rowkey_col_cnt), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as 0;
        schema_rowkey_col_cnt_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(row_count_delta_))) {
          LOG_WARN("deserialization row_count_delta error.", KR(ret), K_(row_count_delta), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as 0;
        row_count_delta_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_mark_deletion_offset_))) {
          LOG_WARN("deserialization micro_block_mark_deletion_offset_ error.", KR(ret), K_(micro_block_mark_deletion_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as 0;
        micro_block_mark_deletion_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(macro_block_deletion_flag_))) {
          LOG_WARN("deserialization macro_block_deletion_flag_ error.", KR(ret), K_(macro_block_deletion_flag), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        // set default value as false;
        macro_block_deletion_flag_ = false;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_delta_offset_))) {
          LOG_WARN("deserialization micro_block_delta_offset error.", KR(ret), K_(micro_block_delta_offset), K(buffer_reader.capacity()), K(buffer_reader.pos()));
        }
      } else {
        micro_block_delta_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(partition_id_))) {
          LOG_WARN("deserialization partition_id error", KR(ret), K(buffer_reader));
        }
      } else {
        partition_id_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(column_checksum_method_))) {
          LOG_WARN("deserialization partition_id error", KR(ret), K(buffer_reader));
        }
      } else {
        column_checksum_method_ = CCM_UNKOWN;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(progressive_merge_round_))) {
          LOG_WARN("deserialization partition_id error", KR(ret), K(buffer_reader));
        }
      } else {
        progressive_merge_round_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_id_))) {
          LOG_WARN("deserialization encrypt_id error", KR(ret), K(buffer_reader));
        }
      } else {
        encrypt_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(master_key_id_))) {
          LOG_WARN("deserialization encrypt_id error", KR(ret), K(buffer_reader));
        }
      } else {
        master_key_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_key_, sizeof(encrypt_key_)))) {
          LOG_WARN("deserialization encrypt_key_ error", KR(ret), K(buffer_reader));
        }
      } else {
        encrypt_key_[0] = '\0';
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(contain_uncommitted_row_))) {
          LOG_WARN("failed to deserialize contain_uncommitted_row", KR(ret), K(buffer_reader));
        }
      } else {
        contain_uncommitted_row_ = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(max_merged_trans_version_))) {
          LOG_WARN("failed to deserialize max_merged_trans_version", KR(ret), K(buffer_reader));
        }
      } else {
        max_merged_trans_version_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos > header_size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("elapsed buffer size larger than object.", KR(ret), K(header_size), K(buffer_reader.pos()), K(start_pos));
      }
    }

  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_reader.set_pos(start_pos + header_size))) {
      LOG_WARN("set pos on buffer reader failed.", KR(ret));
    } else {
      pos = buffer_reader.pos();
    }
  }
  return ret;
}

int64_t ObMacroBlockMetaV3::get_serialize_size() const
{
  int64_t serialize_size = 2 * sizeof(int32_t); // header_size and header_version
  serialize_size += sizeof(attr_);
  serialize_size += sizeof(data_version_);
  if (is_data_block()) {
    serialize_size += sizeof(column_number_);
    serialize_size += sizeof(rowkey_column_number_);
    serialize_size += sizeof(column_index_scale_);
    serialize_size += sizeof(row_store_type_);
    serialize_size += sizeof(row_count_);
    serialize_size += sizeof(occupy_size_);
    serialize_size += sizeof(data_checksum_);
    serialize_size += sizeof(micro_block_count_);
    serialize_size += sizeof(micro_block_data_offset_);
    serialize_size += sizeof(micro_block_index_offset_);
    serialize_size += sizeof(micro_block_endkey_offset_);
  }
  serialize_size += column_number_ * sizeof(int64_t);
  if (is_normal_data_block()) {
    ObRowkey rowkey(endkey_, rowkey_column_number_);
    serialize_size += rowkey.get_serialize_size();
  }
  serialize_size += sizeof(table_id_);
  serialize_size += sizeof(data_seq_);
  serialize_size += sizeof(schema_version_);
  serialize_size += sizeof(snapshot_version_);
  serialize_size += sizeof(schema_rowkey_col_cnt_);
  serialize_size += sizeof(row_count_delta_);
  serialize_size += sizeof(micro_block_mark_deletion_offset_);
  serialize_size += sizeof(macro_block_deletion_flag_);
  serialize_size += sizeof(micro_block_delta_offset_);
  serialize_size += sizeof(partition_id_);
  serialize_size += sizeof(column_checksum_method_);
  serialize_size += sizeof(progressive_merge_round_);
  serialize_size += sizeof(encrypt_id_);
  serialize_size += sizeof(master_key_id_);
  serialize_size += sizeof(encrypt_key_);
  serialize_size += sizeof(contain_uncommitted_row_);
  serialize_size += sizeof(max_merged_trans_version_);
  return serialize_size;
}

/**
 * ObMacroBlockSchemaInfo
 */
ObMacroBlockSchemaInfo::ObMacroBlockSchemaInfo()
  : column_number_(0),
    rowkey_column_number_(0),
    schema_version_(0),
    schema_rowkey_col_cnt_(0),
    compressor_(nullptr),
    column_id_array_(nullptr),
    column_type_array_(nullptr),
    column_order_array_(nullptr)
{
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
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_writer.write(header_size))) {
    LOG_WARN("serialization header_size error.", KR(ret), K(header_size), K(buffer_writer.capacity()), K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(header_version))) {
    LOG_WARN("serialization header_version error.", KR(ret), K(header_version), K(buffer_writer.capacity()), K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(column_number_))) {
    LOG_WARN("fail to write column number", KR(ret));
  } else if (OB_FAIL(buffer_writer.write(rowkey_column_number_))) {
    LOG_WARN("fail to write rowkey column number", KR(ret));
  } else if (OB_FAIL(buffer_writer.write(schema_version_))) {
    LOG_WARN("fail to write schema version", KR(ret));
  } else if (OB_FAIL(buffer_writer.write(schema_rowkey_col_cnt_))) {
    LOG_WARN("fail to write schema rowkey column cnt", KR(ret));
  } else if (OB_FAIL(buffer_writer.append_fmt("%s", compressor_))) {
    LOG_WARN("fail to append fmt", KR(ret));
  } else if (column_number_ > 0) {
    for (int32_t i = 0; i < column_number_ && nullptr != column_id_array_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(buffer_writer.write(column_id_array_[i]))) {
        LOG_WARN("serialization column_id_array_ error.", KR(ret), K(column_id_array_[i]), K(buffer_writer.capacity()), K(buffer_writer.pos()));
      }
    }
    for (int32_t i = 0;
        i < column_number_ && nullptr != column_type_array_ && OB_SUCC(ret);
        ++i) {
      if (OB_FAIL(buffer_writer.write(column_type_array_[i]))) {
        LOG_WARN("serialization column_type_array_ error.", KR(ret), K(column_type_array_[i]), K(buffer_writer.capacity()), K(buffer_writer.pos()));
      }
    }

    for (int32_t i = 0;
        i < column_number_ && nullptr != column_order_array_ && OB_SUCC(ret);
        ++i) {
      if (OB_FAIL(buffer_writer.write(column_order_array_[i]))) {
        LOG_WARN("serialization column_order_array_ error.", KR(ret), K(column_order_array_[i]), K(buffer_writer.capacity()), K(buffer_writer.pos()));
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
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("fail to read header size", KR(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("fail to read header version", KR(ret));
  } else if (OB_FAIL(buffer_reader.read(column_number_))) {
    LOG_WARN("fail to read column number", KR(ret));
  } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
    LOG_WARN("fail to read rowkey column number", KR(ret));
  } else if (OB_FAIL(buffer_reader.read(schema_version_))) {
    LOG_WARN("fail to read schema version", KR(ret));
  } else if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
    LOG_WARN("fail to read schema rowkey column count", KR(ret));
  } else if (OB_FAIL(buffer_reader.read_cstr(compressor_))) {
    LOG_WARN("fail to deserialize compressor", KR(ret), K_(compressor), K(buffer_reader.capacity()), K(buffer_reader.pos()));
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
      LOG_WARN("fail to advance buffer reader", KR(ret));
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

bool ObMacroBlockSchemaInfo::is_valid() const
{
  return (0 == column_number_ ||
         (nullptr != compressor_ &&
         nullptr != column_id_array_ &&
         nullptr != column_type_array_ &&
         nullptr != column_order_array_));
}

/**
 * ObFullMacroBlockMetaEntry
 */
ObFullMacroBlockMetaEntry::ObFullMacroBlockMetaEntry(
    ObMacroBlockMetaV3 &meta,
    ObMacroBlockSchemaInfo &schema)
  : meta_(meta),
    schema_(schema)
{
}

bool ObFullMacroBlockMetaEntry::is_valid() const
{
  return meta_.is_valid() && schema_.is_valid();
}

int ObFullMacroBlockMetaEntry::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, buf_len, pos);
  if (nullptr == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error.", KR(ret), K(header_size), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error.", KR(ret), K(header_version), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (OB_UNLIKELY(ObMacroBlockMetaV3::MACRO_BLOCK_META_VERSION_V2 != header_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected header version", KR(ret), K(header_version));
  } else if (OB_FAIL(meta_.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize macro block meta v2", KR(ret));
  } else if (OB_FAIL(schema_.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize macro block schema info", KR(ret));
  }
  return ret;
}

int64_t ObFullMacroBlockMetaEntry::get_serialize_size() const
{
  return meta_.get_serialize_size() + schema_.get_serialize_size();
}

/**
 * ObBackupMacroData
 */
ObBackupMacroData::ObBackupMacroData(blocksstable::ObBufferHolder &meta, blocksstable::ObBufferReader &data)
  : meta_(meta),
    data_(data)
{
}

OB_SERIALIZE_MEMBER(ObBackupMacroData, meta_, data_);

/**
 * ObMicroBlockHeader
 */
ObMicroBlockHeader::ObMicroBlockHeader()
{
  memset(this, 0, sizeof(*this));
}

bool ObMicroBlockHeader::is_valid() const
{
  return header_size_ > 0 &&
         version_ >= MICRO_BLOCK_HEADER_VERSION &&
         MICRO_BLOCK_HEADER_MAGIC == magic_ &&
         attr_ >= 0 &&
         column_count_ > 0 &&
         row_index_offset_ > 0 &&
         row_count_ > 0;
}

/**
 * ObMicroBlockHeaderV2
 */
ObMicroBlockHeaderV2::ObMicroBlockHeaderV2()
  : header_size_(sizeof(*this)),
    version_(MICRO_BLOCK_HEADERV2_VERSION),
    row_count_(0),
    var_column_count_(0),
    row_data_offset_(0),
    opt_(0),
    reserved_(0)
{
}

void ObMicroBlockHeaderV2::reset()
{
  new (this) ObMicroBlockHeaderV2();
}

bool ObMicroBlockHeaderV2::is_valid() const
{
  return header_size_ >= sizeof(*this) &&
         version_ >= MICRO_BLOCK_HEADERV2_VERSION &&
         row_count_ > 0 &&
         row_index_byte_ >= 0 &&
         extend_value_bit_ >= 0 &&
         row_data_offset_ >= 0;
}

/**
 * ObRecordHeaderV3
 */
int64_t ObRecordHeaderV3::get_serialize_size(const int64_t header_version, const int64_t column_cnt) {
  return RECORD_HEADER_VERSION_V2 == header_version ? sizeof(ObRecordCommonHeader) :
      sizeof(ObRecordCommonHeader) + sizeof(data_encoding_length_) + sizeof(row_count_) + sizeof(column_cnt_) + column_cnt * sizeof(column_checksums_[0]);
}

ObRecordHeaderV3::ObRecordHeaderV3()
  : magic_(0),
    header_length_(0),
    version_(0),
    header_checksum_(0),
    reserved16_(0),
    data_length_(0),
    data_zlength_(0),
    data_checksum_(0),
    data_encoding_length_(0),
    row_count_(0),
    column_cnt_(0),
    column_checksums_()
{
}

int ObRecordHeaderV3::check_and_get_record(
    const char *ptr,
    const int64_t size,
    const int16_t magic,
    const char *&payload_ptr,
    int64_t &payload_size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ptr || magic < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ptr));
  } else if (OB_UNLIKELY(magic != magic_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("record header magic is not match", KR(ret), K(magic), K(magic_));
  } else if (OB_FAIL(check_header_checksum())) {
    LOG_WARN("fail to check header checksum", KR(ret));
  } else {
    const int64_t header_size = get_serialize_size();
    if (size < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer not enough", KR(ret), K(size), K(header_size));
    } else {
      payload_ptr = ptr + header_size;
      payload_size = size - header_size;
      if (OB_FAIL(check_payload_checksum(payload_ptr, payload_size))) {
        LOG_WARN("fail to check payload checksum", KR(ret));
      }
    }
  }
  return ret;
}

int64_t ObRecordHeaderV3::get_serialize_size() const
{
  int64_t size = 0;
  size += sizeof(ObRecordCommonHeader);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    size += sizeof(data_encoding_length_);
    size += sizeof(row_count_);
    size += sizeof(column_cnt_);
    size += column_cnt_ * sizeof(int64_t);
  }
  return size;
}

int ObRecordHeaderV3::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < 8)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len));
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
    pos += sizeof(ObRecordCommonHeader);
    if (RECORD_HEADER_VERSION_V3 == version_) {
      const int64_t *column_checksums = nullptr;
      MEMCPY(&data_encoding_length_, buf + pos, sizeof(data_encoding_length_));
      pos += sizeof(data_encoding_length_);
      MEMCPY(&row_count_, buf + pos, sizeof(row_count_));
      pos += sizeof(row_count_);
      MEMCPY(&column_cnt_, buf + pos, sizeof(column_cnt_));
      pos += sizeof(column_cnt_);
      column_checksums = reinterpret_cast<const int64_t *>(buf + pos);
      column_checksums_ = const_cast<int64_t *>(column_checksums);
      pos += column_cnt_ * sizeof(int64_t);
    }
    pos += pos_orig;
  }
  return ret;
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
  if (RECORD_HEADER_VERSION_V3 == version_) {
    format_i64(data_encoding_length_, checksum);
    format_i64(row_count_, checksum);
    format_i64(column_cnt_, checksum);
    for (int64_t i = 0; i < column_cnt_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }

  if (0 != checksum) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_ERROR("record check checksum failed", KR(ret), K(*this));
  }
  return ret;
}

int ObRecordHeaderV3::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || len < 0 || data_zlength_ != len ||
      (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(len), K(data_zlength_), K(data_length_), K(data_checksum_));
  } else {
    const int64_t data_checksum = ob_crc64_sse42(buf, len);
    if (data_checksum != data_checksum_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      LOG_ERROR("checksum error", KR(ret), K(data_checksum_), K(data_checksum));
    }
  }
  return ret;
}

/**
 * ObRecordHeaderV2
 */
int ObRecordHeaderV2::check_header_checksum() const
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

  if (OB_UNLIKELY(checksum != 0)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("record check checksum failed", K(*this));
  }
  return ret;
}

int ObRecordHeaderV2::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf == nullptr || len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(buf), K(len));
  } else if (OB_UNLIKELY(len == 0 && (data_zlength_ != 0 || data_length_ != 0 || data_checksum_ != 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(buf), K(len), K(data_zlength_), K(data_length_), K(data_checksum_));
  } else if ((OB_UNLIKELY(data_zlength_ != len))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data length is not correct", K(data_zlength_), K(len));
  } else {
    int64_t crc_check_sum = ob_crc64_sse42(buf, len);
    if (OB_UNLIKELY(crc_check_sum !=  data_checksum_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("checksum error", K(crc_check_sum), K(data_checksum_));
    }
  }
  return ret;
}

int ObRecordHeaderV2::check_record(const char *ptr, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t record_header_len = sizeof(ObRecordHeaderV2);
  if (OB_UNLIKELY(ptr == nullptr || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(ptr), K(size));
  } else if (OB_UNLIKELY(record_header_len > size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("invalid args, header size too small", K(record_header_len), K(size));
  } else {
    const ObRecordHeaderV2 *header =
            reinterpret_cast<const ObRecordHeaderV2*>(ptr);
    const char *payload_ptr = ptr + record_header_len;
    int64_t payload_size = size - record_header_len;
    if (header->magic_ != PRE_MICRO_BLOCK_RECORD_HEADER_MAGIC) {
      ret = OB_INVALID_DATA;
      LOG_WARN("record header magic is not match", K(*header), K(header->magic_));
    } else if (header->version_ != PRE_MICRO_BLOCK_RECORD_HEADER_VERSION) {
      ret = OB_INVALID_DATA;
      LOG_WARN("record header version is not match", K(*header));
    } else if (OB_FAIL(header->check_header_checksum())) {
      LOG_WARN("check header checksum failed", KR(ret), K(*header), K(record_header_len));
    } else if (OB_FAIL(header->check_payload_checksum(payload_ptr, payload_size))) {
      LOG_WARN("check data checksum failed", KR(ret), K(*header), KP(payload_ptr), K(payload_size));
    }
  }
  return ret;
}

/**
 * ObMicroBlockEncoderOpt
 */
const bool ObMicroBlockEncoderOpt::ENCODINGS_DEFAULT[ObColumnHeader::MAX_TYPE] = {true, true, true, true, true, true, true, true, true, true};
const bool ObMicroBlockEncoderOpt::ENCODINGS_NONE[ObColumnHeader::MAX_TYPE] = {false, false, false, false, false, false, false, false, false, false};
const bool ObMicroBlockEncoderOpt::ENCODINGS_FOR_PERFORMANCE[ObColumnHeader::MAX_TYPE] = {true, true, false, true, false, false, false, false, false, false};

ObMicroBlockEncoderOpt::ObMicroBlockEncoderOpt()
{
  set_all(true);
}

ObMicroBlockEncoderOpt::ObMicroBlockEncoderOpt(ObRowStoreType store_type) {
  store_row_header_ = true;
  switch (store_type) {
    case SELECTIVE_ENCODING_ROW_STORE:
      enable_bit_packing_ = false;
      store_sorted_var_len_numbers_dict_ = true;
      encodings_ = ENCODINGS_FOR_PERFORMANCE;
      break;
    default:
      enable_bit_packing_ = true;
      store_sorted_var_len_numbers_dict_ = false;
      encodings_ = ENCODINGS_DEFAULT;
      break;
  }
}

void ObMicroBlockEncoderOpt::set_all(const bool enable)
{
  store_row_header_ = enable;
  enable_bit_packing_ = enable;
  if (enable) {
    encodings_ = ENCODINGS_DEFAULT;
  } else {
    encodings_ = ENCODINGS_NONE;
  }
}

/**
 * ObMicroBlockEncodingCtx
 */
ObMicroBlockEncodingCtx::ObMicroBlockEncodingCtx()
  : macro_block_size_(0),
    micro_block_size_(0),
    rowkey_column_cnt_(0),
    column_cnt_(0),
    column_types_(0),
    encoder_opt_(),
    estimate_block_size_(0),
    real_block_size_(0),
    micro_block_cnt_(0),
    column_encodings_(nullptr),
    store_nbr_int_(false),
    major_working_cluster_version_(0)
{
}

bool ObMicroBlockEncodingCtx::is_valid() const
{
  return macro_block_size_ > 0 &&
         micro_block_size_ > 0 &&
         rowkey_column_cnt_ > 0 &&
         column_cnt_ >= rowkey_column_cnt_ &&
         nullptr != column_types_ &&
         encoder_opt_.is_valid() &&
         major_working_cluster_version_ >= 0;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
