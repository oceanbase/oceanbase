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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_block_sstable_struct.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_row_reader.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{
using namespace common;
using namespace blocksstable;

int read_compact_rowkey(ObBufferReader &buffer_reader,
                        const common::ObObjMeta *column_type_array,
                        common::ObObj *endkey,
                        const int64_t count)
{
  int ret = OB_SUCCESS;
  common::ObNewRow row;
  row.cells_ = endkey;
  row.count_ = count;
  ObTableLoadBackupRowReader reader;
  int64_t pos = buffer_reader.pos();
  if (OB_UNLIKELY(buffer_reader.data() == nullptr || buffer_reader.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(buffer_reader.data()), K(buffer_reader.length()));
  } else if (OB_UNLIKELY(column_type_array == nullptr || endkey == nullptr || count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(column_type_array), KP(endkey), K(count));
  } else if (OB_FAIL(reader.read_compact_rowkey(column_type_array,
                                                count,
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
 * ObSchemaInfo
 */
ObSchemaInfo::ObSchemaInfo()
{
  column_desc_.set_tenant_id(MTL_ID());
}

void ObSchemaInfo::reset()
{
  column_desc_.reset();
}

int ObSchemaInfo::assign(const ObSchemaInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_desc_.assign(other.column_desc_))) {
    LOG_WARN("fail to assign", KR(ret), K(other));
  }
  return ret;
}

/**
 * ObTableLoadBackupMacroBlockMeta
 */
int ObTableLoadBackupMacroBlockMeta::deserialize(const char *buf, int64_t data_len, int64_t &pos)
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
      if (OB_FAIL(read_compact_rowkey(buffer_reader, column_type_array_, endkey_, rowkey_column_number_))) {
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

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
