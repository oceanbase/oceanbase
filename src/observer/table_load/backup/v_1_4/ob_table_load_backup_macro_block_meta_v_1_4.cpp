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

#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_meta_v_1_4.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_row_reader_v_1_4.h"

namespace oceanbase
{
namespace observer
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
  ObTableLoadBackupRowReader_V_1_4 reader;
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
 * ObTableLoadBackupMacroBlockMeta_V_1_4
 */
int ObTableLoadBackupMacroBlockMeta_V_1_4::deserialize(const char *buf, int64_t data_len, int64_t &pos)
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
  } else if (attr_ == ObTableLoadBackupMacroBlockType_V_1_4::SSTableData ||
             attr_ == ObTableLoadBackupMacroBlockType_V_1_4::LobData ||
             attr_ == ObTableLoadBackupMacroBlockType_V_1_4::LobIndex) {
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

} // namespace observer
} // namespace oceanbase
