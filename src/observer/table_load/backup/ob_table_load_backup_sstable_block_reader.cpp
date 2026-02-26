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
#include "observer/table_load/backup/ob_table_load_backup_sstable_block_reader.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "observer/table_load/backup/ob_table_load_backup_column_map_v2.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace blocksstable;
using namespace share;

ObTableLoadBackupSSTableBlockReader::ObTableLoadBackupSSTableBlockReader()
  : allocator_("TLD_BSBR"),
    buf_allocator_("TLD_BSBRB"),
    buf_(nullptr),
    buf_size_(0),
    pos_(0),
    macro_block_buf_(nullptr),
    macro_block_buf_size_(0),
    macro_block_buf_pos_(0),
    decomp_buf_(nullptr),
    decomp_buf_size_(0),
    backup_common_header_(nullptr),
    sstable_macro_block_header_(nullptr),
    micro_indexes_(nullptr),
    lob_micro_indexes_(nullptr),
    column_ids_(nullptr),
    column_types_(nullptr),
    column_checksum_(nullptr),
    col_map_(nullptr),
    compressor_(nullptr),
    cur_block_idx_(0),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  buf_allocator_.set_tenant_id(MTL_ID());
  columns_.set_tenant_id(MTL_ID());
  int ret = OB_SUCCESS;
}

ObTableLoadBackupSSTableBlockReader::~ObTableLoadBackupSSTableBlockReader()
{
  reset();
}

int ObTableLoadBackupSSTableBlockReader::init(
    const char *buf,
    int64_t buf_size,
    const ObTableLoadBackupVersion &backup_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(buf == nullptr || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size), K(backup_version));
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    backup_version_ = backup_version;
    if (OB_FAIL(inner_init())) {
      LOG_WARN("fail to inner init", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_backup_common_header())) {
    LOG_WARN("fail to init backup_common_header_", KR(ret));
  } else if (OB_FAIL(init_macro_block_meta())) {
    LOG_WARN("fail to init macro block meta", KR(ret));
  } else if (OB_FAIL(init_macro_block_common_header())) {
    LOG_WARN("fail to init macro block common header", KR(ret));
  } else {
    if (macro_block_common_header_.is_sstable_data_block()) {
      if (OB_FAIL(inner_init_data_block())) {
        LOG_WARN("fail to inner init data block", KR(ret));
      }
    } else if (macro_block_common_header_.is_lob_data_block()) {
      if (OB_FAIL(inner_init_lob_block())) {
        LOG_WARN("fail to inner init lob block", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::init_backup_common_header()
{
  int ret = OB_SUCCESS;
  if (is_logical_backup_version(backup_version_)) {
    // do nothing
  } else {
    backup_common_header_ = reinterpret_cast<const ObBackupCommonHeader*>(buf_);
    if (OB_ISNULL(backup_common_header_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KR(ret));
    } else if (FALSE_IT(pos_ += sizeof(ObBackupCommonHeader))) {
    } else if (OB_FAIL(backup_common_header_->check_valid())) {
      LOG_WARN("fail to check valid", KR(ret),  K(*backup_common_header_));
    } else if (backup_common_header_->data_zlength_ > buf_size_ - pos_) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", KR(ret),  KPC(backup_common_header_), K(buf_size_), K(pos_));
    } else if (OB_FAIL(backup_common_header_->check_data_checksum(buf_ + pos_, backup_common_header_->data_zlength_))) {
      LOG_WARN("failed to check data checksum", KR(ret),  KPC(backup_common_header_));
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::init_macro_block_meta()
{
  int ret = OB_SUCCESS;
  if (backup_version_ == ObTableLoadBackupVersion::V_2_X_LOG) {
    int64_t meta_len = 0;
    int64_t macro_block_size = 0;
    if (OB_ISNULL(macro_block_meta_v2_.endkey_ = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for macro block end key", KR(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf_, buf_size_, pos_, &meta_len))) {
      LOG_WARN("fail to decode", KR(ret), KP(buf_), K(buf_size_), K(pos_));
    } else if (OB_FAIL(macro_block_meta_v2_.deserialize(buf_, buf_size_, pos_))) {
      LOG_WARN("fail to deserialize", KR(ret), KP(buf_), K(buf_size_), K(pos_));
    } else if (OB_FAIL(serialization::decode_i64(buf_, buf_size_, pos_, &macro_block_size))) {
      LOG_WARN("fail to decode", KR(ret), KP(buf_), K(buf_size_), K(pos_));
    } else if (OB_UNLIKELY(!macro_block_meta_v2_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block meta is invalid", KR(ret), K(macro_block_meta_v2_));
    }
  } else if (backup_version_ == ObTableLoadBackupVersion::V_2_X_PHY) {
    ObBufferReader macro_block_meta_reader;
    ObBufferReader macro_block_reader;
    ObBackupMacroData backup_macro_data(macro_block_meta_reader, macro_block_reader);
    if (OB_FAIL(backup_macro_data.deserialize(buf_, buf_size_, pos_))) {
      LOG_WARN("fail to deserialize", KR(ret));
    } else if (OB_ISNULL(macro_block_meta_v2_.endkey_ = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for macro block end key", KR(ret));
    } else if (OB_FAIL(macro_block_meta_reader.read_serialize(macro_block_meta_v2_))) {
      LOG_WARN("fail to read serialize", KR(ret), K(macro_block_meta_reader));
    } else if (OB_UNLIKELY(!macro_block_meta_v2_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block meta is invalid", KR(ret), K(macro_block_meta_v2_));
    } else {
      pos_ = macro_block_reader.current() - buf_;
    }
  } else {
    ObBufferReader macro_block_meta_reader;
    ObBufferReader macro_block_reader;
    ObBackupMacroData backup_macro_data(macro_block_meta_reader, macro_block_reader);
    ObMacroBlockSchemaInfo macro_block_schema_info;
    ObFullMacroBlockMetaEntry full_macro_block_meta_entry(macro_block_meta_v3_, macro_block_schema_info);
    if (OB_FAIL(backup_macro_data.deserialize(buf_, buf_size_, pos_))) {
      LOG_WARN("fail to deserialize", KR(ret));
    } else if (OB_ISNULL(macro_block_meta_v3_.endkey_ = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for macro block end key", KR(ret));
    } else if (OB_FAIL(macro_block_meta_reader.read_serialize(full_macro_block_meta_entry))) {
      LOG_WARN("fail to read serialize", KR(ret), K(macro_block_meta_reader));
    } else if (OB_UNLIKELY(!full_macro_block_meta_entry.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("full macro block meta entry is invalid", KR(ret), K(full_macro_block_meta_entry));
    } else {
      pos_ = macro_block_reader.current() - buf_;
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::init_macro_block_common_header()
{
  int ret = OB_SUCCESS;
  macro_block_buf_ = buf_ + pos_;
  macro_block_buf_pos_ = 0;
  macro_block_buf_size_ = buf_size_ - pos_;
  if (OB_FAIL(macro_block_common_header_.deserialize(macro_block_buf_, macro_block_buf_size_, macro_block_buf_pos_))) {
    LOG_WARN("fail to deserialize", KR(ret));
  } else if (OB_UNLIKELY(!macro_block_common_header_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro block common header", KR(ret), K(macro_block_common_header_));
  } else if (OB_FAIL(macro_block_common_header_.check_integrity())) {
    LOG_WARN("fail to check integrity", KR(ret), K(macro_block_common_header_));
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::inner_init_data_block()
{
  int ret = OB_SUCCESS;
  sstable_macro_block_header_ = reinterpret_cast<const ObSSTableMacroBlockHeader*>(macro_block_buf_ + macro_block_buf_pos_);
  macro_block_buf_pos_ += sizeof(ObSSTableMacroBlockHeader);
  if (OB_UNLIKELY(!sstable_macro_block_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid sstable macro block header", KR(ret), KPC(sstable_macro_block_header_));
  } else {
    const int64_t column_cnt = sstable_macro_block_header_->column_count_;
    column_ids_ = reinterpret_cast<const uint16_t*>(macro_block_buf_ + macro_block_buf_pos_);
    macro_block_buf_pos_ += sizeof(uint16_t) * column_cnt;
    column_types_ = reinterpret_cast<const ObObjMeta*>(macro_block_buf_ + macro_block_buf_pos_);
    macro_block_buf_pos_ += sizeof(ObObjMeta) * column_cnt;
    if (sstable_macro_block_header_->version_ >= SSTABLE_MACRO_BLOCK_HEADER_VERSION_v3) {
      column_orders_ = reinterpret_cast<const ObOrderType*>(macro_block_buf_ + macro_block_buf_pos_);
      macro_block_buf_pos_ += sizeof(ObOrderType) * column_cnt;
    }
    column_checksum_ = reinterpret_cast<const int64_t*>(macro_block_buf_ + macro_block_buf_pos_);
    macro_block_buf_pos_ += sizeof(int64_t) * column_cnt;

    if (sstable_macro_block_header_->micro_block_data_offset_ != macro_block_buf_pos_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("incorrect data offset", KR(ret), K(macro_block_buf_pos_), K(*sstable_macro_block_header_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      ObColDesc col_desc;
      col_desc.col_id_ = column_ids_[i];
      col_desc.col_type_ = column_types_[i];
      if (OB_FAIL(columns_.push_back(col_desc))) {
        LOG_WARN("fail to push col desc to columns", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObColumnMapV2 *col_map = nullptr;
      if (OB_ISNULL(col_map_ = col_map = OB_NEWx(ObColumnMapV2, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObColumnMapV2", KR(ret));
      } else if (OB_FAIL(col_map->init(sstable_macro_block_header_->rowkey_column_count_,
                                       sstable_macro_block_header_->column_count_,
                                       columns_))) {
        LOG_WARN("fail to init col map", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::inner_init_lob_block()
{
  int ret = OB_SUCCESS;
  const ObLobMacroBlockHeader *lob_header = reinterpret_cast<const ObLobMacroBlockHeader*>(macro_block_buf_ + macro_block_buf_pos_);
  if (OB_UNLIKELY(!lob_header->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid lob macro block header", KR(ret), KPC(lob_header));
  } else {
    sstable_macro_block_header_ = reinterpret_cast<const ObLobMacroBlockHeader*>(macro_block_buf_ + macro_block_buf_pos_);
  }
  return ret;
}

void ObTableLoadBackupSSTableBlockReader::reset()
{
  buf_ = nullptr;
  buf_size_ = 0;
  pos_ = 0;
  macro_block_buf_ = nullptr;
  macro_block_buf_size_ = 0;
  macro_block_buf_pos_ = 0;
  decomp_buf_ = nullptr;
  decomp_buf_size_ = 0;
  backup_common_header_ = nullptr;
  macro_block_common_header_.reset();
  sstable_macro_block_header_ = nullptr;
  micro_indexes_ = nullptr;
  lob_micro_indexes_ = nullptr;
  column_ids_ = nullptr;
  column_types_ = nullptr;
  column_orders_ = nullptr;
  column_checksum_ = nullptr;
  columns_.reset();
  if (nullptr != col_map_) {
    col_map_->~ObIColumnMap();
    allocator_.free(col_map_);
    col_map_ = nullptr;
  }
  compressor_ = nullptr;
  allocator_.reset();
  cur_block_idx_ = 0;
  micro_block_data_.reset();
  backup_version_ = ObTableLoadBackupVersion::INVALID;
  is_inited_ = false;
}

int ObTableLoadBackupSSTableBlockReader::get_next_micro_block(const ObMicroBlockData *&micro_block_data)
{
  int ret = OB_SUCCESS;
  micro_block_data = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (sstable_macro_block_header_ == nullptr || cur_block_idx_ == sstable_macro_block_header_->micro_block_count_) {
    ret = OB_ITER_END;
  } else {
    const char *micro_block_buf = nullptr;
    int64_t micro_block_size = 0;
    int64_t header_magic = 0;
    int64_t micro_block_pos = 0;
    ObRecordHeaderV3 header;
    const char *payload_buf = nullptr;
    int64_t payload_size = 0;
    bool is_compressed = false;
    if (macro_block_common_header_.is_sstable_data_block()) {
      if (backup_version_ == ObTableLoadBackupVersion::V_2_X_LOG || backup_version_ == ObTableLoadBackupVersion::V_2_X_PHY) {
        micro_indexes_ = reinterpret_cast<const ObMicroBlockIndex *>(macro_block_buf_ + macro_block_meta_v2_.micro_block_index_offset_);
        micro_block_buf = macro_block_buf_ + micro_indexes_[cur_block_idx_].data_offset_ + macro_block_meta_v2_.micro_block_data_offset_;
      } else {
        micro_indexes_ = reinterpret_cast<const ObMicroBlockIndex *>(macro_block_buf_ + sstable_macro_block_header_->micro_block_index_offset_);
        micro_block_buf = macro_block_buf_ + micro_indexes_[cur_block_idx_].data_offset_ + sstable_macro_block_header_->micro_block_data_offset_;
      }
      micro_block_size = micro_indexes_[cur_block_idx_ + 1].data_offset_ - micro_indexes_[cur_block_idx_].data_offset_;
      header_magic = MICRO_BLOCK_HEADER_MAGIC;
    } else {
      if (backup_version_ == ObTableLoadBackupVersion::V_2_X_LOG || backup_version_ == ObTableLoadBackupVersion::V_2_X_PHY) {
        lob_micro_indexes_ = reinterpret_cast<const ObLobMicroBlockIndex *>(macro_block_buf_ + macro_block_meta_v2_.micro_block_index_offset_);
        micro_block_buf = macro_block_buf_ + lob_micro_indexes_[cur_block_idx_].data_offset_ + macro_block_meta_v2_.micro_block_data_offset_;
      } else {
        lob_micro_indexes_ = reinterpret_cast<const ObLobMicroBlockIndex *>(macro_block_buf_ + sstable_macro_block_header_->micro_block_index_offset_);
        micro_block_buf = macro_block_buf_ + lob_micro_indexes_[cur_block_idx_].data_offset_ + sstable_macro_block_header_->micro_block_data_offset_;
      }
      micro_block_size = lob_micro_indexes_[cur_block_idx_ + 1].data_offset_ - lob_micro_indexes_[cur_block_idx_].data_offset_;
      header_magic = LOB_MICRO_BLOCK_HEADER_MAGIC;
    }
    if (OB_FAIL(header.deserialize(micro_block_buf, micro_block_size, micro_block_pos))) {
      LOG_WARN("fail to deserialize record header", KR(ret));
    } else if (OB_FAIL(header.check_and_get_record(micro_block_buf, micro_block_size, header_magic, payload_buf, payload_size))) {
      LOG_WARN("micro block data is corrupted", KR(ret), KP(micro_block_buf), K(micro_block_size));
    } else if (OB_UNLIKELY(sstable_macro_block_header_->encrypt_id_ > 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("encryption block is not supported", KR(ret), K(sstable_macro_block_header_->encrypt_id_));
    } else {
      is_compressed = header.is_compressed_data();
      micro_block_data_.store_type_ = (ObRowStoreType) sstable_macro_block_header_->row_store_type_;
      if (is_compressed) {
        if (compressor_ == nullptr || strcmp(compressor_->get_compressor_name(), sstable_macro_block_header_->compressor_name_)) {
          if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(sstable_macro_block_header_->compressor_name_, compressor_))) {
            LOG_WARN("fail to get compressor", KR(ret), K(sstable_macro_block_header_->compressor_name_));
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t data_length = header.data_length_;
          int64_t decomp_size;
          if (OB_FAIL(alloc_buf(data_length))) {
            LOG_WARN("Fail to allocate buf, ", KR(ret));
          } else if (OB_FAIL(compressor_->decompress(
              payload_buf, payload_size, decomp_buf_, decomp_buf_size_, decomp_size))) {
            LOG_WARN("compressor fail to decompress.", KR(ret));
          } else {
            micro_block_data_.buf_ = decomp_buf_;
            micro_block_data_.size_ = data_length;
            micro_block_data = &micro_block_data_;
            cur_block_idx_++;
          }
        }
      } else {
        micro_block_data_.buf_ = payload_buf;
        micro_block_data_.size_ = payload_size;
        micro_block_data = &micro_block_data_;
        cur_block_idx_++;
      }
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockReader::alloc_buf(const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (decomp_buf_ == nullptr || decomp_buf_size_ < buf_size) {
    buf_allocator_.reuse();
    if (OB_ISNULL(decomp_buf_ = static_cast<char*>(buf_allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(buf_size));
    } else {
      decomp_buf_size_ = buf_size;
    }
  }
  return ret;
}

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
