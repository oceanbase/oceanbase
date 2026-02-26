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
#include "observer/table_load/backup/ob_table_load_backup_macro_block_reader.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;

ObTableLoadBackupMacroBlockReader::ObTableLoadBackupMacroBlockReader()
  : allocator_("TLD_BMaBReader"),
    micro_index_(nullptr),
    compressor_(nullptr),
    buf_(nullptr),
    buf_size_(0),
    decomp_buf_(nullptr),
    decomp_buf_size_(0),
    pos_(0),
    micro_offset_(0),
    cur_block_idx_(0),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

int ObTableLoadBackupMacroBlockReader::init(
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

int ObTableLoadBackupMacroBlockReader::inner_init()
{
  int ret = OB_SUCCESS;
  int64_t meta_len = 0;
  int64_t macro_block_size = 0;
  ObColumnMapV1 *col_map = nullptr;
  if (OB_ISNULL(meta_.endkey_ = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * ObColumnMapV1::OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc_memory", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf_, buf_size_, pos_, &meta_len))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf_), K(buf_size_), K(pos_));
  } else if (OB_FAIL(meta_.deserialize(buf_, buf_size_, pos_))) {
    LOG_WARN("fail to deserialize meta_", KR(ret), KP(buf_), K(buf_size_), K(pos_));
  } else if (OB_FAIL(serialization::decode_i64(buf_, buf_size_, pos_, &macro_block_size))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf_), K(buf_size_), K(pos_));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(meta_.compressor_, compressor_))) {
    LOG_WARN("fail to get compressor_", KR(ret), K(meta_.compressor_));
  } else if (OB_FAIL(col_map_.init(&meta_))) {
    LOG_WARN("fail to init col map", KR(ret));
  } else {
    micro_index_ = reinterpret_cast<const ObMicroBlockIndex *>(buf_ + pos_ + meta_.micro_block_index_offset_);
  }
  if (meta_.endkey_ != nullptr) {
    allocator_.free(meta_.endkey_);
    meta_.endkey_ = nullptr;
  }
  return ret;
}

void ObTableLoadBackupMacroBlockReader::reset()
{
  col_map_.reset();
  micro_index_ = nullptr;
  compressor_ = nullptr;
  buf_ = nullptr;
  buf_size_ = 0;
  decomp_buf_ = nullptr;
  decomp_buf_size_ = 0;
  pos_ = 0;
  allocator_.reset();
  cur_block_idx_ = 0;
  backup_version_ = ObTableLoadBackupVersion::INVALID;
  is_inited_ = false;
}

int ObTableLoadBackupMacroBlockReader::get_next_micro_block(const ObMicroBlockData *&micro_block_data)
{
  int ret = OB_SUCCESS;
  micro_block_data = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(cur_block_idx_ == meta_.micro_block_count_)) {
    ret = OB_ITER_END;
  } else {
    if (cur_block_idx_ == 0) {
      micro_offset_ = meta_.micro_block_data_offset_;
    } else {
      micro_offset_ += micro_index_[cur_block_idx_].data_offset_ - micro_index_[cur_block_idx_ - 1].data_offset_;
    }
    int64_t size = micro_index_[cur_block_idx_ + 1].data_offset_ - micro_index_[cur_block_idx_].data_offset_;
    if (OB_UNLIKELY(size <= (int64_t)sizeof(ObRecordHeaderV2))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(cur_block_idx_), K(size));
    } else if (OB_FAIL(ObRecordHeaderV2::check_record(buf_ + pos_ + micro_offset_, size))) {
      LOG_WARN("fail to check record", KR(ret));
    } else {
      micro_block_data_.store_type_ = ObRowStoreType::FLAT_ROW_STORE;
      const ObRecordHeaderV2 *header = nullptr;
      header = reinterpret_cast<const ObRecordHeaderV2*>(buf_ + pos_ + micro_offset_);
      const char *comp_buf = buf_ + pos_ + micro_offset_ + sizeof(ObRecordHeaderV2);
      int64_t comp_size = size - sizeof(ObRecordHeaderV2);
      if (header->is_compressed_data()) {
        int64_t decomp_size = 0;
        if (OB_FAIL(alloc_buf(header->data_length_))) {
          LOG_WARN("fail to allocate buf", KR(ret));
        } else if (OB_FAIL(compressor_ != nullptr && compressor_->decompress(comp_buf, comp_size, decomp_buf_, decomp_buf_size_,  decomp_size))) {
          LOG_WARN("fail to decompress", KR(ret));
        } else {
          micro_block_data_.buf_ = decomp_buf_;
          micro_block_data_.size_ = decomp_size;
          micro_block_data = &micro_block_data_;
        }
      } else {
        micro_block_data_.buf_ = comp_buf;
        micro_block_data_.size_ = comp_size;
        micro_block_data = &micro_block_data_;
      }
      cur_block_idx_++;
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockReader::alloc_buf(const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (decomp_buf_ == nullptr || decomp_buf_size_ < buf_size) {
    allocator_.reuse();
    if (OB_ISNULL(decomp_buf_ = static_cast<char*>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(buf_size));
    } else {
      decomp_buf_size_ = buf_size;
    }
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
