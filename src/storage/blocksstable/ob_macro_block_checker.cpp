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

#include "share/ob_define.h"
#include "ob_macro_block_checker.h"
#include "ob_store_file.h"
#include "storage/compaction/ob_micro_block_iterator.h"
#include "storage/blocksstable/ob_lob_struct.h"
#include "storage/blocksstable/ob_lob_micro_block_index_reader.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {

void ObSSTableMacroBlockChecker::destroy()
{
  flat_reader_.reset();
  column_map_.reset();
  allocator_.reset();
}

int ObSSTableMacroBlockChecker::check(const char* macro_block_buf, const int64_t macro_block_buf_size,
    const ObFullMacroBlockMeta& meta, ObMacroBlockCheckLevel check_level)
{
  int ret = OB_SUCCESS;
  allocator_.reuse();
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;
  bool need_check_macro = false;
  bool need_check_micro = false;
  bool need_check_row = false;
  if (NULL == macro_block_buf || macro_block_buf_size <= 0 || !meta.is_valid() || check_level >= CHECK_LEVEL_MAX) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "Invalid argument, ", K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(meta), K(check_level));
  } else if (CHECK_LEVEL_NOTHING == check_level) {
    // do nothing
  } else if (OB_FAIL(common_header.deserialize(macro_block_buf, macro_block_buf_size, pos))) {
    STORAGE_LOG(ERROR,
        "Deserialize common header failed, ",
        K(ret),
        KP(macro_block_buf),
        K(macro_block_buf_size),
        K(pos),
        K(common_header));
  } else if (OB_FAIL(common_header.check_integrity())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(ERROR, "Invalid common header, ", K(ret), K(common_header), K(meta));
  } else {
    const bool can_check_macro = common_header.get_payload_size() > 0;
    switch (check_level) {
      case CHECK_LEVEL_MACRO: {
        need_check_macro = true;
        need_check_micro = false;
        need_check_row = false;
        break;
      }
      case CHECK_LEVEL_MICRO: {
        need_check_macro = true;
        need_check_micro = true;
        need_check_row = false;
        break;
      }
      case CHECK_LEVEL_ROW: {
        need_check_macro = true;
        need_check_micro = true;
        need_check_row = true;
        break;
      }
      case CHECK_LEVEL_AUTO: {
        UNUSED(can_check_macro);
        // FIXME(@wenqu): only verify micro block for now
        need_check_macro = false;
        need_check_micro = true;
        need_check_row = false;  // micro level checksum is always available, so row level check is no need.
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "check level not supported", K(ret), K(check_level));
        break;
      }
    }
  }
  if (OB_SUCC(ret) && need_check_macro) {
    if (OB_FAIL(check_macro_buf(common_header, macro_block_buf, macro_block_buf_size))) {
      STORAGE_LOG(WARN,
          "fail to check physical checksum",
          K(ret),
          K(common_header),
          KP(macro_block_buf),
          K(macro_block_buf_size));
    }
  }
  if (OB_SUCC(ret) && need_check_micro) {
    if (!meta.meta_->is_data_block()) {
      // do nothing
    } else if (OB_FAIL(check_data_header(common_header, macro_block_buf, macro_block_buf_size, meta))) {
      STORAGE_LOG(WARN,
          "Fail to check macro block header, ",
          K(ret),
          K(common_header),
          KP(macro_block_buf),
          K(macro_block_buf_size),
          K(meta));
    } else if (meta.meta_->is_sstable_data_block()) {
      if (OB_FAIL(check_data_block(macro_block_buf, macro_block_buf_size, meta, need_check_row))) {
        STORAGE_LOG(WARN, "Failed to check sstable macro block", K(ret));
      }
    } else if (meta.meta_->is_lob_data_block()) {
      if (OB_FAIL(check_lob_block(macro_block_buf, macro_block_buf_size, meta))) {
        STORAGE_LOG(WARN, "Failed to check lob macro block", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_macro_buf(
    const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const int64_t macro_block_buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == macro_block_buf || macro_block_buf_size <= 0 || !common_header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(common_header));
  } else if (common_header.get_payload_size() != 0) {
    const int64_t header_size = common_header.get_serialize_size();
    if (common_header.get_payload_size() > (macro_block_buf_size - header_size)) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(ERROR, "Invalid payload size, ", K(ret), K(common_header));
    } else {
      const int32_t physical_checksum =
          static_cast<int32_t>(ob_crc64(macro_block_buf + header_size, common_header.get_payload_size()));
      if (physical_checksum != common_header.get_payload_checksum()) {
        ret = OB_PHYSIC_CHECKSUM_ERROR;
        STORAGE_LOG(
            ERROR, "Invalid physical checksum", K(ret), K(physical_checksum), K(common_header.get_payload_checksum()));
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_data_block(const char* macro_block_buf, const int64_t macro_block_buf_size,
    const ObFullMacroBlockMeta& meta, const bool need_check_row)
{
  int ret = OB_SUCCESS;
  compaction::ObMacroBlockLoader macro_loader;
  int64_t* column_checksum = NULL;
  if (OB_FAIL(macro_loader.init(macro_block_buf, macro_block_buf_size, &meta))) {
    STORAGE_LOG(WARN, "Fail to load macro block", K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(meta));
  } else {
    // verify every micro block
    blocksstable::ObMicroBlock micro_block;
    int64_t checksum = 0;
    bool need_verify_column_checksum = need_check_row &&
                                       (CCM_TYPE_AND_VALUE == meta.meta_->column_checksum_method_ ||
                                           CCM_VALUE_ONLY == meta.meta_->column_checksum_method_) &&
                                       meta.meta_->column_number_ > 0 && meta.meta_->column_checksum_[0] != 0;
    if (need_verify_column_checksum) {
      const int64_t checksum_buf_size = sizeof(int64_t) * meta.meta_->column_number_;
      column_checksum = static_cast<int64_t*>(allocator_.alloc(checksum_buf_size));  // reuse in check()
      if (OB_ISNULL(column_checksum)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory for column checksum", K(ret), K(checksum_buf_size));
      } else {
        MEMSET(column_checksum, 0, checksum_buf_size);
      }
    }
    while (OB_SUCC(ret)) {
      int64_t pos = 0;
      ObRecordHeaderV3 header;
      if (OB_FAIL(macro_loader.next_micro_block(micro_block))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next micro block, ", K(ret));
        }
      } else if (!micro_block.data_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Wrong micro buf, ", K(ret), K(micro_block.data_));
      } else if (OB_FAIL(header.deserialize(micro_block.data_.get_buf(), micro_block.data_.get_buf_size(), pos))) {
        STORAGE_LOG(WARN, "fail to deserialize record header", K(ret), K(pos));
      } else if (OB_FAIL(header.check_header_checksum())) {
        STORAGE_LOG(WARN, "fail to verify record header checksum", K(ret), K(header));
      } else if (FALSE_IT(checksum = ob_crc64_sse42(checksum, &header.data_checksum_, sizeof(header.data_checksum_)))) {
      } else if (need_verify_column_checksum) {
        const char* uncomp_buf = NULL;
        int64_t uncomp_size = 0;
        bool is_compressed = false;
        if (OB_FAIL(macro_reader_.decompress_data(meta,
                micro_block.data_.get_buf(),
                micro_block.data_.get_buf_size(),
                uncomp_buf,
                uncomp_size,
                is_compressed))) {
          STORAGE_LOG(WARN, "fail to decompress data", K(ret));
        } else if (OB_FAIL(check_micro_data(uncomp_buf, uncomp_size, meta, column_checksum))) {
          STORAGE_LOG(
              WARN, "fail to check micro data", K(ret), KP(uncomp_buf), K(uncomp_size), K(meta), KP(column_checksum));
        }
      }
    }
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next micro block buf, ", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (checksum != meta.meta_->data_checksum_) {
        ret = OB_PHYSIC_CHECKSUM_ERROR;
        STORAGE_LOG(ERROR, "Macro block checksum error, ", K(ret), K(meta.meta_->data_checksum_), K(checksum), K(meta));
      } else if (need_verify_column_checksum) {
        for (int64_t i = 0; OB_SUCC(ret) && i < meta.meta_->column_number_; ++i) {
          if (meta.meta_->column_checksum_[i] != column_checksum[i]) {
            ret = OB_PHYSIC_CHECKSUM_ERROR;
            STORAGE_LOG(ERROR, "Column checksum error", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_lob_block(
    const char* macro_block_buf, const int64_t macro_block_buf_size, const ObFullMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  ObLobMicroBlockIndexReader micro_index_reader;

  if (OB_FAIL(micro_index_reader.transform(macro_block_buf + meta.meta_->micro_block_index_offset_,
          static_cast<int32_t>(meta.meta_->micro_block_endkey_offset_ - meta.meta_->micro_block_index_offset_),
          meta.meta_->micro_block_count_))) {
    STORAGE_LOG(WARN,
        "Failed to transform micro block index reader",
        K(ret),
        "size_array_offset",
        meta.meta_->micro_block_endkey_offset_,
        "index_offset",
        meta.meta_->micro_block_index_offset_);
  } else {
    ObLobMicroIndexItem index_item;
    int64_t checksum = 0;
    const char* data_buf = macro_block_buf + meta.meta_->micro_block_data_offset_;
    ObRecordHeaderV3 record_header;
    int64_t pos = 0;
    while (OB_SUCC(ret) && OB_SUCC(micro_index_reader.get_next_index_item(index_item))) {
      if (meta.meta_->micro_block_data_offset_ + index_item.offset_ + index_item.data_size_ > macro_block_buf_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected lob micro block", K(index_item), K(macro_block_buf_size), K(meta), K(ret));
      } else if (OB_FAIL(record_header.deserialize(data_buf + index_item.offset_, index_item.data_size_, pos))) {
        STORAGE_LOG(WARN, "fail to deserialize record header", K(ret));
      } else if (OB_FAIL(record_header.check_record(
                     data_buf + index_item.offset_, index_item.data_size_, LOB_MICRO_BLOCK_HEADER_MAGIC))) {
        STORAGE_LOG(WARN, "fail to check record", K(ret));
      } else {
        checksum = ob_crc64_sse42(checksum, &record_header.data_checksum_, sizeof(record_header.data_checksum_));
      }
    }
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next micro block buf", K(ret));
    } else if (checksum != meta.meta_->data_checksum_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      STORAGE_LOG(ERROR, "Macro block checksum error", K(ret), K(meta.meta_->data_checksum_), K(checksum), K(meta));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSSTableMacroBlockChecker::check_sstable_data_header(
    const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const ObFullMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(macro_block_buf) || !meta.is_valid() || !common_header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(meta));
  } else {
    // check sstable header with macro block meta, only check critical members while reading data
    const ObSSTableMacroBlockHeader* sstable_header =
        reinterpret_cast<const ObSSTableMacroBlockHeader*>(macro_block_buf + common_header.get_serialize_size());
    if (!common_header.is_sstable_data_block() || sstable_header->column_count_ != meta.meta_->column_number_ ||
        strcmp(sstable_header->compressor_name_, meta.schema_->compressor_) != 0 ||
        sstable_header->data_checksum_ != meta.meta_->data_checksum_ ||
        sstable_header->magic_ != SSTABLE_DATA_HEADER_MAGIC ||
        sstable_header->micro_block_count_ != meta.meta_->micro_block_count_ ||
        sstable_header->micro_block_data_offset_ != meta.meta_->micro_block_data_offset_ ||
        sstable_header->micro_block_endkey_offset_ != meta.meta_->micro_block_endkey_offset_ ||
        sstable_header->micro_block_index_offset_ != meta.meta_->micro_block_index_offset_ ||
        sstable_header->rowkey_column_count_ != meta.meta_->rowkey_column_number_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      STORAGE_LOG(ERROR, "Check error of sstable header", K(ret), K(*sstable_header), K(meta), K(common_header));
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_lob_data_header(
    const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const ObFullMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(macro_block_buf) || !meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(meta));
  } else {
    const ObLobMacroBlockHeader* lob_header =
        reinterpret_cast<const ObLobMacroBlockHeader*>(macro_block_buf + common_header.get_serialize_size());
    if (!(common_header.is_lob_data_block() || common_header.get_attr() == 0) ||
        lob_header->column_count_ != meta.meta_->column_number_ ||
        strcmp(lob_header->compressor_name_, meta.schema_->compressor_) != 0 ||
        (lob_header->version_ != LOB_MACRO_BLOCK_HEADER_VERSION_V1 &&
            lob_header->version_ != LOB_MACRO_BLOCK_HEADER_VERSION_V2) ||
        lob_header->magic_ != LOB_MACRO_BLOCK_HEADER_MAGIC ||
        lob_header->data_checksum_ != meta.meta_->data_checksum_ ||
        lob_header->micro_block_count_ != meta.meta_->micro_block_count_ ||
        lob_header->micro_block_data_offset_ != meta.meta_->micro_block_data_offset_ ||
        lob_header->micro_block_size_array_offset_ != meta.meta_->micro_block_endkey_offset_ ||
        lob_header->micro_block_index_offset_ != meta.meta_->micro_block_index_offset_ ||
        lob_header->rowkey_column_count_ != meta.meta_->rowkey_column_number_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      STORAGE_LOG(ERROR, "Check error of lob macro block header", K(ret), K(*lob_header), K(meta), K(common_header));
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_bloomfilter_data_header(
    const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const ObFullMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(macro_block_buf) || !meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(meta));
  } else {
    const ObBloomFilterMacroBlockHeader* bf_header =
        reinterpret_cast<const ObBloomFilterMacroBlockHeader*>(macro_block_buf + common_header.get_serialize_size());
    if (!common_header.is_bloom_filter_data_block() || bf_header->attr_ != ObMacroBlockCommonHeader::BloomFilterData ||
        bf_header->micro_block_count_ != 1 || strcmp(bf_header->compressor_name_, meta.schema_->compressor_) != 0 ||
        bf_header->version_ != BF_MACRO_BLOCK_HEADER_VERSION || bf_header->magic_ != BF_MACRO_BLOCK_HEADER_MAGIC ||
        bf_header->data_checksum_ != meta.meta_->data_checksum_ || bf_header->row_count_ != meta.meta_->row_count_ ||
        bf_header->table_id_ != meta.meta_->table_id_ || bf_header->partition_id_ != meta.meta_->partition_id_ ||
        bf_header->micro_block_count_ != meta.meta_->micro_block_count_ ||
        bf_header->micro_block_data_offset_ != meta.meta_->micro_block_data_offset_ ||
        (bf_header->micro_block_data_offset_ + bf_header->micro_block_data_size_) !=
            meta.meta_->micro_block_index_offset_ ||
        bf_header->rowkey_column_count_ != meta.meta_->rowkey_column_number_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      STORAGE_LOG(
          ERROR, "Check error of bloomfilter macro block header", K(ret), K(*bf_header), K(meta), K(common_header));
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_data_header(const ObMacroBlockCommonHeader& common_header,
    const char* macro_block_buf, const int64_t macro_block_buf_size, const ObFullMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  if (NULL == macro_block_buf || macro_block_buf_size <= 0 || !meta.is_valid() || !common_header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(meta), K(common_header));
  } else if (meta.meta_->is_sstable_data_block()) {
    ret = check_sstable_data_header(common_header, macro_block_buf, meta);
  } else if (meta.meta_->is_lob_data_block()) {
    ret = check_lob_data_header(common_header, macro_block_buf, meta);
  } else if (meta.meta_->is_bloom_filter_data_block()) {
    ret = check_bloomfilter_data_header(common_header, macro_block_buf, meta);
  } else {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(ERROR, "Invalid common header", K(ret), K(common_header), K(meta));
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_micro_data(
    const char* micro_buf, const int64_t micro_buf_size, const ObFullMacroBlockMeta& meta, int64_t* checksum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_buf) || micro_buf_size <= 0 || !meta.is_valid() || OB_ISNULL(checksum) ||
      meta.meta_->row_store_type_ >= MAX_ROW_STORE ||
      (meta.meta_->column_checksum_method_ != CCM_TYPE_AND_VALUE &&
          meta.meta_->column_checksum_method_ != CCM_VALUE_ONLY)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), KP(micro_buf), K(micro_buf_size), K(meta));
  } else if (OB_FAIL(build_column_map(meta))) {
    STORAGE_LOG(WARN, "fail to build column map", K(ret));
  } else {
    ObIMicroBlockReader* reader = NULL;
    ObRowStoreType read_out_type = MAX_ROW_STORE;
    ObColumnMap* column_map_ptr = nullptr;
    if (FLAT_ROW_STORE == meta.meta_->row_store_type_) {
      reader = static_cast<ObIMicroBlockReader*>(&flat_reader_);
      read_out_type = FLAT_ROW_STORE;
      column_map_ptr = &column_map_;
    } else if (SPARSE_ROW_STORE == meta.meta_->row_store_type_) {
      reader = static_cast<ObIMicroBlockReader*>(&sparse_reader_);
      read_out_type = SPARSE_ROW_STORE;  // write row type is sparse row
      column_map_ptr = nullptr;          // make reader read full sparse row
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "Unexpeceted row store type", K(ret), K(meta.meta_->row_store_type_));
    }
    if (OB_SUCC(ret)) {
      ObMicroBlockData block_data(micro_buf, micro_buf_size);
      if (OB_FAIL(reader->init(block_data, column_map_ptr, read_out_type))) {
        STORAGE_LOG(WARN, "fail to init micro block reader ", K(ret), K(block_data), K(meta));
      } else {
        ObStoreRow row;
        for (int64_t iter = reader->begin(); OB_SUCC(ret) && iter != reader->end(); ++iter) {
          row.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
          row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
          row.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
          if (OB_FAIL(reader->get_row(iter, row))) {
            STORAGE_LOG(WARN, "fail to get row", K(ret), K(iter));
          } else if (row.row_val_.count_ != meta.meta_->column_number_) {
            ret = OB_INVALID_DATA;
            STORAGE_LOG(WARN, "column number not match", K(ret));
          } else {
            for (int64_t i = 0; i < meta.meta_->column_number_; ++i) {
              if (meta.meta_->column_checksum_method_ == CCM_TYPE_AND_VALUE) {
                checksum[i] += row.row_val_.cells_[i].checksum(0);
              } else if (meta.meta_->column_checksum_method_ == CCM_VALUE_ONLY) {
                checksum[i] += row.row_val_.cells_[i].checksum_v2(0);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::build_column_map(const ObFullMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  column_map_.reset();
  if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(meta));
  } else {
    share::schema::ObColDesc col_desc;
    ObArray<share::schema::ObColDesc> out_cols;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.meta_->column_number_; ++i) {
      col_desc.col_id_ = meta.schema_->column_id_array_[i];
      col_desc.col_type_ = meta.schema_->column_type_array_[i];
      col_desc.col_order_ = meta.schema_->column_order_array_[i];
      if (OB_FAIL(out_cols.push_back(col_desc))) {
        STORAGE_LOG(WARN, "Fail to push col desc to array, ", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(column_map_.init(allocator_,
                   meta.meta_->schema_version_,
                   meta.schema_->schema_rowkey_col_cnt_,
                   meta.meta_->column_number_,
                   out_cols))) {
      STORAGE_LOG(WARN,
          "fail to init column map",
          K(ret),
          K(meta.meta_->schema_version_),
          K(meta.schema_->schema_rowkey_col_cnt_),
          K(meta.meta_->column_number_),
          K(out_cols));
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
