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
#include "ob_macro_block_reader.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/compaction/ob_micro_block_iterator.h"
#include "lib/compress/ob_compressor_pool.h"
#include "ob_micro_block_index_reader.h"
#include "ob_sstable_printer.h"
#include "ob_micro_block_scanner.h"
#include "ob_lob_micro_block_index_reader.h"
#include "ob_macro_block.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace compaction;
namespace blocksstable {

ObMacroBlockReader::ObMacroBlockReader()
    : compressor_(NULL), uncomp_buf_(NULL), uncomp_buf_size_(0), allocator_(ObModIds::OB_CS_SSTABLE_READER)
{}

ObMacroBlockReader::~ObMacroBlockReader()
{
  allocator_.clear();
}

int ObMacroBlockReader::decompress_data(const char* compressor_name, const char* buf, const int64_t size,
    const char*& uncomp_buf, int64_t& uncomp_size, bool& is_compressed)
{
  int ret = OB_SUCCESS;
  ObRecordHeaderV3 header;
  int64_t header_size = 0;
  int64_t pos = 0;
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to decompress data", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(header.deserialize(buf, size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize record header", K(ret));
  } else if (size < (header_size = header.get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(size), "header_size", header.get_serialize_size());
  } else {
    const char* comp_buf = buf + header_size;
    int64_t comp_size = size - header_size;
    is_compressed = header.is_compressed_data();
    if (is_compressed) {
      if (NULL == compressor_ || strcmp(compressor_->get_compressor_name(), compressor_name)) {
        if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_name, compressor_))) {
          STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), "compressor_name", compressor_name);
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t data_length = header.data_length_;
        if (OB_FAIL(alloc_buf(data_length))) {
          STORAGE_LOG(WARN, "Fail to allocate buf, ", K(ret));
        } else if (OB_FAIL(compressor_->decompress(comp_buf, comp_size, uncomp_buf_, uncomp_buf_size_, uncomp_size))) {
          STORAGE_LOG(WARN, "compressor fail to decompress.", K(ret));
        } else {
          uncomp_buf = uncomp_buf_;
        }
      }
    } else {
      uncomp_buf = comp_buf;
      uncomp_size = comp_size;
    }
  }

  return ret;
}

int ObMacroBlockReader::decompress_data(const ObFullMacroBlockMeta& meta, const char* buf, const int64_t size,
    const char*& uncomp_buf, int64_t& uncomp_size, bool& is_compressed, const bool need_deep_copy)
{
  int ret = OB_SUCCESS;

  if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(meta));
  } else if (OB_FAIL(decompress_data(meta.schema_->compressor_, buf, size, uncomp_buf, uncomp_size, is_compressed))) {
    STORAGE_LOG(WARN, "Failed to decompress data", K(ret));
  } else if (need_deep_copy && !is_compressed) {
    if (OB_FAIL(alloc_buf(uncomp_size))) {
      STORAGE_LOG(WARN, "Fail to allocate buf for deepcopy", K(uncomp_size), K(ret));
    } else {
      MEMCPY(uncomp_buf_, uncomp_buf, uncomp_size);
      uncomp_buf = uncomp_buf_;
    }
  }
  return ret;
}

int ObMacroBlockReader::decompress_data_with_prealloc_buf(
    const char* compressor_name, const char* buf, const int64_t size, char* uncomp_buf, const int64_t uncomp_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0) || OB_ISNULL(uncomp_buf) || OB_UNLIKELY(uncomp_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for decompress_data_with_prealloc_buf", K(ret), KP(buf), K(size));
  } else if (size == uncomp_buf_size) {
    MEMCPY(uncomp_buf, buf, size);
  } else {
    if (OB_ISNULL(compressor_) || strcmp(compressor_->get_compressor_name(), compressor_name)) {
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_name, compressor_))) {
        STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), "compressor_name", compressor_name);
      }
    }
    if (OB_SUCC(ret)) {
      int64_t uncomp_size;
      if (OB_FAIL(compressor_->decompress(buf, size, uncomp_buf, uncomp_buf_size, uncomp_size))) {
        LOG_WARN("failed to decompress data", K(ret));
      } else if (OB_UNLIKELY(uncomp_size != uncomp_buf_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("uncomp size is not equal", K(ret), K(uncomp_size), K(uncomp_buf_size));
      }
    }
  }
  return ret;
}

int ObMacroBlockReader::alloc_buf(const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == uncomp_buf_ || uncomp_buf_size_ < buf_size) {
    allocator_.reuse();
    if (NULL == (uncomp_buf_ = static_cast<char*>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "Fail to allocate memory for uncomp_buf, ", K(buf_size), K(ret));
    } else {
      uncomp_buf_size_ = buf_size;
    }
  }
  return ret;
}

ObSSTableDataBlockReader::ObSSTableDataBlockReader()
    : data_(NULL),
      size_(0),
      common_header_(),
      block_header_(NULL),
      column_ids_(NULL),
      column_types_(NULL),
      column_orders_(NULL),
      column_checksum_(NULL),
      macro_reader_(),
      allocator_(ObModIds::OB_CS_SSTABLE_READER),
      is_trans_sstable_(false),
      is_inited_(false)
{}

ObSSTableDataBlockReader::~ObSSTableDataBlockReader()
{}

int ObSSTableDataBlockReader::init(const char* data, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(data) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size));
  } else if (OB_FAIL(common_header_.deserialize(data, size, pos))) {
    STORAGE_LOG(ERROR, "deserialize common header fail", K(ret), KP(data), K(size), K(pos));
  } else if (OB_FAIL(common_header_.check_integrity())) {
    STORAGE_LOG(ERROR, "invalid common header", K(ret), K_(common_header));
  } else {
    data_ = data;
    size_ = size;
    block_header_ = reinterpret_cast<const ObSSTableMacroBlockHeader*>(data_ + pos);
    if (common_header_.is_sstable_data_block()) {
      pos += sizeof(ObSSTableMacroBlockHeader);
    } else if (common_header_.is_lob_data_block()) {
      pos += sizeof(ObLobMacroBlockHeader);
    } else if (common_header_.is_bloom_filter_data_block()) {
      pos += sizeof(ObBloomFilterMacroBlockHeader);
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(ERROR, "Unexpected macro block type", K(ret), K_(common_header));
    }
    if (OB_SUCC(ret) && !common_header_.is_bloom_filter_data_block()) {
      const int64_t column_cnt = block_header_->column_count_;
      column_ids_ = reinterpret_cast<const uint16_t*>(data_ + pos);
      pos += sizeof(uint16_t) * column_cnt;
      column_types_ = reinterpret_cast<const ObObjMeta*>(data_ + pos);
      pos += sizeof(ObObjMeta) * column_cnt;
      if ((common_header_.is_sstable_data_block() &&
              block_header_->version_ >= SSTABLE_MACRO_BLOCK_HEADER_VERSION_v3) ||
          (common_header_.is_lob_data_block() && block_header_->version_ >= LOB_MACRO_BLOCK_HEADER_VERSION_V2)) {
        column_orders_ = reinterpret_cast<const ObOrderType*>(data_ + pos);
        pos += sizeof(ObOrderType) * column_cnt;
      }
      column_checksum_ = reinterpret_cast<const int64_t*>(data_ + pos);
      pos += sizeof(int64_t) * column_cnt;

      if (block_header_->micro_block_data_offset_ != pos) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("incorrect data offset", K(ret), K(pos), K(*block_header_));
      }

      if (OB_SUCC(ret)) {
        share::schema::ObColDesc col_desc;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
          col_desc.col_id_ = column_ids_[i];
          col_desc.col_type_ = column_types_[i];
          if (OB_FAIL(columns_.push_back(col_desc))) {
            LOG_WARN("Fail to push col desc to columns, ", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

void ObSSTableDataBlockReader::reset()
{
  data_ = NULL;
  size_ = 0;
  common_header_.reset();
  block_header_ = NULL;
  column_ids_ = NULL;
  column_types_ = NULL;
  column_orders_ = NULL;
  column_checksum_ = NULL;
  allocator_.reset();
  is_inited_ = false;
}

int ObSSTableDataBlockReader::dump()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSTableDataBlockReader is not inited", K(ret));
  } else if (OB_FAIL(dump_macro_block_header())) {
    STORAGE_LOG(WARN, "Failed to dump macro block header", K(ret));
  } else if (common_header_.is_sstable_data_block()) {
    if (OB_FAIL(dump_sstable_data_block())) {
      STORAGE_LOG(WARN, "Failed to dump sstable macro block", K(ret));
    }
  } else if (common_header_.is_lob_data_block()) {
    if (OB_FAIL(dump_lob_data_block())) {
      STORAGE_LOG(WARN, "Failed to dump lob macro block", K(ret));
    }
  } else if (common_header_.is_bloom_filter_data_block()) {
    if (OB_FAIL(dump_bloom_filter_data_block())) {
      STORAGE_LOG(WARN, "Failed to dump bloom filter macro block", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "Unexpected macro block type", K(ret), K_(common_header));
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_macro_block_header()
{
  int ret = OB_SUCCESS;

  if (common_header_.is_bloom_filter_data_block()) {
    ObSSTablePrinter::print_macro_block_header(reinterpret_cast<const ObBloomFilterMacroBlockHeader*>(block_header_));
  } else {
    if (common_header_.is_sstable_data_block()) {
      ObSSTablePrinter::print_macro_block_header(block_header_);
    } else if (common_header_.is_lob_data_block()) {
      ObSSTablePrinter::print_macro_block_header(reinterpret_cast<const ObLobMacroBlockHeader*>(block_header_));
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(ERROR, "Unexpected macro block type", K(ret), K_(common_header));
    }
    if (OB_SUCC(ret) && block_header_->column_count_ > 0) {
      ObSSTablePrinter::print_cols_info_start("column_id", "column_type", "column_checksum", "collation_type");
      for (int64_t i = 0; i < block_header_->column_count_; ++i) {
        ObSSTablePrinter::print_cols_info_line(
            column_ids_[i], column_types_[i].get_type(), column_checksum_[i], column_types_[i].get_collation_type());
      }
    }
  }
  return ret;
}

int ObSSTableDataBlockReader::dump_sstable_data_block()
{
  int ret = OB_SUCCESS;

  ObMacroBlockLoader macro_loader;
  if (!block_header_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid sstable macro block header", K(ret), K_(block_header));
  } else if (OB_FAIL(col_map_.init(
                 allocator_, 0, block_header_->rowkey_column_count_, block_header_->column_count_, columns_))) {
    LOG_ERROR("Fail to init col map, ", K(ret));
  } else if (OB_FAIL(macro_loader.init(data_, size_, nullptr /*do not use macro block meta*/))) {
    STORAGE_LOG(WARN, "Failed to init macro block loader", K(ret));
  } else if (macro_loader.get_micro_block_infos().count() != block_header_->micro_block_count_ ||
             macro_loader.get_end_keys().count() != block_header_->micro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("count mismatch between micro block infos and end keys",
        K(ret),
        K(*block_header_),
        K(macro_loader.get_micro_block_infos().count()),
        K(macro_loader.get_end_keys().count()));
  } else {
    if (is_trans_table_id(block_header_->table_id_)) {
      is_trans_sstable_ = true;
    } else {
      is_trans_sstable_ = false;
    }

    blocksstable::ObMicroBlock micro_block;
    ObMicroBlockData micro_data;
    for (int64_t i = 0; i < block_header_->micro_block_count_; ++i) {
      ObSSTablePrinter::print_micro_index(
          macro_loader.get_end_keys().at(i), macro_loader.get_micro_block_infos().at(i));
    }
    while (OB_SUCC(ret) && OB_SUCC(macro_loader.next_micro_block(micro_block))) {
      if (OB_FAIL(decompressed_micro_block(
              micro_block.data_.get_buf(), micro_block.data_.get_buf_size(), MICRO_BLOCK_HEADER_MAGIC, micro_data))) {
        STORAGE_LOG(ERROR, "failed to get micro block data", K(ret));
      } else if (OB_UNLIKELY(!micro_data.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected micro data", K(micro_data), K(ret));
      } else if (OB_FAIL(dump_sstable_micro_header(micro_data.get_buf(), macro_loader.get_micro_index()))) {
        STORAGE_LOG(ERROR, "Failed to dump sstble micro block header", K(ret));
      } else if (OB_FAIL(dump_sstable_micro_data(micro_data))) {
        STORAGE_LOG(ERROR, "Failed to dump sstble micro block data", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_sstable_micro_header(const char* micro_block_buf, const int64_t micro_idx)
{
  int ret = OB_SUCCESS;
  const ObRowStoreType row_store_type = (ObRowStoreType)block_header_->row_store_type_;
  int64_t row_cnt = 0;

  if (ObRowStoreType::FLAT_ROW_STORE == row_store_type || ObRowStoreType::SPARSE_ROW_STORE == row_store_type) {
    const ObMicroBlockHeader* micro_block_header = reinterpret_cast<const ObMicroBlockHeader*>(micro_block_buf);
    ObSSTablePrinter::print_micro_header(micro_block_header);
    row_cnt = micro_block_header->row_count_;
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported store type", K(ret), K(row_store_type));
  }
  if (OB_SUCC(ret)) {
    ObSSTablePrinter::print_title("Micro Block", micro_idx, 1);
    ObSSTablePrinter::print_title("Total Rows", row_cnt, 1);
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_sstable_micro_data(const ObMicroBlockData& micro_data)
{
  int ret = OB_SUCCESS;
  const ObRowStoreType row_store_type = (ObRowStoreType)block_header_->row_store_type_;
  ObMicroBlockScanner* micro_scanner = nullptr;
  ObArenaAllocator allocator(ObModIds::OB_CS_SSTABLE_READER);
  ObStoreRange range;
  void* buf = nullptr;
  range.set_whole_range();

  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMicroBlockScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Failed to alloc memory for micro block scanner", K(ret));
  } else if (FALSE_IT(micro_scanner = new (buf) ObMicroBlockScanner())) {

  } else if (OB_FAIL(
                 micro_scanner->set_scan_param(col_map_, range, micro_data, false, false, row_store_type, allocator))) {
    STORAGE_LOG(ERROR, "failed to set scan param", K(ret));
  } else {
    const ObStoreRow* row = NULL;
    int64_t row_idx = 0;
    while (OB_SUCC(micro_scanner->get_next_row(row))) {
      ObSSTablePrinter::print_row_title(row, row_idx++);
      ObSSTablePrinter::print_store_row(row, is_trans_sstable_);
    }
    if (OB_ITER_END != ret) {
      STORAGE_LOG(ERROR, "failed to dump macro data", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_NOT_NULL(micro_scanner)) {
    micro_scanner->~ObMicroBlockScanner();
    micro_scanner = nullptr;
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_lob_data_block()
{
  int ret = OB_SUCCESS;

  ObLobMicroBlockIndexReader micro_index_reader;
  const ObLobMacroBlockHeader* lob_macro_header = reinterpret_cast<const ObLobMacroBlockHeader*>(block_header_);

  if (!lob_macro_header->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid lob macro block header", K(*lob_macro_header), K(ret));
  } else {
    const char* index_buf = data_ + lob_macro_header->micro_block_index_offset_;
    const int64_t index_size =
        lob_macro_header->micro_block_size_array_offset_ - lob_macro_header->micro_block_index_offset_;
    if (OB_FAIL(micro_index_reader.transform(
            index_buf, static_cast<int32_t>(index_size), lob_macro_header->micro_block_count_))) {
      LOG_WARN("fail to transform micro block index reader",
          K(ret),
          K(index_size),
          "size_array_offset",
          lob_macro_header->micro_block_endkey_offset_,
          "index_offset",
          lob_macro_header->micro_block_index_offset_);
    } else {
      const char* lob_data_buf = data_ + lob_macro_header->micro_block_data_offset_;
      ObLobMicroIndexItem index_item;
      ObMicroBlockData micro_data;
      while (OB_SUCC(ret) && OB_SUCC(micro_index_reader.get_next_index_item(index_item))) {
        if (OB_FAIL(decompressed_micro_block(
                lob_data_buf + index_item.offset_, index_item.data_size_, LOB_MICRO_BLOCK_HEADER_MAGIC, micro_data))) {
          STORAGE_LOG(WARN, "Failed to decompress lob micro block data", K(ret));
        } else if (OB_UNLIKELY(!micro_data.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected micro data", K(micro_data), K(ret));
        } else {
          const ObLobMicroBlockHeader* header = reinterpret_cast<const ObLobMicroBlockHeader*>(micro_data.get_buf());
          ObSSTablePrinter::print_lob_micro_header(header);
          ObSSTablePrinter::print_lob_micro_block(micro_data.get_buf() + sizeof(ObLobMicroBlockHeader),
              micro_data.get_buf_size() - sizeof(ObLobMicroBlockHeader));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_bloom_filter_data_block()
{
  int ret = OB_SUCCESS;

  ObLobMicroBlockIndexReader micro_index_reader;
  const ObBloomFilterMacroBlockHeader* bf_macro_header =
      reinterpret_cast<const ObBloomFilterMacroBlockHeader*>(block_header_);

  if (!bf_macro_header->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid bloomfilter macro block header", K(*bf_macro_header), K(ret));
  } else {
    const char* block_buf = data_ + bf_macro_header->micro_block_data_offset_;
    ObMicroBlockData micro_data;
    if (OB_FAIL(decompressed_micro_block(
            block_buf, bf_macro_header->micro_block_data_size_, BF_MICRO_BLOCK_HEADER_MAGIC, micro_data))) {
      STORAGE_LOG(WARN, "Failed to decompress bloom filter micro block data", K(ret));
    } else if (OB_UNLIKELY(!micro_data.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected micro data", K(micro_data), K(ret));
    } else {
      const ObBloomFilterMicroBlockHeader* header =
          reinterpret_cast<const ObBloomFilterMicroBlockHeader*>(micro_data.get_buf());
      ObSSTablePrinter::print_bloom_filter_micro_header(header);
      ObSSTablePrinter::print_bloom_filter_micro_block(micro_data.get_buf() + sizeof(ObBloomFilterMicroBlockHeader),
          micro_data.get_buf_size() - sizeof(ObBloomFilterMicroBlockHeader));
    }
  }

  return ret;
}

int ObSSTableDataBlockReader::decompressed_micro_block(const char* micro_block_buf, const int64_t micro_block_size,
    const int16_t micro_header_magic, ObMicroBlockData& micro_data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(micro_block_buf) || micro_block_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to decompres micro block", K(ret), K(micro_block_buf), K(micro_block_size));
  } else if (OB_ISNULL(block_header_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block_header is null", K(ret));
  } else if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(
                 micro_block_buf, micro_block_size, micro_header_magic))) {
    STORAGE_LOG(ERROR, "micro block data is corrupted", K(ret), KP(micro_block_buf), K(micro_block_size));
  } else {
    bool is_compressed = false;
    if (OB_FAIL(macro_reader_.decompress_data(block_header_->compressor_name_,
            micro_block_buf,
            micro_block_size,
            micro_data.get_buf(),
            micro_data.get_buf_size(),
            is_compressed))) {
      LOG_WARN("fail to decompress data", K(ret));
    }
  }

  return ret;
}

} /* namespace blocksstable */
} /* namespace oceanbase */
