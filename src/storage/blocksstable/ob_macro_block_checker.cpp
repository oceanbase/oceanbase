/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_macro_block_checker.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{

int ObSSTableMacroBlockChecker::check(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const ObMacroBlockCheckLevel check_level)
{
  int ret = OB_SUCCESS;
  const bool need_logic_check = CHECK_LEVEL_LOGICAL == check_level;
  int64_t pos = 0;
  if (OB_ISNULL(macro_block_buf)
      || OB_UNLIKELY(macro_block_buf_size <= 0) || check_level >= CHECK_LEVEL_MAX) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(macro_block_buf_size),
        K(check_level));
  } else if (ObMacroBlockCheckLevel::CHECK_LEVEL_NONE == check_level) {
    //do nothing
  } else if (ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_MAGIC ==
             reinterpret_cast<const ObMacroBlockCommonHeader*>(macro_block_buf + pos)->get_magic()) {
    ObMacroBlockCommonHeader common_header;
    if (OB_FAIL(common_header.deserialize(macro_block_buf, macro_block_buf_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize common header", K(ret), KP(macro_block_buf),
          K(macro_block_buf_size), K(pos), K(common_header));
    } else if (common_header.is_shared_macro_block()) {
      // skip the check
    } else if (OB_FAIL(common_header.check_integrity())) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(ERROR, "Invalid common header", K(ret), K(common_header));
    } else if (OB_FAIL(check_physical_checksum(common_header, macro_block_buf,
        macro_block_buf_size))) {
      STORAGE_LOG(WARN, "fail to check physical checksum", K(ret), K(common_header),
          KP(macro_block_buf), K(macro_block_buf_size));
    } else if (!common_header.is_sstable_data_block()
            && !common_header.is_sstable_index_block()) {
      //no need logic check
    } else if (need_logic_check && OB_FAIL(check_logical_checksum(common_header, macro_block_buf,
      macro_block_buf_size))) {
      STORAGE_LOG(WARN, "fail to check logical checksum", K(ret), K(common_header),
          KP(macro_block_buf), K(macro_block_buf_size));
    }
  } else if (ObSharedObjectHeader::OB_SHARED_BLOCK_HEADER_MAGIC ==
             reinterpret_cast<const ObSharedObjectHeader*>(macro_block_buf + pos)->magic_) {
    ObSharedObjectHeader shared_obj_header;
    if (OB_FAIL(shared_obj_header.deserialize(macro_block_buf, macro_block_buf_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize shared object header", K(ret), KP(macro_block_buf),
          K(macro_block_buf_size), K(pos), K(shared_obj_header));
    } else if (!shared_obj_header.is_valid()) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(ERROR, "Invalid shared object header", K(ret), K(shared_obj_header));
    } else if (OB_FAIL(check_physical_checksum(shared_obj_header, macro_block_buf,
        macro_block_buf_size))) {
      STORAGE_LOG(WARN, "fail to check physical checksum", K(ret), K(shared_obj_header),
          KP(macro_block_buf), K(macro_block_buf_size));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "BlockHeader should be ObMacroBlockCommonHeader or OB_SHARED_BLOCK_HEADER_MAGIC",
              K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(pos));
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_micro_desc_rows(
    const ObMicroBlockDesc &micro_block_desc,
    const ObDataStoreDesc &data_store_desc,
    const ObIndexBlockRowHeader *index_row_header)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *reader = NULL;
  ObMicroBlockData decompressed_data;
  compaction::ObLocalArena micro_block_allocator("CheckMicRow");
  ObDatumRow check_datum_row;
  ObMacroBlockReader macro_reader;
  ObMicroBlockReaderHelper reader_helper;

  ObMicroBlockDesMeta micro_des_meta(
    data_store_desc.get_compressor_type(), data_store_desc.get_row_store_type(),
    data_store_desc.get_encrypt_id(), data_store_desc.get_master_key_id(), data_store_desc.get_encrypt_key());
  bool is_compressed = false;
  int64_t pos = 0;
  int64_t micro_buf_size = 0;
  char *micro_block_buf = nullptr;
  int64_t row_count = 0;
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "micro block desc is not valid", K(ret), K(micro_block_desc));
  } else if (OB_FAIL(reader_helper.init(micro_block_allocator))) {
    STORAGE_LOG(WARN, "fail to init reader helper", K(ret));
  } else if (OB_FAIL(check_datum_row.init(micro_block_allocator, micro_block_desc.header_->column_count_))) {
    STORAGE_LOG(WARN, "fail to init datum row", K(ret), K(micro_block_desc.header_->column_count_));
  } else if (FALSE_IT(micro_buf_size = micro_block_desc.header_->header_size_ + micro_block_desc.buf_size_)) {
  } else if (OB_ISNULL(micro_block_buf = static_cast<char *>(micro_block_allocator.alloc(micro_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc mem", K(ret), K(micro_buf_size), K(micro_block_desc));
  } else if (OB_FAIL(micro_block_desc.header_->serialize(micro_block_buf, micro_buf_size, pos))) {
    STORAGE_LOG(WARN, "failed to serialize header", K(ret), K(micro_block_desc));
  } else {
    MEMCPY(micro_block_buf + micro_block_desc.header_->header_size_, micro_block_desc.buf_, micro_block_desc.buf_size_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(macro_reader.decrypt_and_decompress_data(micro_des_meta, micro_block_buf,
      micro_buf_size, decompressed_data, is_compressed))) {
    STORAGE_LOG(WARN, "fail to decrypt and decompress data", K(ret));
  } else if (OB_FAIL(reader_helper.get_reader(*decompressed_data.get_micro_header(), reader))) {
    STORAGE_LOG(WARN, "fail to get reader", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "reader is null", K(ret), KP(reader));
  } else if (FALSE_IT(reader->reset())) {
  } else if (OB_FAIL(reader->init(decompressed_data, nullptr))) {
    STORAGE_LOG(WARN, "reader init failed", K(ret));
  } else if (OB_FAIL(reader->get_row_count(row_count))) {
    STORAGE_LOG(WARN, "fail to get row count from micro block reader", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
      check_datum_row.reuse();
      if (OB_FAIL(reader->get_row(row_idx, check_datum_row))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(row_idx), K(row_count), K(data_store_desc));
      }
    }
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "Fail to check micro block", K(ret), KPC(micro_block_desc.header_), K(micro_des_meta));
    if (OB_NOT_NULL(index_row_header)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(check_micro_rows_by_index(micro_block_buf, micro_buf_size, index_row_header, check_datum_row))) {
        STORAGE_LOG(WARN, "Fail to check micro block", K(tmp_ret), KPC(index_row_header));
      }
    }
  } else {
    STORAGE_LOG(INFO, "Check micro block success", KPC(micro_block_desc.header_), K(micro_des_meta));
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_micro_block_rows(const ObMicroBlock &micro_block)
{
  int ret = OB_SUCCESS;
  const int64_t micro_buf_size = micro_block.data_.get_buf_size();
  char *micro_block_buf = nullptr;
  compaction::ObLocalArena micro_block_allocator("CheckMicRow");
  ObDatumRow check_datum_row;
  if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "micro block is not valid", K(micro_block));
  } else if (OB_ISNULL(micro_block_buf = static_cast<char *>(micro_block_allocator.alloc(micro_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc mem", K(ret), K(micro_buf_size));
  } else {
    MEMCPY(micro_block_buf, micro_block.data_.get_buf(), micro_buf_size);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_datum_row.init(micro_block_allocator, micro_block.header_.column_count_))) {
    STORAGE_LOG(WARN, "fail to init datum row", K(ret), K(micro_block.header_.column_count_));
  } else if (OB_FAIL(check_micro_rows_by_index(micro_block_buf, micro_buf_size,
      micro_block.micro_index_info_->row_header_, check_datum_row))) {
    STORAGE_LOG(WARN, "Fail to check micro block", K(ret), K(micro_block.header_));
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_micro_rows_by_index(
    const char *micro_block_buf,
    const int64_t micro_buf_size,
    const ObIndexBlockRowHeader *index_row_header,
    ObDatumRow &check_datum_row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("MiBlkChecker");
  ObIMicroBlockReader *reader = NULL;
  ObMicroBlockData decompressed_data;
  const bool deep_copy_des_meta = false;
  ObMicroBlockDesMeta micro_des_meta;
  int64_t row_count = 0;
  ObMacroBlockReader macro_reader;
  ObMicroBlockReaderHelper reader_helper;
  if (OB_ISNULL(index_row_header)
      || OB_ISNULL(micro_block_buf)
      || OB_UNLIKELY(micro_buf_size <= 0)
      || OB_UNLIKELY(!check_datum_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(index_row_header),
      KP(micro_block_buf), K(micro_buf_size), K(check_datum_row));
  } else if (OB_FAIL(index_row_header->fill_micro_des_meta(
      deep_copy_des_meta, micro_des_meta))) {
    STORAGE_LOG(WARN, "fail to fill micro block deserialize meta", K(ret), KPC(index_row_header));
  } else if (OB_FAIL(reader_helper.init(allocator))) {
    STORAGE_LOG(WARN, "fail to init reader helper", K(ret));
  } else {
    bool is_compressed = false;
    if (OB_FAIL(macro_reader.decrypt_and_decompress_data(micro_des_meta, micro_block_buf,
        micro_buf_size, decompressed_data, is_compressed))) {
      STORAGE_LOG(WARN, "fail to decrypt and decompress data", K(ret));
    } else if (OB_FAIL(reader_helper.get_reader(*decompressed_data.get_micro_header(), reader))) {
      STORAGE_LOG(WARN, "fail to get reader", K(ret));
    } else if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "reader is null", K(ret), KP(reader));
    } else if (FALSE_IT(reader->reset())) {
    } else if (OB_FAIL(reader->init(decompressed_data, nullptr))) {
      STORAGE_LOG(WARN, "reader init failed", K(ret));
    } else if (OB_FAIL(reader->get_row_count(row_count))) {
      STORAGE_LOG(WARN, "fail to get row count from micro block reader", K(ret));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
        check_datum_row.reuse();
        if (OB_FAIL(reader->get_row(row_idx, check_datum_row))) {
          STORAGE_LOG(WARN, "get_row failed", K(ret), K(row_idx), K(row_count));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "Fail to check micro block", K(ret), K(micro_des_meta));
  } else {
    STORAGE_LOG(INFO, "Check micro block success", K(micro_des_meta), KPC(index_row_header));
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_data_macro_block_rows_and_index(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const ObMacroBlockCheckLevel check_level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_block_buf_size <= 0) || OB_ISNULL(macro_block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
  } else if (ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL == check_level) {
    ObMacroBlockCommonHeader common_header;
    int64_t pos = 0;
    if (OB_FAIL(common_header.deserialize(macro_block_buf, macro_block_buf_size, pos))) {
      STORAGE_LOG(WARN, "Failed to deserialize common header", K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(pos));
    } else if (ObMacroBlockCommonHeader::SSTableData == common_header.get_type()) {
      if (OB_FAIL(check_sstable_macro_block(macro_block_buf, macro_block_buf_size, common_header))) {
        STORAGE_LOG(WARN, "Failed to check sstable macro block", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_sstable_macro_block(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const ObMacroBlockCommonHeader &common_header)
{
  int ret = OB_SUCCESS;
  compaction::ObLocalArena iter_allocator_temp("MaBlkChecker");
  ObMacroBlockRowBareIterator macro_iter(iter_allocator_temp);
  if (OB_FAIL(macro_iter.open(macro_block_buf, macro_block_buf_size))) {
    STORAGE_LOG(WARN, "Fail to init bare macro block row iterator", K(ret));
  } else {
    int64_t micro_idx = 0;
    do {
      if (OB_FAIL(check_data_micro_block(macro_iter))) {
        STORAGE_LOG(WARN, "Fail to check sstable micro block", K(ret));
      } else {
        ++micro_idx;
      }
    } while (OB_SUCC(ret) && OB_SUCC(macro_iter.open_next_micro_block()));

    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to iterate all micro blocks in macro block", K(ret));
    } else if (FALSE_IT(ret = OB_SUCCESS)) {
    } else if (ObMacroBlockCommonHeader::SSTableData == common_header.get_type()) {
      // dump leaf index block
      if (OB_FAIL(macro_iter.open_leaf_index_micro_block())) {
        STORAGE_LOG(WARN, "Fail to open leaf index micro block", K(ret));
      } else if (OB_FAIL(check_index_micro_block(macro_iter))) {
        STORAGE_LOG(WARN, "Fail to check sstable micro block", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "Successfully check sstable macro block", K(ret), K(common_header), K(micro_idx));
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_data_micro_block(ObMacroBlockRowBareIterator &macro_iter)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData *micro_data = nullptr;
  ObMicroBlockHeader micro_block_header;
  int64_t pos = 0;
  int64_t row_cnt = 0;
  if (OB_FAIL(macro_iter.get_curr_micro_block_data(micro_data))) {
    STORAGE_LOG(WARN, "Fail to get curr micro block data", K(ret));
  } else if (OB_ISNULL(micro_data) || OB_UNLIKELY(!micro_data->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid micro block data", K(ret), KPC(micro_data));
  } else if (OB_FAIL(micro_block_header.deserialize(micro_data->get_buf(), micro_data->get_buf_size(), pos))) {
    STORAGE_LOG(ERROR, "Failed to deserialize sstable micro block header", K(ret), K(micro_data));
  } else if (OB_FAIL(macro_iter.get_curr_micro_block_row_cnt(row_cnt))) {
    STORAGE_LOG(WARN, "Fail to get row count of current micro block", K(ret));
  }
  const ObDatumRow *row = nullptr;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cnt; ++row_idx) {
    if (OB_FAIL(macro_iter.get_next_row(row))) {
      STORAGE_LOG(WARN, "Fail to get next row from iter", K(ret), K(row_idx), K(row_cnt));
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_index_micro_block(ObMacroBlockRowBareIterator &macro_iter)
{
  int ret = OB_SUCCESS;
  // for index micro block, we need to check the logic micro id is unique
  ObIndexBlockRowParser idx_row_parser;
  int64_t row_cnt = 0;
  hash::ObHashSet<ObLogicMicroBlockId> unique_logic_micro_ids;
  ObMicroBlockHeader micro_block_header;
  const ObMicroBlockData *micro_data = nullptr;
  int64_t pos = 0;
  if (OB_FAIL(macro_iter.get_curr_micro_block_data(micro_data))) {
    STORAGE_LOG(WARN, "Fail to get curr micro block data", K(ret));
  } else if (OB_ISNULL(micro_data) || OB_UNLIKELY(!micro_data->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid micro block data", K(ret), KPC(micro_data));
  } else if (OB_FAIL(micro_block_header.deserialize(micro_data->get_buf(), micro_data->get_buf_size(), pos))) {
    STORAGE_LOG(ERROR, "Failed to deserialize sstable micro block header", K(ret), K(micro_data));
  } else if (OB_FAIL(macro_iter.get_curr_micro_block_row_cnt(row_cnt))) {
    STORAGE_LOG(WARN, "Fail to get row count of current micro block", K(ret));
  } else if (OB_FAIL(unique_logic_micro_ids.create(row_cnt))) {
    STORAGE_LOG(WARN, "Fail to create unique logic micro ids", K(ret), K(row_cnt));
  }
  const ObDatumRow *row = nullptr;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cnt; ++row_idx) {
    if (OB_FAIL(macro_iter.get_next_row(row))) {
      STORAGE_LOG(WARN, "Fail to get next row from iter", K(ret), K(row_idx), K(row_cnt));
    } else {
      idx_row_parser.reset();
      if (OB_FAIL(idx_row_parser.init(micro_block_header.rowkey_column_count_, *row))) {
        STORAGE_LOG(WARN, "Fail to init idx row parser", K(ret));
      } else if (OB_FAIL(idx_row_parser.get_header(idx_row_header))) {
        STORAGE_LOG(WARN, "Fail to get index block row header", K(ret));
      } else if (OB_ISNULL(idx_row_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Null pointer to index block row header", K(ret));
      } else if (idx_row_header->has_valid_logic_micro_id()) {
        ObLogicMicroBlockId logic_micro_id = idx_row_header->get_logic_micro_id();
        if (OB_FAIL(unique_logic_micro_ids.set_refactored(logic_micro_id, 0))) {
          STORAGE_LOG(WARN, "Fail to add logic micro id to set", K(ret), K(logic_micro_id));
        }
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_logical_checksum(
    const ObMacroBlockCommonHeader &common_header,
    const char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_MACRO_BLOCK_CHECKER);
  ObSSTableMacroBlockHeader sstable_header;
  ObMicroBlockBareIterator micro_iter;
  const int64_t *column_checksum_in_header = nullptr;
  int64_t *column_checksum = nullptr;
  ObDatumRow datum_row;
  if (OB_UNLIKELY(buf_size <= 0 || !common_header.is_valid()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(buf), K(buf_size), K(common_header));
  } else if (OB_FAIL(get_sstable_header_and_column_checksum(buf, buf_size, sstable_header,
      column_checksum_in_header))) {
    STORAGE_LOG(WARN, "fail to get sstable header and column checksum", K(ret), KP(buf),
                K(buf_size), K(common_header));
  } else if (OB_ISNULL(column_checksum_in_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "column checksum in header should not be NULL", K(ret), KP(column_checksum_in_header));
  } else if (OB_FAIL(micro_iter.open(buf, buf_size))) {
    STORAGE_LOG(WARN, "fail to init micro block iterator", K(ret));
  } else if (OB_FAIL(datum_row.init(allocator, sstable_header.fixed_header_.column_count_))) {
    STORAGE_LOG(WARN, "fail to init datum row", K(ret), K(sstable_header));
  } else if (OB_ISNULL(column_checksum = static_cast<int64_t *>(allocator.alloc(
      sstable_header.fixed_header_.column_count_ * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory for column checksum", K(ret));
  } else {
    const int64_t column_cnt = sstable_header.fixed_header_.column_count_;
    ObMacroBlockReader reader;
    ObMicroBlockData raw_micro_data;
    ObMicroBlockData micro_data;
    MEMSET(column_checksum, 0, column_cnt * sizeof(int64_t));
    ObMicroBlockReaderHelper micro_reader_helper;
    if (OB_FAIL(micro_reader_helper.init(allocator))) {
      STORAGE_LOG(WARN, "fail to init micro reader helper", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(micro_iter.get_next_micro_block_data(raw_micro_data))) {
      bool is_compressed = false;
      ObIMicroBlockReader *micro_reader = nullptr;
      if (OB_FAIL(ObMicroBlockHeader::deserialize_and_check_record(raw_micro_data.get_buf(),
          raw_micro_data.get_buf_size(), MICRO_BLOCK_HEADER_MAGIC))) {
        STORAGE_LOG(ERROR, "micro block data is corrupted", K(ret), K(raw_micro_data));
      } else if (OB_FAIL(reader.decrypt_and_decompress_data(sstable_header,
          raw_micro_data.get_buf(), raw_micro_data.get_buf_size(), false,
          micro_data, is_compressed))) {
        STORAGE_LOG(ERROR, "fail to get micro block data", K(ret), K(sstable_header),
            K(raw_micro_data));
      } else if (OB_UNLIKELY(!micro_data.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid micro block data", K(ret), K(micro_data));
      } else if (OB_FAIL(micro_reader_helper.get_reader(*micro_data.get_micro_header(), micro_reader))) {
        STORAGE_LOG(WARN, "fail to get micro reader by store type",
            K(ret), K(micro_data.get_store_type()));
      } else if (OB_FAIL(micro_reader->init(micro_data, nullptr))) {
        STORAGE_LOG(WARN, "fail to init micro reader", K(ret));
      } else if (OB_FAIL(calc_micro_column_checksum(*micro_reader, datum_row, column_checksum))) {
        STORAGE_LOG(WARN, "fail to accumulate micro column checksum", K(ret), K(datum_row));
      }
    }
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next micro block", K(ret));
    } else {
      ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        if (column_checksum_in_header[i] != column_checksum[i]) {
          ret = OB_PHYSIC_CHECKSUM_ERROR;
          LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg","Column checksum error", K(ret), K(i),
              K(column_checksum_in_header[i]), K(column_checksum[i]));
        }
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::calc_micro_column_checksum(
    ObIMicroBlockReader &reader,
    ObDatumRow &datum_row,
    int64_t *column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(column_checksum));
  } else {
    for (int64_t iter = 0; OB_SUCC(ret) && iter != reader.row_count(); ++iter) {
      if (OB_FAIL(reader.get_row(iter, datum_row))) {
        STORAGE_LOG(WARN, "fail to get row", K(ret), K(iter));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.count_; ++i) {
          column_checksum[i] += datum_row.storage_datums_[i].checksum(0);
        }
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::get_sstable_header_and_column_checksum(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    ObSSTableMacroBlockHeader &header,
    const int64_t *&column_checksum)
{
  int ret = OB_SUCCESS;
  int64_t pos = ObMacroBlockCommonHeader::get_serialize_size();
  if (OB_UNLIKELY(macro_block_buf_size <= 0) || OB_ISNULL(macro_block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(header.deserialize(macro_block_buf, macro_block_buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize macro block header", K(ret), KP(macro_block_buf),
        K(macro_block_buf_size), K(pos));
  } else {
    column_checksum = header.column_checksum_;
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_physical_checksum(
    const ObSharedObjectHeader &shared_obj_header,
    const char *macro_block_buf,
    const int64_t macro_block_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_block_buf_size <= 0 || !shared_obj_header.is_valid())
      || OB_ISNULL(macro_block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", KR(ret), KP(macro_block_buf), K(macro_block_buf_size),
        K(shared_obj_header));
  } else if (shared_obj_header.data_size_ != 0) {
    const int64_t header_size = shared_obj_header.header_size_;
    if (shared_obj_header.data_size_ > (macro_block_buf_size - header_size)) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(ERROR, "Invalid payload size", KR(ret), K(shared_obj_header), K(macro_block_buf_size));
    } else {
      const int64_t physical_checksum = ob_crc64_sse42(macro_block_buf + header_size, shared_obj_header.data_size_);
      if (physical_checksum != shared_obj_header.checksum_) {
        ret = OB_PHYSIC_CHECKSUM_ERROR;
        LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "Invalid physical checksum", K(ret), K(physical_checksum),
            K(shared_obj_header));
      }
    }
  }
  return ret;
}

int ObSSTableMacroBlockChecker::check_physical_checksum(
    const ObMacroBlockCommonHeader &common_header,
    const char *macro_block_buf,
    const int64_t macro_block_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_block_buf_size <= 0 || !common_header.is_valid())
      || OB_ISNULL(macro_block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(macro_block_buf), K(macro_block_buf_size),
        K(common_header));
  } else if (common_header.get_payload_size() != 0) {
    const int64_t header_size = common_header.get_serialize_size();
    if (common_header.get_payload_size() > (macro_block_buf_size - header_size)) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(ERROR, "Invalid payload size", K(ret), K(common_header));
    } else {
      const int32_t physical_checksum = static_cast<int32_t>(ob_crc64(macro_block_buf + header_size,
          common_header.get_payload_size()));
      if (physical_checksum != common_header.get_payload_checksum()) {
        ret = OB_PHYSIC_CHECKSUM_ERROR;
        LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "Invalid physical checksum", K(ret), K(physical_checksum),
            K(common_header));
      }
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
