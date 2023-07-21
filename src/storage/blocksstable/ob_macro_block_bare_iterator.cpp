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

#include "encoding/ob_micro_block_decoder.h"
#include "ob_index_block_row_struct.h"
#include "ob_macro_block_bare_iterator.h"
#include "ob_micro_block_reader.h"
#include "ob_datum_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{

ObMicroBlockBareIterator::ObMicroBlockBareIterator()
  : allocator_(), macro_block_buf_(nullptr), macro_block_buf_size_(0), common_header_(),
    macro_block_header_(), reader_(nullptr), micro_reader_helper_(),
    begin_idx_(0), end_idx_(0), iter_idx_(0), read_pos_(0),
    need_deserialize_(false), is_inited_(false)
{
}

ObMicroBlockBareIterator::~ObMicroBlockBareIterator()
{
  reset();
}

void ObMicroBlockBareIterator::reset()
{
  macro_block_buf_ = nullptr;
  macro_block_buf_size_ = 0;
  common_header_.reset();
  macro_block_header_.reset();
  micro_reader_helper_.reset();
  reader_ = nullptr;
  begin_idx_ = 0;
  end_idx_ = 0;
  iter_idx_ = 0;
  read_pos_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

void ObMicroBlockBareIterator::reuse()
{
  macro_block_buf_ = nullptr;
  macro_block_buf_size_ = 0;
  common_header_.reset();
  macro_block_header_.reset();
  begin_idx_ = 0;
  end_idx_ = 0;
  iter_idx_ = 0;
  read_pos_ = 0;
  is_inited_ = false;
}

int ObMicroBlockBareIterator::open(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const bool need_check_data_integrity,
    const bool need_deserialize)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_ISNULL(macro_block_buf) || OB_UNLIKELY(macro_block_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block buf", KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(common_header_.deserialize(macro_block_buf, macro_block_buf_size, read_pos_))) {
    LOG_WARN("Failed to deserialize macro header", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(common_header_.check_integrity())) {
    LOG_ERROR("Invalid common header", K(ret), K_(common_header));
  } else if (OB_UNLIKELY(!common_header_.is_sstable_data_block()
      && !common_header_.is_sstable_index_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Macro block type not supported for data iterator", K(ret));
  } else if (need_check_data_integrity && OB_FAIL(check_macro_block_data_integrity(
      macro_block_buf + read_pos_, common_header_.get_payload_size()))) {
    LOG_WARN("Invalid macro block payload data", K(ret));
  } else if (OB_FAIL(macro_block_header_.deserialize(macro_block_buf, macro_block_buf_size, read_pos_))) {
    LOG_WARN("fail to deserialize macro block header", K(ret), K(macro_block_header_),
        K(macro_block_buf_size), K(read_pos_));
  } else {
    macro_block_buf_ = macro_block_buf;
    macro_block_buf_size_ = macro_block_buf_size;
    iter_idx_ = 0;
    begin_idx_ = 0;
    end_idx_ = macro_block_header_.fixed_header_.micro_block_count_ - 1;
    need_deserialize_ = need_deserialize;
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockBareIterator::open(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_ISNULL(macro_block_buf)
      || OB_UNLIKELY(macro_block_buf_size <= 0 || !range.is_valid() || !rowkey_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block buf", KP(macro_block_buf), K(macro_block_buf_size), K(range), K(rowkey_read_info));
  } else if (OB_FAIL(common_header_.deserialize(macro_block_buf, macro_block_buf_size, read_pos_))) {
    LOG_WARN("Failed to deserialize macro header", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(common_header_.check_integrity())) {
    LOG_ERROR("Invalid common header", K(ret), K_(common_header));
  } else if (OB_UNLIKELY(!common_header_.is_sstable_data_block()
      && !common_header_.is_sstable_index_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Macro block type not supported for data iterator", K(ret));
  } else if (OB_FAIL(macro_block_header_.deserialize(macro_block_buf, macro_block_buf_size, read_pos_))) {
    LOG_WARN("fail to deserialize macro block header", K(ret), K(macro_block_header_),
        K(macro_block_buf_size), K(read_pos_));
  } else {
    macro_block_buf_ = macro_block_buf;
    macro_block_buf_size_ = macro_block_buf_size;
    need_deserialize_ = true;
  }

  if (OB_FAIL(ret)) {
  } else if (range.is_whole_range() || (!is_left_border && !is_right_border)) {
    // Do not need to locate range
    begin_idx_ = 0;
    end_idx_ = macro_block_header_.fixed_header_.micro_block_count_ - 1;
    iter_idx_ = begin_idx_;
    is_inited_ = true;
  } else if (OB_FAIL(locate_range(range, rowkey_read_info, is_left_border, is_right_border))) {
    LOG_WARN("fail to locate range for micro block", K(ret), K(range), K(rowkey_read_info));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockBareIterator::get_next_micro_block_data(ObMicroBlockData &micro_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (iter_idx_ > end_idx_) {
    ret = OB_ITER_END;
  } else {
    ObMicroBlockHeader header;
    const char *micro_buf = macro_block_buf_ + read_pos_;
    int64_t pos = 0;
    int64_t micro_buf_size = 0;
    bool is_compressed = false;
    if (OB_FAIL(header.deserialize(micro_buf, macro_block_buf_size_ - read_pos_, pos))) {
      LOG_WARN("Fail to deserialize record header", K(ret), K_(read_pos), K(macro_block_header_));
    } else if (FALSE_IT(micro_buf_size = header.header_size_ + header.data_zlength_)) {
    } else if (OB_FAIL(header.check_record(micro_buf, micro_buf_size, MICRO_BLOCK_HEADER_MAGIC))) {
      LOG_WARN("Fail to check record header", K(ret), K(header));
    } else if (!need_deserialize_) {
      micro_block.get_buf() = micro_buf;
      micro_block.get_buf_size() = micro_buf_size;
    } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(
        macro_block_header_,
        micro_buf,
        micro_buf_size,
        micro_block.get_buf(),
        micro_block.get_buf_size(),
        is_compressed))) {
      LOG_WARN("Fail to decrypt and decompress micro block data", K(ret), K(macro_block_header_));
    }

    if (OB_SUCC(ret)) {
      ++iter_idx_;
      read_pos_ += micro_buf_size;
    }
  }
  return ret;
}

int ObMicroBlockBareIterator::get_macro_block_header(ObSSTableMacroBlockHeader &macro_header)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    macro_header = macro_block_header_;
  }
  return ret;
}

int ObMicroBlockBareIterator::get_micro_block_count(int64_t &micro_block_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    micro_block_count = macro_block_header_.fixed_header_.micro_block_count_;
  }
  return ret;
}

int ObMicroBlockBareIterator::get_index_block(ObMicroBlockData &micro_block, const bool is_macro_meta_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_block_header_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Macro block header not inited", K(ret), K_(macro_block_header), K_(is_inited));
  } else if (OB_UNLIKELY(0 == macro_block_header_.fixed_header_.idx_block_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block", K(ret), K_(macro_block_header));
  } else {
    ObMicroBlockHeader header;
    const int64_t index_block_offset = is_macro_meta_block ? macro_block_header_.fixed_header_.meta_block_offset_
        : macro_block_header_.fixed_header_.idx_block_offset_;
    int64_t micro_buf_size = is_macro_meta_block ? macro_block_header_.fixed_header_.meta_block_size_
        : macro_block_header_.fixed_header_.idx_block_size_;
    const char *micro_buf = macro_block_buf_ + index_block_offset;
    int64_t pos = 0;
    bool is_compressed = false;
    if (OB_FAIL(header.deserialize(micro_buf, macro_block_buf_size_ - index_block_offset, pos))) {
      LOG_WARN("Fail to deserialize record header", K(ret), K(macro_block_header_));
    } else if (FALSE_IT(micro_buf_size = header.header_size_ + header.data_zlength_)) {
    } else if (OB_FAIL(header.check_record(micro_buf, micro_buf_size, MICRO_BLOCK_HEADER_MAGIC))) {
      LOG_WARN("Fail to check record header", K(ret), K(header));
    } else if (!need_deserialize_) {
      micro_block.get_buf() = micro_buf;
      micro_block.get_buf_size() = micro_buf_size;
    } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(
        macro_block_header_,
        micro_buf,
        micro_buf_size,
        micro_block.get_buf(),
        micro_block.get_buf_size(),
        is_compressed))) {
      LOG_WARN("Fail to decrypt and decompress micro block data", K(ret), K_(macro_block_header));
    }
  }
  return ret;
}

int ObMicroBlockBareIterator::check_macro_block_data_integrity(
    const char *payload_buf,
    const int64_t payload_size)
{
  int ret = OB_SUCCESS;
  int32_t payload_checksum = static_cast<int32_t>(ob_crc64(payload_buf, payload_size));
  if (OB_UNLIKELY(payload_checksum != common_header_.get_payload_checksum())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("macro block checksum inconsistant", K(ret), K(payload_checksum), K_(common_header));
  }
  return ret;
}

int ObMicroBlockBareIterator::locate_range(
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData index_block;
  if (OB_FAIL(get_index_block(index_block))) {
    LOG_WARN("Fail to get index block", K(ret), K(index_block));
  } else if (OB_FAIL(set_reader(static_cast<ObRowStoreType>(
      macro_block_header_.fixed_header_.row_store_type_)))) {
    LOG_WARN("Fail to set reader for index block", K(ret));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null micro reader", K(ret));
  } else if (OB_FAIL(reader_->init(index_block, &(rowkey_read_info.get_datum_utils())))) {
    LOG_WARN("Fail to init reader for index block", K(ret), K(index_block), K(rowkey_read_info));
  } else if (OB_FAIL(reader_->locate_range(
      range, is_left_border, is_right_border, begin_idx_, end_idx_, true))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Fail to locate range with leaf index block", K(ret));
    } else {
      LOG_DEBUG("block beyond range", K(ret), K_(begin_idx), K_(end_idx), K_(macro_block_header));
      iter_idx_ = end_idx_ + 1;
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(iter_idx_ = begin_idx_)) {
  } else if (0 == begin_idx_) {
    // skip
  } else {
    ObMicroBlockHeader header;
    for (int64_t i = 0; OB_SUCC(ret) && i < begin_idx_; ++i) {
      const char *micro_buf = macro_block_buf_ + read_pos_;
      int64_t pos = 0;
      if (OB_FAIL(header.deserialize(micro_buf, macro_block_buf_size_ - read_pos_, pos))) {
        LOG_WARN("Fail to deserialize micro header", K(ret), K_(read_pos), K_(macro_block_header));
      } else {
        read_pos_ += (header.header_size_ + header.data_zlength_);
      }
    }
  }
  return ret;
}

int ObMicroBlockBareIterator::set_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  if (!micro_reader_helper_.is_inited() && OB_FAIL(micro_reader_helper_.init(allocator_))) {
    LOG_WARN("Fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(store_type, reader_))) {
    LOG_WARN("Fail to get micro reader", K(ret), K(store_type));
  }
  return ret;
}

ObMacroBlockRowBareIterator::ObMacroBlockRowBareIterator(common::ObIAllocator &allocator)
  : row_(), micro_iter_(), column_types_(nullptr), column_checksums_(nullptr),
    col_read_info_(), allocator_(&allocator), micro_reader_(nullptr),
    curr_micro_block_data_(), curr_block_row_idx_(-1), curr_block_row_cnt_(0), is_inited_(false)
{
}

ObMacroBlockRowBareIterator::~ObMacroBlockRowBareIterator()
{
  reset();
}

void ObMacroBlockRowBareIterator::reset()
{
  row_.reset();
  micro_iter_.reset();
  column_types_ = nullptr;
  column_checksums_ = nullptr;
  col_read_info_.reset();
  if (nullptr != micro_reader_) {
    micro_reader_->~ObIMicroBlockReader();
    if (nullptr != allocator_) {
      allocator_->free(micro_reader_);
    }
    micro_reader_ = nullptr;
  }
  allocator_ = nullptr;
  curr_micro_block_data_.reset();
  curr_block_row_idx_ = -1;
  curr_block_row_cnt_ = 0;
  is_inited_ = false;
}

int ObMacroBlockRowBareIterator::open(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const bool need_check_integrity)
{
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockHeader macro_header;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_ISNULL(macro_block_buf) || OB_UNLIKELY(macro_block_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block buf", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(micro_iter_.open(
      macro_block_buf, macro_block_buf_size, need_check_integrity))) {
    LOG_WARN("Fail to open bare micro block iterator", K(ret));
  } else if (OB_FAIL(micro_iter_.get_macro_block_header(macro_header))) {
    LOG_WARN("Fail to get macro block header", K(ret));
  } else if (OB_UNLIKELY(!macro_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro header", K(ret), K(macro_header));
  } else {
    const int64_t column_cnt = macro_header.fixed_header_.column_count_;
    const int64_t schema_rowkey_cnt = macro_header.fixed_header_.rowkey_column_count_
                - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_types_ = macro_header.column_types_;
    column_checksums_ = macro_header.column_checksum_;
    if (OB_FAIL(row_.init(*allocator_, column_cnt))) {
      LOG_WARN("Fail to init datum row", K(ret));
    }

    ObSEArray<share::schema::ObColDesc, 16> columns;
    share::schema::ObColDesc col_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_header.fixed_header_.get_col_type_array_cnt(); ++i) {
      col_desc.col_id_ = common::OB_APP_MIN_COLUMN_ID + i;
      col_desc.col_type_ = column_types_[i];
      if (OB_FAIL(columns.push_back(col_desc))) {
        LOG_WARN("Fail to push col desc to columns", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(col_read_info_.init(*allocator_,
                macro_header.fixed_header_.column_count_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                schema_rowkey_cnt,
                lib::is_oracle_mode(),
                columns))) {
      LOG_WARN("Fail to init column read info", K(ret), K(macro_header));
    } else if (OB_FAIL(init_micro_reader(static_cast<ObRowStoreType>(
                    macro_header.fixed_header_.row_store_type_)))) {
      LOG_WARN("Fail to init micro block reader", K(ret), K(macro_header));
    } else {
      is_inited_ = true;
      if (OB_FAIL(open_next_micro_block())) {
        LOG_WARN("Fail to open the first micro block", K(ret));
      }
    }
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObMacroBlockRowBareIterator::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iterator not inited", K(ret));
  } else if (curr_block_row_idx_ >= curr_block_row_cnt_ && OB_FAIL(open_next_micro_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Fail to get next row", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_reader_->get_row(curr_block_row_idx_, row_))) {
    LOG_WARN("Fail to get current row", K(ret), K_(curr_block_row_idx), K_(col_read_info));
  } else {
    row = &row_;
    ++curr_block_row_idx_;
  }

  return ret;
}

int ObMacroBlockRowBareIterator::open_leaf_index_micro_block(const bool is_macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iterator not inited", K(ret));
  } else if (OB_UNLIKELY(curr_block_row_idx_ != curr_block_row_cnt_)) {
    ret = OB_ITER_STOP;
    LOG_WARN("Previous block iterate not finished",
        K(ret), K_(curr_block_row_idx), K_(curr_block_row_cnt));
  } else if (OB_FAIL(micro_iter_.get_index_block(curr_micro_block_data_, is_macro_meta))) {
    LOG_WARN("Fail to get leaf index block data", K(ret));
  } else if (OB_UNLIKELY(!curr_micro_block_data_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Read an invalid micro block data", K(ret));
  } else if (OB_FAIL(micro_reader_->init(curr_micro_block_data_, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K_(curr_micro_block_data));
  } else if (OB_FAIL(micro_reader_->get_row_count(curr_block_row_cnt_))) {
    LOG_WARN("Fail to get micro block row count", K(ret));
  } else {
    curr_block_row_idx_ = 0;
  }
  return ret;
}

int ObMacroBlockRowBareIterator::open_next_micro_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iterator not inited", K(ret));
  } else if (OB_FAIL(micro_iter_.get_next_micro_block_data(curr_micro_block_data_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Fail to get next micro block data", K(ret));
    }
  } else if (OB_UNLIKELY(!curr_micro_block_data_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Read an invalid micro block data", K(ret));
  } else if (OB_FAIL(micro_reader_->init(curr_micro_block_data_, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K_(curr_micro_block_data));
  } else if (OB_FAIL(micro_reader_->get_row_count(curr_block_row_cnt_))) {
    LOG_WARN("Fail to get micro block row count", K(ret));
  } else {
    curr_block_row_idx_ = 0;
  }
  return ret;
}

int ObMacroBlockRowBareIterator::get_curr_micro_block_data(const ObMicroBlockData *&block_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iterator not intied", K(ret));
  } else {
    block_data = &curr_micro_block_data_;
  }
  return ret;
}

int ObMacroBlockRowBareIterator::get_curr_micro_block_row_cnt(int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iterator not inited", K(ret));
  } else {
    row_count = curr_block_row_cnt_;
  }
  return ret;
}

int ObMacroBlockRowBareIterator::get_column_checksums(const int64_t *&column_checksums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iterator not inited", K(ret));
  } else {
    column_checksums = column_checksums_;
  }
  return ret;
}

int ObMacroBlockRowBareIterator::get_macro_block_header(
    ObSSTableMacroBlockHeader &macro_header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_iter_.get_macro_block_header(macro_header))) {
    LOG_WARN("Fail to get macro block header", K(ret));
  }
  return ret;
}

int ObMacroBlockRowBareIterator::init_micro_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Micro block reader should be null before init", K(ret));
  } else {
    switch (store_type) {
    case FLAT_ROW_STORE: {
      if (OB_ISNULL(micro_reader_ = OB_NEWx(ObMicroBlockReader, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to new flat micro block reader", K(ret));
      }
      break;
    }
    case ENCODING_ROW_STORE:
    case SELECTIVE_ENCODING_ROW_STORE: {
      if (OB_ISNULL(micro_reader_ = OB_NEWx(ObMicroBlockDecoder, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to new micro block decoder", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Not supported row store type", K(ret), K(store_type));
    }
    }
  }
  return ret;
}


} // namespace blocksstable
} // namespace oceanbase
