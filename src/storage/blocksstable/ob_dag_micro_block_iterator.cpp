/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/blocksstable/ob_dag_micro_block_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{
ObDagMicroBlockIterator::ObDagMicroBlockIterator()
  : ObMicroBlockBareIterator(),
    cg_block_(nullptr),
    simplified_macro_header_()
{
}

ObDagMicroBlockIterator::~ObDagMicroBlockIterator()
{
  reset();
}

void ObDagMicroBlockIterator::reset()
{
  ObMicroBlockBareIterator::reset();
  cg_block_ = nullptr;
  simplified_macro_header_.reset();
}

int ObDagMicroBlockIterator::open_cg_block(ObCGBlock *cg_block)
{
  int ret = OB_SUCCESS;
  ObMicroBlockBareIterator::reset();
  if (OB_FAIL(open(cg_block->get_macro_block_buffer(), cg_block->get_macro_buffer_size(), false))) {
    STORAGE_LOG(WARN, "fail to open macro block", K(ret));
  } else if (OB_FAIL(fast_locate_micro_block(cg_block->get_cg_block_offset(), cg_block->get_micro_block_idx()))) {
    STORAGE_LOG(WARN, "fail to move to the target micro block", K(ret), K(cg_block->get_cg_block_offset()), K(cg_block->get_micro_block_idx()));
  } else {
    cg_block_ = cg_block;
  }
  return ret;
}

int ObDagMicroBlockIterator::open(const char *macro_block_buf,
                                  const int64_t macro_block_buf_size,
                                  const bool need_deserialize)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited", K(ret), K(is_inited_));
  } else if (OB_ISNULL(macro_block_buf) || OB_UNLIKELY(macro_block_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block buf", KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(simplified_macro_header_.deserialize(macro_block_buf, macro_block_buf_size, read_pos_))) {
    STORAGE_LOG(WARN, "fail to deserialize simplified macro header", K(ret), K(simplified_macro_header_),
                                                                     K(macro_block_buf_size), K(read_pos_));
  } else if (OB_UNLIKELY(!simplified_macro_header_.is_valid())) {
    STORAGE_LOG(WARN, "invalid simplified macro header", K(ret), K(simplified_macro_header_));
  } else if (FALSE_IT(index_rowkey_cnt_ = simplified_macro_header_.rowkey_column_count_ == 0 ?
      1 : simplified_macro_header_.rowkey_column_count_)) { // for cg
  } else {
    read_pos_ = simplified_macro_header_.first_data_micro_block_offset_;
    macro_block_buf_ = macro_block_buf;
    macro_block_buf_size_ = macro_block_buf_size;
    iter_idx_ = 0;
    begin_idx_ = 0;
    end_idx_ = simplified_macro_header_.micro_block_count_ - 1;
    need_deserialize_ = need_deserialize;
  }
  if (OB_FAIL(ret)) {
  } else {
    ObMicroBlockData index_block;
    if (OB_FAIL(get_index_block(index_block, true))) {
      STORAGE_LOG(WARN, "Fail to get index block", K(ret), K(index_block));
    } else if (OB_FAIL(set_reader(static_cast<ObRowStoreType>(
        simplified_macro_header_.row_store_type_)))) {
      STORAGE_LOG(WARN, "Fail to set reader for index block", K(ret));
    } else if (OB_ISNULL(reader_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null micro reader", K(ret));
    } else if (OB_FAIL(reader_->init(index_block, nullptr))) {
      STORAGE_LOG(WARN, "Fail to init reader for index block", K(ret), K(index_block));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDagMicroBlockIterator::get_index_block(ObMicroBlockData &micro_block, const bool force_deserialize)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!simplified_macro_header_.is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Macro block header not inited", K(ret), K_(simplified_macro_header), K_(is_inited));
  } else if (OB_UNLIKELY(0 == simplified_macro_header_.idx_block_size_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null index block", K(ret), K_(simplified_macro_header));
  } else {
    ObMicroBlockHeader header;
    const int64_t index_block_offset = simplified_macro_header_.idx_block_offset_;
    int64_t micro_buf_size = simplified_macro_header_.idx_block_size_;
    const char *micro_buf = macro_block_buf_ + index_block_offset;
    int64_t pos = 0;
    bool is_compressed = false;
    if (OB_FAIL(header.deserialize(micro_buf, macro_block_buf_size_ - index_block_offset, pos))) {
      STORAGE_LOG(WARN, "Fail to deserialize record header", K(ret),
               K(simplified_macro_header_), K(macro_block_buf_size_),
               K(index_block_offset));
    } else if (FALSE_IT(micro_buf_size = header.header_size_ + header.data_zlength_)) {
    } else if (OB_FAIL(header.check_record(micro_buf, micro_buf_size, MICRO_BLOCK_HEADER_MAGIC))) {
      STORAGE_LOG(WARN, "Fail to check record header", K(ret), K(header));
    } else if (!need_deserialize_ && !force_deserialize) {
      micro_block.get_buf() = micro_buf;
      micro_block.get_buf_size() = micro_buf_size;
    } else if (OB_FAIL(index_reader_.decrypt_and_decompress_data(
        simplified_macro_header_,
        micro_buf,
        micro_buf_size,
        false,
        micro_block.get_buf(),
        micro_block.get_buf_size(),
        is_compressed))) {
      STORAGE_LOG(WARN, "Fail to decrypt and decompress micro block data", K(ret), K_(simplified_macro_header));
    }
  }
  return ret;
}

int ObDagMicroBlockIterator::fast_locate_micro_block(const int64_t cg_block_offset, const int64_t micro_block_idx)
{
  int ret = OB_SUCCESS;
  if (micro_block_idx < 0 || micro_block_idx > end_idx_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro_block_idx", K(micro_block_idx), K(end_idx_));
  } else if (0 == cg_block_offset ||  0 == micro_block_idx) {
    //cg block has not written any micro blocks before, skip it
    if (0 != cg_block_offset || 0 != micro_block_idx) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(WARN, "invalid cg_block_offset or micro_block_idx", K(cg_block_offset), K(micro_block_idx));
    }
  } else {
    iter_idx_ = micro_block_idx;
    read_pos_ = cg_block_offset;
  }
  return ret;
}

int ObDagMicroBlockIterator::update_cg_block_offset_and_micro_idx()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(cg_block_))) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDagMicroBlockIterator hasn't been inited.", KP(cg_block_));
  } else if (OB_UNLIKELY(0 == read_pos_ || 0 == iter_idx_ || !cg_block_->is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid read_pos_ or iter_idx_ or cg_block_", K(read_pos_), K(iter_idx_), KPC(cg_block_));
  } else {
    cg_block_->set_cg_block_offset(read_pos_);
    cg_block_->set_micro_block_idx(iter_idx_);
  }
  return ret;
}


}//end namespace blocksstable
}//end namespace oceanbase