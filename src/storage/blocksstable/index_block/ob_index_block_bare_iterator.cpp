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

#include "ob_index_block_bare_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{

ObIndexBlockBareIterator::ObIndexBlockBareIterator(const uint64_t tenant_id)
    : ObMicroBlockBareIterator(tenant_id),
      rowkey_column_count_(0),
      cur_row_idx_(0),
      row_count_(0),
      row_(tenant_id)
{}

ObIndexBlockBareIterator::~ObIndexBlockBareIterator()
{
  if (is_inited_) {
    reset();
  }
}

void ObIndexBlockBareIterator::reset()
{
  rowkey_column_count_ = 0;
  cur_row_idx_ = 0;
  row_count_ = 0;
  row_.reset();
  ObMicroBlockBareIterator::reset();
}

int ObIndexBlockBareIterator::open(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const bool is_macro_meta_block,
    const bool need_check_data_integrity)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData index_micro_block;
  const ObMicroBlockHeader *index_micro_block_header = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexBlockBareIterator already inited", KR(ret));
  } else if (OB_FAIL(ObMicroBlockBareIterator::open(
      macro_block_buf, macro_block_buf_size,
      need_check_data_integrity, false/*need_deserialize*/))) {
    LOG_WARN("fail to open micro bare iterator", KR(ret), KP(macro_block_buf),
        K(macro_block_buf_size), K(need_check_data_integrity));
  } else if (OB_FAIL(get_index_block(
      index_micro_block, true/*force_deserialize*/, is_macro_meta_block))) {
    LOG_WARN("fail to get index micro block", KR(ret), K(is_macro_meta_block), KPC(this));
  } else if (OB_ISNULL(index_micro_block_header = index_micro_block.get_micro_header())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get index micro block header", KR(ret), K(index_micro_block), KPC(this));
  } else if (OB_FAIL(set_reader(get_row_type()))) {
    LOG_WARN("fail to init reader for index block", KR(ret), KPC(this));
  } else if (OB_FAIL(reader_->init(index_micro_block, nullptr/*datum_utils*/))) {
    LOG_WARN("fail to init reader for index block", KR(ret), K(index_micro_block), KPC(this));
  } else if (OB_FAIL(reader_->get_row_count(row_count_))) {
    LOG_WARN("fail to get row count ", KR(ret), K(index_micro_block), KPC(this));
  } else if (OB_FAIL(row_.init(allocator_, index_micro_block_header->column_count_))) {
    LOG_WARN("fail to init row ", KR(ret),
        K(index_micro_block), KPC(index_micro_block_header), KPC(this));
  } else {
    is_inited_ = true;
    rowkey_column_count_ = index_micro_block_header->rowkey_column_count_;
    cur_row_idx_ = 0;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObIndexBlockBareIterator::get_next_logic_micro_id(
    ObLogicMicroBlockId &logic_micro_id, int64_t &micro_checksum)
{
  int ret = OB_SUCCESS;
  row_.reuse();
  logic_micro_id.reset();
  micro_checksum = 0;
  ObIndexBlockRowParser idx_row_parser;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexBlockBareIterator not inited", KR(ret));
  } else if (OB_UNLIKELY(cur_row_idx_ >= row_count_)) {
    ret = OB_ITER_END;
    // skip log
  } else if (OB_FAIL(reader_->get_row(cur_row_idx_, row_))) {
    LOG_WARN("fail to get row", KR(ret), KPC(this));
  } else if (OB_FAIL(idx_row_parser.init(rowkey_column_count_, row_))) {
    LOG_WARN("fail to init idx row parser", KR(ret), K(row_), KPC(this));
  } else if (OB_FAIL(idx_row_parser.get_header(idx_row_header))) {
    LOG_WARN("fail to get idx micro block header", KR(ret), K(idx_row_parser), K(row_), KPC(this));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx_row_header is NULL", KR(ret), K(idx_row_parser), K(row_), KPC(this));
  } else if (OB_UNLIKELY(!idx_row_header->has_valid_logic_micro_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logic micro id is invalid", KR(ret),
        K(idx_row_header), K(idx_row_parser), K(row_), KPC(this));
  } else {
    logic_micro_id = idx_row_header->get_logic_micro_id();
    micro_checksum = idx_row_header->get_data_checksum();
    cur_row_idx_++;
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase