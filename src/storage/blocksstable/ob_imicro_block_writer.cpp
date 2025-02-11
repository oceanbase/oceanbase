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

#include "ob_imicro_block_writer.h"
#include  "ob_row_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"
namespace oceanbase
{
namespace blocksstable
{

/**
 * -------------------------------------------------------------------ObMicroBlockDesc-------------------------------------------------------------------
 */
bool ObMicroBlockDesc::is_valid() const
{
  return last_rowkey_.is_valid()
         && NULL != header_ && header_->is_valid()
         && NULL != buf_ && buf_size_ > 0
         && data_size_ > 0 && row_count_ > 0 && column_count_ > 0
         && max_merged_trans_version_  >= 0;
}

void ObMicroBlockDesc::reset()
{
  last_rowkey_.reset();
  buf_ = NULL;
  header_ = NULL;
  aggregated_row_ = NULL;
  buf_size_ = 0;
  data_size_ = 0;
  row_count_ = 0;
  column_count_ = 0;
  max_merged_trans_version_ = 0;
  macro_id_.reset();
  logic_micro_id_.reset();
  block_offset_ = 0;
  block_checksum_ = 0;
  row_count_delta_ = 0;
  contain_uncommitted_row_ = false;
  can_mark_deletion_ = false;
  has_string_out_row_ = false;
  has_lob_out_row_ = false;
  original_size_ = 0;
  is_last_row_last_flag_ = false;
}

int ObMicroBlockDesc::deep_copy(
    common::ObIAllocator& allocator,
    ObMicroBlockDesc& dst) const
{
  int ret = OB_SUCCESS;
  if (this != &dst) {
    dst.reset();
    char * block_buffer = nullptr;
    ObMicroBlockHeader *micro_header = nullptr;
    void * row_buffer = nullptr;
    ObDatumRow *row = nullptr;

    if (OB_ISNULL(header_) || OB_ISNULL(buf_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "can't copy invalid desc", K(ret), K(*this));
    } else if (OB_FAIL(last_rowkey_.deep_copy(dst.last_rowkey_, allocator))) {
      STORAGE_LOG(WARN, "failed to copy last key", K(ret));
    } else {
      const int64_t block_size = header_->header_size_ + buf_size_;
      int64_t pos = 0;
      if (block_size == 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "empty micro block desc", K(ret), K(*this));
      } else if (OB_ISNULL(block_buffer = (char *)allocator.alloc(block_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc micro block buf", K(ret));
      } else if (FALSE_IT(micro_header = reinterpret_cast<ObMicroBlockHeader *>(block_buffer))) {
      } else if (OB_FAIL(header_->deep_copy(block_buffer, block_size, pos, micro_header))) {
        STORAGE_LOG(WARN, "failed to deep copy header", K(ret));
      } else if (OB_UNLIKELY(pos != micro_header->header_size_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "header deep copy size mismatch", K(ret), K(*micro_header), K(pos));
      } else {
        MEMCPY(block_buffer + pos, buf_, buf_size_);
        dst.buf_ = block_buffer + pos;
        dst.header_ = micro_header;
        dst.buf_size_ = buf_size_;
        dst.data_size_ = data_size_;
        dst.original_size_ = original_size_;
        dst.row_count_ = row_count_;
        dst.column_count_ = column_count_;
        dst.max_merged_trans_version_ = max_merged_trans_version_;
        dst.macro_id_ = macro_id_;
        dst.block_offset_ = block_offset_;
        dst.block_checksum_ = block_checksum_;
        dst.row_count_delta_ = row_count_delta_;
        dst.contain_uncommitted_row_ = contain_uncommitted_row_;
        dst.can_mark_deletion_ = can_mark_deletion_;
        dst.has_string_out_row_ = has_string_out_row_;
        dst.has_lob_out_row_ = has_lob_out_row_;
        dst.is_last_row_last_flag_ = is_last_row_last_flag_;

        if (nullptr != aggregated_row_) {
          if (OB_ISNULL(row_buffer = allocator.alloc(sizeof(ObDatumRow)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "failed to alloc row buf", K(ret));
          } else if (FALSE_IT(row = new (row_buffer)ObDatumRow())) {
          } else if (OB_FAIL(row->init(allocator, aggregated_row_->get_column_count()))) {
            STORAGE_LOG(WARN, "failed to init datum row", K(ret));
          } else if (OB_FAIL(row->deep_copy(*aggregated_row_, allocator))) {
            STORAGE_LOG(WARN, "failed to copy datum row", K(ret));
          } else {
            dst.aggregated_row_ = row;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(block_buffer)) {
        allocator.free(block_buffer);
      }

      if (OB_NOT_NULL(row)) {
        row->~ObDatumRow();
      }

      if (OB_NOT_NULL(row_buffer)) {
        allocator.free(row_buffer);
      }
    }
  }
  return ret;
}

 /**
 * -------------------------------------------------------------------ObMicroBufferWriter-------------------------------------------------------------------
 */
int ObMicroBufferWriter::write_row(const ObDatumRow &row, const int64_t rowkey_cnt, int64_t &size)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;

  if (remain_buffer_size() <= 0 && OB_FAIL(expand(ObCompactionBuffer::size()))) {
    STORAGE_LOG(WARN, "failed to reserve", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_SUCC(row_writer.write(rowkey_cnt, row, current(), remain_buffer_size(), size))) {
      break;
    } else {
      if (OB_UNLIKELY(ret != OB_BUF_NOT_ENOUGH)) {
        STORAGE_LOG(WARN, "failed to write row", K(ret), KPC(this));
      } else if (!check_could_expand()) { //break
      } else if (OB_FAIL(expand(ObCompactionBuffer::size()))) {
        STORAGE_LOG(WARN, "failed to reserve", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    write_nop(size);
  }
  return ret;
}

 /**
 * -------------------------------------------------------------------ObIMicroBlockWriter-------------------------------------------------------------------
 */
int ObIMicroBlockWriter::build_micro_block_desc(ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  micro_block_desc.reset();

  char *block_buffer = NULL;
  int64_t block_size = 0;
  if (OB_FAIL(build_block(block_buffer, block_size))) {
    STORAGE_LOG(WARN, "failed to build micro block", K(ret));
  } else {
    ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(block_buffer);
    micro_block_desc.header_ = micro_header;
    micro_block_desc.buf_ = block_buffer + micro_block_desc.header_->header_size_;
    micro_block_desc.buf_size_ = block_size - micro_block_desc.header_->header_size_;
    micro_block_desc.data_size_ = micro_block_desc.buf_size_;
    micro_block_desc.row_count_ = get_row_count();
    micro_block_desc.column_count_ = get_column_count();
    micro_block_desc.row_count_delta_ = get_row_count_delta();
    micro_block_desc.max_merged_trans_version_ = get_max_merged_trans_version();
    micro_block_desc.contain_uncommitted_row_ = is_contain_uncommitted_row();
    micro_block_desc.block_checksum_ = get_micro_block_checksum();
    micro_block_desc.has_string_out_row_ = has_string_out_row_;
    micro_block_desc.has_lob_out_row_ = has_lob_out_row_;
    micro_block_desc.original_size_ = get_original_size();
    micro_block_desc.is_last_row_last_flag_ = is_last_row_last_flag();
    // fill micro header for bugfix on micro block that bypass compression/encryption
    // since these fields will be only filled on compression in current implementation
    micro_header->data_length_ = micro_block_desc.buf_size_;
    micro_header->data_zlength_ = micro_block_desc.buf_size_;
    micro_header->data_checksum_ = ob_crc64_sse42(0, micro_block_desc.buf_, micro_block_desc.buf_size_);
    micro_header->original_length_ = micro_block_desc.original_size_;
    micro_header->set_header_checksum();
  }
  // do not reuse micro writer here
  return ret;
}

int ObIMicroBlockWriter::get_pre_agg_param(const int64_t col_idx, ObMicroDataPreAggParam &pre_agg_param) const
{
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(WARN, "unsupported get data from micro writer", K(ret));
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase
