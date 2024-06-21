/**
 *
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
#include "share/schema/ob_column_schema.h"
#include "share/cache/ob_kvcache_pointer_swizzle.h"
#include "ob_index_block_row_scanner.h"
#include "ob_index_block_row_struct.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/blocksstable/index_block/ob_ddl_index_block_row_iterator.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{

int ObIndexBlockDataHeader::get_index_data(
    const int64_t row_idx, const char *&index_ptr, int64_t &index_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || row_idx >= row_cnt_ || row_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_idx), K_(row_cnt), KPC(this));
  } else {
    const ObStorageDatum &datum = index_datum_array_[row_idx];
    ObString index_data_buf = datum.get_string();
    if (OB_UNLIKELY(index_data_buf.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null index data buf", K(ret), K(datum), K(row_idx));
    } else {
      index_ptr = index_data_buf.ptr();
      index_len = index_data_buf.length();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("get index data", K(ret), K(row_idx), KPC(this));
  }
  return ret;
}

int ObIndexBlockDataHeader::deep_copy_transformed_index_block(
    const ObIndexBlockDataHeader &header,
    const int64_t buf_size,
    char *buf,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!header.is_valid() || buf_size < 0 || pos >= buf_size || header.data_buf_size_ > buf_size)
      || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for copy transformed index block", K(ret), KP(buf),
        K(header), K(buf_size), K(pos));
  } else {
    char *data_buf = buf + pos;
    ObStorageDatum *index_datum_array = new (buf + pos) ObStorageDatum [header.row_cnt_];
    pos += sizeof(ObStorageDatum) * header.row_cnt_;
    const int64_t align_inc = common::upper_align(reinterpret_cast<uint64_t>(buf + pos), ObMicroBlockData::ALIGN_SIZE) - reinterpret_cast<uint64_t>(buf + pos);
    pos += align_inc;
    common::ObPointerSwizzleNode *ps_node_arr = new (buf + pos) common::ObPointerSwizzleNode[header.row_cnt_];
    pos += sizeof(common::ObPointerSwizzleNode) * header.row_cnt_;
    ObRowkeyVector *rowkey_vector = new (buf + pos) ObRowkeyVector();
    pos += sizeof(ObRowkeyVector);
    if (OB_FAIL(rowkey_vector->deep_copy(buf, pos, buf_size, *header.rowkey_vector_))) {
      LOG_WARN("Failed to deep copy rowkey vector", K(ret));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < header.row_cnt_; ++row_idx) {
        ps_node_arr[row_idx] = header.ps_node_array_[row_idx];
        if (OB_FAIL(index_datum_array[row_idx].deep_copy(header.index_datum_array_[row_idx], buf, buf_size, pos))) {
          LOG_WARN("Failed to deep copy storage datum to buf", K(ret), K(row_idx));
        }
      }
    }
    if (OB_SUCC(ret)) {
      pos += ObMicroBlockData::ALIGN_REDUNDANCY_SIZE - align_inc;
      rowkey_vector_ = rowkey_vector;
      index_datum_array_ = index_datum_array;
      row_cnt_ = header.row_cnt_;
      col_cnt_ = header.col_cnt_;
      ps_node_array_ = ps_node_arr;
      data_buf_ = data_buf;
      data_buf_size_ = header.data_buf_size_;
    }
  }
  return ret;
}

ObIndexBlockDataTransformer::ObIndexBlockDataTransformer()
  : allocator_(SET_USE_500(lib::ObMemAttr(OB_SERVER_TENANT_ID, "IdxBlkDataTrans"))), micro_reader_helper_() {}

ObIndexBlockDataTransformer::~ObIndexBlockDataTransformer()
{
}

// Transform block data to look-up format and store in transform buffer
int ObIndexBlockDataTransformer::transform(
    const ObMicroBlockData &block_data,
    ObMicroBlockData &transformed_data,
    ObIAllocator &allocator,
    char *&allocated_buf,
    const ObIArray<share::schema::ObColDesc> *rowkey_col_descs)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  char *block_buf = nullptr; // transformed block buf
  int64_t pos = 0;
  ObIMicroBlockReader *micro_reader = nullptr;
  ObMicroBlockHeader *new_micro_header = nullptr;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(block_data.get_buf());
  const int64_t col_cnt = micro_block_header->column_count_;
  const int64_t row_cnt = micro_block_header->row_count_;
  int64_t mem_limit = 0;
  if (OB_UNLIKELY(nullptr != rowkey_col_descs && col_cnt - 1 > rowkey_col_descs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected rowkey count", K(ret), K(col_cnt), KPC(rowkey_col_descs));
  } else if (OB_UNLIKELY(!block_data.is_valid() || !micro_block_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(block_data), KPC(micro_block_header));
  } else if (OB_FAIL(get_reader(block_data.get_store_type(), micro_reader))) {
    LOG_WARN("Fail to set micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(block_data, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K(block_data));
  } else if (OB_FAIL(row.init(allocator, col_cnt))) {
    LOG_WARN("Failed to init datum row", K(ret), K(col_cnt));
  } else if (OB_FAIL(get_transformed_upper_mem_size(rowkey_col_descs, block_data.get_buf(), mem_limit))) {
    LOG_WARN("Failed to get upper bound of transformed block size", K(ret), K(block_data));
    } else if (OB_ISNULL(block_buf = static_cast<char *>(allocator.alloc(mem_limit)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for transformed block buf", K(ret), K(mem_limit));
  } else if (OB_FAIL(micro_block_header->deep_copy(block_buf, mem_limit, pos, new_micro_header))) {
    LOG_WARN("Failed to serialize micro block header to transformed buf",
        K(ret), KPC(micro_block_header), K(pos));
  } else if (OB_UNLIKELY(!new_micro_header->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid copied micro block header", K(ret), KPC(new_micro_header));
  } else {
    const int64_t micro_header_size = pos;
    ObIndexBlockDataHeader *idx_header = new (block_buf + pos) ObIndexBlockDataHeader();
    pos += sizeof(ObIndexBlockDataHeader);
    ObStorageDatum *index_datum_array = new (block_buf + pos) ObStorageDatum [row_cnt];
    pos += sizeof(ObStorageDatum) * row_cnt;
    // The ps_node_arr undergoes atomic operations during usage and thus requires alignment to
    // ObMicroBlockData::ALIGN_SIZE bytes on ARM architecture for proper functioning.
    const int64_t align_inc = common::upper_align(reinterpret_cast<uint64_t>(block_buf + pos), ObMicroBlockData::ALIGN_SIZE) - reinterpret_cast<uint64_t>(block_buf + pos);
    pos += align_inc;
    common::ObPointerSwizzleNode *ps_node_arr = new (block_buf + pos) common::ObPointerSwizzleNode[row_cnt];
    pos += sizeof(common::ObPointerSwizzleNode) * row_cnt;
    ObRowkeyVector *rowkey_vector = nullptr;
    if (OB_FAIL(ObRowkeyVector::construct_rowkey_vector(row_cnt,
                                                        col_cnt - 1,
                                                        rowkey_col_descs,
                                                        block_buf,
                                                        pos,
                                                        mem_limit,
                                                        rowkey_vector))) {
      LOG_WARN("Failed to construct rowkey vector", K(ret));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cnt; ++row_idx) {
        row.reuse();
        if (OB_FAIL(micro_reader->get_row(row_idx, row))) {
          LOG_WARN("Failed to get row", K(ret), K(row_idx));
        } else {
          for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_cnt - 1; ++col_idx) {
            if (OB_FAIL(rowkey_vector->columns_[col_idx].fill_column_datum(block_buf,
                                                                           pos,
                                                                           mem_limit,
                                                                           row_idx,
                                                                           row.storage_datums_[col_idx]))) {
              LOG_WARN("Failed to fill column vector", K(ret), K(row_idx), K(col_idx));
            }
          }
          if (FAILEDx(index_datum_array[row_idx].deep_copy(row.storage_datums_[col_cnt - 1], block_buf, mem_limit, pos))) {
            LOG_WARN("Failed to deep copy storage datum to buf", K(ret), K(row_idx), K(col_cnt));
          }
        }
      }
      if (FAILEDx(rowkey_vector->set_construct_finished())) {
        LOG_WARN("Failed to set construct finished", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      idx_header->row_cnt_ = row_cnt;
      idx_header->col_cnt_ = col_cnt;
      idx_header->rowkey_vector_ = rowkey_vector;
      idx_header->index_datum_array_ = index_datum_array;
      idx_header->ps_node_array_ = ps_node_arr;
      idx_header->data_buf_ = block_buf + micro_header_size;
      // Ensure that extra_buf can at most accommodate ObMicroBlockData::ALIGN_REDUNDANCY_SIZE additional bytes for redundancy.
      idx_header->data_buf_size_ = pos + (ObMicroBlockData::ALIGN_REDUNDANCY_SIZE - align_inc) - micro_header_size;
      transformed_data.buf_ = block_buf;
      transformed_data.size_ = micro_header_size;
      transformed_data.extra_buf_ = idx_header->data_buf_;
      transformed_data.extra_size_ = idx_header->data_buf_size_;
      transformed_data.type_ = ObMicroBlockData::INDEX_BLOCK;
      allocated_buf = block_buf;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to transform index block to in_memory format", K(ret),
        KPC(micro_block_header), KPC(new_micro_header), K(block_data));
    if (nullptr != block_buf) {
      allocator.free(block_buf);
    }
  }
  return ret;
}

int ObIndexBlockDataTransformer::fix_micro_header_and_transform(
    const ObMicroBlockData &raw_data,
    ObMicroBlockData &transformed_data,
    ObIAllocator &allocator,
    char *&allocated_buf)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator; // tmp allocator to fix micro header
  ObDatumRow row;
  ObIMicroBlockReader *micro_reader = nullptr;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(raw_data.get_buf());
  if (OB_UNLIKELY(!raw_data.is_valid() || !micro_block_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(raw_data), KPC(micro_block_header));
  } else if (OB_FAIL(get_reader(raw_data.get_store_type(), micro_reader))) {
    LOG_WARN("Fail to set micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(raw_data, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K(raw_data));
  } else if (OB_FAIL(row.init(allocator, micro_block_header->column_count_))) {
    LOG_WARN("Failed to init datum row", K(ret), KPC(micro_block_header));
  } else {
    int64_t raw_data_size = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < micro_block_header->row_count_; ++row_idx) {
      row.reuse();
      if (OB_FAIL(micro_reader->get_row(row_idx, row))) {
        LOG_WARN("Fail to get row", K(ret), K(row_idx));
      }
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < micro_block_header->column_count_; ++col_idx) {
        if (row.storage_datums_[col_idx].is_null()) {
          raw_data_size += sizeof(int64_t);
        } else {
          raw_data_size += row.storage_datums_[col_idx].len_;
        }
      }
    }
    ObMicroBlockHeader new_micro_header;
    char *new_data_buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(new_data_buf = static_cast<char *>(tmp_allocator.alloc(raw_data.get_buf_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for new micro block buf", K(ret), K(raw_data));
    } else {
      MEMCPY(new_data_buf, raw_data.get_buf(), raw_data.get_buf_size());
      int64_t copy_pos = 0;
      ObMicroBlockHeader *copied_header = nullptr;
      if (OB_FAIL(raw_data.get_micro_header()->deep_copy(new_data_buf, raw_data.get_buf_size(), copy_pos, copied_header))) {
        LOG_WARN("Failed to deep copy micro header", K(ret), K(raw_data), KPC(raw_data.get_micro_header()));
      } else {
        copied_header->data_length_ = raw_data.get_buf_size() - copy_pos;
        copied_header->data_zlength_ = copied_header->data_length_;
        copied_header->original_length_ = raw_data_size;
        ObMicroBlockData copied_micro_data(new_data_buf, raw_data.get_buf_size());
        if (OB_FAIL(transform(copied_micro_data, transformed_data, allocator, allocated_buf))) {
          LOG_WARN("Failed to transform with copied micro data", K(ret),
              KPC(micro_block_header), K(new_micro_header), KPC(copied_header));
        }
      }
    }
  }
  return ret;
}

int ObIndexBlockDataTransformer::get_transformed_upper_mem_size(
    const ObIArray<share::schema::ObColDesc> *rowkey_col_descs,
    const char *raw_block_data,
    int64_t &mem_limit)
{
  int ret = OB_SUCCESS;
  mem_limit = 0;
  const ObMicroBlockHeader *micro_header =
      reinterpret_cast<const ObMicroBlockHeader *>(raw_block_data);
  if (OB_ISNULL(raw_block_data) || OB_UNLIKELY(!micro_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(raw_block_data), KPC(micro_header));
  } else {
    int64_t rowkey_vector_size = 0;
    mem_limit += micro_header->get_serialize_size();
    mem_limit += sizeof(ObIndexBlockDataHeader);
    mem_limit += micro_header->row_count_ * sizeof(ObStorageDatum);
    mem_limit += micro_header->row_count_ * sizeof(common::ObPointerSwizzleNode);
    if (OB_FAIL(ObRowkeyVector::get_occupied_size(micro_header->row_count_,
                                                  micro_header->column_count_ - 1,
                                                  rowkey_col_descs,
                                                  rowkey_vector_size))) {
      LOG_WARN("Failed to get occupied size of rowkey vector", K(ret));
    } else {
      mem_limit += rowkey_vector_size;
      mem_limit += micro_header->original_length_;
      mem_limit += ObMicroBlockData::ALIGN_REDUNDANCY_SIZE;
    }
  }
  return ret;
}

int ObIndexBlockDataTransformer::get_reader(
    const ObRowStoreType store_type,
    ObIMicroBlockReader *&micro_reader)
{
  int ret = OB_SUCCESS;
  if (!micro_reader_helper_.is_inited() && OB_FAIL(micro_reader_helper_.init(allocator_))) {
    LOG_WARN("Fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(store_type, micro_reader))) {
    LOG_WARN("Fail to get micro reader", K(ret), K(store_type));
  }
  return ret;
}

/******************             ObIndexBlockIterParam              **********************/
ObIndexBlockIterParam::ObIndexBlockIterParam()
  : sstable_(nullptr),
    tablet_(nullptr)
{
}

ObIndexBlockIterParam::ObIndexBlockIterParam(const ObSSTable *sstable, const ObTablet *tablet)
  : sstable_(sstable),
    tablet_(tablet)
{
}

ObIndexBlockIterParam::~ObIndexBlockIterParam()
{
  reset();
}

ObIndexBlockIterParam &ObIndexBlockIterParam::operator=(const ObIndexBlockIterParam &other)
{
  sstable_ = other.sstable_;
  tablet_ = other.tablet_;
  return *this;
}

int ObIndexBlockIterParam::assign(const ObIndexBlockIterParam &other)
{
  int ret = OB_SUCCESS;
  sstable_ = other.sstable_;
  tablet_ = other.tablet_;
  return ret;
}

void ObIndexBlockIterParam::reset()
{
  sstable_ = nullptr;
  tablet_ = nullptr;
}

bool ObIndexBlockIterParam::is_valid() const
{
  return OB_NOT_NULL(sstable_) && OB_NOT_NULL(tablet_);
}

/******************             ObIndexBlockRowIterator              **********************/
ObIndexBlockRowIterator::ObIndexBlockRowIterator()
  : is_inited_(false),
    is_reverse_scan_(false),
    iter_step_(1),
    idx_row_parser_(),
    datum_utils_(nullptr)
{

}

ObIndexBlockRowIterator::~ObIndexBlockRowIterator()
{
  reset();
}

void ObIndexBlockRowIterator::reset()
{
  iter_step_ = 1;
  datum_utils_ = nullptr;
  is_reverse_scan_ = false;
  idx_row_parser_.reset();
  is_inited_ = false;
}

/******************             ObRAWIndexBlockRowIterator              **********************/
ObRAWIndexBlockRowIterator::ObRAWIndexBlockRowIterator()
  : current_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    start_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    end_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    micro_reader_(nullptr),
    allocator_(nullptr),
    datum_row_(nullptr),
    micro_reader_helper_(),
    endkey_()
{

}

ObRAWIndexBlockRowIterator::~ObRAWIndexBlockRowIterator()
{
  reset();
}

void ObRAWIndexBlockRowIterator::reset()
{
  ObIndexBlockRowIterator::reset();
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  micro_reader_ = nullptr;
  if (nullptr != datum_row_) {
    datum_row_->~ObDatumRow();
    if (nullptr != allocator_) {
      allocator_->free(datum_row_);
    }
    datum_row_ = nullptr;
  }
  micro_reader_helper_.reset();
  allocator_ = nullptr;
  endkey_.reset();
}

void ObRAWIndexBlockRowIterator::reuse()
{
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  idx_row_parser_.reset();
  endkey_.reset();
}

int ObRAWIndexBlockRowIterator::init(const ObMicroBlockData &idx_block_data,
                                     const ObStorageDatumUtils *datum_utils,
                                     ObIAllocator *allocator,
                                     const bool is_reverse_scan,
                                     const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator) || OB_ISNULL(datum_utils) || !datum_utils->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), KP(allocator), KPC(datum_utils));
  } else if (!micro_reader_helper_.is_inited() && OB_FAIL(micro_reader_helper_.init(*allocator))) {
    LOG_WARN("Fail to init micro reader helper", K(ret), KP(allocator));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(idx_block_data.get_store_type(), micro_reader_))) {
    LOG_WARN("Fail to get micro block reader", K(ret), K(idx_block_data), K(idx_block_data.get_store_type()));
  } else if (OB_FAIL(micro_reader_->init(idx_block_data, datum_utils))) {
    LOG_WARN("Fail to init micro reader", K(ret), K(idx_block_data));
  } else if (OB_FAIL(init_datum_row(*datum_utils, allocator))) {
    LOG_WARN("Fail to init datum row", K(ret));
  } else {
    is_reverse_scan_ = is_reverse_scan;
    iter_step_ = is_reverse_scan_ ? -1 : 1;
    datum_utils_ = datum_utils;
    allocator_ = allocator;
    is_inited_ = true;
  }
  return ret;
}


int ObRAWIndexBlockRowIterator::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = -1;
  int64_t end_idx = -1;
  ObDatumRange range;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid() || OB_ISNULL(micro_reader_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey), KP(micro_reader_));
  } else {
    range.set_start_key(rowkey);
    range.end_key_.set_max_rowkey();
    range.set_left_closed();
    range.set_right_open();
    if (OB_FAIL(micro_reader_->locate_range(range, true, false, begin_idx, end_idx, true))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to locate range in micro data", K(ret));
      } else {
        current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      }
    }
    LOG_TRACE("Binary search rowkey with micro reader", K(ret), K(range), K(begin_idx), K(rowkey));
  }
  if (OB_SUCC(ret)) {
    current_ = begin_idx;
    start_ = begin_idx;
    end_ = begin_idx;
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::locate_range(const ObDatumRange &range,
                                             const bool is_left_border,
                                             const bool is_right_border,
                                             const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = -1;
  int64_t end_idx = -1;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid() || OB_ISNULL(micro_reader_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(range), KP(micro_reader_));
  } else if (OB_FAIL(micro_reader_->locate_range(
          range, is_left_border, is_right_border, begin_idx, end_idx, true))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Fail to locate range with micro reader", K(ret));
    }
  } else {
    LOG_TRACE("Binary search range with micro reader", K(ret), K(range), K(begin_idx), K(end_idx));
  }

  if (OB_SUCC(ret)) {
    start_ = begin_idx;
    end_ = end_idx;
    current_ = is_reverse_scan_ ? end_idx : begin_idx;
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::locate_range()
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(micro_reader_->get_row_count(row_count))) {
    LOG_WARN("Failed to get row count", K(ret), K(micro_reader_));
  } else {
    start_ = 0;
    end_ = row_count - 1;
    current_ = 0;
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::skip_to_next_valid_position(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  bool equal = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(micro_reader_->find_bound(rowkey, true, current_, current_, equal))) {
    LOG_WARN("Failed to skip to next valid position in micro block reader", K(ret), K(current_), K(rowkey));
  } else if (current_ == (end_ + 1)) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::find_rowkeys_belong_to_same_idx_row(ObMicroIndexInfo &idx_block_row, int64_t &rowkey_begin_idx, int64_t &rowkey_end_idx, const ObRowsInfo *&rows_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(rows_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rows info", K(ret));
  } else {
    bool is_decided = false;
    for (; OB_SUCC(ret) && rowkey_begin_idx < rowkey_end_idx; ++rowkey_begin_idx) {
      if (rows_info->is_row_skipped(rowkey_begin_idx)) {
        continue;
      }
      const ObDatumRowkey &rowkey = rows_info->get_rowkey(rowkey_begin_idx);
      int32_t cmp_ret = 0;
      if (OB_FAIL(compare_rowkey(rowkey, cmp_ret))) {
        LOG_WARN("fail to cmp rowkey in iter", K(ret), K(*this), K(rowkey));
      } else {
        cmp_ret = -cmp_ret;
      }

      if (OB_FAIL(ret)) {
      } else if (cmp_ret > 0) {
        idx_block_row.rowkey_end_idx_ = rowkey_begin_idx;
        is_decided = true;
        break;
      } else if (cmp_ret == 0) {
        idx_block_row.rowkey_end_idx_ = rowkey_begin_idx + 1;
        is_decided = true;
        break;
      }
    }

    if (OB_SUCC(ret) && !is_decided) {
      idx_block_row.rowkey_end_idx_ = rowkey_begin_idx;
    }
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::compare_rowkey(const ObDatumRowkey &rowkey, int32_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(micro_reader_->compare_rowkey(rowkey, current_, cmp_ret))) {
    LOG_WARN("Failed to compare rowkey", K(ret), K(rowkey));
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  ObDatumRowkey last_endkey;
  ObDatumRow tmp_datum_row; // Normally will use local datum buf, won't allocate memory
  const int64_t request_cnt = datum_utils_->get_rowkey_count() + 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else if (OB_FAIL(tmp_datum_row.init(request_cnt))) {
    LOG_WARN("Fail to init tmp_datum_row", K(ret));
  } else if (OB_FAIL(micro_reader_->get_row(end_, tmp_datum_row))) {
    LOG_WARN("Fail to get last row of micro block", K(ret), K_(end));
  } else if (OB_FAIL(last_endkey.assign(tmp_datum_row.storage_datums_, datum_utils_->get_rowkey_count()))) {
    LOG_WARN("Fail to assign storage datum to endkey", K(ret), K(tmp_datum_row));
  } else if (OB_FAIL(last_endkey.compare(rowkey, *datum_utils_, cmp_ret, false))) {
    LOG_WARN("Fail to compare rowkey", K(ret), K(last_endkey), K(rowkey));
  } else {
    can_blockscan = cmp_ret < 0;
  }
  return ret;
}

bool ObRAWIndexBlockRowIterator::end_of_block() const
{
  return current_ < start_
      || current_ > end_
      || current_ == ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
}

int ObRAWIndexBlockRowIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                            ObCommonDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  const int64_t rowkey_column_count = datum_utils_->get_rowkey_count();
  idx_row_parser_.reset();
  endkey_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(datum_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null pointer to index row", K(ret));
  } else if (OB_FAIL(micro_reader_->get_row(current_, *datum_row_))) {
    LOG_WARN("Fail to read index row from block", K(ret), K(current_));
  } else if (OB_FAIL(idx_row_parser_.init(rowkey_column_count, *datum_row_))) {
    LOG_WARN("Fail to parser index block row", K(ret), KPC(datum_row_), K(rowkey_column_count));
  } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else if (OB_FAIL(endkey_.assign(datum_row_->storage_datums_, rowkey_column_count))) {
    LOG_WARN("Fail to assign storage datum to endkey", K(ret), KPC(datum_row_), K(rowkey_column_count));
  } else {
    endkey.set_compact_rowkey(&endkey_);
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                         ObCommonDatumRowkey &endkey,
                                         bool &is_scan_left_border,
                                         bool &is_scan_right_border,
                                         const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                         const char *&agg_row_buf,
                                         int64_t &agg_buf_size,
                                         int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  row_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(get_current(idx_row_header, endkey))) {
    LOG_WARN("read cur idx row failed", K(ret), KPC(idx_row_header), K(endkey));
  } else if (OB_UNLIKELY(nullptr == idx_row_header || !endkey.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header/endkey", K(ret), KP(idx_row_header), K(endkey));
  } else if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
    if (OB_FAIL(idx_row_parser_.get_minor_meta(idx_minor_info))) {
      LOG_WARN("Fail to get minor meta info", K(ret));
    }
  } else if (!idx_row_header->is_major_node() || !idx_row_header->is_pre_aggregated()) {
    // Do not have aggregate data
  } else if (OB_FAIL(idx_row_parser_.get_agg_row(agg_row_buf, agg_buf_size))) {
    LOG_WARN("Fail to get aggregate", K(ret));
  }
  if (OB_SUCC(ret)) {
    row_offset = idx_row_parser_.get_row_offset();
    is_scan_left_border = current_ == start_;
    is_scan_right_border = current_ == end_;
    current_ += iter_step_;
  }
  return ret;
}

int ObRAWIndexBlockRowIterator::init_datum_row(const ObStorageDatumUtils &datum_utils, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr != datum_row_ && datum_row_->is_valid()) {
    // row allocated
  } else if (nullptr != datum_row_) {
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator is null", K(ret), KP(allocator));
    } else {
      datum_row_->~ObDatumRow();
      allocator->free(datum_row_);
      datum_row_ = nullptr;
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == datum_row_) {
      const int64_t request_cnt = datum_utils.get_rowkey_count() + 1;
      void *buf = nullptr;
      if (OB_ISNULL(allocator)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("allocator is null", K(ret), KP(allocator));
      } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObDatumRow)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for datum row", K(ret));
      } else if (FALSE_IT(datum_row_ = new (buf) ObDatumRow())) {
      } else if (OB_FAIL(datum_row_->init(*allocator, request_cnt))) {
        LOG_WARN("Fail to init datum row", K(ret), K(request_cnt));
      }

      if (OB_FAIL(ret) && nullptr != buf) {
        if (OB_NOT_NULL(datum_row_)) {
          datum_row_->~ObDatumRow();
        }
        allocator->free(buf);
        datum_row_ = nullptr;
      }
    }
  }
  return ret;
}

bool ObRAWIndexBlockRowIterator::is_in_border(bool is_reverse_scan, bool is_left_border, bool is_right_border)
{
  bool in_border = false;
  if (!is_reverse_scan) {
    in_border = is_right_border && current_ == end_;
  } else {
    in_border = is_left_border && current_ == start_;
  }
  return in_border;
}

int ObRAWIndexBlockRowIterator::get_index_row_count(const ObDatumRange &range,
                                                    const bool is_left_border,
                                                    const bool is_right_border,
                                                    int64_t &index_row_count,
                                                    int64_t &data_row_count)
{
  int ret = OB_SUCCESS;
  index_row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else {
    if (start_ < 0 || end_ < 0) {
      index_row_count = 0;
    } else {
      index_row_count = end_ - start_ + 1;
    }
  }
  return ret;
}

/******************             ObTFMIndexBlockRowIterator              **********************/
ObTFMIndexBlockRowIterator::ObTFMIndexBlockRowIterator()
  : idx_data_header_(nullptr),
    cur_node_index_(0)
{

}

ObTFMIndexBlockRowIterator::~ObTFMIndexBlockRowIterator()
{
  reset();
}

void ObTFMIndexBlockRowIterator::reset()
{
  ObRAWIndexBlockRowIterator::reset();
  idx_data_header_ = nullptr;
  cur_node_index_ = 0;
}

void ObTFMIndexBlockRowIterator::reuse()
{
  ObRAWIndexBlockRowIterator::reuse();
  idx_data_header_ = nullptr;
}

int ObTFMIndexBlockRowIterator::init(const ObMicroBlockData &idx_block_data,
                                     const ObStorageDatumUtils *datum_utils,
                                     ObIAllocator *allocator,
                                     const bool is_reverse_scan,
                                     const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  idx_data_header_ = reinterpret_cast<const ObIndexBlockDataHeader *>(idx_block_data.get_extra_buf());
  if (OB_ISNULL(allocator) || OB_ISNULL(datum_utils) || !datum_utils->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), KP(allocator), KPC(datum_utils));
  } else if (!micro_reader_helper_.is_inited() && OB_FAIL(micro_reader_helper_.init(*allocator_))) {
    LOG_WARN("Fail to init micro reader helper", K(ret));
  } else if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index block data header", K(ret), KPC(idx_data_header_));
  } else {
    is_reverse_scan_ = is_reverse_scan;
    iter_step_ = is_reverse_scan_ ? -1 : 1;
    datum_utils_ = datum_utils;
    is_inited_ = true;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = -1;
  int64_t end_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid() || OB_ISNULL(idx_data_header_) || !idx_data_header_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey), KPC(idx_data_header_));
  } else if (OB_FAIL(idx_data_header_->rowkey_vector_->locate_key(0,
                                                                  idx_data_header_->row_cnt_,
                                                                  rowkey,
                                                                  *datum_utils_,
                                                                  begin_idx))) {
    LOG_WARN("Failed to locate key in rowkey vector", K(ret), KPC(idx_data_header_));
  } else if (begin_idx == idx_data_header_->row_cnt_) {
    begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("Binary search rowkey in transformed block with rowkey vector", K(ret), K(begin_idx),
              K(rowkey), KPC(idx_data_header_));
    current_ = begin_idx;
    start_ = begin_idx;
    end_ = begin_idx;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::locate_range(const ObDatumRange &range,
                                             const bool is_left_border,
                                             const bool is_right_border,
                                             const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = -1;
  int64_t end_idx = -1;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid() || OB_ISNULL(idx_data_header_) || !idx_data_header_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(range), KPC(idx_data_header_));
  } else if (OB_FAIL(locate_range_by_rowkey_vector(range, is_left_border, is_right_border, is_normal_cg, begin_idx, end_idx))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Failed to locate range by rowkey vector", K(ret));
    }
  } else {
    LOG_TRACE("Locate range in index block by range", K(ret), K(range), K(begin_idx), K(end_idx),
              K(is_left_border), K(is_right_border), K_(current), KPC(idx_data_header_));
    start_ = begin_idx;
    end_ = end_idx;
    current_ = is_reverse_scan_ ? end_idx : begin_idx;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::locate_range()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else {
    start_ = 0;
    end_ = idx_data_header_->row_cnt_ - 1;
    current_ = 0;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index data header", K(ret), KPC(idx_data_header_));
  } else if (OB_FAIL(idx_data_header_->rowkey_vector_->compare_rowkey(rowkey, end_, *datum_utils_, cmp_ret, false))) {
    LOG_WARN("Fail to compare rowkey", K(ret), K(rowkey));
  } else {
    can_blockscan = cmp_ret < 0;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                            ObCommonDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  const int64_t rowkey_column_count = datum_utils_->get_rowkey_count();
  idx_row_parser_.reset();
  const char *idx_data_buf = nullptr;
  int64_t idx_data_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(idx_data_header_->get_index_data(current_, idx_data_buf, idx_data_len))) {
    LOG_WARN("Fail to get index data", K(ret), K_(current), KPC_(idx_data_header));
  } else if (OB_FAIL(idx_row_parser_.init(idx_data_buf, idx_data_len))) {
    LOG_WARN("Fail to parse index block row", K(ret), K_(current), KPC(idx_data_header_));
  } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid idx data header", K(ret), KPC(idx_data_header_));
  } else if (OB_FAIL(idx_data_header_->rowkey_vector_->get_rowkey(current_, endkey))) {
    LOG_WARN("Fail to get rowkey from vector", K(ret), K(current_), KPC(idx_data_header_));
  } else {
    cur_node_index_ = current_;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                         ObCommonDatumRowkey &endkey,
                                         bool &is_scan_left_border,
                                         bool &is_scan_right_border,
                                         const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                         const char *&agg_row_buf,
                                         int64_t &agg_buf_size,
                                         int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(get_current(idx_row_header, endkey))) {
    LOG_WARN("read cur idx row failed", K(ret), KPC(idx_row_header), K(endkey));
  } else if (OB_UNLIKELY(nullptr == idx_row_header || !endkey.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header/endkey", K(ret), KP(idx_row_header), K(endkey));
  } else if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
    if (OB_FAIL(idx_row_parser_.get_minor_meta(idx_minor_info))) {
      LOG_WARN("Fail to get minor meta info", K(ret));
    }
  } else if (!idx_row_header->is_major_node() || !idx_row_header->is_pre_aggregated()) {
    // Do not have aggregate data
  } else if (OB_FAIL(idx_row_parser_.get_agg_row(agg_row_buf, agg_buf_size))) {
    LOG_WARN("Fail to get aggregate", K(ret));
  }
  if (OB_SUCC(ret)) {
    row_offset = idx_row_parser_.get_row_offset();
    is_scan_left_border = current_ == start_;
    is_scan_right_border = current_ == end_;
    current_ += iter_step_;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::get_idx_row_header_in_target_idx(const int64_t idx,
                                                                 const ObIndexBlockRowHeader *&idx_row_header)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  idx_row_parser_.reset();
  const char *idx_data_buf = nullptr;
  int64_t idx_data_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(idx_data_header_->get_index_data(idx, idx_data_buf, idx_data_len))) {
    LOG_WARN("Fail to get index data", K(idx), K_(start), K_(end), K_(current), KPC_(idx_data_header));
  } else if (OB_FAIL(idx_row_parser_.init(idx_data_buf, idx_data_len))) {
    LOG_WARN("Fail to parse index block row", K(idx), KPC(idx_data_header_));
  } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
    LOG_WARN("Fail to get index block row header", KPC(idx_row_header));
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::find_out_rows(const int32_t range_idx,
                                              const int64_t scanner_range_idx,
                                              int64_t &found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (range_idx == scanner_range_idx && !end_of_block()) {
    const int64_t start_idx = current_;
    const int64_t end_idx = is_reverse_scan_ ? start_ : end_;
    for (int64_t i = start_idx; OB_SUCC(ret) && (i * iter_step_) <= (end_idx * iter_step_); i += iter_step_) {
      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      if (OB_FAIL(get_idx_row_header_in_target_idx(i, idx_row_header))) {
        LOG_WARN("Failed to get idx row header", K(i));
      } else if (idx_row_header->has_lob_out_row()) {
        found_idx = i;
        break;
      }
    }
  }
  LOG_DEBUG("ObTFMIndexBlockRowIterator::find_out_rows", K(range_idx), KPC(this));
  return ret;
}

int ObTFMIndexBlockRowIterator::find_out_rows_from_start_to_end(const int32_t range_idx,
                                                                const int64_t scanner_range_idx,
                                                                const ObCSRowId start_row_id,
                                                                const ObCSRange &parent_row_range,
                                                                bool &is_certain,
                                                                int64_t &found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  is_certain = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (range_idx == scanner_range_idx) {
    const int64_t start_idx = is_reverse_scan_ ? end_ : start_;
    const int64_t end_idx = is_reverse_scan_ ? start_ : end_;
    bool meet_start_row_id = false;
    for (int64_t i = start_idx; OB_SUCC(ret) && (i * iter_step_) <= (end_idx * iter_step_); i += iter_step_) {
      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      if (OB_FAIL(get_idx_row_header_in_target_idx(i, idx_row_header))) {
        LOG_WARN("Failed to get idx row header", K(i));
      }
      if (OB_SUCC(ret)) {
        if (!meet_start_row_id) {
          ObCSRowId cur_start_row_id = idx_row_parser_.get_row_offset() - idx_row_header->get_row_count() + 1;
          ObCSRowId cur_end_row_id = idx_row_parser_.get_row_offset();
          if (idx_row_header->is_data_block()) {
            cur_start_row_id += parent_row_range.start_row_id_;
            cur_end_row_id += parent_row_range.start_row_id_;
          }
          meet_start_row_id = (start_row_id >= cur_start_row_id && start_row_id <= cur_end_row_id);
        }
        if (meet_start_row_id && idx_row_header->has_lob_out_row()) {
          if ((i * iter_step_) >= (current_ * iter_step_)) {
            found_idx = i;
          } else {
            is_certain = false;
          }
          break;
        }
      }
    }
  } else {
    is_certain = false;
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::advance_to_border(const ObDatumRowkey &rowkey,
                                                  const bool is_left_border,
                                                  const bool is_right_border,
                                                  const ObCSRange &parent_row_range,
                                                  ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(end_of_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error", K(ret), K(end_of_block()));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index data header", K(ret), KPC(idx_data_header_));
  } else if (OB_FAIL(advance_to_border_by_rowkey_vector(rowkey, is_left_border, is_right_border, parent_row_range, cs_range))) {
    LOG_WARN("Failed to advance border", K(ret));
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::advance_to_border_by_rowkey_vector(const ObDatumRowkey &rowkey,
                                                                   const bool is_left_border,
                                                                   const bool is_right_border,
                                                                   const ObCSRange &parent_row_range,
                                                                   ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  const bool is_range_end = is_reverse_scan_ ? is_left_border : is_right_border;
  const int64_t begin = is_reverse_scan_ ? start_ : current_;
  const int64_t end = is_reverse_scan_ ? current_ + 1 : end_ + 1;
  int64_t found_pos = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  // do not need upper_bound for reverse scan, as only co sstable reach here.
  if (OB_FAIL(idx_data_header_->rowkey_vector_->locate_key(begin,
                                                           end,
                                                           rowkey,
                                                           *datum_utils_,
                                                           found_pos))) {
    LOG_WARN("Failed to locate key in rowkey vector", K(ret), KPC(idx_data_header_));
  } else if (!is_reverse_scan_) {
    // found_pos is safe to skip(end_key < border_rowkey).
    found_pos--;
    if (is_range_end && found_pos == end_) {
      // if is_range_end is true, we cannot skip all rowids because only subset of rowids statisy query range.
      found_pos--;
    }
    LOG_DEBUG("ObTFMIndexBlockRowIterator::advance_to_border_by_rowkey_vector", K(found_pos), K(is_range_end),
                 KPC(this), K(is_range_end));
    if (found_pos >= current_) {
      current_ = found_pos + 1;
      if (OB_FAIL(get_cur_row_id_range(parent_row_range, cs_range))) {
        LOG_WARN("Failed to get cur row id range", K(ret), K(rowkey), KPC(this), K(is_range_end));
      }
    }
  } else {
    // found_pos is safe to skip(end_key > border_rowkey).
    found_pos++;
    // found_pos != start_, there is no need to check is_range_end.
    if (found_pos <= current_ + 1) {
      current_ = found_pos - 1;
      if (OB_FAIL(get_cur_row_id_range(parent_row_range, cs_range))) {
        LOG_WARN("Failed to get cur row id range", K(ret), K(rowkey), KPC(this), K(is_range_end));
      }
    }
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::get_end_key(ObCommonDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index data header", K(ret), KPC(idx_data_header_));
  } else {
    endkey.set_compact_rowkey(idx_data_header_->rowkey_vector_->get_last_rowkey());
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::get_cur_row_id_range(const ObCSRange &parent_row_range,
                                                     ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  ObCommonDatumRowkey endkey;
  bool is_scan_left_border = false;
  bool is_scan_right_border = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (end_of_block()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected end of index block scanner", KPC(this));
  } else if (OB_FAIL(get_current(idx_row_header, endkey))) {
    LOG_WARN("get next idx block row failed", K(ret), KP(idx_row_header), K(endkey), K(is_reverse_scan_));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header", K(ret));
  } else {
    cs_range.start_row_id_ = idx_row_parser_.get_row_offset() - idx_row_header->get_row_count() + 1;
    cs_range.end_row_id_ = idx_row_parser_.get_row_offset();
    if (idx_row_header->is_data_block()) {
      cs_range.start_row_id_ += parent_row_range.start_row_id_;
      cs_range.end_row_id_ += parent_row_range.start_row_id_;
    }
    LOG_DEBUG("ObTFMIndexBlockRowIterator::get_cur_row_id_range",
               K(cs_range), K(parent_row_range), KPC(this));
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::skip_to_next_valid_position(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid idx data header", K(ret), KPC(idx_data_header_));
  } else {
    int64_t found_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    if (OB_FAIL(idx_data_header_->rowkey_vector_->locate_key(current_,
                                                             end_ + 1,
                                                             rowkey,
                                                             *datum_utils_,
                                                             found_idx))) {
      LOG_WARN("Failed to locate key in rowkey vector", K(ret), KPC(idx_data_header_));
    } else if (end_ + 1 == found_idx) {
      ret = OB_ITER_END;
    } else {
      current_ = found_idx;
    }
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::find_rowkeys_belong_to_same_idx_row(ObMicroIndexInfo &idx_block_row, int64_t &rowkey_begin_idx, int64_t &rowkey_end_idx, const ObRowsInfo *&rows_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == rows_info || !idx_data_header_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rows info or header", K(ret), KP(rows_info), KPC(idx_data_header_));
  } else {
    bool is_decided = false;
    for (; OB_SUCC(ret) && rowkey_begin_idx < rowkey_end_idx; ++rowkey_begin_idx) {
      if (rows_info->is_row_skipped(rowkey_begin_idx)) {
        continue;
      }
      const ObDatumRowkey &rowkey = rows_info->get_rowkey(rowkey_begin_idx);
      int32_t cmp_ret = 0;
      if (OB_FAIL(idx_data_header_->rowkey_vector_->compare_rowkey(rowkey, current_, *datum_utils_, cmp_ret, false))) {
        LOG_WARN("Failed to compare rowkey", K(ret), K(rowkey), K(current_));
      } else if (cmp_ret < 0) {
        idx_block_row.rowkey_end_idx_ = rowkey_begin_idx;
        is_decided = true;
        break;
      } else if (cmp_ret == 0) {
        idx_block_row.rowkey_end_idx_ = rowkey_begin_idx + 1;
        is_decided = true;
        break;
      }
    }

    if (OB_SUCC(ret) && !is_decided) {
      idx_block_row.rowkey_end_idx_ = rowkey_begin_idx;
    }
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::find_rowkeys_belong_to_curr_idx_row(ObMicroIndexInfo &idx_block_row, const int64_t rowkey_end_idx, const ObRowKeysInfo *rowkeys_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!idx_block_row.endkey_.is_valid() ||
                         nullptr == rowkeys_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkeys info", K(ret), K(idx_block_row.endkey_), KP(rowkeys_info));
  } else {
    for (; OB_SUCC(ret) && idx_block_row.rowkey_end_idx_ < rowkey_end_idx; ++idx_block_row.rowkey_end_idx_) {
      if (rowkeys_info->is_rowkey_not_exist(idx_block_row.rowkey_end_idx_)) {
        continue;
      }
      const ObDatumRowkey &rowkey = rowkeys_info->get_rowkey(idx_block_row.rowkey_end_idx_);
      int cmp_ret = 0;
      if (OB_FAIL(rowkey.compare(idx_block_row.endkey_, *datum_utils_, cmp_ret, false))) {
        LOG_WARN("Failed to compare rowkey", K(ret), K(rowkey), K(idx_block_row.endkey_));
      } else if (cmp_ret >= 0) {
        break;
      }
    }
  }
  return ret;
}

int ObTFMIndexBlockRowIterator::locate_range_by_rowkey_vector(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    const bool is_normal_cg,
    int64_t &begin_idx,
    int64_t &end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(idx_data_header_->rowkey_vector_->locate_range(range,
                                                             is_left_border,
                                                             is_right_border,
                                                             is_normal_cg,
                                                             *datum_utils_,
                                                             begin_idx,
                                                             end_idx))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Failed to locate range by rowkey vector", K(ret));
    }
  }
  return ret;
}

/******************             ObIndexBlockRowScanner              **********************/
ObIndexBlockRowScanner::ObIndexBlockRowScanner()
  : query_range_(nullptr), macro_id_(), allocator_(nullptr), raw_iter_(nullptr), transformed_iter_(nullptr),
    ddl_iter_(nullptr), ddl_merge_iter_(nullptr), iter_(nullptr), datum_utils_(nullptr),
    range_idx_(0), nested_offset_(0), rowkey_begin_idx_(0), rowkey_end_idx_(0),
    index_format_(ObIndexFormat::INVALID), parent_row_range_(), is_get_(false), is_reverse_scan_(false),
    is_left_border_(false), is_right_border_(false), is_inited_(false),
    is_normal_cg_(false), is_normal_query_(true), filter_constant_type_(sql::ObBoolMaskType::PROBABILISTIC),
    iter_param_(), rowkey_col_descs_(nullptr)
{}

ObIndexBlockRowScanner::~ObIndexBlockRowScanner()
{
  reset();
}

void ObIndexBlockRowScanner::reuse()
{
  query_range_ = nullptr;
  if (OB_NOT_NULL(raw_iter_)) {
    raw_iter_->reuse();
  }
  if (OB_NOT_NULL(transformed_iter_)) {
    transformed_iter_->reuse();
  }
  if (OB_NOT_NULL(ddl_iter_)) {
    ddl_iter_->reuse();
  }
  if (OB_NOT_NULL(ddl_merge_iter_)) {
    ddl_merge_iter_->reuse();
  }
  is_left_border_ = false;
  is_right_border_ = false;
  parent_row_range_.reset();
  filter_constant_type_ = sql::ObBoolMaskType::PROBABILISTIC;
  rowkey_col_descs_ = nullptr;
}

void ObIndexBlockRowScanner::reset()
{
  query_range_ = nullptr;
  parent_row_range_.reset();
  if (nullptr != raw_iter_) {
    raw_iter_->reset();
    if (nullptr != allocator_) {
      allocator_->free(raw_iter_);
      raw_iter_ = nullptr;
    }
  }
  if (nullptr != transformed_iter_) {
    transformed_iter_->reset();
    if (nullptr != allocator_) {
      allocator_->free(transformed_iter_);
      transformed_iter_ = nullptr;
    }
  }
  if (nullptr != ddl_iter_) {
    ddl_iter_->reset();
    if (nullptr != allocator_) {
      allocator_->free(ddl_iter_);
      ddl_iter_ = nullptr;
    }
  }
  if (nullptr != ddl_merge_iter_) {
    ddl_merge_iter_->reset();
    if (nullptr != allocator_) {
      allocator_->free(ddl_merge_iter_);
      ddl_merge_iter_ = nullptr;
    }
  }
  iter_ = nullptr;
  datum_utils_ = nullptr;
  range_idx_ = 0;
  nested_offset_ = 0;
  rowkey_begin_idx_ = 0;
  rowkey_end_idx_ = 0;
  index_format_ = ObIndexFormat::INVALID;
  is_get_ = false;
  is_reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  is_inited_ = false;
  is_normal_cg_ = false;
  is_normal_query_ = true;
  iter_param_.reset();
  allocator_ = nullptr;
  filter_constant_type_ = sql::ObBoolMaskType::PROBABILISTIC;
  rowkey_col_descs_ = nullptr;
}

int ObIndexBlockRowScanner::init(
    const ObStorageDatumUtils &datum_utils,
    ObIAllocator &allocator,
    const common::ObQueryFlag &query_flag,
    const int64_t nested_offset,
    const bool is_normal_cg,
    const ObIArray<share::schema::ObColDesc> *rowkey_col_descs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Already inited", K(ret));
  } else if (OB_UNLIKELY(!datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum utils", K(ret), K(datum_utils));
  } else {
    allocator_ = &allocator;
    is_reverse_scan_ = query_flag.is_reverse_scan();
    datum_utils_ = &datum_utils;
    nested_offset_ = nested_offset;
    is_normal_cg_ = is_normal_cg;
    is_normal_query_ = !query_flag.is_daily_merge() && !query_flag.is_multi_version_minor_merge();
    rowkey_col_descs_ = rowkey_col_descs;
    is_inited_ = true;
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObDatumRowkey &rowkey,
    const int64_t range_idx,
    const ObMicroIndexInfo *idx_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || !rowkey.is_valid()
      || !idx_block_data.is_index_block() || (is_normal_cg_ && nullptr == idx_info))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret),
        K(macro_id), K(idx_block_data), K(rowkey), K_(is_normal_cg), KP(idx_info));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data), K(index_format_));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret));
  } else if (is_normal_cg_ && !idx_info->is_root() && idx_info->is_macro_node()) {
    // Rowkey offset in macro node is local
    ObStorageDatum offset;
    ObDatumRowkey offset_rowkey;
    offset.set_int(rowkey.datums_[0].get_int() - idx_info->get_row_range().start_row_id_);
    offset_rowkey.assign(&offset, 1);
    if (OB_FAIL(iter_->locate_key(offset_rowkey))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to locate rowkey", K(ret), K(idx_block_data), K(offset_rowkey), KPC(iter_));
      } else {
        ret = OB_SUCCESS; // return OB_ITER_END on get_next() for get
      }
    }
  } else if (OB_FAIL(iter_->locate_key(rowkey))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Fail to locate rowkey", K(ret), K(idx_block_data), K(rowkey), KPC(iter_));
    } else {
      ret = OB_SUCCESS; // return OB_ITER_END on get_next() for get
    }
  }
  if (OB_SUCC(ret)) {
    macro_id_ = macro_id;
    range_idx_ = range_idx;
    rowkey_ = &rowkey;
    is_get_ = true;
    if (nullptr != idx_info) {
      parent_row_range_ = idx_info->get_row_range();
    } else {
      parent_row_range_.reset();
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObRowsInfo *rows_info,
    const int64_t rowkey_begin_idx,
    const int64_t rowkey_end_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || nullptr == rows_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret), K(macro_id), K(idx_block_data),
              KP(rows_info));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret), KPC(iter_));
  } else if (OB_FAIL(iter_->locate_range())) {
    LOG_WARN("fail to locate range", K(ret), KPC(iter_));
  } else {
    macro_id_ = macro_id;
    rows_info_ = rows_info;
    rowkey_begin_idx_ = rowkey_begin_idx;
    rowkey_end_idx_ = rowkey_end_idx;
    is_get_ = false;
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObRowKeysInfo *row_keys_info,
    const int64_t rowkey_begin_idx,
    const int64_t rowkey_end_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_keys_info || rowkey_begin_idx >= rowkey_end_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret), KP(row_keys_info), K(rowkey_begin_idx), K(rowkey_end_idx));
  } else {
    const ObDatumRowkey &first_rowkey = row_keys_info->get_rowkey(rowkey_begin_idx);
    if (OB_FAIL(open(macro_id, idx_block_data, first_rowkey, rowkey_begin_idx))) {
      LOG_WARN("Fail to open", K(ret));
    } else {
      rowkeys_info_ = row_keys_info;
      rowkey_begin_idx_ = rowkey_begin_idx;
      rowkey_end_idx_ = rowkey_end_idx;
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObDatumRange &range,
    const int64_t range_idx,
    const bool is_left_border,
    const bool is_right_border,
    const ObMicroIndexInfo *idx_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || !range.is_valid()
      || !idx_block_data.is_index_block() || (is_normal_cg_ && nullptr == idx_info))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret), K(idx_block_data), K(range), K_(is_normal_cg), KP(idx_info));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret));
  } else if (OB_FAIL(locate_range(range, is_left_border, is_right_border))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Fail to locate range", K(ret), K(range), K(is_left_border), K(is_right_border));
    }
  } else {
    macro_id_ = macro_id;
    is_left_border_ = is_left_border;
    is_right_border_ = is_right_border;
    range_idx_ = range_idx;
    is_get_ = false;
    if (nullptr != idx_info) {
      parent_row_range_ = idx_info->get_row_range();
      filter_constant_type_ = idx_info->get_filter_constant_type();
    } else {
      parent_row_range_.reset();
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::get_next(
    ObMicroIndexInfo &idx_block_row,
    const bool is_multi_check,
    const bool is_sorted_multi_get)
{
  int ret = OB_SUCCESS;
  idx_block_row.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (end_of_block()) {
    ret = OB_ITER_END;
  } else if (is_multi_check && OB_FAIL(skip_to_next_valid_position(idx_block_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to skip to next valid position", K(ret), K(rowkey_begin_idx_), K(rowkey_end_idx_), KPC(rows_info_));
    } else if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(index_format_), K(ret));
    } else {
      iter_->set_iter_end();
    }
  } else if (OB_FAIL(get_next_idx_row(idx_block_row))) {
    LOG_WARN("Failed to get next idx row", K(ret), K(is_multi_check));
  } else if (is_sorted_multi_get) {
    idx_block_row.rowkeys_info_ = rowkeys_info_;
    idx_block_row.rowkey_begin_idx_ = rowkey_begin_idx_;
    idx_block_row.rowkey_end_idx_ = rowkey_begin_idx_ + 1;
    if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(index_format_), K(ret));
    } else if (OB_FAIL(iter_->find_rowkeys_belong_to_curr_idx_row(idx_block_row, rowkey_end_idx_, rowkeys_info_))) {
      LOG_WARN("Failed to find rowkeys", K(ret));
    }
  }
  return ret;
}

void ObIndexBlockRowScanner::set_iter_param(const blocksstable::ObSSTable *sstable,
                                            const ObTablet *tablet)
{
  iter_param_.sstable_ = sstable;
  iter_param_.tablet_ = tablet;
}

bool ObIndexBlockRowScanner::end_of_block() const
{
  int ret = OB_SUCCESS;
  bool bret = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret));
  } else {
    bret = iter_->end_of_block();
  }
  return bret;
}

bool ObIndexBlockRowScanner::is_ddl_merge_type() const
{
  return OB_NOT_NULL(iter_param_.sstable_) && iter_param_.sstable_->is_ddl_merge_sstable();
}

int ObIndexBlockRowScanner::get_index_row_count(int64_t &index_row_count) const
{
  int ret = OB_SUCCESS;
  index_row_count = 0;
  int64_t data_row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_ISNULL(iter_) || OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret), KP(iter_), KP(range_));
  } else if (OB_FAIL(iter_->get_index_row_count(*range_, is_left_border_, is_right_border_, index_row_count, data_row_count))) {
    LOG_WARN("get index row count failed", K(ret), KP(range_));
  }
 return ret;
}

int ObIndexBlockRowScanner::check_blockscan(
    const ObDatumRowkey &rowkey,
    bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (is_reverse_scan_) {
    if (rowkey.is_min_rowkey()) {
      can_blockscan = true;
    } else {
      // TODO(yuanzhe) opt this
      can_blockscan = false;
    }
  } else if (rowkey.is_max_rowkey()) {
    can_blockscan = true;
  } else {
    if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(index_format_), K(ret));
    } else if (OB_FAIL(iter_->check_blockscan(rowkey, can_blockscan))) {
      LOG_WARN("fail to check iter can block scan", K(ret), KPC(iter_), K(can_blockscan), K(rowkey));
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::init_by_micro_data(const ObMicroBlockData &idx_block_data)
{
  int ret = OB_SUCCESS;
  void *iter_buf = nullptr;
  if (ObMicroBlockData::INDEX_BLOCK == idx_block_data.type_ || ObMicroBlockData::DDL_MERGE_INDEX_BLOCK == idx_block_data.type_) {
    if (ObMicroBlockData::DDL_MERGE_INDEX_BLOCK == idx_block_data.type_ && is_ddl_merge_type() && is_normal_query_) {
      if (OB_NOT_NULL(ddl_merge_iter_)) {
        iter_ = ddl_merge_iter_;
        index_format_ = ObIndexFormat::DDL_MERGE;
      } else {
        if (OB_ISNULL(iter_buf = allocator_->alloc(sizeof(ObDDLMergeBlockRowIterator)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDDLMergeBlockRowIterator)));
        } else if (FALSE_IT(ddl_merge_iter_ = new (iter_buf) ObDDLMergeBlockRowIterator)) {
        } else {
          iter_ = ddl_merge_iter_;
          index_format_ = ObIndexFormat::DDL_MERGE;
        }
      }
    } else {
      if (nullptr == idx_block_data.get_extra_buf()) {
        if (OB_NOT_NULL(raw_iter_)) {
          iter_ = raw_iter_;
          index_format_ = ObIndexFormat::RAW_DATA;
        } else {
          if (OB_ISNULL(iter_buf = allocator_->alloc(sizeof(ObRAWIndexBlockRowIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObRAWIndexBlockRowIterator)));
          } else if (FALSE_IT(raw_iter_ = new (iter_buf) ObRAWIndexBlockRowIterator)) {
          } else {
            iter_ = raw_iter_;
            index_format_ = ObIndexFormat::RAW_DATA;
          }
        }
      } else {
        if (OB_NOT_NULL(transformed_iter_)) {
          iter_ = transformed_iter_;
          index_format_ = ObIndexFormat::TRANSFORMED;
        } else {
          if (OB_ISNULL(iter_buf = allocator_->alloc(sizeof(ObTFMIndexBlockRowIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObTFMIndexBlockRowIterator)));
          } else if (FALSE_IT(transformed_iter_ = new (iter_buf) ObTFMIndexBlockRowIterator)) {
          } else {
            iter_ = transformed_iter_;
            index_format_ = ObIndexFormat::TRANSFORMED;
          }
        }
      }
    }
  } else if (ObMicroBlockData::DDL_BLOCK_TREE == idx_block_data.type_) {
    if (OB_NOT_NULL(ddl_iter_)) {
      iter_ = ddl_iter_;
      index_format_ = ObIndexFormat::BLOCK_TREE;
    } else {
      if (OB_ISNULL(iter_buf = allocator_->alloc(sizeof(ObDDLIndexBlockRowIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDDLIndexBlockRowIterator)));
      } else if (FALSE_IT(ddl_iter_ = new (iter_buf) ObDDLIndexBlockRowIterator)) {
      } else {
        iter_ = ddl_iter_;
        index_format_ = ObIndexFormat::BLOCK_TREE;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(index_format_), K(ret));
    } else if (OB_FAIL(iter_->init(idx_block_data, datum_utils_, allocator_, is_reverse_scan_, iter_param_))) {
      LOG_WARN("fail to init iter", K(ret), K(idx_block_data), KPC(iter_));
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::locate_range(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret), KPC(iter_));
  } else if (OB_FAIL(iter_->locate_range(range, is_left_border, is_right_border, is_normal_cg_))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Fail to locate range", K(ret), K(range), K(is_left_border), K(is_right_border), KPC(iter_));
    }
  } else {
    range_ = &range;
    LOG_DEBUG("Locate range in index block by range", K(ret), K(range), KPC(iter_),
      K(is_left_border), K(is_right_border), KP(this));
  }
  return ret;
}

int ObIndexBlockRowScanner::advance_to_border(
    const ObDatumRowkey &rowkey,
    const int32_t range_idx,
    ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_format_ != ObIndexFormat::TRANSFORMED) || OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error", K(ret), K(index_format_), KP(iter_));
  } else if (OB_UNLIKELY(end_of_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error", K(ret), K(end_of_block()));
  } else if (range_idx == range_idx_) {
    if(OB_FAIL(iter_->advance_to_border(rowkey, is_left_border_, is_right_border_, parent_row_range_, cs_range))) {
      LOG_WARN("Failed to advance to border", K(range_idx));
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::find_out_rows(
    const int32_t range_idx,
    int64_t &found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K_(is_inited));
  } else if (OB_ISNULL(iter_) || OB_UNLIKELY(index_format_ != ObIndexFormat::TRANSFORMED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null or wrong format", K(index_format_), K(ret));
  } else if (OB_FAIL(iter_->find_out_rows(range_idx, range_idx_, found_idx))) {
    LOG_WARN("fail to find out rows", K(ret), K(range_idx), K(range_idx_), K(found_idx));
  }
  LOG_DEBUG("ObIndexBlockRowScanner::find_out_rows", K(range_idx), KPC(iter_), K(found_idx));
  return ret;
}

int ObIndexBlockRowScanner::find_out_rows_from_start_to_end(
    const int32_t range_idx,
    const ObCSRowId start_row_id,
    bool &is_certain,
    int64_t &found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  is_certain = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K_(is_inited));
  } else if (OB_ISNULL(iter_) || OB_UNLIKELY(index_format_ != ObIndexFormat::TRANSFORMED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null or wrong format", KP(iter_), K(index_format_), K(ret));
  } else if (OB_FAIL(iter_->find_out_rows_from_start_to_end(range_idx, range_idx_, start_row_id, parent_row_range_, is_certain, found_idx))) {
    LOG_WARN("fail to find out rows from start to end", K(ret), K(range_idx), K(start_row_id), K(parent_row_range_), K(is_certain), K(found_idx));
  }
  return ret;
}

int ObIndexBlockRowScanner::get_end_key(ObCommonDatumRowkey &endkey) const
{
  int ret = OB_SUCCESS;
  endkey.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K_(is_inited));
  } else if (OB_ISNULL(iter_) || OB_UNLIKELY(index_format_ != ObIndexFormat::TRANSFORMED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null or wrong format", KP(iter_), K(index_format_), K(ret));
  } else if (OB_FAIL(iter_->get_end_key(endkey))) {
    LOG_WARN("Failed to get end key", K(ret));
  }
  return ret;
}

void ObIndexBlockRowScanner::switch_context(const ObSSTable &sstable,
                                            const ObTablet *tablet,
                                            const ObStorageDatumUtils &datum_utils,
                                            ObTableAccessContext &access_ctx,
                                            const ObIArray<share::schema::ObColDesc> *rowkey_col_descs)
{
  nested_offset_ = sstable.get_macro_offset();
  datum_utils_ = &datum_utils;
  is_normal_cg_ = sstable.is_normal_cg_sstable();
  is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
  is_normal_query_ = !access_ctx.query_flag_.is_daily_merge() && !access_ctx.query_flag_.is_multi_version_minor_merge();
  rowkey_col_descs_ = rowkey_col_descs;
  iter_param_.sstable_ = &sstable;
  iter_param_.tablet_ = tablet;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(iter_)) {
    ObStorageDatumUtils *switch_datum_utils = const_cast<ObStorageDatumUtils *>(datum_utils_);
    iter_->switch_context(switch_datum_utils);
  }
}

int ObIndexBlockRowScanner::get_next_idx_row(ObMicroIndexInfo &idx_block_row)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowMinorMetaInfo *idx_minor_info = nullptr;
  const char *idx_data_buf = nullptr;
  const char *agg_row_buf = nullptr;
  int64_t agg_buf_size = 0;
  int64_t row_offset = 0;
  bool is_scan_left_border = false;
  bool is_scan_right_border = false;
  if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(ret), K(index_format_), KP(iter_));
  } else {
    if (OB_FAIL(iter_->get_next(idx_row_header, idx_block_row.endkey_, is_scan_left_border, is_scan_right_border, idx_minor_info, agg_row_buf, agg_buf_size, row_offset))) {
      LOG_WARN("get next idx block row failed", K(ret), KP(idx_row_header), K(is_reverse_scan_));
    } else if (OB_UNLIKELY(nullptr == idx_row_header || !idx_block_row.endkey_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null index block row header/endkey", K(ret), KPC(iter_),
              K(index_format_), KP(idx_row_header), K(idx_block_row.endkey_));
    }
  }

  if (OB_SUCC(ret)) {
    idx_block_row.ps_node_ = iter_->get_cur_ps_node();
    idx_block_row.flag_ = 0;
    idx_block_row.row_header_ = idx_row_header;
    idx_block_row.minor_meta_info_ = idx_minor_info;
    idx_block_row.is_get_ = is_get_;
    idx_block_row.is_left_border_ = is_left_border_ && is_scan_left_border;
    idx_block_row.is_right_border_ = is_right_border_ && is_scan_right_border;
    idx_block_row.copy_lob_out_row_flag();
    idx_block_row.range_idx_ = range_idx_;
    idx_block_row.query_range_ = query_range_;
    idx_block_row.parent_macro_id_ = macro_id_;
    idx_block_row.nested_offset_ = nested_offset_;
    idx_block_row.agg_row_buf_ = agg_row_buf;
    idx_block_row.agg_buf_size_ = agg_buf_size;
    idx_block_row.rowkey_col_descs_ = rowkey_col_descs_;
    if (is_normal_cg_) {
      int64_t row_offset;
      if (OB_FAIL(idx_block_row.endkey_.get_column_int(0, row_offset))) {
        LOG_WARN("Failed to get datum int", K(ret));
      } else {
        idx_block_row.cs_row_range_.start_row_id_ =row_offset - idx_block_row.get_row_count() + 1;
        idx_block_row.cs_row_range_.end_row_id_ = row_offset;
        idx_block_row.set_filter_constant_type(filter_constant_type_);
      }
    } else {
      idx_block_row.cs_row_range_.start_row_id_ = row_offset - idx_block_row.get_row_count() + 1;
      idx_block_row.cs_row_range_.end_row_id_ = row_offset;
    }
    if (OB_SUCC(ret) && idx_block_row.is_data_block()) {
      idx_block_row.cs_row_range_.start_row_id_ += parent_row_range_.start_row_id_;
      idx_block_row.cs_row_range_.end_row_id_ += parent_row_range_.start_row_id_;
    }
  }
  LOG_DEBUG("Get next index block row", K(ret), KPC(iter_), K(idx_block_row), KP(this), K(idx_block_row.endkey_));
  return ret;
}

void ObIndexBlockRowScanner::skip_index_rows()
{
  for (; rowkey_begin_idx_ < rowkey_end_idx_; ++rowkey_begin_idx_) {
    if (!rows_info_->is_row_skipped(rowkey_begin_idx_)) {
      break;
    }
  }
}

int ObIndexBlockRowScanner::skip_to_next_valid_position(ObMicroIndexInfo &idx_block_row)
{
  int ret = OB_SUCCESS;
  skip_index_rows();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (rowkey_begin_idx_ == rowkey_end_idx_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(index_format_), K(ret));
  } else if (OB_FAIL(iter_->skip_to_next_valid_position(rows_info_->get_rowkey(rowkey_begin_idx_)))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to skip to next valid position", K(ret), K(rowkey_begin_idx_), K(rowkey_end_idx_), KPC(rows_info_), KPC(iter_));
    }
  } else {
    idx_block_row.rows_info_ = rows_info_;
    idx_block_row.rowkey_begin_idx_ = rowkey_begin_idx_;
    if (OB_FAIL(iter_->find_rowkeys_belong_to_same_idx_row(idx_block_row, rowkey_begin_idx_, rowkey_end_idx_, rows_info_))) {
      LOG_WARN("Failed to find rowkeys belong to same index row", K(ret), KPC(iter_));
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
