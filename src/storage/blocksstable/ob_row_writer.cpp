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
#include "ob_row_writer.h"
#include "ob_block_sstable_struct.h"
#include "storage/ob_i_store.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace blocksstable;

ObRowWriter::ObRowWriter()
  : buf_(NULL),
    buf_size_(0),
    start_pos_(0),
    pos_(0),
    row_header_(NULL),
    column_index_count_(0),
    rowkey_column_cnt_(0),
    update_idx_array_(nullptr),
    update_array_idx_(0),
    cluster_cnt_(0),
    row_buffer_()
{
  STATIC_ASSERT(sizeof(ObBitArray<48>) < 400, "row buffer size");
  STATIC_ASSERT(sizeof(ObRowWriter) < 6100, "row buffer size");
}

ObRowWriter::~ObRowWriter()
{
  reset();
}

void ObRowWriter::reset()
{
  buf_ = nullptr;
  buf_size_ = 0;
  start_pos_ = 0;
  pos_ = 0;
  row_header_ = nullptr;
  column_index_count_ = 0;
  column_index_count_ = 0;
  update_idx_array_ = nullptr;
  update_array_idx_ = 0;
  cluster_cnt_ = 0;
  //row_buffer_.reset(); row_buffer_ maybe do not need reset
}

int ObRowWriter::init_common(char *buf, const int64_t buf_size, const int64_t pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_size <= 0 || pos < 0 || pos >= buf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row writer input argument", K(ret), K(buf), K(buf_size), K(pos));
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    start_pos_ = pos;
    pos_ = pos;
    row_header_ = NULL;
    column_index_count_ = 0;
    update_array_idx_ = 0;
  }
  return ret;
}

int ObRowWriter::check_row_valid(
    const ObStoreRow &row,
    const int64_t rowkey_column_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row writer input argument", K(row), K(ret));
  } else if (OB_UNLIKELY(rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row writer input argument",
        K(rowkey_column_count), K(row.row_val_.count_), K(ret));
  }
  return ret;
}

int ObRowWriter::append_row_header(
    const uint8_t row_flag,
    const uint8_t multi_version_flag,
    const int64_t trans_id,
    const int64_t column_cnt,
    const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t row_header_size = ObRowHeader::get_serialized_size();
  if (OB_UNLIKELY(pos_ + row_header_size > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not enough", K(ret), K(pos_), K(buf_size_));
  } else {
    row_header_ = reinterpret_cast<ObRowHeader*>(buf_ + pos_);
    row_header_->set_version(ObRowHeader::ROW_HEADER_VERSION_1);
    row_header_->set_row_flag(row_flag);
    row_header_->set_row_mvcc_flag(multi_version_flag);
    row_header_->set_column_count(column_cnt);
    row_header_->set_rowkey_count(rowkey_cnt);
    row_header_->clear_reserved_bits();
    row_header_->set_trans_id(trans_id); // TransID

    pos_ += row_header_size; // move forward
  }
  return ret;
}

int ObRowWriter::write(
    const int64_t rowkey_column_count,
    const ObDatumRow &row,
    char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(buf, buf_size, pos))) { // check argument in init_common
    LOG_WARN("row writer fail to init common", K(ret), K(OB_P(buf)), K(buf_size), K(pos));
  } else if (OB_FAIL(append_row_header(
      row.row_flag_.get_serialize_flag(),
      row.mvcc_row_flag_.flag_,
      row.trans_id_.get_id(),
      row.count_,
      rowkey_column_count))) {
    LOG_WARN("row writer fail to append row header", K(ret), K(row));
  } else {
    update_idx_array_ = nullptr;
    rowkey_column_cnt_ = rowkey_column_count;
    if (OB_FAIL(inner_write_cells(row.storage_datums_, row.count_))) {
      LOG_WARN("failed to write datums", K(ret), K(row));
    } else {
      LOG_DEBUG("write row", K(ret), K(pos_), K(row));
      pos = pos_;
    }
  }
  return ret;
}

int ObRowWriter::alloc_buf_and_init(const bool retry)
{
  int ret = OB_SUCCESS;
  if (retry && OB_FAIL(row_buffer_.extend_buf())) {
    STORAGE_LOG(WARN, "Failed to extend buffer", K(ret), K(row_buffer_));
  } else if (OB_FAIL(init_common(row_buffer_.get_buf(), row_buffer_.get_buf_size(), 0))) {
    LOG_WARN("row writer fail to init common", K(ret), K(row_buffer_));
  }
  return ret;
}

int ObRowWriter::write_rowkey(const common::ObStoreRowkey &rowkey, char *&buf, int64_t &len)
{
  int ret = OB_SUCCESS;
  len = 0;
  do {
    if (OB_FAIL(alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) {
      LOG_WARN("row writer fail to alloc and init", K(ret));
    } else if (OB_FAIL(append_row_header(0, 0, 0, rowkey.get_obj_cnt(), rowkey.get_obj_cnt()))) {
      LOG_WARN("row writer fail to append row header", K(ret));
    } else {
      update_idx_array_ = nullptr;
      rowkey_column_cnt_ = rowkey.get_obj_cnt();
      if (OB_FAIL(inner_write_cells(rowkey.get_obj_ptr(), rowkey.get_obj_cnt()))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("failed to write cells", K(ret), K(rowkey));
        }
      } else {
        buf = row_buffer_.get_buf();
        len = pos_;
        LOG_DEBUG("finish write rowkey", K(ret), KPC(row_header_), K(rowkey));
      }
    }
  } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

  if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH == ret)) {
    LOG_WARN("fail to append row due to buffer not enough", K(ret), K(row_buffer_), K(rowkey));
  }
  return ret;
}

int ObRowWriter::write(const int64_t rowkey_column_cnt, const ObDatumRow &datum_row, char *&buf, int64_t &len)
{
  int ret = OB_SUCCESS;
  len = 0;
  do {
    if (OB_FAIL(alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) {
      LOG_WARN("row writer fail to alloc and init", K(ret));
    } else if (OB_FAIL(append_row_header(
            datum_row.row_flag_.get_serialize_flag(),
            datum_row.mvcc_row_flag_.flag_,
            datum_row.trans_id_.get_id(),
            datum_row.count_,
            rowkey_column_cnt))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("row writer fail to append row header", K(ret), K(datum_row));
      }
    } else {
      update_idx_array_ = nullptr;
      rowkey_column_cnt_ = rowkey_column_cnt;
      if (OB_FAIL(inner_write_cells(datum_row.storage_datums_, datum_row.count_))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("failed to write datums", K(ret), K(datum_row));
        }
      } else {
        len = pos_;
        buf = row_buffer_.get_buf();
        LOG_DEBUG("finish write row", K(ret), KPC(row_header_), K(datum_row));
      }
    }
  } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

  if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH == ret)) {
    LOG_WARN("fail to append row due to buffer not enough", K(ret), K(row_buffer_), K(datum_row), K(rowkey_column_cnt));
  }
  return ret;
}

// when update_idx == nullptr, write full row; else only write rowkey + update cells
int ObRowWriter::write(
    const int64_t rowkey_column_count,
    const storage::ObStoreRow &row,
    const ObIArray<int64_t> *update_idx,
    char *&buf,
    int64_t &len)
{
  int ret = OB_SUCCESS;
  len = 0;
  if (OB_UNLIKELY(nullptr != update_idx && update_idx->count() > row.row_val_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update idx is invalid", K(ret), KPC(update_idx), K_(row.row_val_.count), K(rowkey_column_count));
  } else {
    do {
      if (OB_FAIL(alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) {
        LOG_WARN("row writer fail to alloc and init", K(ret));
      } else if (OB_FAIL(inner_write_row(rowkey_column_count, row, update_idx))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("row writer fail to append row header", K(ret), K(row_buffer_), K(pos_));
        }
      } else {
        len = pos_;
        buf = row_buffer_.get_buf();
      }
    } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH == ret)) {
      LOG_WARN("fail to append row due to buffer not enough", K(ret), K_(row_buffer), K(rowkey_column_count));
    }
  }
  return ret;
}

int ObRowWriter::check_update_idx_array_valid(
    const int64_t rowkey_column_count,
    const ObIArray<int64_t> *update_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_column_count <= 0 || nullptr == update_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_column_count), KPC(update_idx));
  }
  for (int i = 0; OB_SUCC(ret) && i < update_idx->count(); ++i) {
    if (i > 0) {
      if (update_idx->at(i) <= update_idx->at(i - 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update idx array is invalid", K(ret), K(i), KPC(update_idx));
      }
    } else if (update_idx->at(i) < rowkey_column_count) { // i == 0
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update idx array is invalid", K(ret), K(i), KPC(update_idx));
    }
  }
  return ret;
}

int ObRowWriter::inner_write_row(
    const int64_t rowkey_column_count,
    const ObStoreRow &row,
    const ObIArray<int64_t> *update_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_row_valid(row, rowkey_column_count))) {
    LOG_WARN("row writer fail to init store row", K(ret), K(rowkey_column_count));
  } else if (nullptr != update_idx && OB_FAIL(check_update_idx_array_valid(rowkey_column_count, update_idx))) {
    LOG_WARN("invalid update idx array", K(ret));
  } else if (OB_FAIL(append_row_header(
          row.flag_.get_serialize_flag(),
          row.row_type_flag_.flag_,
          row.trans_id_.get_id(),
          row.row_val_.count_,
          rowkey_column_count))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to append row header", K(ret), K(row));
    }
  } else {
    update_idx_array_ = update_idx;
    rowkey_column_cnt_ = rowkey_column_count;
    if (OB_FAIL(inner_write_cells(row.row_val_.cells_, row.row_val_.count_))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write cells", K(ret), K(row));
      }
    }
  }
  return ret;
}

template <typename T>
int ObRowWriter::inner_write_cells(
    const T *cells,
    const int64_t cell_cnt)
{
  int ret = OB_SUCCESS;
  if (cell_cnt <= ObRowHeader::USE_CLUSTER_COLUMN_COUNT) {
    cluster_cnt_ = 1;
    use_sparse_row_[0] = false;
  } else { // loop cells to decide cluster & sparse
    loop_cells(cells, cell_cnt, cluster_cnt_, use_sparse_row_);
  }
  if (OB_UNLIKELY(cluster_cnt_ >= MAX_CLUSTER_CNT
      || (cluster_cnt_ > 1 && cluster_cnt_ != ObRowHeader::calc_cluster_cnt(rowkey_column_cnt_, cell_cnt)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster count is invalid", K(ret), K(cluster_cnt_));
  } else if (OB_FAIL(build_cluster(cell_cnt, cells))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to build cluster", K(ret));
    }
  } else {
    row_header_->set_single_cluster(1 == cluster_cnt_);
    LOG_DEBUG("inner_write_row", K(ret), KPC(row_header_), K(use_sparse_row_[0]), K(column_index_count_), K(pos_));
  }

  return ret;
}

template<typename T, typename R>
int ObRowWriter::append_row_and_index(
    const T *cells,
    const int64_t offset_start_pos,
    const int64_t start_idx,
    const int64_t end_idx,
    const bool is_sparse_row,
    R &bytes_info)
{
  int ret = OB_SUCCESS;
  column_index_count_ = 0; // set the column_index & column_idx array empty
  ObColClusterInfoMask::BYTES_LEN offset_type = ObColClusterInfoMask::BYTES_MAX;
  if (is_sparse_row) {
    ObColClusterInfoMask::BYTES_LEN col_idx_type = ObColClusterInfoMask::BYTES_MAX;
    if (OB_FAIL(append_sparse_cell_array(cells, offset_start_pos, start_idx, end_idx))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("row writer fail to append store row", K(ret));
      }
    } else if (OB_FAIL(append_array(column_idx_, column_index_count_, col_idx_type))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to append column idx array", K(ret));
      }
    } else if (OB_FAIL(bytes_info.set_column_idx_type(col_idx_type))) {
      LOG_WARN("failed to set column idx bytes", K(ret), K(col_idx_type), K(column_index_count_));
    }
  } else if (OB_FAIL(append_flat_cell_array(cells, offset_start_pos, start_idx, end_idx))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to append store row", K(ret));
    }
  }
  LOG_DEBUG("before append column offset array", K(ret), K(pos_), K(column_index_count_));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array(column_offset_, column_index_count_, offset_type))) {
    if (OB_BUF_NOT_ENOUGH != ret)  {
      LOG_WARN("row writer fail to append column index", K(ret));
    }
  } else if (OB_FAIL(bytes_info.set_offset_type(offset_type))) {
    LOG_WARN("failed to set offset bytes", K(ret));
  } else if (OB_FAIL(append_special_val_array(column_index_count_))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("failed to append special val array", K(ret), K(column_index_count_));
    }
  }
  return ret;
}

int ObRowWriter::append_special_val_array(const int64_t cell_count)
{
  int ret = OB_SUCCESS;
  const int64_t copy_size = (sizeof(uint8_t) * cell_count + 1) >> 1;
  if (pos_ + copy_size > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    for (int64_t i = 0; i < copy_size; i++) {
      // Two special value in one byte.
      // In eache byte, The special value behind locates in higher bit.
      // Each value in special_vals_ must be less than 0x0F.
      const int64_t low_bit_idx = i * 2;
      const int64_t high_bit_idx = low_bit_idx + 1;
      const uint8_t low_bit_value = special_vals_[low_bit_idx];
      const uint8_t high_bit_value = (high_bit_idx == cell_count 
                                         ? 0 : special_vals_[high_bit_idx]);   
      const uint8_t packed_value = low_bit_value | (high_bit_value << 4);  
      special_vals_[i] = packed_value;
    }
    MEMCPY(buf_ + pos_, special_vals_, copy_size);
    pos_ += copy_size;
  }
  return ret;
}

bool ObRowWriter::check_col_exist(const int64_t col_idx)
{
  bool bret = false;
  if (nullptr == update_idx_array_) {
    bret = true;
  } else if (update_array_idx_ < update_idx_array_->count()
      && col_idx == update_idx_array_->at(update_array_idx_)) {
    update_array_idx_++;
    bret = true;
  } else if (col_idx < rowkey_column_cnt_) {
    bret = true;
  }
  return bret;
}

template<typename T>
int ObRowWriter::write_col_in_cluster(
    const T *cells,
    const int64_t cluster_idx,
    const int64_t start_col_idx,
    const int64_t end_col_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_idx < 0 || start_col_idx < 0 || start_col_idx > end_col_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cluster_idx), K(start_col_idx), K(end_col_idx));
  } else if (pos_ + ObColClusterInfoMask::get_serialized_size() > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    // serialize info mask for current cluster
    const int64_t offset_start_pos = pos_;
    ObColClusterInfoMask *info_mask = reinterpret_cast<ObColClusterInfoMask*>(buf_ + pos_);
    pos_ += ObColClusterInfoMask::get_serialized_size();
    info_mask->reset();

    if (OB_FAIL(append_row_and_index(
          cells,
          offset_start_pos,
          start_col_idx,
          end_col_idx,
          use_sparse_row_[cluster_idx],
          *info_mask))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to append row and index", K(ret), K(cluster_idx), K(start_col_idx), K(end_col_idx));
      }
    } else {
      info_mask->set_sparse_row_flag(use_sparse_row_[cluster_idx]);
      info_mask->set_sparse_column_count(use_sparse_row_[cluster_idx] ? column_index_count_ : 0);
      info_mask->set_column_count(1 == cluster_cnt_ ? UINT8_MAX : end_col_idx - start_col_idx);
      LOG_DEBUG("after append_row_and_index", K(ret), KPC(info_mask), K(pos_), K(cluster_idx),
          K(start_col_idx), K(end_col_idx));
    }
  }
  return ret;
}

template<typename T>
int ObRowWriter::build_cluster(const int64_t col_cnt, const T *cells)
{
  int ret = OB_SUCCESS;
  ObColClusterInfoMask::BYTES_LEN cluster_offset_type = ObColClusterInfoMask::BYTES_MAX;
  bool rowkey_independent_cluster = ObRowHeader::need_rowkey_independent_cluster(rowkey_column_cnt_);

  int cluster_idx = 0;
  int64_t start_col_idx = 0;
  int64_t end_col_idx = 0;
  if (rowkey_independent_cluster || 1 == cluster_cnt_) {
    // write rowkey cluster OR only cluster
    cluster_offset_.set_val(cluster_idx, pos_ - start_pos_);
    end_col_idx = (1 == cluster_cnt_) ? col_cnt : rowkey_column_cnt_;
    if (OB_FAIL(write_col_in_cluster(
        cells,
        cluster_idx++,
        0/*start_col_idx*/,
        end_col_idx))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write col in cluster", K(ret));
      }
    }
  }
  LOG_DEBUG("chaser debug rowkey independent", K(ret), K(rowkey_column_cnt_),
      K(rowkey_independent_cluster), K(cluster_idx), K(end_col_idx));

  for ( ; OB_SUCC(ret) && cluster_idx < cluster_cnt_; ++cluster_idx) {
    const int64_t cluster_offset = pos_ - start_pos_;
    cluster_offset_.set_val(cluster_idx, cluster_offset);
    LOG_DEBUG("build_cluster", K(cluster_idx), K(cluster_offset), K(cluster_cnt_),
        K(use_sparse_row_[cluster_idx]));
    start_col_idx = end_col_idx;
    end_col_idx += ObRowHeader::CLUSTER_COLUMN_CNT;
    if (OB_FAIL(write_col_in_cluster(
        cells,
        cluster_idx,
        start_col_idx,
        MIN(end_col_idx, col_cnt)))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write col in cluster", K(ret), K(cluster_idx), K(start_col_idx), K(end_col_idx));
      }
    }
    LOG_DEBUG("chaser debug writer", K(ret), K(cluster_idx), K(start_col_idx), K(end_col_idx), K(cells));
  } // end of for

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array(cluster_offset_, cluster_cnt_, cluster_offset_type))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to append cluster offset array", K(ret));
    }
  } else if(OB_FAIL(row_header_->set_offset_type(cluster_offset_type))) {
    LOG_WARN("failed to set offset bytes", K(ret));
  }
  return ret;
}


template <typename T>
void ObRowWriter::loop_cells(
    const T *cells,
    const int64_t cell_cnt,
    int64_t &cluster_cnt,
    bool *output_sparse_row)
{
  int32_t total_nop_count = 0;
  const bool rowkey_dependent_cluster  = ObRowHeader::need_rowkey_independent_cluster(rowkey_column_cnt_);
  const bool multi_cluster = cell_cnt > ObRowHeader::USE_CLUSTER_COLUMN_COUNT;
  if (!multi_cluster) { // no cluster
    cluster_cnt = 1;
    if (rowkey_dependent_cluster) {
      output_sparse_row[0] = false;
    } else {
      if (nullptr != update_idx_array_) {
        output_sparse_row[0] = (cell_cnt - update_idx_array_->count() + rowkey_column_cnt_) >= USE_SPARSE_NOP_CNT_IN_CLUSTER;
      } else { // no update idx array, need loop row
        for (int i = rowkey_column_cnt_; i < cell_cnt; ++i) {
          if (cells[i].is_nop_value()) {
            ++total_nop_count;
          }
        }
        output_sparse_row[0] = total_nop_count >= USE_SPARSE_NOP_CNT_IN_CLUSTER;
      }
    }
  } else { // no update idx array, need loop row
    int64_t idx = 0;
    int32_t cluster_idx = 0;
    int64_t update_idx = 0;
    if (rowkey_dependent_cluster) { // for rowkey cluster
      output_sparse_row[cluster_idx++] = false;
      idx = rowkey_column_cnt_;
    }
    while (idx < cell_cnt) {
      int32_t tmp_nop_count = 0;
      for (int i = 0; i < ObRowHeader::CLUSTER_COLUMN_CNT && idx < cell_cnt; ++i, ++idx) {
        if (cells[idx].is_nop_value()) {
          tmp_nop_count++;
        } else if (nullptr != update_idx_array_) {
          if (idx < rowkey_column_cnt_) {
            // rowkey col
          } else if (update_idx < update_idx_array_->count() && idx == update_idx_array_->at(update_idx)) {
            update_idx++;
          } else {
            tmp_nop_count++;
          }
        }
      }
      output_sparse_row[cluster_idx++] = tmp_nop_count >= USE_SPARSE_NOP_CNT_IN_CLUSTER;
      total_nop_count += tmp_nop_count;
    } // end of while

    if ((cell_cnt - total_nop_count) <= ObColClusterInfoMask::MAX_SPARSE_COL_CNT
         && cell_cnt < ObRowHeader::MAX_CLUSTER_COLUMN_CNT) {
      // only few non-nop columns in whole row
      cluster_cnt = 1;
      output_sparse_row[0] = true;
    } else {
      cluster_cnt = cluster_idx;
    }
  }
}

template <typename T>
int ObRowWriter::append_sparse_cell_array(
    const T *cells,
    const int64_t offset_start_pos,
    const int64_t start_idx,
    const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == cells || start_idx < 0 || start_idx > end_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row input argument", K(ret), KP(cells), K(start_idx), K(end_idx));
  } else {
    int64_t column_offset = 0;
    bool is_nop_val = false;
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      is_nop_val = false;
      if (!check_col_exist(i)) { // col should not serialize
        is_nop_val = true;
      } else if (cells[i].is_ext()) { // is nop
        if (OB_UNLIKELY(!cells[i].is_nop_value())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported extend type", K(ret),
              "extend_type", cells[i].get_ext());
        } else {
          is_nop_val = true;
        }
      } else if (FALSE_IT(column_offset = pos_ - offset_start_pos)) { // record offset
      } else if (cells[i].is_null()) {
        special_vals_[column_index_count_] = ObRowHeader::VAL_NULL;
      } else {
        special_vals_[column_index_count_] = cells[i].is_outrow() ? ObRowHeader::VAL_OUTROW : ObRowHeader::VAL_NORMAL;
        if (OB_FAIL(append_column(cells[i]))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            LOG_WARN("row writer fail to append column", K(ret), K(i), K(cells[i]));
          }
          break;
        }
      }
      if (OB_SUCC(ret) && !is_nop_val) {
        column_offset_.set_val(column_index_count_, column_offset);
        column_idx_.set_val(column_index_count_, i - start_idx);
        LOG_DEBUG("append_sparse_cell_array", K(ret), K(column_index_count_), K(column_offset),
            K(i - start_idx));
        column_index_count_++;
      }
    }
  }
  return ret;
}

template <typename T>
int ObRowWriter::append_flat_cell_array(
    const T *cells,
    const int64_t offset_start_pos,
    const int64_t start_idx,
    const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == cells || start_idx < 0 || start_idx > end_idx
       || (start_idx != 0 && end_idx - start_idx > ObRowHeader::CLUSTER_COLUMN_CNT))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row input argument", K(ret), KP(cells), K(start_idx), K(end_idx));
  } else {
    int64_t column_offset = 0;
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      column_offset = pos_ - offset_start_pos; // record offset
      if (!check_col_exist(i)) { // col should not serialize
        special_vals_[column_index_count_] = ObRowHeader::VAL_NOP;
      } else if (cells[i].is_ext()) {
        if (cells[i].is_nop_value()) {
          special_vals_[column_index_count_] = ObRowHeader::VAL_NOP;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported extend type", K(ret), "extend_type", cells[i].get_ext());
        }
      } else if (cells[i].is_null()) {
        special_vals_[column_index_count_] = ObRowHeader::VAL_NULL;
      } else {
        special_vals_[column_index_count_] = cells[i].is_outrow() ? ObRowHeader::VAL_OUTROW : ObRowHeader::VAL_NORMAL;
        if (OB_FAIL(append_column(cells[i]))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            LOG_WARN("row writer fail to append column", K(ret), K(i), K(cells[i]));
          }
          break;
        }
      } 
      if (OB_SUCC(ret)) {
        column_offset_.set_val(column_index_count_++, column_offset);
        LOG_DEBUG("append_flat_cell_array", K(ret), K(column_index_count_), K(column_offset),
            K(i), K(pos_), K(cells[i]),
            "start_pos", column_offset + offset_start_pos,
            "len", pos_ - column_offset - offset_start_pos,
            K(cells[i].is_nop_value()), K(cells[i].is_null()));
      }
    }
  }
  return ret;
}

template <int64_t MAX_CNT>
int ObRowWriter::append_array(
    ObBitArray<MAX_CNT> &bit_array,
    const int64_t count,
    ObColClusterInfoMask::BYTES_LEN &type)
{
  int ret = OB_SUCCESS;
  int64_t bytes = 0;
  if (0 == count) {
    type = ObColClusterInfoMask::BYTES_ZERO;
  } else {
    uint32_t largest_val = bit_array.get_val(count - 1);
    if (OB_FAIL(ObRowWriter::get_uint_byte(largest_val, bytes))) {
      LOG_WARN("fail to get column_index_bytes", K(ret), K(largest_val));
    } else {
      const int64_t copy_size = bytes * count;
      if (pos_ + copy_size > buf_size_) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (FALSE_IT(type = (ObColClusterInfoMask::BYTES_LEN )((bytes >> 1) + 1))) {
      } else {
        MEMCPY(buf_ + pos_, bit_array.get_bit_array_ptr(type), copy_size);
        pos_ += copy_size;
      }
    }
  }
  return ret;
}

int ObRowWriter::append_column(const ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos_ + datum.len_ > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (datum.len_ == sizeof(uint64_t)) {
    ret = append_8_bytes_column(datum); 
  } else {
    MEMCPY(buf_ + pos_, datum.ptr_, datum.len_);
    pos_ += datum.len_;
  }
  return ret;
}

int ObRowWriter::append_8_bytes_column(const ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t bytes = 0;
  uint64_t value = datum.get_uint64();
  if (OB_FAIL(get_uint_byte(value, bytes))) {
    LOG_WARN("failed to get byte size", K(value), K(ret));
  } else if (OB_UNLIKELY(bytes <= 0 || bytes > sizeof(uint64_t))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected byte size", K(ret), K(bytes));
  } else {
    if (bytes == sizeof(uint64_t)) {
      MEMCPY(buf_ + pos_, datum.ptr_, datum.len_);
      pos_ += datum.len_;
    } else if (OB_FAIL(write_uint(value, bytes))) {
      LOG_WARN("failed to write variable length 8 bytes column", K(ret), K(value), K(bytes));
    } else {
      if (OB_UNLIKELY(ObRowHeader::VAL_NORMAL != special_vals_[column_index_count_])) {
        ret = OB_ERR_UNEXPECTED;
        uint32_t print_special_value = special_vals_[column_index_count_];
        LOG_WARN("Only normal column might be encoded ", K(ret), K(print_special_value), K(column_index_count_));
      } else {
        special_vals_[column_index_count_] = ObRowHeader::VAL_ENCODING_NORMAL;
        LOG_DEBUG("ObRowWriter write 8 bytes value ", K(value), K(bytes));
      }
    }
  }
  return ret;
}

int ObRowWriter::append_column(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObStorageDatum datum;
  if (OB_FAIL(datum.from_obj_enhance(obj))) {
    STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(obj));
  } else if (OB_FAIL(append_column(datum))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "Failed to append datum column", K(ret), K(datum));
    }
  }
  return ret;
}

template<class T>
int ObRowWriter::append(const T &value)
{
  int ret = OB_SUCCESS;
  if (pos_ + static_cast<int64_t>(sizeof(T)) > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *(reinterpret_cast<T*>(buf_ + pos_)) = value;
    pos_ += sizeof(T);
  }
  return ret;
}

template<class T>
void ObRowWriter::append_with_no_check(const T &value)
{
  *(reinterpret_cast<T*>(buf_ + pos_)) = value;
  pos_ += sizeof(T);
}

int ObRowWriter::get_uint_byte(const uint64_t uint_value, int64_t &bytes)
{
  int ret = OB_SUCCESS;
  if (uint_value <= 0xFF) {
    bytes = 1;
  } else if (uint_value <= static_cast<uint16_t>(0xFFFF)){
    bytes = 2;
  } else if (uint_value <= static_cast<uint32_t>(0xFFFFFFFF)) {
    bytes = 4;
  } else {
    bytes = 8;
  }
  return ret;
}

int ObRowWriter::write_uint(const uint64_t value, const int64_t bytes)
{
  int ret = OB_SUCCESS;
  switch(bytes) {
    case 1: {
      *(reinterpret_cast<uint8_t *>(buf_ + pos_)) = static_cast<uint8_t> (value);
      break;
    }
    case 2: {
      *(reinterpret_cast<uint16_t *>(buf_ + pos_)) = static_cast<uint16_t> (value);
      break;
    }
    case 4: {
      *(reinterpret_cast<uint32_t *>(buf_ + pos_)) = static_cast<uint32_t> (value);
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("Not supported value", K(ret), K(value));
    }
  }
  if (OB_SUCC(ret)) {
    pos_ += bytes;
  }
  return ret;
}


int ObRowWriter::write_char(
    const ObString &char_value,
    const int64_t max_length)
{
  int ret = OB_SUCCESS;
  const uint32_t char_value_len = char_value.length();
  int64_t need_len = sizeof(uint32_t) + char_value_len; // collation_type + char_len + char_val

  if (OB_UNLIKELY(char_value.length() > max_length)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("varchar value is overflow", K(ret), K(char_value_len));
  } else if (pos_ + need_len > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    append_with_no_check<uint32_t>(char_value_len);
    MEMCPY(buf_ + pos_, char_value.ptr(), char_value_len);
    pos_ += char_value_len;
  }
  return ret;
}

