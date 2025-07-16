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
#include "ob_row_reader.h"
#include "ob_row_writer.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace blocksstable;

static OB_INLINE bool is_sorted_ascend(const ObIArray<int64_t> &array)
{
  bool sorted_ret = true;
  for (int i = 1; i < array.count(); i++) {
    if (array.at(i - 1) >= array.at(i)) {
      sorted_ret = false;
      break;
    }
  }
  return sorted_ret;
}

ObRowWriter::ObRowWriter()
    : buf_(nullptr), buf_size_(0), pos_(0), row_header_(nullptr), col_descs_(nullptr),
      data_estimate_len_(0), cluster_meta_len_(0), cluster_cnt_(0), rowkey_local_buffer_(),
      rowkey_buffer_(rowkey_local_buffer_), rowkey_buffer_size_(ARRAYSIZEOF(rowkey_local_buffer_)),
      row_buffer_()
{
  STATIC_ASSERT(sizeof(ObRowWriter) < 6 * 1024, "row writer stack size can not be very large");
}

ObRowWriter::~ObRowWriter()
{
  if (rowkey_buffer_ != rowkey_local_buffer_ && rowkey_buffer_ != nullptr) {
    common::ob_free(rowkey_buffer_);
  }
}

template <typename R>
void ObRowWriter::inner_append_offset_array(const Offset *offset_array,
                                            const ColumnCnt array_length)
{
  R *target = reinterpret_cast<R *>(buf_ + pos_);

#pragma unroll(4)
  for (ColumnIdx i = 0; i < array_length; i++) {
    target[i] = static_cast<R>(offset_array[i]);
  }

  pos_ += sizeof(R) * array_length;
}

template <>
void ObRowWriter::inner_append_offset_array<uint32_t>(const Offset *offset_array,
                                                      const ColumnCnt array_length)
{
  static_assert(sizeof(uint32_t) == sizeof(Offset), "Mismatch length");
  memcpy(buf_ + pos_, offset_array, sizeof(uint32_t) * array_length);
  pos_ += sizeof(uint32_t) * array_length;
}

OB_INLINE void ObRowWriter::append_column(const ObStorageDatum &datum,
                                          const ColumnIdx datum_idx,
                                          bool &is_zip,
                                          uint8_t &flag)
{
  uint32_t len = is_int_type(datum_idx)
                     ? ObIntSizeHelper::byte_size(ObIntSizeHelper::from_int(datum.get_uint64()))
                     : datum.len_;
  is_zip = len != datum.len_;

  switch (len) {
  case 8:
    *reinterpret_cast<uint64_t *>(buf_ + pos_) = *reinterpret_cast<const uint64_t *>(datum.ptr_);
    break;
  case 4:
    *reinterpret_cast<uint32_t *>(buf_ + pos_) = *reinterpret_cast<const uint32_t *>(datum.ptr_);
    break;
  case 2:
    *reinterpret_cast<uint16_t *>(buf_ + pos_) = *reinterpret_cast<const uint16_t *>(datum.ptr_);
    break;
  case 1:
    *reinterpret_cast<uint8_t *>(buf_ + pos_) = *reinterpret_cast<const uint8_t *>(datum.ptr_);
    break;
  default:
    MEMCPY(buf_ + pos_, datum.ptr_, len);
    break;
  };

  pos_ += len;

  // TODO: encoding does't store flag information, so that decoding will make flag = 0.
  //       if flat format store flag, it will make checksum not equal when do compaction
  // flag = datum.flag_;
  flag = 0;
}

OB_INLINE int ObRowWriter::append_offset_array(const Offset *offset_array,
                                               const ColumnCnt array_length,
                                               ObIntSize &offset_element_type)
{
  int ret = OB_SUCCESS;

  // we only append (array_length - 1) element because the val of last offset can be calc
  if (array_length > 1) {
    offset_element_type = ObIntSizeHelper::from_int(offset_array[array_length - 2]);
    switch (offset_element_type) {
    case ObIntSize::Int8:
      inner_append_offset_array<uint8_t>(offset_array, array_length - 1);
      break;
    case ObIntSize::Int16:
      inner_append_offset_array<uint16_t>(offset_array, array_length - 1);
      break;
    case ObIntSize::Int32:
      inner_append_offset_array<uint32_t>(offset_array, array_length - 1);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to append offset array", KR(ret), K(offset_element_type));
      break;
    };
  } else {
    offset_element_type = ObIntSize::Int8;
  }

  return ret;
}

OB_INLINE int ObRowWriter::check_param_validity(const ObDatumRow &row,
                                                const ObIArray<int64_t> *update_array,
                                                const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey_cnt < 0 || rowkey_cnt > row.get_column_count() || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid datum row", KR(ret), K(rowkey_cnt), K(row));
  } else if (OB_NOT_NULL(update_array) && update_array->count() >= 1
             && (!is_sorted_ascend(*update_array)
                 || (update_array->at(0) < rowkey_cnt
                     || update_array->at(update_array->count() - 1) >= row.get_column_count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid update array", KR(ret), K(update_array));
  }

  return ret;
}

OB_INLINE int ObRowWriter::init_writer_param(char *buf,
                                             const int64_t buf_size,
                                             const ObIArray<ObColDesc> *col_descs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf || buf_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row writer input argument", KR(ret), K(buf), K(buf_size));
  } else {
    buf_ = reinterpret_cast<uint8_t *>(buf);
    buf_size_ = buf_size;
    pos_ = 0;
    col_descs_ = col_descs;
  }

  return ret;
}

int ObRowWriter::append_row_header(const uint8_t row_flag,
                                   const uint8_t multi_version_flag,
                                   const int64_t trans_id,
                                   const ColumnCnt column_cnt,
                                   const RowkeyCnt rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(buf_size_ < sizeof(ObRowHeader))) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    row_header_ = reinterpret_cast<ObRowHeader *>(buf_);
    row_header_->set_version(ObRowHeader::ROW_HEADER_VERSION_2);
    row_header_->set_row_flag(row_flag);
    row_header_->set_row_mvcc_flag(multi_version_flag);
    row_header_->set_rowkey_count(rowkey_cnt);
    row_header_->set_column_count(column_cnt);
    row_header_->clear_header_mask();
    row_header_->clear_reserved_bits();
    row_header_->set_trans_id(trans_id);
    pos_ += sizeof(ObRowHeader);
  }

  return ret;
}

int ObRowWriter::inner_write_row_without_update_for_less_column(const ObStorageDatum *datums,
                                                                const ColumnCnt column_cnt,
                                                                const RowkeyCnt rowkey_cnt)
{
  int ret = OB_SUCCESS;

  // (header + offset) * (rowkey_cluster + normal_cluster = 2)
  constexpr uint32_t max_cluster_meta_len = (sizeof(ObFlatClusterHeader) + sizeof(uint32_t)) << 1;

  // bitmap + offset
  uint32_t all_meta_len
      = (ObDenseClusterBitmap::calc_bitmap_size(column_cnt) << 1) + (column_cnt << 2);

  // data len
  uint32_t data_len = 0;
  #pragma unroll(4)
  for (ColumnIdx i = 0; i < column_cnt; i++) {
    data_len += datums[i].len_;
  }

  // step 1. check memory enough
  if (pos_ + max_cluster_meta_len + all_meta_len + data_len > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    // step 2. build rowkey cluster
    ClusterIdx cluster_idx = 0;

    cluster_headers_[0].set_dense();
    cluster_headers_[1].set_dense();

    if (rowkey_cnt > 0
        && OB_FAIL(build_dense_cluster<false>(datums, 0, rowkey_cnt, cluster_idx++))) {
      LOG_WARN("Fail to build rowkey cluster", KR(ret));
    } else if (column_cnt - rowkey_cnt > 0
               && OB_FAIL(
                      build_dense_cluster<false>(datums, rowkey_cnt, column_cnt, cluster_idx++))) {
      LOG_WARN("Fail to build normal cluster", KR(ret));
    }

    cluster_cnt_ = cluster_idx;
  }

  return ret;
}

template <bool HAS_UPDATE_ARRAY>
int ObRowWriter::inner_write_row(const ObStorageDatum *datums,
                                 const ColumnCnt column_cnt,
                                 const RowkeyCnt rowkey_cnt,
                                 const ObIArray<int64_t> *update_array)

{
  int ret = OB_SUCCESS;

  ClusterIdx cluster_idx = 0;
  ObIntSize offset_element_type = ObIntSize::Int8;

  if (!HAS_UPDATE_ARRAY && column_cnt - rowkey_cnt <= OB_FLAT_CLUSTER_COLUMN_CNT
      && ObDmlRowFlag(row_header_->get_row_flag()).is_insert()) {
    // In most cases, insert rows are dense rows, and performing analyze_cluster on them is to
    // distinguish null sparse rows. However, for scenarios with fewer columns, the overhead of
    // related analysis and dynamic calls can be reduced by defaulting to building dense rows.
    if (OB_FAIL(inner_write_row_without_update_for_less_column(datums, column_cnt, rowkey_cnt))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("Fail to use fast path build row for less column", KR(ret));
      }
    }
  } else if (OB_FAIL(
                 analyze_cluster<HAS_UPDATE_ARRAY>(datums, column_cnt, rowkey_cnt, update_array))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("Fail to analyze cluster", KR(ret), K(datums));
    }
  } else if (rowkey_cnt > 0
             && OB_FAIL(build_dense_cluster<false>(datums, 0, rowkey_cnt, cluster_idx++))) {
    LOG_WARN("Fail to build rowkey cluster", KR(ret));
  } else if (row_header_->is_global_sparse()) {
    if (OB_FAIL(
            build_cluster<HAS_UPDATE_ARRAY>(datums,
                                            rowkey_cnt,
                                            column_cnt,
                                            cluster_idx++,
                                            HAS_UPDATE_ARRAY ? update_array->get_data() : nullptr,
                                            HAS_UPDATE_ARRAY ? update_array->count() : 0))) {
      LOG_WARN("Fail to build global sparse cluster", KR(ret));
    }
  } else {
    ColumnIdx start = rowkey_cnt;
    ColumnIdx update_array_start = 0;
    while (OB_SUCC(ret) && cluster_idx < cluster_cnt_) {
      ColumnIdx end = min(static_cast<ColumnIdx>(start + OB_FLAT_CLUSTER_COLUMN_CNT), column_cnt);
      ColumnIdx update_array_end = update_array_start;

      if (HAS_UPDATE_ARRAY) { // TODO: use if constexpr
        while (update_array_end < update_array->count()
               && update_array->at(update_array_end) < end) {
          update_array_end++;
        }
      }

      if (OB_FAIL(build_cluster<HAS_UPDATE_ARRAY>(
              datums,
              start,
              end,
              cluster_idx++,
              HAS_UPDATE_ARRAY ? update_array->get_data() + update_array_start : nullptr,
              HAS_UPDATE_ARRAY ? update_array_end - update_array_start : 0))) {
        LOG_WARN("Fail to build normal cluster", KR(ret));
      } else {
        start = end;
        update_array_start = update_array_end;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_offset_array(cluster_offset_, cluster_cnt_, offset_element_type))) {
    LOG_WARN("Fail to append offset array", KR(ret));
  } else {
    row_header_->set_offset_type(static_cast<uint8_t>(offset_element_type));
  }

  return ret;
}

int ObRowWriter::write(const int64_t rowkey_cnt,
                       const ObDatumRow &row,
                       const ObIArray<int64_t> *update_array,
                       const ObIArray<ObColDesc> *col_descs,
                       char *buf,
                       const int64_t buf_size,
                       int64_t &writed_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_param_validity(row, update_array, rowkey_cnt))) {
    LOG_WARN("Invalid param", KR(ret), K(row), KPC(update_array), K(rowkey_cnt));
  } else if (OB_FAIL(init_writer_param(buf, buf_size, col_descs))) {
    LOG_WARN("Fail to init writer buffer", KR(ret), K(buf), K(buf_size));
  } else if (OB_FAIL(append_row_header(row.row_flag_.get_serialize_flag(),
                                       row.mvcc_row_flag_.flag_,
                                       row.trans_id_.get_id(),
                                       row.count_,
                                       rowkey_cnt))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("Fail to write row header", KR(ret), K(row));
    }
  } else if (OB_FAIL(update_array != nullptr
                         ? inner_write_row<true>(
                               row.storage_datums_, row.count_, rowkey_cnt, update_array)
                         : inner_write_row<false>(
                               row.storage_datums_, row.count_, rowkey_cnt, update_array))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("Fail to write row", KR(ret), K(row));
    }
  } else {
    writed_len = pos_;
  }

  return ret;
}

OB_INLINE int ObRowWriter::extend_rowkey_buf()
{
  int ret = OB_SUCCESS;

  if (rowkey_buffer_size_ >= OB_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("Fail to extend rowkey buf", KR(ret), K(rowkey_buffer_size_));
  } else if (OB_ISNULL(rowkey_buffer_ = reinterpret_cast<ObStorageDatum *>(common::ob_malloc(
                           sizeof(ObStorageDatum) * OB_MAX_ROWKEY_COLUMN_NUMBER,
                           ObMemAttr(MTL_ID(), "ObRowkeyBuffer"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for row buffer", KR(ret));
  } else {
    rowkey_buffer_size_ = OB_MAX_ROWKEY_COLUMN_NUMBER;
  }

  return ret;
}

OB_INLINE int ObRowWriter::transform_rowkey_to_datums(const ObStoreRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(rowkey.get_obj_cnt() <= rowkey_buffer_size_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); i++) {
      if (OB_FAIL(rowkey_buffer_[i].from_obj_enhance(rowkey.get_obj_ptr()[i]))) {
        LOG_WARN("Fail to convert obj to dautm", KR(ret));
      }
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
  }

  return ret;
}

int ObRowWriter::write_lock_rowkey(const common::ObStoreRowkey &rowkey,
                                   const ObIArray<ObColDesc> *col_descs,
                                   char *&buf,
                                   int64_t &len)
{
  int ret = OB_SUCCESS;

  do {
    if (ret == OB_BUF_NOT_ENOUGH && OB_FAIL(extend_rowkey_buf())) {
      LOG_WARN("Fail to extend rowkey buffer", KR(ret));
    } else if (OB_FAIL(transform_rowkey_to_datums(rowkey))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("Fail to transfrom rowkey to datum", KR(ret));
      }
    }
  } while (OB_BUF_NOT_ENOUGH == ret && rowkey_buffer_size_ < OB_MAX_ROWKEY_COLUMN_NUMBER);

  if (OB_SUCC(ret)) {
    do {
      if (ret == OB_BUF_NOT_ENOUGH && OB_FAIL(row_buffer_.extend_buf())) {
        LOG_WARN("Fail to extend buffer", KR(ret));
      } else if (OB_FAIL(init_writer_param(row_buffer_.get_buf(),
                                           row_buffer_.get_buf_size(),
                                           col_descs))) {
        LOG_WARN("Fail to set row writer buffer", KR(ret), K(row_buffer_));
      } else if (OB_FAIL(append_row_header(ObDmlFlag::DF_LOCK,
                                           0,
                                           0,
                                           rowkey.get_obj_cnt(),
                                           rowkey.get_obj_cnt()))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("Fail to write row header", KR(ret), K(rowkey));
        }
      } else if (OB_FAIL(inner_write_row<false>(
                     rowkey_buffer_, rowkey.get_obj_cnt(), rowkey.get_obj_cnt(), nullptr))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("Fail to write row", KR(ret), K(rowkey));
        }
      } else {
        buf = row_buffer_.get_buf();
        len = pos_;
      }
    } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());
  }

  if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH == ret)) {
    LOG_WARN("Fail to append row due to buffer not enough", KR(ret), K(rowkey));
  }

  return ret;
}

int ObRowWriter::write(const int64_t rowkey_column_count,
                       const ObDatumRow &row,
                       const ObIArray<int64_t> *update_array,
                       const ObIArray<ObColDesc> *col_descs,
                       char *&buf,
                       int64_t &len)
{
  int ret = OB_SUCCESS;

  do {
    if (OB_BUF_NOT_ENOUGH == ret && OB_FAIL(row_buffer_.extend_buf())) {
      LOG_WARN("Fail to extend buffer", KR(ret));
    } else if (OB_FAIL(write(rowkey_column_count,
                             row,
                             update_array,
                             col_descs,
                             row_buffer_.get_buf(),
                             row_buffer_.get_buf_size(),
                             len))) {
    } else {
      buf = row_buffer_.get_buf();
    }
  } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

  if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH == ret)) {
    LOG_WARN("Fail to append row due to buffer not enough", KR(ret), K(row));
  }

  return ret;
}

template <bool HAS_UPDATE_ARRAY>
int ObRowWriter::build_dense_cluster(const ObStorageDatum *datums,
                                     const ColumnIdx start,
                                     const ColumnIdx end,
                                     const ClusterIdx cluster_idx,
                                     const int64_t *update_array,
                                     const ColumnCnt update_array_cnt)
{
  int ret = OB_SUCCESS;

  ObIntSize offset_element_type;
  ObDenseClusterBitmap bitmap;
  ObFlatClusterHeader *header = reinterpret_cast<ObFlatClusterHeader *>(buf_ + pos_);
  uint32_t bitmap_size = ObDenseClusterBitmap::calc_bitmap_size(end - start);

  // step 1. write header & bitmap
  *header = cluster_headers_[cluster_idx];
  bitmap.init_with_clear(buf_ + pos_ + sizeof(ObFlatClusterHeader), bitmap_size);
  pos_ += sizeof(ObFlatClusterHeader) + bitmap_size;

  // step 2. write data
  bool is_zip = false;
  uint8_t flag = 0;
  ColumnIdx update_array_idx = 0;
  ColumnIdx write_idx = 0;
  Position content_start_pos = pos_;
  for (ColumnIdx i = start; i < end; i++) {
    bool is_nop = false;
    if (HAS_UPDATE_ARRAY) { // TODO: use if constexpr
      if (update_array_idx < update_array_cnt && update_array[update_array_idx] == i) {
        update_array_idx++;
      } else {
        // i is not in update_array, it's nop
        is_nop = true;
      }
    } else {
      is_nop = datums[i].is_nop();
    }

    if (OB_UNLIKELY(is_nop)) {
      bitmap.set_value(write_idx, ObFlatBitmapValue::nop());
    } else if (datums[i].is_null()) {
      bitmap.set_value(write_idx, ObFlatBitmapValue::null());
    } else {
      append_column(datums[i], i, is_zip, flag);
      bitmap.set_value(write_idx,
                       is_zip ? ObFlatBitmapValue::zip_data() : ObFlatBitmapValue::data(flag));
    }
    offset_array_[write_idx++] = pos_ - content_start_pos;
  }

  // step 3. write offset array
  if (OB_FAIL(append_offset_array(offset_array_, write_idx, offset_element_type))) {
    LOG_WARN("Fail to append offset array", KR(ret));
  } else {
    header->offset_size_ = offset_element_type;
  }

  // step 4. write cluster offset, don't need to check ret is succ
  cluster_offset_[cluster_idx] = pos_;

  return ret;
}

template <bool HAS_UPDATE_ARRAY, typename R, ObFlatEmptyDatumType EmptyDatumType>
int ObRowWriter::build_sparse_cluster(const ObStorageDatum *datums,
                                      const ColumnIdx start,
                                      const ColumnIdx end,
                                      const ClusterIdx cluster_idx,
                                      const int64_t *update_array,
                                      const ColumnCnt update_array_cnt)
{
  int ret = OB_SUCCESS;

  ObIntSize offset_element_type;
  ObFlatClusterHeader *header = reinterpret_cast<ObFlatClusterHeader *>(buf_ + pos_);
  ColumnCnt column_idx_array_size = cluster_headers_[cluster_idx].sparse_column_cnt_;
  ObFlatColumnIDXWithFlag<R> *column_idx_array
      = reinterpret_cast<ObFlatColumnIDXWithFlag<R> *>(buf_ + pos_ + sizeof(ObFlatClusterHeader));

  // step 1. write header
  *header = cluster_headers_[cluster_idx];
  pos_ += sizeof(ObFlatClusterHeader) + sizeof(R) * column_idx_array_size;

  // step 2. write data
  bool is_zip = false;
  uint8_t flag = 0;
  Position content_start_pos = pos_;
  ColumnIdx write_idx = 0;
  ColumnIdx update_array_idx = 0;

  bool iterated_by_update_array = HAS_UPDATE_ARRAY && EmptyDatumType == ObFlatEmptyDatumType::Nop;
  ColumnIdx for_start = iterated_by_update_array ? 0 : start;
  ColumnIdx for_end = iterated_by_update_array ? update_array_cnt : end;

  for (ColumnIdx i = for_start; i < for_end && write_idx < column_idx_array_size; i++) {
    const ColumnIdx idx_in_row = iterated_by_update_array ? update_array[i] : i;
    const ColumnIdx idx_in_cluster = idx_in_row - start;

    bool is_nop = false;
    if (iterated_by_update_array) {
      // iterated by update array, is_nop must be false
    } else if (HAS_UPDATE_ARRAY) { // TODO: use if constexpr
      if (update_array_idx < update_array_cnt && update_array[update_array_idx] == idx_in_row) {
        update_array_idx++;
      } else {
        // i is not in update_array, it's nop
        is_nop = true;
      }
    } else {
      is_nop = datums[idx_in_row].is_nop();
    }

    if (is_nop) {
      if (EmptyDatumType
          != ObFlatEmptyDatumType::Nop) { // TODO: use if constexpr
        column_idx_array[write_idx].set_nop_or_null_value(idx_in_cluster);
        offset_array_[write_idx++] = pos_ - content_start_pos;
      }
    } else if (datums[idx_in_row].is_null()) {
      if (EmptyDatumType
          != ObFlatEmptyDatumType::Null) { // TODO: use if constexpr
        column_idx_array[write_idx].set_nop_or_null_value(idx_in_cluster);
        offset_array_[write_idx++] = pos_ - content_start_pos;
      }
    } else {
      append_column(datums[idx_in_row], idx_in_row, is_zip, flag);
      column_idx_array[write_idx].set_value(idx_in_cluster, is_zip, flag);
      offset_array_[write_idx++] = pos_ - content_start_pos;
    }
  }

  // step 3. write offset array
  if (OB_FAIL(append_offset_array(offset_array_, write_idx, offset_element_type))) {
    LOG_WARN("Fail to append offset array", KR(ret));
  } else {
    header->offset_size_ = offset_element_type;
  }

  // step 4. write cluster offset, don't need to check ret is succ
  cluster_offset_[cluster_idx] = pos_;

  return ret;
}

OB_INLINE void ObRowWriter::analyze_rowkey_cluster(const ObStorageDatum *datums,
                                                   const RowkeyCnt rowkey_cnt)
{
  if (OB_UNLIKELY(rowkey_cnt == 0)) {
    // empty rowkey cluster, skip it
  } else {
    // always build dense cluster for rowkey
    cluster_headers_[cluster_cnt_].set_dense();
    cluster_meta_len_ += cluster_headers_[cluster_cnt_].get_dense_meta_len(rowkey_cnt);
    cluster_cnt_++;
    for (ColumnIdx i = 0; i < rowkey_cnt; i++) {
      data_estimate_len_ += datums[i].len_;
    }
  }
}

OB_INLINE void ObRowWriter::analyze_normal_cluster(const ColumnCnt column_cnt,
                                                   const ColumnCnt nop_cnt,
                                                   const ColumnCnt null_cnt)
{
  bool nop_sparse = column_cnt - nop_cnt <= OB_FLAT_SPARSE_CLUSTER_COLUMN_LIMIT
                    && nop_cnt >= 2 * (column_cnt - nop_cnt);
  bool null_sparse = column_cnt - null_cnt <= OB_FLAT_SPARSE_CLUSTER_COLUMN_LIMIT
                     && null_cnt >= 2 * (column_cnt - null_cnt);

  if (column_cnt == 0) {
    // an empty cluster, skip it
  } else if (nop_sparse || null_sparse) {
    // sprase cluster
    cluster_headers_[cluster_cnt_].set_sparse(
        null_sparse ? ObFlatEmptyDatumType::Null : ObFlatEmptyDatumType::Nop,
        null_sparse ? column_cnt - null_cnt : column_cnt - nop_cnt);
    cluster_meta_len_ += cluster_headers_[cluster_cnt_].get_sparse_meta_len();
    cluster_cnt_++;
  } else {
    // dense cluster
    cluster_headers_[cluster_cnt_].set_dense();
    cluster_meta_len_ += cluster_headers_[cluster_cnt_].get_dense_meta_len(column_cnt);
    cluster_cnt_++;
  }
}

OB_INLINE void ObRowWriter::analyze_global_sparse_cluster(const ColumnCnt column_cnt,
                                                          const ColumnCnt nop_cnt,
                                                          const ColumnCnt null_cnt,
                                                          const ColumnCnt row_max_value_idx,
                                                          const ColumnCnt row_max_nop_idx,
                                                          const ColumnCnt row_max_null_idx)
{
  bool nop_sparse = column_cnt - nop_cnt <= OB_FLAT_SPARSE_CLUSTER_COLUMN_LIMIT
                    && nop_cnt >= 2 * (column_cnt - nop_cnt);
  bool null_sparse = column_cnt - null_cnt <= OB_FLAT_SPARSE_CLUSTER_COLUMN_LIMIT
                     && null_cnt >= 2 * (column_cnt - null_cnt);

  if (column_cnt == 0) {
    // an empty cluster, skip it
  } else if (nop_sparse || null_sparse) {
    // global sparse, remove all normal cluster
    cluster_cnt_ = row_header_->get_rowkey_count() > 0;
    cluster_headers_[cluster_cnt_].set_global_sparse(
        null_sparse ? max(row_max_value_idx, row_max_nop_idx)
                    : max(row_max_value_idx, row_max_null_idx),
        null_sparse ? ObFlatEmptyDatumType::Null : ObFlatEmptyDatumType::Nop,
        null_sparse ? column_cnt - null_cnt : column_cnt - nop_cnt);
    cluster_meta_len_ += cluster_headers_[cluster_cnt_].get_global_sparse_meta_len();
    cluster_cnt_++;
    row_header_->set_global_sparse(true);
  } else {
    // don't build global sparse
    row_header_->set_global_sparse(false);
  }
}

template <bool HAS_UPDATE_ARRAY>
int ObRowWriter::analyze_cluster(const ObStorageDatum *datums,
                                 const ColumnCnt column_cnt,
                                 const RowkeyCnt rowkey_cnt,
                                 const ObIArray<int64_t> *update_array)
{
  int ret = OB_SUCCESS;

  // clear basic meta data
  cluster_cnt_ = cluster_meta_len_ = data_estimate_len_ = 0;

  // row stat information
  ColumnCnt row_nop_cnt = 0, last_nop_idx = 0, row_null_cnt = 0, last_null_idx = 0,
            last_value_idx = 0;

  // step 1. analyze rowkey cluster
  analyze_rowkey_cluster(datums, rowkey_cnt);

  // step 2. analyze normal cluster
  if (HAS_UPDATE_ARRAY && update_array->empty()) {
    // fast path for empty update array when row delete
    row_nop_cnt = column_cnt - rowkey_cnt;
    last_nop_idx = column_cnt - 1;
  } else {
    ColumnIdx update_array_idx = 0;
    for (ColumnIdx start = rowkey_cnt; start < column_cnt; start += OB_FLAT_CLUSTER_COLUMN_CNT) {
      ColumnIdx end = min(static_cast<ColumnIdx>(start + OB_FLAT_CLUSTER_COLUMN_CNT), column_cnt);
      ColumnCnt nop_cnt = 0;
      ColumnCnt null_cnt = 0;

      if (HAS_UPDATE_ARRAY) { // TODO: use if constexpr
        // the size of update_array is generally much smaller than the length of the row.
        // we should ONLY iterated the update array instead of all row
        nop_cnt = end - start;
        while (update_array_idx < update_array->count()
               && update_array->at(update_array_idx) < end) {
          ColumnIdx update_idx = update_array->at(update_array_idx);
          if (datums[update_idx].is_nop()) {
          } else if (datums[update_idx].is_null()) {
            nop_cnt--;
            null_cnt++;
            last_null_idx = update_idx;
          } else {
            nop_cnt--;
            last_value_idx = update_idx;
            data_estimate_len_ += datums[update_idx].len_;
          }
          update_array_idx++;
        }
        last_nop_idx = max(last_nop_idx, column_cnt - 1);
      } else {
        // we should iterated all row
        for (ColumnIdx i = start; i < end; i++) {
          if (datums[i].is_nop()) {
            nop_cnt++;
            last_nop_idx = i;
          } else if (datums[i].is_null()) {
            null_cnt++;
            last_null_idx = i;
          } else {
            last_value_idx = i;
            data_estimate_len_ += datums[i].len_;
          }
        }
      }

      analyze_normal_cluster(end - start, nop_cnt, null_cnt);

      row_nop_cnt += nop_cnt;
      row_null_cnt += null_cnt;
    }
  }

  // step 3. analyze global spare cluster
  analyze_global_sparse_cluster(column_cnt - rowkey_cnt,
                                row_nop_cnt,
                                row_null_cnt,
                                last_value_idx,
                                last_nop_idx,
                                last_null_idx);

  // step 4. check buffer length
  cluster_meta_len_ += cluster_cnt_ * sizeof(ObFlatClusterHeader) + (cluster_cnt_ << 2);
  if (pos_ + data_estimate_len_ + cluster_meta_len_ > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  }

  return ret;
}

int ObRowWriter::check_write_result(const ObDatumRow &row,
                                    const int64_t rowkey_cnt,
                                    const ObIArray<int64_t> *update_array,
                                    const ObIArray<ObColDesc> *col_descs,
                                    const char *buf,
                                    const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  ObRowReaderV1 reader;
  ObDatumRow read_row;
  if (OB_FAIL(reader.read_row(buf, buf_len, nullptr, read_row))) {
    LOG_WARN("Fail to read row",
             KR(ret),
             KPC(update_array),
             K(row),
             K(rowkey_cnt),
             KPHEX(buf, buf_len));
  } else if (OB_UNLIKELY(row.get_column_count() != read_row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result is not match",
             KR(ret),
             KPC(update_array),
             K(row),
             K(read_row),
             KPHEX(buf, buf_len));
  } else if (update_array) {
    // check rowkey + update idx array
    int update_array_idx = 0;
    for (int i = 0; OB_SUCC(ret) && i < row.get_column_count(); i++) {
      bool should_be_nop = true;
      if (update_array_idx < update_array->count() && update_array->at(update_array_idx) == i) {
        update_array_idx++;
        should_be_nop = false;
      } else if (i < rowkey_cnt) {
        should_be_nop = false;
      }

      if (!should_be_nop && !(row.storage_datums_[i] == read_row.storage_datums_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Result is not match",
                 KR(ret),
                 K(rowkey_cnt),
                 KPC(update_array),
                 K(i),
                 K(row),
                 K(read_row),
                 KPHEX(buf, buf_len));
      } else if (should_be_nop && !read_row.storage_datums_[i].is_nop()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Result Row Idx should be nop",
                 KR(ret),
                 K(rowkey_cnt),
                 KPC(update_array),
                 K(i),
                 K(row),
                 K(read_row),
                 KPHEX(buf, buf_len));
      }
    }
  } else {
    // check the full row
    for (int i = 0; OB_SUCC(ret) && i < row.get_column_count(); i++) {
      if (!(row.storage_datums_[i] == read_row.storage_datums_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Result is not match",
                 KR(ret),
                 K(i),
                 K(row),
                 K(read_row),
                 K(rowkey_cnt),
                 KPC(col_descs),
                 KPHEX(buf, buf_len));
      }
    }
  }

  return ret;
}

ObRowWriterV0::ObRowWriterV0()
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
  STATIC_ASSERT(sizeof(ObRowWriterV0) < 6100, "row buffer size");
}

ObRowWriterV0::~ObRowWriterV0()
{
  reset();
}

void ObRowWriterV0::reset()
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

int ObRowWriterV0::init_common(char *buf, const int64_t buf_size, const int64_t pos)
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

int ObRowWriterV0::check_row_valid(
    const ObDatumRow &row,
    const int64_t rowkey_column_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row writer input argument", K(ret), K(row));
  } else if (OB_UNLIKELY(rowkey_column_count <= 0 || rowkey_column_count > row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row writer input argument", K(ret), K(rowkey_column_count), K(row.count_));
  }
  return ret;
}

int ObRowWriterV0::append_row_header(
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
    //LOG_WARN("buf is not enough", K(ret), K(pos_), K(buf_size_), K(row_header_size));
  } else {
    row_header_ = reinterpret_cast<ObRowHeader*>(buf_ + pos_);
    row_header_->set_version(ObRowHeader::ROW_HEADER_VERSION_1);
    row_header_->set_row_flag(row_flag);
    row_header_->set_row_mvcc_flag(multi_version_flag);
    row_header_->set_column_count(column_cnt);
    row_header_->set_rowkey_count(rowkey_cnt);
    row_header_->clear_header_mask();
    row_header_->clear_reserved_bits();
    row_header_->set_trans_id(trans_id); // TransID

    pos_ += row_header_size; // move forward
  }
  return ret;
}

int ObRowWriterV0::write(
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
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to append row header", K(ret), K(row), K(buf_size_), K(pos_));
    }
  } else {
    update_idx_array_ = nullptr;
    rowkey_column_cnt_ = rowkey_column_count;
    if (OB_FAIL(inner_write_cells(row.storage_datums_, row.count_))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write datums", K(ret), K(row));
      }
    } else {
      LOG_DEBUG("write row", K(ret), K(pos_), K(row));
      pos = pos_;
    }
  }
  return ret;
}

int ObRowWriterV0::alloc_buf_and_init(const bool retry)
{
  int ret = OB_SUCCESS;
  if (retry && OB_FAIL(row_buffer_.extend_buf())) {
    STORAGE_LOG(WARN, "Failed to extend buffer", K(ret), K(row_buffer_));
  } else if (OB_FAIL(init_common(row_buffer_.get_buf(), row_buffer_.get_buf_size(), 0))) {
    LOG_WARN("row writer fail to init common", K(ret), K(row_buffer_));
  }
  return ret;
}

int ObRowWriterV0::write_lock_rowkey(const common::ObStoreRowkey &rowkey, char *&buf, int64_t &len)
{
  int ret = OB_SUCCESS;
  len = 0;
  do {
    if (OB_FAIL(alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) {
      LOG_WARN("row writer fail to alloc and init", K(ret));
    } else if (OB_FAIL(append_row_header(ObDmlFlag::DF_LOCK, 0, 0, rowkey.get_obj_cnt(), rowkey.get_obj_cnt()))) {
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

int ObRowWriterV0::write(const int64_t rowkey_column_cnt, const ObDatumRow &datum_row, char *&buf, int64_t &len)
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

// when update_idx == nullptr, write full row; else only write rowkey + update storage_datums
int ObRowWriterV0::write(
    const int64_t rowkey_column_count,
    const ObDatumRow &datum_row,
    const ObIArray<int64_t> *update_idx,
    char *&buf,
    int64_t &len)
{
  int ret = OB_SUCCESS;
  len = 0;
  if (OB_UNLIKELY(nullptr != update_idx && update_idx->count() > datum_row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update idx is invalid", K(ret), KPC(update_idx), K_(datum_row.count), K(rowkey_column_count));
  } else {
    do {
      if (OB_FAIL(alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) {
        LOG_WARN("row writer fail to alloc and init", K(ret));
      } else if (OB_FAIL(inner_write_row(rowkey_column_count, datum_row, update_idx))) {
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

int ObRowWriterV0::check_update_idx_array_valid(
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

int ObRowWriterV0::inner_write_row(
    const int64_t rowkey_column_count,
    const ObDatumRow &datum_row,
    const ObIArray<int64_t> *update_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_row_valid(datum_row, rowkey_column_count))) {
    LOG_WARN("row writer fail to init store row", K(ret), K(rowkey_column_count));
  } else if (nullptr != update_idx && OB_FAIL(check_update_idx_array_valid(rowkey_column_count, update_idx))) {
    LOG_WARN("invalid update idx array", K(ret));
  } else if (OB_FAIL(append_row_header(
          datum_row.row_flag_.get_serialize_flag(),
          datum_row.mvcc_row_flag_.flag_,
          datum_row.trans_id_.get_id(),
          datum_row.count_,
          rowkey_column_count))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to append row header", K(ret), K(datum_row));
    }
  } else {
    update_idx_array_ = update_idx;
    rowkey_column_cnt_ = rowkey_column_count;
    if (OB_FAIL(inner_write_cells(datum_row.storage_datums_, datum_row.count_))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write cells", K(ret), K(datum_row));
      }
    }
  }
  return ret;
}

template <typename T>
int ObRowWriterV0::inner_write_cells(
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
int ObRowWriterV0::append_row_and_index(
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

int ObRowWriterV0::append_special_val_array(const int64_t cell_count)
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

bool ObRowWriterV0::check_col_exist(const int64_t col_idx)
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
int ObRowWriterV0::write_col_in_cluster(
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
int ObRowWriterV0::build_cluster(const int64_t col_cnt, const T *cells)
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
void ObRowWriterV0::loop_cells(
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
int ObRowWriterV0::append_sparse_cell_array(
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
int ObRowWriterV0::append_flat_cell_array(
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
int ObRowWriterV0::append_array(
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
    if (OB_FAIL(ObRowWriterV0::get_uint_byte(largest_val, bytes))) {
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

int ObRowWriterV0::append_column(const ObStorageDatum &datum)
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

int ObRowWriterV0::append_8_bytes_column(const ObStorageDatum &datum)
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
        LOG_DEBUG("ObRowWriterV0 write 8 bytes value ", K(value), K(bytes));
      }
    }
  }
  return ret;
}

int ObRowWriterV0::append_column(const ObObj &obj)
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
int ObRowWriterV0::append(const T &value)
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
void ObRowWriterV0::append_with_no_check(const T &value)
{
  *(reinterpret_cast<T*>(buf_ + pos_)) = value;
  pos_ += sizeof(T);
}

int ObRowWriterV0::get_uint_byte(const uint64_t uint_value, int64_t &bytes)
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

int ObRowWriterV0::write_uint(const uint64_t value, const int64_t bytes)
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


int ObRowWriterV0::write_char(
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
