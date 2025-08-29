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

namespace oceanbase
{
using namespace storage;
using namespace common;

namespace blocksstable
{
template <typename T>
void ObDenseClusterReader<T>::init(const char *buf,
                                   const uint64_t buf_len,
                                   const uint32_t column_cnt,
                                   const uint32_t coverd_column_cnt,
                                   const uint32_t first_column_idx)
{
  coverd_column_cnt_ = coverd_column_cnt;
  first_column_idx_ = first_column_idx;
  bitmap_.init(reinterpret_cast<const uint8_t *>(buf));
  content_ = buf + ObDenseClusterBitmap::calc_bitmap_size(coverd_column_cnt);
  offset_array_ = reinterpret_cast<const T *>(buf + buf_len) - coverd_column_cnt + 1;
}

template <typename T>
OB_INLINE int ObDenseClusterReader<T>::read_column(const uint32_t col_idx,
                                                   ObStorageDatum &datum) const
{
  int ret = OB_SUCCESS;

  const ObFlatBitmapValue value = bitmap_.get_value(col_idx);

  // in dense cluster, nop is unlikely case
  if (OB_UNLIKELY(value.is_nop())) {
    datum.set_nop();
  } else if (value.is_null()) {
    datum.set_null();
  } else {
    const char *start = content_ + (col_idx == 0 ? 0 : offset_array_[col_idx - 1]);
    const char *end = col_idx == coverd_column_cnt_ - 1
                          ? reinterpret_cast<const char *>(offset_array_)
                          : content_ + offset_array_[col_idx];

    if (OB_FAIL(value.is_zip_data() ? datum.read_int(start, end - start)
                                    : datum.from_buf_enhance(start, end - start))) {
      LOG_WARN("Fail to read datum", KR(ret), K(col_idx));
    } else {
      datum.flag_ = value.get_flag();
    }
  }

  return ret;
}

template <typename T>
int ObDenseClusterReader<T>::read_specific_column(const uint32_t idx_in_cluster,
                                                  ObStorageDatum &datum) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(idx_in_cluster >= coverd_column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected idx", KR(ret), K(idx_in_cluster), K(coverd_column_cnt_));
  } else if (OB_FAIL(read_column(idx_in_cluster, datum))) {
    LOG_WARN("Fail to read column", KR(ret));
  }

  return ret;
}

template <typename T>
int ObDenseClusterReader<T>::batch_read(ObStorageDatum *datums, const uint32_t size) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size > coverd_column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to do batch read", KR(ret), K(size), K(coverd_column_cnt_));
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < size; i++) {
      if (OB_FAIL(read_column(i, datums[i]))) {
        LOG_WARN("Fail to read column", KR(ret), K(i));
      }
    }
  }

  return ret;
}

template <typename T>
int ObDenseClusterReader<T>::try_batch_read(ObStorageDatum *datums,
                                            const storage::ObColumnIndexArray &cols_index_array,
                                            const uint32_t array_curr_idx,
                                            const uint32_t array_cnt,
                                            uint32_t &batch_read_cnt,
                                            memtable::ObNopBitMap *nop_bitmap) const
{
  int ret = OB_SUCCESS;

  batch_read_cnt = 0;

  for (uint32_t i = array_curr_idx; OB_SUCC(ret) && i < array_cnt; i++) {
    int64_t col_idx_in_cluster = cols_index_array.at(i) - first_column_idx_;
    if (col_idx_in_cluster >= 0 && col_idx_in_cluster < coverd_column_cnt_) {
      if (nop_bitmap == nullptr || nop_bitmap->test(i)) {
        if (OB_FAIL(read_column(col_idx_in_cluster, datums[batch_read_cnt]))) {
          LOG_WARN("Fail to read column when try batch read",
                   KR(ret),
                   K(col_idx_in_cluster),
                   K(coverd_column_cnt_));
        } else if (nop_bitmap != nullptr && OB_LIKELY(!datums[batch_read_cnt].is_nop())) {
          nop_bitmap->set_false(i);
        }
      }
      batch_read_cnt++;
    } else {
      // the column is not in this cluster, batch read end
      break;
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(batch_read_cnt == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to try batch read", KR(ret));
  }

  return ret;
}

template <typename T>
int ObDenseClusterReader<T>::batch_compare(const ObDatumRowkey &rhs,
                                           const ObStorageDatumUtils &datum_utils,
                                           int &cmp_ret) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rhs.get_datum_cnt() > coverd_column_cnt_
                  || datum_utils.get_rowkey_count() < rhs.get_datum_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to do batch compare", KR(ret), K(coverd_column_cnt_), K(rhs.get_datum_cnt()));
  } else {
    ObStorageDatum datum;

    cmp_ret = 0;
    for (uint32_t i = 0; OB_SUCC(ret) && 0 == cmp_ret && i < rhs.get_datum_cnt(); i++) {
      if (OB_FAIL(read_column(i, datum))) {
        LOG_WARN("Fail to read column", KR(ret), K(i));
      } else if (OB_FAIL(
                     datum_utils.get_cmp_funcs().at(i).compare(datum, rhs.datums_[i], cmp_ret))) {
        LOG_WARN("Fail to compare datums", K(ret), K(i), K(datum), K(rhs.datums_[i]));
      }
    }
  }

  return ret;
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
void ObSparseClusterReader<T, R, EmptyDatumType>::init(const char *buf,
                                                       const uint64_t buf_len,
                                                       const uint32_t column_cnt,
                                                       const uint32_t coverd_column_cnt,
                                                       const uint32_t first_column_idx)
{
  column_cnt_ = column_cnt;
  coverd_column_cnt_ = coverd_column_cnt;
  first_column_idx_ = first_column_idx;
  column_idx_array_ = reinterpret_cast<const ObFlatColumnIDXWithFlag<R> *>(buf);
  content_ = reinterpret_cast<const char *>(column_idx_array_ + column_cnt);
  offset_array_ = reinterpret_cast<const T *>(buf + buf_len) - column_cnt + 1;
  inner_build_cache();
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
OB_INLINE int ObSparseClusterReader<T, R, EmptyDatumType>::read_column_in_array(
    const uint32_t idx,
    ObStorageDatum &datum) const
{
  int ret = OB_SUCCESS;

  if (column_idx_array_[idx].is_nop_or_null()) {
    set_datum_for_highbit_mask(datum);
  } else {
    const char *start = content_ + (idx == 0 ? 0 : offset_array_[idx - 1]);
    const char *end = idx == column_cnt_ - 1 ? reinterpret_cast<const char *>(offset_array_)
                                             : content_ + offset_array_[idx];

    if (OB_FAIL(column_idx_array_[idx].is_zip_data()
                    ? datum.read_int(start, end - start)
                    : datum.from_buf_enhance(start, end - start))) {
      LOG_WARN("Fail to read datum", KR(ret), K(idx));
    } else {
      datum.flag_ = column_idx_array_[idx].get_flag();
    }
  }

  return ret;
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
OB_INLINE int32_t ObSparseClusterReader<T, R, EmptyDatumType>::get_idx_in_array(const uint32_t column_idx) const
{
  int32_t idx_in_array = -1;

  if (std::is_same<R, uint8_t>::value) {
    // the max column idx is less than OB_FLAT_CLUSTER_COLUMN_CNT
    // so if column_idx >= cache size, this column must be not found
    if (column_idx < ARRAYSIZEOF(cache_for_idx_find_)) {
      // we can use cache
      idx_in_array = cache_for_idx_find_[column_idx];
    }
  } else {
    #pragma unroll(4)
    for (uint32_t i = 0; i < column_cnt_; i++) {
      if (column_idx == column_idx_array_[i].get_column_idx()) {
        idx_in_array = i;
        break;
      }
    }
  }

  return idx_in_array;
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
OB_INLINE void ObSparseClusterReader<T, R, EmptyDatumType>::inner_build_cache()
{
  // for this case, column_idx_with_flag in this cluster is using 8 bit to represent
  // that means the max_column_idx is less than OB_FLAT_CLUSTER_COLUMN_CNT
  if (std::is_same<R, uint8_t>::value) {
    // the size of cache array is OB_FLAT_CLUSTER_COLUMN_CNT
    // this memset will optimize to one simd256 or 4 * int64 instruction
    static_assert(sizeof(cache_for_idx_find_) == sizeof(int8_t) * OB_FLAT_CLUSTER_COLUMN_CNT,
                  "size is incorrect");
    MEMSET(cache_for_idx_find_, 0xFF, sizeof(cache_for_idx_find_));

    for (uint32_t i = 0; i < column_cnt_; i++) {
      cache_for_idx_find_[column_idx_array_[i].get_column_idx()] = i;
    }
  }
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
int ObSparseClusterReader<T, R, EmptyDatumType>::read_specific_column(const uint32_t idx_in_cluster,
                                                                      ObStorageDatum &datum) const
{
  int ret = OB_SUCCESS;

  int32_t idx_in_array = -1;
  if (OB_UNLIKELY(idx_in_cluster >= coverd_column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected idx", KR(ret), K(idx_in_cluster), K(column_cnt_));
  } else if (OB_FALSE_IT(idx_in_array = get_idx_in_array(idx_in_cluster))) {
  } else if (idx_in_array == -1) {
    set_datum_for_not_exists(datum);
  } else if (OB_FAIL(read_column_in_array(idx_in_array, datum))) {
    LOG_WARN("Fail to read specific column", KR(ret));
  }

  return ret;
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
int ObSparseClusterReader<T, R, EmptyDatumType>::batch_read(ObStorageDatum *datums,
                                                            const uint32_t size) const
{
  int ret = OB_SUCCESS;

  // step 1. we firstly suppose all datum is not exists
  #pragma unroll(4)
  for (uint32_t idx = 0; idx < size; idx++) {
    set_datum_for_not_exists(datums[idx]);
  }

  // step 2. then, we only read the datums in column id array
  for (uint32_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
    if (column_idx_array_[i].get_column_idx() >= size) {
      break;
    } else if (OB_FAIL(read_column_in_array(i, datums[column_idx_array_[i].get_column_idx()]))) {
      LOG_WARN("Fail to read column in column id array", KR(ret), K(i));
    }
  }

  return ret;
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
int ObSparseClusterReader<T, R, EmptyDatumType>::try_batch_read(
    ObStorageDatum *datums,
    const storage::ObColumnIndexArray &cols_index_array,
    const uint32_t array_curr_idx,
    const uint32_t array_cnt,
    uint32_t &batch_read_cnt,
    memtable::ObNopBitMap *nop_bitmap) const
{
  int ret = OB_SUCCESS;

  batch_read_cnt = 0;

  for (uint32_t i = array_curr_idx; OB_SUCC(ret) && i < array_cnt; i++) {
    int64_t col_idx_in_cluster = cols_index_array.at(i) - first_column_idx_;
    if (col_idx_in_cluster >= 0 && col_idx_in_cluster < coverd_column_cnt_) {
      if (nop_bitmap == nullptr || nop_bitmap->test(i)) {
        // try to find the store_idx in column_idx_array
        int32_t column_array_idx = get_idx_in_array(col_idx_in_cluster);

        // read this column in column array
        if (column_array_idx == -1) {
          // can not find this column in column idx array
          set_datum_for_not_exists(datums[batch_read_cnt]);
          if (nop_bitmap != nullptr && EmptyDatumType != ObFlatEmptyDatumType::Nop) {
            nop_bitmap->set_false(i);
          }
        } else if (OB_FAIL(read_column_in_array(column_array_idx, datums[batch_read_cnt]))) {
          LOG_WARN("Fail to read column when try batch read",
                   KR(ret),
                   K(column_array_idx),
                   K(col_idx_in_cluster));
        } else if (nop_bitmap != nullptr && OB_LIKELY(!datums[batch_read_cnt].is_nop())) {
          nop_bitmap->set_false(i);
        }
      }
      batch_read_cnt++;
    } else {
      // the column is not in this cluster, batch read end
      break;
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(batch_read_cnt == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to try batch read", KR(ret));
  }

  return ret;
}

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
int ObSparseClusterReader<T, R, EmptyDatumType>::batch_compare(
    const ObDatumRowkey &rhs,
    const ObStorageDatumUtils &datum_utils,
    int &cmp_ret) const
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("Rowkey cluster must be dense cluster", KR(ret));
  return ret;
}

int ObRowReaderV1::init(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  // [offset_type] [is_global_sparse] [has_rowkey_cluster (rowkey_cnt > 0)]
  static constexpr InitClusterFunc funcs[3][2][2]
      = {{{
              &ObRowReaderV1::inner_init_cluster<uint8_t, false, false>,
              &ObRowReaderV1::inner_init_cluster<uint8_t, false, true>,
          },
          {
              &ObRowReaderV1::inner_init_cluster<uint8_t, true, false>,
              &ObRowReaderV1::inner_init_cluster<uint8_t, true, true>,
          }},
         {{
              &ObRowReaderV1::inner_init_cluster<uint16_t, false, false>,
              &ObRowReaderV1::inner_init_cluster<uint16_t, false, true>,
          },
          {
              &ObRowReaderV1::inner_init_cluster<uint16_t, true, false>,
              &ObRowReaderV1::inner_init_cluster<uint16_t, true, true>,
          }},
         {{
              &ObRowReaderV1::inner_init_cluster<uint32_t, false, false>,
              &ObRowReaderV1::inner_init_cluster<uint32_t, false, true>,
          },
          {
              &ObRowReaderV1::inner_init_cluster<uint32_t, true, false>,
              &ObRowReaderV1::inner_init_cluster<uint32_t, true, true>,
          }}};

  if (OB_UNLIKELY(nullptr == buf || buf_len < sizeof(ObRowHeader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row reader argument", KR(ret), K(buf), K(buf_len));
  } else {
    buf_ = buf;
    row_header_ = reinterpret_cast<const ObRowHeader *>(buf);

    if (OB_UNLIKELY(row_header_->get_version() != ObRowHeader::ROW_HEADER_VERSION_2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid row header version", KR(ret), KPHEX(buf, buf_len));
    } else {
      column_cnt_ = row_header_->get_column_count();
      rowkey_cnt_ = row_header_->get_rowkey_count();
      cluster_column_cnt_bit_ = row_header_->is_global_sparse() ? sizeof(uint32_t) * 8 : OB_FLAT_CLUSTER_COLUMN_CNT_BIT;
      cluster_cnt_ = transform_to_cluster_idx(column_cnt_ - 1) + 1;
      cluster_offset_ = buf + buf_len - ((cluster_cnt_ - 1) << row_header_->get_offset_type());
      init_cluster_func_ = funcs[row_header_->get_offset_type()][row_header_->is_global_sparse()][rowkey_cnt_ > 0];
    }
  }

  return ret;
}

int ObRowReaderV1::read_row_header(const char *row_buf,
                                   const int64_t row_len,
                                   const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == row_buf || row_len < sizeof(ObRowHeader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row", KR(ret), K(row_buf), K(row_len));
  } else {
    row_header = reinterpret_cast<const ObRowHeader *>(row_buf);
  }

  return ret;
}

int ObRowReaderV1::read_memtable_row(const char *row_buf,
                                     const int64_t row_len,
                                     const ObITableReadInfo &read_info,
                                     ObDatumRow &row,
                                     memtable::ObNopBitMap &nop_bitmap,
                                     bool &read_finished,
                                     const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!read_info.is_valid() || read_info.get_request_count() > row.get_capacity()
                  || read_finished)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to read row", KR(ret), K(read_info), K(row), K(read_finished));
  } else if (OB_FAIL(init(row_buf, row_len))) {
    LOG_WARN("Fail to init row", KR(ret), K(row_len));
  } else {
    row.count_ = read_info.get_request_count();
    const ObColumnIndexArray &cols_index = read_info.get_memtable_columns_index();

    int64_t idx = 0;
    while (OB_SUCC(ret) && idx < row.count_) {
      if (nop_bitmap.test(idx)) { // is_nop, read current cell
        int64_t store_idx = cols_index.at(idx);
        uint32_t batch_read_cnt = 0;
        if (store_idx < 0 || store_idx >= column_cnt_) { // not exists, skip this column
          idx++;
        } else if (OB_FAIL(init_cluster(transform_to_cluster_idx(store_idx)))) {
          LOG_WARN("Fail to init cluster", KR(ret));
        } else if (OB_FAIL(reader_->try_batch_read(row.storage_datums_ + idx,
                                                   cols_index,
                                                   idx,
                                                   row.count_,
                                                   batch_read_cnt,
                                                   &nop_bitmap))) {
          LOG_WARN("Fail to read datum", KR(ret), KPHEX(row_buf, row_len), K(store_idx));
        } else {
          idx += batch_read_cnt;
        }
      } else {
        idx++;
      }
    }

    row_header = row_header_;
  }

  if (OB_SUCC(ret)
      && (nop_bitmap.is_empty() || row_header_->get_row_flag().is_delete()
          || row_header_->get_row_flag().is_insert())) {
    read_finished = true;
  }

  return ret;
}

int ObRowReaderV1::read_row(const char *row_buf,
                            const int64_t row_len,
                            const ObITableReadInfo *read_info,
                            ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  int64_t seq_read_cnt = 0;
  int64_t column_cnt = 0;

  if (OB_UNLIKELY((nullptr != read_info && !read_info->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to read row", KR(ret), KPC(read_info));
  } else if (OB_FAIL(init(row_buf, row_len))) {
    LOG_WARN("Fail to set up row", KR(ret), K(row_len));
  } else if (FALSE_IT(seq_read_cnt
                      = read_info ? read_info->get_seq_read_column_count() : column_cnt_)) {
  } else if (FALSE_IT(column_cnt = read_info ? read_info->get_request_count() : column_cnt_)) {
  } else if (OB_FAIL(row.is_valid() ? row.reserve(column_cnt) : row.init(column_cnt))) {
    LOG_WARN("Fail to reserve datum row", KR(ret), K(column_cnt));
  } else {
    row.row_flag_ = row_header_->get_row_flag();
    row.mvcc_row_flag_.flag_ = row_header_->get_mvcc_row_flag();
    row.trans_id_ = row_header_->get_trans_id();
    row.count_ = column_cnt;

    // sequence read
    uint64_t idx = 0;
    while (OB_SUCC(ret) && idx < seq_read_cnt) {
      if (idx < column_cnt_) {
        uint32_t batch_read_cnt = 0;
        if (OB_FAIL(init_cluster(transform_to_cluster_idx(idx)))) {
          LOG_WARN("Fail to init cluster", KR(ret), K(seq_read_cnt));
        } else if (OB_FALSE_IT(batch_read_cnt = min(reader_->get_coverd_column_cnt(),
                                                    static_cast<uint32_t>(seq_read_cnt - idx)))) {
        } else if (OB_FAIL(reader_->batch_read(row.storage_datums_ + idx, batch_read_cnt))) {
          LOG_WARN("Fail to batch read datum",
                   KR(ret),
                   KPHEX(row_buf, row_len),
                   K(idx),
                   K(batch_read_cnt));
        } else {
          idx += batch_read_cnt;
        }
      } else {
        row.storage_datums_[idx].set_nop();
        idx++;
      }
    }

    if (nullptr != read_info) {
      const ObColumnIndexArray &cols_index = read_info->get_columns_index();
      while (OB_SUCC(ret) && idx < column_cnt) {
        int64_t store_idx = cols_index.at(idx);
        uint32_t batch_read_cnt = 0;
        if (store_idx < 0 || store_idx >= column_cnt_) {
          row.storage_datums_[idx].set_nop();
          idx++;
        } else if (OB_FAIL(init_cluster(transform_to_cluster_idx(store_idx)))) {
          LOG_WARN("Fail to init cluster", KR(ret), K(store_idx));
        } else if (OB_FAIL(reader_->try_batch_read(
                       row.storage_datums_ + idx, cols_index, idx, column_cnt, batch_read_cnt))) {
          LOG_WARN("Fail to read datum", KR(ret), KPHEX(row_buf, row_len), K(idx));
        } else {
          idx += batch_read_cnt;
        }
      }
    }
  }

  return ret;
}

int ObRowReaderV1::read_column(const char *row_buf,
                               const int64_t row_len,
                               const int64_t col_idx,
                               ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(col_idx));
  } else if (OB_FAIL(init(row_buf, row_len))) {
    LOG_WARN("Fail to setup row", KR(ret), K(row_buf), K(row_len));
  } else if (OB_UNLIKELY(col_idx >= column_cnt_)) {
    datum.set_nop();
  } else if (OB_FAIL(init_cluster(transform_to_cluster_idx(col_idx)))) {
    LOG_WARN("Fail to init cluster", KR(ret));
  } else if (OB_FAIL(
                 reader_->read_specific_column(col_idx - reader_->get_first_column_idx(), datum))) {
    LOG_WARN("Fail to read datum", KR(ret), K(col_idx));
  }

  return ret;
}

int ObRowReaderV1::compare_meta_rowkey(const ObDatumRowkey &rhs,
                                       const ObStorageDatumUtils &datum_utils,
                                       const char *buf,
                                       const int64_t row_len,
                                       int32_t &cmp_result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!datum_utils.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(datum_utils), K(rhs), K(buf), K(row_len));
  } else if (OB_FAIL(init(buf, row_len))) {
    LOG_WARN("Fail to init buffer", K(ret), K(buf), K(row_len));
  } else if (OB_FAIL(init_cluster(0))) {
    LOG_WARN("Fail to init cluster", KR(ret));
  } else if (OB_FAIL(reader_->batch_compare(rhs, datum_utils, cmp_result))) {
    LOG_WARN("Failed to compare datums", K(ret), K(rhs));
  }

  return ret;
}


using ReaderFactoryFunc = void *(*)(void *buffer);

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
static OB_INLINE void *create_sparse_cluster_reader(void *buffer)
{
  return new (buffer) ObSparseClusterReader<T, R, EmptyDatumType>();
}

template <typename T> static OB_INLINE void *create_dense_cluster_reader(void *buffer)
{
  return new (buffer) ObDenseClusterReader<T>();
}

static OB_INLINE void *nullptr_reader(void *buffer) { return nullptr; }

template <typename T, bool IsGlobalSparse, bool HasRowkeyCluster>
int ObRowReaderV1::inner_init_cluster(const uint32_t cluster_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_idx >= cluster_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected cluster idx", KR(ret), K(cluster_idx), K(cluster_cnt_));
  } else {
    uint32_t cluster_start = cluster_idx == 0
                                 ? sizeof(ObRowHeader)
                                 : static_cast<const T *>(cluster_offset_)[cluster_idx - 1];
    uint32_t cluster_end = (cluster_idx == cluster_cnt_ - 1)
                               ? static_cast<const char *>(cluster_offset_) - buf_
                               : static_cast<const T *>(cluster_offset_)[cluster_idx];
    ObFlatClusterHeader header
        = *reinterpret_cast<const ObFlatClusterHeader *>(buf_ + cluster_start);

    init_cluster_reader(IsGlobalSparse && (cluster_idx > 0 || !HasRowkeyCluster), header);

    if (OB_ISNULL(reader_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected header", KR(ret), K(header.header_));
    } else {
      uint32_t cluster_first_column_idx
          = cluster_idx > 0
                ? rowkey_cnt_ + ((cluster_idx - HasRowkeyCluster) << OB_FLAT_CLUSTER_COLUMN_CNT_BIT)
                : 0;
      uint32_t coverd_column_cnt = rowkey_cnt_;
      if (!HasRowkeyCluster || cluster_idx > 0) {
        if (IsGlobalSparse) {
          coverd_column_cnt = column_cnt_ - rowkey_cnt_;
        } else {
          coverd_column_cnt = cluster_idx == cluster_cnt_ - 1
                                  ? column_cnt_ - cluster_first_column_idx
                                  : OB_FLAT_CLUSTER_COLUMN_CNT;
        }
      }
      reader_->init(buf_ + cluster_start + 1,
                    cluster_end - cluster_start - 1,
                    header.sparse_column_cnt_,
                    coverd_column_cnt,
                    cluster_first_column_idx);
    }
  }

  return ret;
}

OB_INLINE void ObRowReaderV1::init_cluster_reader(const bool is_global_sparse,
                                                const ObFlatClusterHeader header)
{
  // header -> 0x0000 00    0     0
  //                  off empty sparse

  static constexpr ReaderFactoryFunc func[2][16]
      = {{
             &create_dense_cluster_reader<uint8_t>,
             &create_sparse_cluster_reader<uint8_t, uint8_t, ObFlatEmptyDatumType::Nop>,
             &create_dense_cluster_reader<uint8_t>,
             &create_sparse_cluster_reader<uint8_t, uint8_t, ObFlatEmptyDatumType::Null>,
             &create_dense_cluster_reader<uint16_t>,
             &create_sparse_cluster_reader<uint16_t, uint8_t, ObFlatEmptyDatumType::Nop>,
             &create_dense_cluster_reader<uint16_t>,
             &create_sparse_cluster_reader<uint16_t, uint8_t, ObFlatEmptyDatumType::Null>,
             &create_dense_cluster_reader<uint32_t>,
             &create_sparse_cluster_reader<uint16_t, uint8_t, ObFlatEmptyDatumType::Nop>,
             &create_dense_cluster_reader<uint32_t>,
             &create_sparse_cluster_reader<uint32_t, uint8_t, ObFlatEmptyDatumType::Null>,
             &nullptr_reader,
             &nullptr_reader,
             &nullptr_reader,
             &nullptr_reader,
         },
         // is_global_sparse = true
         {
             &create_sparse_cluster_reader<uint8_t, uint8_t, ObFlatEmptyDatumType::Nop>,
             &create_sparse_cluster_reader<uint8_t, uint16_t, ObFlatEmptyDatumType::Nop>,
             &create_sparse_cluster_reader<uint8_t, uint8_t, ObFlatEmptyDatumType::Null>,
             &create_sparse_cluster_reader<uint8_t, uint16_t, ObFlatEmptyDatumType::Null>,
             &create_sparse_cluster_reader<uint16_t, uint8_t, ObFlatEmptyDatumType::Nop>,
             &create_sparse_cluster_reader<uint16_t, uint16_t, ObFlatEmptyDatumType::Nop>,
             &create_sparse_cluster_reader<uint16_t, uint8_t, ObFlatEmptyDatumType::Null>,
             &create_sparse_cluster_reader<uint16_t, uint16_t, ObFlatEmptyDatumType::Null>,
             &create_sparse_cluster_reader<uint32_t, uint8_t, ObFlatEmptyDatumType::Nop>,
             &create_sparse_cluster_reader<uint32_t, uint16_t, ObFlatEmptyDatumType::Nop>,
             &create_sparse_cluster_reader<uint32_t, uint8_t, ObFlatEmptyDatumType::Null>,
             &create_sparse_cluster_reader<uint32_t, uint16_t, ObFlatEmptyDatumType::Null>,
             &nullptr_reader,
             &nullptr_reader,
             &nullptr_reader,
             &nullptr_reader,
         }};

  reader_ = static_cast<ObClusterReader *>(
      func[is_global_sparse][static_cast<uint8_t>(header.header_) & 0b1111](buffer_));
}

template class ObDenseClusterReader<uint8_t>;
template class ObDenseClusterReader<uint16_t>;
template class ObDenseClusterReader<uint32_t>;
template class ObSparseClusterReader<uint8_t, uint16_t, ObFlatEmptyDatumType::Null>;
template class ObSparseClusterReader<uint16_t, uint16_t, ObFlatEmptyDatumType::Null>;
template class ObSparseClusterReader<uint32_t, uint16_t, ObFlatEmptyDatumType::Null>;
template class ObSparseClusterReader<uint8_t, uint8_t, ObFlatEmptyDatumType::Null>;
template class ObSparseClusterReader<uint16_t, uint8_t, ObFlatEmptyDatumType::Null>;
template class ObSparseClusterReader<uint32_t, uint8_t, ObFlatEmptyDatumType::Null>;
template class ObSparseClusterReader<uint8_t, uint16_t, ObFlatEmptyDatumType::Nop>;
template class ObSparseClusterReader<uint16_t, uint16_t, ObFlatEmptyDatumType::Nop>;
template class ObSparseClusterReader<uint32_t, uint16_t, ObFlatEmptyDatumType::Nop>;
template class ObSparseClusterReader<uint8_t, uint8_t, ObFlatEmptyDatumType::Nop>;
template class ObSparseClusterReader<uint16_t, uint8_t, ObFlatEmptyDatumType::Nop>;
template class ObSparseClusterReader<uint32_t, uint8_t, ObFlatEmptyDatumType::Nop>;

/* ====================== old ob row reader (need remove) ============================ */

#define SET_ROW_BASIC_INFO(row) \
  { \
    row.row_flag_ = row_header_->get_row_flag(); \
    row.mvcc_row_flag_.flag_ = row_header_->get_mvcc_row_flag(); \
    row.trans_id_ = row_header_->get_trans_id(); \
  }

ObRowReaderV0::ObRowReaderV0()
   : buf_(NULL),
     row_len_(0),
     row_header_(NULL),
     cluster_offset_(NULL),
     column_offset_(NULL),
     column_idx_array_(NULL),
     cluster_reader_(),
     cur_read_cluster_idx_(-1),
     cluster_cnt_(0),
     rowkey_independent_cluster_(false),
     is_setuped_(false)
{
}

void ObRowReaderV0::reset()
{
  buf_ = NULL;
  row_len_ = 0;
  row_header_ = NULL;
  cluster_offset_ = NULL;
  column_offset_ = NULL;
  column_idx_array_ = NULL;
  cluster_reader_.reset();
  cur_read_cluster_idx_ = -1;
  cluster_cnt_ = 0;
  rowkey_independent_cluster_ = false;
  is_setuped_ = false;
}

bool ObRowReaderV0::is_valid() const
{
  return is_setuped_ && NULL != row_header_  && row_header_->is_valid() && NULL != buf_ && row_len_ > 0;
}

static uint64_t get_offset_0(const void *offset_array, const int64_t idx)
{ UNUSEDx(offset_array, idx); return INT64_MAX; }
static uint64_t get_offset_8(const void *offset_array, const int64_t idx)
{ return reinterpret_cast<const uint8_t*>(offset_array)[idx]; }
static uint64_t get_offset_16(const void *offset_array, const int64_t idx)
{ return reinterpret_cast<const uint16_t*>(offset_array)[idx]; }
static uint64_t get_offset_32(const void *offset_array, const int64_t idx)
{ return reinterpret_cast<const uint32_t*>(offset_array)[idx]; }

uint64_t (*get_offset_func[ObColClusterInfoMask::BYTES_MAX])(const void *, const int64_t)
    = {get_offset_0, get_offset_8, get_offset_16, get_offset_32};

int ObClusterColumnReader::init(
    const char *cluster_buf,
    const uint64_t cluster_len,
    const uint64_t cluster_col_cnt,
    const ObColClusterInfoMask &info_mask)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    reset();
  }
  int64_t serialize_column_cnt = 0;
  if (OB_UNLIKELY(nullptr == cluster_buf || !info_mask.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cluster_buf), K(cluster_len), K(info_mask));
  } else if (FALSE_IT(serialize_column_cnt = info_mask.is_sparse_row() ? info_mask.get_sparse_column_count() : cluster_col_cnt)) {
  } else if (OB_UNLIKELY(info_mask.get_total_array_size(serialize_column_cnt) >= cluster_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid cluster reader argument", K(ret), K(info_mask), K(cluster_len));
  } else {
    cluster_buf_ = cluster_buf;
    const int64_t specail_val_pos = cluster_len - info_mask.get_special_value_array_size(serialize_column_cnt);
    special_vals_ = reinterpret_cast<const uint8_t*>(cluster_buf_ + specail_val_pos);

    cell_end_pos_ = specail_val_pos - info_mask.get_offset_type_len() * serialize_column_cnt;
    column_cnt_ = cluster_col_cnt;
    column_offset_ = cluster_buf_ + cell_end_pos_;
    offset_bytes_ = info_mask.get_offset_type();
    if (info_mask.is_sparse_row()) {
      is_sparse_row_ = true;
      sparse_column_cnt_ = info_mask.get_sparse_column_count();
      col_idx_bytes_ = info_mask.get_column_idx_type();
      cell_end_pos_ -= info_mask.get_column_idx_type_len() * sparse_column_cnt_;
      column_idx_array_ = cluster_buf_ + cell_end_pos_;
    }
    is_inited_ = true;
    LOG_DEBUG("success to init cluster column reader", K(ret), K(cluster_len),
        KPC(this), K(cell_end_pos_), K(column_cnt_), K(sparse_column_cnt_));
  }
  return ret;
}

void ObClusterColumnReader::reset()
{
  cluster_buf_ = nullptr;
  is_sparse_row_ = false;
  offset_bytes_ = ObColClusterInfoMask::BYTES_MAX;
  col_idx_bytes_ = ObColClusterInfoMask::BYTES_MAX;
  cell_end_pos_ = 0;
  cur_idx_ = 0;
  column_offset_ = nullptr;
  column_idx_array_ = nullptr;
  is_inited_ = false;
}

int64_t ObClusterColumnReader::get_sparse_col_idx(const int64_t column_idx)
{
  int64_t idx = -1;
  int64_t col_idx = 0;
  for (int i = 0; i < sparse_column_cnt_; ++i) {
    col_idx = get_offset_func[col_idx_bytes_](column_idx_array_, i);
    if (col_idx == column_idx) {
      idx = i;
      break;
    } else if (col_idx > column_idx) {
      break;
    }
  }
  return idx;
}

int ObClusterColumnReader::read_storage_datum(const int64_t column_idx, ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cluster column reader is not init", K(ret), K(column_idx));
  } else if (OB_UNLIKELY(column_idx < 0 || column_idx >= column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_idx), K(column_cnt_));
  } else {
    int64_t idx = is_sparse_row_ ? get_sparse_col_idx(column_idx) : column_idx;

    if (-1 == idx) {
      datum.set_nop();
    } else if (idx >= 0 && idx < column_cnt_) {
      if (OB_FAIL(read_datum(idx, datum))) {
        LOG_WARN("read datum fail", K(ret), KP(cluster_buf_), K(cell_end_pos_), K(idx), K(column_idx));
      } else {
        LOG_DEBUG("read_storage_datum", K(idx), K(datum));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid idx for read datum", K(ret), K(column_idx), K(idx), K(datum));
    }
  }
  return ret;
}

int ObClusterColumnReader::sequence_read_datum(const int64_t column_idx, ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cluster column reader is not init", K(ret), K(column_idx));
  } else if (OB_UNLIKELY(column_idx < 0 || column_idx >= column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_idx), K(column_cnt_));
  } else {
    int64_t idx = -1;
    if (is_sparse_row_) {
      if (cur_idx_ < sparse_column_cnt_) {
        if (get_offset_func[col_idx_bytes_](column_idx_array_, cur_idx_) == column_idx) {
          idx = cur_idx_++;
        }
      }
    } else {
      idx = column_idx;
    }
    if (-1 == idx) {
      datum.set_nop();
    } else if (idx >= 0 && idx < column_cnt_) {
      if (OB_FAIL(read_datum(idx, datum))) {
        LOG_WARN("read datum fail", K(ret), KP(cluster_buf_), K(cell_end_pos_), K(idx), K(column_idx));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid idx for sequence read obj", K(column_idx), K(idx), K(datum));
    }
  }
  return ret;
}

int ObClusterColumnReader::sequence_deep_copy_datums_of_sparse(
    const int64_t start_idx,
    ObStorageDatum *datums)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = -1;
  int64_t next_pos = -1;
  int64_t col_idx = 0;
  ObRowHeader::SPECIAL_VAL special_val = ObRowHeader::VAL_MAX;
  for (int i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    if (cur_idx_ < sparse_column_cnt_
        && i == get_offset_func[col_idx_bytes_](column_idx_array_, cur_idx_)) { // have val
      col_idx = start_idx + i;
      special_val = (ObRowHeader::SPECIAL_VAL)read_special_value(cur_idx_);
      if (OB_UNLIKELY(ObRowHeader::VAL_NOP == special_val)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nop val", K(ret), K(i), K(col_idx), K(tmp_pos));
      } else if (ObRowHeader::VAL_NULL == special_val) {
        datums[col_idx].set_null();
      } else if (OB_UNLIKELY(ObRowHeader::VAL_NORMAL != special_val
          && ObRowHeader::VAL_ENCODING_NORMAL != special_val)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected specail val", K(ret), K(i), K(col_idx), K(special_val));
      } else {
        tmp_pos = get_offset_func[offset_bytes_](column_offset_, cur_idx_);
        if (cur_idx_ + 1 < sparse_column_cnt_) {
          next_pos = get_offset_func[offset_bytes_](column_offset_, cur_idx_ + 1);
        } else { // cur cell is the last cell
          next_pos = cell_end_pos_;
        }
        LOG_DEBUG("sequence_deep_copy_datums_of_sparse", K(col_idx), K(tmp_pos), K(next_pos), K(cell_end_pos_));
        if (OB_FAIL(read_column_from_buf(tmp_pos, next_pos, special_val, datums[col_idx]))) {
          LOG_WARN("failed to read column from buf", K(ret), K(tmp_pos), K(next_pos), K(special_val));
        }
      }
      cur_idx_++;
    } else { // set nop
      datums[start_idx + i].set_nop();
    }
  } // end of for
  return ret;
}

int ObClusterColumnReader::sequence_deep_copy_datums_of_dense(const int64_t start_idx, ObStorageDatum *datums)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = -1;
  int64_t next_pos = -1;
  int64_t cur_idx = 0;
  ObRowHeader::SPECIAL_VAL special_val = ObRowHeader::VAL_MAX;
  for (int idx = 0; OB_SUCC(ret) && idx < column_cnt_; ++idx) {
    tmp_pos = get_offset_func[offset_bytes_](column_offset_, idx);
    cur_idx = start_idx + idx;
    special_val = (ObRowHeader::SPECIAL_VAL)read_special_value(idx);
    if (ObRowHeader::VAL_NOP == special_val) {
      datums[cur_idx].set_nop();
    } else if (ObRowHeader::VAL_NULL == special_val) {
      datums[cur_idx].set_null();
    } else if (OB_UNLIKELY(ObRowHeader::VAL_NORMAL != special_val
        && ObRowHeader::VAL_ENCODING_NORMAL != special_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected specail val", K(ret), K(idx), K(cur_idx), K(special_val));
    } else {
      datums[cur_idx].reset();
      if (idx + 1 < column_cnt_) {
        next_pos = get_offset_func[offset_bytes_](column_offset_, idx + 1);
      } else { // cur cell is the last cell
        next_pos = cell_end_pos_;
      }
      LOG_DEBUG("sequence_deep_copy_datums_of_dense", K(cur_idx), K(tmp_pos), K(next_pos));
      if (OB_FAIL(read_column_from_buf(tmp_pos, next_pos, special_val, datums[cur_idx]))) {
        LOG_WARN("failed to read column from buf", K(ret), K(tmp_pos), K(next_pos), K(special_val));
      }
    }
  } // end of for
  return ret;
}

int ObClusterColumnReader::sequence_deep_copy_datums(const int64_t start_idx, ObStorageDatum *datums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cluster column reader is not init", K(ret), K(start_idx));
  } else if (OB_UNLIKELY(start_idx < 0 || nullptr == datums)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_idx), K(datums));
  } else if (is_sparse_row_) {
    if (OB_FAIL(sequence_deep_copy_datums_of_sparse(start_idx, datums))) {
      LOG_WARN("failed to deep copy datums in sparse cluster", K(ret), K(start_idx), K(datums));
    }
  } else if (OB_FAIL(sequence_deep_copy_datums_of_dense(start_idx, datums))) {
    LOG_WARN("failed to deep copy datums in dense cluster", K(ret), K(start_idx), K(datums));
  }
  return ret;
}

int ObClusterColumnReader::read_cell_with_bitmap(
    const int64_t start_idx,
    const ObITableReadInfo &read_info,
    ObDatumRow &datum_row,
    memtable::ObNopBitMap &nop_bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cluster column reader is not init", K(ret));
  } else {
    int64_t cur_idx = 0;
    const ObColumnIndexArray &cols_index = read_info.get_memtable_columns_index();

    for (int i = 0; OB_SUCC(ret) && i < read_info.get_request_count(); ++i) {
      if (!nop_bitmap.test(i)) { // is_nop, read current cell
      } else if (FALSE_IT(cur_idx = cols_index.at(i) - start_idx)) {
      } else if (cur_idx >= 0 && cur_idx < column_cnt_) {
        if (is_sparse_row_) {
          for (int64_t j = 0; OB_SUCC(ret) && j < sparse_column_cnt_; ++j) {
            if (get_offset_func[col_idx_bytes_](column_idx_array_, j) == cur_idx) {
              if (OB_FAIL(read_datum(j, datum_row.storage_datums_[i]))) {
                LOG_WARN("failed to read non nop datum", K(ret), K(j), K(i), K(cur_idx), K(start_idx));
              } else {
                if (datum_row.storage_datums_[i].is_nop()) {
                  LOG_ERROR("chaser debug unexpected nop datum", K(j), K(i), K(cur_idx), K(start_idx), K(*this));
                }
                nop_bitmap.set_false(i);
                break;
              }
            }
          }
          LOG_DEBUG("chaser debug read sparse column", K(i), K(cur_idx), K(start_idx), K(nop_bitmap.get_nop_cnt()));
        } else if (read_special_value(cur_idx) == ObRowHeader::VAL_NOP) {
          // skip nop
        } else if (OB_FAIL(read_datum(cur_idx, datum_row.storage_datums_[i]))) {
          LOG_WARN("failed to read non nop datum", K(ret), K(i), K(cur_idx), K(start_idx), K(is_sparse_row_));
        } else {
          nop_bitmap.set_false(i);
          LOG_DEBUG("chaser debug read dense column", K(i), K(cur_idx), K(start_idx), K(datum_row.storage_datums_[i]), K(nop_bitmap.get_nop_cnt()));
        }
      }
    }
  }
  return ret;
}

int ObClusterColumnReader::read_8_bytes_column(
    const char *buf,
    const int64_t buf_len,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  if (OB_UNLIKELY(buf_len <= 0 || buf_len >= sizeof(uint64_t))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid size of column ", K(ret), KP(buf), K(buf_len));
  } else {
    switch (buf_len) {
    case 1:
      value = reinterpret_cast<const uint8_t *>(buf)[0];
    break;
    case 2:
      value = reinterpret_cast<const uint16_t *>(buf)[0];
    break;
    case 4:
      value = reinterpret_cast<const uint32_t *>(buf)[0];
    break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Not supported buf_len ", KP(buf), K(buf_len), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    datum.reuse();
    datum.set_uint(value);
    LOG_DEBUG("ObClusterColumnReader read 8 bytes column ", K(value));
  }
  return ret;
}

int ObClusterColumnReader::read_column_from_buf(
    const int64_t tmp_pos,
    const int64_t next_pos,
    const ObRowHeader::SPECIAL_VAL special_val,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  const char *buf = cluster_buf_ + tmp_pos;
  const int64_t buf_len = next_pos - tmp_pos;
  if (special_val == ObRowHeader::VAL_ENCODING_NORMAL) {
    if (OB_FAIL(read_8_bytes_column(buf, buf_len, datum))) {
      LOG_WARN("failed to decode 8 bytes column", K(ret), K(special_val), KP(buf), K(buf_len), KPC(this));
    }
  } else if (OB_FAIL(datum.from_buf_enhance(buf, buf_len))) {
    LOG_WARN("failed to copy datum", K(ret), K(special_val), KP(buf), K(buf_len), KPC(this));
  }
  return ret;
}

int ObClusterColumnReader::read_datum(const int64_t column_idx, ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObRowHeader::SPECIAL_VAL special_val = (ObRowHeader::SPECIAL_VAL)read_special_value(column_idx);
  if (ObRowHeader::VAL_NOP == special_val) {
    datum.set_nop();
  } else if (ObRowHeader::VAL_NULL == special_val) {
    datum.set_null();
  } else if (OB_UNLIKELY(ObRowHeader::VAL_NORMAL != special_val
      && ObRowHeader::VAL_ENCODING_NORMAL != special_val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected specail val", K(ret), K(column_idx), K(special_val));
  } else {
    int64_t next_pos = -1;
    int64_t tmp_pos = get_offset_func[offset_bytes_](column_offset_, column_idx);
    if (column_idx + 1 < (is_sparse_row_ ? sparse_column_cnt_ : column_cnt_)) {
      next_pos = get_offset_func[offset_bytes_](column_offset_, column_idx+ 1);
    } else { // cur cell is the last cell
      next_pos = cell_end_pos_;
    }
    if (OB_FAIL(read_column_from_buf(tmp_pos, next_pos, special_val, datum))) {
      LOG_WARN("failed to read column from buf", K(ret), KP(tmp_pos), K(next_pos), K(special_val));
    }
  }
  return ret;
}
/*
 * ObRowReaderV2 implement
 * */
inline int ObRowReaderV0::setup_row(
    const char *buf,
    const int64_t row_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || row_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row reader argument", K(ret), K(OB_P(buf)), K(row_len));
  } else {
    // set all position
    buf_ = buf;
    row_len_ = row_len;
    cur_read_cluster_idx_ = -1;
    if (OB_FAIL(analyze_row_header())) {
      LOG_WARN("invalid row reader argument.", K(ret));
    } else {
      is_setuped_ = true;
    }
  }
  LOG_DEBUG("ObRowReaderV2::setup_row", K(ret), KPC(row_header_), KPC(this));
  return ret;
}

int ObRowReaderV0::read_row_header(
    const char *row_buf,
    const int64_t row_len,
    const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;
  row_header = nullptr;
  if (OB_UNLIKELY(nullptr == row_buf || row_len < sizeof(ObRowHeader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row reader argument", K(ret), K(OB_P(row_buf)), K(row_len));
  } else {
    row_header = reinterpret_cast<const ObRowHeader*>(row_buf); // get NewRowHeader
  }
  return ret;
}

int ObRowReaderV0::read_memtable_row(
    const char *row_buf,
    const int64_t row_len,
    const ObITableReadInfo &read_info,
    ObDatumRow &datum_row,
    memtable::ObNopBitMap &nop_bitmap,
    bool &read_finished,
    const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid() || read_info.get_request_count() > datum_row.get_capacity() || read_finished)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to read row", K(ret), K(read_info), K(datum_row), K(read_finished));
  } else if (OB_FAIL(setup_row(row_buf, row_len))) {
    LOG_WARN("Fail to set up row", K(ret), K(row_len));
  } else {
    datum_row.count_ = read_info.get_request_count();
    int64_t store_idx = 0;
    const ObColumnIndexArray &cols_index = read_info.get_memtable_columns_index();
    for (int i = 0; OB_SUCC(ret) && i < read_info.get_request_count(); ++i) {
      if (!nop_bitmap.test(i)) { // is_nop, read current cell
      } else  {
        store_idx = cols_index.at(i);
        if (store_idx < 0 || store_idx >= row_header_->get_column_count()) { // not exists
        } else if (OB_FAIL(read_specific_column_in_cluster(store_idx, datum_row.storage_datums_[i]))) {
          LOG_WARN("failed to read datum from cluster column reader", K(ret), KPC(row_header_), K(store_idx));
        } else if (!datum_row.storage_datums_[i].is_nop()) {
          nop_bitmap.set_false(i);
        }
      }
    }
    row_header = row_header_;
  }

  if (OB_FAIL(ret)) {
  } else if (nop_bitmap.is_empty()
      || row_header_->get_row_flag().is_delete()
      || row_header_->get_row_flag().is_insert()) {
    read_finished = true;
  }
  LOG_DEBUG("chaser debug read memtable row", K(nop_bitmap.get_nop_cnt()), KPC(row_header_), K(datum_row));
  return ret;
}

/*
 * Row with cluster : ObRowHeader | Column Cluster | Cluster Offset Array
 *       Column Cluster: ClusterInfoMask | Column Array | Column Offset Array
 * Row without cluster : ObRowHeader | Column Array | Column Offset Array
 * */
OB_INLINE int ObRowReaderV0::analyze_row_header()
{
  int ret = OB_SUCCESS;
  row_header_ = reinterpret_cast<const ObRowHeader*>(buf_); // get NewRowHeader
  if (OB_UNLIKELY(!row_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row header is invalid", K(ret), K(row_len_), KPC(row_header_));
  } else if (OB_FAIL(analyze_cluster_info())) {
    LOG_WARN("failed to analyze cluster info", K(ret), KPC(row_header_));
  } else if (row_header_->get_version() != ObRowHeader::ROW_HEADER_VERSION_1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid version for row reader", K(ret), KPC(row_header_));
  }

  return ret;
}

int ObRowReaderV0::analyze_cluster_info()
{
  int ret = OB_SUCCESS;
  rowkey_independent_cluster_ = row_header_->has_rowkey_independent_cluster();
  cluster_cnt_ = row_header_->get_cluster_cnt();
  const int64_t cluster_offset_len = row_header_->get_offset_type_len() * cluster_cnt_;
  if (OB_UNLIKELY(ObRowHeader::get_serialized_size() + cluster_offset_len >= row_len_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid row reader argument", K(ret), K(row_len_), KPC(row_header_));
    row_header_ = NULL;
  } else {
    cluster_offset_ = buf_ + row_len_ - cluster_offset_len;
    LOG_DEBUG("analyze cluster info", K(ret), K(cluster_offset_),
        K(cluster_offset_len), K(cluster_cnt_),
        K(row_len_), KPC(row_header_));
  }
  return ret;
}

uint64_t ObRowReaderV0::get_cluster_offset(const int64_t cluster_idx) const
{
  return cluster_idx == 0 ? sizeof(ObRowHeader) :
             get_offset_func[row_header_->get_offset_type_v0()](cluster_offset_, cluster_idx);
}

uint64_t ObRowReaderV0::get_cluster_end_pos(const int64_t cluster_idx) const
{
  return cluster_cnt_ - 1 == cluster_idx ? row_len_ - row_header_->get_offset_type_len() * cluster_cnt_:
          get_offset_func[row_header_->get_offset_type_v0()](cluster_offset_, cluster_idx + 1);
}

int ObRowReaderV0::analyze_info_and_init_reader(const int64_t cluster_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_idx < 0 || cluster_idx > cluster_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(row_header_), K(cluster_idx));
  } else if (cur_read_cluster_idx_ != cluster_idx) { // need init another ClusterReader
    cluster_reader_.reset();
    const uint64_t cluster_start_pos = get_cluster_offset(cluster_idx);
    const ObColClusterInfoMask *info_mask = reinterpret_cast<const ObColClusterInfoMask*>(buf_ + cluster_start_pos);
    if (OB_FAIL(cluster_reader_.init(
            buf_ + cluster_start_pos,
            get_cluster_end_pos(cluster_idx) - cluster_start_pos, // cluster len
            row_header_->is_single_cluster() ? row_header_->get_column_count() : info_mask->get_column_count(),
            *info_mask))) {
      LOG_WARN("failed to init cluster column reader", K(ret), KPC(row_header_), K(cluster_idx));
    } else {
      // only rowkey independent cluster have columns more than CLUSTER_COLUMN_CNT, but it can't be sparse
      cur_read_cluster_idx_ = cluster_idx;
      LOG_DEBUG("success to init cluster reader", K(cluster_idx), KPC(info_mask), KPC(row_header_),
          K(cluster_start_pos), K(get_cluster_end_pos(cluster_idx)));
    }
  }
  return ret;
}

int ObRowReaderV0::read_row(
    const char *row_buf,
    const int64_t row_len,
    const ObITableReadInfo *read_info,
    ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  // set position and get row header
  if (OB_UNLIKELY((nullptr != read_info && !read_info->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to read row", K(ret), KPC(read_info));
  } else if (OB_FAIL(setup_row(row_buf, row_len))) {
    LOG_WARN("Fail to set up row", K(ret), K(row_len));
  } else {
    int64_t seq_read_cnt = 0;
    int64_t column_cnt = 0;
    if (nullptr == read_info) {
      seq_read_cnt = column_cnt = row_header_->get_column_count();
    } else {
      seq_read_cnt = read_info->get_seq_read_column_count();
      column_cnt = read_info->get_request_count();
    }
    if (datum_row.is_valid()) {
      if (OB_FAIL(datum_row.reserve(column_cnt))) {
        STORAGE_LOG(WARN, "Failed to reserve datum row", K(ret), K(column_cnt));
      }
    } else if (OB_FAIL(datum_row.init(column_cnt))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(column_cnt));
    }
    if (OB_SUCC(ret)) {
      SET_ROW_BASIC_INFO(datum_row);  // set flag/row_type_flag/trans_id
      datum_row.count_ = column_cnt;
      // sequence read
      int64_t idx = 0;
      if (seq_read_cnt > 0) {
        int64_t cluster_col_cnt = 0;
      for (int64_t cluster_idx = 0; OB_SUCC(ret) && cluster_idx < row_header_->get_cluster_cnt() && idx < seq_read_cnt; ++cluster_idx) {
        if (OB_FAIL(analyze_info_and_init_reader(cluster_idx))) {
          LOG_WARN("failed to init cluster column reader", K(ret), KPC(row_header_), K(cluster_idx));
        } else {
          cluster_col_cnt = cluster_reader_.get_column_count();
          for (int64_t i = 0; OB_SUCC(ret) && i < cluster_col_cnt && idx < seq_read_cnt; ++idx, ++i) {
            if (idx >= datum_row.count_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("idx is invalid", K(i), K(idx), K(seq_read_cnt), K(column_cnt), KPC(read_info));
            } else if (i >= row_header_->get_column_count()) { // not exists
              datum_row.storage_datums_[i].set_nop();
            } else if (OB_FAIL(cluster_reader_.sequence_read_datum(i, datum_row.storage_datums_[idx]))) {
              LOG_WARN("Fail to read column", K(ret), K(idx));
            } else {
              LOG_DEBUG("sequence read datum", K(ret), K(idx), K(i), K(datum_row.storage_datums_[idx]),
                  K(cluster_col_cnt));
            }
          }
        }
      } // end of for
    }

    if (nullptr != read_info) {
      int64_t store_idx = 0;
      const ObColumnIndexArray &cols_index = read_info->get_columns_index();
      for (; OB_SUCC(ret) && idx < read_info->get_request_count(); ++idx) { // loop the ColumnIndex array
        store_idx = cols_index.at(idx);
        if (store_idx < 0 || store_idx >= row_header_->get_column_count()) { // not exists
          datum_row.storage_datums_[idx].set_nop();
        } else if (OB_FAIL(read_specific_column_in_cluster(store_idx, datum_row.storage_datums_[idx]))) {
          LOG_WARN("failed to read datum from cluster column reader", K(ret), KPC(row_header_), K(store_idx));
        }
      } // end of for
    }
    }
  }
  reset();
  return ret;
}

int ObRowReaderV0::read_column(
    const char *row_buf,
    const int64_t row_len,
    const int64_t col_idx,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_row(row_buf, row_len))) {
    LOG_WARN("failed to setup row", K(ret), K(row_buf), K(row_len));
  } else if (OB_UNLIKELY(col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_idx));
  } else if (OB_UNLIKELY(col_idx >= row_header_->get_column_count())) {
    datum.set_nop();
  } else if (OB_FAIL(read_specific_column_in_cluster(col_idx, datum))) {
    LOG_WARN("failed to read obj from cluster column reader", K(ret), KPC(row_header_), K(col_idx));
  }
  return ret;
}

int ObRowReaderV0::read_specific_column_in_cluster(
    const int64_t store_idx,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t cluster_idx = 0;
  int64_t col_idx_in_cluster = 0;
  if (row_header_->is_single_cluster()) {
    cluster_idx = 0;
    col_idx_in_cluster = store_idx;
  } else if (!rowkey_independent_cluster_) {
    cluster_idx = ObRowHeader::calc_cluster_idx(store_idx);
    col_idx_in_cluster = ObRowHeader::calc_column_idx_in_cluster(store_idx);
  } else if (store_idx < row_header_->get_rowkey_count()) { // rowkey independent
    cluster_idx = 0;
    col_idx_in_cluster = store_idx;
  } else {
    int64_t idx = store_idx - row_header_->get_rowkey_count();
    cluster_idx = ObRowHeader::calc_cluster_idx(idx) + 1;
    col_idx_in_cluster = ObRowHeader::calc_column_idx_in_cluster(idx);
  }
  LOG_DEBUG("DEBUG cluster reader", K(cluster_idx), K(col_idx_in_cluster), K(cur_read_cluster_idx_));
  if (OB_FAIL(analyze_info_and_init_reader(cluster_idx))) {
    LOG_WARN("failed to init cluster column reader", K(ret), KPC(row_header_),
        K(cluster_idx), K(col_idx_in_cluster));
  } else if (OB_FAIL(cluster_reader_.read_storage_datum(col_idx_in_cluster, datum))) {
    LOG_WARN("failed to read datum from cluster column reader", K(ret), KPC(row_header_),
        K(cluster_idx), K(col_idx_in_cluster));
  }
  return ret;
}

int ObRowReaderV0::read_char(
    const char* buf,
    int64_t end_pos,
    int64_t &pos,
    ObString &value)
{
  int ret = OB_SUCCESS;
  const uint32_t *len = read<uint32_t>(buf, pos);
  if (OB_UNLIKELY(pos + *len > end_pos)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not large", K(ret), K(pos), K(*len), K(end_pos));
  } else {
    value.assign_ptr((char*) (buf + pos), *len);
    pos += *len;
  }
  return ret;
}

// called by ObIMicroBlockFlatReader::find_bound_::PreciseCompare
int ObRowReaderV0::compare_meta_rowkey(
    const ObDatumRowkey &rhs,
    const ObStorageDatumUtils &datum_utils,
    const char *buf,
    const int64_t row_len,
    int32_t &cmp_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rhs.is_valid() || !datum_utils.is_valid() || nullptr == buf || row_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row header argument.", K(ret), K(datum_utils),
             K(rhs), K(buf), K(row_len));
  } else {
    cmp_result = 0;
    const int64_t compare_column_count = rhs.get_datum_cnt();
    if (OB_UNLIKELY(datum_utils.get_rowkey_count() < compare_column_count)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument to compare meta rowkey", K(ret), K(compare_column_count), K(rhs), K(datum_utils));
    } else if (OB_FAIL(setup_row(buf, row_len))) {
      LOG_WARN("row reader fail to setup.", K(ret), K(OB_P(buf)), K(row_len));
    } else if (OB_UNLIKELY(row_header_->get_rowkey_count() < compare_column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected rowkey count", K(ret), K(compare_column_count), K(rhs), KPC(row_header_));
    } else {
      ObStorageDatum datum;
      int64_t cluster_col_cnt = 0;
      int64_t idx = 0;
      for (int64_t cluster_idx = 0;
          OB_SUCC(ret) && cmp_result == 0 && cluster_idx < row_header_->get_cluster_cnt() && idx < compare_column_count;
          ++cluster_idx) {
        if (OB_FAIL(analyze_info_and_init_reader(cluster_idx))) {
          LOG_WARN("failed to init cluster column reader", K(ret), KPC(row_header_), K(cluster_idx));
        } else {
          cluster_col_cnt = cluster_reader_.get_column_count();
          for (int64_t i = 0;
              OB_SUCC(ret) && cmp_result == 0 && i < cluster_col_cnt && idx < compare_column_count;
              ++idx, ++i) {
            if (OB_FAIL(cluster_reader_.sequence_read_datum(i, datum))) {
              LOG_WARN("Fail to read column", K(ret), K(i), K(idx), K(datum_utils));
            } else if (OB_FAIL(datum_utils.get_cmp_funcs().at(idx).compare(datum, rhs.datums_[idx], cmp_result))) {
              STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(idx), K(datum), K(rhs.datums_[idx]));
            }
            LOG_DEBUG("chaser debug compare rowkey", K(datum), K(idx), K(datum), K(rhs.datums_[idx]));
          }
        }
      }
    }
  }
  reset();
  return ret;
}

int ObRowReaderV0::dump_row(
    const char *row_buf,
    const int64_t buf_len,
    FILE* fd)
{
  UNUSEDx(row_buf, buf_len, fd);
  return OB_NOT_SUPPORTED;
}

} // end namespace blocksstable
} // end namespace oceanbase
