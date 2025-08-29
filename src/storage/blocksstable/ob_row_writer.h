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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_WRITER_H_

#include "common/object/ob_obj_type.h"
#include "ob_flat_format.h"
#include "share/ob_define.h"
#include "storage/ob_i_store.h"
#include "storage/ob_micro_block_format_version_helper.h"

namespace oceanbase
{
namespace blocksstable
{

class ObRowWriterV0
{
public:
  ObRowWriterV0();
  virtual ~ObRowWriterV0();
  int write(
      const int64_t rowkey_column_count,
      const ObDatumRow &row,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  int write(const int64_t rowkey_column_cnt, const ObDatumRow &datum_row, char *&buf, int64_t &len);
  int write_lock_rowkey(const common::ObStoreRowkey &rowkey, char *&buf, int64_t &len);
  int write(
      const int64_t rowkey_cnt,
      const ObDatumRow &datum_row,
      const ObIArray<int64_t> *update_idx,
      char *&buf,
      int64_t &len);

  OB_INLINE int write(const int64_t rowkey_cnt,
                      const ObDatumRow &datum_row,
                      const ObIArray<int64_t> *,
                      const ObIArray<ObColDesc> *,
                      char *buf,
                      const int64_t buf_len,
                      int64_t &pos)
  {
    // for compact interface
    return write(rowkey_cnt, datum_row, buf, buf_len, pos);
  }

  void reset();
private:
  template <int64_t MAX_CNT>
  struct ObBitArray
  {
    ObBitArray()
    {
      bit_array_ptrs_[0] = nullptr;
      bit_array_ptrs_[1] = reinterpret_cast<char *> (bit_array_byte1_);
      bit_array_ptrs_[2] = reinterpret_cast<char *> (bit_array_byte2_);
      bit_array_ptrs_[3] = reinterpret_cast<char *> (bit_array_byte4_);
    }
    OB_INLINE void set_val(const int64_t idx, const int64_t offset)
    {
      bit_array_byte1_[idx] = static_cast<uint8_t>(offset);
      bit_array_byte2_[idx] = static_cast<uint16_t>(offset);
      bit_array_byte4_[idx] = static_cast<uint32_t>(offset);
    }
    OB_INLINE uint32_t get_val(const int64_t idx)
    {
      OB_ASSERT(idx < MAX_CNT);
      return bit_array_byte4_[idx];
    }
    OB_INLINE char *get_bit_array_ptr(const ObColClusterInfoMask::BYTES_LEN type)
    {
      OB_ASSERT(type > 0 && type < 4);
      return reinterpret_cast<char *> (bit_array_ptrs_[type]);
    }
    uint32_t bit_array_byte4_[MAX_CNT];
    uint16_t bit_array_byte2_[MAX_CNT];
    uint8_t bit_array_byte1_[MAX_CNT];
    char *bit_array_ptrs_[4];
  };
  int inner_write_row(
      const int64_t rowkey_column_count,
      const ObDatumRow &row,
      const ObIArray<int64_t> *update_idx);
  OB_INLINE int write_oracle_timestamp(const common::ObOTimestampData &ot_data, const common::ObOTimestampMetaAttrType otmat);
  int append_column(const common::ObObj &obj);
  int append_column(const ObStorageDatum &datum);
  int append_8_bytes_column(const ObStorageDatum &datum);
  int init_common(char *buf, const int64_t buf_size, const int64_t pos);
  int check_row_valid(
      const ObDatumRow &row,
      const int64_t rowkey_column_count);
  int append_row_header(
      const uint8_t row_flag,
      const uint8_t multi_version_flag,
      const int64_t trans_id,
      const int64_t column_cnt,
      const int64_t rowkey_cnt);
  template <typename T>
  int inner_write_cells(
      const T *cells,
      const int64_t cell_cnt);
  template <typename T>
  int append_flat_cell_array(
      const T *cells,
      const int64_t offset_start_pos,
      const int64_t start_idx,
      const int64_t end_idx);
  template <typename T>
  int append_sparse_cell_array(
      const T *cells,
      const int64_t offset_start_pos,
      const int64_t start_idx,
      const int64_t end_idx);
  template<typename T, typename R>
  int append_row_and_index(
      const T *cells,
      const int64_t offset_start_pos,
      const int64_t start_idx,
      const int64_t end_idx,
      const bool is_spase_row,
      R &bytes_info);
  template<typename T>
  int build_cluster(
      const int64_t cell_cnt,
      const T *cells);
  template<typename T>
  int write_col_in_cluster(
      const T *cells,
      const int64_t cluster_idx,
      const int64_t start_col_idx,
      const int64_t end_col_idx);

  template <int64_t MAX_CNT>
  OB_INLINE int append_array(
      ObBitArray<MAX_CNT> &bit_array,
      const int64_t count,
      ObColClusterInfoMask::BYTES_LEN &offset_type);
  template<class T>
  OB_INLINE int append(const T &value);
  template<class T>
  void append_with_no_check(const T &value);
  OB_INLINE int write_uint(const uint64_t value, const int64_t bytes);
  OB_INLINE int write_number(const common::number::ObNumber &number);
  OB_INLINE int write_char(
      const ObString &char_value,
      const int64_t max_length);
  static int get_uint_byte(const uint64_t uint_value, int64_t &bytes);
  int alloc_buf_and_init(const bool retry = false);
  // if return false: cell in [col_idx] is NOP
  OB_INLINE bool check_col_exist(const int64_t col_idx);
  OB_INLINE int append_special_val_array(const int64_t cell_count);
  TO_STRING_KV(KP_(buf), K_(buf_size), K_(start_pos), K_(pos), K_(column_index_count), K_(rowkey_column_cnt),
      K_(cluster_cnt), KPC_(row_header), K_(update_array_idx), KPC_(update_idx_array), K_(cluster_cnt));

private:
  static const int64_t USE_SPARSE_NOP_CNT_IN_CLUSTER = ObRowHeader::CLUSTER_COLUMN_CNT - ObColClusterInfoMask::MAX_SPARSE_COL_CNT;  // 29
  static const int64_t MAX_CLUSTER_CNT = common::OB_ROW_MAX_COLUMNS_COUNT / ObRowHeader::CLUSTER_COLUMN_CNT + 1;
  static const int64_t MAX_COLUMN_COUNT_IN_CLUSTER = ObRowHeader::USE_CLUSTER_COLUMN_COUNT; // independent rowkey cluster + less than a cluster column cnt
  template <typename T>
  void loop_cells(
      const T *cells,
      const int64_t col_cnt,
      int64_t &cluster_cnt,
      bool *output_sparse_row);
  int check_update_idx_array_valid(
      const int64_t rowkey_column_count,
      const ObIArray<int64_t> *update_idx);

private:
  char *buf_;
  int64_t buf_size_;
  int64_t start_pos_;
  int64_t pos_;
  ObRowHeader *row_header_;
  int64_t column_index_count_;
  int64_t rowkey_column_cnt_;
  const ObIArray<int64_t> *update_idx_array_;
  int64_t update_array_idx_;
  int64_t cluster_cnt_;
  ObRowBuffer row_buffer_; //4k buffer
  bool use_sparse_row_[MAX_CLUSTER_CNT];
  ObBitArray<MAX_COLUMN_COUNT_IN_CLUSTER> column_idx_; // for sparse row
  ObBitArray<MAX_CLUSTER_CNT> cluster_offset_;
  ObBitArray<MAX_COLUMN_COUNT_IN_CLUSTER> column_offset_;
  uint8_t special_vals_[MAX_COLUMN_COUNT_IN_CLUSTER];
  DISALLOW_COPY_AND_ASSIGN(ObRowWriterV0);
};

class ObRowWriter
{
public:
  // avoid using int64_t because of unsigned int can be optimized when it do mutiple or mod
  using RowkeyCnt = uint32_t;
  using ColumnCnt = uint32_t;
  using ColumnIdx = uint32_t;
  using Position = uint64_t;
  using ClusterCnt = uint32_t;
  using ClusterIdx = uint32_t;
  using Offset = uint32_t;

public:
  ObRowWriter();

  virtual ~ObRowWriter();

  /**
   * @brief write a row into a specified buffer with a size limit
   *
   * @param rowkey_cnt
   * @param row
   * @param update_array An array specifying the indices of columns to be update, any other column
   *                     will be consider as nop (except rowkey column)
   * @param col_descs    A schema used for zip int type column.
   *                     If it's nullptr, no column will be zip.
   *                     If some column has not corresponding schema, it won't be zip.
   * @param buf
   * @param buf_size
   * @param writed_len   The actual number of bytes written to buffer
   * @return int
   */
  int write(const int64_t rowkey_cnt,
            const ObDatumRow &row,
            const ObIArray<int64_t> *update_array,
            const ObIArray<ObColDesc> *col_descs,
            char *buf,
            const int64_t buf_size,
            int64_t &writed_len);

  /**
   * @brief it's same as below write function, but it's using local buffer to write data
   *
   * @param rowkey_cnt
   * @param row
   * @param update_array
   * @param col_descs
   * @param buf
   * @param len
   * @return int
   */
  int write(const int64_t rowkey_cnt,
            const ObDatumRow &row,
            const ObIArray<int64_t> *update_array,
            const ObIArray<ObColDesc> *col_descs,
            char *&buf,
            int64_t &len);

  /**
   * @brief same as below write function but just write rowkey
   *
   * @param rowkey
   * @param col_descs
   * @param buf
   * @param len
   * @return int
   */
  int write_lock_rowkey(const common::ObStoreRowkey &rowkey,
                        const ObIArray<ObColDesc> *col_descs,
                        char *&buf,
                        int64_t &len);

private:
  /**
   * @brief using ObRowReader to check the RowWriter result (for debug)
   *
   * @param row
   * @param rowkey_cnt
   * @param update_array
   * @param col_descs
   * @param buf
   * @param buf_len
   * @return int
   */
  static int check_write_result(const ObDatumRow &row,
                                const int64_t rowkey_cnt,
                                const ObIArray<int64_t> *update_array,
                                const ObIArray<ObColDesc> *col_descs,
                                const char *buf,
                                const int64_t buf_len);

  /**
   * @brief check if the parameter is valid
   *
   * @param row
   * @param update_array
   * @param rowkey_cnt
   * @return OB_INLINE
   */
  OB_INLINE int check_param_validity(const ObDatumRow &row,
                                     const ObIArray<int64_t> *update_array,
                                     const int64_t rowkey_cnt);

  /**
   * @brief check if the buffer is valid, and set buffer
   *
   * @param buf
   * @param buf_size
   * @param col_descs
   * @return OB_INLINE
   */
  OB_INLINE int init_writer_param(char *buf,
                                  const int64_t buf_size,
                                  const ObIArray<ObColDesc> *col_descs);

  OB_INLINE int extend_rowkey_buf();

  OB_INLINE int transform_rowkey_to_datums(const ObStoreRowkey &rowkey);

  // ----------------------------------------------------------------------------------------------
  // All functions below should try to use the unsigned type whenever possible, in order to speed up
  // any potential multiplication or mod operations.

  int append_row_header(const uint8_t row_flag,
                        const uint8_t multi_version_flag,
                        const int64_t trans_id,
                        const ColumnCnt column_cnt,
                        const RowkeyCnt rowkey_cnt);

  template <bool HAS_UPDATE_ARRAY>
  int inner_write_row(const ObStorageDatum *datums,
                      const ColumnCnt column_cnt,
                      const RowkeyCnt rowkey_cnt,
                      const ObIArray<int64_t> *update_array);

  int inner_write_row_without_update_for_less_column(const ObStorageDatum *datums,
                                                     const ColumnCnt column_cnt,
                                                     const RowkeyCnt rowkey_cnt);

  template <bool HAS_UPDATE_ARRAY>
  int analyze_cluster(const ObStorageDatum *datums,
                      const ColumnCnt column_cnt,
                      const RowkeyCnt rowkey_cnt,
                      const ObIArray<int64_t> *update_array);

  OB_INLINE void analyze_rowkey_cluster(const ObStorageDatum *datums, const RowkeyCnt rowkey_cnt);

  OB_INLINE void analyze_normal_cluster(const ColumnCnt column_cnt,
                                        const ColumnCnt nop_cnt,
                                        const ColumnCnt null_cnt);

  /**
   * @brief When the number of value columns in a full row is very small, we consider constructing a
   * global sparse cluster instead of creating multiple sparse clusters.
   *
   * @param column_cnt
   * @param nop_cnt
   * @param null_cnt
   * @param row_max_value_idx
   * @param row_max_nop_idx
   * @param row_max_null_idx
   */
  OB_INLINE void analyze_global_sparse_cluster(const ColumnCnt column_cnt,
                                               const ColumnCnt nop_cnt,
                                               const ColumnCnt null_cnt,
                                               const ColumnCnt row_max_value_idx,
                                               const ColumnCnt row_max_nop_idx,
                                               const ColumnCnt row_max_null_idx);

  template <bool HAS_UPDATE_ARRAY, typename R, ObFlatEmptyDatumType EmptyDatumType>
  int build_sparse_cluster(const ObStorageDatum *datums,
                           const ColumnIdx start,
                           const ColumnIdx end,
                           const ClusterIdx cluster_idx,
                           const int64_t *update_array,
                           const ColumnCnt update_array_cnt);

  template <bool HAS_UPDATE_ARRAY>
  int build_dense_cluster(const ObStorageDatum *datums,
                          const ColumnIdx start,
                          const ColumnIdx end,
                          const ClusterIdx cluster_idx,
                          const int64_t *update_array = nullptr,
                          const ColumnCnt update_array_cnt = 0);

  using BuildClusterFunc = int (ObRowWriter::*)(const ObStorageDatum *,
                                                const ColumnIdx,
                                                const ColumnIdx,
                                                const ClusterIdx,
                                                const int64_t *,
                                                const ColumnCnt);

  template <bool HAS_UPDATE_ARRAY>
  OB_INLINE int build_cluster(const ObStorageDatum *datums,
                              const ColumnIdx start,
                              const ColumnIdx end,
                              const ClusterIdx cluster_idx,
                              const int64_t *update_array,
                              const ColumnCnt update_array_cnt)
  {
    static_assert(static_cast<bool>(ObFlatEmptyDatumType::Nop) == 0, "nop must be false(0)");
    static_assert(static_cast<bool>(ObFlatEmptyDatumType::Null) == 1, "null must be true(1)");

    static const BuildClusterFunc BUILD_CLUSTER_FUNC[2][2][2][2]
        = {{{{// global_sparse = 0, is_sparse = 0
              &ObRowWriter::build_dense_cluster<false>,
              &ObRowWriter::build_dense_cluster<false>},
             {// global_sparse = 0, is_sparse = 1
              &ObRowWriter::build_sparse_cluster<false, uint8_t, ObFlatEmptyDatumType::Nop>,
              &ObRowWriter::build_sparse_cluster<false, uint8_t, ObFlatEmptyDatumType::Null>}},
            {{// global_sparse = 1, is_sparse(column int size) = 0
              &ObRowWriter::build_sparse_cluster<false, uint8_t, ObFlatEmptyDatumType::Nop>,
              &ObRowWriter::build_sparse_cluster<false, uint8_t, ObFlatEmptyDatumType::Null>},
             {// global_sparse = 1, is_sparse(column int size) = 1
              &ObRowWriter::build_sparse_cluster<false, uint16_t, ObFlatEmptyDatumType::Nop>,
              &ObRowWriter::build_sparse_cluster<false, uint16_t, ObFlatEmptyDatumType::Null>}}},
           {{{// global_sparse = 0, is_sparse = 0
              &ObRowWriter::build_dense_cluster<true>,
              &ObRowWriter::build_dense_cluster<true>},
             {// global_sparse = 0, is_sparse = 1
              &ObRowWriter::build_sparse_cluster<true, uint8_t, ObFlatEmptyDatumType::Nop>,
              &ObRowWriter::build_sparse_cluster<true, uint8_t, ObFlatEmptyDatumType::Null>}},
            {{// global_sparse = 1, is_sparse(column int size) = 0
              &ObRowWriter::build_sparse_cluster<true, uint8_t, ObFlatEmptyDatumType::Nop>,
              &ObRowWriter::build_sparse_cluster<true, uint8_t, ObFlatEmptyDatumType::Null>},
             {// global_sparse = 1, is_sparse(column int size) = 1
              &ObRowWriter::build_sparse_cluster<true, uint16_t, ObFlatEmptyDatumType::Nop>,
              &ObRowWriter::build_sparse_cluster<true, uint16_t, ObFlatEmptyDatumType::Null>}}}};

    bool is_global_sparse = row_header_->is_global_sparse();
    bool is_sparse = cluster_headers_[cluster_idx].is_sparse_;
    bool empty_datum_type_ = static_cast<bool>(cluster_headers_[cluster_idx].empty_datum_type_);

    return (this->*BUILD_CLUSTER_FUNC[HAS_UPDATE_ARRAY][is_global_sparse][is_sparse]
                                     [empty_datum_type_])(
        datums, start, end, cluster_idx, update_array, update_array_cnt);
  }

  OB_INLINE void append_column(const ObStorageDatum &datum,
                               const ColumnIdx datum_idx,
                               bool &is_zip,
                               uint8_t &flag);

  OB_INLINE int append_offset_array(const Offset *offset_array,
                                    const ColumnCnt array_length,
                                    ObIntSize &offset_element_type);

  template <typename R>
  void inner_append_offset_array(const Offset *offset_array, const ColumnCnt array_length);

  OB_INLINE bool is_int_type(ColumnIdx idx)
  {
    return col_descs_ && idx < col_descs_->count()
           && ob_is_integer_type(col_descs_->at(idx).col_type_.get_type());
  }

  TO_STRING_KV(KP_(buf), K_(buf_size), K_(pos), K_(cluster_cnt), KPC_(row_header));

private:
  uint8_t *buf_;
  uint64_t buf_size_;
  Position pos_;
  ObRowHeader *row_header_;
  const ObIArray<ObColDesc> *col_descs_;
  uint32_t data_estimate_len_;
  uint32_t cluster_meta_len_;
  ClusterCnt cluster_cnt_;
  ObStorageDatum rowkey_local_buffer_[OB_MAX_ROWKEY_COLUMN_NUMBER >> 4];
  ObStorageDatum *rowkey_buffer_;
  RowkeyCnt rowkey_buffer_size_;
  ObFlatClusterHeader cluster_headers_[OB_FLAT_MAX_CLUSTER_CNT];
  Offset offset_array_[OB_MAX_ROWKEY_COLUMN_NUMBER];
  Offset cluster_offset_[OB_FLAT_MAX_CLUSTER_CNT];
  ObRowBuffer row_buffer_;
  DISALLOW_COPY_AND_ASSIGN(ObRowWriter);
};

class ObCompatRowWriter
{
public:
  ObCompatRowWriter()
      : write_ext_buffer_func_(&ObCompatRowWriter::inner_not_init_write),
        write_local_buffer_func_(&ObCompatRowWriter::inner_not_init_write_local_buffer),
        write_rowkey_func_(&ObCompatRowWriter::inner_not_init_write_lock_rowkey),
        destroy_func_(&ObCompatRowWriter::destroy_not_init), is_inited_(false)
  {
  }

  ~ObCompatRowWriter() { (this->*destroy_func_)(); }

  // init will choose which writer use
  OB_INLINE int init(int64_t micro_block_format_version)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "Init twice", KR(ret));
    } else if (storage::ObMicroBlockFormatVersionHelper::decide_flat_format(
                   micro_block_format_version)
               == FLAT_ROW_STORE) {
      write_ext_buffer_func_ = &ObCompatRowWriter::inner_old_write;
      write_local_buffer_func_ = &ObCompatRowWriter::inner_old_write_local_buffer;
      write_rowkey_func_ = &ObCompatRowWriter::inner_old_write_lock_rowkey;
      destroy_func_ = &ObCompatRowWriter::destroy_old;
      new (&writer_v0_) ObRowWriterV0();
      is_inited_ = true;
    } else {
      write_ext_buffer_func_ = &ObCompatRowWriter::inner_new_write;
      write_local_buffer_func_ = &ObCompatRowWriter::inner_new_write_local_buffer;
      write_rowkey_func_ = &ObCompatRowWriter::inner_new_write_lock_rowkey;
      destroy_func_ = &ObCompatRowWriter::destroy_new;
      new (&writer_) ObRowWriter();
      is_inited_ = true;
    }

    return ret;
  }

  // use function pointer for performance, don't check data_version when call write
  using WriteUsingOtherBufferFunc
      = int (ObCompatRowWriter::*)(const int64_t rowkey_cnt,
                                    const ObDatumRow &row,
                                    const ObIArray<int64_t> *update_array,
                                    const ObIArray<ObColDesc> *col_descs,
                                    char *buf,
                                    const int64_t buf_size,
                                    int64_t &writed_len);

  using WriteUsingLocalBufferFunc
      = int (ObCompatRowWriter::*)(const int64_t rowkey_cnt,
                                    const ObDatumRow &row,
                                    const ObIArray<int64_t> *update_array,
                                    const ObIArray<ObColDesc> *col_descs,
                                    char *&buf,
                                    int64_t &len);

  using WriteRowkeyFunc = int (ObCompatRowWriter::*)(const common::ObStoreRowkey &rowkey,
                                                      const ObIArray<ObColDesc> *col_descs,
                                                      char *&buf,
                                                      int64_t &len);

  using DestroyFunc = void (ObCompatRowWriter::*)();

  // proxy funcs for writer
  OB_INLINE int write(const int64_t rowkey_cnt,
                      const ObDatumRow &row,
                      const ObIArray<int64_t> *update_array,
                      const ObIArray<ObColDesc> *col_descs,
                      char *buf,
                      const int64_t buf_size,
                      int64_t &writed_len)
  {
    return (this->*write_ext_buffer_func_)(rowkey_cnt,
                                           row,
                                           update_array,
                                           col_descs,
                                           buf,
                                           buf_size,
                                           writed_len);
  }

  OB_INLINE int write(const int64_t rowkey_cnt,
                      const ObDatumRow &row,
                      const ObIArray<int64_t> *update_array,
                      const ObIArray<ObColDesc> *col_descs,
                      char *&buf,
                      int64_t &len)
  {
    return (this->*write_local_buffer_func_)(rowkey_cnt, row, update_array, col_descs, buf, len);
  }

  OB_INLINE int write_lock_rowkey(const common::ObStoreRowkey &rowkey,
                                  const ObIArray<ObColDesc> *col_descs,
                                  char *&buf,
                                  int64_t &len)
  {
    return (this->*write_rowkey_func_)(rowkey, col_descs, buf, len);
  }

private:
  int inner_new_write(const int64_t rowkey_cnt,
                      const ObDatumRow &row,
                      const ObIArray<int64_t> *update_array,
                      const ObIArray<ObColDesc> *col_descs,
                      char *buf,
                      const int64_t buf_size,
                      int64_t &writed_len)
  {
    return writer_.write(rowkey_cnt, row, update_array, col_descs, buf, buf_size, writed_len);
  }

  int inner_old_write(const int64_t rowkey_cnt,
                      const ObDatumRow &row,
                      const ObIArray<int64_t> *,
                      const ObIArray<ObColDesc> *,
                      char *buf,
                      const int64_t buf_size,
                      int64_t &writed_len)
  {
    return writer_v0_.write(rowkey_cnt, row, buf, buf_size, writed_len);
  }

  int inner_not_init_write(const int64_t,
                           const ObDatumRow &,
                           const ObIArray<int64_t> *,
                           const ObIArray<ObColDesc> *,
                           char *,
                           const int64_t,
                           int64_t &)
  {
    int ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not init", K(ret));
    return ret;
  }

  int inner_new_write_local_buffer(const int64_t rowkey_cnt,
                                   const ObDatumRow &row,
                                   const ObIArray<int64_t> *update_array,
                                   const ObIArray<ObColDesc> *col_descs,
                                   char *&buf,
                                   int64_t &len)
  {
    return writer_.write(rowkey_cnt, row, update_array, col_descs, buf, len);
  }

  int inner_old_write_local_buffer(const int64_t rowkey_cnt,
                                   const ObDatumRow &row,
                                   const ObIArray<int64_t> *update_array,
                                   const ObIArray<ObColDesc> *,
                                   char *&buf,
                                   int64_t &len)
  {
    return writer_v0_.write(rowkey_cnt, row, update_array, buf, len);
  }

  int inner_not_init_write_local_buffer(const int64_t,
                                        const ObDatumRow &,
                                        const ObIArray<int64_t> *,
                                        const ObIArray<ObColDesc> *,
                                        char *&,
                                        int64_t &)
  {
    int ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not init", K(ret));
    return ret;
  }

  int inner_new_write_lock_rowkey(const common::ObStoreRowkey &rowkey,
                                  const ObIArray<ObColDesc> *col_descs,
                                  char *&buf,
                                  int64_t &len)
  {
    return writer_.write_lock_rowkey(rowkey, col_descs, buf, len);
  }

  int inner_old_write_lock_rowkey(const common::ObStoreRowkey &rowkey,
                                  const ObIArray<ObColDesc> *col_descs,
                                  char *&buf,
                                  int64_t &len)
  {
    return writer_v0_.write_lock_rowkey(rowkey, buf, len);
  }

  int inner_not_init_write_lock_rowkey(const common::ObStoreRowkey &,
                                       const ObIArray<ObColDesc> *,
                                       char *&,
                                       int64_t &)
  {
    int ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not init", K(ret));
    return ret;
  }

  void destroy_new() { writer_.~ObRowWriter(); }

  void destroy_old() { writer_v0_.~ObRowWriterV0(); }

  void destroy_not_init() {}

  union {
    ObRowWriter writer_;
    ObRowWriterV0 writer_v0_;
  };

  WriteUsingOtherBufferFunc write_ext_buffer_func_;
  WriteUsingLocalBufferFunc write_local_buffer_func_;
  WriteRowkeyFunc write_rowkey_func_;
  DestroyFunc destroy_func_;

  bool is_inited_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif
