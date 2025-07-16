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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_READER_H_
#include "common/object/ob_obj_type.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/allocator/page_arena.h"
#include "ob_datum_rowkey.h"
#include "ob_flat_format.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/ob_nop_bitmap.h"

#include <stdint.h>

namespace oceanbase
{
namespace blocksstable
{
typedef common::ObIArray<share::schema::ObColDesc> ObColDescIArray;
typedef common::ObIArray<ObColumnParam *> ObColumnParamIArray;
class ObClusterReader
{
public:
  enum class ObClusterReaderType : uint8_t
  {
    Dense,
    Sparse,
  };

  static constexpr uint32_t MAX_READER_MEMORY_SIZE = 128;

  ObClusterReader(ObClusterReaderType type) : type_(type){};

  virtual ~ObClusterReader() = default;

  OB_INLINE ObClusterReaderType get_type() const { return type_; }

  OB_INLINE uint32_t get_coverd_column_cnt() const { return coverd_column_cnt_; }

  OB_INLINE uint32_t get_first_column_idx() const { return first_column_idx_; }

  /**
   * @brief init cluster reader (dense or sparse)
   *
   *    The column_cnt represents the number of columns stored in this cluster. For a dense cluster,
   * this value itself is not stored but can be calculated based on rowkey_cnt, column_cnt,
   * cluster_idx, and other parameters. For a sparse cluster, this value can be read from the
   * header.
   *    However, column_cnt does not represent the total number of columns that this cluster can
   * potentially represent. For a sparse cluster, it may be capable of representing up to 32 columns
   * of values, but this cluster only stores column_cnt number of values, with the remaining values
   * being represented as "nop" or "null".
   *
   * @param buf
   * @param buf_len
   * @param column_cnt see comment
   * @param coverd_column_cnt the column cnt of this cluster coverd
   * @param first_column_idx the first column idx of cluster coverd
   */
  virtual void init(const char *buf,
                    const uint64_t buf_len,
                    const uint32_t column_cnt,
                    const uint32_t coverd_column_cnt,
                    const uint32_t first_column_idx)
      = 0;

  /**
   * @brief read the @idx_in_cluster column value into @datum
   *
   * @param idx_in_cluster
   * @param datum
   * @return int
   */
  virtual int read_specific_column(const uint32_t idx_in_cluster, ObStorageDatum &datum) const = 0;

  /**
   * @brief sequence batch read [0, @size) columns into @datums
   *
   * @param datums
   * @param size
   * @return int
   */
  virtual int batch_read(ObStorageDatum *datums, const uint32_t size) const = 0;

  /**
   * @brief Read as many consecutive columns within this cluster from cols_index_array as possible.
   * Also, ensure compatibility with the checking and optimization of nop_bitmap.
   *
   * @param datums
   * @param cols_index_array
   * @param array_curr_idx
   * @param array_cnt
   * @param batch_read_cnt the real readed column cnt
   * @param nop_bitmap
   * @return int
   */
  virtual int try_batch_read(ObStorageDatum *datums,
                             const storage::ObColumnIndexArray &cols_index_array,
                             const uint32_t array_curr_idx,
                             const uint32_t array_cnt,
                             uint32_t &batch_read_cnt,
                             memtable::ObNopBitMap *nop_bitmap = nullptr) const = 0;

  /**
   * @brief sequence batch compare [0, rhs.count) columns value with rhs
   *
   * @param rhs
   * @param datum_utils
   * @param cmp_ret
   * @return int
   */
  virtual int batch_compare(const ObDatumRowkey &rhs,
                            const ObStorageDatumUtils &datum_utils,
                            int &cmp_ret) const = 0;

protected:
  ObClusterReaderType type_;
  uint32_t coverd_column_cnt_;
  uint32_t first_column_idx_;
};

template <typename T> class ObDenseClusterReader : public ObClusterReader
{
public:
  ObDenseClusterReader() : ObClusterReader(ObClusterReaderType::Dense)
  {
    static_assert(sizeof(*this) < MAX_READER_MEMORY_SIZE, "should expansion size");
  }

  virtual ~ObDenseClusterReader() = default;

  void init(const char *buf,
            const uint64_t buf_len,
            const uint32_t column_cnt,
            const uint32_t coverd_column_cnt,
            const uint32_t first_column_idx) override;

  int read_specific_column(const uint32_t idx_in_cluster, ObStorageDatum &datum) const override;

  int batch_read(ObStorageDatum *datums, const uint32_t size) const override;

  int try_batch_read(ObStorageDatum *datums,
                     const storage::ObColumnIndexArray &cols_index_array,
                     const uint32_t array_curr_idx,
                     const uint32_t array_cnt,
                     uint32_t &batch_read_cnt,
                     memtable::ObNopBitMap *nop_bitmap = nullptr) const override;

  int batch_compare(const ObDatumRowkey &rhs,
                    const ObStorageDatumUtils &datum_utils,
                    int &cmp_ret) const override;

private:
  OB_INLINE int read_column(const uint32_t col_idx, ObStorageDatum &datum) const;

  const ObDenseClusterBitmap bitmap_;
  const char *content_;
  const T *offset_array_;
};

template <typename T, typename R, ObFlatEmptyDatumType EmptyDatumType>
class ObSparseClusterReader : public ObClusterReader
{
public:
  ObSparseClusterReader() : ObClusterReader(ObClusterReaderType::Sparse)
  {
    static_assert(sizeof(*this) < MAX_READER_MEMORY_SIZE, "should expansion size");
  }

  virtual ~ObSparseClusterReader() = default;

  void init(const char *buf,
            const uint64_t buf_len,
            const uint32_t column_cnt,
            const uint32_t coverd_column_cnt,
            const uint32_t first_column_idx) override;

  int read_specific_column(const uint32_t idx_in_cluster, ObStorageDatum &datum) const override;

  int batch_read(ObStorageDatum *datums, const uint32_t size) const override;

  int try_batch_read(ObStorageDatum *datums,
                     const storage::ObColumnIndexArray &cols_index_array,
                     const uint32_t array_curr_idx,
                     const uint32_t array_cnt,
                     uint32_t &batch_read_cnt,
                     memtable::ObNopBitMap *nop_bitmap = nullptr) const override;

  int batch_compare(const ObDatumRowkey &rhs,
                    const ObStorageDatumUtils &datum_utils,
                    int &cmp_ret) const override;

private:
  OB_INLINE int read_column_in_array(const uint32_t idx, ObStorageDatum &datum) const;

  OB_INLINE int32_t get_idx_in_array(const uint32_t column_idx) const;

  OB_INLINE void inner_build_cache();

  template <ObFlatEmptyDatumType U = EmptyDatumType>
  OB_INLINE typename std::enable_if<U == ObFlatEmptyDatumType::Nop>::type
  set_datum_for_highbit_mask(ObStorageDatum &datum) const
  {
    datum.set_null();
  }

  template <ObFlatEmptyDatumType U = EmptyDatumType>
  OB_INLINE typename std::enable_if<U == ObFlatEmptyDatumType::Null>::type
  set_datum_for_highbit_mask(ObStorageDatum &datum) const
  {
    datum.set_nop();
  }

  template <ObFlatEmptyDatumType U = EmptyDatumType>
  OB_INLINE typename std::enable_if<U == ObFlatEmptyDatumType::Nop>::type
  set_datum_for_not_exists(ObStorageDatum &datum) const
  {
    datum.set_nop();
  }

  template <ObFlatEmptyDatumType U = EmptyDatumType>
  OB_INLINE typename std::enable_if<U == ObFlatEmptyDatumType::Null>::type
  set_datum_for_not_exists(ObStorageDatum &datum) const
  {
    datum.set_null();
  }

  const ObFlatColumnIDXWithFlag<R> *column_idx_array_;
  const char *content_;
  const T *offset_array_;
  uint32_t column_cnt_;
  int8_t cache_for_idx_find_[OB_FLAT_CLUSTER_COLUMN_CNT];
};

class ObClusterColumnReader
{
public:
  ObClusterColumnReader()
   : cluster_buf_(nullptr),
     cell_end_pos_(0),
     column_cnt_(0),
     sparse_column_cnt_(0),
     cur_idx_(0),
     column_offset_(nullptr),
     column_idx_array_(nullptr),
     special_vals_(nullptr),
     offset_bytes_(ObColClusterInfoMask::BYTES_MAX),
     col_idx_bytes_(ObColClusterInfoMask::BYTES_MAX),
     is_sparse_row_(false),
     is_inited_(false)
  {
  }
  bool is_init() const { return is_inited_; }
  int init(
      const char *cluster_buf,
      const uint64_t cluster_len,
      const uint64_t cluster_col_cnt,
      const ObColClusterInfoMask &info_mask);
  void reset();
  int read_storage_datum(const int64_t column_idx, ObStorageDatum &datum);
  int sequence_read_datum(const int64_t column_idx, ObStorageDatum &datum);
  int sequence_deep_copy_datums(const int64_t start_idx, ObStorageDatum *datum);
  int read_cell_with_bitmap(
      const int64_t start_idx,
      const storage::ObITableReadInfo &read_info,
      ObDatumRow &datum_row,
      memtable::ObNopBitMap &nop_bitmap);
  OB_INLINE int64_t get_sparse_col_idx(const int64_t column_idx);
  OB_INLINE int64_t get_column_count() const { return column_cnt_; }

  TO_STRING_KV(KP_(cluster_buf), K_(cell_end_pos), K_(column_cnt),
      K_(is_sparse_row), K_(offset_bytes), K_(col_idx_bytes), KP_(special_vals));
private:
  int sequence_deep_copy_datums_of_sparse(const int64_t start_idx, ObStorageDatum *datums);
  int sequence_deep_copy_datums_of_dense(const int64_t start_idx, ObStorageDatum *datums);
  int read_8_bytes_column(
      const char *buf,
      const int64_t buf_len,
      ObStorageDatum &datum);
  int read_column_from_buf(
      int64_t tmp_pos,
      int64_t next_pos,
      const ObRowHeader::SPECIAL_VAL special_val,
      ObStorageDatum &datum);
  int read_datum(const int64_t column_idx, ObStorageDatum &datum);
  OB_INLINE uint8_t read_special_value(const int64_t column_idx)
  {
    const int64_t index = column_idx >> 1;
    const int64_t shift = (column_idx % 2) << 2;
    return (special_vals_[index] >> shift) & 0x0F;
  }
private:
  const char *cluster_buf_;
  int64_t cell_end_pos_;
  int64_t column_cnt_;
  int64_t sparse_column_cnt_;
  int64_t cur_idx_; // only use when sequence read sparse cell
  const void *column_offset_;
  const void *column_idx_array_;
  const uint8_t *special_vals_;
  ObColClusterInfoMask::BYTES_LEN offset_bytes_;
  ObColClusterInfoMask::BYTES_LEN col_idx_bytes_;
  bool is_sparse_row_;
  bool is_inited_;
};

class ObRowReaderV0
{
public:
  ObRowReaderV0();
  virtual ~ObRowReaderV0() { reset(); }
  // read row from flat storage(RowHeader | cells array | column index array)
  // @param (row_buf + pos) point to RowHeader
  // @param row_len is buffer capacity
  // @param column_map use when schema version changed use column map to read row
  // @param [out]row parsed row object.
  // @param out_type indicates the type of ouput row
  int read_row(
      const char *row_buf,
      const int64_t row_len,
      const storage::ObITableReadInfo *read_info,
      ObDatumRow &datum_row);
  // only read cells where bitmap shows col_idx = TRUE
  int read_memtable_row(
      const char *row_buf,
      const int64_t row_len,
      const storage::ObITableReadInfo &read_info,
      ObDatumRow &datum_row,
      memtable::ObNopBitMap &nop_bitmap,
      bool &read_finished,
      const ObRowHeader *&row_header);
  int read_row_header(const char *row_buf, const int64_t row_len, const ObRowHeader *&row_header);
  int dump_row(const char *row_buf, const int64_t buf_len, FILE* fd);
  int read_column(
      const char *row_buf,
      const int64_t row_len,
      const int64_t col_index,
      ObStorageDatum &datum);
  int compare_meta_rowkey(
      const ObDatumRowkey &rhs,
      const blocksstable::ObStorageDatumUtils &datum_utils,
      const char *buf,
      const int64_t row_len,
      int32_t &cmp_result);
  void reset();
  TO_STRING_KV(KP_(buf), K_(row_len), KPC_(row_header), K_(cluster_cnt),
      K_(cur_read_cluster_idx), K_(cluster_reader));
private:
  int setup_row(const char *buf, const int64_t row_len);
  OB_INLINE int analyze_row_header();
  OB_INLINE int analyze_cluster_info();

  OB_INLINE int read_specific_column_in_cluster(const int64_t store_idx, ObStorageDatum &datum);
  static int read_char(const char* buf, int64_t end_pos, int64_t &pos, ObString &value);
  template<class T> static const T *read(const char *row_buf, int64_t &pos);
  OB_INLINE bool is_valid() const;
  OB_INLINE uint64_t get_cluster_offset(const int64_t cluster_idx) const;
  OB_INLINE uint64_t get_cluster_end_pos(const int64_t cluster_idx) const;
  OB_INLINE int analyze_info_and_init_reader(const int64_t cluster_idx);
protected:
  const char *buf_;
  int64_t row_len_;
  const ObRowHeader *row_header_;
  const void *cluster_offset_;
  const void *column_offset_;
  const void *column_idx_array_;
  ObClusterColumnReader cluster_reader_;
  uint32_t cur_read_cluster_idx_;
  uint32_t cluster_cnt_;
  bool rowkey_independent_cluster_;
  bool is_setuped_;
};

template<class T>
inline const T *ObRowReaderV0::read(const char *row_buf, int64_t &pos)
{
  const T *ptr = reinterpret_cast<const T*>(row_buf + pos);
  pos += sizeof(T);
  return ptr;
}

class ObRowReaderV1
{
public:
  ObRowReaderV1() = default;

  virtual ~ObRowReaderV1() = default;

  /**
   * @brief read row using read_info
   *
   * @param row_buf
   * @param row_len
   * @param read_info (contains seq_read_cnt and specific column idx)
   * @param row
   * @return int
   */
  int read_row(const char *row_buf,
               const int64_t row_len,
               const storage::ObITableReadInfo *read_info,
               ObDatumRow &row);

  /**
   * @brief read row in memtable, the nop bitmap indicate which column is still nop and should read
   *
   * @param row_buf
   * @param row_len
   * @param read_info
   * @param row
   * @param nop_bitmap
   * @param read_finished
   * @param row_header
   * @return int
   */
  int read_memtable_row(const char *row_buf,
                        const int64_t row_len,
                        const storage::ObITableReadInfo &read_info,
                        ObDatumRow &row,
                        memtable::ObNopBitMap &nop_bitmap,
                        bool &read_finished,
                        const ObRowHeader *&row_header);

  /**
   * @brief only get the row header
   *
   * @param row_buf
   * @param row_len
   * @param row_header
   * @return int
   */
  int read_row_header(const char *row_buf, const int64_t row_len, const ObRowHeader *&row_header);

  /**
   * @brief read the specific column
   *
   * @param row_buf
   * @param row_len
   * @param col_index
   * @param datum
   * @return int
   */
  int read_column(const char *row_buf,
                  const int64_t row_len,
                  const int64_t col_index,
                  ObStorageDatum &datum);

  /**
   * @brief compare the row's rowkey and rhs
   *
   * @param rhs
   * @param datum_utils
   * @param buf
   * @param row_len
   * @param cmp_result
   * @return int
   */
  int compare_meta_rowkey(const ObDatumRowkey &rhs,
                          const blocksstable::ObStorageDatumUtils &datum_utils,
                          const char *buf,
                          const int64_t row_len,
                          int32_t &cmp_result);

  TO_STRING_KV(KP_(buf), KPC_(row_header), K_(cluster_cnt));

private:
  OB_INLINE int init(const char *row_buf, const int64_t buf_len);

  /**
   * @brief transform the column idx into the cluster id which cover it
   *
   * @param col_idx
   * @return OB_INLINE
   */
  OB_INLINE uint32_t transform_to_cluster_idx(const uint32_t col_idx) const
  {
    // cluster_column_cnt_bit_ maybe
    //  - 32: when the row has a global sparse cluster
    //  - 5:  otherwise
    // this trick will erase the if(is_global_sparse) check cost

    return col_idx < rowkey_cnt_
               ? 0
               : (rowkey_cnt_ != 0)
                     + (static_cast<uint64_t>(col_idx - rowkey_cnt_) >> cluster_column_cnt_bit_);
  }

  OB_INLINE void init_cluster_reader(const bool is_global_sparse, const ObFlatClusterHeader header);

  /**
   * @brief init cluster
   *
   * @tparam T the type of cluster offset array(uint8, uint16, uint32)
   * @tparam IsGlobalSparse
   * @tparam HasRowkeyCluster
   * @param column_idx
   */
  template <typename T, bool IsGlobalSparse, bool HasRowkeyCluster>
  int inner_init_cluster(const uint32_t cluster_idx);

  OB_INLINE int init_cluster(const uint32_t cluster_idx)
  {
    return (this->*init_cluster_func_)(cluster_idx);
  }

  using InitClusterFunc = int (ObRowReaderV1::*)(const uint32_t);

protected:
  const char *buf_;
  const ObRowHeader *row_header_;
  const void *cluster_offset_;
  InitClusterFunc init_cluster_func_;
  ObClusterReader *reader_;
  uint32_t cluster_cnt_;
  uint32_t column_cnt_;
  uint32_t rowkey_cnt_;
  uint32_t cluster_column_cnt_bit_;
  alignas(size_t) char buffer_[ObClusterReader::MAX_READER_MEMORY_SIZE];
};

class ObCompatRowReader
{
public:
  int read_row(const char *row_buf,
               const int64_t row_len,
               const storage::ObITableReadInfo *read_info,
               ObDatumRow &row)
  {
    return should_use_reader_v0(row_buf, row_len)
               ? reader_v0_.read_row(row_buf, row_len, read_info, row)
               : reader_.read_row(row_buf, row_len, read_info, row);
  }

  OB_INLINE int read_memtable_row(const char *row_buf,
                                  const int64_t row_len,
                                  const storage::ObITableReadInfo &read_info,
                                  ObDatumRow &row,
                                  memtable::ObNopBitMap &nop_bitmap,
                                  bool &read_finished,
                                  const ObRowHeader *&row_header)
  {
    return should_use_reader_v0(row_buf, row_len)
               ? reader_v0_.read_memtable_row(
                     row_buf, row_len, read_info, row, nop_bitmap, read_finished, row_header)
               : reader_.read_memtable_row(
                     row_buf, row_len, read_info, row, nop_bitmap, read_finished, row_header);
  }

  OB_INLINE int read_row_header(const char *row_buf,
                                const int64_t row_len,
                                const ObRowHeader *&row_header)
  {
    return reader_.read_row_header(row_buf, row_len, row_header);
  }

  int read_column(const char *row_buf,
                  const int64_t row_len,
                  const int64_t col_index,
                  ObStorageDatum &datum)
  {
    return should_use_reader_v0(row_buf, row_len)
               ? reader_v0_.read_column(row_buf, row_len, col_index, datum)
               : reader_.read_column(row_buf, row_len, col_index, datum);
  }

  int compare_meta_rowkey(const ObDatumRowkey &rhs,
                          const blocksstable::ObStorageDatumUtils &datum_utils,
                          const char *buf,
                          const int64_t row_len,
                          int32_t &cmp_result)
  {
    return should_use_reader_v0(buf, row_len)
               ? reader_v0_.compare_meta_rowkey(rhs, datum_utils, buf, row_len, cmp_result)
               : reader_.compare_meta_rowkey(rhs, datum_utils, buf, row_len, cmp_result);
  }

private:
  OB_INLINE bool should_use_reader_v0(const char *row_buf, const int64_t buf_len)
  {
    return OB_NOT_NULL(row_buf) && buf_len >= sizeof(ObRowHeader)
           && reinterpret_cast<const ObRowHeader *>(row_buf)->get_version()
                  == ObRowHeader::ROW_HEADER_VERSION_1;
  }

  ObRowReaderV1 reader_;
  ObRowReaderV0 reader_v0_;
};

using ObRowReader = ObCompatRowReader;

} // end namespace blocksstable
} // end namespace oceanbase
#endif
