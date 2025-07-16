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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_FLAT_FORMAT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_FLAT_FORMAT_H_

#include "share/ob_define.h"
#include "ob_datum_row.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObRowBuffer
{
  ObRowBuffer() : buf_(local_buffer_), buf_size_(INIT_ROW_BUFFER_SIZE), local_buffer_() {}
  ~ObRowBuffer() { reset(); }
  OB_INLINE void reset()
  {
    if (buf_ != local_buffer_) {
      if (nullptr != buf_) {
        common::ob_free(buf_);
      }
      buf_ = local_buffer_;
      buf_size_ = INIT_ROW_BUFFER_SIZE;
    }
  }
  OB_INLINE int extend_buf()
  {
    int ret = common::OB_SUCCESS;
    if (buf_size_ >= MAX_ROW_BUFFER_SIZE) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "Fail to extend row buf", KR(ret), K(*this));
    } else if (OB_ISNULL(buf_ = reinterpret_cast<char *>(
                             common::ob_malloc(MAX_ROW_BUFFER_SIZE,
                                               ObMemAttr(MTL_ID(), "ObRowBuffer"))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to alloc memory for row buffer", KR(ret));
      reset();
    } else {
      buf_size_ = MAX_ROW_BUFFER_SIZE;
    }
    return ret;
  }
  OB_INLINE char *get_buf() { return buf_; }
  OB_INLINE int64_t get_buf_size() const { return buf_size_; }
  OB_INLINE bool is_buf_extendable() const { return buf_size_ < MAX_ROW_BUFFER_SIZE; }

  TO_STRING_KV(KP_(buf), K_(buf_size), KP_(local_buffer));

private:
  static constexpr int64_t INIT_ROW_BUFFER_SIZE = 4096;
  static constexpr int64_t MAX_ROW_BUFFER_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
  char *buf_;
  int64_t buf_size_;
  char local_buffer_[INIT_ROW_BUFFER_SIZE];
};

// ===============================================================
// ================ flat format base data struct =================
// ===============================================================

class ObColClusterInfoMask
{
public:
  enum BYTES_LEN
  {
    BYTES_ZERO = 0,
    BYTES_UINT8 = 1,
    BYTES_UINT16 = 2,
    BYTES_UINT32 = 3,
    BYTES_MAX = 4,
  };
  static constexpr uint8_t BYTES_TYPE_TO_LEN[] = {
      0,
      1,
      2,
      4,
      UINT8_MAX,
  };
  OB_INLINE static uint8_t get_bytes_type_len(const BYTES_LEN type)
  {
    STATIC_ASSERT(static_cast<int64_t>(BYTES_MAX + 1) == ARRAYSIZEOF(BYTES_TYPE_TO_LEN),
                  "type len array is mismatch");
    uint8_t ret_val = UINT8_MAX;
    if (OB_LIKELY(type >= BYTES_ZERO && type < BYTES_MAX)) {
      ret_val = BYTES_TYPE_TO_LEN[type];
    }
    return ret_val;
  }

public:
  ObColClusterInfoMask() : column_cnt_(0), info_mask_(0) {}
  static int get_serialized_size() { return sizeof(ObColClusterInfoMask); }
  static bool is_valid_col_idx_type(const BYTES_LEN col_idx_type)
  {
    return col_idx_type >= BYTES_ZERO && col_idx_type <= BYTES_UINT8;
  }
  static bool is_valid_offset_type(const BYTES_LEN column_offset_type)
  {
    return column_offset_type >= BYTES_ZERO && column_offset_type <= BYTES_UINT32;
  }
  OB_INLINE void reset()
  {
    column_cnt_ = 0;
    info_mask_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return is_valid_offset_type(get_offset_type())
           && (!is_sparse_row_
               || (is_valid_col_idx_type(get_column_idx_type()) && sparse_column_cnt_ >= 0))
           && column_cnt_ > 0;
  }
  OB_INLINE int64_t get_special_value_array_size(const int64_t serialize_column_cnt) const
  {
    return (sizeof(uint8_t) * serialize_column_cnt + 1) >> 1;
  }
  OB_INLINE int64_t get_total_array_size(const int64_t serialize_column_cnt) const
  {
    // offset_array + special_val_array + column_idx_array[SPARSE]
    return (get_offset_type_len() + (is_sparse_row_ ? get_column_idx_type_len() : 0))
               * serialize_column_cnt
           + (get_special_value_array_size(serialize_column_cnt));
  }
  OB_INLINE int set_offset_type(const BYTES_LEN column_offset_type)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_offset_type(column_offset_type))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid column ofset bytes", K(column_offset_type));
    } else {
      offset_type_ = column_offset_type;
    }
    return ret;
  }
  OB_INLINE int set_column_idx_type(const BYTES_LEN column_idx_type)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_col_idx_type(column_idx_type))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid column idx bytes", K(column_idx_type));
    } else {
      column_idx_type_ = column_idx_type;
    }
    return ret;
  }
  OB_INLINE BYTES_LEN get_offset_type() const { return (BYTES_LEN)offset_type_; }
  OB_INLINE uint8_t get_offset_type_len() const { return get_bytes_type_len(get_offset_type()); }
  OB_INLINE BYTES_LEN get_column_idx_type() const { return (BYTES_LEN)column_idx_type_; }
  OB_INLINE uint8_t get_column_idx_type_len() const
  {
    return get_bytes_type_len(get_column_idx_type());
  }
  OB_INLINE void set_sparse_row_flag(const bool is_sparse_row) { is_sparse_row_ = is_sparse_row; }
  OB_INLINE bool is_sparse_row() const { return is_sparse_row_; }
  OB_INLINE void set_column_count(const uint8_t column_count) { column_cnt_ = column_count; }
  OB_INLINE uint8_t get_column_count() const { return column_cnt_; }
  OB_INLINE void set_sparse_column_count(const uint8_t sparse_column_count)
  {
    sparse_column_cnt_ = sparse_column_count;
  }
  OB_INLINE uint8_t get_sparse_column_count() const { return sparse_column_cnt_; }
  TO_STRING_KV(K_(column_cnt),
               K_(offset_type),
               K_(is_sparse_row),
               K_(column_idx_type),
               K_(sparse_column_cnt));

  static const int64_t SPARSE_COL_CNT_BYTES = 2;
  static const int64_t MAX_SPARSE_COL_CNT = (0x1 << SPARSE_COL_CNT_BYTES) - 1; // 3
private:
  uint8_t column_cnt_; // if row is single cluster, column_cnt= UINT8_MAX
  union {
    uint8_t info_mask_;
    struct
    {
      uint8_t offset_type_ : 2;
      uint8_t is_sparse_row_ : 1;   // is sparse row
      uint8_t column_idx_type_ : 1; // means col_idx array bytes when is_sparse_row_ = true
      uint8_t sparse_column_cnt_
          : SPARSE_COL_CNT_BYTES; // 2 | means sparse column count when is_sparse_row_ = true
      uint8_t reserved_ : 2;
    };
  };
};

class ObRowHeader
{
public:
  ObRowHeader() { memset(this, 0, sizeof(*this)); }
  static int get_serialized_size() { return sizeof(ObRowHeader); }

  OB_INLINE bool is_valid() const
  {
    return column_cnt_ > 0 && rowkey_cnt_ >= 0
           && ObColClusterInfoMask::is_valid_offset_type(
               (ObColClusterInfoMask::BYTES_LEN)offset_type_);
  }

  constexpr static int64_t ROW_HEADER_VERSION_1 = 0;
  constexpr static int64_t ROW_HEADER_VERSION_2 = 1;

  OB_INLINE uint8_t get_version() const { return version_; }
  OB_INLINE void set_version(const uint8_t version) { version_ = version; }
  OB_INLINE ObDmlRowFlag get_row_flag() const { return ObDmlRowFlag(row_flag_); }
  OB_INLINE void set_row_flag(const uint8_t row_flag) { row_flag_ = row_flag; }

  OB_INLINE uint8_t get_mvcc_row_flag() const { return multi_version_flag_; }
  OB_INLINE ObMultiVersionRowFlag get_row_multi_version_flag() const
  {
    return ObMultiVersionRowFlag(multi_version_flag_);
  }
  OB_INLINE void set_row_mvcc_flag(const uint8_t row_type_flag)
  {
    multi_version_flag_ = row_type_flag;
  }

  OB_INLINE uint16_t get_column_count() const { return column_cnt_; }
  OB_INLINE void set_column_count(const uint16_t column_count) { column_cnt_ = column_count; }

  OB_INLINE void clear_header_mask() { header_info_mask_ = 0; }
  OB_INLINE void clear_reserved_bits() { reserved8_ = 0; }

  OB_INLINE void set_rowkey_count(const uint8_t rowkey_cnt) { rowkey_cnt_ = rowkey_cnt; }
  OB_INLINE uint8_t get_rowkey_count() const { return rowkey_cnt_; }

  OB_INLINE int64_t get_trans_id() const { return trans_id_; }
  OB_INLINE void set_trans_id(const int64_t trans_id) { trans_id_ = trans_id; }

  OB_INLINE bool is_single_cluster() const { return single_cluster_; }
  OB_INLINE void set_single_cluster(bool sigle_cluster) { single_cluster_ = sigle_cluster; }

  // this is for version 2, we have reused this flag.
  // TODO: for compatibility reasons, we will temporarily keep the name unchanged.
  OB_INLINE bool is_global_sparse() const { return single_cluster_; }
  OB_INLINE void set_global_sparse(bool global_sparse) { single_cluster_ = global_sparse; }

  OB_INLINE void set_offset_type(const uint8_t offset_type) { offset_type_ = offset_type; }
  OB_INLINE uint8_t get_offset_type() const { return offset_type_; }

  OB_INLINE int set_offset_type(const ObColClusterInfoMask::BYTES_LEN column_offset_type)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!ObColClusterInfoMask::is_valid_offset_type(column_offset_type))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid column offset bytes", K(column_offset_type));
    } else {
      offset_type_ = column_offset_type;
    }
    return ret;
  }
  OB_INLINE ObColClusterInfoMask::BYTES_LEN get_offset_type_v0() const
  {
    return (ObColClusterInfoMask::BYTES_LEN)offset_type_;
  }
  OB_INLINE uint8_t get_offset_type_len() const
  {
    return ObColClusterInfoMask::get_bytes_type_len(get_offset_type_v0());
  }

  OB_INLINE bool has_rowkey_independent_cluster() const
  {
    return need_rowkey_independent_cluster(rowkey_cnt_);
  }
  OB_INLINE int64_t get_cluster_cnt() const
  {
    int64_t cluster_cnt = 0;
    if (single_cluster_) {
      cluster_cnt = 1;
    } else {
      cluster_cnt = calc_cluster_cnt(rowkey_cnt_, column_cnt_);
    }
    return cluster_cnt;
  }
  static int64_t calc_cluster_cnt(const int64_t rowkey_cnt, const int64_t column_cnt)
  {
    return need_rowkey_independent_cluster(rowkey_cnt)
               ? 1 + calc_cluster_cnt_by_row_col_count(column_cnt - rowkey_cnt)
               : calc_cluster_cnt_by_row_col_count(column_cnt);
  }

  ObRowHeader &operator=(const ObRowHeader &src)
  {
    MEMCPY(this, &src, sizeof(ObRowHeader));
    return *this;
  }

  TO_STRING_KV(K_(version),
               K_(row_flag),
               K_(multi_version_flag),
               K_(column_cnt),
               K_(rowkey_cnt),
               K_(trans_id));

  static const int64_t CLUSTER_COLUMN_BYTES = 5;
  static const int64_t CLUSTER_COLUMN_CNT = 0x1 << CLUSTER_COLUMN_BYTES; // 32
  static const int64_t MAX_CLUSTER_COLUMN_CNT = 256;
  static const int64_t CLUSTER_COLUMN_CNT_MASK = CLUSTER_COLUMN_CNT - 1;
  static const int64_t USE_CLUSTER_COLUMN_COUNT = CLUSTER_COLUMN_CNT * 1.5; // 48
  static bool need_rowkey_independent_cluster(const int64_t rowkey_count)
  {
    return rowkey_count >= CLUSTER_COLUMN_CNT && rowkey_count <= USE_CLUSTER_COLUMN_COUNT;
  }
  static int64_t calc_cluster_cnt_by_row_col_count(const int64_t col_count)
  {
    return (col_count >> CLUSTER_COLUMN_BYTES) + ((col_count & CLUSTER_COLUMN_CNT_MASK) != 0);
  }
  static int64_t calc_cluster_idx(const int64_t column_idx)
  {
    return column_idx >> CLUSTER_COLUMN_BYTES;
  }
  static int64_t calc_column_cnt(const int64_t cluster_idx)
  {
    return cluster_idx << CLUSTER_COLUMN_BYTES;
  }
  static int64_t calc_column_idx_in_cluster(const int64_t column_count)
  {
    return column_count & CLUSTER_COLUMN_CNT_MASK;
  }
  enum SPECIAL_VAL
  {
    VAL_NORMAL = 0,
    VAL_OUTROW = 1,
    VAL_NOP = 2,
    VAL_NULL = 3,
    VAL_ENCODING_NORMAL = 4,
    VAL_MAX
  };

private:
  uint8_t version_;
  uint8_t row_flag_;
  uint8_t multi_version_flag_;
  uint8_t rowkey_cnt_;
  uint16_t column_cnt_;
  union {
    uint8_t header_info_mask_;
    struct
    {
      uint8_t offset_type_ : 2; // cluster offset
      uint8_t single_cluster_ : 1;
      uint8_t reserved_ : 5;
    };
  };
  uint8_t reserved8_;
  int64_t trans_id_;
};

// Some cluster related constant
// - a cluster has at most 32 column (CLUSTER_COLUMN_CNT)
// - use CLUSTER_COLUMN_CNT_BIT to do div operation
// - use CLUSTER_COLUMN_CNT_MASK to do mod operation
// - a sparse cluster has at most 8 column (SPARSE_CLUSTER_COLUMN_LIMIT)

static constexpr uint64_t OB_FLAT_CLUSTER_COLUMN_CNT_BIT = 5;

static constexpr uint64_t OB_FLAT_CLUSTER_COLUMN_CNT = 1 << OB_FLAT_CLUSTER_COLUMN_CNT_BIT;

static constexpr uint64_t OB_FLAT_CLUSTER_COLUMN_CNT_MASK = OB_FLAT_CLUSTER_COLUMN_CNT - 1;

static constexpr uint64_t OB_FLAT_SPARSE_CLUSTER_COLUMN_LIMIT = 8;

static constexpr uint64_t OB_FLAT_MAX_CLUSTER_CNT
    = common::OB_ROW_MAX_COLUMNS_COUNT / OB_FLAT_CLUSTER_COLUMN_CNT + 1;

// For dense cluster
// - dense cluster has a bitmap
// - bitmap don't do boundary check for performance
// - dense cluster: [bitmap array] [content array] [offset array]
// - bitmap element --> [0000] (using 4 bit)

class ObFlatBitmapValue
{
public:
  ObFlatBitmapValue(uint8_t value) : value_type_(value), deprecated_(0) {}

  OB_INLINE static ObFlatBitmapValue nop() { return ObFlatBitmapValue(0b0000); }
  OB_INLINE static ObFlatBitmapValue null() { return ObFlatBitmapValue(0b0001); }
  OB_INLINE static ObFlatBitmapValue zip_data() { return ObFlatBitmapValue(0b0010); }
  static ObFlatBitmapValue data(uint8_t flag) { return ObFlatBitmapValue(0b0100 | flag); }

  bool operator==(const ObFlatBitmapValue &rhs) const { return all_ == rhs.all_; }

  OB_INLINE bool is_nop() const { return *this == nop(); };

  OB_INLINE bool is_null() const { return *this == null(); };

  OB_INLINE bool is_zip_data() const { return *this == zip_data(); };

  OB_INLINE uint8_t get_value() const { return value_type_; }

  OB_INLINE bool is_data() const { return value_type_ & 0b0100; }

  OB_INLINE uint8_t get_flag() const { return is_data() ? value_type_ & 0b11 : 0; }

private:
  union {
    uint8_t all_;

    struct
    {
      uint8_t value_type_ : 4;
      uint8_t deprecated_ : 4;
    };
  };
};

class ObDenseClusterBitmap
{
public:
  ObDenseClusterBitmap() : buf_(nullptr) {}

  OB_INLINE void init(const uint8_t *buf) const { buf_ = const_cast<uint8_t *>(buf); };

  OB_INLINE void init_with_clear(const uint8_t *buf, uint32_t size)
  {
    buf_ = const_cast<uint8_t *>(buf);
    memset(buf_, 0, size);
  }

  OB_INLINE void set_value(uint64_t idx, ObFlatBitmapValue val)
  {
    buf_[idx >> 1] |= val.get_value() << ((idx & 1) << 2);
  }

  OB_INLINE ObFlatBitmapValue get_value(uint64_t idx) const
  {
    return ObFlatBitmapValue((buf_[idx >> 1] >> ((idx & 1) << 2)));
  }

  OB_INLINE static constexpr uint32_t calc_bitmap_size(uint32_t column_cnt)
  {
    return (column_cnt >> 1) + (column_cnt & 1);
  }

private:
  mutable uint8_t *buf_;
};

// For sparse cluster
// - empty datum type means the not exists column in sparse cluster is nop or null

enum class ObFlatEmptyDatumType : bool
{
  Nop = false,
  Null = true
};

// For sparse cluster
// - format: [column id array] [content] [offset array]
// - column id 0b 000 00000
//                 |    |
//        flag <---     ------> column idx

template <typename Base> class ObFlatColumnIDXWithFlag
{
public:
  OB_INLINE void set_value(Base column_idx, bool is_zip, uint8_t flag)
  {
    column_idx_ = column_idx;
    flag_ = is_zip ? 1 : flag;
    is_extend_ = is_zip;
  }

  OB_INLINE void set_nop_or_null_value(Base column_idx)
  {
    column_idx_ = column_idx;
    flag_ = 0;
    is_extend_ = 1;
  }

  OB_INLINE Base get_column_idx() const { return column_idx_; }

  OB_INLINE bool is_nop_or_null() const { return is_extend_ && (!flag_); }

  OB_INLINE bool is_zip_data() const { return is_extend_ && (flag_ & 1); }

  OB_INLINE uint8_t get_flag() const { return is_extend_ ? 0 : flag_; }

private:
  Base column_idx_ : std::is_same<Base, uint8_t>::value ? OB_FLAT_CLUSTER_COLUMN_CNT_BIT
                                                        : OB_FLAT_CLUSTER_COLUMN_CNT_BIT + 8;
  Base flag_ : 2;
  Base is_extend_ : 1;
};

static_assert(sizeof(ObFlatColumnIDXWithFlag<uint8_t>) == sizeof(uint8_t), "size is not correct");
static_assert(sizeof(ObFlatColumnIDXWithFlag<uint16_t>) == sizeof(uint16_t), "size is not correct");

// For int zip & calc offset array size

enum class ObIntSize : uint8_t
{
  Int8 = 0,
  Int16 = 1,
  Int32 = 2,
  Int64 = 3,
  Max = 4,
};

class ObIntSizeHelper
{
public:
  OB_INLINE constexpr static uint8_t byte_size(ObIntSize int_size)
  {
    return 1 << static_cast<uint8_t>(int_size);
  }

  OB_INLINE static ObIntSize from_int(uint64_t bigint)
  {
    uint8_t u8 = 0;
    u8 += (bigint >> 32) != 0;
    u8 += (bigint >> 16) != 0;
    u8 += (bigint >> 8) != 0;
    return static_cast<ObIntSize>(u8);
  }

  OB_INLINE static ObIntSize from_int(uint32_t bigint)
  {
    uint8_t u8 = 0;
    u8 += (bigint >> 16) != 0;
    u8 += (bigint >> 8) != 0;
    return static_cast<ObIntSize>(u8);
  }
};

// Cluster Header

class ObFlatClusterHeader
{
public:
  OB_INLINE void set_dense() { header_ = 0; }

  OB_INLINE uint32_t get_dense_meta_len(const uint32_t column_cnt)
  {
    // Dense Cluster : | bitmap | data | offset |
    // we can estimate len as follows, suppose offset array element is 4 byte
    return ObDenseClusterBitmap::calc_bitmap_size(column_cnt) + (column_cnt << 2);
  }

  OB_INLINE void set_sparse(const ObFlatEmptyDatumType empty_datum_type, const uint8_t column_cnt)
  {
    is_sparse_ = 1;
    empty_datum_type_ = empty_datum_type;
    offset_size_ = ObIntSize::Int8;
    sparse_column_cnt_ = column_cnt;
  }

  OB_INLINE uint32_t get_sparse_meta_len()
  {
    // Normal Cluster : | column id array | data | offset |
    // we can estimate len as follows, suppose offset array element is 4 byte
    return sparse_column_cnt_ + (sparse_column_cnt_ << 2);
  }

  OB_INLINE void set_global_sparse(const uint32_t max_column_idx,
                                   const ObFlatEmptyDatumType empty_datum_type,
                                   const uint8_t column_cnt)
  {
    is_sparse_ = max_column_idx >= OB_FLAT_CLUSTER_COLUMN_CNT;
    empty_datum_type_ = empty_datum_type;
    offset_size_ = ObIntSize::Int8;
    sparse_column_cnt_ = column_cnt;
  }

  OB_INLINE uint32_t get_global_sparse_meta_len()
  {
    // Global Sparse Cluster : | column id array(may be 2 byte) | data | offset |
    // we can estimate len as follows, suppose offset array element is 4 byte
    return (sparse_column_cnt_ << is_sparse_) + (sparse_column_cnt_ << 2);
  }

  union {
    uint8_t header_;
    struct
    {
      uint8_t is_sparse_ : 1;
      ObFlatEmptyDatumType empty_datum_type_ : 1;
      ObIntSize offset_size_ : 2;
      uint8_t sparse_column_cnt_ : 4;
    };
  };
};

static_assert(sizeof(ObIntSize::Max) <= 4, "size is incorrect");
static_assert(sizeof(ObFlatClusterHeader) == sizeof(uint8_t), "size is incorrect");

} // end namespace blocksstable
} // end namespace oceanbase

#endif
