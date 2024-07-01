/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_BLOCKSSTABLE_DATUM_ROWKEY_VECTOR_H_
#define OB_STORAGE_BLOCKSSTABLE_DATUM_ROWKEY_VECTOR_H_
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{
namespace blocksstable
{

class ObRowkeyVectorHelper
{
public:
  OB_INLINE static bool can_use_non_datum_rowkey_vector(const bool is_cg, const ObTabletID &tablet_id)
  {
    return !is_cg && !tablet_id.is_inner_tablet();
  }
};

enum class ObColumnVectorType: int8_t
{
  UNKNOW_TYPE = -1,
  DATUM_TYPE = 0,
  SIGNED_INTEGER_TYPE = 1,
  UNSIGNED_INTEGER_TYPE = 2,
  MAX_TYPE = 3,
};

OB_INLINE bool is_valid_vector_type(const ObColumnVectorType type)
{
  return type > ObColumnVectorType::UNKNOW_TYPE && type < ObColumnVectorType::MAX_TYPE;
}

template<typename T>
struct ObRVIntegerCell
{
  ObRVIntegerCell() = default;
  ObRVIntegerCell(T d, const bool null) : d_(d), null_(null) {}
  ~ObRVIntegerCell() = default;
  TO_STRING_KV(K_(d), K_(null));
  T d_;
  bool null_;
};

class ObColumnVector
{
public:
  ObColumnVector() : flag_(0), data_(nullptr), nulls_(nullptr)
  {
    type_ = ObColumnVectorType::UNKNOW_TYPE;
  }
  virtual ~ObColumnVector() = default;
  OB_INLINE bool is_valid() const { return is_valid_vector_type(type_) && nullptr != data_ && row_cnt_ > 0 && is_filled_; }
  int locate_key(
      const bool need_upper_bound,
      int64_t &begin,
      int64_t &end,
      const ObStorageDatum &key,
      const ObStorageDatumCmpFunc &cmp_func,
      const bool is_oracle_mode) const;
  int fill_column_datum(
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      const int64_t row_idx,
      const ObStorageDatum &datum);
  int get_deep_copy_size(const int64_t row_idx, int64_t &size) const;
  int get_column_datum(const int64_t row_idx, ObStorageDatum &dst, char *buf, const int64_t buf_size, int64_t &pos);
  int get_column_int(const int64_t row_idx, int64_t &int_val) const;
  int deep_copy(
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      const ObColumnVector &other);
  template<typename T>
  int inner_locate_key(
      const bool need_upper_bound,
      int64_t &begin,
      int64_t &end,
      const ObStorageDatum &key,
      const ObStorageDatumCmpFunc &cmp_func,
      const bool is_oracle_mode) const;
  template<typename T>
  int locate_integer_key(
      const bool need_upper_bound,
      int64_t &begin,
      int64_t &end,
      const T *data,
      const T key) const;
  template<typename T>
  int locate_integer_key_with_null(
      const bool need_upper_bound,
      int64_t &begin,
      int64_t &end,
      T *data,
      const ObRVIntegerCell<T> &cell,
      const bool is_oracle_mode) const;
  static int construct_column_vector(
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      const int64_t row_cnt,
      const ObObjMeta *obj_meta,
      ObColumnVector &vector);
  static int construct_datum_vector(
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      const int64_t row_cnt,
      ObColumnVector &vector);
  static int construct_integer_vector(
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      const int64_t row_cnt,
      const bool is_signed,
      ObColumnVector &vector);
  DECLARE_TO_STRING;
  union {
    struct {
      ObColumnVectorType type_ : 8;
      int64_t has_null_ : 1;
      int64_t row_cnt_ : 32;
      int64_t is_filled_: 1;
      int64_t reserved_ : 22;
    };
    int64_t flag_;
  };
  union {
    ObStorageDatum *datums_;
    int64_t *signed_ints_;
    uint64_t *unsigned_ints_;
    void *data_;
  };
  bool *nulls_;
};

class ObRowkeyVector
{
public:
  ObRowkeyVector()
    : flag_(0),
      columns_(nullptr),
      last_rowkey_(nullptr),
      discrete_rowkey_array_(nullptr)
  {}
  ~ObRowkeyVector() {};
  int locate_key(
      const int64_t begin,
      const int64_t end,
      const ObDatumRowkey &rowkey,
      const ObStorageDatumUtils &datum_utils,
      int64_t &rowkey_idx,
      const bool is_lower_bound = true) const;
  int locate_range(
      const ObDatumRange &range,
      const bool is_left_border,
      const bool is_right_border,
      const bool is_normal_cg,
      const ObStorageDatumUtils &datum_utils,
      int64_t &endkey_begin_idx,
      int64_t &endkey_end_idx) const;
  int compare_rowkey(
      const ObDatumRowkey &rowkey,
      const int64_t row_idx,
      const ObStorageDatumUtils &datum_utils,
      int &cmp_ret,
      const bool compare_datum_cnt) const;
  int compare_rowkey(
      const ObDiscreteDatumRowkey &rowkey,
      const int64_t row_idx,
      const ObStorageDatumUtils &datum_utils,
      int &cmp_ret,
      const bool compare_datum_cnt) const;
  int get_rowkey(const int64_t row_idx, ObDatumRowkey &rowkey) const;
  int get_rowkey(const int64_t row_idx, ObCommonDatumRowkey &common_rowkey) const;
  int get_deep_copy_rowkey_size(const int64_t row_idx, int64_t &size) const;
  int deep_copy_rowkey(const int64_t row_idx, ObDatumRowkey &dest, char *buf, const int64_t buf_size) const;
  int get_column_int(const int64_t row_idx, const int64_t col_idx, int64_t &int_val) const;
  int fill_last_rowkey();
  int set_construct_finished();
  OB_INLINE const ObDatumRowkey *get_last_rowkey() const
  {
    return last_rowkey_;
  }
  int deep_copy(
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      const ObRowkeyVector &other);
  static int get_occupied_size(
      const int64_t row_cnt,
      const int64_t col_cnt,
      const ObIArray<share::schema::ObColDesc> *col_descs,
      int64_t &size);
  static int construct_rowkey_vector(
      const int64_t row_cnt,
      const int64_t col_cnt,
      const ObIArray<share::schema::ObColDesc> *col_descs,
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      ObRowkeyVector *&rowkey_vector);
  static int prepare_rowkeys_buffer(
      const int64_t row_cnt,
      const int64_t col_cnt,
      char *buf,
      int64_t &pos,
      const int64_t buf_size,
      ObRowkeyVector *rowkey_vector);
  static bool is_all_integer_cols(const int64_t col_cnt, const ObIArray<share::schema::ObColDesc> *col_descs);
  DECLARE_TO_STRING;
private:
  int compare_datum_rowkey(
      const ObDatumRowkey &rowkey,
      const int64_t row_idx,
      const ObStorageDatumUtils &datum_utils,
      const int64_t cmp_cnt,
      int &cmp_ret,
      const bool compare_datum_cnt) const;
public:
  union {
    struct {
      int64_t col_cnt_ : 16;
      int64_t row_cnt_ : 32;
      int64_t is_datum_vectors_ : 1;
      int64_t reserved_ : 15;
    };
    int64_t flag_;
  };
  ObColumnVector *columns_;
  ObDatumRowkey *last_rowkey_;
  ObDiscreteDatumRowkey *discrete_rowkey_array_;
};

template<typename T>
int ObColumnVector::locate_integer_key(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    const T *data,
    const T key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin >= end)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(begin), K(end));
  } else {
    const T *first = data + begin;
    const T *last = data + end;
    const T *lb = std::lower_bound(first, last, key);
    if (lb == last) {
      begin = end;
    } else if (FALSE_IT(begin = lb - data)) {
    } else if (need_upper_bound) {
      if (OB_UNLIKELY(*lb < key)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected cmp ret", K(ret), K(*lb), K(key), K(begin), K(end));
      } else if (*lb > key) {
        end = begin;
      } else {
        const T *ub = std::upper_bound(lb, last, key);
        if (ub == last) {
        } else {
          end = ub - data;
        }
      }
    } else {
      end = begin;
    }
  }
  return ret;
}

template<typename T>
struct ObRVIntegerIterator
{
  typedef std::random_access_iterator_tag iterator_category;
  typedef ObRVIntegerCell<T> value_type;
  typedef int64_t difference_type;
  typedef T* pointer;
  typedef T& reference;
  ObRVIntegerIterator(const int64_t pos, T *data, const bool *nulls)
    : pos_(pos), data_(data), nulls_(nulls) {}
  ~ObRVIntegerIterator() = default;
  OB_INLINE value_type &operator*()
  {
    cell_.d_ = data_[pos_];
    cell_.null_ = nulls_[pos_];
    return cell_;
  }
  OB_INLINE ObRVIntegerIterator operator--()
  {
    pos_--;
    return *this;
  }
  OB_INLINE ObRVIntegerIterator operator--(int)
  {
    return ObRVIntegerIterator(pos_--, data_, nulls_);
  }
  OB_INLINE ObRVIntegerIterator operator++()
  {
    pos_++;
    return *this;
  }
  OB_INLINE ObRVIntegerIterator operator++(int)
  {
    return ObRVIntegerIterator(pos_++, data_, nulls_);
  }
  OB_INLINE ObRVIntegerIterator &operator+(int64_t offset)
  {
    pos_ += offset;
    return *this;
  }
  OB_INLINE ObRVIntegerIterator &operator+=(int64_t offset)
  {
    pos_ += offset;
    return *this;
  }
  OB_INLINE difference_type operator-(const ObRVIntegerIterator &rhs)
  {
    return pos_ - rhs.pos_;
  }
  OB_INLINE ObRVIntegerIterator &operator-(int64_t offset)
  {
    pos_ -= offset;
    return *this;
  }
  OB_INLINE bool operator==(const ObRVIntegerIterator &rhs) const
  {
    return pos_ == rhs.pos_;
  }
  OB_INLINE bool operator!=(const ObRVIntegerIterator &rhs)
  {
    return pos_ != rhs.pos_;
  }
  OB_INLINE bool operator<(const ObRVIntegerIterator &rhs)
  {
    return pos_ < rhs.pos_;
  }
  OB_INLINE bool operator<=(const ObRVIntegerIterator &rhs)
  {
    return pos_ <= rhs.pos_;
  }
  OB_INLINE bool is_null() const
  {
    return nulls_[pos_];
  }
  int64_t pos_;
  T *data_;
  const bool *nulls_;
  value_type cell_;
};

// null first
// [left_is_null][right_is_null][left>right][left==right]
static int NULL_FIRST_CMP_RET[2][2][2][2] =
{
  // left_is_null = false
  {
    // right_is_null = false
    {
      // left > right = false
      {-1, 0},
      // left > right = true
      {1, 1}
    },
    // right_is_null = true
    {
      {1, 1},
      {1, 1}
    }
  },
  // left_is_null = true;
  {
    // right_is_null = false
    {
      {-1, -1},
      {-1, -1}
    },
    // right_is_null = true
    {
      {0, 0},
      {0, 0}
    }
  }
};
// null last
// [left_is_null][right_is_null][left>right][left==right]
static int NULL_LAST_CMP_RET[2][2][2][2] =
{
  // left_is_null = false
  {
    // right_is_null = false
    {
      // left > right = false
      {-1, 0},
      // left > right = true
      {1, 1}
    },
    // right_is_null = true
    {
      {-1, -1},
      {-1, -1}
    }
  },
  // left_is_null = true;
  {
    // right_is_null = false
    {
      {1, 1},
      {1, 1}
    },
    // right_is_null = true
    {
      {0, 0},
      {0, 0}
    }
  }
};

template<typename T>
class ObRVIntegerWithNullComparor
{
public:
  ObRVIntegerWithNullComparor(const bool null_first) : null_first_(null_first) {}
  ~ObRVIntegerWithNullComparor() = default;
  bool operator() (const ObRVIntegerCell<T> &left, const ObRVIntegerCell<T> &right)
  {
    return compare(left, right) < 0;
  }
  int compare(const ObRVIntegerCell<T> &left, const ObRVIntegerCell<T> &right)
  {
    int cmp_ret = 0;
    if (null_first_) {
      cmp_ret = NULL_FIRST_CMP_RET[left.null_][right.null_][left.d_ > right.d_][left.d_ == right.d_];
    } else {
      cmp_ret = NULL_LAST_CMP_RET[left.null_][right.null_][left.d_ > right.d_][left.d_ == right.d_];
    }
    return cmp_ret;
  }
private:
  bool null_first_;
};

template<typename T>
int ObColumnVector::locate_integer_key_with_null(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    T *data,
    const ObRVIntegerCell<T> &cell,
    const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin >= end || (!has_null_ && !cell.null_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(begin), K(end), K(has_null_), K(cell));
  } else {
    ObRVIntegerWithNullComparor<T> comparor(!is_oracle_mode);
    const ObRVIntegerIterator<T> first(begin, data, nulls_);
    const ObRVIntegerIterator<T> last(end, data, nulls_);
    ObRVIntegerIterator<T> lb = std::lower_bound(first, last, cell, comparor);
    if (lb == last) {
      begin = end;
    } else if (FALSE_IT(begin = lb.pos_)) {
    } else if (need_upper_bound) {
      const int cmp_ret =  comparor.compare(*lb, cell);
      if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected cmp ret", K(ret), K(cmp_ret), K(*lb), K(cell), K(begin), K(end));
      } else if (cmp_ret > 0) {
        end = begin;
      } else {
        ObRVIntegerIterator<T> ub = std::upper_bound(lb, last, cell, comparor);
        if (ub == last) {
        } else {
          end = ub.pos_;
        }
      }
    } else {
      end = begin;
    }
  }
  return ret;
}


} // namespace blocksstable
} // namespace oceanbase
#endif
