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

#define USING_LOG_PREFIX STORAGE
#include "ob_datum_rowkey_vector.h"

namespace oceanbase
{
namespace blocksstable
{

class ObStorageDatumComparor
{
public:
  ObStorageDatumComparor(int &ret, const ObStorageDatumCmpFunc &cmp_func)
    : ret_(ret), cmp_func_(cmp_func) {}
  ~ObStorageDatumComparor() = default;
  OB_INLINE bool operator() (const ObStorageDatum &left, const ObStorageDatum &right)
  {
    int &ret = ret_;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cmp_func_.compare(left, right, cmp_ret))) {
      LOG_WARN("Failed to compare", K(ret), K(left), K(right));
    }
    return cmp_ret < 0;
  }
private:
  int &ret_;
  const ObStorageDatumCmpFunc &cmp_func_;
};

class ObVectorDatumRowkeyComparor
{
public:
  ObVectorDatumRowkeyComparor(int &ret, const int64_t cmp_cnt, const ObStorageDatumUtils &datum_utils)
    : ret_(ret), cmp_cnt_(cmp_cnt), datum_utils_(datum_utils) {}
  ~ObVectorDatumRowkeyComparor() = default;
  OB_INLINE bool operator() (const ObDiscreteDatumRowkey &left, const ObDatumRowkey &right)
  {
    int &ret = ret_;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else {
      const ObStoreCmpFuncs &cmp_funcs = datum_utils_.get_cmp_funcs();
      for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt_ && 0 == cmp_ret; ++i) {
        if (OB_FAIL(cmp_funcs.at(i).compare(left.rowkey_vector_->columns_[i].datums_[left.row_idx_], right.datums_[i], cmp_ret))) {
          LOG_WARN("Failed to compare key", K(ret), K(i), K(left), K(right));
        }
      }
      if (OB_SUCC(ret) && 0 == cmp_ret) {
        cmp_ret = left.rowkey_vector_->col_cnt_ - right.datum_cnt_;
      }
    }
    return cmp_ret < 0;
  }

  OB_INLINE bool operator() (const ObDatumRowkey &left, const ObDiscreteDatumRowkey &right)
  {
    int &ret = ret_;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else {
      const ObStoreCmpFuncs &cmp_funcs = datum_utils_.get_cmp_funcs();
      for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt_ && 0 == cmp_ret; ++i) {
        if (OB_FAIL(cmp_funcs.at(i).compare(right.rowkey_vector_->columns_[i].datums_[right.row_idx_], left.datums_[i], cmp_ret))) {
          LOG_WARN("Failed to compare key", K(ret), K(i), K(left), K(right));
        }
      }
      if (OB_SUCC(ret) && 0 == cmp_ret) {
        cmp_ret = right.rowkey_vector_->col_cnt_ - left.datum_cnt_;
      }
    }
    return cmp_ret > 0;
  }
private:
  int &ret_;
  int64_t cmp_cnt_;
  const ObStorageDatumUtils &datum_utils_;
};

template<>
int ObColumnVector::inner_locate_key<ObStorageDatum>(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin >= end)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(begin), K(end));
  } else {
    ObStorageDatumComparor compactor(ret, cmp_func);
    const ObStorageDatum *first = datums_ + begin;
    const ObStorageDatum *last = datums_ + end;
    const ObStorageDatum *lb = std::lower_bound(first, last, key, compactor);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to locate key", K(ret), K(key));
    } else if (lb == last) {
      begin = end;
    } else if (FALSE_IT(begin = lb - datums_)) {
    } else if (need_upper_bound) {
      int cmp_ret = 0;
      if (OB_FAIL(cmp_func.compare(*lb, key, cmp_ret))) {
        LOG_WARN("Failed to compare", K(ret));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cmp ret", K(ret), K(cmp_ret), K(*lb), K(key), K(begin), K(end));
      } else if (cmp_ret > 0) {
        end = begin;
      } else {
        const ObStorageDatum *ub = std::upper_bound(lb, last, key, compactor);
        if (ub == last) {
        } else {
          end = ub - datums_;
        }
      }
    } else {
      end = begin;
    }
  }
  return ret;
}

template<>
int ObColumnVector::inner_locate_key<int64_t>(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  if (has_null_) {
    ObRVIntegerCell<int64_t> cell(key.is_null() ? 0 : key.get_int(), key.is_null());
    if (OB_FAIL(locate_integer_key_with_null(need_upper_bound, begin, end, signed_ints_, cell, is_oracle_mode))) {
      LOG_WARN("Failed to locate integer key", K(ret), K(need_upper_bound),
               K(begin), K(end), K(key));
    }
  } else if (key.is_null()) {
    if (is_oracle_mode) {
      begin = end;
    } else {
      end = begin;
    }
  } else if (OB_FAIL(locate_integer_key(need_upper_bound, begin, end, signed_ints_, key.get_int()))) {
    LOG_WARN("Failed to locate integer key", K(ret), K(need_upper_bound),
             K(begin), K(end), K(key));
  }
  return ret;
}

template<>
int ObColumnVector::inner_locate_key<uint64_t>(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  if (has_null_) {
    ObRVIntegerCell<uint64_t> cell(key.is_null() ? 0 : key.get_uint(), key.is_null());
    if (OB_FAIL(locate_integer_key_with_null(need_upper_bound, begin, end, unsigned_ints_, cell, is_oracle_mode))) {
      LOG_WARN("Failed to locate integer key", K(ret), K(need_upper_bound), K(begin), K(end), K(key));
    }
  } else if (key.is_null()) {
    if (is_oracle_mode) {
      begin = end;
    } else {
      end = begin;
    }
  } else if (OB_FAIL(locate_integer_key(need_upper_bound, begin, end, unsigned_ints_, key.get_uint()))) {
    LOG_WARN("Failed to locate integer key", K(ret), K(need_upper_bound), K(begin), K(end), K(key));
  }
  return ret;
}

typedef int (ObColumnVector::*locate_key_func)(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode) const;
static locate_key_func LOCATE_KEY_FUNCS[(int8_t)ObColumnVectorType::MAX_TYPE] =
{
  &ObColumnVector::inner_locate_key<ObStorageDatum>,
  &ObColumnVector::inner_locate_key<int64_t>,
  &ObColumnVector::inner_locate_key<uint64_t>
};

int ObColumnVector::locate_key(
    const bool need_upper_bound,
    int64_t &begin,
    int64_t &end,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_vector_type(type_) || key.is_ext())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row idx", K(ret), K(type_), K(key));
  } else {
    locate_key_func func = LOCATE_KEY_FUNCS[(int8_t) type_];
    if (OB_FAIL((this->*func)(need_upper_bound, begin, end, key, cmp_func, is_oracle_mode))) {
      LOG_WARN("Failed to locate key", K(ret), K(begin), K(end), K(key));
    }
  }
  return ret;
}

int ObColumnVector::fill_column_datum(
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    const int64_t row_idx,
    const ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_vector_type(type_) || row_idx >= row_cnt_ || datum.is_ext())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row idx", K(ret), K(type_), K(row_idx), K(row_cnt_), K(datum));
  } else if (ObColumnVectorType::DATUM_TYPE == type_) {
    if (OB_FAIL(datums_[row_idx].deep_copy(datum, buf, buf_size, pos))) {
      LOG_WARN("Failed to deep copy storage datum to buf", K(ret), K(row_idx), K(datum));
    }
  } else if (datum.is_null()) {
    nulls_[row_idx] = true;
    has_null_ = true;
  } else {
    signed_ints_[row_idx] = datum.get_int();
    nulls_[row_idx] = false;
  }
  return ret;
}

int ObColumnVector::get_deep_copy_size(const int64_t row_idx, int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_UNLIKELY(!is_valid_vector_type(type_) || row_idx >= row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row idx", K(ret), K(type_), K(row_idx), K(row_cnt_));
  } else if (ObColumnVectorType::DATUM_TYPE == type_) {
    size = datums_[row_idx].get_deep_copy_size();
  }
  return ret;
}

int ObColumnVector::get_column_datum(const int64_t row_idx, ObStorageDatum &dst, char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_vector_type(type_) || row_idx >= row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row idx", K(ret), K(type_), K(row_idx), K(row_cnt_));
  } else {
    dst.reuse();
    if (ObColumnVectorType::DATUM_TYPE == type_) {
      if (OB_FAIL(dst.deep_copy(datums_[row_idx], buf, buf_size, pos))) {
        LOG_WARN("Failed to deep copy storage datum", K(ret), K(row_idx));
      }
    } else if (nulls_[row_idx]) {
      dst.set_null();
    } else {
      dst.set_int(signed_ints_[row_idx]);
    }
  }
  return ret;
}

int ObColumnVector::get_column_int(const int64_t row_idx, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_vector_type(type_) || row_idx >= row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row idx", K(ret), K(type_), K(row_idx), K(row_cnt_));
  } else {
    switch (type_) {
      case ObColumnVectorType::DATUM_TYPE: {
      int_val = datums_[row_idx].get_int();
        break;
      }
      case ObColumnVectorType::SIGNED_INTEGER_TYPE: {
        int_val = signed_ints_[row_idx];
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected column vector type", K(ret), K(type_));
        break;
      }
    }
  }
  return ret;
}

int ObColumnVector::deep_copy(
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    const ObColumnVector &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || !other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid buf or column vector", K(ret), KP(buf), K(other));
  } else {
    flag_ = other.flag_;
    switch (type_) {
      case ObColumnVectorType::DATUM_TYPE: {
        if (OB_UNLIKELY(nullptr == buf || buf_size < pos + sizeof(ObStorageDatum) * row_cnt_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid buf", K(ret), KP(buf), K(buf_size), K(pos), K(sizeof(ObStorageDatum)), K(row_cnt_));
        } else {
          datums_ = new (buf + pos) ObStorageDatum[row_cnt_];
          pos += sizeof(ObStorageDatum) * row_cnt_;
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cnt_; ++row_idx) {
            if (OB_FAIL(datums_[row_idx].deep_copy(other.datums_[row_idx], buf, buf_size, pos))) {
              LOG_WARN("Failed to deep copy datum", K(ret), K(row_idx), K(other.datums_[row_idx]));
            }
          }
        }
        break;
      }
      case ObColumnVectorType::SIGNED_INTEGER_TYPE:
      case ObColumnVectorType::UNSIGNED_INTEGER_TYPE: {
        if (OB_UNLIKELY(nullptr == buf || buf_size < pos + sizeof(int64_t) * row_cnt_ + sizeof(bool) * row_cnt_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid buf", K(ret), KP(buf), K(buf_size), K(pos), K(row_cnt_));
        } else {
          signed_ints_ = reinterpret_cast<int64_t*>(buf + pos);
          pos += sizeof(int64_t) * row_cnt_;
          nulls_ = reinterpret_cast<bool*>(buf + pos);
          pos += sizeof(bool) * row_cnt_;
          MEMCPY(data_, other.data_, sizeof(int64_t) * row_cnt_);
          MEMCPY(nulls_, other.nulls_, sizeof(bool) * row_cnt_);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected column vector type", K(ret), K(type_));
        break;
      }
    }
  }
  return ret;
}

int ObColumnVector::construct_column_vector(
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    const int64_t row_cnt,
    const ObObjMeta *obj_meta,
    ObColumnVector &vector)
{
  int ret = OB_SUCCESS;
  if (nullptr == obj_meta || !obj_meta->is_integer_type()) {
    ret = construct_datum_vector(buf, pos, buf_size, row_cnt, vector);
  } else {
    ret = construct_integer_vector(buf, pos, buf_size, row_cnt, obj_meta->is_signed_integer(), vector);
  }
  return ret;
}

int ObColumnVector::construct_datum_vector(
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    const int64_t row_cnt,
    ObColumnVector &vector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_size < pos + sizeof(ObStorageDatum) * row_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid buf size", K(ret), K(buf_size), K(pos), K(sizeof(ObStorageDatum)), K(row_cnt));
  } else {
    vector.type_ = ObColumnVectorType::DATUM_TYPE;
    vector.row_cnt_ = row_cnt;
    vector.datums_ = new (buf + pos) ObStorageDatum[row_cnt];
    pos += sizeof(ObStorageDatum) * row_cnt;
  }
  return ret;
}

int ObColumnVector::construct_integer_vector(
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    const int64_t row_cnt,
    const bool is_signed,
    ObColumnVector &vector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_size < pos + sizeof(int64_t) * row_cnt + sizeof(bool) * row_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid buf size", K(ret), K(buf_size), K(pos), K(row_cnt));
  } else {
    vector.type_ = is_signed ? ObColumnVectorType::SIGNED_INTEGER_TYPE : ObColumnVectorType::UNSIGNED_INTEGER_TYPE;
    vector.row_cnt_ = row_cnt;
    vector.signed_ints_ = reinterpret_cast<int64_t*>(buf + pos);
    pos += sizeof(int64_t) * row_cnt;
    vector.nulls_ = reinterpret_cast<bool*>(buf + pos);
    pos += sizeof(bool) * row_cnt;
    MEMSET(vector.nulls_, false, sizeof(bool) * row_cnt);
  }
  return ret;
}
int64_t ObColumnVector::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type),
       K_(has_null),
       K_(row_cnt),
       K_(is_filled),
       K_(flag),
       KP_(data),
       KP_(nulls));
  J_COMMA();
  if (ObColumnVectorType::SIGNED_INTEGER_TYPE == type_) {
    J_KV(K(ObArrayWrap<int64_t>(signed_ints_, row_cnt_)));
  } else if (ObColumnVectorType::UNSIGNED_INTEGER_TYPE == type_) {
    J_KV(K(ObArrayWrap<uint64_t>(unsigned_ints_, row_cnt_)));
  } else if (ObColumnVectorType::DATUM_TYPE == type_ && row_cnt_ > 0) {
    J_KV(K(datums_[0]));
  }
  J_OBJ_END();
  return pos;
}

// is_lower_bound = true: locate the first position which covers this rowkey
// is_lower_bound = false: locate the last position which covers this rowkey
int ObRowkeyVector::locate_key(
    const int64_t begin,
    const int64_t end,
    const ObDatumRowkey &rowkey,
    const ObStorageDatumUtils &datum_utils,
    int64_t &rowkey_idx,
    const bool is_lower_bound) const
{
  int ret = OB_SUCCESS;
  const int64_t cmp_cnt = MIN(col_cnt_, rowkey.get_datum_cnt());
  if (OB_UNLIKELY(datum_utils.get_rowkey_count() < cmp_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid datum utils", K(ret), K(datum_utils), K(cmp_cnt));
  } else if (cmp_cnt > 1 && is_datum_vectors_) {
    ObVectorDatumRowkeyComparor cmp(ret, cmp_cnt, datum_utils);
    const ObDiscreteDatumRowkey *first = discrete_rowkey_array_ + begin;
    const ObDiscreteDatumRowkey *last = discrete_rowkey_array_ + end;
    const ObDiscreteDatumRowkey *found = nullptr;
    if (is_lower_bound) {
      found = std::lower_bound(first, last, rowkey, cmp);
    } else {
      found = std::upper_bound(first, last, rowkey, cmp);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to locate key", K(ret), K(rowkey), K(*this));
    } else if (found == last) {
      rowkey_idx = end;
    } else {
      rowkey_idx = found - discrete_rowkey_array_;
    }
  } else {
    int64_t begin_idx = begin;
    int64_t end_idx = end;
    rowkey_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    const bool need_upper_bound = cmp_cnt > 1 || !is_lower_bound;
    const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
    const bool is_oracle_mode = datum_utils.is_oracle_mode();
    for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt; ++i) {
      const ObStorageDatum &key = rowkey.get_datum(i);
      if (key.is_min()) {
        rowkey_idx = begin_idx;
        break;
      } else if (key.is_max()) {
        rowkey_idx = end_idx;
        break;
      } else {
        ObColumnVector *vec = &columns_[i];
        if (OB_FAIL(vec->locate_key(need_upper_bound, begin_idx, end_idx, key, cmp_funcs.at(i), is_oracle_mode))) {
          LOG_WARN("Failed to locate key", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (begin_idx == end_idx) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && ObIMicroBlockReaderInfo::INVALID_ROW_INDEX == rowkey_idx) {
      if (is_lower_bound) {
        rowkey_idx = begin_idx;
      } else {
        rowkey_idx = end_idx;
      }
    }
  }
  return ret;
}

int ObRowkeyVector::locate_range(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    const bool is_normal_cg,
    const ObStorageDatumUtils &datum_utils,
    int64_t &begin_idx,
    int64_t &end_idx) const
{
  int ret = OB_SUCCESS;
  if (!is_left_border || range.get_start_key().is_min_rowkey()) {
    begin_idx = 0;
  } else if (OB_FAIL(locate_key(0, row_cnt_, range.get_start_key(), datum_utils, begin_idx))) {
    LOG_WARN("Failed to locate start key", K(ret));
  } else if (row_cnt_ == begin_idx) {
    ret = OB_BEYOND_THE_RANGE;
  } else if (!range.get_border_flag().inclusive_start()) {
    int cmp_ret = 0;
    if (OB_FAIL(compare_rowkey(range.get_start_key(), begin_idx, datum_utils, cmp_ret, false))) {
      LOG_WARN("Failed to compare rowkey", K(ret), K(begin_idx), K(range), K(datum_utils));
    } else if (0 == cmp_ret) {
      ++begin_idx;
      if (begin_idx == row_cnt_) {
        ret = OB_BEYOND_THE_RANGE;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_right_border || range.get_end_key().is_max_rowkey()) {
    end_idx = row_cnt_ - 1;
  } else if (OB_FAIL(locate_key(begin_idx, row_cnt_, range.get_end_key(), datum_utils,
                                end_idx, is_normal_cg || !range.get_border_flag().inclusive_end()))) {
    LOG_WARN("Failed to locate start key", K(ret));
  } else if (row_cnt_ == end_idx) {
    end_idx--;
  }
  return ret;
}

template<typename T>
OB_INLINE int compare_column_key(
    ObColumnVector &vector,
    const int64_t row_idx,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObRVIntegerWithNullComparor<int64_t> comparor(!is_oracle_mode);
  ObRVIntegerCell<int64_t> left(vector.signed_ints_[row_idx], vector.nulls_[row_idx]);
  ObRVIntegerCell<int64_t> right(key.is_null() ? 0 : key.get_int(), key.is_null());
  cmp_ret = comparor.compare(left, right);
  return ret;
}

template<>
OB_INLINE int compare_column_key<uint64_t>(
    ObColumnVector &vector,
    const int64_t row_idx,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObRVIntegerWithNullComparor<uint64_t> comparor(!is_oracle_mode);
  ObRVIntegerCell<uint64_t> left(vector.unsigned_ints_[row_idx], vector.nulls_[row_idx]);
  ObRVIntegerCell<uint64_t> right(key.is_null() ? 0 : key.get_uint(), key.is_null());
  cmp_ret = comparor.compare(left, right);
  return ret;
}

template<>
OB_INLINE int compare_column_key<ObStorageDatum>(
    ObColumnVector &vector,
    const int64_t row_idx,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cmp_func.compare(vector.datums_[row_idx], key, cmp_ret))) {
    LOG_WARN("Failed to compare", K(ret));
  }
  return ret;
}

typedef int (*compare_column_key_func) (
    ObColumnVector &vector,
    const int64_t row_idx,
    const ObStorageDatum &key,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret);
static compare_column_key_func COMPARE_COLUMN_KEY_FUNCS[(int8_t)ObColumnVectorType::MAX_TYPE] =
{ compare_column_key<ObStorageDatum>, compare_column_key<int64_t>, compare_column_key<uint64_t> };

int ObRowkeyVector::compare_rowkey(
    const ObDatumRowkey &rowkey,
    const int64_t row_idx,
    const ObStorageDatumUtils &datum_utils,
    int &cmp_ret,
    const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  const int64_t cmp_cnt = MIN(col_cnt_, rowkey.datum_cnt_);
  if (OB_UNLIKELY(row_idx >= row_cnt_ || !rowkey.is_valid() || !datum_utils.is_valid()
      || datum_utils.get_rowkey_count() < col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to compare", K(ret), K(row_idx), K(rowkey), K(row_cnt_),
             K(col_cnt_), K(datum_utils));
  } else if (is_datum_vectors_) {
    if (OB_FAIL(compare_datum_rowkey(rowkey, row_idx, datum_utils, cmp_cnt, cmp_ret, compare_datum_cnt))) {
      LOG_WARN("Failed to compare datum rowkey", K(ret));
    }
  } else {
    const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
    const bool is_oracle_mode = datum_utils.is_oracle_mode();
    cmp_ret = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt && 0 == cmp_ret; ++i) {
      if (rowkey.datums_[i].is_min()) {
        cmp_ret = 1;
      } else if (rowkey.datums_[i].is_max()) {
        cmp_ret = -1;
      } else {
        compare_column_key_func func = COMPARE_COLUMN_KEY_FUNCS[(int8_t)columns_[i].type_];
        if (OB_FAIL(func(columns_[i], row_idx, rowkey.datums_[i], cmp_funcs.at(i), is_oracle_mode, cmp_ret))) {
          LOG_WARN("Failed to compare key", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && 0 == cmp_ret && compare_datum_cnt) {
      cmp_ret = col_cnt_ - rowkey.datum_cnt_;
    }
  }
  return ret;
}

int ObRowkeyVector::compare_datum_rowkey(
    const ObDatumRowkey &rowkey,
    const int64_t row_idx,
    const ObStorageDatumUtils &datum_utils,
    const int64_t cmp_cnt,
    int &cmp_ret,
    const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
  cmp_ret = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt && 0 == cmp_ret; ++i) {
    if (OB_FAIL(cmp_funcs.at(i).compare(columns_[i].datums_[row_idx], rowkey.datums_[i], cmp_ret))) {
      LOG_WARN("Failed to compare key", K(ret), K(i), K(row_idx), K(rowkey));
    }
  }
  if (OB_SUCC(ret) && 0 == cmp_ret && compare_datum_cnt) {
    cmp_ret = col_cnt_ - rowkey.datum_cnt_;
  }
  return ret;
}

template<typename T>
int compare_column_row_idx(
    ObColumnVector &left_vector,
    ObColumnVector &right_vector,
    const int64_t left_row_idx,
    const int64_t right_row_idx,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObRVIntegerWithNullComparor<int64_t> comparor(!is_oracle_mode);
  ObRVIntegerCell<int64_t> left(left_vector.signed_ints_[left_row_idx], left_vector.nulls_[left_row_idx]);
  ObRVIntegerCell<int64_t> right(right_vector.signed_ints_[right_row_idx], right_vector.nulls_[right_row_idx]);
  cmp_ret = comparor.compare(left, right);
  return ret;
}

template<>
int compare_column_row_idx<uint64_t>(
    ObColumnVector &left_vector,
    ObColumnVector &right_vector,
    const int64_t left_row_idx,
    const int64_t right_row_idx,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObRVIntegerWithNullComparor<uint64_t> comparor(!is_oracle_mode);
  ObRVIntegerCell<uint64_t> left(left_vector.unsigned_ints_[left_row_idx], left_vector.nulls_[left_row_idx]);
  ObRVIntegerCell<uint64_t> right(right_vector.unsigned_ints_[right_row_idx], right_vector.nulls_[right_row_idx]);
  cmp_ret = comparor.compare(left, right);
  return ret;
}

template<>
int compare_column_row_idx<ObStorageDatum>(
    ObColumnVector &left_vector,
    ObColumnVector &right_vector,
    const int64_t left_row_idx,
    const int64_t right_row_idx,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cmp_func.compare(left_vector.datums_[left_row_idx], right_vector.datums_[right_row_idx], cmp_ret))) {
    LOG_WARN("Failed to compare", K(ret));
  }
  return ret;
}

template<typename T>
int compare_column_datum_row_idx(
    ObColumnVector &left_vector,
    ObStorageDatum &right_datum,
    const int64_t left_row_idx,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObRVIntegerWithNullComparor<int64_t> comparor(!is_oracle_mode);
  ObRVIntegerCell<int64_t> left(left_vector.signed_ints_[left_row_idx], left_vector.nulls_[left_row_idx]);
  ObRVIntegerCell<int64_t> right(right_datum.get_int(), right_datum.is_null());
  cmp_ret = comparor.compare(left, right);
  return ret;
}

template<>
int compare_column_datum_row_idx<uint64_t>(
    ObColumnVector &left_vector,
    ObStorageDatum &right_datum,
    const int64_t left_row_idx,
    const bool is_oracle_mode,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObRVIntegerWithNullComparor<uint64_t> comparor(!is_oracle_mode);
  ObRVIntegerCell<uint64_t> left(left_vector.unsigned_ints_[left_row_idx], left_vector.nulls_[left_row_idx]);
  ObRVIntegerCell<uint64_t> right(right_datum.get_uint(), right_datum.is_null());
  cmp_ret = comparor.compare(left, right);
  return ret;
}

typedef int (*compare_column_value_func) (
    ObColumnVector &left_vector,
    ObColumnVector &right_vector,
    const int64_t left_row_idx,
    const int64_t right_row_idx,
    const ObStorageDatumCmpFunc &cmp_func,
    const bool is_oracle_mode,
    int &cmp_ret);
static compare_column_value_func COMPARE_COLUMN_VALUE_FUNCS[(int8_t)ObColumnVectorType::MAX_TYPE + 1] =
{ compare_column_row_idx<ObStorageDatum>, compare_column_row_idx<int64_t>, compare_column_row_idx<uint64_t> };

typedef int (*compare_column_value_datum_func) (
    ObColumnVector &left_vector,
    ObStorageDatum &right_datum,
    const int64_t left_row_idx,
    const bool is_oracle_mode,
    int &cmp_ret);
static compare_column_value_datum_func COMPARE_COLUMN_VALUE_DATUM_FUNCS[(int8_t)ObColumnVectorType::MAX_TYPE + 1] =
{ nullptr, compare_column_datum_row_idx<int64_t>, compare_column_datum_row_idx<uint64_t> };

int ObRowkeyVector::compare_rowkey(
    const ObDiscreteDatumRowkey &rowkey,
    const int64_t row_idx,
    const ObStorageDatumUtils &datum_utils,
    int &cmp_ret,
    const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_idx >= row_cnt_ || !rowkey.is_valid() || !datum_utils.is_valid()
      || datum_utils.get_rowkey_count() < col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to compare", K(ret), K(row_idx), K(rowkey), K(row_cnt_),
             K(col_cnt_), K(datum_utils));
  } else if (OB_UNLIKELY(col_cnt_ != rowkey.rowkey_vector_->col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid rowkey vector to compare, col cnt is not equal", K(ret), K(col_cnt_), K(rowkey));
  } else if (this == rowkey.rowkey_vector_ && row_idx == rowkey.row_idx_) {
    cmp_ret = 0;
  } else {
    const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
    const bool is_oracle_mode = datum_utils.is_oracle_mode();
    cmp_ret = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_ && 0 == cmp_ret; ++i) {
      if (OB_LIKELY(columns_[i].type_ == rowkey.rowkey_vector_->columns_[i].type_)) {
        compare_column_value_func func = COMPARE_COLUMN_VALUE_FUNCS[(int8_t)columns_[i].type_];
        if (OB_FAIL(func(columns_[i], rowkey.rowkey_vector_->columns_[i], row_idx, rowkey.row_idx_, cmp_funcs.at(i), is_oracle_mode, cmp_ret))) {
          LOG_WARN("Failed to compare key", K(ret));
        }
      } else if (ObColumnVectorType::DATUM_TYPE == rowkey.rowkey_vector_->columns_[i].type_) {
        compare_column_value_datum_func func = COMPARE_COLUMN_VALUE_DATUM_FUNCS[(int8_t)columns_[i].type_];
        if (OB_FAIL(func(columns_[i], rowkey.rowkey_vector_->columns_[i].datums_[rowkey.row_idx_], row_idx, is_oracle_mode, cmp_ret))) {
          LOG_WARN("Failed to compare key", K(ret));
        }
      } else if (ObColumnVectorType::DATUM_TYPE == columns_[i].type_) {
        compare_column_value_datum_func func = COMPARE_COLUMN_VALUE_DATUM_FUNCS[(int8_t)rowkey.rowkey_vector_->columns_[i].type_];
        if (OB_FAIL(func(rowkey.rowkey_vector_->columns_[i], columns_[i].datums_[row_idx], rowkey.row_idx_, is_oracle_mode, cmp_ret))) {
          LOG_WARN("Failed to compare key", K(ret));
        } else {
          cmp_ret = -cmp_ret;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected not equal column type", K(ret), K(i), K(columns_[i]), K(rowkey.rowkey_vector_->columns_[i]));
      }
    }
  }
  return ret;
}

template<typename T>
void extract_datum_value(ObColumnVector &vector, const int64_t row_idx, ObStorageDatum &datum)
{
  datum.reuse();
  if (vector.nulls_[row_idx]) {
    datum.set_null();
  } else {
    datum.set_int(vector.signed_ints_[row_idx]);
  }
}

template<>
void extract_datum_value<ObStorageDatum>(ObColumnVector &vector, const int64_t row_idx, ObStorageDatum &datum)
{
  datum.shallow_copy_from_datum(vector.datums_[row_idx]);
}

typedef void (*extract_datum_value_func) (ObColumnVector &vector, const int64_t row_idx, ObStorageDatum &datum);
static extract_datum_value_func EXTRACT_DATUM_VALUE_FUNCS[(int8_t)ObColumnVectorType::MAX_TYPE] =
{ extract_datum_value<ObStorageDatum>, extract_datum_value<int64_t>, extract_datum_value<int64_t> };

int ObRowkeyVector::get_rowkey(const int64_t row_idx, ObDatumRowkey &rowkey) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_idx >= row_cnt_ || !rowkey.is_valid() || rowkey.get_datum_cnt() < col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpeted row idx", K(ret), K(row_idx), K(rowkey.get_datum_cnt()), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; ++i) {
      if (OB_UNLIKELY(!is_valid_vector_type(columns_[i].type_))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid row idx", K(ret), K(i), K(columns_[i]));
      } else {
        extract_datum_value_func func = EXTRACT_DATUM_VALUE_FUNCS[(int8_t)columns_[i].type_];
        func(columns_[i], row_idx, rowkey.datums_[i]);
      }
    }
  }
  return ret;
}

int ObRowkeyVector::get_rowkey(const int64_t row_idx, ObCommonDatumRowkey &common_rowkey) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_idx >= row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpeted row idx", K(ret), K(row_idx), KPC(this));
  } else {
    common_rowkey.set_discrete_rowkey(&discrete_rowkey_array_[row_idx]);
  }
  return ret;
}

int ObRowkeyVector::get_deep_copy_rowkey_size(const int64_t row_idx, int64_t &size) const
{
  int ret = OB_SUCCESS;
  int64_t column_size = 0;
  size = col_cnt_ * sizeof(ObStorageDatum);
  for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; ++i) {
    if (OB_FAIL(columns_[i].get_deep_copy_size(row_idx, column_size))) {
      LOG_WARN("Failed to get column datum size", K(ret), K(row_idx));
    } else {
      size += column_size;
    }
  }
  return ret;
}

int ObRowkeyVector::deep_copy_rowkey(const int64_t row_idx, ObDatumRowkey &dest, char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to deep copy datum rowkey", K(ret), KP(buf), K(buf_size));
  } else {
    ObStorageDatum *datums = new (buf) ObStorageDatum[col_cnt_];
    int64_t pos = sizeof(ObStorageDatum) * col_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; i++) {
      if (OB_FAIL(columns_[i].get_column_datum(row_idx, datums[i], buf, buf_size, pos))) {
        STORAGE_LOG(WARN, "Failed to get column datum", K(ret), K(i), K(*this));
      }
    }
    if (OB_SUCC(ret)) {
      dest.datums_ = datums;
      dest.datum_cnt_ = col_cnt_;
    }
  }
  return ret;
}

int ObRowkeyVector::get_column_int(const int64_t row_idx, const int64_t col_idx, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_idx >= col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpeted col idx", K(ret), K(col_idx), KPC(this));
  } else {
    ret = columns_[col_idx].get_column_int(row_idx, int_val);
  }
  return ret;
}

int ObRowkeyVector::fill_last_rowkey()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_cnt_ <= 0 || nullptr == last_rowkey_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected rowkey vector", K(ret), KPC(this));
  } else if (OB_FAIL(get_rowkey(row_cnt_ - 1, *last_rowkey_))) {
    LOG_WARN("Failed to get rowkey", K(ret), KPC(this));
  }
  return ret;
}

int ObRowkeyVector::set_construct_finished()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_last_rowkey())) {
    LOG_WARN("Failed to fill last rowkey", K(ret));
  } else {
    for (int64_t i = 0; i < col_cnt_; ++i) {
      columns_[i].is_filled_ = true;
    }
  }
  return ret;
}

int ObRowkeyVector::deep_copy(
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    const ObRowkeyVector &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf ||
      buf_size < pos + sizeof(ObColumnVector) * other.col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid buf", K(ret), KP(buf), K(buf_size), K(pos), K(sizeof(ObColumnVector)), K(other.col_cnt_));
  } else {
    flag_ = other.flag_;
    columns_ = new (buf + pos) ObColumnVector[col_cnt_];
    pos += sizeof(ObColumnVector) * col_cnt_;
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_cnt_; ++col_idx) {
      if (OB_FAIL(columns_[col_idx].deep_copy(buf,
                                              pos,
                                              buf_size,
                                              other.columns_[col_idx]))) {
        LOG_WARN("Failed to deep copy column vector", K(ret), K(col_idx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_rowkeys_buffer(row_cnt_, col_cnt_, buf, pos, buf_size, this))) {
      LOG_WARN("Failed to prepare rowkeys buffer", K(ret));
    }  else if (OB_FAIL(fill_last_rowkey())) {
      LOG_WARN("Failed to fill last rowkey", K(ret));
    }
  }
  return ret;
}

int ObRowkeyVector::get_occupied_size(
    const int64_t row_cnt,
    const int64_t col_cnt,
    const ObIArray<share::schema::ObColDesc> *col_descs,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != col_descs &&
      col_cnt > col_descs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected rowkey count", K(ret), K(col_cnt), K(col_descs->count()));
  } else {
    size = 0;
    size += sizeof(ObRowkeyVector);
    size += col_cnt * sizeof(ObColumnVector);
    if (is_all_integer_cols(col_cnt, col_descs)) {
      size += col_cnt * row_cnt * (sizeof(int64_t) + sizeof(bool));
    } else {
      size += col_cnt * row_cnt * sizeof(ObStorageDatum);
    }
    size += sizeof(ObDatumRowkey);
    size += sizeof(ObStorageDatum) * col_cnt;
    size += sizeof(ObDiscreteDatumRowkey) * row_cnt;
  }
  return ret;
}

int ObRowkeyVector::construct_rowkey_vector(
    const int64_t row_cnt,
    const int64_t col_cnt,
    const ObIArray<share::schema::ObColDesc> *col_descs,
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    ObRowkeyVector *&rowkey_vector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf ||
      buf_size < pos + sizeof(ObRowkeyVector) + sizeof(ObColumnVector) * col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid buf", K(ret), KP(buf), K(buf_size), K(pos), K(sizeof(ObRowkeyVector)),
             K(sizeof(ObColumnVector)), K(col_cnt));
  } else {
    ObRowkeyVector * tmp_rowkey_vector = new (buf + pos) ObRowkeyVector();
    tmp_rowkey_vector->col_cnt_ = col_cnt;
    tmp_rowkey_vector->row_cnt_ = row_cnt;
    pos += sizeof(ObRowkeyVector);
    tmp_rowkey_vector->columns_ = new (buf + pos) ObColumnVector[col_cnt];
    pos += sizeof(ObColumnVector) * col_cnt;
    const bool all_cols_is_integer = is_all_integer_cols(col_cnt, col_descs);
    const ObObjMeta *obj_meta = nullptr;
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_cnt; ++col_idx) {
      ObColumnVector &vector = tmp_rowkey_vector->columns_[col_idx];
      obj_meta = all_cols_is_integer ? &col_descs->at(col_idx).col_type_ : nullptr;
      if (OB_FAIL(ObColumnVector::construct_column_vector(buf, pos, buf_size, row_cnt, obj_meta, vector))) {
        LOG_WARN("Failed to construct column vector", K(ret), K(col_idx), KPC(obj_meta));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(tmp_rowkey_vector->is_datum_vectors_ = !all_cols_is_integer)) {
    } else if (OB_FAIL(prepare_rowkeys_buffer(row_cnt, col_cnt, buf, pos, buf_size, tmp_rowkey_vector))) {
      LOG_WARN("Failed to prepare rowkeys buffer", K(ret));
    } else {
      rowkey_vector = tmp_rowkey_vector;
    }
  }
  return ret;
}

int ObRowkeyVector::prepare_rowkeys_buffer(
    const int64_t row_cnt,
    const int64_t col_cnt,
    char *buf,
    int64_t &pos,
    const int64_t buf_size,
    ObRowkeyVector *rowkey_vector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_size < pos + sizeof(ObDatumRowkey) + sizeof(ObStorageDatum) * col_cnt
                             + sizeof(ObDiscreteDatumRowkey) * row_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid buf", K(ret), K(buf_size), K(pos), K(sizeof(ObDatumRowkey)), K(sizeof(ObStorageDatum)),
              K(col_cnt), K(sizeof(ObDiscreteDatumRowkey)), K(row_cnt));
  } else {
    rowkey_vector->last_rowkey_ = new (buf + pos) ObDatumRowkey();
    pos += sizeof(ObDatumRowkey);
    ObStorageDatum *datums = new (buf + pos) ObStorageDatum[col_cnt];
    pos += sizeof(ObStorageDatum) * col_cnt;
    if (OB_FAIL(rowkey_vector->last_rowkey_->assign(datums, col_cnt))) {
      LOG_WARN("Failed to assign", K(ret));
    } else {
      rowkey_vector->discrete_rowkey_array_ = new (buf + pos) ObDiscreteDatumRowkey[row_cnt];
      pos += sizeof(ObDiscreteDatumRowkey) * row_cnt;
      for (int64_t i = 0; i < row_cnt; ++i) {
        rowkey_vector->discrete_rowkey_array_[i].row_idx_ = i;
        rowkey_vector->discrete_rowkey_array_[i].rowkey_vector_ = rowkey_vector;
      }
    }
  }
  return ret;
}

bool ObRowkeyVector::is_all_integer_cols(const int64_t col_cnt, const ObIArray<share::schema::ObColDesc> *col_descs)
{
  bool is_all_integer_cols = false;
  if (nullptr != col_descs) {
    is_all_integer_cols = true;
    for (int64_t col_idx = 0; is_all_integer_cols && col_idx < col_cnt; ++col_idx) {
      is_all_integer_cols = col_descs->at(col_idx).col_type_.is_integer_type();
    }
  }
  return is_all_integer_cols;
}

int64_t ObRowkeyVector::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(col_cnt),
       K_(row_cnt),
       K_(is_datum_vectors),
       KP_(columns),
       KP_(last_rowkey),
       KP_(discrete_rowkey_array));
  J_COMMA();
  if (nullptr != last_rowkey_ && last_rowkey_->is_valid()) {
    J_KV(KPC(last_rowkey_));
  }
  J_OBJ_END();
  return pos;
}

} // namespace blocksstable
} // namespace oceanbase
