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

#ifndef OB_STORAGE_BLOCKSSTABLE_DATUM_RANGE_H
#define OB_STORAGE_BLOCKSSTABLE_DATUM_RANGE_H

#include "common/ob_store_range.h"
#include "ob_datum_rowkey.h"
#include "lib/utility/ob_print_kv.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObDatumRange
{
public:
  ObDatumRange() : start_key_(), end_key_(), group_idx_(0), border_flag_() { }
  ~ObDatumRange() = default;
  OB_INLINE void reset();
  OB_INLINE bool is_valid() const;
  OB_INLINE bool is_memtable_valid() const;
  OB_INLINE const ObDatumRowkey& get_start_key() const { return start_key_; }
  OB_INLINE const ObDatumRowkey& get_end_key() const { return end_key_; }
  OB_INLINE const ObBorderFlag& get_border_flag() const { return border_flag_; }
  OB_INLINE int64_t get_group_idx() const { return group_idx_; }
  OB_INLINE void set_inclusive(ObBorderFlag flag) { border_flag_.set_inclusive(flag.get_data()); }
  OB_INLINE void set_border_flag(ObBorderFlag flag) { border_flag_ = flag; }
  OB_INLINE void set_start_key(const ObDatumRowkey &start_key) { start_key_ = start_key; }
  OB_INLINE void set_end_key(const ObDatumRowkey &end_key) { end_key_ = end_key; }
  OB_INLINE void set_group_idx(const int64_t group_idx) { group_idx_ = group_idx; }

  OB_INLINE bool is_left_open() const { return !border_flag_.inclusive_start(); }
  OB_INLINE bool is_left_closed() const { return border_flag_.inclusive_start(); }
  OB_INLINE bool is_right_open() const { return !border_flag_.inclusive_end(); }
  OB_INLINE bool is_right_closed() const { return border_flag_.inclusive_end(); }
  OB_INLINE void set_left_open() { border_flag_.unset_inclusive_start(); }
  OB_INLINE void set_left_closed() { border_flag_.set_inclusive_start(); }
  OB_INLINE void set_right_open() { border_flag_.unset_inclusive_end(); }
  OB_INLINE void set_right_closed() { border_flag_.set_inclusive_end(); }
  OB_INLINE void set_whole_range();
  OB_INLINE bool is_whole_range() const { return start_key_.is_min_rowkey() && end_key_.is_max_rowkey(); }
  OB_INLINE int is_single_rowkey(const ObStorageDatumUtils &datum_utils, bool &is_single) const;
  OB_INLINE void change_boundary(const ObDatumRowkey &rowkey, bool is_reverse);
  OB_INLINE int from_range(const common::ObStoreRange &range, ObIAllocator &allocator);
  OB_INLINE int from_range(const common::ObNewRange &range, ObIAllocator &allocator);
  OB_INLINE int to_store_range(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                              common::ObIAllocator &allocator,
                              common::ObStoreRange &store_range) const;
  OB_INLINE int to_multi_version_range(common::ObIAllocator &allocator, ObDatumRange &dest) const;
  OB_INLINE int prepare_memtable_readable(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                          common::ObIAllocator &allocator);
  // !!Attension only compare start key
  OB_INLINE int compare(const ObDatumRange &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret) const;
  // maybe we will need serialize
  // NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(start_key), K_(end_key), K_(group_idx), K_(border_flag));
public:
  ObDatumRowkey start_key_;
  ObDatumRowkey end_key_;
  int64_t group_idx_;
  //TODO maybe we should use a new border flag
  common::ObBorderFlag border_flag_;
};

template<typename T>
struct ObDatumComparor
{
  ObDatumComparor(
      const ObStorageDatumUtils &datum_utils,
      int &ret,
      bool reverse = false,
      bool lower_bound = true,
      bool compare_datum_cnt = true)
    : datum_utils_(datum_utils), ret_(ret), reverse_(reverse), lower_bound_(lower_bound),
      compare_datum_cnt_(compare_datum_cnt)
  {}
  ObDatumComparor() = delete;
  ~ObDatumComparor() = default;
  OB_INLINE bool operator()(const T &left, const T &right)
  {
    return compare<T>(left, right);
  }
  OB_INLINE bool is_reverse() const
  {
    return reverse_;
  }
  OB_INLINE bool is_lower_bound() const
  {
    return lower_bound_;
  }
  OB_INLINE bool is_compare_datum_cnt() const
  {
    return compare_datum_cnt_;
  }
  OB_INLINE const ObStorageDatumUtils &get_datum_utils() const
  {
    return datum_utils_;
  }
private:
  template <typename DataType>
  OB_INLINE bool compare(const DataType &left, const DataType &right)
  {
    int &ret = ret_;
    bool bret = false;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else if (lower_bound_ || reverse_) {
      if (OB_FAIL(left.compare(right, datum_utils_, cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare datum rowkey or range", K(ret), K(left), K(right));
      } else {
        bret = reverse_ ? cmp_ret > 0 : cmp_ret < 0;
      }
    } else if (OB_FAIL(right.compare(left, datum_utils_, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare datum rowkey or range", K(ret), K(left), K(right));
    } else {
      bret = cmp_ret > 0;
    }
    return bret;
  }

  template <>
  OB_INLINE bool compare<ObDatumRowkey>(const ObDatumRowkey &left, const ObDatumRowkey &right)
  {
    int &ret = ret_;
    bool bret = false;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else if (lower_bound_ || reverse_) {
      if (OB_FAIL(left.compare(right, datum_utils_, cmp_ret, compare_datum_cnt_))) {
        STORAGE_LOG(WARN, "Failed to compare datum rowkey or range", K(ret), K(left), K(right));
      } else {
        bret = reverse_ ? cmp_ret > 0 : cmp_ret < 0;
      }
    } else if (OB_FAIL(right.compare(left, datum_utils_, cmp_ret, compare_datum_cnt_))) {
      STORAGE_LOG(WARN, "Failed to compare datum rowkey or range", K(ret), K(left), K(right));
    } else {
      bret = cmp_ret > 0;
    }
    return bret;
  }
private:
  const ObStorageDatumUtils &datum_utils_;
  int &ret_;
  bool reverse_;
  bool lower_bound_;
  bool compare_datum_cnt_;
};

OB_INLINE void ObDatumRange::reset()
{
  start_key_.reset();
  end_key_.reset();
  group_idx_ = 0;
  border_flag_.set_data(0);
}

//TODO without rowkey type, we cannot judge empty
OB_INLINE bool ObDatumRange::is_valid() const
{
  return start_key_.is_valid() && end_key_.is_valid();
}

OB_INLINE bool ObDatumRange::is_memtable_valid() const
{
  return start_key_.is_memtable_valid() && end_key_.is_memtable_valid();
}

OB_INLINE void ObDatumRange::set_whole_range()
{
  start_key_.set_min_rowkey();
  end_key_.set_max_rowkey();
  group_idx_= 0;
  border_flag_.set_all_open();
}

OB_INLINE int ObDatumRange::is_single_rowkey(const ObStorageDatumUtils &datum_utils, bool &is_single) const
{
  int ret = OB_SUCCESS;

  is_single = false;
  if (!border_flag_.inclusive_start() || !border_flag_.inclusive_end()) {
  } else if (start_key_.is_ext_rowkey()) {
  } else if (OB_FAIL(start_key_.equal(end_key_, datum_utils, is_single))) {
    STORAGE_LOG(WARN, "Failed to check datum rowkey equal", K(ret), K(*this));
  }

  return ret;
}

OB_INLINE int ObDatumRange::from_range(const common::ObNewRange &range, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  //we should not defend the range valid
  if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to ", K(ret), K(range));
  } else if (OB_FAIL(start_key_.from_rowkey(range.get_start_key(), allocator))) {
    STORAGE_LOG(WARN, "Failed to from start key", K(ret));
  } else if (OB_FAIL(end_key_.from_rowkey(range.get_end_key(), allocator))) {
    STORAGE_LOG(WARN, "Failed to from end key", K(ret));
  } else {
    border_flag_ = range.border_flag_;
    group_idx_ = range.get_group_id();
  }

  return ret;
}

OB_INLINE int ObDatumRange::from_range(const common::ObStoreRange &range, ObIAllocator &allocator)
{
  ObNewRange new_range;
  range.to_new_range(new_range);
  return from_range(new_range, allocator);
}

OB_INLINE int ObDatumRange::to_store_range(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                              common::ObIAllocator &allocator,
                              common::ObStoreRange &store_range) const
{
  int ret = OB_SUCCESS;

  store_range.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer to store range", K(ret), K(*this));
  } else if (OB_FAIL(start_key_.to_store_rowkey(col_descs, allocator, store_range.get_start_key()))) {
    STORAGE_LOG(WARN, "Failed to transfer start key", K(ret), K(start_key_));
  } else if (OB_FAIL(end_key_.to_store_rowkey(col_descs, allocator, store_range.get_end_key()))) {
    STORAGE_LOG(WARN, "Failed to transfer end key", K(ret), K(end_key_));
  } else {
    store_range.set_border_flag(border_flag_);
    store_range.set_group_idx(group_idx_);
  }

  return ret;
}

OB_INLINE int ObDatumRange::compare(const ObDatumRange &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to compa datum range", K(ret), K(*this), K(rhs));
  } else {
    ret = start_key_.compare(rhs.get_start_key(), datum_utils, cmp_ret);
  }
  return ret;
}

OB_INLINE void ObDatumRange::change_boundary(const ObDatumRowkey &rowkey, bool is_reverse)
{
  if (is_reverse) {
    end_key_ = rowkey;
    border_flag_.unset_inclusive_end();
  }  else {
    start_key_ = rowkey;
    border_flag_.unset_inclusive_start();
  }
}

OB_INLINE int ObDatumRange::to_multi_version_range(common::ObIAllocator &allocator, ObDatumRange &dest) const
{
  int ret = OB_SUCCESS;
  const bool include_start = get_border_flag().inclusive_start();
  const bool include_end = get_border_flag().inclusive_end();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer multi version range", K(ret), K(*this));
  } else if (OB_FAIL(start_key_.to_multi_version_rowkey(include_start, allocator, dest.start_key_))) {
    STORAGE_LOG(WARN, "Failed to transfer multi version rowkey", K(ret), K(include_start), K_(start_key));
  } else if (OB_FAIL(end_key_.to_multi_version_rowkey(!include_end, allocator, dest.end_key_))) {
    STORAGE_LOG(WARN, "Failed to transfer multi version rowkey", K(ret), K(include_end), K_(end_key));
  } else {
    dest.border_flag_ = border_flag_;
    dest.group_idx_ = group_idx_;
  }

  return ret;
}

OB_INLINE int ObDatumRange::prepare_memtable_readable(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                          common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_key_.prepare_memtable_readable(col_descs, allocator))) {
    STORAGE_LOG(WARN, "Failed to prepare start key", K(ret), K(start_key_), K(col_descs));
  } else if (OB_FAIL(end_key_.prepare_memtable_readable(col_descs, allocator))) {
    STORAGE_LOG(WARN, "Failed to prepare end key", K(ret), K(end_key_), K(col_descs));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
#endif
