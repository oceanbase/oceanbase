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

#ifndef OB_STORAGE_BLOCKSSTABLE_DATUM_ROWKEY_H
#define OB_STORAGE_BLOCKSSTABLE_DATUM_ROWKEY_H

#include "ob_datum_row.h"
#include "lib/utility/ob_print_kv.h"
//to be removed
#include "common/rowkey/ob_store_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRange;

struct ObDatumRowkey
{
  OB_UNIS_VERSION(1);
public:
  ObDatumRowkey() { reset(); }
  ObDatumRowkey(ObStorageDatum *datum, const int64_t datum_cnt);
  ObDatumRowkey(ObStorageDatumBuffer &datum_buffer);
  ~ObDatumRowkey() = default;
  OB_INLINE void reset() { MEMSET(this, 0, sizeof(ObDatumRowkey)); }
  void destroy(ObIAllocator &allocator);
  OB_INLINE int assign(ObStorageDatum *datums, const int datum_cnt);
  OB_INLINE bool is_valid() const { return nullptr != datums_ && datum_cnt_ > 0; }
  OB_INLINE bool is_memtable_valid() const { return store_rowkey_.is_valid() && is_valid(); }
  OB_INLINE int32_t get_datum_cnt() const { return datum_cnt_; }
  OB_INLINE const ObStorageDatum *get_datum_ptr() const { return datums_; }
  OB_INLINE const ObStorageDatum& get_datum(const int64_t idx) const { OB_ASSERT(idx < datum_cnt_); return datums_[idx]; }
  OB_INLINE int64_t get_deep_copy_size() const;
  OB_INLINE int deep_copy(ObDatumRowkey &dest, common::ObIAllocator &allocator) const;
  OB_INLINE int deep_copy(ObDatumRowkey &dest, char *buf, const int64_t buf_len) const;
  OB_INLINE int shallow_copy(ObDatumRowkey &dest) const;
  OB_INLINE int semi_copy(ObDatumRowkey &dest, common::ObIAllocator &allocator) const;
  int murmurhash(const uint64_t seed, const ObStorageDatumUtils &datum_utils, uint64_t &hash) const;
  OB_INLINE int hash(const ObStorageDatumUtils &datum_utils, uint64_t &hash) const { return murmurhash(hash, datum_utils, hash); }
  static int ext_safe_compare(const ObStorageDatum &left, const ObStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret);

  OB_INLINE void set_max_rowkey() { *this = MAX_ROWKEY; store_rowkey_.set_max(); }
  OB_INLINE void set_min_rowkey() { *this = MIN_ROWKEY; store_rowkey_.set_min(); }
  OB_INLINE bool is_static_rowkey() const { return datums_ == &MIN_DATUM || datums_ == &MAX_DATUM; }
  OB_INLINE void set_group_idx(const int32_t group_idx) { group_idx_ = group_idx; }
  OB_INLINE int64_t get_group_idx() const { return group_idx_; }
  OB_INLINE const common::ObStoreRowkey &get_store_rowkey() const { return store_rowkey_; }
  //only for unittest
  OB_INLINE bool operator==(const ObDatumRowkey &other) const;

  #define DEF_ROWKEY_TYPE_FUNCS(FUNC_NAME, DATUM_TYPE)   \
    OB_INLINE bool FUNC_NAME() const                     \
    {                                                    \
      bool bret = is_valid();                            \
      for (int64_t i = 0; bret && i < datum_cnt_; i++) { \
        bret = datums_[i].is_##DATUM_TYPE();             \
      }                                                  \
      return bret;                                       \
    }
  DEF_ROWKEY_TYPE_FUNCS(is_max_rowkey, max);
  DEF_ROWKEY_TYPE_FUNCS(is_min_rowkey, min);
  DEF_ROWKEY_TYPE_FUNCS(is_ext_rowkey, ext);
  #undef DEF_ROWKEY_TYPE_FUNCS

  int equal(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, bool &is_equal) const;
  int compare(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
              const bool compare_datum_cnt = true) const;
  int from_rowkey(const ObRowkey &rowkey, common::ObIAllocator &allocator);
  int from_rowkey(const ObRowkey &rowkey, ObStorageDatumBuffer &datum_buffer);
  int to_store_rowkey(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                      common::ObIAllocator &allocator,
                      common::ObStoreRowkey &store_rowkey) const;
  int to_multi_version_rowkey(const bool min_value, common::ObIAllocator &allocator, ObDatumRowkey &dest) const;
  int to_multi_version_range(common::ObIAllocator &allocator, ObDatumRange &dest) const;
  OB_INLINE int prepare_memtable_readable(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                          common::ObIAllocator &allocator)
  { return to_store_rowkey(col_descs, allocator, store_rowkey_); }
  void reuse();
  DECLARE_TO_STRING;
public:
  int32_t datum_cnt_;
  int32_t group_idx_;
  mutable uint64_t hash_;
  ObStorageDatum *datums_;
  common::ObStoreRowkey store_rowkey_;
public:
  static ObDatumRowkey MIN_ROWKEY;
  static ObDatumRowkey MAX_ROWKEY;
  static ObStorageDatum MIN_DATUM;
  static ObStorageDatum MAX_DATUM;
};


struct ObDatumRowkeyHelper
{
public:
  ObDatumRowkeyHelper()
    : local_allocator_(), allocator_(&local_allocator_), datum_buffer_(&local_allocator_), obj_buffer_()
   {}
  ObDatumRowkeyHelper(common::ObIAllocator &allocator)
    : local_allocator_(), allocator_(&allocator), datum_buffer_(&allocator), obj_buffer_()
   {}
  ~ObDatumRowkeyHelper() {}
  int convert_datum_rowkey(const common::ObRowkey &rowkey, ObDatumRowkey &datum_rowkey);
  int convert_store_rowkey(const ObDatumRowkey &datum_rowkey,
                           const common::ObIArray<share::schema::ObColDesc> &col_descs,
                           common::ObStoreRowkey &rowkey);
  int reserve(const int64_t rowkey_cnt);
  OB_INLINE ObStorageDatum *get_datums() { return datum_buffer_.get_datums(); }
  OB_INLINE int64_t get_capacity() const { return datum_buffer_.get_capacity(); }
  TO_STRING_KV(K_(datum_buffer));
private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator *allocator_;
  ObStorageDatumBuffer datum_buffer_;
  storage::ObObjBufArray obj_buffer_;
};

struct ObDatumRowkeyWrapper
{
public:
  ObDatumRowkeyWrapper() : rowkey_(nullptr), datum_utils_(nullptr)
  {}
  ObDatumRowkeyWrapper(const ObDatumRowkey *rowkey, const ObStorageDatumUtils *datum_utils)
    : rowkey_(rowkey), datum_utils_(datum_utils)
  {}
  bool is_valid() const { return nullptr != rowkey_ && nullptr != datum_utils_; }
  const ObDatumRowkey *get_rowkey() const { return rowkey_; }
  int compare(const ObDatumRowkeyWrapper &other, int &cmp) const { return rowkey_->compare(*(other.get_rowkey()), *datum_utils_, cmp); }
  const ObStorageDatum *get_ptr() const { return rowkey_->get_datum_ptr(); }
  const char *repr() const { return to_cstring(rowkey_); }
  TO_STRING_KV(KPC_(rowkey), KPC_(datum_utils));
  const ObDatumRowkey *rowkey_;
  const ObStorageDatumUtils *datum_utils_;
};


/*
 *ObDatumRowkey
 */
OB_INLINE int ObDatumRowkey::assign(ObStorageDatum *datums, const int datum_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == datums || datum_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to assign datum rowkey", K(ret), KP(datums), K(datum_cnt));
  } else {
    reset();
    datums_ = datums;
    datum_cnt_ = datum_cnt;
  }

  return ret;
}

// deep copy size only include the datums
OB_INLINE int64_t ObDatumRowkey::get_deep_copy_size() const
{
  int64_t size = 0;
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    int ret = common::OB_INVALID_DATA;
    STORAGE_LOG(ERROR, "illegal datum rowkey to get deep copy size", K(ret), K(*this));
  } else {
    size = datum_cnt_ * sizeof(ObStorageDatum);
    for (int64_t i = 0; i < datum_cnt_; ++i) {
      size += datums_[i].get_deep_copy_size();
    }
  }

  return size;
}

OB_INLINE int ObDatumRowkey::deep_copy(ObDatumRowkey &dest, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum rowkey", K(ret), KP(buf), K(buf_len));
  } else {
    ObStorageDatum *datums = new (buf) ObStorageDatum[datum_cnt_];
    int64_t pos = sizeof(ObStorageDatum) * datum_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      if (OB_FAIL(datums[i].deep_copy(datums_[i], buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "Failed to deep copy storage datum", K(ret), K(i), K(*this));
      }
    }
    if (OB_SUCC(ret)) {
      dest.datums_ = datums;
      dest.hash_ = hash_;
      dest.group_idx_ = group_idx_;
      dest.datum_cnt_ = datum_cnt_;
      dest.store_rowkey_.reset();
    }
  }

  return ret;
}

OB_INLINE int ObDatumRowkey::deep_copy(ObDatumRowkey &dest, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  int64_t deep_copy_size = get_deep_copy_size();
  char *buf = nullptr;

  if (OB_UNLIKELY(!is_valid() || 0 == deep_copy_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for deep copy invalid datum rowkey", K(ret), K(*this));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(deep_copy_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for datum rowkey", K(ret), K(deep_copy_size));
  } else if (OB_FAIL(deep_copy(dest, buf, deep_copy_size))) {
    STORAGE_LOG(WARN, "Failed to deep copy datum rowkey", K(ret));
  }

  if (OB_FAIL(ret) && nullptr != buf) {
    dest.reset();
    allocator.free(buf);
  }

  return ret;
}

OB_INLINE int ObDatumRowkey::shallow_copy(ObDatumRowkey &dest) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!dest.is_valid() || dest.get_datum_cnt() < datum_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to shallow copy datum rowkey", K(ret), K(dest), K(*this));
  } else {
    ObStorageDatum *datums = const_cast<ObStorageDatum*>(dest.datums_);
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      datums[i] = datums_[i];
    }
    dest.datum_cnt_ = datum_cnt_;
    dest.hash_ = hash_;
    dest.store_rowkey_ = store_rowkey_;
    dest.group_idx_ = group_idx_;
    dest.store_rowkey_.reset();
  }

  return ret;
}

OB_INLINE int ObDatumRowkey::semi_copy(ObDatumRowkey &dest, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;

  if (OB_UNLIKELY(!is_valid() || !dest.is_valid() || dest.get_datum_cnt() < datum_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for deep copy invalid datum rowkey", K(ret), K(*this), K(dest));
  } else {
    ObStorageDatum *datums = const_cast<ObStorageDatum *> (dest.datums_);
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      if (OB_FAIL(datums[i].deep_copy(datums_[i], allocator))) {
        STORAGE_LOG(WARN, "Failed to deep copy datum", K(ret), K(i), K(datums_[i]));
      }
    }
  }

  return ret;
}

//ATTENTION only use in unittest
OB_INLINE bool ObDatumRowkey::operator==(const ObDatumRowkey &other) const
{
  bool is_equal = true;
  if (&other == this) {

  } else if (datum_cnt_ != other.datum_cnt_) {
    is_equal = false;
    STORAGE_LOG(DEBUG, "datum rowkey count no equal", K(other), K(*this));
  } else {
    for (int64_t i = 0; is_equal && i < datum_cnt_; i++) {
      is_equal = datums_[i] == other.datums_[i];
      if (!is_equal) {
        STORAGE_LOG(DEBUG, "datum not equal", K(i), K(other), K(*this));
      }
    }
  }
  return is_equal;
}



} // namespace blocksstable
} // namespace oceanbase
#endif
