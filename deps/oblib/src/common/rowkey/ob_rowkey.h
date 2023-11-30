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

#ifndef OCEANBASE_COMMON_OB_ROWKEY_H_
#define OCEANBASE_COMMON_OB_ROWKEY_H_

#include <stdint.h>
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/json/ob_yson.h"
#include "common/ob_common_utility.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace common
{
class ObStoreRowkey;

class ObRowkey
{
public:
  ObRowkey() : obj_ptr_(NULL), obj_cnt_(0) {}
  ObRowkey(ObObj *ptr, const int64_t cnt) : obj_ptr_(ptr), obj_cnt_(cnt) {}
  ~ObRowkey() {}
public:
  int to_store_rowkey(ObStoreRowkey &store_rowkey) const;
  void reset() {obj_ptr_ = NULL; obj_cnt_ = 0; }
  void destroy(ObIAllocator &allocator);
  inline int64_t get_obj_cnt() const { return obj_cnt_; }
  inline const ObObj *get_obj_ptr() const { return obj_ptr_; }
  inline ObObj *get_obj_ptr() { return obj_ptr_; }
  // for convenience compactible with ObString
  inline int64_t length()  const { return obj_cnt_; }
  inline const ObObj *ptr() const { return obj_ptr_; }
  inline bool is_legal() const { return !(NULL == obj_ptr_ && obj_cnt_ > 0); }
  inline bool is_valid() const { return NULL != obj_ptr_ && obj_cnt_ > 0; }
  // is min rowkey or max rowkey
  inline bool is_min_row(void) const
  {
    bool bret = false;
    int64_t i = 0;
    for (i = 0; i < obj_cnt_; i++) {
      if (!obj_ptr_[i].is_min_value()) {
        break;
      }
    }
    if (obj_cnt_ > 0 && i >= obj_cnt_) {
      bret = true;
    }
    return bret;
  }

  inline bool is_max_row(void) const
  {
    bool bret = false;
    int64_t i = 0;
    for (i = 0; i < obj_cnt_; i++) {
      if (!obj_ptr_[i].is_max_value()) {
        break;
      }
    }
    if (obj_cnt_ > 0 && i >= obj_cnt_) {
      bret = true;
    }
    return bret;
  }

  inline void set_min_row(void) { *this = ObRowkey::MIN_ROWKEY; }
  inline void set_max_row(void) { *this = ObRowkey::MAX_ROWKEY; }
  inline void set_length(const int64_t cnt) { obj_cnt_ = cnt; }
  inline void assign(ObObj *ptr, const int64_t cnt)
  {
    obj_ptr_ = ptr;
    obj_cnt_ = cnt;
  }
  inline int obj_copy(ObRowkey &rowkey, ObObj *obj_buf, const int64_t cnt) const;

  int64_t get_deep_copy_size() const;

  template <typename Allocator>
  int deep_copy(ObRowkey &rhs, Allocator &allocator) const;

  OB_INLINE int deep_copy(char *buffer, int64_t size) const;
  OB_INLINE int deep_copy(ObRowkey &out, char *buffer, int64_t size) const;
  OB_INLINE int deep_copy(const ObRowkey &src, char *ptr, int64_t size, int64_t &pos);

  template <typename Allocator>
  int deserialize(Allocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size(void) const;
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos, bool check_zero = false);

  int serialize_objs(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_objs_size(void) const;
  int deserialize_objs(const char *buf, const int64_t buf_len, int64_t &pos);

  TO_YSON_KV(OB_ID(rowkey), to_cstring(*this));
  int64_t to_string(char *buffer, const int64_t length) const;
  //to_smart_string and to_format_string are for log_tool use
  int64_t to_smart_string(char *buffer, const int64_t length) const;
  int64_t to_format_string(char *buffer, const int64_t length) const;

  int64_t to_plain_string(char *buffer, const int64_t length) const;
  int need_transform_to_collation_free(bool &need_transform) const;
  template <typename Allocator>
  int to_collation_free_rowkey(common::ObRowkey &collation_free_rowkey, Allocator &allocator) const;
  template <typename Allocator>
  int to_collation_free_rowkey_on_demand(common::ObRowkey &collation_free_rowkey, Allocator &allocator) const;

  int checksum(ObBatchChecksum &bc) const;
  uint64_t murmurhash(const uint64_t hash) const;
  inline uint64_t hash() const
  {
    return murmurhash(0);
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  static int get_common_prefix_length(const ObRowkey &lhs, const ObRowkey &rhs, int64_t &prefix_len);

public:
  int equal(const ObRowkey &rhs, bool &is_equal) const;
  bool simple_equal(const ObRowkey &rhs) const;
  //FIXME temporarily, rowkey compare with column order use seperate func
  OB_INLINE int compare(const ObRowkey &rhs, int &cmp) const
  {
    int ret = OB_SUCCESS;
    cmp = 0;
    if (obj_ptr_ == rhs.obj_ptr_) {
      cmp = static_cast<int32_t>(obj_cnt_ - rhs.obj_cnt_);
    } else {
      cmp = fast_compare(rhs);
    }
    return ret;
  }
  // TODO by fengshuo.fs: remove this function
  OB_INLINE int compare(const ObRowkey &rhs) const
  {
    int cmp = 0;
    if (obj_ptr_ == rhs.obj_ptr_) {
      cmp = static_cast<int32_t>(obj_cnt_ - rhs.obj_cnt_);
    } else {
      cmp = fast_compare(rhs);
    }
    return cmp;
  }

  OB_INLINE int compare_prefix(const ObRowkey &rhs, int &cmp) const
  {
    int ret = OB_SUCCESS;
    int lv = 0;
    int rv = 0;
    cmp = 0;
    //TODO remove disgusting loop, useless max min judge
    if (is_min_row()) { lv = -1; }
    else if (is_max_row()) { lv = 1; }
    if (rhs.is_min_row()) { rv = -1; }
    else if (rhs.is_max_row()) { rv  = 1; }
    if (0 == lv && 0 == rv) {
      int64_t i = 0;
      int64_t cmp_cnt = std::min(obj_cnt_, rhs.obj_cnt_);
      for (; i < cmp_cnt && 0 == cmp && OB_SUCC(ret); ++i) {
        //TODO remove collation free check
        ret = obj_ptr_[i].check_collation_free_and_compare(rhs.obj_ptr_[i], cmp);
      }
    } else {
      cmp = lv - rv;
    }
    return ret;
  }

  // TODO by fengshuo.fs: remove this function
  OB_INLINE int compare_prefix(const ObRowkey &rhs) const
  {
    int cmp = 0;
    int lv = 0;
    int rv = 0;
    //TODO remove disgusting loop, useless max min judge
    if (is_min_row()) { lv = -1; }
    else if (is_max_row()) { lv = 1; }
    if (rhs.is_min_row()) { rv = -1; }
    else if (rhs.is_max_row()) { rv  = 1; }
    if (0 == lv && 0 == rv) {
      int64_t i = 0;
      int64_t cmp_cnt = std::min(obj_cnt_, rhs.obj_cnt_);
      for (; i < cmp_cnt && 0 == cmp; ++i) {
        //TODO remove collation free check
        cmp = obj_ptr_[i].check_collation_free_and_compare(rhs.obj_ptr_[i]);
      }
    } else {
      cmp = lv - rv;
    }
    return cmp;
  }

  OB_INLINE int32_t fast_compare(const ObRowkey &rhs) const
  {
    int32_t cmp = 0;
    int64_t i = 0;
    for (; i < obj_cnt_ && 0 == cmp; ++i) {
      __builtin_prefetch(&rhs.obj_ptr_[i]);
      if (i < rhs.obj_cnt_) {
        // optimize for int
        if (obj_ptr_[i].is_int32() && rhs.obj_ptr_[i].is_int32()) {
          int32_t left = obj_ptr_[i].get_int32();
          int32_t right = rhs.obj_ptr_[i].get_int32();
          if (left > right) {
            cmp = 1;
          } else if (left < right) {
            cmp = -1;
          } else {
            cmp = 0;
          }
        } else if (obj_ptr_[i].is_int() && rhs.obj_ptr_[i].is_int()) {
          int64_t left = obj_ptr_[i].get_int();
          int64_t right = rhs.obj_ptr_[i].get_int();
          if (left > right) {
            cmp = 1;
          } else if (left < right) {
            cmp = -1;
          } else {
            cmp = 0;
          }
        } else {
          cmp = obj_ptr_[i].compare(rhs.obj_ptr_[i]);
        }
      } else {
        cmp = 1;
      }
    }

    // rhs.cnt > this.cnt
    if (0 == cmp && i < rhs.obj_cnt_) {
      cmp = -1;
    }

    return cmp;
  }

  inline bool operator<(const ObRowkey &rhs) const
  {
    return compare(rhs) < 0;
  }

  inline bool operator<=(const ObRowkey &rhs) const
  {
    return compare(rhs) <= 0;
  }

  inline bool operator>(const ObRowkey &rhs) const
  {
    return compare(rhs) > 0;
  }

  inline bool operator>=(const ObRowkey &rhs) const
  {
    return compare(rhs) >= 0;
  }

  inline bool operator==(const ObRowkey &rhs) const
  {
    return compare(rhs) == 0;
  }
  inline bool operator!=(const ObRowkey &rhs) const
  {
    return compare(rhs) != 0;
  }
private:
  ObObj *obj_ptr_;
  int64_t obj_cnt_;
public:
  static ObObj MIN_OBJECT;
  static ObObj MAX_OBJECT;
  static ObRowkey MIN_ROWKEY;
  static ObRowkey MAX_ROWKEY;
};

inline int64_t ObRowkey::get_deep_copy_size() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t obj_arr_len = obj_cnt_ * sizeof(common::ObObj);
  int64_t total_len = obj_arr_len;

  if (OB_UNLIKELY(!is_legal())) {
    tmp_ret = OB_INVALID_DATA;
    COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(tmp_ret));
  } else {
    for (int64_t i = 0; i < obj_cnt_; ++i) {
      total_len += obj_ptr_[i].get_deep_copy_size();
    }
  }

  return total_len;
}

template <typename Allocator>
int ObRowkey::deserialize(Allocator &allocator, const char *buf, const int64_t data_len,
                          int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey copy_rowkey;
  copy_rowkey.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);

  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(copy_rowkey.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize to shallow copy key failed.",
               KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(copy_rowkey.deep_copy(*this, allocator))) {
    COMMON_LOG(WARN, "deep copy to self failed.",
               KP(buf), K(data_len), K(pos), K(ret));
  }

  return ret;
}

OB_INLINE int ObRowkey::deep_copy(char *ptr, int64_t size) const
{
  int ret = OB_SUCCESS;

  int64_t obj_arr_len = obj_cnt_ * sizeof(ObObj);
  if (OB_UNLIKELY(NULL == ptr) || OB_UNLIKELY(size < obj_arr_len)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(ptr), K(size), KP_(obj_cnt), K(ret));
  } else if (obj_cnt_ > 0 && NULL != obj_ptr_) {
    ObObj *obj_ptr = NULL;
    int64_t pos = 0;

    obj_ptr = reinterpret_cast<ObObj *>(ptr);
    pos += obj_arr_len;

    for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(obj_ptr[i].deep_copy(obj_ptr_[i], ptr, size, pos))) {
        COMMON_LOG(WARN, "deep copy object failed.",
                   K(i), K(obj_ptr_[i]), K(size), K(pos), K(ret));
      }
    }
  }

  return ret;

}

OB_INLINE int ObRowkey::deep_copy(const ObRowkey &src, char *ptr, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "illegal src rowkey.", KP(src.obj_ptr_), K(src.obj_cnt_), K(ret));
  } else {
    int64_t total_len = src.get_deep_copy_size();
    if (size < total_len + pos || OB_UNLIKELY(NULL == ptr)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments.", KP(ptr), K(size), K(total_len), K(pos), K(ret));
    } else if (src.obj_cnt_ > 0 && NULL != src.obj_ptr_) {
      ObObj *obj_ptr = NULL;
      obj_ptr = reinterpret_cast<ObObj *>(ptr + pos);
      pos += (src.obj_cnt_ * sizeof(ObObj));

      for (int64_t i = 0; i < src.obj_cnt_ && OB_SUCC(ret); ++i) {
        if (OB_FAIL(obj_ptr[i].deep_copy(src.obj_ptr_[i], ptr, size, pos))) {
          COMMON_LOG(WARN, "deep copy object failed.",  K(i), K(src.obj_ptr_[i]), K(size), K(pos), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        obj_ptr_ = obj_ptr;
        obj_cnt_ = src.obj_cnt_;
      }
   }
  }

  return ret;
}

OB_INLINE int ObRowkey::deep_copy(ObRowkey &rhs, char *ptr, int64_t total_len) const
{
  int ret = OB_SUCCESS;
  rhs.reset();

  if (OB_UNLIKELY(!is_legal())) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(WARN, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  } else if (obj_cnt_ > 0 && NULL != obj_ptr_) {
    if (OB_FAIL(deep_copy(ptr, total_len))) {
      COMMON_LOG(WARN, "deep copy rowkey failed.",
                 KP_(obj_ptr), KP_(obj_cnt), K(ret));
    } else {
      rhs.assign(reinterpret_cast<ObObj *>(ptr), obj_cnt_);
    }
  } else {
    rhs.assign(NULL, 0);
  }
  return ret;
}

template <typename Allocator>
int ObRowkey::deep_copy(ObRowkey &rhs, Allocator &allocator) const
{
  int ret = OB_SUCCESS;

  int64_t total_len = get_deep_copy_size();
  char *ptr = NULL;
  if (0 == total_len) {
    rhs.obj_ptr_ = NULL;
    rhs.obj_cnt_ = 0;
  } else if (OB_ISNULL(ptr = (char *)allocator.alloc(total_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate mem for obj array failed.",
               K(total_len), K(ret));
  } else if (OB_FAIL(deep_copy(rhs, ptr, total_len))) {
    COMMON_LOG(WARN, "failed to deep copy", K(ret));
  }

  if (OB_FAIL(ret) && NULL != ptr) {
    allocator.free(ptr);
    ptr = NULL;
  }

  return ret;

}

template<typename Allocator>
int ObRowkey::to_collation_free_rowkey(ObRowkey &collation_free_rowkey, Allocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (obj_cnt_ <= 0 || NULL == obj_ptr_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(obj_cnt_), K(obj_ptr_));
  } else {
    ObObj *endkey = NULL;
    if (NULL == (endkey = reinterpret_cast<ObObj*>(allocator.alloc(
              sizeof(ObObj) * obj_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc endkey", K(ret));
    } else {
      const bool is_copy_all = true;
      bool is_obj_collation_free_valid = false;
      collation_free_rowkey.assign(endkey, obj_cnt_);
      for (int64_t i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
        if (obj_ptr_[i].is_character_type()) {
          if (OB_FAIL(obj_ptr_[i].to_collation_free_obj(collation_free_rowkey.obj_ptr_[i], is_obj_collation_free_valid, allocator))) {
            COMMON_LOG(WARN, "fail to convert obj to collation free obj", K(ret), K(obj_ptr_[i]));
          } else if (!is_obj_collation_free_valid) {
            collation_free_rowkey.assign(NULL, 0);
            break;
          }
        } else {
          obj_ptr_[i].copy_value_or_obj(collation_free_rowkey.obj_ptr_[i], is_copy_all);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    collation_free_rowkey.assign(NULL, 0);
  }

  return ret;
}

template<typename Allocator>
int ObRowkey::to_collation_free_rowkey_on_demand(ObRowkey &collation_free_rowkey, Allocator &allocator) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  if (obj_cnt_ <= 0 || NULL == obj_ptr_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(obj_cnt_), K(obj_ptr_));
  } else if (OB_FAIL(need_transform_to_collation_free(need_transform))) {
    STORAGE_LOG(WARN, "fail to get if need to transform to collation free rowkey", K(ret));
  } else if (need_transform && OB_FAIL(to_collation_free_rowkey(collation_free_rowkey, allocator))) {
    STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret));
  }
  return ret;
}

inline std::ostream &operator<<(std::ostream &os, const ObRowkey &key)  // for google test
{
  os << " len=" << key.get_obj_cnt();
  return os;
}

template <typename AllocatorT>
int ob_write_rowkey(AllocatorT &allocator, const ObRowkey &src, ObRowkey &dst)
{
  return src.deep_copy(dst, allocator);
}

// only shallow copy obj, varchar is not copied
inline int ObRowkey::obj_copy(ObRowkey &dest_rowkey, ObObj *obj_buf, const int64_t cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == obj_buf)
      || OB_UNLIKELY(cnt < obj_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.",
               KP(obj_buf), K(cnt), K_(obj_cnt), K(ret));
  } else if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  } else {
    for (int64_t i = 0; i < obj_cnt_; ++i) {
      obj_buf[i] = obj_ptr_[i];
    }
    dest_rowkey.assign(obj_buf, obj_cnt_);
  }

  return ret;
}

}
}

#endif //OCEANBASE_COMMON_OB_ROWKEY_H_
