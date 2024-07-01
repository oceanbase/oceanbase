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

#ifndef OCEANBASE_COMMON_OB_RANGE2_H_
#define OCEANBASE_COMMON_OB_RANGE2_H_

#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/ob_string_buf.h"
#include "share/ob_cluster_version.h"


namespace oceanbase
{
namespace common
{
class ObStoreRange;

class ObBorderFlag
{
public:
  static const int8_t INCLUSIVE_START = 0x1;
  static const int8_t INCLUSIVE_END = 0x2;
  static const int8_t MIN_VALUE = 0x4;
  static const int8_t MAX_VALUE = 0x8;

public:
  ObBorderFlag() : data_(0) {}
  ~ObBorderFlag() {}

  inline void set_inclusive_start() { data_ |= INCLUSIVE_START; }

  inline void unset_inclusive_start() { data_ &= (~INCLUSIVE_START); }

  inline bool inclusive_start() const { return (data_ & INCLUSIVE_START) == INCLUSIVE_START; }

  inline void set_inclusive_end() { data_ |= INCLUSIVE_END; }

  inline void unset_inclusive_end() { data_ &= (~INCLUSIVE_END); }

  inline bool inclusive_end() const { return (data_ & INCLUSIVE_END) == INCLUSIVE_END; }

  inline void set_min_value() { data_ |= MIN_VALUE; }
  inline void unset_min_value() { data_ &= (~MIN_VALUE); }
  inline bool is_min_value() const { return (data_ & MIN_VALUE) == MIN_VALUE; }

  inline void set_max_value() { data_ |= MAX_VALUE; }
  inline void unset_max_value() { data_ &= (~MAX_VALUE); }
  inline bool is_max_value() const { return (data_ & MAX_VALUE) == MAX_VALUE; }

  inline void set_data(const int8_t data) { data_ = data; }
  inline void set_all_open() { set_data(0); }
  inline void set_all_close() { data_ = INCLUSIVE_START | INCLUSIVE_END; }
  inline int8_t get_data() const { return data_; }
  inline void set_inclusive(const int8_t data)
  {
    data_ &= MIN_VALUE + MAX_VALUE;
    data_ += data & (INCLUSIVE_START + INCLUSIVE_END);
  }

  TO_STRING_KV(N_FLAG, data_);
private:
  int8_t data_;
};

struct ObVersion
{
  const static int16_t START_MINOR_VERSION = 1;
  const static int16_t MAX_MINOR_VERSION = INT16_MAX;
  const static int32_t START_MAJOR_VERSION = 2;
  const static int32_t MAX_MAJOR_VERSION = INT32_MAX;
  const static int32_t DEFAULT_MAJOR_VERSION = 1;

  ObVersion() : version_(0) {}
  explicit ObVersion(int64_t version) : version_(version) {}
  ObVersion(const int64_t major, const int64_t minor)
      : major_(static_cast<int32_t>(major)),
        minor_(static_cast<int16_t>(minor)),
        is_final_minor_(0)
  {
  }

  union
  {
    int64_t version_;
    struct
    {
      int32_t major_           : 32;
      int16_t minor_           : 16;
      int16_t is_final_minor_  : 16;
    };
  };
  static ObVersion MIN_VERSION;
  static ObVersion MAX_VERSION;

  bool is_valid() const
  {
    return version_ >= 0;
  }

  void reset()
  {
    major_ = 0;
    minor_ = 0;
    is_final_minor_ = 0;
  }

  int64_t operator=(int64_t version)
  {
    version_ = version;
    return version_;
  }

  operator int64_t() const
  {
    return version_;
  }

  static int64_t get_version(int64_t major, int64_t minor, bool is_final_minor)
  {
    ObVersion v;
    v.major_          = static_cast<int32_t>(major);
    v.minor_          = static_cast<int16_t>(minor);
    v.is_final_minor_ = is_final_minor ? 1 : 0;
    return v.version_;
  }

  static int64_t get_major(int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    return v.major_;
  }

  static int64_t get_minor(int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    return v.minor_;
  }

  static bool is_final_minor(int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    return v.is_final_minor_ != 0;
  }

  static ObVersion get_version_with_max_minor(const int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    v.minor_ = INT16_MAX;
    return v;
  }

  static ObVersion get_version_with_min_minor(const int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    v.minor_ = 0;
    return v;
  }
  static int compare(int64_t l, int64_t r)
  {
    int ret = 0;
    ObVersion lv(l);
    ObVersion rv(r);

    //ignore is_final_minor
    if ((lv.major_ == rv.major_) && (lv.minor_ == rv.minor_)) {
      ret = 0;
    } else if ((lv.major_ < rv.major_) ||
               ((lv.major_ == rv.major_) && lv.minor_ < rv.minor_)) {
      ret = -1;
    } else {
      ret = 1;
    }
    return ret;
  }

  inline int compare(const ObVersion &rhs) const
  {
    int ret = 0;

    //ignore is_final_minor
    if ((major_ == rhs.major_) && (minor_ == rhs.minor_)) {
      ret = 0;
    } else if ((major_ < rhs.major_) ||
               ((major_ == rhs.major_) && minor_ < rhs.minor_)) {
      ret = -1;
    } else {
      ret = 1;
    }

    return ret;
  }

  inline bool operator<(const ObVersion &rhs) const
  {
    return compare(rhs) < 0;
  }

  inline bool operator<=(const ObVersion &rhs) const
  {
    return compare(rhs) <= 0;
  }

  inline bool operator>(const ObVersion &rhs) const
  {
    return compare(rhs) > 0;
  }

  inline bool operator>=(const ObVersion &rhs) const
  {
    return compare(rhs) >= 0;
  }

  inline bool operator==(const ObVersion &rhs) const
  {
    return compare(rhs) == 0;
  }

  inline bool operator!=(const ObVersion &rhs) const
  {
    return compare(rhs) != 0;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "\"%d-%hd-%hd\"",
                    major_, minor_, is_final_minor_);
    return pos;
  }

  int version_to_string(char *buf, const int64_t buf_len) const
  {
    int ret = OB_SUCCESS;
    if (NULL != buf && buf_len > 0) {
      if (0 > snprintf(buf, buf_len, "%d-%d-%d",
                       major_, minor_, is_final_minor_)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }
  int fixed_length_encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int fixed_length_decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_fixed_length_encoded_size() const;
  TO_YSON_KV(OB_Y_(version));
  OB_UNIS_VERSION(1);
};

class ObVersionProvider
{
public:
  virtual ~ObVersionProvider() {}
  virtual const ObVersion get_frozen_version() const = 0;
  virtual const ObVersion get_merged_version() const = 0;
};

//  1. for multi version sstable minor merge:
//    - rows in range (MIN_VERSION, base_store_version_] will not be iterated (i.e. skipped);
//    - rows in range (base_store_version_, multi_version_start] will be compacted;
//    - rows in range (multi_version_start, read_snapshot_] will be outputted directly;
//    - if base_store_version_ > multi_version_start_,
//      then set base_store_version_ = multi_version_start_.
//  2. for multi version sstable query:
//    - rows in range (MIN_VERSION, base_store_version_] will not be iterated (i.e. skipped);
//    - rows in range (base_store_version_, read_snapshot_] will be fused and outputted;
struct ObVersionRange
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MIN_VERSION = 0;

  ObVersionRange();
  OB_INLINE void reset();
  OB_INLINE bool is_valid() const;
  int64_t hash() const;
  OB_INLINE bool operator ==(const ObVersionRange &range) const;
  OB_INLINE bool operator !=(const ObVersionRange &range) const { return !this->operator ==(range); }
  OB_INLINE bool contain(const int64_t snaptshot_version) const;
  bool contain(const ObVersionRange &range) const;
  void union_version_range(const ObVersionRange &range);

  int64_t multi_version_start_;
  int64_t base_version_;
  int64_t snapshot_version_;

  TO_STRING_KV(K_(multi_version_start), K_(base_version), K_(snapshot_version));
};

struct ObNewVersionRange
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MIN_VERSION = 0;

  ObNewVersionRange();
  OB_INLINE void reset();
  OB_INLINE bool is_valid() const;
  int64_t hash() const;
  OB_INLINE bool operator ==(const ObNewVersionRange &range) const;
  OB_INLINE bool operator !=(const ObNewVersionRange &range) const { return !this->operator ==(range); }

  int64_t base_version_;
  int64_t snapshot_version_;

  TO_STRING_KV(K_(base_version), K_(snapshot_version));
};

class ObNewRange
{

public:
  uint64_t table_id_;
  ObBorderFlag border_flag_;
  ObRowkey start_key_;
  ObRowkey end_key_;
  union {
    int64_t flag_;
    struct {
      int64_t group_idx_: 32;
      int64_t is_physical_rowid_range_: 1;
      int64_t index_ordered_idx_ : 16;  // used for keep order of global index lookup
      int64_t reserved_: 15;
    };
  };


  ObNewRange()
  {
    reset();
  }

  ~ObNewRange()
  {
    reset();
  }

  inline void reset()
  {
    table_id_ = OB_INVALID_ID;
    border_flag_.set_data(0);
    start_key_.assign(NULL, 0);
    end_key_.assign(NULL, 0);
    flag_ = 0;
  }

  inline const ObRowkey &get_start_key()
  {
    return start_key_;
  }
  inline const ObRowkey &get_start_key() const
  {
    return start_key_;
  }


  inline const ObRowkey &get_end_key()
  {
    return end_key_;
  }
  inline const ObRowkey &get_end_key() const
  {
    return end_key_;
  }

  inline int32_t get_group_idx() const
  {
    return group_idx_;
  }
  inline int32_t get_index_ordered_idx() const
  {
    return index_ordered_idx_;
  }

  // pseudo-column [GROUP_ID], with high 32 bits as group_idx_ and low 32 bits as index_ordered_idx_
  // when cluster version < 4.3.2, the das keep order optimization is disabled, we should only fill
  // group_idx to [GROUP_ID] for compatibility.
  inline int64_t get_group_id() const
  {
    return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_2_0 ? group_idx_ :
        (static_cast<int64_t>(group_idx_) << 32) | (index_ordered_idx_ & 0xffffffff);
  }
  // get group_idx from [GROUP_ID]
  static int64_t get_group_idx(int64_t group_id)
  {
    return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_2_0 ? group_id : (group_id >> 32);
  }
  // get index_order_idx from [GROUP_ID]
  static int64_t get_index_ordered_idx(int64_t group_id)
  {
    return group_id & 0xffffffff;
  }

  int build_range(uint64_t table_id, ObRowkey rowkey)
  {
    int ret = OB_SUCCESS;
    if (OB_INVALID_ID == table_id || !rowkey.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments.", K(table_id), K(rowkey), K(ret));
    } else {
      table_id_ = table_id;
      start_key_ = rowkey;
      end_key_ = rowkey;
      border_flag_.set_inclusive_start();
      border_flag_.set_inclusive_end();
      flag_ = 0;
    }
    return ret;
  }

  // new compare func for tablet.range and scan_param.range
  inline int compare_with_endkey2(const ObNewRange &r) const
  {
    int cmp = 0;
    if (end_key_.is_max_row()) {
      if (!r.end_key_.is_max_row()) {
        cmp = 1;
      }
    } else if (r.end_key_.is_max_row()) {
      cmp = -1;
    } else {
      cmp = end_key_.compare(r.end_key_);
      if (0 == cmp) {
        if (border_flag_.inclusive_end() && !r.border_flag_.inclusive_end()) {
          cmp = 1;
        } else if (!border_flag_.inclusive_end() && r.border_flag_.inclusive_end()) {
          cmp = -1;
        }
      }
    }
    return cmp;
  }

  inline int compare_with_startkey2(const ObNewRange &r) const
  {
    int cmp = 0;
    if (start_key_.is_min_row()) {
      if (!r.start_key_.is_min_row()) {
        cmp = -1;
      }
    } else if (r.start_key_.is_min_row()) {
      cmp = 1;
    } else {
      cmp = start_key_.compare(r.start_key_);
      if (0 == cmp) {
        if (border_flag_.inclusive_start() && !r.border_flag_.inclusive_start()) {
          cmp = -1;
        } else if (!border_flag_.inclusive_start() && r.border_flag_.inclusive_start()) {
          cmp = 1;
        }
      }
    }
    return cmp;
  }

  inline bool is_valid() const { return !empty(); }

  inline bool empty() const
  {
    bool ret = false;
    if (start_key_.is_min_row() || end_key_.is_max_row()) {
      ret = false;
    } else {
      const int32_t result = end_key_.compare(start_key_);
      ret  = result < 0
             || ((0 == result)
                 && !((border_flag_.inclusive_end())
                      && border_flag_.inclusive_start()));
    }
    return ret;
  }

  // from MIN to MAX, complete set.
  inline void set_whole_range()
  {
    start_key_.set_min_row();
    end_key_.set_max_row();
    border_flag_.unset_inclusive_start();
    border_flag_.unset_inclusive_end();
  }

  inline void set_false_range()
  {
    start_key_.set_max_row();
    end_key_.set_min_row();
    border_flag_.unset_inclusive_start();
    border_flag_.unset_inclusive_end();
  }

  // from MIN to MAX, complete set.
  inline bool is_whole_range() const
  {
    return (start_key_.is_min_row()) && (end_key_.is_max_row());
  }

  inline bool is_false_range() const
  {
    return (start_key_.is_max_row()) && (end_key_.is_min_row());
  }

  /*
  inline bool is_close_range() const
  {
    if (start_key_.length() <= 0 || end_key_.length() <= 0) {
      COMMON_LOG(ERROR, "invalid range keys", K_(start_key),
                K_(end_key));
    }
    return !((start_key_.length() > 0 && start_key_.ptr()[0].is_min_value()) ||
             (end_key_.length() > 0 && end_key_.ptr()[0].is_max_value()));
  }
  */

  inline bool is_left_open_right_closed() const
  {
    return (!border_flag_.inclusive_start() && border_flag_.inclusive_end()) || end_key_.is_max_row();
  }

  // return true if the range is a single key value(but not min or max); false otherwise
  inline bool is_single_rowkey() const
  {
    int ret = false;
    if (start_key_.is_min_row() || start_key_.is_max_row()
        || end_key_.is_min_row() || end_key_.is_max_row()) {
      ret = false;
    } else if (start_key_ == end_key_ && border_flag_.inclusive_start()
               && border_flag_.inclusive_end()) {
      ret = true;
    }
    return ret;
  }

  inline bool equal(const ObNewRange &r) const
  {
    return equal2(r);
  }

  inline bool equal2(const ObNewRange &r) const
  {
    return (table_id_ == r.table_id_) && (compare_with_startkey2(r) == 0) &&
           (compare_with_endkey2(r) == 0);
  }

  inline bool include(const ObNewRange &r) const
  {
    return (table_id_ == r.table_id_)
        && (compare_with_startkey2(r) <= 0)
        && (compare_with_endkey2(r) >= 0);
  }

  inline bool operator == (const ObNewRange &other) const
  {
    return equal(other);
  };

  TO_YSON_KV(OB_ID(range), to_cstring(*this));
  int64_t to_string(char *buffer, const int64_t length) const;
  int64_t to_simple_string(char *buffer, const int64_t length) const;
  int64_t to_plain_string(char *buffer, const int64_t length) const;
  uint64_t hash() const;


  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size(void) const;

  template <typename Allocator>
     int deserialize(Allocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);

  inline int get_common_rowkey(ObRowkey &rowkey) const
  {
    int ret = OB_SUCCESS;
    int64_t prefix_len = 0;
    if (OB_FAIL(ObRowkey::get_common_prefix_length(start_key_, end_key_, prefix_len))) {
      STORAGE_LOG(WARN, "fail to get common prefix length", K(ret));
    } else {
      rowkey.assign(const_cast<ObObj *>(start_key_.get_obj_ptr()), prefix_len);
    }
    return ret;
  }

};

class ObNewRangeCmp
{
public:
  ObNewRangeCmp() {}

  // caller ensures no intersection between any two ranges
  OB_INLINE bool operator() (const ObNewRange *a, const ObNewRange *b) const
  {
    int cmp = 0;
    if (OB_NOT_NULL(a) && OB_NOT_NULL(b)) {
      cmp = a->compare_with_startkey2(*b);
    }
    return cmp < 0;
  }
};

template <typename Allocator>
int ObNewRange::deserialize(Allocator &allocator, const char *buf, const int64_t data_len,
                            int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  ObNewRange copy_range;
  copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
  copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
  if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize range to shallow copy object failed.",
               KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(deep_copy_range(allocator, copy_range, *this))) {
    COMMON_LOG(WARN, "deep_copy_range failed.",
               KP(buf), K(data_len), K(pos), K(copy_range), K(ret));
  }

  return ret;
}

template <typename Allocator>
inline int deep_copy_range(Allocator &allocator, const ObNewRange &src, ObNewRange &dst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.start_key_.deep_copy(dst.start_key_, allocator))) {
    COMMON_LOG(WARN, "deep copy start key failed.", K(src.start_key_), K(ret));
  } else if (OB_FAIL(src.end_key_.deep_copy(dst.end_key_, allocator))) {
    COMMON_LOG(WARN, "deep copy end key failed.", K(src.end_key_), K(ret));
  } else {
    dst.table_id_ = src.table_id_;
    dst.border_flag_ = src.border_flag_;
    dst.flag_ = src.flag_;
  }
  return ret;
}

template <typename Allocator>
inline int deep_copy_range(Allocator &allocator, const ObNewRange &src, ObNewRange *&dst)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (NULL == (ptr = allocator.alloc(sizeof(ObNewRange)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "allocate new range failed", K(ret));
  } else {
    dst = new(ptr) ObNewRange();
    if (OB_FAIL(src.start_key_.deep_copy(dst->start_key_, allocator))) {
      COMMON_LOG(WARN, "deep copy start key failed.", K(src.start_key_), K(ret));
    } else if (OB_FAIL(src.end_key_.deep_copy(dst->end_key_, allocator))) {
      COMMON_LOG(WARN, "deep copy end key failed.", K(src.end_key_), K(ret));
    } else {
      dst->table_id_ = src.table_id_;
      dst->border_flag_ = src.border_flag_;
      dst->flag_= src.flag_;
    }
    if (OB_FAIL(ret) && NULL != ptr) {
      allocator.free(ptr);
      ptr = NULL;
    }
  }

  return ret;
}

void ObVersionRange::reset()
{
  multi_version_start_ = -1;
  base_version_ = -1;
  snapshot_version_ = -1;
}

bool ObVersionRange::is_valid() const
{
  // TODO(weixue): remove 0 == multi_version_start_
  return (0 == multi_version_start_ || multi_version_start_ >= base_version_)
      && multi_version_start_ <= snapshot_version_
      && base_version_ >= MIN_VERSION
      && base_version_ <= snapshot_version_;
}

bool ObVersionRange::operator ==(const ObVersionRange &range) const
{
  return multi_version_start_ == range.multi_version_start_
      && base_version_ == range.base_version_
      && snapshot_version_ == range.snapshot_version_;
}

bool ObVersionRange::contain(const int64_t snaptshot_version) const
{
  bool is_contain = true;

  if (snaptshot_version > snapshot_version_) {
    is_contain = false;
  } else if (snaptshot_version < multi_version_start_) {
    is_contain = false;
  } else if (snaptshot_version == multi_version_start_) {
    if (multi_version_start_ == base_version_) {
      is_contain = false;
    }
  }

  return is_contain;
}

void ObNewVersionRange::reset()
{
  base_version_ = -1;
  snapshot_version_ = -1;
}

bool ObNewVersionRange::is_valid() const
{
  return snapshot_version_ >= MIN_VERSION;
}

bool ObNewVersionRange::operator ==(const ObNewVersionRange &range) const
{
  return base_version_ == range.base_version_
      && snapshot_version_ == range.snapshot_version_;
}


} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_RANGE_H_
