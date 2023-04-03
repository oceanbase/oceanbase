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

#ifndef OCEANBASE_COMMON_OB_STORE_RANGE_H_
#define OCEANBASE_COMMON_OB_STORE_RANGE_H_

#include <stdint.h>
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/ob_range.h"
#include "common/rowkey/ob_store_rowkey.h"

namespace oceanbase
{
namespace common
{
// Only used in storage layer.
// No compare semantics except for the special whole_range is provided.
// Note that one cannot judge if an ObStoreRange object is empty based on that object alone
// because the comparison of the start_key_ and end_key_ of the range requires additional
// (column order) information.
class ObStoreRange
{
public:
  ObStoreRange() { reset(); }
  ~ObStoreRange() { }

  inline void reset()
  {
    table_id_ = OB_INVALID_ID;
    border_flag_.set_data(0);
    start_key_.reset();
    end_key_.reset();
    group_idx_ = 0;
  }

  inline void assign(const ObNewRange &range)
  {
    table_id_ = range.table_id_;
    border_flag_ = range.border_flag_;
    start_key_.get_rowkey() = range.start_key_;
    end_key_.get_rowkey() = range.end_key_;
  }
  inline void to_new_range(ObNewRange &range) const
  {
    range.table_id_ = table_id_;
    range.border_flag_ = border_flag_;
    range.start_key_ = start_key_.get_rowkey();
    range.end_key_ = end_key_.get_rowkey();
  }

  void change_boundary(const ObStoreRowkey &store_rowkey, bool is_reverse, bool exclusive = false);
  uint64_t get_table_id() const { return table_id_; }
  const ObBorderFlag& get_border_flag() const { return border_flag_; }
  ObBorderFlag& get_border_flag() { return border_flag_; }
  const ObStoreRowkey& get_start_key() const { return start_key_; }
  ObStoreRowkey &get_start_key() { return start_key_; }
  const ObStoreRowkey& get_end_key() const { return end_key_; }
  ObStoreRowkey &get_end_key() { return end_key_; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  void set_border_flag(ObBorderFlag flag) { border_flag_ = flag; }
  void set_start_key(const ObStoreRowkey &start_key) { start_key_ = start_key; }
  void set_end_key(const ObStoreRowkey &end_key) { end_key_ = end_key; }
  void set_group_idx(const int64_t group_idx) { group_idx_ = group_idx; }
  int64_t get_group_idx() const { return group_idx_; }

  bool is_left_open() const { return !border_flag_.inclusive_start(); }
  bool is_left_closed() const { return border_flag_.inclusive_start(); }
  bool is_right_open() const { return !border_flag_.inclusive_end(); }
  bool is_right_closed() const { return border_flag_.inclusive_end(); }

  void set_left_open() { border_flag_.unset_inclusive_start(); }
  void set_left_closed() { border_flag_.set_inclusive_start(); }
  void set_right_open() { border_flag_.unset_inclusive_end(); }
  void set_right_closed() { border_flag_.set_inclusive_end(); }

  inline int build_range(uint64_t table_id, ObStoreRowkey store_rowkey);

  // a valid ObStoreRange need NOT refer to a specific, valid table
  inline bool is_valid() const
  {
    //return start_key_.is_valid() && end_key_.is_valid();

    // FiXME-yangsuli: temporally using this to preserve semantics with ObNewRange
    // eventually caller need to call empty() with the column order passed in
    // or a simpler is_valid() check based on there need
    return !empty();
  }

  inline int compare_with_startkey(const ObStoreRange &r, int &cmp) const;

  // return true if the range is a single key value(but not min or max); false otherwise
  inline bool is_single_rowkey() const;

  int64_t to_plain_string(char *buffer, const int64_t length) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
  int deserialize(ObIAllocator &allocator, const char *buf, const int64_t buf_len, int64_t &pos);
  int deep_copy(ObIAllocator &allocator, ObStoreRange &dst) const;

  inline int get_common_store_rowkey(ObStoreRowkey &store_rowkey) const;
  TO_STRING_KV(K_(table_id), K_(border_flag), K_(start_key), K_(end_key));

private:
  uint64_t table_id_;
  ObBorderFlag border_flag_;
  ObStoreRowkey start_key_;
  ObStoreRowkey end_key_;
  int64_t group_idx_;

  //FIXME-yangsuli: remove interfaces below after changing code that calls below methods
  //ObStoreRange should NOT provide compare semantics
public:
  inline void set_whole_range();
  inline bool is_whole_range() const
  {
    return (start_key_.is_min()) && (end_key_.is_max());
  }

  inline bool include(const ObStoreRange &r) const
  {
    return (table_id_ == r.table_id_)
        && (compare_with_startkey2(r) <= 0)
        && (compare_with_endkey2(r) >= 0);
  }

  inline int compare_with_endkey2(const ObStoreRange &r) const
  {
    int cmp = 0;
    if (end_key_.is_max()) {
      if (!r.end_key_.is_max()) {
        cmp = 1;
      }
    } else if (r.end_key_.is_max()) {
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

  inline int compare_with_startkey2(const ObStoreRange &r) const
  {
    int cmp = 0;
    if (start_key_.is_min()) {
      if (!r.start_key_.is_min()) {
        cmp = -1;
      }
    } else if (r.start_key_.is_min()) {
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

  inline bool empty() const
  {
    bool ret = false;
    if (start_key_.is_min() || end_key_.is_max()) {
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

  friend int deep_copy_range(ObIAllocator &allocator, const ObStoreRange &src, ObStoreRange *&dst);


};

int ObStoreRange::build_range(uint64_t table_id, ObStoreRowkey store_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id || !store_rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(table_id), K(store_rowkey), K(ret));
  } else {
    table_id_ = table_id;
    start_key_ = store_rowkey;
    end_key_ = store_rowkey;
    border_flag_.set_inclusive_start();
    border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObStoreRange::compare_with_startkey(const ObStoreRange &r, int &cmp) const
{
  int ret = OB_SUCCESS;
  if (0 != (cmp = start_key_.compare(r.start_key_))) {
    // do nothing
  } else if (border_flag_.inclusive_start() && !r.border_flag_.inclusive_start()) {
    cmp = -1;
  } else if (!border_flag_.inclusive_start() && r.border_flag_.inclusive_start()) {
    cmp = 1;
  }
  return ret;
}

void ObStoreRange::set_whole_range()
{
  start_key_.set_min();
  end_key_.set_max();
  border_flag_.unset_inclusive_start();
  border_flag_.unset_inclusive_end();
}

bool ObStoreRange::is_single_rowkey() const
{
  int ret = true;

  if (!start_key_.simple_equal(end_key_)) {
    ret = false;
  } else if (!border_flag_.inclusive_start() || !border_flag_.inclusive_end()) {
    ret = false;
  } else if (start_key_.contains_min_or_max_obj()) {
    ret = false;
  }

  return ret;
}

int ObStoreRange::get_common_store_rowkey(ObStoreRowkey &store_rowkey) const
{
  int ret = OB_SUCCESS;
  int64_t prefix_len = 0;
  if (OB_FAIL(ObStoreRowkey::get_common_prefix_length(start_key_, end_key_, prefix_len))) {
    COMMON_LOG(WARN, "fail to get common prefix length", K(ret));
  } else if (prefix_len > 0) {
    ret = store_rowkey.assign(const_cast<ObObj *>(start_key_.get_obj_ptr()), prefix_len);
  }
  return ret;
}


//FIXME-yangsuli: remove this method later
inline int deep_copy_range(ObIAllocator &allocator, const ObStoreRange &src, ObStoreRange *&dst)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (NULL == (ptr = allocator.alloc(sizeof(ObStoreRange)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "allocate new range failed", K(ret));
  } else {
    dst = new(ptr) ObStoreRange();
    if (OB_FAIL(src.start_key_.deep_copy(dst->start_key_, allocator))) {
      COMMON_LOG(WARN, "deep copy start key failed.", K(src.start_key_), K(ret));
    } else if (OB_FAIL(src.end_key_.deep_copy(dst->end_key_, allocator))) {
      COMMON_LOG(WARN, "deep copy end key failed.", K(src.end_key_), K(ret));
    } else {
      dst->table_id_ = src.table_id_;
      dst->border_flag_ = src.border_flag_;
    }
    if (OB_FAIL(ret) && NULL != ptr) {
      allocator.free(ptr);
      ptr = NULL;
    }
  }

  return ret;
}

class ObVersionStoreRangeConversionHelper
{
public:
  static int store_rowkey_to_multi_version_range(const ObStoreRowkey &src_rowkey,
                                           const ObVersionRange &version_range,
                                           ObIAllocator &allocator,
                                           ObStoreRange &multi_version_range);
  static int range_to_multi_version_range(const ObStoreRange &src_range,
                                          const ObVersionRange &version_range,
                                          ObIAllocator &allocator,
                                          ObStoreRange &multi_version_range);
private:
  static int build_multi_version_store_rowkey(const ObStoreRowkey &rowkey,
                                        const bool min_value,
                                        ObIAllocator &allocator,
                                        ObStoreRowkey &multi_version_rowkey);
};

} //end namespace common
} //end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_STORE_RANGE_H_
