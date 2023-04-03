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

#ifndef OCEANBASE_COMMON_OB_STORE_ROWKEY_H_
#define OCEANBASE_COMMON_OB_STORE_ROWKEY_H_

#include <stdint.h>
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace common
{

/*
 * ObStoreRowkey can only be compared according to column orders.
 * FIXME-yangsuli: reduce some of the log levels once the code stablizes
 */
class ObStoreRowkey
{
public:
  ObStoreRowkey() : key_(), hash_(0), group_idx_(0) {}
  ~ObStoreRowkey() {};
  inline void reset() {key_.reset(); hash_ = 0; group_idx_ = 0; }
  void destroy(ObIAllocator &allocator);

  //TODO column order is fake now, need to enable by someone in some day
  //FIXME-yangsuli
  //used for converting to regular ObRowkey, may need to change later
  //FIXME-yangsuli: the following interfaces are only kept for compatibility
  //and need to be reoved later
  ObRowkey to_rowkey() const;
  ObRowkey &get_rowkey() { return key_; }
  const ObRowkey &get_rowkey() const { return key_; }

  inline bool is_valid() const { return key_.is_valid(); }
  inline int64_t get_obj_cnt() const { return key_.get_obj_cnt(); }
  inline const ObObj *get_obj_ptr() const { return key_.get_obj_ptr(); }
  inline ObObj *get_obj_ptr() { return key_.get_obj_ptr(); }

  inline int assign(ObObj *ptr, const int64_t cnt)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == ptr || cnt <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "Invalid argument to assign store rowkey", KP(ptr), K(cnt));
    } else {
      key_.assign(ptr, cnt);
      hash_ = 0;
    }
    return ret;
  }

  //Note: this function does NOT return the size a newly deep-copied ObStoreRowkey would occupied
  //instead, it returns the *extra* space that is needed *in addition to* a plain ObStoreRowkey struct,
  //a.k.a., the spaced that is required to be allocated from the allocator
  OB_INLINE int64_t get_deep_copy_size() const { return key_.get_deep_copy_size(); }
  OB_INLINE int deep_copy(ObStoreRowkey &rhs, ObIAllocator &allocator) const
  {
    rhs.hash_ = hash_;
    return key_.deep_copy(rhs.key_, allocator);
  }

  OB_INLINE int deep_copy(ObStoreRowkey &rhs, char *buf, const int64_t buf_len) const
  {
    rhs.hash_ = hash_;
    return key_.deep_copy(rhs.key_, buf, buf_len);
  }

  inline int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    return key_.deserialize(buf, data_len, pos);
  }
  inline int deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
  {
    return key_.deserialize(allocator, buf, data_len, pos);
  }
  inline int serialize(char *buf, int64_t buf_len, int64_t &pos) const
  {
    return key_.serialize(buf, buf_len, pos);
  }
  int64_t get_serialize_size(void) const
  {
    return key_.get_serialize_size();
  }


  TO_YSON_KV(OB_ID(store_rowkey), to_cstring(*this));
  inline int64_t to_string(char *buffer, const int64_t length) const
  {
    return key_.to_string(buffer, length);
  }
  inline int64_t to_plain_string(char *buffer, const int64_t length) const
  {
    return key_.to_plain_string(buffer, length);
  }
  //to_smart_string and to_format_string are for log_tool use
  inline int64_t to_smart_string(char *buffer, const int64_t length) const
  {
    return key_.to_smart_string(buffer, length);
  }
  int64_t to_format_string(char *buffer, const int64_t length) const
  {
    return key_.to_format_string(buffer, length);
  }
  const char *repr() const { return common::to_cstring(*this); }

  inline int need_transform_to_collation_free(bool &need_transform) const
  {
    return key_.need_transform_to_collation_free(need_transform);
  }
  inline int to_collation_free_store_rowkey(ObStoreRowkey &collation_free_key, ObIAllocator &allocator) const
  {
    return key_.to_collation_free_rowkey(collation_free_key.key_, allocator);
  }
  inline int to_collation_free_store_rowkey_on_demand(ObStoreRowkey &collation_free_key, ObIAllocator &allocator) const
  {
    return key_.to_collation_free_rowkey_on_demand(collation_free_key.key_, allocator);
  }


  //for compatibility, regular ObStoreRowkey must return the same checksum as ObRowkey
  inline int checksum(ObBatchChecksum &bc) const { return key_.checksum(bc); }
  uint64_t murmurhash(const uint64_t hash) const;
  inline uint64_t hash() const
  {
    if (0 == hash_) {
      hash_ = key_.hash();
    }
    return hash_;
  }

  static inline int get_common_prefix_length(const ObStoreRowkey &lhs,
                                      const ObStoreRowkey &rhs, int64_t &prefix_len)
  {
    return ObRowkey::get_common_prefix_length(lhs.key_, rhs.key_, prefix_len);
  }
  bool contains_min_or_max_obj() const;
  bool contains_null_obj() const;

  // returns the same value as compare() == 0, but faster, and without requiring column order info
  OB_INLINE int equal(const ObStoreRowkey &rhs, bool &is_equal) const
  {
    int ret = OB_SUCCESS;
    if (hash() != rhs.hash()) {
      is_equal = false;
    } else if (OB_FAIL(key_.equal(rhs.key_, is_equal))) {
      COMMON_LOG(ERROR, "failed to compare", K(ret), K(key_), K(rhs.key_));
    } else {
      // do nothing
    }
    return ret;
  }

  // TODO by fengshuo.fs: remove this function
  // returns the same value as compare() == 0, but faster, and without requiring column order info
  OB_INLINE bool simple_equal(const ObStoreRowkey &rhs) const
  {
    return (hash() == rhs.hash()) && key_.simple_equal(rhs.key_);
  }
  OB_INLINE int compare(const ObStoreRowkey &rhs, int &cmp) const { return key_.compare(rhs.key_, cmp); }
  // TODO by fengshuo.fs: remove this function
  OB_INLINE int compare(const ObStoreRowkey &rhs) const { return key_.compare(rhs.key_); }
  OB_INLINE int compare_prefix(const ObStoreRowkey &rhs, int &cmp) const { return key_.compare_prefix(rhs.key_, cmp); }
  void set_group_idx(const int64_t group_idx) { group_idx_ = group_idx; }
  int64_t get_group_idx() const { return group_idx_; }

public:
  inline bool operator==(const ObStoreRowkey &rhs) const
  {
    return simple_equal(rhs);
  }
  inline bool operator!=(const ObStoreRowkey &rhs) const
  {
    return !simple_equal(rhs);
  }
  inline bool operator<(const ObStoreRowkey &rhs) const
  {
    return compare(rhs) < 0;
  }
  inline bool operator<=(const ObStoreRowkey &rhs) const
  {
    return compare(rhs) <= 0;
  }
  inline bool operator>(const ObStoreRowkey &rhs) const
  {
    return compare(rhs) > 0;
  }
  inline bool operator>=(const ObStoreRowkey &rhs) const
  {
    return compare(rhs) >= 0;
  }
  inline bool is_regular() const { return key_.is_valid(); }
  inline bool is_max() const { return key_.is_max_row(); }
  inline bool is_min() const { return key_.is_min_row(); }
  inline void set_max() { *this = MAX_STORE_ROWKEY; }
  inline void set_min() { *this = MIN_STORE_ROWKEY; }
  static ObObj MIN_OBJECT;
  static ObObj MAX_OBJECT;
  static ObStoreRowkey MIN_STORE_ROWKEY;
  static ObStoreRowkey MAX_STORE_ROWKEY;
private:
  ObStoreRowkey(ObObj *ptr, const int64_t cnt)
  {
    key_.assign(ptr, cnt);
    hash_ = 0;
    group_idx_ = 0;
  }

private:
  enum MinMaxFlag {
    MIN,
    MAX
  };

private:
  ObRowkey key_; // only used as a container for ObObjs
  mutable uint64_t hash_;
  int64_t group_idx_;
};

class ObStoreRowkeyComparator
{
public:
  ObStoreRowkeyComparator(int &ret) :ret_(ret)
  {}
  ~ObStoreRowkeyComparator() {}
  inline bool operator() (const common::ObStoreRowkey &lhs, const common::ObStoreRowkey &rhs)
  {
    int cmp_ret = 0;
    int &ret = ret_;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(lhs.compare(rhs, cmp_ret))) {
      COMMON_LOG(WARN, "fail to compare rowkey ", K(ret), K(lhs), K(rhs));
    }
    return cmp_ret < 0;
  }

private:
  int &ret_;
};

// TODO(yuanzhe) will removed later, shoud not use!!!
// Contains both the original and collation_free store_rowkey for better performance
// (no need to convert to collation_free multiple times)
// Only used in the storage layer.
class ObExtStoreRowkey
{
public:
  ObExtStoreRowkey();
  explicit ObExtStoreRowkey(const ObStoreRowkey &store_rowkey);
  ObExtStoreRowkey(const ObStoreRowkey &store_rowkey,
                   const ObStoreRowkey &collation_free_store_rowkey);

  int need_transform_to_collation_free(bool &need_transform) const;
  int to_collation_free_store_rowkey(ObIAllocator &allocator);
  int to_collation_free_store_rowkey_on_demand(ObIAllocator &allocator);
  int check_use_collation_free(const bool exist_invalid_macro_meta_collation_free,
                               bool &use_collation_free) const;
  int to_collation_free_on_demand_and_cutoff_range(ObIAllocator &allocator);
  int get_possible_range_pos();

  OB_INLINE bool is_range_cutoffed() const { return range_cut_pos_ >= 0; }
  OB_INLINE int16_t get_range_cut_pos() const { return range_cut_pos_; }
  OB_INLINE int16_t get_first_null_pos() const { return first_null_pos_; }
  OB_INLINE bool is_range_check_min() const { return range_check_min_; }
  const ObStoreRowkey &get_store_rowkey() const { return store_rowkey_; }
  ObStoreRowkey &get_store_rowkey() { return store_rowkey_; }
  const ObStoreRowkey &get_collation_free_store_rowkey() const { return collation_free_store_rowkey_; }

  OB_INLINE void reset()
  {
    store_rowkey_.reset();
    reset_collation_free_and_range();
  }

  OB_INLINE void reset_collation_free_and_range()
  {
    collation_free_store_rowkey_.reset();
    range_cut_pos_ = -1;
    first_null_pos_ = -1;
    range_check_min_ = true;
  }

  void set_group_idx(const int64_t group_idx) { group_idx_ = group_idx; }
  int64_t get_group_idx() const { return group_idx_; }

  // collation_free_store_rowkey_ is NOT serialized or deserialized.
  // After deserialization, one must perform a to_collation_free_store_rowkey if one wishes to
  // get that field.
  NEED_SERIALIZE_AND_DESERIALIZE;
  int deep_copy(ObExtStoreRowkey &extkey, ObIAllocator &allocator) const;
  int deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);

  TO_STRING_KV(K_(store_rowkey), K_(collation_free_store_rowkey), K_(range_cut_pos), K_(range_check_min),
      K_(group_idx));
private:
  ObStoreRowkey store_rowkey_;
  ObStoreRowkey collation_free_store_rowkey_;
  int16_t range_cut_pos_; // cut-off from min/max object
  int16_t first_null_pos_; // first null pos
  bool range_check_min_; // min or max
  int64_t group_idx_;
};

} //end namespace common
} //end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_STORE_ROWKEY_H_
