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

#ifndef __CBTREE_BTREE_VARCHAR_KEY_H__
#define __CBTREE_BTREE_VARCHAR_KEY_H__

#include "lib/cbtree/btree.h"

namespace cbtree {
inline int cbt_memcmp(const char* s1_, const char* s2_, int limit, int& offset)
{
  int cmp = 0;
  unsigned char* s1 = (unsigned char*)s1_;
  unsigned char* s2 = (unsigned char*)s2_;
  while (offset < limit && (cmp = (*s1++ - *s2++)) == 0) {
    offset++;
  }
  return cmp;
}

inline int cbt_memcmp_with_prefix(
    const char* s1, const char* s2, const char* prefix, int prefix_len, int limit, int& offset)
{
  return cbt_memcmp(s1, prefix, prefix_len, offset) ?: cbt_memcmp(s1 + offset, s2 + offset, limit, offset);
}

struct VarCharBuffer {
  VarCharBuffer() : len_(0)
  {}
  ~VarCharBuffer()
  {}
  int get_total_len() const
  {
    return len_ + (int)sizeof(*this);
  }
  static uint16_t min(uint16_t x1, uint16_t x2)
  {
    return x1 < x2 ? x1 : x2;
  }
  uint16_t len_;
  char buf_[0];
  inline int compare(const VarCharBuffer& that, int& offset) const
  {
    return cbt_memcmp(this->buf_, that.buf_, min(this->len_, that.len_), offset) ?: this->len_ - that.len_;
  }
};

struct VarCharKey {
  typedef VarCharKey key_t;
  enum { OFFSET_MASK = (1 << 13) - 1, SUFFIX_LEN_MASK = (1 << 3) - 1 };
  struct comp_helper_t {
    comp_helper_t() : last_decided_offset_(-1)
    {}
    void reset()
    {
      last_decided_offset_ = -1;
    }
    void reset_diff(key_t& key)
    {
      key.diff_offset_ = key.key_->len_ & OFFSET_MASK;
      key.diff_suffix_len_ = 0 & SUFFIX_LEN_MASK;
    }
    void calc_diff(key_t& thiskey, const key_t that)
    {
      int offset = 0;
      thiskey.key_->compare(*that.key_, offset);
      thiskey.diff_offset_ = offset & OFFSET_MASK;
      MEMCPY(thiskey.diff_suffix_,
          thiskey.key_->buf_ + offset,
          VarCharBuffer::min((uint16_t)sizeof(thiskey.diff_suffix_), (uint16_t)(thiskey.key_->len_ - offset)));
    }
    bool ideq(const key_t& key1, const key_t& key2) const
    {
      return key1.key_ == key2.key_;
    }
    bool veq(const key_t& key1, const key_t& key2) const
    {
      return key1.compare(key2) == 0;
    }
    int compare(const key_t& key1, const key_t& key2) const
    {
      return key1.compare(key2);
    }
    int linear_search_compare(const key_t search_key, const key_t idx_key)
    {
      int cmp = 0;
      int decided_offset = 0;
      if (last_decided_offset_ < 0) {
        if ((cmp = search_key.key_->compare(*idx_key.key_, decided_offset)) >= 0) {
          last_decided_offset_ = decided_offset;
        }
      } else if (idx_key.diff_offset_ > last_decided_offset_) {
        cmp = 1;
      } else if ((cmp = (cbt_memcmp_with_prefix(search_key.key_->buf_ + idx_key.diff_offset_,
                             idx_key.key_->buf_ + idx_key.diff_offset_,
                             idx_key.diff_suffix_,
                             idx_key.diff_suffix_len_,
                             VarCharBuffer::min(search_key.key_->len_, idx_key.key_->len_) - idx_key.diff_offset_,
                             decided_offset)
                             ?: search_key.key_->len_ - idx_key.key_->len_)) >= 0) {
        last_decided_offset_ = idx_key.diff_offset_ + decided_offset;
      }
      return cmp;
    }
    int last_decided_offset_;
  };
  VarCharKey() : diff_offset_(0), diff_suffix_len_(0), key_(0)
  {}
  ~VarCharKey()
  {}
  inline int compare(const VarCharKey& key) const
  {
    int offset = 0;
    return key_->compare(*key.key_, offset);
  }
  uint16_t diff_offset_ : 13;
  uint16_t diff_suffix_len_ : 3;
  char diff_suffix_[6];
  VarCharBuffer* key_;
};
};     // namespace cbtree
#endif /* __CBTREE_BTREE_VARCHAR_KEY_H__ */
