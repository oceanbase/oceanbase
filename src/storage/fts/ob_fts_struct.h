/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_FTS_STRUCT_H_
#define OB_FTS_STRUCT_H_

#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace storage
{

class ObFTWord final
{
public:
  ObFTWord() : word_(), type_(ObCollationType::CS_TYPE_INVALID) {}
  ObFTWord(const int64_t length, const char *ptr, const ObCollationType &type) : word_(length, ptr), type_(type) {}
  ~ObFTWord() = default;

  OB_INLINE const common::ObString &get_word() const { return word_; }
  OB_INLINE const ObCollationType &get_collation_type() const { return type_; }
  OB_INLINE int hash(uint64_t &hash_val) const
  {
    hash_val = ObCharset::hash(type_, word_);
    return common::OB_SUCCESS;
  }
  OB_INLINE uint64_t hash() const { return word_.hash(); }
  OB_INLINE bool empty() const { return word_.empty(); }

  OB_INLINE bool operator ==(const ObFTWord &other) const
  {
    bool is_equal = false;
    if (other.type_ == type_) {
      is_equal = 0 == ObCharset::strcmp(type_, word_, other.word_);
    }
    return is_equal;
  }
  OB_INLINE bool operator !=(const ObFTWord &other) const { return !(other == *this); }

  TO_STRING_KV(K_(type), K_(word));
private:
  common::ObString word_;
  ObCollationType type_;
};

class ObFTWordCount final
{
public:
  ObFTWordCount() : ft_word_(), word_cnt_(0) {}
  ~ObFTWordCount() = default;
  OB_INLINE int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return common::OB_SUCCESS;
  }
  OB_INLINE uint64_t hash() const
  {
    int64_t hash_value = ft_word_.hash();
    hash_value = common::murmurhash(&word_cnt_, sizeof(word_cnt_), hash_value);
    return hash_value;
  }
  OB_INLINE bool is_valid() const { return !ft_word_.empty() && word_cnt_ > 1; }
  TO_STRING_KV(K_(ft_word), K_(word_cnt));
public:
  ObFTWord ft_word_;
  int64_t word_cnt_;
};

} // end namespace storage
} // end namespace oceanbase

#endif// OB_FTS_STRUCT_H_
