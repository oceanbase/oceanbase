/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace storage
{
/**
 * @class ObDATrieHit
 * @brief Record the matching path in a Double-Array-Trie and some info.
 *
 * @desc:
 * - It goes word by word;
 * - Word means a single character in a language;
 */
class ObIFTDict;
class ObDATrieHit final
{
public:
  ObDATrieHit(const ObIFTDict *dict, const int64_t start_pos)
      : dict_(dict), char_cnt_(0), start_pos_(start_pos), end_pos_(start_pos_), base_idx_(0),
        current_check_(0), is_match_(false), is_prefix_(false)
  {
  }

  bool is_match() const { return is_match_; }

  bool is_prefix() const { return is_prefix_; }

  bool is_unmatch() const
  {
    bool unmatch = (!is_match_) && (!is_prefix_);
    return unmatch;
  }

  void set_unmatch()
  {
    is_match_ = false;
    is_prefix_ = false;
  }

  void set_match() { is_match_ = true; }

  void set_prefix() { is_prefix_ = true; }

  ObDATrieHit &operator=(const ObDATrieHit &other)
  {
    char_cnt_ = other.char_cnt_;
    start_pos_ = other.start_pos_;
    end_pos_ = other.end_pos_;
    base_idx_ = other.base_idx_;
    current_check_ = other.current_check_;
    is_match_ = other.is_match_;
    is_prefix_ = other.is_prefix_;
    return *this;
  }

  bool operator==(const ObDATrieHit &other) const
  {
    return start_pos_ == other.start_pos_ && end_pos_ == other.end_pos_
           && base_idx_ == other.base_idx_ && current_check_ == other.current_check_;
  }

public:
  const ObIFTDict *dict_;
  // Character count already matched.
  int64_t char_cnt_;

  // The start position in original text.
  int64_t start_pos_;

  // The end position in original text already matched.
  int64_t end_pos_;

  // The base index in Double-Array-Trie.
  uint32_t base_idx_;

  // The current check index in Double-Array-Trie.
  uint32_t current_check_;

  // Status of matching.
  bool is_match_;  // full match
  bool is_prefix_; // prefix match
};

/**
 * @class ObIFTDict
 * @brief Dict interface
 */
class ObIFTDict
{
public:
  ObIFTDict() = default;
  virtual ~ObIFTDict() = default;

  virtual int init() = 0;

  // Match words in dict.
  virtual int match(const ObString &words, bool &is_match) const = 0;

  // Match single word.
  virtual int match(const ObString &single_word, ObDATrieHit &hit) const = 0;

  // Match single word with hit.
  virtual int match_with_hit(const ObString &single_word,
                             const ObDATrieHit &last_hit,
                             ObDATrieHit &hit) const = 0;
};

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_H_
