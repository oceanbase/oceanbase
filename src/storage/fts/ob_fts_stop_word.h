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

#ifndef OB_FTS_STOP_WORD_H_
#define OB_FTS_STOP_WORD_H_

#include "lib/ob_plugin.h"
#include "lib/container/ob_iarray.h"
#include "storage/fts/ob_fts_struct.h"

namespace oceanbase
{
namespace storage
{

#define FTS_STOP_WORD_MAX_LENGTH 10

static const char ob_stop_word_list[][FTS_STOP_WORD_MAX_LENGTH] = {
  "a",
  "about",
  "an",
  "are",
  "as",
  "at",
  "be",
  "by",
  "com",
  "de",
  "en",
  "for",
  "from",
  "how",
  "i",
  "in",
  "is",
  "it",
  "la",
  "of",
  "on",
  "or",
  "that",
  "the",
  "this",
  "to",
  "was",
  "what",
  "when",
  "where",
  "who",
  "will",
  "with",
  "und",
  "the",
  "www"
};

class ObAddWord final
{
public:
  ObAddWord(
      const ObCollationType &type,
      const ObAddWordFlag &flag,
      common::ObIAllocator &allocator,
      ObFTWordMap &word_map);
  ~ObAddWord() = default;
  int process_word(
      const char *word,
      const int64_t word_len,
      const int64_t char_cnt,
      const int64_t word_freq);
  virtual int64_t get_add_word_count() const { return non_stopword_cnt_; }
  VIRTUAL_TO_STRING_KV(K_(collation_type), K_(min_max_word_cnt), K_(non_stopword_cnt), K_(stopword_cnt),
      K(word_map_.size()));
public:
  static const int64_t FT_MIN_WORD_LEN = 3;
  static const int64_t FT_MAX_WORD_LEN = 84;
private:
  bool is_min_max_word(const int64_t c_len) const;
  int casedown_word(const ObFTWord &src, ObFTWord &dst);
  int check_stopword(const ObFTWord &word, bool &is_stopword);
  int groupby_word(const ObFTWord &word, const int64_t word_cnt);
private:
  ObCollationType collation_type_;
  common::ObIAllocator &allocator_;
  ObFTWordMap &word_map_;
  int64_t min_max_word_cnt_;
  int64_t non_stopword_cnt_;
  int64_t stopword_cnt_;
  ObAddWordFlag flag_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_STOP_WORD_H_
