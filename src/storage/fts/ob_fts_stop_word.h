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

class ObNoStopWordAddWord final : public lib::ObFTParserParam::ObIAddWord
{
public:
  ObNoStopWordAddWord(
      const ObCollationType &type,
      common::ObIAllocator &allocator,
      common::ObIArray<ObFTWord> &word);
  virtual ~ObNoStopWordAddWord() = default;
  virtual int operator()(
      lib::ObFTParserParam *param,
      const char *word,
      const int64_t word_len) override;
  virtual int64_t get_add_word_count() const override { return word_count_; }
  VIRTUAL_TO_STRING_KV(K_(collation_type), K_(word_count), K_(words));
private:
  OB_INLINE common::ObIArray<ObFTWord> &get_words() { return words_; }
private:
  ObCollationType collation_type_;
  common::ObIAllocator &allocator_;
  common::ObIArray<ObFTWord> &words_;
  int64_t word_count_;
};

class ObStopWordAddWord final : public lib::ObFTParserParam::ObIAddWord
{
public:
  ObStopWordAddWord(
      const ObCollationType &type,
      common::ObIAllocator &allocator,
      common::ObIArray<ObFTWord> &word);
  virtual ~ObStopWordAddWord() = default;
  virtual int operator()(
      lib::ObFTParserParam *param,
      const char *word,
      const int64_t word_len) override;
  virtual int64_t get_add_word_count() const { return non_stopword_count_; }
  VIRTUAL_TO_STRING_KV(K_(collation_type), K_(non_stopword_count), K_(stopword_count), K_(words));
private:
  ObCollationType collation_type_;
  common::ObIAllocator &allocator_;
  common::ObIArray<ObFTWord> &words_;
  int64_t non_stopword_count_;
  int64_t stopword_count_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_STOP_WORD_H_
