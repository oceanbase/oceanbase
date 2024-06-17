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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_plugin_mgr.h"

namespace oceanbase
{
namespace storage
{

ObAddWord::ObAddWord(
    const ObCollationType &type,
    const ObAddWordFlag &flag,
    common::ObIAllocator &allocator,
    ObFTWordMap &word_map)
  : collation_type_(type),
    allocator_(allocator),
    word_map_(word_map),
    min_max_word_cnt_(0),
    non_stopword_cnt_(0),
    stopword_cnt_(0),
    flag_(flag)
{
}

int ObAddWord::process_word(
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt,
    const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  bool is_stopword = false;
  ObFTWord src_word(word_len, word, collation_type_);
  ObFTWord dst_word;
  if (OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len || 0 >= char_cnt || 0 >= word_freq)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(word), K(word_len), K(char_cnt), K(word_freq));
  } else if (is_min_max_word(char_cnt)) {
    ++min_max_word_cnt_;
    LOG_DEBUG("skip too small or large word", K(ret), K(src_word), K(char_cnt));
  } else if (OB_FAIL(casedown_word(src_word, dst_word))) {
    LOG_WARN("fail to casedown word", K(ret), K(src_word));
  } else if (check_stopword(dst_word, is_stopword)) {
    LOG_WARN("fail to check stopword", K(ret), K(dst_word));
  } else if (OB_UNLIKELY(is_stopword)) {
    ++stopword_cnt_;
    LOG_DEBUG("skip stopword", K(ret), K(dst_word));
  } else if (OB_FAIL(groupby_word(dst_word, word_freq))) {
    LOG_WARN("fail to groupby word into word map", K(ret), K(dst_word), K(word_freq));
  } else {
    non_stopword_cnt_ += word_freq;
    LOG_DEBUG("add word", K(ret), KP(word), K(word_len), K(char_cnt), K(word_freq), K(src_word), K(dst_word));
  }
  return ret;
}

bool ObAddWord::is_min_max_word(const int64_t c_len) const
{
  return flag_.min_max_word() && (c_len < FT_MIN_WORD_LEN || c_len > FT_MAX_WORD_LEN);
}

int ObAddWord::casedown_word(const ObFTWord &src, ObFTWord &dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src ft word", K(ret), K(src));
  } else if (flag_.casedown()) {
    ObString dst_str;
    if (OB_FAIL(ObCharset::tolower(collation_type_, src.get_word(), dst_str, allocator_))) {
      LOG_WARN("fail to tolower", K(ret), K(src), K(collation_type_));
    } else {
      ObFTWord tmp(dst_str.length(), dst_str.ptr(), collation_type_);
      dst = tmp;
    }
  } else {
    dst = src;
  }
  return ret;
}

int ObAddWord::check_stopword(const ObFTWord &ft_word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ft_word.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ft_word));
  } else if (flag_.stopword() && OB_FAIL(OB_FT_PLUGIN_MGR.check_stopword(ft_word, is_stopword))) {
    LOG_WARN("fail to check stopword", K(ret));
  }
  return ret;
}

int ObAddWord::groupby_word(const ObFTWord &word, const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  int64_t word_count = 0;
  if (OB_UNLIKELY(word.empty() || word_freq <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(word), K(word_freq));
  } else if (!flag_.groupby_word()) {
    if (OB_FAIL(word_map_.set_refactored(word, 1/*word count*/))) {
      LOG_WARN("fail to set fulltext word and count", K(ret), K(word));
    }
  } else if (OB_FAIL(word_map_.get_refactored(word, word_count)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("fail to get fulltext word", K(ret), K(word));
  } else {
    if (OB_HASH_NOT_EXIST == ret) {
      word_count = 1;
    } else {
      word_count += word_freq;
    }
    if (OB_FAIL(word_map_.set_refactored(word, word_count, 1/*overwrite*/))) {
      LOG_WARN("fail to set fulltext word and count", K(ret), K(word), K(word_count));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
