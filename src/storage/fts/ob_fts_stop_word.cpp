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
    common::ObIArray<ObFTWord> &word)
  : collation_type_(type),
    allocator_(allocator),
    words_(word),
    min_max_word_cnt_(0),
    non_stopword_cnt_(0),
    stopword_cnt_(0),
    flag_(flag)
{
}

int ObAddWord::operator()(
    lib::ObFTParserParam *param,
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt)
{
  int ret = OB_SUCCESS;
  bool is_stopword = false;
  ObFTWord src_word(word_len, word, collation_type_);
  ObFTWord dst_word;
  if (OB_ISNULL(param) || OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(word), K(word_len));
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
  } else if (OB_FAIL(words_.push_back(dst_word))) {
    LOG_WARN("fail to push word into words array", K(ret), K(dst_word));
  } else {
    ++non_stopword_cnt_;
    LOG_DEBUG("add word", K(ret), KPC(param), KP(word), K(word_len), K(char_cnt), K(src_word), K(dst_word));
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

} // end namespace storage
} // end namespace oceanbase
