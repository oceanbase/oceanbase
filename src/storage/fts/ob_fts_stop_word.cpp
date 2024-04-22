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

ObNoStopWordAddWord::ObNoStopWordAddWord(
    const ObCollationType &type,
    common::ObIAllocator &allocator,
    common::ObIArray<ObFTWord> &word)
  : collation_type_(type),
    allocator_(allocator),
    words_(word),
    word_count_(0)
{
}

int ObNoStopWordAddWord::operator()(
    lib::ObFTParserParam *param,
    const char *word,
    const int64_t word_len)
{
  int ret = OB_SUCCESS;
  char *w_buf = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(word), K(word_len));
  } else if (OB_ISNULL(w_buf = static_cast<char *>(allocator_.alloc(word_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for fulltext word", K(ret), K(word_len));
  } else {
    MEMCPY(w_buf, word, word_len);
    ObFTWord ft_word(word_len, w_buf, collation_type_);
    if (OB_FAIL(words_.push_back(ft_word))) {
      LOG_WARN("fail to push word into words array", K(ret), K(ft_word));
    } else {
      ++word_count_;
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(w_buf)) {
      allocator_.free(w_buf);
      w_buf = nullptr;
    }
  }
  LOG_DEBUG("add word", K(ret), KPC(param), KP(word), K(word_len));
  return ret;
}

ObStopWordAddWord::ObStopWordAddWord(
    const ObCollationType &type,
    common::ObIAllocator &allocator,
    common::ObIArray<ObFTWord> &word)
  : collation_type_(type),
    allocator_(allocator),
    words_(word),
    non_stopword_count_(0),
    stopword_count_(0)
{
}

int ObStopWordAddWord::operator()(
    lib::ObFTParserParam *param,
    const char *word,
    const int64_t word_len)
{
  int ret = OB_SUCCESS;
  bool is_stopword = false;
  ObFTWord ft_word(word_len, word, collation_type_);
  if (OB_ISNULL(param) || OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(word), K(word_len));
  } else if (OB_FAIL(OB_FT_PLUGIN_MGR.check_stopword(ft_word, is_stopword))) {
    LOG_WARN("fail to check stopword", K(ret));
  } else if (is_stopword) {
    // the word is stop word, just skip it.
    ++stopword_count_;
  } else {
    char *w_buf = nullptr;
    if (OB_ISNULL(w_buf = static_cast<char *>(allocator_.alloc(word_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for fulltext word", K(ret), K(word_len));
    } else {
      MEMCPY(w_buf, word, word_len);
      ObFTWord non_stopword_ft_word(word_len, w_buf, collation_type_);
      if (OB_FAIL(words_.push_back(non_stopword_ft_word))) {
        LOG_WARN("fail to push word into words array", K(ret), K(non_stopword_ft_word));
      } else {
        ++non_stopword_count_;
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(w_buf)) {
        allocator_.free(w_buf);
        w_buf = nullptr;
      }
    }
  }
  LOG_DEBUG("add word", K(ret), KPC(param), KP(word), K(word_len), K(is_stopword));
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
