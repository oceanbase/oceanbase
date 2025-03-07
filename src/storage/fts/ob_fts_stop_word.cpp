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

#include "share/rc/ob_tenant_base.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace storage
{

////////////////////////////////////////////////////////////////////////////////
// class ObStopWordChecker
ObStopWordChecker::~ObStopWordChecker()
{
  destroy();
}

int ObStopWordChecker::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (OB_INVALID_TENANT_ID == tenant_id) {
    tenant_id = OB_SERVER_TENANT_ID;
  }

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(stopword_set_.create(DEFAULT_STOPWORD_BUCKET_NUM, "StopWordSet", "StopWordSet", tenant_id))) {
    LOG_WARN("fail to create stop word set", K(ret));
  } else {
    stopword_type_ = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    const int64_t stopword_count = sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]);
    for (int64_t i = 0; OB_SUCC(ret) && i < stopword_count; ++i) {
      ObFTWord stopword(STRLEN(ob_stop_word_list[i]), ob_stop_word_list[i], stopword_type_);
      if (OB_FAIL(stopword_set_.set_refactored(stopword))) {
        LOG_WARN("fail to set stop word", K(ret), K(stopword));
      }
    }

    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

void ObStopWordChecker::destroy()
{
  if (inited_) {
    stopword_set_.destroy();
    stopword_type_ = ObCollationType::CS_TYPE_INVALID;
    inited_ = false;
  }
}

int ObStopWordChecker::check_stopword(const ObFTWord &word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (OB_INVALID_TENANT_ID == tenant_id) {
    tenant_id = OB_SERVER_TENANT_ID;
  }
  common::ObArenaAllocator allocator(lib::ObMemAttr(tenant_id, "ChkStopWord"));
  common::ObString cmp_word_str;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStopWordChecker hasn't been initialized", K(ret), K(inited_));
  } else if (OB_UNLIKELY(word.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("word is empty", K(ret), K(word));
  } else {
    const bool conv_cs = word.get_collation_type() != stopword_type_;
    if (conv_cs && OB_FAIL(common::ObCharset::charset_convert(allocator, word.get_word(), word.get_collation_type(),
                                                              stopword_type_, cmp_word_str))) {
      LOG_WARN("fail to convert charset", K(ret), K(word), K(stopword_type_));
    } else {
      ObFTWord cmp_word(cmp_word_str.length(), cmp_word_str.ptr(), stopword_type_);
      ret = stopword_set_.exist_refactored(conv_cs ? cmp_word : word);
      if (OB_HASH_NOT_EXIST == ret) {
        is_stopword = false;
        ret = OB_SUCCESS;
      } else if (OB_HASH_EXIST == ret) {
        is_stopword = true;
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the exist of hastset shouldn't return success", K(ret), K(word), K(conv_cs), K(cmp_word));
      } else {
        LOG_WARN("fail to do exist", K(ret), K(word), K(conv_cs), K(cmp_word));
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// class ObAddWord
ObAddWord::ObAddWord(const ObFTParserProperty &property,
                     const ObCollationType &type,
                     const ObAddWordFlag &flag,
                     common::ObIAllocator &allocator,
                     ObFTWordMap &word_map)
    : collation_type_(type), allocator_(allocator), word_map_(word_map), min_max_word_cnt_(0),
      non_stopword_cnt_(0), stopword_cnt_(0), min_token_size_(property.min_token_size_),
      max_token_size_(property.max_token_size_), flag_(flag)
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
  } else if (OB_FAIL(check_stopword(dst_word, is_stopword))) {
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
  return flag_.min_max_word() && (c_len < min_token_size_ || c_len > max_token_size_);
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
  } else if (flag_.stopword()) {
    ObStopWordChecker *stop_word_checker = ObFTParsePluginData::instance().stop_word_checker();
    if (OB_ISNULL(stop_word_checker)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got null stop word checker", K(ret));
    } else if (OB_FAIL(stop_word_checker->check_stopword(ft_word, is_stopword))) {
      LOG_WARN("fail to check stopword", K(ret));
    }
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
