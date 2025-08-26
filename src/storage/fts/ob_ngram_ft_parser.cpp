/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/fts/utils/ob_ft_ngram_impl.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_ngram_ft_parser.h"
#include "storage/fts/ob_fts_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{

ObNgramFTParser::ObNgramFTParser() : ngram_impl_(), is_inited_(false) {}

ObNgramFTParser::~ObNgramFTParser()
{
  reset();
}

void ObNgramFTParser::reset()
{
  ngram_impl_.reset();
  is_inited_ = false;
}

int ObNgramFTParser::init(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(param), KPC(this));
  } else if (OB_ISNULL(param)
      || OB_ISNULL(param->cs_)
      || OB_ISNULL(param->fulltext_)
      || OB_UNLIKELY(0 >= param->ft_length_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param));
  } else if (OB_FAIL(ngram_impl_.init(param->cs_,
                                      param->fulltext_,
                                      param->ft_length_,
                                      param->ngram_token_size_,
                                      param->ngram_token_size_))) {
    LOG_WARN("fail to init ngram impl", K(ret), KPC(param));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObNgramFTParser::get_next_token(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ngram ft parser isn't initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(ngram_impl_.get_next_token(word, word_len, char_len, word_freq))) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("fail to get next token", K(ret));
    }
  }
  return ret;
}

ObNgramFTParserDesc::ObNgramFTParserDesc()
  : is_inited_(false)
{
}

int ObNgramFTParserDesc::init(ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObNgramFTParserDesc::deinit(ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObNgramFTParserDesc::segment(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObNgramFTParser *parser = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ngram ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_ISNULL(parser = OB_NEWx(ObNgramFTParser, param->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ngram ft parser", K(ret));
  } else {
    if (OB_FAIL(parser->init(param))) {
      LOG_WARN("fail to init ngram fulltext parser", K(ret), KPC(param));
    } else {
      iter = parser;
    }
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObNgramFTParser, param->allocator_, parser);
  }
  return ret;
}

void ObNgramFTParserDesc::free_token_iter(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    abort_unless(nullptr != param);
    abort_unless(nullptr != param->allocator_);
    iter->~ObITokenIterator();
    param->allocator_->free(iter);
  }
}

int ObNgramFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
