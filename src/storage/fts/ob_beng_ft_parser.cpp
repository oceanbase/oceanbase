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

#define USING_LOG_PREFIX STORAGE_FTS

#include "lib/string/ob_string.h"
#include "storage/fts/ob_beng_ft_parser.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

int ObBEngFTParser::get_next_token(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  ObDatum token;
  int64_t token_freq = 0;
  char *buf = nullptr;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("beng ft parser isn't initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(token_stream_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("token stream is nullptr", K(ret), KP(token_stream_));
  } else if (OB_FAIL(token_stream_->get_next(token, token_freq))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next token", K(ret), KPC(token_stream_));
    }
  } else if (OB_ISNULL(token.ptr_) || OB_UNLIKELY(0 >= token.len_ || 0 >= token_freq)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(token.ptr_), K(token.len_), K(token_freq));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(token.len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate word memory", K(ret), K(token.len_));
  } else {
    MEMCPY(buf, token.ptr_, token.len_);
    word = buf;
    word_len = token.len_;
    char_len = token.len_;
    word_freq = token_freq;
    LOG_DEBUG("succeed to add word", K(ObString(word_len, word)), K(word_freq));
  }
  return ret;
}

int ObBEngFTParser::init(lib::ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is nullptr", K(ret), KPC(param));
  } else if (OB_UNLIKELY(UINT32_MAX < param->ft_length_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("too large document, english analyzer hasn't be supported", K(ret), K(param->ft_length_));
  } else {
    doc_.set_string(param->fulltext_, param->ft_length_);
    analysis_ctx_.cs_ = param->cs_;
    analysis_ctx_.filter_stopword_ = false;
    analysis_ctx_.need_grouping_ = false;
    if (OB_FAIL(english_analyzer_.init(analysis_ctx_, *param->allocator_))) {
      LOG_WARN("fail to init english analyzer", K(ret), KPC(param), K(analysis_ctx_));
    } else if (OB_FAIL(segment(doc_, token_stream_))) {
      LOG_WARN("fail to segment fulltext by parser", K(ret), KP(param->fulltext_), K(param->ft_length_));
    } else if (OB_ISNULL(token_stream_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("token stream is nullptr", K(ret), KP(token_stream_));
    } else {
      is_inited_ = true;
      LOG_DEBUG("succeed to init beng parser", K(ret), K(english_analyzer_), KPC(token_stream_), K(doc_));
    }
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObBEngFTParser::segment(
    const common::ObDatum &doc,
    share::ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(doc.ptr_) || OB_UNLIKELY(0 >= doc.len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(doc.ptr_), K(doc.len_));
  } else if (OB_UNLIKELY(UINT32_MAX < doc.len_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("too large document, english analyzer hasn't be supported", K(ret), K(doc.len_));
  } else if (OB_FAIL(english_analyzer_.analyze(doc, token_stream))) {
    LOG_WARN("fail to analyze document", K(ret), K(english_analyzer_), KP(doc.ptr_), K(doc.len_));
  }
  return ret;
}

void ObBEngFTParser::reset()
{
  analysis_ctx_.reset();
  english_analyzer_.reset();
  doc_.reset();
  token_stream_ = nullptr;
  is_inited_ = false;
}

ObBasicEnglishFTParserDesc::ObBasicEnglishFTParserDesc()
  : is_inited_(false)
{
}

int ObBasicEnglishFTParserDesc::init(lib::ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObBasicEnglishFTParserDesc::deinit(lib::ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObBasicEnglishFTParserDesc::segment(
    lib::ObFTParserParam *param,
    lib::ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("default ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_ISNULL(buf = param->allocator_->alloc(sizeof(ObBEngFTParser)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate basic english ft parser", K(ret));
  } else {
    ObBEngFTParser *parser = new (buf) ObBEngFTParser(*(param->allocator_));
    if (OB_FAIL(parser->init(param))) {
      LOG_WARN("fail to init basic english parser", K(ret), KPC(param));
    } else {
      iter = parser;
    }
  }
  return ret;
  return ret;
}

void ObBasicEnglishFTParserDesc::free_token_iter(
    lib::ObFTParserParam *param,
    lib::ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    abort_unless(nullptr != param);
    abort_unless(nullptr != param->allocator_);
    iter->~ObITokenIterator();
    param->allocator_->free(iter);
  }
}

} // end namespace storage
} // end namespace oceanbase
