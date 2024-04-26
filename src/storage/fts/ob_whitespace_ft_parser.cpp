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
#include "storage/fts/ob_whitespace_ft_parser.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

/*static*/ int ObSpaceFTParser::segment(
    lib::ObFTParserParam *param,
    const char *ft,
    const int64_t ft_len)
{
  int ret = OB_SUCCESS;
  ObDatum doc;
  doc.set_string(ft, ft_len);
  ObSpaceFTParser parser;
  share::ObITokenStream *token_stream = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(param->cs_) || OB_ISNULL(ft) || OB_UNLIKELY(0 >= ft_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(ft), K(ft_len));
  } else if (OB_FAIL(parser.init(param))) {
    LOG_WARN("fail to initialize space parser", K(ret), KPC(param));
  } else if (FALSE_IT(doc.set_string(ft, ft_len))) {
  } else if (OB_FAIL(parser.segment(doc, token_stream))) {
    LOG_WARN("fail to segment fulltext by parser", K(ret), KP(ft), K(ft_len));
  } else if (OB_ISNULL(token_stream)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("token stream is nullptr", K(ret), KP(token_stream));
  } else {
    ObDatum token;
    int64_t token_freq = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(token_stream->get_next(token, token_freq))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next token", K(ret), KPC(token_stream));
        }
      } else if (OB_FAIL(add_word(param, param->allocator_, token.ptr_, token.len_))) {
        LOG_WARN("fail to add word", K(ret), K(token), KPC(param));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/*static*/ int ObSpaceFTParser::add_word(
    lib::ObFTParserParam *param,
    common::ObIAllocator *allocator,
    const char *word,
    int64_t word_len)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_ISNULL(param)
      || OB_ISNULL(allocator)
      || OB_ISNULL(word)
      || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(allocator), KP(word), K(word_len));
  } else if (word_len < FT_MIN_WORD_LEN || word_len > FT_MAX_WORD_LEN) {
    LOG_DEBUG("skip too small or large word", K(ret), K(word_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(word_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate word memory", K(ret), K(word_len));
  } else if (FALSE_IT(MEMCPY(buf, word, word_len))) {
  } else if (OB_FAIL(param->add_word(param, buf, word_len))) {
    LOG_WARN("fail to add word", K(ret), KPC(param), K(ObString(word_len, buf)), K(ObString(word_len, word)));
  } else {
    LOG_DEBUG("succeed to add word", K(ObString(word_len, word)));
  }
  return ret;
}

int ObSpaceFTParser::init(lib::ObFTParserParam *param)
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
    analysis_ctx_.cs_ = param->cs_;
    analysis_ctx_.filter_stopword_ = false;
    analysis_ctx_.need_grouping_ = false;
    if (OB_FAIL(english_analyzer_.init(analysis_ctx_, *param->allocator_))) {
      LOG_WARN("fail to init english analyzer", K(ret), KPC(param), K(analysis_ctx_));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObSpaceFTParser::segment(
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

void ObSpaceFTParser::reset()
{
  analysis_ctx_.reset();
  english_analyzer_.reset();
  is_inited_ = false;
}

ObWhiteSpaceFTParserDesc::ObWhiteSpaceFTParserDesc()
  : is_inited_(false)
{
}

int ObWhiteSpaceFTParserDesc::init(lib::ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObWhiteSpaceFTParserDesc::deinit(lib::ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObWhiteSpaceFTParserDesc::segment(lib::ObFTParserParam *param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("default ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_FAIL(ObSpaceFTParser::segment(param, param->fulltext_, param->ft_length_))) {
    LOG_WARN("fail to segment words for fulltext by spaces", K(ret), KPC(param),
        K(param->fulltext_), K(param->ft_length_));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
