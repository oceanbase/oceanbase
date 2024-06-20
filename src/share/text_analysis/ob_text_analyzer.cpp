/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/text_analysis/ob_text_analyzer.h"

namespace oceanbase
{
namespace share
{

void ObITextAnalyzer::reset()
{
  for (int64_t i = analyze_pipeline_.count() - 1; i >= 0; --i) {
    ObITokenStream *cur_ts = analyze_pipeline_.at(i);
    cur_ts->~ObITokenStream();
    if (nullptr != allocator_) {
      allocator_->free(cur_ts);
    }
  }
  allocator_ = nullptr;
  ctx_ = nullptr;
  analyze_pipeline_.reset();
  is_inited_ = false;
}

int ObITextAnalyzer::init(const ObTextAnalysisCtx &ctx, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid text analysis ctx", K(ret), K(ctx));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (OB_FAIL(inner_init(ctx, allocator))) {
    LOG_WARN("failed to inner init analyzer", K(ret), K(ctx));
  } else {
    ctx_ = &ctx;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObITextAnalyzer::add_tokenizer(const ObTextTokenizer::TokenizerType &type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!analyze_pipeline_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is already an tokenizer in analyse pipeline", K(ret));
  } else {
    ObITokenStream *token_stream = nullptr;
    switch (type) {
    case ObTextTokenizer::WHITESPACE: {
      if (OB_FAIL(add_token_stream<ObTextWhitespaceTokenizer>(token_stream))) {
        LOG_WARN("failed to add token stread", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported tokenizer type", K(ret));
    }
    }
    if (FAILEDx(analyze_pipeline_.push_back(token_stream))) {
      LOG_WARN("failed to add tokenizer to analyse pipeline", K(ret),
          K(type), KPC(token_stream), K_(analyze_pipeline));
    }
  }
  return ret;
}

int ObITextAnalyzer::add_normalizer(
    const ObTokenNormalizer::TokenNormalizerType &type,
    const ObTextAnalysisCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(analyze_pipeline_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot add a normalizer to an empty analyse pipeline", K(ret), K(type));
  } else {
    ObITokenStream *token_stream = nullptr;
    switch (type) {
    case ObTokenNormalizer::STOPWORD_FILTER: {
      if (OB_FAIL(add_token_stream<ObTokenStopWordNormalizer>(token_stream))) {
        LOG_WARN("failed to add token stop word filter", K(ret));
      }
      break;
    }
    case ObTokenNormalizer::TEXT_GROUPING_FILTER: {
      if (OB_FAIL(add_token_stream<ObTextTokenGroupNormalizer>(token_stream))) {
        LOG_WARN("failed to add text grouping filter", K(ret));
      }
      break;
    }
    case ObTokenNormalizer::ENG_BASIC_NORM: {
      if (OB_FAIL(add_token_stream<ObBasicEnglishNormalizer>(token_stream))) {
        LOG_WARN("failed to add basic english normalizer", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported tokenizer type", K(ret));
    }
    }

    if (OB_SUCC(ret)) {
      ObTokenNormalizer *normalizer = static_cast<ObTokenNormalizer *>(token_stream);
      if (OB_FAIL(normalizer->init(ctx.cs_, *get_tail_token_stream()))) {
        LOG_WARN("failed to init normalizer", K(ret), K(ctx), K(type), KPC(normalizer));
      } else if (OB_FAIL(analyze_pipeline_.push_back(normalizer))) {
        LOG_WARN("failed to add normalizer to analyse pipeline", K(ret),
            K(type), K(ctx), KPC(normalizer), K_(analyze_pipeline));
      }
    }
  }
  return ret;
}

template<typename T>
int ObITextAnalyzer::add_token_stream(ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(sizeof(T))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(sizeof(T)));
  } else {
    token_stream = new (buf) T();
  }
  return ret;
}

ObITokenStream *ObITextAnalyzer::get_tail_token_stream()
{
  OB_ASSERT(!analyze_pipeline_.empty());
  ObITokenStream *tail_stream = analyze_pipeline_.at(analyze_pipeline_.count() - 1);
  OB_ASSERT(nullptr != tail_stream);
  return tail_stream;
}

int ObEnglishTextAnalyzer::inner_init(const ObTextAnalysisCtx &ctx, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  UNUSEDx(ctx); // TODO: generate specific analyse pipeline by ctx
  if (OB_FAIL(add_tokenizer(ObTextTokenizer::WHITESPACE))) {
    LOG_WARN("failed to add white space tokenizer", K(ret));
  } else if (ctx.filter_stopword_ && OB_FAIL(add_normalizer(ObTokenNormalizer::STOPWORD_FILTER, ctx))) {
    LOG_WARN("failed to add stop word filter", K(ret));
  } else if (OB_FAIL(add_normalizer(ObTokenNormalizer::ENG_BASIC_NORM, ctx))) {
    LOG_WARN("failed to add basic english normalizer", K(ret));
  } else if (ctx.need_grouping_ && OB_FAIL(add_normalizer(ObTokenNormalizer::TEXT_GROUPING_FILTER, ctx))) {
    LOG_WARN("failed to add text grouping filter", K(ret));
  }
  return ret;
}

int ObEnglishTextAnalyzer::analyze(const ObDatum &document, ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    get_tail_token_stream()->reuse();
    ObTextTokenizer *tokenizer = static_cast<ObTextTokenizer *>(analyze_pipeline_.at(0));
    if (OB_FAIL(tokenizer->open(document, ctx_->cs_))) {
      LOG_WARN("failed to open tokenizer", K(ret), KPC_(ctx));
    } else {
      token_stream = get_tail_token_stream();
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
