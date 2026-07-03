/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/fts/analyzer/tokenizer/ob_legacy_parser_tokenizer.h"
#include "storage/fts/analyzer/ob_analyzer.h"
#include "storage/fts/ob_i_ft_parser.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"
#include "storage/fts/ob_whitespace_ft_parser.h"
#include "storage/fts/ob_ngram_ft_parser.h"
#include "storage/fts/ob_beng_ft_parser.h"
#include "storage/fts/ob_ik_ft_parser.h"
#include "storage/fts/ob_ngram2_ft_parser.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace storage
{

// ============================================================
// ObLegacyParserTokenizer — base class shared implementation
// ============================================================

int ObLegacyParserTokenizer::set_input(
    const char *text,
    int64_t text_len,
    ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tokenizer is not initialized", K(ret));
  } else if (OB_UNLIKELY(text_len < 0) || (OB_ISNULL(text) && text_len > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(text), K(text_len));
  } else if (OB_ISNULL(cs = common::ObCharset::get_charset(coll_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get charset info", K(ret), K(coll_type));
  } else {
    position_ = 0;
    empty_input_ = false;
    if (text_len <= 0) {
      // Mark empty input so get_next_token() returns OB_ITER_END directly.
      // Keep the parser alive to avoid expensive teardown/rebuild on subsequent
      // non-empty rows (e.g. IK dict reload).
      empty_input_ = true;
    } else if (OB_NOT_NULL(legacy_parser_)) {
      // Reuse the existing parser to avoid expensive re-init (e.g. IK dict reload
      // would otherwise do a full table scan of the dict table per row in DDL mode).
      // Tokenizer is bound to a fixed source collation at create time, so reusing is safe.
      if (OB_FAIL(legacy_parser_->reuse_parser(text, text_len))) {
        LOG_WARN("failed to reuse legacy parser", K(ret), K(text_len));
      }
    } else if (OB_FAIL(create_parser_impl(cs, text, text_len))) {
      LOG_WARN("failed to create parser", K(ret), K(text_len));
    }
  }
  return ret;
}

int ObLegacyParserTokenizer::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  const char *word = nullptr;
  int64_t word_len = 0;
  int64_t char_len = 0;
  int64_t word_freq = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tokenizer is not initialized", K(ret));
  } else if (empty_input_ || OB_ISNULL(legacy_parser_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(legacy_parser_->get_next_token(word, word_len, char_len, word_freq))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next token from legacy parser", K(ret));
    }
  } else if (OB_FAIL(convert_token(word, word_len, token))) {
    LOG_WARN("failed to convert token", K(ret), K(word_len));
  }

  return ret;
}

void ObLegacyParserTokenizer::reset()
{
  destroy_parser_impl();
  metadata_alloc_ = nullptr;
  scratch_alloc_ = nullptr;
  position_ = 0;
  is_inited_ = false;
  empty_input_ = false;
}

void ObLegacyParserTokenizer::destroy_parser_impl()
{
  if (OB_NOT_NULL(legacy_parser_)) {
    legacy_parser_->~ObIFTParser();
    if (OB_NOT_NULL(metadata_alloc_)) {
      metadata_alloc_->free(legacy_parser_);
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "legacy_parser_ not null but metadata_alloc_ is null, potential leak", KP(legacy_parser_));
    }
    legacy_parser_ = nullptr;
  }
}

int ObLegacyParserTokenizer::convert_token(
    const char *word,
    int64_t word_len,
    ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(word) || OB_UNLIKELY(word_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token from legacy parser", K(ret), KP(word), K(word_len));
  } else {
    token.token_ptr_ = word;
    token.token_len_ = static_cast<int32_t>(word_len);
    token.pos_inc_ = 1;
    token.is_keyword_ = false;
    position_++;
  }
  return ret;
}

// ============================================================
// ObSpaceTokenizer
// ============================================================

int ObSpaceTokenizer::init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("space tokenizer is already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_SPACE != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type for space tokenizer", K(ret), K(spec.type_));
  } else {
    metadata_alloc_ = &alloc;
    is_inited_ = true;
  }
  return ret;
}

int ObSpaceTokenizer::create_parser_impl(
    const ObCharsetInfo *cs, const char *text, int64_t text_len)
{
  int ret = OB_SUCCESS;
  ObSpaceFTParser *parser = nullptr;
  if (OB_ISNULL(metadata_alloc_) || OB_ISNULL(scratch_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tokenizer allocators not initialized", K(ret), KP(metadata_alloc_), KP(scratch_alloc_));
  } else if (OB_ISNULL(parser = OB_NEWx(ObSpaceFTParser, metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate space parser", K(ret));
  } else {
    plugin::ObFTParserParam param;
    param.cs_ = cs;
    param.fulltext_ = text;
    param.ft_length_ = text_len;
    param.parser_version_ = ANALYZER_PARSER_VERSION_PLACEHOLDER;
    param.metadata_alloc_ = metadata_alloc_;
    param.scratch_alloc_ = scratch_alloc_;
    if (OB_FAIL(parser->init(&param))) {
      LOG_WARN("failed to init space parser", K(ret));
      parser->~ObSpaceFTParser();
      metadata_alloc_->free(parser);
    } else {
      legacy_parser_ = parser;
    }
  }
  return ret;
}

// ============================================================
// ObNgramTokenizer
// ============================================================

int ObNgramTokenizer::init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ngram tokenizer is already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_NGRAM != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type for ngram tokenizer", K(ret), K(spec.type_));
  } else {
    const ObNgramTokenizerSpec &ngram_spec = static_cast<const ObNgramTokenizerSpec &>(spec);
    ngram_token_size_ = ngram_spec.ngram_token_size_;
    metadata_alloc_ = &alloc;
    is_inited_ = true;
  }
  return ret;
}

int ObNgramTokenizer::create_parser_impl(
    const ObCharsetInfo *cs, const char *text, int64_t text_len)
{
  int ret = OB_SUCCESS;
  ObNgramFTParser *parser = nullptr;
  if (OB_ISNULL(metadata_alloc_) || OB_ISNULL(scratch_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tokenizer allocators not initialized", K(ret), KP(metadata_alloc_), KP(scratch_alloc_));
  } else if (OB_ISNULL(parser = OB_NEWx(ObNgramFTParser, metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ngram parser", K(ret));
  } else {
    plugin::ObFTParserParam param;
    param.cs_ = cs;
    param.fulltext_ = text;
    param.ft_length_ = text_len;
    param.parser_version_ = ANALYZER_PARSER_VERSION_PLACEHOLDER;
    param.metadata_alloc_ = metadata_alloc_;
    param.scratch_alloc_ = scratch_alloc_;
    param.ngram_token_size_ = ngram_token_size_;
    if (OB_FAIL(parser->init(&param))) {
      LOG_WARN("failed to init ngram parser", K(ret));
      parser->~ObNgramFTParser();
      metadata_alloc_->free(parser);
    } else {
      legacy_parser_ = parser;
    }
  }
  return ret;
}

// ============================================================
// ObBengTokenizer
// ============================================================

int ObBengTokenizer::init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("beng tokenizer is already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_BENG != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type for beng tokenizer", K(ret), K(spec.type_));
  } else {
    metadata_alloc_ = &alloc;
    is_inited_ = true;
  }
  return ret;
}

int ObBengTokenizer::create_parser_impl(
    const ObCharsetInfo *cs, const char *text, int64_t text_len)
{
  int ret = OB_SUCCESS;
  ObBEngFTParser *parser = nullptr;
  if (OB_ISNULL(metadata_alloc_) || OB_ISNULL(scratch_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tokenizer allocators not initialized", K(ret), KP(metadata_alloc_), KP(scratch_alloc_));
  } else if (OB_ISNULL(parser = OB_NEWx(ObBEngFTParser, metadata_alloc_, *metadata_alloc_, *scratch_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate beng parser", K(ret));
  } else {
    plugin::ObFTParserParam param;
    param.cs_ = cs;
    param.fulltext_ = text;
    param.ft_length_ = text_len;
    param.parser_version_ = ANALYZER_PARSER_VERSION_PLACEHOLDER;
    param.metadata_alloc_ = metadata_alloc_;
    param.scratch_alloc_ = scratch_alloc_;
    if (OB_FAIL(parser->init(&param))) {
      LOG_WARN("failed to init beng parser", K(ret));
      parser->~ObBEngFTParser();
      metadata_alloc_->free(parser);
    } else {
      legacy_parser_ = parser;
    }
  }
  return ret;
}

// ============================================================
// ObIKTokenizer
// ============================================================

int ObIKTokenizer::init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ik tokenizer is already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_IK != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type for ik tokenizer", K(ret), K(spec.type_));
  } else {
    const ObIKTokenizerSpec &ik_spec = static_cast<const ObIKTokenizerSpec &>(spec);
    ik_mode_smart_ = ik_spec.ik_mode_smart_;
    is_ddl_mode_ = ik_spec.is_ddl_mode_;
    need_casedown_ = ik_spec.need_casedown_;
    main_dict_id_ = ik_spec.main_dict_id_;
    quan_dict_id_ = ik_spec.quan_dict_id_;
    stopword_dict_id_ = ik_spec.stopword_dict_id_;
    main_dict_name_ = ik_spec.main_dict_name_;
    quan_dict_name_ = ik_spec.quan_dict_name_;
    stopword_dict_name_ = ik_spec.stopword_dict_name_;
    metadata_alloc_ = &alloc;
    is_inited_ = true;
  }
  return ret;
}

int ObIKTokenizer::create_parser_impl(
    const ObCharsetInfo *cs, const char *text, int64_t text_len)
{
  int ret = OB_SUCCESS;
  ObIKFTParser *parser = nullptr;

  if (OB_ISNULL(metadata_alloc_) || OB_ISNULL(scratch_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tokenizer allocators not initialized", K(ret), KP(metadata_alloc_), KP(scratch_alloc_));
  } else if (OB_ISNULL(parser = OB_NEWx(ObIKFTParser, metadata_alloc_, *metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ik parser", K(ret));
  } else {
    // TODO: support custom dict table
    plugin::ObFTParserParam param;
    param.cs_ = cs;
    param.fulltext_ = text;
    param.ft_length_ = text_len;
    param.parser_version_ = ANALYZER_PARSER_VERSION_PLACEHOLDER;
    param.metadata_alloc_ = metadata_alloc_;
    param.scratch_alloc_ = scratch_alloc_;
    param.is_ddl_mode_ = is_ddl_mode_;
    param.need_casedown_ = need_casedown_;
    param.ik_param_.mode_ = ik_mode_smart_
                                ? plugin::ObFTIKParam::Mode::SMART
                                : plugin::ObFTIKParam::Mode::MAX_WORD;
    param.ik_param_.main_dict_id_ = main_dict_id_;
    param.ik_param_.quan_dict_id_ = quan_dict_id_;
    param.ik_param_.stopword_dict_id_ = stopword_dict_id_;
    param.ik_param_.main_dict_name_ = main_dict_name_;
    param.ik_param_.quan_dict_name_ = quan_dict_name_;
    param.ik_param_.stopword_dict_name_ = stopword_dict_name_;
    if (OB_FAIL(parser->init(param))) {
      LOG_WARN("failed to init ik parser", K(ret));
      parser->~ObIKFTParser();
      metadata_alloc_->free(parser);
    } else {
      legacy_parser_ = parser;
    }
  }
  return ret;
}

// ============================================================
// ObNgram2Tokenizer
// ============================================================

int ObNgram2Tokenizer::init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ngram2 tokenizer is already initialized", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_NGRAM2 != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type for ngram2 tokenizer", K(ret), K(spec.type_));
  } else {
    const ObNgram2TokenizerSpec &ngram2_spec = static_cast<const ObNgram2TokenizerSpec &>(spec);
    if (OB_UNLIKELY(!ObFTParserJsonProps::is_valid_min_ngram_token_size(ngram2_spec.min_ngram_size_)
                    || !ObFTParserJsonProps::is_valid_max_ngram_token_size(ngram2_spec.max_ngram_size_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ngram2 tokenizer size", K(ret), K(ngram2_spec));
    } else if (OB_UNLIKELY(ngram2_spec.max_ngram_size_ < ngram2_spec.min_ngram_size_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("max ngram size is smaller than min ngram size", K(ret), K(ngram2_spec));
    } else {
      min_ngram_size_ = ngram2_spec.min_ngram_size_;
      max_ngram_size_ = ngram2_spec.max_ngram_size_;
      metadata_alloc_ = &alloc;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObNgram2Tokenizer::create_parser_impl(
    const ObCharsetInfo *cs, const char *text, int64_t text_len)
{
  int ret = OB_SUCCESS;
  ObNgram2FTParser *parser = nullptr;
  if (OB_ISNULL(metadata_alloc_) || OB_ISNULL(scratch_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tokenizer allocators not initialized", K(ret), KP(metadata_alloc_), KP(scratch_alloc_));
  } else if (OB_ISNULL(parser = OB_NEWx(ObNgram2FTParser, metadata_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ngram2 parser", K(ret));
  } else {
    plugin::ObFTParserParam param;
    param.cs_ = cs;
    param.fulltext_ = text;
    param.ft_length_ = text_len;
    param.parser_version_ = ANALYZER_PARSER_VERSION_PLACEHOLDER;
    param.metadata_alloc_ = metadata_alloc_;
    param.scratch_alloc_ = scratch_alloc_;
    param.min_ngram_size_ = min_ngram_size_;
    param.max_ngram_size_ = max_ngram_size_;
    if (OB_FAIL(parser->init(&param))) {
      LOG_WARN("failed to init ngram2 parser", K(ret));
      parser->~ObNgram2FTParser();
      metadata_alloc_->free(parser);
    } else {
      legacy_parser_ = parser;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
