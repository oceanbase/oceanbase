/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/fts/analyzer/ob_analyzer.h"

namespace oceanbase
{
namespace storage
{

ObFTSAnalyzer::~ObFTSAnalyzer()
{
  reset();
}

int ObFTSAnalyzer::analyze(
    const char *text,
    const int64_t text_len,
    common::ObIAllocator &alloc,
    ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  const char *cur_text = text;
  int64_t cur_len = text_len;
  token_stream = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("text analyzer is not initialized", K(ret));
  } else if (OB_ISNULL(text) || OB_UNLIKELY(text_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(text), K(text_len));
  }

  // Step 1: apply char filters sequentially
  for (int64_t i = 0; OB_SUCC(ret) && i < char_filters_.count(); ++i) {
    const char *output = nullptr;
    int64_t output_len = 0;
    if (OB_ISNULL(char_filters_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("char filter is null", K(ret), K(i));
    } else if (OB_FAIL(char_filters_.at(i)->filter(cur_text, cur_len, output, output_len))) {
      LOG_WARN("char filter failed", K(ret), K(i));
    } else {
      cur_text = output;
      cur_len = output_len;
    }
  }

  // Step 2: set input on tokenizer
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tokenizer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tokenizer is null", K(ret));
    } else {
      const ObCollationType tokenizer_collation =
          (ObAnalyzerType::ANALYZER_TYPE_LEGACY == analyzer_type_) ? source_collation_ : CS_TYPE_UTF8MB4_BIN;
      if (OB_FAIL(tokenizer_->set_input(cur_text, cur_len, tokenizer_collation))) {
        LOG_WARN("failed to set tokenizer input", K(ret));
      }
    }
  }

  // Step 3: return the tail of the filter chain
  if (OB_SUCC(ret)) {
    token_stream = tail_;
  }

  return ret;
}

void ObFTSAnalyzer::reset()
{
  // Mirrors prior destructor logic: ObFixedArray holds raw pointers; ~T on pointer slots
  // does not free pointees, so each object must be explicitly destroyed and freed via alloc_.
  if (OB_NOT_NULL(alloc_)) {
    for (int64_t i = 0; i < token_filters_.count(); ++i) {
      if (OB_NOT_NULL(token_filters_.at(i))) {
        token_filters_.at(i)->~ObITokenFilter();
        alloc_->free(token_filters_.at(i));
      }
    }
    if (OB_NOT_NULL(tokenizer_)) {
      tokenizer_->~ObITokenizer();
      alloc_->free(tokenizer_);
      tokenizer_ = nullptr;
    }
    for (int64_t i = 0; i < char_filters_.count(); ++i) {
      if (OB_NOT_NULL(char_filters_.at(i))) {
        char_filters_.at(i)->~ObICharFilter();
        alloc_->free(char_filters_.at(i));
      }
    }
  }
  is_inited_ = false;
  alloc_ = nullptr;
  scratch_alloc_.reset();
  analyzer_type_ = ObAnalyzerType::ANALYZER_TYPE_INVALID;
  source_collation_ = CS_TYPE_INVALID;
  char_filters_.reset();
  tokenizer_ = nullptr;
  token_filters_.reset();
  tail_ = nullptr;
}

} // namespace storage
} // namespace oceanbase
