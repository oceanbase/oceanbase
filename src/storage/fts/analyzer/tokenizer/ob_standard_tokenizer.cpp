/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_standard_tokenizer.h"
#include "storage/fts/analyzer/ob_utf8_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

ObStandardTokenizer::ObStandardTokenizer()
  : max_token_length_(255),
    bi_(nullptr),
    text_(),
    utext_(nullptr),
    last_boundary_(0),
    last_emitted_(0),
    is_inited_(false)
{}

ObStandardTokenizer::~ObStandardTokenizer()
{
  reset();
}

int ObStandardTokenizer::init(const ObTokenizerSpec &spec, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  UErrorCode status = U_ZERO_ERROR;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "StdTknz"));
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_STANDARD != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type", K(ret), K_(spec.type));
  }
  const ObStandardTokenizerSpec &tkzn_spec = static_cast<const ObStandardTokenizerSpec &>(spec);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(tkzn_spec.max_token_length_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max token length", K(ret), K(tkzn_spec.max_token_length_));
  } else if (OB_ISNULL(bi_ = icu::BreakIterator::createWordInstance(icu::Locale::getRoot(), status))
      || U_FAILURE(status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create word break iterator", K(ret), K(status));
  } else {
    max_token_length_ = tkzn_spec.max_token_length_;
    is_inited_ = true;
  }
  return ret;
}

int ObStandardTokenizer::set_input(const char *text, int64_t text_len, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  UErrorCode status = U_ZERO_ERROR;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "StdTknz"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_ISNULL(text) || OB_UNLIKELY(text_len < 0)
      || OB_UNLIKELY(CS_TYPE_UTF8MB4_BIN != coll_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input text", K(ret), KP(text), K(text_len), K(coll_type));
  } else if (OB_FAIL(ObUTF8Utils::validate(text, text_len))) {
    LOG_WARN("failed to validate input text", K(ret), KP(text), K(text_len));
  } else if (FALSE_IT(text_.assign_ptr(text, text_len))) {
  } else if (text_.empty()) {
    last_boundary_ = icu::BreakIterator::DONE;
    last_emitted_ = icu::BreakIterator::DONE;
  } else if (OB_ISNULL(utext_ = utext_openUTF8(utext_, text_.ptr(), text_.length(), &status))
      || U_FAILURE(status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to open utext from text", K(ret), K(status), K_(text));
  } else if (FALSE_IT(status = U_ZERO_ERROR)) {
  } else if (FALSE_IT(bi_->setText(utext_, status)) || U_FAILURE(status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set text of break iterator", K(ret), K(status), K_(text));
  } else {
    last_boundary_ = 0;
    last_emitted_ = 0;
  }
  if (OB_FAIL(ret)) {
    text_.reset();
    last_boundary_ = icu::BreakIterator::DONE;
    last_emitted_ = icu::BreakIterator::DONE;
  }
  return ret;
}

int ObStandardTokenizer::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "StdTknz"));
  token = ObTokenAttr();
  int32_t token_start = 0;
  int32_t token_limit = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (text_.empty()
      || (icu::BreakIterator::DONE == last_boundary_ && last_emitted_ == last_boundary_)) {
    ret = OB_ITER_END;
  } else if (last_emitted_ < last_boundary_) {
    token_start = last_emitted_;
    token_limit = last_boundary_;
  } else {
    bool found = false;
    int32_t next_boundary = 0;
    int32_t rule_status = 0;
    bool is_emoji = false;
    while (OB_SUCC(ret) && !found) {
      next_boundary = bi_->next();
      if (icu::BreakIterator::DONE == next_boundary) {
        ret = OB_ITER_END;
      } else if (OB_UNLIKELY(next_boundary <= last_boundary_ || next_boundary > text_.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected boundary", K(ret), K_(text), K_(last_boundary), K(next_boundary));
      } else if (FALSE_IT(rule_status = bi_->getRuleStatus())) {
      } else if (FALSE_IT(is_emoji = false)) {
      } else if (rule_status <= 0
          && OB_FAIL(ObUTF8Utils::is_emoji(text_.ptr() + last_boundary_,
                                           next_boundary - last_boundary_,
                                           is_emoji))) {
        LOG_WARN("failed to check emoji", K(ret), K_(text), K_(last_boundary), K(next_boundary));
      } else if (rule_status > 0 || is_emoji) {
        token_start = last_boundary_;
        token_limit = next_boundary;
        found = true;
      }
      last_boundary_ = next_boundary;
    }
  }

  int32_t token_len = 0;
  int32_t codepoint_cnt = 0;
  if (FAILEDx(ObUTF8Utils::truncate(text_.ptr() + token_start,
                                    token_limit - token_start,
                                    max_token_length_,
                                    token_len,
                                    codepoint_cnt))) {
    LOG_WARN("failed to truncate token by code point",
        K(ret), K(token_start), K(token_limit), K_(max_token_length));
  } else {
    token.token_ptr_ = text_.ptr() + token_start;
    token.token_len_ = token_len;
    token.pos_inc_ = 1;
    token.is_keyword_ = false;
    last_emitted_ = token_start + token_len;
  }
  return ret;
}

void ObStandardTokenizer::reset()
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "StdTknz"));
  if (OB_NOT_NULL(utext_)) {
    utext_close(utext_);
    utext_ = nullptr;
  }
  if (OB_NOT_NULL(bi_)) {
    delete bi_;
    bi_ = nullptr;
  }
  last_boundary_ = 0;
  last_emitted_ = 0;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
