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
    segment_start_(0),
    segment_end_(0),
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
  }
  if (OB_SUCC(ret)) {
    text_.assign_ptr(text, text_len);
  } else {
    text_.reset();
  }
  segment_start_ = 0;
  segment_end_ = 0;
  last_boundary_ = 0;
  last_emitted_ = 0;
  return ret;
}

int ObStandardTokenizer::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  UErrorCode status = U_ZERO_ERROR;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "StdTknz"));
  token = ObTokenAttr();
  int32_t token_start = 0;
  int32_t token_limit = 0;
  UText *new_utext = nullptr;
  bool found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (text_.empty()
      || (icu::BreakIterator::DONE == last_boundary_ && last_emitted_ == last_boundary_)) {
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret) && !found) {
    if (last_emitted_ < last_boundary_) {
      token_start = last_emitted_;
      token_limit = last_boundary_;
      found = true;
    } else if (last_boundary_ == segment_end_ - segment_start_) {
      if (OB_FAIL(find_segment())) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to find next segment", K(ret), K_(last_boundary));
      } else if (FALSE_IT(status = U_ZERO_ERROR)) {
      } else if (OB_ISNULL(new_utext = utext_openUTF8(utext_,
                                                      text_.ptr() + segment_start_,
                                                      segment_end_ - segment_start_,
                                                      &status)) || U_FAILURE(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to open utext from text", K(ret), K(status),
            K_(text), K_(segment_start), K_(segment_end));
      } else if (FALSE_IT(utext_ = new_utext)) {
      } else if (FALSE_IT(status = U_ZERO_ERROR)) {
      } else if (FALSE_IT(bi_->setText(utext_, status)) || U_FAILURE(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set text of break iterator", K(ret), K(status),
            K_(text), K_(segment_start), K_(segment_end));
      } else {
        last_boundary_ = 0;
        last_emitted_ = 0;
      }
    } else if (OB_FAIL(find_boundary(token_start, token_limit))) {
      if (OB_ITER_END == ret && segment_end_ < text_.length()) {
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to find next boundary", K(ret), K_(last_boundary));
      }
    } else if (OB_UNLIKELY(token_start >= token_limit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected token boundary", K(ret), K(token_start), K(token_limit),
          K_(segment_start), K_(segment_end));
    } else {
      found = true;
    }
  }

  int32_t token_len = 0;
  int32_t codepoint_cnt = 0;
  if (FAILEDx(ObUTF8Utils::truncate(text_.ptr() + segment_start_ + token_start,
                                    token_limit - token_start,
                                    max_token_length_,
                                    token_len,
                                    codepoint_cnt))) {
    LOG_WARN("failed to truncate token by code point", K(ret),
        K(token_start), K(token_limit), K_(max_token_length), K_(segment_start), K_(segment_end));
  } else {
    token.token_ptr_ = text_.ptr() + segment_start_ + token_start;
    token.token_len_ = token_len;
    token.pos_inc_ = 1;
    token.is_keyword_ = false;
    last_emitted_ = token_start + token_len;
  }
  return ret;
}

int ObStandardTokenizer::check_script_compatibility(
    UScriptCode &segment_script,
    const UChar32 codepoint,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  UErrorCode status = U_ZERO_ERROR;
  UScriptCode script = USCRIPT_INVALID_CODE;
  int8_t type = U_UNASSIGNED;
  is_compatible = false;
  if (codepoint >= 0 && codepoint < 128) {
    if ((codepoint >= 65 && codepoint <= 90) || (codepoint >= 97 && codepoint <= 122)) {
      script = USCRIPT_LATIN;
    } else {
      script = USCRIPT_COMMON;
    }
  } else if (FALSE_IT(script = uscript_getScript(codepoint, &status)) || U_FAILURE(status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get script", K(ret), K(status), K(codepoint));
  } else if (USCRIPT_HIRAGANA == script || USCRIPT_KATAKANA == script) {
    script = USCRIPT_HAN;
  }
  if (OB_FAIL(ret)) {
  } else if (segment_script == script
      || segment_script <= USCRIPT_INHERITED
      || script <= USCRIPT_INHERITED
      || uscript_hasScript(codepoint, segment_script)) {
    is_compatible = true;
  } else if (FALSE_IT(type = u_charType(codepoint))) {
  } else if (U_COMBINING_SPACING_MARK == type
      || U_NON_SPACING_MARK == type
      || U_ENCLOSING_MARK == type) {
    is_compatible = true;
  }
  if (OB_SUCC(ret) && segment_script <= USCRIPT_INHERITED && script > USCRIPT_INHERITED) {
    segment_script = script;
  }
  return ret;
}

int ObStandardTokenizer::find_segment()
{
  int ret = OB_SUCCESS;
  int32_t offset = segment_end_;
  UChar32 codepoint = U_SENTINEL;
  UScriptCode segment_script = USCRIPT_COMMON;
  bool is_compatible = false;
  bool found_end = false;
  if (OB_UNLIKELY(segment_end_ < 0 || segment_end_ > text_.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected segment end", K(ret), K_(text), K_(segment_end));
  } else if (segment_end_ == text_.length()) {
    ret = OB_ITER_END;
  } else {
    segment_start_ = segment_end_;
  }
  while (OB_SUCC(ret) && offset < text_.length() && !found_end) {
    if (OB_FAIL(ObUTF8Utils::next_codepoint(text_.ptr(), text_.length(), offset, codepoint))) {
      LOG_WARN("failed to get next codepoint", K(ret), K_(text), K(offset));
    } else if (FALSE_IT(is_compatible = false)) {
    } else if (OB_FAIL(check_script_compatibility(segment_script, codepoint, is_compatible))) {
      LOG_WARN("failed to check script compatibility", K(ret), K(codepoint), K(segment_script));
    } else if (is_compatible) {
      segment_end_ = offset;
    } else {
      found_end = true;
    }
  }
  return ret;
}

int ObStandardTokenizer::find_boundary(int32_t &token_start, int32_t &token_limit)
{
  int ret = OB_SUCCESS;
  bool found = false;
  int32_t next_boundary = 0;
  int32_t rule_status = 0;
  bool is_emoji = false;
  token_start = 0;
  token_limit = 0;
  while (OB_SUCC(ret) && !found) {
    next_boundary = bi_->next();
    if (icu::BreakIterator::DONE == next_boundary) {
      ret = OB_ITER_END;
      last_boundary_ = segment_end_ - segment_start_;
      last_emitted_ = last_boundary_;
    } else if (OB_UNLIKELY(next_boundary <= last_boundary_
        || next_boundary > segment_end_ - segment_start_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected boundary", K(ret),
          K_(text), K_(last_boundary), K(next_boundary), K_(segment_start), K_(segment_end));
    } else if (FALSE_IT(rule_status = bi_->getRuleStatus())) {
    } else if (FALSE_IT(is_emoji = false)) {
    } else if (rule_status <= 0
        && OB_FAIL(ObUTF8Utils::is_emoji(text_.ptr() + segment_start_ + last_boundary_,
                                         next_boundary - last_boundary_,
                                         is_emoji))) {
      LOG_WARN("failed to check emoji", K(ret),
          K_(text), K_(last_boundary), K(next_boundary), K_(segment_start), K_(segment_end));
    } else if (rule_status > 0 || is_emoji) {
      token_start = last_boundary_;
      token_limit = next_boundary;
      found = true;
    }
    if (OB_SUCC(ret)) {
      last_boundary_ = next_boundary;
    }
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
  segment_start_ = 0;
  segment_end_ = 0;
  last_boundary_ = 0;
  last_emitted_ = 0;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
