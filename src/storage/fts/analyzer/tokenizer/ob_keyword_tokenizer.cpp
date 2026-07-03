/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_keyword_tokenizer.h"
#include "storage/fts/analyzer/ob_utf8_utils.h"

namespace oceanbase
{
namespace storage
{

ObKeywordTokenizer::ObKeywordTokenizer()
  : text_(),
    emitted_(false),
    is_inited_(false)
{}

ObKeywordTokenizer::~ObKeywordTokenizer()
{
  reset();
}

int ObKeywordTokenizer::init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObTokenizerType::TOKENIZER_TYPE_KEYWORD != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type", K(ret), K_(spec.type));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObKeywordTokenizer::set_input(const char *text, int64_t text_len, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_ISNULL(text) || OB_UNLIKELY(text_len < 0)
      || OB_UNLIKELY(CS_TYPE_UTF8MB4_BIN != coll_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input text", K(ret), KP(text), K(text_len), K(coll_type));
  } else if (OB_FAIL(ObUTF8Utils::validate(text, text_len))) {
    LOG_WARN("failed to validate input text", K(ret), KP(text), K(text_len));
  } else {
    text_.assign_ptr(text, static_cast<ObString::obstr_size_t>(text_len));
    emitted_ = text_.empty();
  }
  if (OB_FAIL(ret)) {
    text_.reset();
    emitted_ = false;
  }
  return ret;
}

int ObKeywordTokenizer::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  token = ObTokenAttr();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (text_.empty() || emitted_) {
    ret = OB_ITER_END;
  } else {
    token.token_ptr_ = text_.ptr();
    token.token_len_ = static_cast<int32_t>(text_.length());
    token.pos_inc_ = 1;
    token.is_keyword_ = false;
    emitted_ = true;
  }
  return ret;
}

void ObKeywordTokenizer::reset()
{
  text_.reset();
  emitted_ = false;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
