/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_snowball_filter.h"
#include "share/rc/ob_tenant_base.h"

extern "C" {
  #include <libstemmer.h>
}

namespace oceanbase
{
namespace storage
{

ObSnowballFilter::ObSnowballFilter()
  : stemmer_(nullptr),
    is_inited_(false)
{}

ObSnowballFilter::~ObSnowballFilter()
{
  reset();
}

int ObSnowballFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "SnblFlt"));
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_SNOWBALL != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type", K(ret), K_(spec.type));
  }
  const ObSnowballFilterSpec &filter_spec = static_cast<const ObSnowballFilterSpec &>(spec);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(filter_spec.algo_ <= ObSnowballFilterSpec::Algorithm::INVALID
      || filter_spec.algo_ >= ObSnowballFilterSpec::Algorithm::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid snowball algorithm", K(ret), K(filter_spec));
  } else if (OB_ISNULL(stemmer_ = sb_stemmer_new(
      ALGORITHM_CODES[static_cast<int>(filter_spec.algo_) - 1], nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create snowball stemmer", K(ret), K(filter_spec));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSnowballFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "SnblFlt"));
  token = ObTokenAttr();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_ISNULL(input_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null input", K(ret));
  }
  bool found = false;
  ObTokenAttr input_token;
  const sb_symbol *output_ptr = nullptr;
  int32_t output_len = 0;
  int32_t pending_pos_inc = 0;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(input_->get_next_token(input_token))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next token from input", K(ret));
    } else if (OB_UNLIKELY(!input_token.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid token", K(ret), K(input_token));
    } else if (input_token.is_keyword_) {
      token = input_token;
      token.pos_inc_ = input_token.pos_inc_ + pending_pos_inc;
      found = true;
      // skip the following
    } else if (OB_ISNULL(output_ptr = sb_stemmer_stem(
        stemmer_,
        reinterpret_cast<const sb_symbol *>(input_token.token_ptr_),
        input_token.token_len_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to stem token", K(ret), K(input_token));
    } else if (FALSE_IT(output_len = sb_stemmer_length(stemmer_))) {
    } else if (OB_UNLIKELY(output_len < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected output len", K(ret), K(input_token), K(output_len));
    } else if (0 == output_len) {
      pending_pos_inc += input_token.pos_inc_;
    } else {
      token.token_ptr_ = reinterpret_cast<const char *>(output_ptr);
      token.token_len_ = output_len;
      token.pos_inc_ = input_token.pos_inc_ + pending_pos_inc;
      token.is_keyword_ = input_token.is_keyword_;
      found = true;
    }
  }
  return ret;
}

void ObSnowballFilter::reset()
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "SnblFlt"));
  if (OB_NOT_NULL(stemmer_)) {
    sb_stemmer_delete(stemmer_);
    stemmer_ = nullptr;
  }
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
