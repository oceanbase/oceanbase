/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/analyzer/filter/ob_charset_convert_filter.h"

#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

ObCharsetConvertFilter::ObCharsetConvertFilter()
    : is_inited_(false),
      target_collation_(common::CS_TYPE_INVALID),
      convert_arena_(lib::ObMemAttr(MTL_ID(), "CSConvertFilt"), OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

ObCharsetConvertFilter::~ObCharsetConvertFilter()
{
  reset();
}

int ObCharsetConvertFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("charset convert filter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter spec type for charset convert filter", K(ret), K(spec.type_));
  } else {
    const ObCharsetConvertFilterSpec &cc_spec =
        static_cast<const ObCharsetConvertFilterSpec &>(spec);
    if (OB_UNLIKELY(common::CS_TYPE_INVALID == cc_spec.target_collation_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid target collation for charset convert filter", K(ret), K(cc_spec));
    } else {
      target_collation_ = cc_spec.target_collation_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCharsetConvertFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(input_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("charset convert filter not initialized", K(ret), KP(input_));
  } else if (OB_FAIL(input_->get_next_token(token))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("upstream token stream failed", K(ret));
    }
  } else if (!token.is_valid()) {
    // pass through invalid token unchanged
  } else if (CHARSET_UTF8MB4 == common::ObCharset::charset_type_by_coll(target_collation_)) {
    // target charset is already utf8mb4: bytes are identical, only collation tag differs.
    // No per-token conversion needed.
  } else {
    convert_arena_.reuse();
    const common::ObString src(token.token_len_, token.token_ptr_);
    common::ObString dst;
    if (OB_FAIL(common::ObCharset::charset_convert(
            convert_arena_, src, common::CS_TYPE_UTF8MB4_BIN, target_collation_, dst))) {
      LOG_WARN("fail to convert token charset from utf8mb4 to target",
               K(ret), K_(target_collation), K(token.token_len_));
    } else {
      token.token_ptr_ = dst.ptr();
      token.token_len_ = dst.length();
    }
  }
  return ret;
}

void ObCharsetConvertFilter::reset()
{
  if (OB_NOT_NULL(input_) || IS_INIT) {
    input_ = nullptr;
    convert_arena_.reset();
    target_collation_ = common::CS_TYPE_INVALID;
    is_inited_ = false;
  }
}

} // namespace storage
} // namespace oceanbase
