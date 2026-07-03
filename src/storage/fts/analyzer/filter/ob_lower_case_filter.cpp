/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/analyzer/filter/ob_lower_case_filter.h"

#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

ObLowerCaseFilter::ObLowerCaseFilter()
    : is_inited_(false),
      tolower_arena_(lib::ObMemAttr(MTL_ID(), "LowerCaseFilter"), OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

ObLowerCaseFilter::~ObLowerCaseFilter()
{
  reset();
}

int ObLowerCaseFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lowercase filter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter spec type for lowercase filter", K(ret), K(spec.type_));
  } else {
    UNUSED(alloc);
    is_inited_ = true;
  }
  return ret;
}

int ObLowerCaseFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(input_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lowercase filter not initialized", K(ret), KP(input_));
  } else {
    bool found_token = false;
    while (OB_SUCC(ret) && !found_token) {
      if (OB_FAIL(input_->get_next_token(token))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("upstream token stream failed", K(ret));
        }
      } else if (!token.is_valid()) {
        // skip invalid token
      } else {
        tolower_arena_.reuse();
        const ObString src(token.token_len_, token.token_ptr_);
        ObString dst;
        if (OB_FAIL(ObCharset::tolower(&ob_charset_utf8mb4_general_ci, src, dst, tolower_arena_))) {
          LOG_WARN("tolower failed for token", K(ret), K(token.token_len_));
        } else {
          token.token_ptr_ = dst.ptr();
          token.token_len_ = dst.length();
          found_token = true;
        }
      }
    }
  }
  return ret;
}

void ObLowerCaseFilter::reset()
{
  if (OB_NOT_NULL(input_) || IS_INIT) {
    input_ = nullptr;
    tolower_arena_.reset();
    is_inited_ = false;
  }
}

} // namespace storage
} // namespace oceanbase
