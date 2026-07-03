/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_icu_normalizer2_filter.h"
#include "share/rc/ob_tenant_base.h"

#include <unicode/bytestream.h>
#include <unicode/stringpiece.h>

namespace oceanbase
{
namespace storage
{

ObICUNormalizer2Filter::ObICUNormalizer2Filter()
  : allocator_(common::ObMemAttr(MTL_ID(), "ICUNorm")),
    normalizer_(nullptr),
    local_buffer_(),
    buffer_(local_buffer_),
    buffer_size_(LOCAL_BUFFER_SIZE),
    is_inited_(false)
{}

ObICUNormalizer2Filter::~ObICUNormalizer2Filter()
{
  reset();
}

int ObICUNormalizer2Filter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  UErrorCode status = U_ZERO_ERROR;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "ICUNorm"));
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_NORMALIZATION != spec.type_
      && ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tokenizer type", K(ret), K_(spec.type));
  }
  const ObICUNormalizer2FilterSpec &filter_spec
      = static_cast<const ObICUNormalizer2FilterSpec &>(spec);
  if (OB_FAIL(ret)) {
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING == spec.type_) {
    if (OB_ISNULL(normalizer_ = icu::Normalizer2::getInstance(
        "icu_folding", "utr30", UNormalization2Mode::UNORM2_COMPOSE, status))
        || U_FAILURE(status)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get normalizer instance", K(ret), K(status), K(filter_spec));
    }
  } else if (OB_UNLIKELY(filter_spec.name_ <= ObICUNormalizer2FilterSpec::Name::INVALID
      || filter_spec.name_ >= ObICUNormalizer2FilterSpec::Name::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid normalizer name", K(ret), K(filter_spec));
  } else if (OB_ISNULL(normalizer_ = icu::Normalizer2::getInstance(
      nullptr,
      NORMALIZER_NAMES[static_cast<int>(filter_spec.name_) - 1],
      filter_spec.mode_,
      status))
      || U_FAILURE(status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get normalizer instance", K(ret), K(status), K(filter_spec));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObICUNormalizer2Filter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  UErrorCode status = U_ZERO_ERROR;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "ICUNorm"));
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
  int32_t output_len = 0;
  int32_t pending_pos_inc = 0;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(input_->get_next_token(input_token))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next token from input", K(ret));
    } else if (OB_UNLIKELY(!input_token.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid token", K(ret), K(input_token));
    } else if (OB_ISNULL(buffer_) || OB_UNLIKELY(buffer_size_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null buffer", K(ret), KP_(buffer), K_(buffer_size));
    } else {
      icu::CheckedArrayByteSink sink(buffer_, buffer_size_);
      normalizer_->normalizeUTF8(
          0,
          icu::StringPiece(input_token.token_ptr_, input_token.token_len_),
          sink,
          nullptr,
          status);
      output_len = sink.NumberOfBytesAppended();
      if (U_FAILURE(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to normalize token", K(ret), K(status), K(input_token));
      } else if (!sink.Overflowed()) {
        // do nothing
      } else if (OB_FAIL(reserve_buffer_on_demand(output_len))) {
        LOG_WARN("failed to reserve buffer", K(ret), K(output_len), K(input_token));
      } else {
        status = U_ZERO_ERROR;
        icu::CheckedArrayByteSink retry_sink(buffer_, buffer_size_);
        normalizer_->normalizeUTF8(
            0,
            icu::StringPiece(input_token.token_ptr_, input_token.token_len_),
            retry_sink,
            nullptr,
            status);
        output_len = retry_sink.NumberOfBytesAppended();
        if (U_FAILURE(status) || OB_UNLIKELY(retry_sink.Overflowed())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to normalize token on retry",
              K(ret), K(status), K(input_token), K(output_len));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(output_len < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected output len", K(ret), K(input_token), K(output_len));
    } else if (0 == output_len) {
      pending_pos_inc += input_token.pos_inc_;
    } else {
      token.token_ptr_ = buffer_;
      token.token_len_ = output_len;
      token.pos_inc_ = input_token.pos_inc_ + pending_pos_inc;
      token.is_keyword_ = input_token.is_keyword_;
      found = true;
      pending_pos_inc = 0;
    }
  }
  return ret;
}

int ObICUNormalizer2Filter::reserve_buffer_on_demand(const int32_t new_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(new_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer length", K(ret), K(new_len));
  } else if (buffer_size_ >= new_len) {
    // do nothing
  } else if (FALSE_IT(allocator_.reuse())) {
  } else if (OB_ISNULL(buffer_ = static_cast<char *>(allocator_.alloc(new_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    buffer_ = local_buffer_;
    buffer_size_ = LOCAL_BUFFER_SIZE;
    LOG_WARN("failed to allocate buffer", K(ret), K(new_len));
  } else {
    buffer_size_ = new_len;
  }
  return ret;
}

void ObICUNormalizer2Filter::reset()
{
  allocator_.reset();
  normalizer_ = nullptr; // singleton, must not be deleted
  buffer_ = local_buffer_;
  buffer_size_ = LOCAL_BUFFER_SIZE;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
