/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/analyzer/filter/ob_decimal_digit_filter.h"

#include "lib/charset/ob_charset_string_helper.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include <unicode/uchar.h>

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

static int normalize_utf8_decimal_digits(const char *src,
                                         const int32_t src_len,
                                         common::ObIAllocator &alloc,
                                         const char *&out_ptr,
                                         int32_t &out_len)
{
  int ret = OB_SUCCESS;
  out_ptr = nullptr;
  out_len = 0;
  if (OB_ISNULL(src) || 0 >= src_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("normalize_utf8_decimal_digits invalid argument", K(ret), KP(src), K(src_len));
  } else {
    char *buf = static_cast<char *>(alloc.alloc(src_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("normalize_utf8_decimal_digits alloc failed", K(ret), K(src_len));
    } else {
      const unsigned char *s = reinterpret_cast<const unsigned char *>(src);
      const unsigned char *const end = s + src_len;
      unsigned char *d = reinterpret_cast<unsigned char *>(buf);
      while (s < end) {
        ob_wc_t wc = 0;
        const int clen = ob_charset_decode_unicode<CHARSET_UTF8MB4>(s, end, wc);
        if (OB_UNLIKELY(0 >= clen)) {
          *d++ = *s++;
        } else {
          const int32_t dv = static_cast<int32_t>(u_digit(static_cast<UChar32>(wc), 10));
          if (0 <= dv && 9 >= dv) {
            *d++ = static_cast<unsigned char>('0' + dv);
          } else {
            for (int i = 0; i < clen; ++i) {
              *d++ = s[i];
            }
          }
          s += clen;
        }
      }
      const int32_t nbytes = static_cast<int32_t>(d - reinterpret_cast<unsigned char *>(buf));
      out_ptr = buf;
      out_len = nbytes;
    }
  }
  return ret;
}

ObDecimalDigitFilter::ObDecimalDigitFilter()
    : is_inited_(false),
      normalize_arena_(lib::ObMemAttr(MTL_ID(), "DecDigitFltr"), OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

ObDecimalDigitFilter::~ObDecimalDigitFilter()
{
  reset();
}

int ObDecimalDigitFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("decimal digit filter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_DECIMAL_DIGIT != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter spec type for decimal digit filter", K(ret), K(spec.type_));
  } else {
    UNUSED(alloc);
    is_inited_ = true;
  }
  return ret;
}

int ObDecimalDigitFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(input_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("decimal digit filter not initialized", K(ret), KP(input_));
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
        normalize_arena_.reuse();
        const char *norm_ptr = nullptr;
        int32_t norm_len = 0;
        if (OB_FAIL(normalize_utf8_decimal_digits(token.token_ptr_,
                                                   token.token_len_,
                                                   normalize_arena_,
                                                   norm_ptr,
                                                   norm_len))) {
          LOG_WARN("normalize_utf8_decimal_digits failed", K(ret), K(token.token_len_));
        } else {
          token.token_ptr_ = norm_ptr;
          token.token_len_ = norm_len;
          found_token = true;
        }
      }
    }
  }
  return ret;
}

void ObDecimalDigitFilter::reset()
{
  if (OB_NOT_NULL(input_) || IS_INIT) {
    input_ = nullptr;
    normalize_arena_.reset();
    is_inited_ = false;
  }
}

} // namespace storage
} // namespace oceanbase
