/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/analyzer/filter/ob_english_possessive_filter.h"

#include "lib/alloc/alloc_assist.h"
#include "lib/oblog/ob_log_module.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

static const uint8_t ASCII_APOSTROPHE = 0x27;
// U+2019 RIGHT SINGLE QUOTATION MARK / U+FF07 FULLWIDTH APOSTROPHE, UTF-8
static const unsigned char UTF8_U2019[] = {0xE2, 0x80, 0x99};
static const unsigned char UTF8_UFF07[] = {0xEF, 0xBC, 0x87};
static const int32_t UTF8_APOS_SEQ_LEN = 3;

ObEnglishPossessiveFilter::ObEnglishPossessiveFilter()
  : is_inited_(false)
{}

ObEnglishPossessiveFilter::~ObEnglishPossessiveFilter()
{
  reset();
}

int ObEnglishPossessiveFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("english possessive filter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_ENGLISH_POSSESSIVE != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter spec type for english possessive filter", K(ret), K(spec.type_));
  } else {
    UNUSED(alloc);
    is_inited_ = true;
  }
  return ret;
}

bool ObEnglishPossessiveFilter::calc_stripped_len(const char *data, const int32_t len, int32_t &new_len)
{
  bool stripped = false;
  if (OB_UNLIKELY(0 >= len)) {
    new_len = 0;
  } else {
    new_len = len;
    if (2 <= len && ASCII_APOSTROPHE == static_cast<uint8_t>(data[len - 2])
        && ('s' == data[len - 1] || 'S' == data[len - 1])) {
      new_len = len - 2;
    } else if (4 <= len && ('s' == data[len - 1] || 'S' == data[len - 1])
               && 0 == MEMCMP(data + len - 4, UTF8_U2019, static_cast<size_t>(UTF8_APOS_SEQ_LEN))) {
      new_len = len - 4;
    } else if (4 <= len && ('s' == data[len - 1] || 'S' == data[len - 1])
               && 0 == MEMCMP(data + len - 4, UTF8_UFF07, static_cast<size_t>(UTF8_APOS_SEQ_LEN))) {
      new_len = len - 4;
    } else if (1 < len && ASCII_APOSTROPHE == static_cast<uint8_t>(data[len - 1])) {
      new_len = len - 1;
    } else if (UTF8_APOS_SEQ_LEN <= len
               && 0 == MEMCMP(data + len - UTF8_APOS_SEQ_LEN, UTF8_U2019,
                              static_cast<size_t>(UTF8_APOS_SEQ_LEN))) {
      new_len = len - UTF8_APOS_SEQ_LEN;
    } else if (UTF8_APOS_SEQ_LEN <= len
               && 0 == MEMCMP(data + len - UTF8_APOS_SEQ_LEN, UTF8_UFF07,
                              static_cast<size_t>(UTF8_APOS_SEQ_LEN))) {
      new_len = len - UTF8_APOS_SEQ_LEN;
    }
    stripped = (new_len != len);
  }
  return stripped;
}

int ObEnglishPossessiveFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(input_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("english possessive filter not initialized", K(ret), KP(input_));
  } else {
    bool found_token = false;
    int32_t pending_pos_inc = 0;
    // Suffix is removed in place: token_ptr_ still points at upstream buffer; only len is reduced.
    while (OB_SUCC(ret) && !found_token) {
      if (OB_FAIL(input_->get_next_token(token))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("upstream token stream failed", K(ret));
        }
      } else if (!token.is_valid()) {
        // invalid upstream token; pull again
      } else {
        const int32_t old_len = token.token_len_;
        int32_t new_len = old_len;
        if (!calc_stripped_len(token.token_ptr_, old_len, new_len)) {
          token.pos_inc_ += pending_pos_inc;
          found_token = true;
        } else if (0 >= new_len) {
          pending_pos_inc += token.pos_inc_;
        } else {
          token.token_len_ = new_len;
          token.pos_inc_ += pending_pos_inc;
          found_token = true;
        }
      }
    }
  }
  return ret;
}

void ObEnglishPossessiveFilter::reset()
{
  if (OB_NOT_NULL(input_) || IS_INIT) {
    input_ = nullptr;
    is_inited_ = false;
  }
}

} // namespace storage
} // namespace oceanbase
