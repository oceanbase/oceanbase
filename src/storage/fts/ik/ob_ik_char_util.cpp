/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_processor.h"

namespace oceanbase
{
namespace storage
{

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_alpha(const char *input, const uint8_t char_len, bool &is_alpha)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_alpha = ObUnicodeBlockUtils::is_alpha(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_arabic(const char *input, const uint8_t char_len, bool &is_arabic)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_arabic = ObUnicodeBlockUtils::is_arabic(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_chinese(const char *input, const uint8_t char_len, bool &is_chinese)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_chinese = ObUnicodeBlockUtils::is_chinese(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_other_cjk(const char *input, const uint8_t char_len, bool &is_other_cjk)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_other_cjk = ObUnicodeBlockUtils::is_other_cjk(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
ObFTCharUtil::is_surrogate_high(const char *input, const uint8_t char_len, bool &is_surrogate_high)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_surrogate_high = ObUnicodeBlockUtils::check_high_surrogate(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
ObFTCharUtil::is_surrogate_low(const char *input, const uint8_t char_len, bool &is_surrogate_low)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_surrogate_low = ObUnicodeBlockUtils::check_low_surrogate(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<!(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
ObFTCharUtil::is_surrogate_high(const char *input, const uint8_t char_len, bool &is_surrogate_high)
{
  int ret = OB_SUCCESS;
  is_surrogate_high = false;
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<!(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
ObFTCharUtil::is_surrogate_low(const char *input, const uint8_t char_len, bool &is_surrogate_low)
{
  int ret = OB_SUCCESS;
  is_surrogate_low = false;
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_ignore(const char *input, const uint8_t char_len, bool &ignore)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    ignore = ObUnicodeBlockUtils::check_ignore_as_single(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_cn_number(const char *input, const uint8_t char_len, bool &is_cn_number)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_cn_number = ObUnicodeBlockUtils::is_unicode_cn_number(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_letter_connector(const char *input, const uint8_t char_len, bool &is_connector)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_connector = ObUnicodeBlockUtils::check_letter_connector(unicode);
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                || CS_TYPE == CHARSET_UTF16LE),
                               int>::type
ObFTCharUtil::is_num_connector(const char *input, const uint8_t char_len, bool &is_connector)
{
  int ret = OB_SUCCESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else {
    is_connector = ObUnicodeBlockUtils::check_num_connector(unicode);
  }
  return ret;
}

// Support GBK

// Implementation of some frame;

inline int ObFTCharUtil::check_cn_number(ObCollationType coll_type,
                                         const char *input,
                                         const uint8_t char_len,
                                         bool &is_cn_number)
{
  int ret = OB_SUCCESS;
  ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);

  switch (cs_type) {
  case CHARSET_UTF8MB4: {
    ret = ObFTCharUtil::is_cn_number<CHARSET_UTF8MB4>(input, char_len, is_cn_number);
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    STORAGE_FTS_LOG(WARN, "Not supported charset type", K(ret), K(cs_type));
  }
  return ret;
}

inline int ObFTCharUtil::check_num_connector(ObCollationType coll_type,
                                             const char *input,
                                             const uint8_t char_len,
                                             bool &is_connector)
{
  int ret = OB_SUCCESS;
  ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);

  switch (cs_type) {
  case CHARSET_UTF8MB4: {
    ret = ObFTCharUtil::is_num_connector<CHARSET_UTF8MB4>(input, char_len, is_connector);
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    STORAGE_FTS_LOG(WARN, "Not supported charset type", K(ret), K(cs_type));
  }
  return ret;
}

inline int ObFTCharUtil::check_letter_connector(ObCollationType coll_type,
                                                const char *input,
                                                const uint8_t char_len,
                                                bool &is_connector)
{
  int ret = OB_SUCCESS;
  ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);

  switch (cs_type) {
  case CHARSET_UTF8MB4: {
    // ret = ObFTCharUtil::do_check_letter_connector<CHARSET_UTF8MB4>(input, char_len,
    // is_connector);
    ret = ObFTCharUtil::is_letter_connector<CHARSET_UTF8MB4>(input, char_len, is_connector);
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    STORAGE_FTS_LOG(WARN, "Not supported charset type", K(ret), K(cs_type));
  }
  return ret;
}

inline int ObFTCharUtil::is_ignore_single_cjk(ObCollationType coll_type,
                                              const char *input,
                                              const uint8_t char_len,
                                              bool &ignore)
{
  int ret = OB_SUCCESS;
  ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);

  switch (cs_type) {
  case CHARSET_UTF8MB4: {
    ret = ObFTCharUtil::is_ignore<CHARSET_UTF8MB4>(input, char_len, ignore);
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    STORAGE_FTS_LOG(WARN, "Not supported charset type", K(ret), K(cs_type));
    break;
  }
  return ret;
}

template <ObCharsetType CS_TYPE>
inline int ObFTCharUtil::do_classify(const char *input, const uint8_t char_len, CharType &type)
{
  int ret = OB_SUCCESS;
  type = CharType::USELESS;
  ob_wc_t unicode = 0;
  if (OB_FAIL(decode_unicode<CS_TYPE>(input, char_len, unicode))) {
    STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(ret));
  } else if (ObUnicodeBlockUtils::is_chinese(unicode)) {
    type = CharType::CHINESE;
  } else if (ObUnicodeBlockUtils::is_alpha(unicode)) {
    type = CharType::ENGLISH_LETTER;
  } else if (ObUnicodeBlockUtils::is_arabic(unicode)) {
    type = CharType::ARABIC_LETTER;
  } else if (ObUnicodeBlockUtils::is_other_cjk(unicode)) {
    type = CharType::OTHER_CJK;
  } else if (!(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE)) {
    type = CharType::USELESS;
  } else if (ObUnicodeBlockUtils::check_high_surrogate(unicode)) {
    type = CharType::SURROGATE_HIGH;
  } else if (ObUnicodeBlockUtils::check_low_surrogate(unicode)) {
    type = CharType::SURROGATE_LOW;
  }
  return ret;
}

int ObFTCharUtil::classify_first_valid_char(ObCharsetType cs_type,
                                            const char *buf,
                                            int64_t buf_size,
                                            size_t (*well_formed_len_fn)(const struct ObCharsetInfo *,
                                                                         const char *,
                                                                         const char *,
                                                                         size_t,
                                                                         int *),
                                            const ObCharsetInfo *cs,
                                            int64_t &char_len,
                                            CharType &type)
{
  int ret = OB_SUCCESS;
  char_len = 0;
  type = CharType::USELESS;
  if (OB_UNLIKELY(buf_size <= 0)) {
  } else {
    int error = 0;
    int64_t len = 0;
    len = static_cast<int64_t>(well_formed_len_fn(cs, buf, buf + buf_size, 1, &error));
    if (OB_LIKELY(0 == error)) {
      char_len = len;
      if (OB_LIKELY(CHARSET_UTF8MB4 == cs_type)) {
        ret = do_classify<CHARSET_UTF8MB4>(buf, char_len, type);
      } else {
        ret = OB_NOT_SUPPORTED;
        STORAGE_FTS_LOG(WARN, "Not supported charset type", K(ret), K(cs_type));
      }
    } else {
      // illegal encoding, return OB_ITER_END to indicate the end of the input buffer
      ret = OB_ITER_END;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase