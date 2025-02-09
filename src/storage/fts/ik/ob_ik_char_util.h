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

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_CHAR_UTIL_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_CHAR_UTIL_H_

#include "lib/charset/ob_charset.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "lib/charset/ob_ctype.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/utils/unicode_utils.h"

#include <cstdint>
#include <type_traits>

namespace oceanbase
{
namespace storage
{
// See: lang/Character.java
class ObFTCharUtil
{
public:
  enum class CharType : int8_t
  {
    USELESS = 0,
    ARABIC_LETTER = 1,
    ENGLISH_LETTER = 2,
    CHINESE = 3,
    OTHER_CJK = 4,
    SURROGATE_HIGH = 5,
    SURROGATE_LOW = 6,
  };

  static int classify_first_char(ObCollationType coll_type,
                                 const char *input,
                                 const uint8_t char_len,
                                 CharType &type);

  static int check_cn_number(ObCollationType coll_type,
                             const char *input,
                             const uint8_t char_len,
                             bool &is_cn_number);

  static int check_num_connector(ObCollationType coll_type,
                                 const char *input,
                                 const uint8_t char_len,
                                 bool &is_connector);

  static int check_letter_connector(ObCollationType coll_type,
                                    const char *input,
                                    const uint8_t char_len,
                                    bool &is_connector);

  // some cjk word should be ignored
  static int is_ignore_single_cjk(ObCollationType coll_type,
                                  const char *input,
                                  const uint8_t char_len,
                                  bool &ignore);

private:
  template <ObCharsetType CS_TYPE>
  static int do_classify(const char *input, const uint8_t char_len, CharType &type);

private:
  /*************************  Alpha: english letter abc *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_alpha(const char *input, const uint8_t char_len, bool &is_alpha);
  /****************************************************************************************/

  /*************************  Arabic: num letter 123 *********************/

  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_arabic(const char *input, const uint8_t char_len, bool &is_arabic);
  /****************************************************************************************/

  /*************************  Chinese: 汉字 *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_chinese(const char *input, const uint8_t char_len, bool &is_chinese);
  /****************************************************************************************/

  /*************************  Other CJK:  Japanese Korean Chinese *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_other_cjk(const char *input, const uint8_t char_len, bool &is_other_cjk);
  /****************************************************************************************/

  /*************************  Surrogate: only for UTF16 *********************/
  template <ObCharsetType CS_TYPE>
  static
      typename std::enable_if<(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
      is_surrogate_high(const char *input, const uint8_t char_len, bool &is_surrogate_high);

  template <ObCharsetType CS_TYPE>
  static
      typename std::enable_if<(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
      is_surrogate_low(const char *input, const uint8_t char_len, bool &is_surrogate_low);

  template <ObCharsetType CS_TYPE>
  static
      typename std::enable_if<!(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
      is_surrogate_high(const char *input, const uint8_t char_len, bool &is_surrogate_high);

  template <ObCharsetType CS_TYPE>
  static
      typename std::enable_if<!(CS_TYPE == CHARSET_UTF16 || CS_TYPE == CHARSET_UTF16LE), int>::type
      is_surrogate_low(const char *input, const uint8_t char_len, bool &is_surrogate_low);
  /****************************************************************************************/

  /*************************  Ignore: some char should not output itself *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_ignore(const char *input, const uint8_t char_len, bool &ignore);
  /****************************************************************************************/

  /*************************  CN Number: 一二两三 *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_cn_number(const char *input, const uint8_t char_len, bool &is_cn_number);
  /****************************************************************************************/

  /*************************  English letter connector: #,-@ *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_letter_connector(const char *input, const uint8_t char_len, bool &is_connector);
  /****************************************************************************************/

  /*************************  Number letter connector: ,. *********************/
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  is_num_connector(const char *input, const uint8_t char_len, bool &is_connector);
  /****************************************************************************************/

private:
  // only for unicode charset
  template <ObCharsetType CS_TYPE>
  static typename std::enable_if<(CS_TYPE == CHARSET_UTF8MB4 || CS_TYPE == CHARSET_UTF16
                                  || CS_TYPE == CHARSET_UTF16LE),
                                 int>::type
  decode_unicode(const char *input, const uint8_t char_len, ob_wc_t &unicode)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(input)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_FTS_LOG(WARN, "Invalid input", K(input), K(ret));
    } else {
      const unsigned char *ustart = reinterpret_cast<const unsigned char *>(input);
      const unsigned char *uend = reinterpret_cast<const unsigned char *>(&input[char_len]);
      int code_size = common::ob_charset_decode_unicode<CS_TYPE>(ustart, uend, unicode);
      if (code_size < 0) {
        ret = OB_UNEXPECT_INTERNAL_ERROR;
        STORAGE_FTS_LOG(WARN, "Failed to decode unicode", K(code_size), K(ret));
      }
    }
    return ret;
  }
}; // End of class.

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
    is_connector = ObUnicodeBlockUtils::is_unicode_cn_number(unicode);
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
  bool checker = false;
  type = CharType::USELESS;

  if (OB_FAIL(is_alpha<CS_TYPE>(input, char_len, checker))) {
  } else if (checker) {
    type = CharType::ENGLISH_LETTER;
  } else if (OB_FAIL(is_arabic<CS_TYPE>(input, char_len, checker))) {
    STORAGE_FTS_LOG(WARN, "Failed to check arabic letter", K(ret));
  } else if (checker) {
    type = CharType::ARABIC_LETTER;
  } else if (OB_FAIL(is_chinese<CS_TYPE>(input, char_len, checker))) {
    STORAGE_FTS_LOG(WARN, "Failed to check chinese letter", K(ret));
  } else if (checker) {
    type = CharType::CHINESE;
  } else if (OB_FAIL(is_other_cjk<CS_TYPE>(input, char_len, checker))) {
    STORAGE_FTS_LOG(WARN, "Failed to check other cjk letter", K(ret));
  } else if (checker) {
    type = CharType::OTHER_CJK;
  } else if (OB_FAIL(is_surrogate_high<CS_TYPE>(input, char_len, checker))) {
    STORAGE_FTS_LOG(WARN, "Failed to check surrogate high letter", K(ret));
  } else if (checker) {
    type = CharType::SURROGATE_HIGH;
  } else if (OB_FAIL(is_surrogate_low<CS_TYPE>(input, char_len, checker))) {
    STORAGE_FTS_LOG(WARN, "Failed to check surrogate low letter", K(ret));
  } else if (checker) {
    type = CharType::SURROGATE_LOW;
  }
  return ret;
}

inline int ObFTCharUtil::classify_first_char(ObCollationType coll_type,
                                             const char *input,
                                             const uint8_t char_len,
                                             CharType &type)
{
  int ret = OB_SUCCESS;
  ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);

  switch (cs_type) {
  case CHARSET_UTF8MB4: {
    ret = do_classify<CHARSET_UTF8MB4>(input, char_len, type);
    break;
  }
  case CHARSET_UTF16: {
    ret = do_classify<CHARSET_UTF16>(input, char_len, type);
    break;
  }
  case CHARSET_UTF16LE: {
    ret = do_classify<CHARSET_UTF16LE>(input, char_len, type);
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    STORAGE_FTS_LOG(WARN, "Not supported charset type", K(ret), K(cs_type));
    break;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_CHAR_UTIL_H_