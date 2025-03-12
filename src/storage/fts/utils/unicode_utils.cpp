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

#include "src/storage/fts/utils/unicode_utils.h"

#include "lib/charset/ob_charset_string_helper.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{
// See: java's Character.UnicodeBlock

bool ObUnicodeBlockUtils::is_alpha(const ob_wc_t unicode)
{
  bool is_ascii_alpha
      = (unicode >= 0x41 && unicode <= 0x5a) || (unicode >= 0x61 && unicode <= 0x7a);
  bool is_fw_alpha
      = (unicode >= 0xFF21 && unicode <= 0xFF3A) || (unicode >= 0xFF41 && unicode <= 0xFF5A);
  return is_ascii_alpha || is_fw_alpha;
}

bool ObUnicodeBlockUtils::is_arabic(const ob_wc_t unicode)
{
  bool is_ascii_arabic = (unicode >= 0x30 && unicode <= 0x39);
  bool is_fw_arabic = (unicode >= 0xFF10 && unicode <= 0xFF19);
  return is_ascii_arabic || is_fw_arabic;
}

bool ObUnicodeBlockUtils::check_high_surrogate(const ob_wc_t unicode)
{
  return unicode >= 0xD800 && unicode <= 0xDBFF;
}

bool ObUnicodeBlockUtils::check_high_private_use_surrogate(const ob_wc_t unicode)
{
  return unicode >= 0xDB80 && unicode <= 0xDBFF;
}

bool ObUnicodeBlockUtils::check_low_surrogate(const ob_wc_t unicode)
{
  return unicode >= 0xDC00 && unicode <= 0xDFFF;
}

bool ObUnicodeBlockUtils::check_letter_connector(const ob_wc_t unicode)
{
  static constexpr char LETTER_CONNECTOR[] = {'#', '&', '+', '-', '.', '@', '_'};

  ob_wc_t code = unicode;
  if (code > 0xFF00 && code < 0xFF5F) {
    code -= 0xFEE0;
  }
  for (int i = 0; i < ARRAYSIZEOF(LETTER_CONNECTOR); i++) {
    if (code == LETTER_CONNECTOR[i]) {
      return true;
    }
  }
  return false;
}

bool ObUnicodeBlockUtils::check_num_connector(const ob_wc_t unicode)
{
  static constexpr char NUM_CONNECTOR[] = {',', '.'};
  ob_wc_t code = unicode;
  if (code > 0xFF00 && code < 0xFF5F) {
    code -= 0xFEE0;
  }
  for (int i = 0; i < ARRAYSIZEOF(NUM_CONNECTOR); i++) {
    if (code == NUM_CONNECTOR[i]) {
      return true;
    }
  }
  return false;
}

bool ObUnicodeBlockUtils::check_ignore_as_single(const ob_wc_t unicode)
{
  // cjk to ascii part
  if (unicode > 0xFF00 && unicode < 0xFF5F) {
    // if output as a single char, it will be ignored
    if (!is_alpha(unicode) && !is_arabic(unicode)) {
      return true;
    }
  }
  return false;
}

bool ObUnicodeBlockUtils::is_chinese(const ob_wc_t unicode)
{
  // variables name keep same with it's code block name, skip code style.
  bool CJK_UNIFIED_IDEOGRAPHS = (unicode >= 0x4E00 && unicode <= 0x9FFF);
  bool CJK_COMPATIBILITY_IDEOGRAPHS = (unicode >= 0xF900 && unicode <= 0xFAFF);
  bool CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A = (unicode >= 0x3400 && unicode <= 0x4DBF);
  return CJK_UNIFIED_IDEOGRAPHS || CJK_COMPATIBILITY_IDEOGRAPHS
         || CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A;
}

bool ObUnicodeBlockUtils::is_other_cjk(const ob_wc_t unicode)
{
  // variables name keep same with it's code block name, skip code style.
  bool HALFWIDTH_AND_FULLWIDTH_FORMS = (unicode >= 0xFF00 && unicode <= 0xFFEF);

  bool HANGUL_SYLLABLES = (unicode >= 0xAC00 && unicode <= 0xD7AF);
  bool HANGUL_JAMO = (unicode >= 0x1100 && unicode <= 0x11FF);
  bool HANGUL_COMPATIBILITY_JAMO = (unicode >= 0x3130 && unicode <= 0x318f);

  bool HIRAGANA = (unicode >= 0x3040 && unicode <= 0x309F);
  bool KATAKANA = (unicode >= 0x30A0 && unicode <= 0x30FF);
  bool KATAKANA_PHONETIC_EXTENSIONS = (unicode >= 0x31F0 && unicode <= 0x31FF);

  return HALFWIDTH_AND_FULLWIDTH_FORMS || HANGUL_SYLLABLES || HANGUL_JAMO
         || HANGUL_COMPATIBILITY_JAMO || HIRAGANA || KATAKANA || KATAKANA_PHONETIC_EXTENSIONS;
}

bool ObUnicodeBlockUtils::is_surrogate(const ob_wc_t unicode)
{
  bool HIGH_SURROGATES = (unicode >= 0xD800 && unicode <= 0xDB7F);
  bool LOW_SURROGATES = (unicode >= 0xDC00 && unicode <= 0xDFFF);

  return (HIGH_SURROGATES || LOW_SURROGATES);
}

bool ObUnicodeBlockUtils::is_convertable_fullwidth(const ob_wc_t unicode)
{
  bool FW_SPACE = (unicode == 0x3000);
  bool FW_OHTER = (unicode > 0xFF00 && unicode < 0xFF5F);
  return FW_SPACE || FW_OHTER;
}

ob_wc_t ObUnicodeBlockUtils::get_unicode_from_u8(const char *input, const uint8_t char_len)
{
  ob_wc_t unicode = 0;
  const unsigned char *ustart = reinterpret_cast<const unsigned char *>(input);
  const unsigned char *uend = reinterpret_cast<const unsigned char *>(&input[char_len]);
  int code_size = common::ob_charset_decode_unicode<common::CHARSET_UTF8MB4>(ustart, uend, unicode);
  return unicode;
}

#define OB_IK_GET_NUMBER_UNICODE(str) get_unicode_from_u8(str, sizeof(str))
bool ObUnicodeBlockUtils::is_unicode_cn_number(ob_wc_t unicode)
{
  // "一二两三四五六七八九十零壹贰叁肆伍陆柒捌玖拾百千万亿拾佰仟萬億兆卅廿"
  static const ob_wc_t cn_number_array[] = {
      OB_IK_GET_NUMBER_UNICODE(u8"一"), OB_IK_GET_NUMBER_UNICODE(u8"二"),
      OB_IK_GET_NUMBER_UNICODE(u8"两"), OB_IK_GET_NUMBER_UNICODE(u8"三"),
      OB_IK_GET_NUMBER_UNICODE(u8"四"), OB_IK_GET_NUMBER_UNICODE(u8"五"),
      OB_IK_GET_NUMBER_UNICODE(u8"六"), OB_IK_GET_NUMBER_UNICODE(u8"七"),
      OB_IK_GET_NUMBER_UNICODE(u8"八"), OB_IK_GET_NUMBER_UNICODE(u8"九"),
      OB_IK_GET_NUMBER_UNICODE(u8"十"), OB_IK_GET_NUMBER_UNICODE(u8"零"),
      OB_IK_GET_NUMBER_UNICODE(u8"壹"), OB_IK_GET_NUMBER_UNICODE(u8"贰"),
      OB_IK_GET_NUMBER_UNICODE(u8"叁"), OB_IK_GET_NUMBER_UNICODE(u8"肆"),
      OB_IK_GET_NUMBER_UNICODE(u8"伍"), OB_IK_GET_NUMBER_UNICODE(u8"陆"),
      OB_IK_GET_NUMBER_UNICODE(u8"柒"), OB_IK_GET_NUMBER_UNICODE(u8"捌"),
      OB_IK_GET_NUMBER_UNICODE(u8"玖"), OB_IK_GET_NUMBER_UNICODE(u8"拾"),
      OB_IK_GET_NUMBER_UNICODE(u8"百"), OB_IK_GET_NUMBER_UNICODE(u8"千"),
      OB_IK_GET_NUMBER_UNICODE(u8"万"), OB_IK_GET_NUMBER_UNICODE(u8"亿"),
      OB_IK_GET_NUMBER_UNICODE(u8"拾"), OB_IK_GET_NUMBER_UNICODE(u8"佰"),
      OB_IK_GET_NUMBER_UNICODE(u8"仟"), OB_IK_GET_NUMBER_UNICODE(u8"萬"),
      OB_IK_GET_NUMBER_UNICODE(u8"億"), OB_IK_GET_NUMBER_UNICODE(u8"兆"),
      OB_IK_GET_NUMBER_UNICODE(u8"卅"), OB_IK_GET_NUMBER_UNICODE(u8"廿"),
  };
  for (int i = 0; i < sizeof(cn_number_array) / sizeof(cn_number_array[0]); i++) {
    if (unicode == cn_number_array[i]) {
      return true;
    }
  }
  return false;
}

} //  namespace storage
} //  namespace oceanbase
