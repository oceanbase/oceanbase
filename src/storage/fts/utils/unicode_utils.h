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

#ifndef _OCEANBASE_STORAGE_FTS_UTILS_UNICODE_UTILS_H_
#define _OCEANBASE_STORAGE_FTS_UTILS_UNICODE_UTILS_H_

#include "lib/charset/ob_ctype.h"

namespace oceanbase
{
namespace storage
{
class ObUnicodeBlockUtils final
{
public:
  static bool is_unicode_cn_number(const ob_wc_t unicode);

  static bool is_alpha(const ob_wc_t unicode);

  static bool is_arabic(const ob_wc_t unicode);

  static bool check_high_surrogate(const ob_wc_t unicode);

  static bool check_high_private_use_surrogate(const ob_wc_t unicode);

  static bool check_low_surrogate(const ob_wc_t unicode);

  static bool check_letter_connector(const ob_wc_t unicode);

  static bool check_num_connector(const ob_wc_t unicode);

  static bool check_ignore_as_single(const ob_wc_t unicode);

  static bool is_chinese(const ob_wc_t unicode);

  static bool is_other_cjk(const ob_wc_t unicode);

  static bool is_surrogate(const ob_wc_t unicode);

  static bool is_convertable_fullwidth(const ob_wc_t unicode);

private:
  // Do compare with u8 char
  // only for static number
  static ob_wc_t get_unicode_from_u8(const char *input, const uint8_t char_len);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_UTILS_UNICODE_UTILS_H_
