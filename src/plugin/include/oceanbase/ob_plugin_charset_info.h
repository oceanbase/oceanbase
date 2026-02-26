/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "oceanbase/ob_plugin_base.h"

/**
 * @addtogroup ObPlugin
 * @{
 */

#ifdef __cplusplus
extern "C" {
#endif

enum OBP_PUBLIC_API ObPluginCharType
{
  OBP_CHAR_TYPE_UPPER   = 01,
  OBP_CHAR_TYPE_LOWER   = 02,
  OBP_CHAR_TYPE_NUMBER  = 04,
  OBP_CHAR_TYPE_SPACE   = 010,
  OBP_CHAR_TYPE_PUNCT   = 020,  /*< Punctuation */
  OBP_CHAR_TYPE_CONTROL = 040,
  OBP_CHAR_TYPE_BLANK   = 0100,
  OBP_CHAR_TYPE_HEX     = 0200  /*< hexadecimal digit */
};

typedef ObPluginDatum ObPluginCharsetInfoPtr;

/**
 * Test whether this is `utf8` charset
 */
OBP_PUBLIC_API int obp_charset_is_utf8mb4(ObPluginCharsetInfoPtr cs);
OBP_PUBLIC_API const char *obp_charset_csname(ObPluginCharsetInfoPtr cs);

/**
 * Get the ctype of the char
 * @param[in] cs The charset
 * @param[out] ctype The char type. Refer to ObPluginCharType for details.
 * @param[in] str The beginning of the string.
 * @param[in] end The end of the string(not inclusive).
 * @return The byte number of the char(> 0) or error.
 */
OBP_PUBLIC_API int obp_charset_ctype(ObPluginCharsetInfoPtr cs, int *ctype, const unsigned char *str, const unsigned char *end);

/**
 * Count the char number in the string
 * @param[in] cs The charset
 * @param[in] str The beginning of the string.
 * @param[in] end The end of the string(not inclusive).
 */
OBP_PUBLIC_API size_t obp_charset_numchars(ObPluginCharsetInfoPtr cs, const char *str, const char *end);

#ifdef __cplusplus
} // extern "C"
#endif

/** @} */
