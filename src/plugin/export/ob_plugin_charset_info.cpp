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

#include "oceanbase/ob_plugin_charset_info.h"
#include "oceanbase/ob_plugin_errno.h"
#include "lib/charset/ob_ctype.h"

#ifdef __cplusplus
extern "C" {
#endif

static ObCharsetInfo *get_charset_info(ObPluginDatum cs)
{
  return (ObCharsetInfo*)cs;
}

OBP_PUBLIC_API int obp_charset_ctype(ObPluginCharsetInfoPtr cs, int *ctype, const unsigned char *s, const unsigned char *e)
{
  return get_charset_info(cs)->cset->ctype(get_charset_info(cs), ctype, s, e);
}

OBP_PUBLIC_API const char *obp_charset_csname(ObPluginCharsetInfoPtr cs)
{
  return get_charset_info(cs)->csname;
}

OBP_PUBLIC_API int obp_charset_is_utf8mb4(ObPluginCharsetInfoPtr cs)
{
  bool result = 0 == strcasecmp(get_charset_info(cs)->csname, OB_UTF8MB4);
  return result ? OBP_SUCCESS : OBP_PLUGIN_ERROR;
}

OBP_PUBLIC_API size_t obp_charset_numchars(ObPluginCharsetInfoPtr cs, const char *pos, const char *end)
{
  return ob_numchars_mb(get_charset_info(cs), pos, end);
}

#ifdef __cplusplus
} // extern "C"
#endif
