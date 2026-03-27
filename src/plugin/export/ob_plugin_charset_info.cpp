/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
