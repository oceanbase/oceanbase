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

#define USING_LOG_PREFIX SHARE

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/alloc/alloc_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "plugin/adaptor/ob_plugin_ftparser_adaptor.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::plugin;

inline static ObFTParserParamExport *get_param(ObPluginFTParserParamPtr param)
{
  return (ObFTParserParamExport *)param;
}

#ifdef __cplusplus
extern "C" {
#endif

OBP_PUBLIC_API uint64_t obp_ftparser_parser_version(ObPluginFTParserParamPtr param)
{
  uint64_t version = 0;
  if (OB_NOT_NULL(param)) {
    version = get_param(param)->parser_version_;
  }
  return version;
}
OBP_PUBLIC_API const char * obp_ftparser_fulltext(ObPluginFTParserParamPtr param)
{
  const char *fulltext = nullptr;
  if (OB_NOT_NULL(param)) {
    fulltext = get_param(param)->fulltext_;
  }
  return fulltext;
}

OBP_PUBLIC_API int64_t obp_ftparser_fulltext_length(ObPluginFTParserParamPtr param)
{
  int64_t length = 0;
  if (OB_NOT_NULL(param)) {
    length = get_param(param)->ft_length_;
  }
  return length;
}

OBP_PUBLIC_API ObPluginCharsetInfoPtr obp_ftparser_charset_info(ObPluginFTParserParamPtr param)
{
  ObPluginCharsetInfoPtr cs = nullptr;
  if (OB_NOT_NULL(param)) {
    cs = (ObPluginCharsetInfoPtr)(get_param(param)->cs_);
  }
  return cs;
}

OBP_PUBLIC_API ObPluginParamPtr obp_ftparser_plugin_param(ObPluginFTParserParamPtr param)
{
  ObPluginParamPtr ptr = nullptr;
  if (OB_NOT_NULL(param)) {
    ptr = (ObPluginParamPtr)(get_param(param)->plugin_param_);
  }
  return ptr;
}

OBP_PUBLIC_API ObPluginDatum obp_ftparser_user_data(ObPluginFTParserParamPtr param)
{
  ObPluginDatum ptr = nullptr;
  if (OB_NOT_NULL(param)) {
    ptr = get_param(param)->user_data_;
  }
  return ptr;
}

OBP_PUBLIC_API void obp_ftparser_set_user_data(ObPluginFTParserParamPtr param, ObPluginDatum user_data)
{
  if (OB_NOT_NULL(param)) {
    get_param(param)->user_data_ = user_data;
  }
}

OBP_PUBLIC_API int obp_register_plugin_ftparser(ObPluginParamPtr param,
                                                const char *name,
                                                ObPluginVersion version,
                                                ObPluginFTParser *ftparser,
                                                int64_t ftparser_sizeof,
                                                const char *description)
{
  int ret = OB_SUCCESS;
  ObFtParserAdaptor * ftparser_desc = nullptr;
  ObPluginParam *param_ptr = static_cast<ObPluginParam *>(param);
  if (OB_ISNULL(param) || OB_ISNULL(param_ptr->plugin_mgr_) || OB_ISNULL(ftparser)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(param), K(ftparser));
  } else if (OB_ISNULL(ftparser_desc = OB_NEW(ObFtParserAdaptor, OB_PLUGIN_MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ftparser adaptor", K(ret));
  } else if (OB_FAIL(ftparser_desc->init_adaptor(*ftparser, ftparser_sizeof))) {
    LOG_WARN("failed to init ftparser adaptor", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_plugin_entry(param, OBP_PLUGIN_TYPE_FT_PARSER, name, version,
                                                           ftparser_desc, description))) {
    LOG_WARN("failed to register ftparser plugin entry", K(ret));
  }
  return ret;
}

#ifdef __cplusplus
} // extern "C"
#endif
