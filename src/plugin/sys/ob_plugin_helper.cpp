/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "lib/string/ob_string.h"
#include "share/ob_server_struct.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "plugin/sys/ob_plugin_entry_handle.h"
#include "plugin/sys/ob_plugin_handle.h"

namespace oceanbase {

using namespace lib;
using namespace common;
using namespace storage;

namespace plugin {

int ObPluginHelper::find_ftparser_entry(const ObString &parser_name, ObPluginEntryHandle *&entry_handle)
{
  int ret = OB_SUCCESS;
  entry_handle = nullptr;
  if (OB_FAIL(GCTX.plugin_mgr_->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, parser_name, entry_handle))) {
    LOG_DEBUG("failed to find parser", K(parser_name), K(ret));
  } else if (OB_ISNULL(entry_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find plugin success but got null", K(parser_name), K(ret));
  } else if (!entry_handle->ready()) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("plugin is not ready", K(parser_name));
  } else if (entry_handle->entry().interface_version > OBP_FTPARSER_INTERFACE_VERSION_CURRENT) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("invalid interface version",
             K(ObPluginVersionAdaptor(entry_handle->entry().interface_version)),
             K(ObPluginVersionAdaptor(OBP_FTPARSER_INTERFACE_VERSION_CURRENT)));
  } else if (OB_ISNULL(entry_handle->entry().descriptor)) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("find ftparser but descriptor is null", K(ret), K(parser_name));
  } else {
    LOG_TRACE("find ftparser plugin", K(parser_name));
  }
  return ret;
}

int ObPluginHelper::find_ftparser(const ObString &parser_name, ObFTParser &ftparser)
{
  int ret = OB_SUCCESS;
  ObPluginEntryHandle *entry_handle = nullptr;
  if (OB_FAIL(find_ftparser_entry(parser_name, entry_handle))) {
    if (OB_FUNCTION_NOT_DEFINED == ret) {
      LOG_USER_ERROR(OB_FUNCTION_NOT_DEFINED, parser_name.length(), parser_name.ptr());
      LOG_DEBUG("no such parser", K(parser_name));
    } else {
      LOG_WARN("failed to find ftparser", K(parser_name), K(ret));
    }
  } else {
    share::ObPluginName plugin_name;
    plugin_name.set_name(parser_name);
    ftparser.set_name_and_version(plugin_name, static_cast<int64_t>(entry_handle->library_version()));
    LOG_TRACE("find ftparser plugin", K(parser_name), K(entry_handle->library_version()));
  }
  return ret;
}

int ObPluginHelper::find_ftparser(const ObString &parser_name, ObIFTParserDesc *&ftparser, ObPluginParam *&param)
{
  int ret = OB_SUCCESS;
  ftparser = nullptr;
  param = nullptr;
  ObPluginEntryHandle *entry_handle = nullptr;
  if (OB_FAIL(find_ftparser_entry(parser_name, entry_handle))) {
    LOG_DEBUG("failed to find parser", K(parser_name), K(ret));
  } else if (OB_ISNULL(entry_handle->entry().plugin_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find a plugin entry without plugin handle", K(ret), KPC(entry_handle));
  } else {
    ftparser = reinterpret_cast<ObIFTParserDesc *>(entry_handle->entry().descriptor);
    param = &entry_handle->entry().plugin_handle->plugin_param();
    LOG_TRACE("find ftparser plugin", K(parser_name));
  }
  return ret;
}

int ObPluginHelper::register_plugin_entry(ObPluginParamPtr param,
                                          ObPluginType type,
                                          const char *name,
                                          ObPluginVersion interface_version,
                                          ObIPluginDescriptor *descriptor,
                                          const char *description)
{
  int ret = OB_SUCCESS;
  ObPluginParam *param_ptr = static_cast<ObPluginParam *>(param);
  if (OB_ISNULL(param) || OB_ISNULL(name) || OB_ISNULL(param_ptr->plugin_mgr_) || OB_ISNULL(descriptor) ||
      type >= OBP_PLUGIN_TYPE_MAX || type <= OBP_PLUGIN_TYPE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(param), KCSTRING(name), K(interface_version), K(descriptor), K(type));
  } else {
    ObPluginEntry plugin_entry;
    plugin_entry.interface_type    = type;
    plugin_entry.name              = ObString(name);
    plugin_entry.interface_version = interface_version;
    plugin_entry.descriptor        = descriptor;
    plugin_entry.description       = ObString(description);
    plugin_entry.plugin_handle     = param_ptr->plugin_handle_;

    if (OB_FAIL(param_ptr->plugin_mgr_->register_plugin(plugin_entry))) {
      LOG_WARN("failed to register plugin", KPC(param_ptr->plugin_handle_), K(ret), K(plugin_entry));
    } else {
      LOG_INFO("register plugin susccess", K(plugin_entry));
    }
  }
  return ret;
}

} // namespace plugin
} // namespace oceanbase
