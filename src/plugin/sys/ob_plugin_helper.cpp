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

#include "oceanbase/ob_plugin_kms.h"
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
const ObString ObPluginHelper::KMS_NAME_PREFIX{"plugin."};

int ObPluginHelper::find_plugin_entry(const ObString &name,
                                      ObPluginType type,
                                      ObPluginVersion current_version,
                                      ObPluginEntryHandle *&entry_handle)
{
  int ret = OB_SUCCESS;
  entry_handle = nullptr;
  if (OB_FAIL(GCTX.plugin_mgr_->find_plugin(type, name, entry_handle))) {
    LOG_DEBUG("failed to find plugin", K(name), K(ret));
  } else if (OB_ISNULL(entry_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find plugin success but got null", K(name), K(ret));
  } else if (!entry_handle->ready()) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("plugin is not ready", K(name));
  } else if (entry_handle->entry().interface_version > current_version) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("invalid interface version",
             K(ObPluginVersionAdaptor(entry_handle->entry().interface_version)),
             K(ObPluginVersionAdaptor(current_version)));
  } else if (OB_ISNULL(entry_handle->entry().descriptor)) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_WARN("find plugin but descriptor is null", K(ret), K(name));
  } else {
    LOG_TRACE("find plugin", K(name));
  }
  return ret;
}

int ObPluginHelper::find_ftparser_entry(const ObString &parser_name, ObPluginEntryHandle *&entry_handle)
{
  return find_plugin_entry(parser_name,
                           OBP_PLUGIN_TYPE_FT_PARSER,
                           OBP_FTPARSER_INTERFACE_VERSION_CURRENT,
                           entry_handle);
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
    if (OB_FUNCTION_NOT_DEFINED == ret) {
      LOG_USER_ERROR(OB_FUNCTION_NOT_DEFINED, parser_name.length(), parser_name.ptr());
      LOG_DEBUG("no such parser", K(parser_name));
    } else {
      LOG_WARN("failed to find ftparser", K(parser_name), K(ret));
    }
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

int ObPluginHelper::find_kms(const ObString &name, ObPluginKmsIntf *&kms, ObPluginParam *&param)
{
  int ret = OB_SUCCESS;
  kms = nullptr;
  param = nullptr;
  ObPluginEntryHandle *entry_handle = nullptr;
  ObString real_name;
  if (name.prefix_match_ci(KMS_NAME_PREFIX)) {
    real_name.assign_ptr(name.ptr() + KMS_NAME_PREFIX.length(), name.length() - KMS_NAME_PREFIX.length());
  } else {
    real_name.assign_ptr(name.ptr(), name.length());
  }

  if (OB_FAIL(find_plugin_entry(real_name, OBP_PLUGIN_TYPE_KMS, OBP_KMS_INTERFACE_VERSION_CURRENT, entry_handle))) {
    if (OB_FUNCTION_NOT_DEFINED == ret) {
      LOG_USER_ERROR(OB_FUNCTION_NOT_DEFINED, real_name.length(), real_name.ptr());
      LOG_DEBUG("no such plugin", K(real_name), K(name));
    } else {
      LOG_WARN("failed to find plugin", K(real_name), K(name), K(ret));
    }
  } else if (OB_ISNULL(entry_handle->entry().plugin_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find a plugin entry without plugin handle", K(ret), KPC(entry_handle));
  } else {
    kms = reinterpret_cast<ObPluginKmsIntf *>(entry_handle->entry().descriptor);
    param = &entry_handle->entry().plugin_handle->plugin_param();
    LOG_TRACE("find kms plugin", K(real_name), K(name));
  }
  return ret;
}

int ObPluginHelper::find_external_table(const common::ObString &name,
                                        ObIExternalDescriptor *&external_desc)
{
  int ret = OB_SUCCESS;
  ObPluginEntryHandle *entry_handle = nullptr;
  if (OB_FAIL(find_plugin_entry(name, OBP_PLUGIN_TYPE_EXTERNAL, OBP_EXTERNAL_INTERFACE_VERSION_CURRENT, entry_handle))) {
    LOG_DEBUG("failed to find external table plugin entry", K(name));
  } else if (OB_ISNULL(entry_handle->entry().plugin_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find a plugin entry without plugin handle", K(ret), KPC(entry_handle));
  } else {
    external_desc = reinterpret_cast<ObIExternalDescriptor *>(entry_handle->entry().descriptor);
    LOG_DEBUG("find external table plugin entry successfully", K(name), KP(external_desc));
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
