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

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/sys/ob_plugin_handle.h"
#include "plugin/sys/ob_plugin_dl_handle.h"

namespace oceanbase {
namespace plugin {

ObPluginHandle::~ObPluginHandle()
{
  destroy();
}

int ObPluginHandle::init(ObPluginMgr *plugin_mgr, const ObString &dl_dir, const ObString &dl_name)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(dl_handle_) || OB_NOT_NULL(plugin_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(plugin_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin mgr is null", K(ret));
  } else if (OB_ISNULL(dl_handle_ = OB_NEW(ObPluginDlHandle, OB_PLUGIN_MEMORY_LABEL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for dl handle", K(ret));
  } else if (OB_FAIL(dl_handle_->init(dl_dir, dl_name))) {
      LOG_WARN("failed to open dl", K(ret));
  } else if (OB_FAIL(dl_handle_->read_value<ObPluginVersion>(OBP_DYNAMIC_PLUGIN_API_VERSION_NAME, api_version_))) {
    LOG_WARN("failed to read interface version value from dl",
             K(OBP_DYNAMIC_PLUGIN_API_VERSION_NAME), K(dl_dir), K(dl_name), K(ret));
  } else if (OB_FAIL(dl_handle_->read_symbol(OBP_DYNAMIC_PLUGIN_PLUGIN_NAME, reinterpret_cast<void *&>(plugin_)))) {
    LOG_WARN("failed to read plugins from dl", K(OBP_DYNAMIC_PLUGIN_PLUGIN_NAME), K(dl_dir), K(dl_name), K(ret));
  } else if (OB_ISNULL(plugin_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get plugins from dl, got null");
  } else if (FALSE_IT(name_ = dl_handle_->name())) {
  } else if (FALSE_IT(init_plugin_param(plugin_mgr))) {
  }

  // read build revision information
  // It's OK if read failed
  if (OB_SUCC(ret) && OB_FAIL(dl_handle_->read_value<const char *>
                                (OBP_DYNAMIC_PLUGIN_BUILD_REVISION_NAME, plugin_build_revision_))) {
    LOG_WARN("[ignore warning] failed to read build revision information from library", K(ret));
    ret = OB_SUCCESS;
  }

  LOG_INFO("init plugin suite handle done", K(*this), K(ret));

  return ret;
}

int ObPluginHandle::init(ObPluginMgr *plugin_mgr, ObPluginVersion api_version, ObString name, ObPlugin *plugin)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(dl_handle_) || OB_NOT_NULL(plugin_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(plugin_mgr) ||
             api_version > OBP_PLUGIN_API_VERSION_CURRENT ||
             OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(plugin_mgr), K(api_version), K(OBP_PLUGIN_API_VERSION_CURRENT), KP(plugin));
  } else {
    api_version_ = api_version;
    plugin_      = plugin;
    name_        = name;
    init_plugin_param(plugin_mgr);
  }
  return ret;
}

void ObPluginHandle::init_plugin_param(ObPluginMgr *plugin_mgr)
{
  plugin_param_.plugin_handle_    = this;
  plugin_param_.plugin_mgr_       = plugin_mgr;
  plugin_param_.plugin_user_data_ = nullptr;
}

void ObPluginHandle::destroy()
{
  api_version_ = 0;
  plugin_      = nullptr;

  if (OB_NOT_NULL(dl_handle_)) {
    OB_DELETE(ObPluginDlHandle, OB_PLUGIN_MEMORY_LABEL, dl_handle_);
    dl_handle_ = nullptr;
  }
}

int ObPluginHandle::valid() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plugin_)) {
    ret = OB_ERROR;
  } else if (OB_ISNULL(dl_handle_)) {
  } else if (this->plugin_api_version() < OBP_PLUGIN_API_VERSION
             || this->plugin_api_version() > OBP_PLUGIN_API_VERSION_CURRENT) {
    ret = OB_PLUGIN_VERSION_INCOMPATIBLE;
    LOG_WARN("plugin API version is incompatible with server",
             "plugin_version", this->plugin_api_version(),
             "server version", OBP_PLUGIN_API_VERSION_CURRENT);
  }
  return ret;
}

bool ObPluginHandle::plugin_is_valid(ObPlugin *plugin)
{
  bool ret = false;
  if (OB_ISNULL(plugin) ||
      OB_ISNULL(plugin->init)) {
    ret = false;
  } else {
    ret = true;
  }

  return ret;
}

} // namespace plugin
} // namespace oceanbase
