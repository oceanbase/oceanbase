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
#include "plugin/sys/ob_plugin_entry_handle.h"
#include "plugin/sys/ob_plugin_handle.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/interface/ob_plugin_intf.h"

namespace oceanbase {
namespace plugin {

const char *ob_plugin_state_to_string(ObPluginState state)
{
  switch (state) {
    case ObPluginState::INVALID: return "INVALID";
    case ObPluginState::UNINIT:  return "UNINIT";
    case ObPluginState::READY:   return "READY";
    case ObPluginState::DEAD:    return "DEAD";
    default:                     return "UNKNOWN_STATE";
  }
}

////////////////////////////////////////////////////////////////////////////////

bool ObPluginEntry::is_valid() const
{
  bool bret = true;
  if (interface_type <= OBP_PLUGIN_TYPE_INVALID || interface_type >= OBP_PLUGIN_TYPE_MAX ||
      name.length() == 0 || name.length() > OB_PLUGIN_NAME_MAX_LENGTH ||
      OB_ISNULL(descriptor) ||
      OB_ISNULL(plugin_handle)) {
    bret = false;
  }
  return bret;
}

////////////////////////////////////////////////////////////////////////////////

ObPluginEntryHandle::~ObPluginEntryHandle()
{
  destroy();
}

int ObPluginEntryHandle::init(const ObPluginEntry &plugin_entry)
{
  int ret = OB_SUCCESS;
  if (!plugin_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try to init an invalid plugin", K(plugin_entry));
  } else {
    plugin_entry_ = plugin_entry;
    state_  = ObPluginState::UNINIT;
  }
  return ret;
}

void ObPluginEntryHandle::destroy()
{
  deinit_plugin();
  state_ = ObPluginState::DEAD;
}

int ObPluginEntryHandle::init_plugin()
{
  int ret = OB_SUCCESS;
  if (state_ >= ObPluginState::READY) {
    ret = OB_INIT_TWICE;
    LOG_WARN("plugin cannot be inited twice", K(*this));
  } else if (OB_ISNULL(plugin_entry_.descriptor) || OB_ISNULL(plugin_entry_.plugin_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't call plugin init", K(ret), K(plugin_entry_));
  } else if (OB_FAIL(plugin_entry_.descriptor->init(&plugin_entry_.plugin_handle->plugin_param()))) {
    LOG_WARN("failed to init plugin descriptor", K(ret));
  } else {
    state_ = ObPluginState::READY;
  }
  return ret;
}

void ObPluginEntryHandle::deinit_plugin()
{
  int ret = OB_SUCCESS;
  if (state_ != ObPluginState::READY) {
  } else if (OB_ISNULL(plugin_entry_.descriptor) || OB_ISNULL(plugin_entry_.plugin_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't call plugin deinit", K(ret), K(plugin_entry_));
  } else if (OB_FAIL(plugin_entry_.descriptor->deinit(&plugin_entry_.plugin_handle->plugin_param()))) {
    LOG_WARN("failed to run plugin descriptor deinit", K(ret));
  } else {
    OB_DELETE(ObIPluginDescriptor, OB_PLUGIN_MEMORY_LABEL, plugin_entry_.descriptor);
    plugin_entry_.descriptor = nullptr;
    state_ = ObPluginState::DEAD;
  }
}

bool ObPluginEntryHandle::ready() const
{
  return state_ == ObPluginState::READY;
}

ObString ObPluginEntryHandle::entry_name() const
{
  return plugin_entry_.name;
}

ObPluginVersion ObPluginEntryHandle::library_version() const
{
  ObPluginVersion version = 0;
  if (OB_NOT_NULL(plugin_entry_.plugin_handle) && OB_NOT_NULL(plugin_entry_.plugin_handle->plugin())) {
    version = plugin_entry_.plugin_handle->plugin()->version;
  }
  return version;
}

} // namespace plugin
} // namespace oceanbase
