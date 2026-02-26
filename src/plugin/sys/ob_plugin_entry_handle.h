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

#pragma once

#include "plugin/adaptor/ob_plugin_adaptor.h"

namespace oceanbase {
namespace plugin {

class ObPluginHandle;
class ObIPluginDescriptor;

enum class ObPluginState
{
  INVALID,
  UNINIT,
  READY,
  DEAD
};

const char *ob_plugin_state_to_string(ObPluginState state);

struct ObPluginEntry final
{
  ObPluginType         interface_type = OBP_PLUGIN_TYPE_INVALID;
  ObString             name;
  ObPluginVersion      interface_version;
  ObIPluginDescriptor *descriptor = nullptr;
  ObString             description;
  ObPluginHandle      *plugin_handle = nullptr;

  bool is_valid() const;

  TO_STRING_KV(K(interface_type),
               K(name),
               K(ObPluginVersionAdaptor(interface_version)),
               KP(descriptor),
               K(description),
               KP(plugin_handle));
};

/**
 * Indicate one plugin
 */
class ObPluginEntryHandle final
{
public:
  ObPluginEntryHandle() = default;
  ~ObPluginEntryHandle();

  /**
   * init plugin handle it self
   * @note plugin is not inited here
   */
  int init(const ObPluginEntry &entry);
  void destroy();

  /**
   * init the plugin it self
   */
  int init_plugin();
  void deinit_plugin();

  ObString entry_name() const;
  const ObPluginEntry &entry() const { return plugin_entry_; }

  ObPluginState state() const { return state_; }
  ObPluginHandle *plugin_handle() const { return plugin_entry_.plugin_handle; }
  ObPluginVersion library_version() const;

  /**
   * test whether the plugin is ready
   * @details if the plugin is loaded but not inited success, then it's not ready
   * @note you shouldn't use this plugin if it's not ready.
   */
  bool ready() const;

  TO_STRING_KV(K_(plugin_entry), K_(state), KCSTRING(ob_plugin_state_to_string(state_)));

private:
  ObPluginEntry plugin_entry_;
  ObPluginState state_  = ObPluginState::INVALID;
};

} // namespace plugin
} // namespace oceanbase
