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

#include "oceanbase/ob_plugin.h"
#include "plugin/interface/ob_plugin_intf.h"
#include "plugin/adaptor/ob_plugin_adaptor.h"

namespace oceanbase {
namespace plugin {

class ObPluginDlHandle;

/**
 * A suite of plugins in one declare statement
 * @details A plugin handle indicate a dynamic library or a builtin plugin declared by OBP_DECLARE_PLUGIN.
 * A plugin may has more than one plugin entries which registered in `ObPlugin::init`.
 */
class ObPluginHandle final
{
public:
  ObPluginHandle() = default;
  ~ObPluginHandle();

  /**
   * Init a dynamic library plugin
   * @param[in] plugin_mgr The global plugin manager
   * @param[in] plugin_dir The full path of the dynamic library.
   * @param[in] dl_name    The dynamic library name including prefix and suffix to be loaded.
   */
  int init(ObPluginMgr *plugin_mgr, const ObString &plugin_dir, const ObString &dl_name);

  /**
   * Init a builtin plugin
   * @param[in] plugin_mgr  The global plugin manager.
   * @param[in] api_version Plugin API version. Builtin plugins use inner interfaces and structures usually.
   * @param[in] name        The name of the builtin plugin.
   * @param[in] plugin      The ObPlugin pointer.
   */
  int init(ObPluginMgr *plugin_mgr, ObPluginVersion api_version, const ObString name, ObPlugin *plugin);
  void destroy();

  ObPluginVersion plugin_api_version() const { return api_version_; }
  ObPlugin *plugin() const { return plugin_; }
  ObPluginParam &plugin_param() { return plugin_param_; }
  const char *build_revision() const { return plugin_build_revision_; }

  ObPluginDlHandle *dl_handle() const { return dl_handle_; }

  ObString name() const { return name_; }

  int valid() const;

  /**
   * test whether a plugin is valid
   * @details test the version, size and so on
   */
  static bool plugin_is_valid(ObPlugin *plugin);

  TO_STRING_KV(K_(api_version),
               KP_(plugin),
               K_(plugin_param),
               KP_(dl_handle),
               K_(name),
               KCSTRING_(plugin_build_revision));

private:
  void init_plugin_param(ObPluginMgr *plugin_mgr);

private:
  ObPluginVersion   api_version_ = 0;
  ObPlugin *        plugin_      = nullptr;
  ObPluginParam     plugin_param_;

  ObPluginDlHandle *dl_handle_  = nullptr;  /// dynamic library, or shared object if exists

  ObString name_;

  const char *plugin_build_revision_ = nullptr;
};

} // namespace plugin
} // namespace oceanbase
