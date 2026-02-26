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

#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_vector.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/sys/ob_plugin_helper.h"

namespace oceanbase {
namespace plugin {

struct ObPluginEntry;
class ObPluginEntryHandle;
class ObPluginHandle;
class ObBuiltinPlugin;

/**
 * plugin manager
 *
 * @details including builtin plugins (built as static library and linked into observer)
 * and dynamic plugins (loaded when starting observer).
 * A plugin library contains a plugin structure(plugin handle) and some plugin entries.
 * The plugin library declare the plugin structure by OBP_DECLARE_PLUGIN macro and
 * register plugin entries in the `plugin.init` routine.
 */
class ObPluginMgr final
{
public:
  /**
   * plugin entry list
   * A plugin with the same name can have more than one entries with difference version.
   * The version is `library_version` which is defined by `OBP_DECLARE_PLUGIN` macro.
   * A library only has one `library_version` so the plugin entries in the same library
   * have the same `library_version`.
   */
  using PluginEntryList      = common::ObSortedVector<ObPluginEntryHandle *>;

  /**
   * plugin entry map. Map plugin name to plugin entry list
   */
  using PluginEntryHandleMap = common::hash::ObHashMap<ObString,
                                                       PluginEntryList *,
                                                       common::hash::LatchReadWriteDefendMode,
                                                       ObPluginNameHash,
                                                       ObPluginNameEqual>;

  /**
   * plugin (library) map.
   * One plugin library can has more than one plugin entries.
   * The name is the library name or builtin plugin name.
   */
  using PluginHandleMap = common::hash::ObHashMap<ObString, ObPluginHandle *>;

public:
  ObPluginMgr() = default;
  ~ObPluginMgr();

  /**
   * init
   *
   * @param plugin_dir the dynamic libraries location
   */
  int init(const ObString &plugin_dir);
  void destroy();

  /**
   * find plugin
   */
  int find_plugin(ObPluginType plugin_type, const ObString &plugin_name, ObPluginEntryHandle *&entry_handle);

  /**
   * find plugin with specific version
   * @param[in] plugin_type   The plugin type
   * @param[in] plugin_name   The plugin name to find
   * @param[in] version       The plugin library version
   * @param[out] entry_handle The plugin entry handle if found
   * @return
   *   - OB_SUCCESS found success.
   *   - OB_FUNCTION_NOT_DEFINED no error occur but can't find the plugin entry.
   *   - Others error
   */
  int find_plugin(ObPluginType plugin_type,
                  const ObString &plugin_name,
                  ObPluginVersion version,
                  ObPluginEntryHandle *&entry_handle);

  int register_plugin(const ObPluginEntry &plugin_entry);

  /**
   * load all builtin plugins
   * @note the observer will fail to start if builtin plugins load fail
   */
  int load_builtin_plugins();

  /**
   * load dynamic plugins
   * @param plugins_load the parameter string of plugins to load
   * @note Observer will continue to start even if failed to load some libraries.
   * You should check the DBA_OB_PLUGINS view to make sure the plugins all loaded success.
   */
  int load_dynamic_plugins(const ObString &plugins_load);

  int list_all_plugin_entries(ObIArray<ObPluginEntryHandle *> &plugins);

private:
  int init_plugin_handle_maps(const lib::ObMemAttr &mem_attr);

  /**
   * install plugin in the dynamic library
   *
   * Make sure the library file located in `plugin_dir`.
   */
  int install_library(const ObString &dl_name);

  int find_or_load_dl(const ObString &dl_name, ObPluginHandle *&plugin_handle);

  int load_plugin(ObPluginHandle *plugin_handle);
  int load_builtin_plugin(const ObBuiltinPlugin &plugin);

  int find_plugin_entry_list(ObPluginType type, const ObString &name, PluginEntryList *&entry_list);

private:
  static const int64_t DEFAULT_PLUGIN_BUCKET_NUM = 53L;

private:
  bool inited_ = false;

  PluginEntryHandleMap entry_handle_maps_[OBP_PLUGIN_TYPE_MAX];
  PluginHandleMap      plugin_handle_map_;

  ObString plugin_dir_;

  DISALLOW_COPY_AND_ASSIGN(ObPluginMgr);
};

} // namespace plugin
} // namespace oceanbase
