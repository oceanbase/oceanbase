/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_PLUGIN_BUILTIN_H
#define OCEANBASE_PLUGIN_BUILTIN_H

#include "oceanbase/ob_plugin.h"
#include "lib/container/ob_iarray.h"
#include "plugin/adaptor/ob_plugin_adaptor.h"

namespace oceanbase {
namespace plugin {

struct ObBuiltinPlugin final
{
  ObString        name;
  ObPluginAdaptor plugin;

  ObBuiltinPlugin() = default;
  ObBuiltinPlugin(const char *_name, ObPlugin *_plugin) : name(_name), plugin(_plugin)
  {}

  TO_STRING_KV(K(name), K(plugin));
};

int plugin_register_global_plugins(common::ObIArray<ObBuiltinPlugin> &plugins);

} // namespace plugin
} // namespace oceanbase

#endif // OCEANBASE_PLUGIN_BUILTIN_H
