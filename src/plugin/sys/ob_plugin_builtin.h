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
