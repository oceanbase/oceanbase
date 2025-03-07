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

#include "lib/ob_errno.h"
#include "plugin/sys/ob_plugin_builtin.h"

using namespace oceanbase::common;
using namespace oceanbase::plugin;

#define OB_DECLARE_BUILTIN_PLUGIN(name)                                                     \
  do {                                                                                      \
    extern ObPlugin OBP_BUILTIN_PLUGIN_VAR(name);                                           \
    const char *plugin_name = OBP_STRINGIZE(OBP_BUILTIN_PLUGIN_VAR(name));                  \
    ObPlugin *plugin = &OBP_BUILTIN_PLUGIN_VAR(name);                                       \
    if (OB_SUCC(ret) && OB_FAIL(plugins.push_back(ObBuiltinPlugin(plugin_name, plugin)))) { \
      LOG_WARN("failed to push back builtin plugin", KCSTRING(#name), K(ret));              \
    }                                                                                       \
  } while (false)

extern "C" {
static int __plugin_register_global_plugins(ObIArray<ObBuiltinPlugin> &plugins)
{
  int ret = OB_SUCCESS;
  /// Append builtin plugins here
  OB_DECLARE_BUILTIN_PLUGIN(fts_parser);
  return ret;
}

} // extern "C"

namespace oceanbase {
namespace plugin {

int plugin_register_global_plugins(ObIArray<ObBuiltinPlugin> &plugins)
{
  return __plugin_register_global_plugins(plugins);
}

} // namespace plugin
} // namespace oceanbase
