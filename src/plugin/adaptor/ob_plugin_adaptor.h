/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/utility/ob_print_utils.h"
#include "oceanbase/ob_plugin.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase {
namespace plugin {

extern lib::ObLabel default_plugin_memory_label;

class ObPluginVersionAdaptor final
{
public:
  ObPluginVersionAdaptor() = default;
  explicit ObPluginVersionAdaptor(int64_t version);
  explicit ObPluginVersionAdaptor(ObPluginVersion version);
  ObPluginVersionAdaptor(uint16_t major, uint16_t minor, uint16_t patch);
  ~ObPluginVersionAdaptor() = default;

  ObPluginVersion version() const;

  ObPluginVersion major() const;
  ObPluginVersion minor() const;
  ObPluginVersion patch() const;

  int64_t to_string(char buf[], int64_t buf_len) const;

private:
  ObPluginVersion version_;
};

class ObPluginPrinter final
{
public:
  ObPluginPrinter(const ObPlugin &plugin) : plugin_(plugin)
  {}

  TO_STRING_KV("author",  plugin_.author,
               "version", ObPluginVersionAdaptor(plugin_.version),
               "license", plugin_.license,
               "init",    plugin_.init,
               "deinit",  plugin_.deinit
               );
private:
  const ObPlugin &plugin_;
};

class ObPluginAdaptor final
{
public:
  ObPluginAdaptor() = default;
  explicit ObPluginAdaptor(ObPlugin *plugin) : plugin_(plugin)
  {}

  ObPlugin *plugin() const { return plugin_; }

  int64_t to_string(char buf[], int64_t buf_len) const
  {
    int64_t ret = 0;
    if (OB_ISNULL(plugin_)) {
      ret = snprintf(buf, buf_len, "nullptr");
    } else {
      ret = ObPluginPrinter(*plugin_).to_string(buf, buf_len);
    }
    return ret;
  }

private:
  ObPlugin *plugin_ = nullptr;
};

} // plugin
} // oceanbase
