/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "oceanbase/ob_plugin.h"

namespace oceanbase {
namespace plugin {

class ObPluginHandle;
class ObPluginMgr;

class ObPluginParam final
{
public:
  ObPluginParam() = default;
  ~ObPluginParam() = default;

  TO_STRING_KV(KP_(plugin_handle), KP_(plugin_mgr), KP_(plugin_user_data));

public:
  ObPluginHandle *plugin_handle_  = nullptr;
  ObPluginMgr *   plugin_mgr_     = nullptr;
  ObPluginDatum plugin_user_data_ = nullptr;
};

class ObIPluginDescriptor
{
public:
  virtual ~ObIPluginDescriptor() = default;

  virtual int init(ObPluginParam *param) { return OB_SUCCESS; }
  virtual int deinit(ObPluginParam *param) { return OB_SUCCESS; }
};
} // namespace plugin
} // namespace oceanbase
