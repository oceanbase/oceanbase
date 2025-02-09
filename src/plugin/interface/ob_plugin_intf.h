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

#include "oceanbase/ob_plugin.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

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
