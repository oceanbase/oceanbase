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

#include "plugin/interface/ob_plugin_intf.h"

namespace oceanbase {

namespace share {
class ObKmsClient;
}

namespace plugin {

class ObPluginKmsIntf : public ObIPluginDescriptor
{
public:
  ObPluginKmsIntf() = default;
  virtual ~ObPluginKmsIntf() = default;

  virtual int create_client(ObIAllocator &allocator, ObPluginParam &param, share::ObKmsClient *&client) = 0;
};
} // namespace plugin
} // namespace oceanbase
