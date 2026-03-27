/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
