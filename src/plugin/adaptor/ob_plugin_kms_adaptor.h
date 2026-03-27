/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "oceanbase/ob_plugin_kms.h"
#include "plugin/interface/ob_plugin_kms_intf.h"

namespace oceanbase {
namespace plugin {

class ObKmsAdaptor final : public ObPluginKmsIntf
{
public:
  ObKmsAdaptor() = default;
  virtual ~ObKmsAdaptor() = default;

  int init_adaptor(const ObPluginKms &kms, int64_t descriptor_sizeof);

  int create_client(ObIAllocator &allocator, ObPluginParam &param, share::ObKmsClient *&client);

private:
  ObPluginKms kms_;
  bool        inited_ = false;
};

} // namespace plugin
} // namespace oceanbase
