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
