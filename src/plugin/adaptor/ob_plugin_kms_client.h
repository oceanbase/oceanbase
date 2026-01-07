#ifdef OB_BUILD_TDE_SECURITY
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

#include "share/ob_encrypt_kms.h"
#include "oceanbase/ob_plugin_kms.h"
#include "plugin/interface/ob_plugin_intf.h"

namespace oceanbase {
namespace plugin {

class ObKmsClientPlugin final : public share::ObKmsClient
{
public:
  ObKmsClientPlugin(ObPluginKms &kms, ObPluginParam &param) : kms_(kms), param_(param)
  {}

  virtual ~ObKmsClientPlugin();

  int init(const char *kms_info, int64_t kms_len) override;
  int generate_key(const share::ObPostKmsMethod method, int64_t &key_version, common::ObString &encrypted_key) override;
  int update_key(int64_t &key_version, common::ObString &encrypted_key) override;
  int get_key(int64_t key_version,
              const common::ObString &encrypted_key,
              const share::ObPostKmsMethod method,
              common::ObString &key) override;
private:
  ObPluginKmsClientPtr client_ = nullptr;
  ObPluginKms &        kms_;
  ObPluginParam &      param_;
};

} // namespace plugin
} // namespace oceanbase

#endif // OB_BUILD_TDE_SECURITY
