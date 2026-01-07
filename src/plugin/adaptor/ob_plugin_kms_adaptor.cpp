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

#define USING_LOG_PREFIX SHARE

#include "plugin/adaptor/ob_plugin_kms_adaptor.h"

#ifdef OB_BUILD_TDE_SECURITY
#include "plugin/adaptor/ob_plugin_kms_client.h"
#endif // OB_BUILD_TDE_SECURITY

namespace oceanbase {
namespace plugin {

using namespace oceanbase::share;

////////////////////////////////////////////////////////////////////////////////
// ObKmsClientAdaptor
int64_t plugin_kms_descriptor_min_size()
{
  ObPluginKms *kms = nullptr;
  return (int64_t)(&kms->get_key) - (int64_t)(kms) + sizeof(kms->get_key);
}

int ObKmsAdaptor::init_adaptor(const ObPluginKms &kms, int64_t descriptor_sizeof)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited", K(ret));
  } else if (descriptor_sizeof < plugin_kms_descriptor_min_size() || descriptor_sizeof > sizeof(ObPluginKms)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid kms descriptor size",
             K(ret), K(descriptor_sizeof), K(plugin_kms_descriptor_min_size), K(sizeof(ObPluginKms)));
  } else if (OB_ISNULL(kms.create_client) ||
             OB_ISNULL(kms.generate_key) ||
             OB_ISNULL(kms.update_key) ||
             OB_ISNULL(kms.get_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid kms descriptor: some member functions are null", K(ret));
  } else {
    memset(&kms_, 0, sizeof(kms_));
    memcpy(&kms_, &kms, descriptor_sizeof);
    inited_ = true;
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObKmsAdaptor::create_client(ObIAllocator &allocator, ObPluginParam &param, ObKmsClient *&client_ret)
{
  int ret = OB_SUCCESS;
  client_ret = nullptr;
  ObKmsClientPlugin *client = nullptr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(client = (ObKmsClientPlugin *)allocator.alloc(sizeof(*client)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObKmsClientPlugin", K(sizeof(ObKmsClientPlugin)), K(ret));
  } else {
    new (client) ObKmsClientPlugin(kms_, param);
    client_ret = client;
    client = nullptr;
  }

  if (OB_NOT_NULL(client)) {
    allocator.free(client);
    client = nullptr;
  }

  return ret;
}
#else // OB_BUILD_TDE_SECURITY
int ObKmsAdaptor::create_client(ObIAllocator &/*allocator*/, ObPluginParam &/*param*/, ObKmsClient *&client_ret)
{
  client_ret = nullptr;
  return OB_NOT_SUPPORTED;
}
#endif // OB_BUILD_TDE_SECURITY


} // namespace plugin
} // namespace oceanbase
