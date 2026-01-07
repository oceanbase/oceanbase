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

#include "oceanbase/ob_plugin.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "plugin/adaptor/ob_plugin_kms_adaptor.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;
using namespace oceanbase::plugin;

#ifdef __cplusplus
extern "C" {
#endif

OBP_PUBLIC_API
int obp_register_plugin_kms(ObPluginParamPtr param,
                            const char *name,
                            ObPluginVersion version,
                            ObPluginKms *kms_descriptor,
                            int64_t descriptor_sizeof,
                            const char *description)
{
  // TODO hnwyllmm add plugin. to the name. we should support copy name
  int ret = OB_SUCCESS;
  ObKmsAdaptor * kms_adaptor = nullptr;
  ObPluginParam *param_ptr = static_cast<ObPluginParam *>(param);
  if (OB_ISNULL(param) || OB_ISNULL(param_ptr->plugin_mgr_) || OB_ISNULL(kms_descriptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(param), K(kms_descriptor));
  } else if (OB_ISNULL(name) || name[0] == '\0') {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plugin name", KCSTRING(name));
  } else if (ObString(name).prefix_match_ci(ObPluginHelper::KMS_NAME_PREFIX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plugin name: should not starts with the prefix", K(ObPluginHelper::KMS_NAME_PREFIX));
  } else if (OB_NOT_NULL(ObString(name).find('_'))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plugin name: should not contains character '_'", KCSTRING(name));
  } else if (OB_ISNULL(kms_adaptor = OB_NEW(ObKmsAdaptor, OB_PLUGIN_MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate kms adaptor", K(ret), K(sizeof(*kms_adaptor)));
  } else if (OB_FAIL(kms_adaptor->init_adaptor(*kms_descriptor, descriptor_sizeof))) {
    LOG_WARN("failed to init kms adaptor", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_plugin_entry(param, OBP_PLUGIN_TYPE_KMS, name, version,
                                                           kms_adaptor, description))) {
    LOG_WARN("failed to register kms plugin entry", K(ret));
  } else {
    LOG_INFO("register KMS plugin entry success", KCSTRING(name), K(version));
  }
  return ret;
}

#ifdef __cplusplus
} // extern "C"
#endif
