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
/**
 * @defgroup ObPluginKms
 * @{
 */

/**
 * KMS interface version
 * @note this is the minimum version.
 * You should add new version if you add new routines.
 */
#define OBP_KMS_INTERFACE_VERSION OBP_PLUGIN_API_VERSION_0_3_0


/**
 * current KMS interface version
 * @note you should change this value if you add some new routines in interface struct.
 */
#define OBP_KMS_INTERFACE_VERSION_CURRENT OBP_KMS_INTERFACE_VERSION

#ifdef __cplusplus
extern "C" {
#endif

typedef ObPluginDatum ObPluginKmsClientPtr;

/**
 * The interface for KMS client
 * To get encryption key from remote KMS system.
 */
struct OBP_PUBLIC_API ObPluginKms
{
  /** this routine will be called when loading the library */
  int (*init)(ObPluginParamPtr param);

  /** this routine will be called when unloading the library */
  int (*deinit)(ObPluginParamPtr param);

  /**
   * Create a KMS client
   * @param[in] kms_info configuration for KMS client. Not '\0' terminated.
   * @param[in] kms_len  the length of `kms_info`.
   * @param[out] client  The KMS client.
   */
  int (*create_client)(ObPluginParamPtr param, const void *kms_info, int64_t kms_len, ObPluginKmsClientPtr *client);

  /**
   * Release the KMS client
   * @param[in] client The KMS client to be released. Do nothing if `client` is null.
   */
  int (*release_client)(ObPluginKmsClientPtr client);

  /**
   * create/generate a KMS key
   * @param[in] client The KMS client.
   * @param[in|out] key_info The buffer to store the key generated.
   *                `key_info` is used to retrieve KMS key by `get_key`.
   * @param[in] key_max_size The buffer size of key_info.
   * @param[out] key_info_size The size of `key_info` generated.
   */
  int (*generate_key)(ObPluginKmsClientPtr client, void *key_info, int64_t key_max_size, int64_t *key_info_size);

  /**
   * Update/rotate a KMS key.
   */
  int (*update_key)(ObPluginKmsClientPtr client, void *key_info, int64_t key_max_size, int64_t *key_info_size);

  /**
   * Get the KMS key correspond to `key_info`
   * @param[in] client The KMS client.
   * @param[in] key_info The `id` to get KMS key.
   * @param[in] key_info_size The size of `key_info`.
   * @param[in|out] key The buffer to store KMS `key`.
   * @param[in] max_key_size The buffer size of `key`.
   * @param[out] key_size The size of the `key`.
   */
  int (*get_key)(ObPluginKmsClientPtr client,
                 const void *key_info, int64_t key_info_size,
                 void *key, int64_t max_key_size, int64_t *key_size);
};

/**
 * Register KMS plugin
 * @note  use `OBP_REGISTER_KMS` instead
 * @param[in] param the param of ObPluginParam which is a passed in param in `plugin::init`
 * @param[in] name  KMS client name, 64 characters maximum
 * @param[in] kms_descriptor the KMS struct. The object will be copied.
 * @param[in] descriptor_sizeof the size of `kms_descriptor`
 * @param[in] description the description of the plugin.
 */
OBP_PUBLIC_API int obp_register_plugin_kms(ObPluginParamPtr param,
                                           const char *name,
                                           ObPluginVersion version,
                                           ObPluginKms *kms_descriptor,
                                           int64_t descriptor_sizeof,
                                           const char *description);

#define OBP_REGISTER_KMS(param, name, descriptor, description)    \
  obp_register_plugin_kms(param,                                  \
                          name,                                   \
                          OBP_KMS_INTERFACE_VERSION_CURRENT,      \
                          &descriptor,                            \
                          (int64_t)sizeof(descriptor),            \
                          description);


#ifdef __cplusplus
} // extern "C"
#endif

/** @} */
