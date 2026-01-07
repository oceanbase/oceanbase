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
#define USING_LOG_PREFIX SHARE

#include "plugin/adaptor/ob_plugin_kms_client.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace plugin {

using namespace oceanbase::share;
////////////////////////////////////////////////////////////////////////////////
// ObKmsClientPlugin
ObKmsClientPlugin::~ObKmsClientPlugin()
{
  if (OB_NOT_NULL(kms_.release_client) && OB_NOT_NULL(client_)) {
    kms_.release_client(client_);
    client_ = nullptr;
  }
}

int ObKmsClientPlugin::init(const char *kms_info, int64_t kms_len)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(client_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KP(client_));
  } else if (OB_ISNULL(kms_.create_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("member function create_client is null", K(ret));
  } else if (OB_FAIL(kms_.create_client(&param_, kms_info, kms_len, &client_))) {
    LOG_WARN("failed to create kms client", K(ret));
  }
  return ret;
}

typedef int (*create_key_func)(ObPluginKmsClientPtr client, void *key_info, int64_t key_max_size, int64_t *key_info_size);

int create_key(ObPluginKmsClientPtr client, create_key_func key_creator, ObIAllocator &allocator, ObString &encrypted_key)
{
  int ret = OB_SUCCESS;
  const int64_t key_info_max_size = OB_MAX_ENCRYPTION_KEY_NAME_LENGTH;
  char key_info[key_info_max_size];
  int64_t key_info_size = 0;
  encrypted_key.reset();
  if (OB_ISNULL(key_creator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("member function is null", K(ret));
  } else if (OB_FAIL(key_creator(client, key_info, key_info_max_size, &key_info_size))) {
    LOG_WARN("failed to create key", K(ret));
  } else if (key_info_size < 0 || key_info_size > key_info_max_size) {
    ret = OB_PLUGIN_ERROR;
    LOG_WARN("plugin return error value", K(key_info_size), K(key_info_max_size), K(ret));
  } else if (key_info_size == 0) {
    // do nothing
  } else if (OB_FAIL(ob_write_string(allocator, ObString(key_info_size, key_info), encrypted_key))) {
    LOG_WARN("failed to copy key_info to encrypted_key", K(ret), K(key_info_size));
  }
  return ret;
}

int ObKmsClientPlugin::generate_key(const ObPostKmsMethod /*method*/, int64_t &/*key_version*/, common::ObString &encrypted_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_key(client_, kms_.generate_key, allocator_, encrypted_key))) {
    LOG_WARN("failed to generate key", K(ret));
  }
  return ret;
}

int ObKmsClientPlugin::update_key(int64_t &/*key_version*/, common::ObString &encrypted_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_key(client_, kms_.update_key, allocator_, encrypted_key))) {
    if (OBP_ENTRY_NOT_EXIST == ret) {
      ret = OB_HASH_NOT_EXIST;
      LOG_TRACE("key not exist", K(ret));
    } else {
      LOG_WARN("failed to update key", K(ret));
    }
  }
  return ret;
}

int ObKmsClientPlugin::get_key(int64_t /*key_version*/,
                               const ObString &encrypted_key,
                               const ObPostKmsMethod /*method*/,
                               ObString &key_ret)
{
  int ret = OB_SUCCESS;
  const int64_t key_max_size = OB_MAX_ENCRYPTION_KEY_NAME_LENGTH;
  char key[key_max_size];
  int64_t key_size = 0;
  if (OB_ISNULL(kms_.get_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kms.get_key is null");
  } else if (OB_FAIL(kms_.get_key(client_, encrypted_key.ptr(), encrypted_key.length(), key, key_max_size, &key_size))) {
    LOG_WARN("failed to get key", K(ret));
  } else if (key_size <= 0 || key_size > key_max_size) {
    ret = OB_PLUGIN_ERROR;
    LOG_WARN("plugin return invalid value", K(ret), K(key_size), K(key_max_size));
  } else if (OB_FAIL(ob_write_string(allocator_, ObString(key_size, key), key_ret))) {
    LOG_WARN("failed to copy key buffer to key", K(ret), K(key_size));
  }
  return ret;
}
} // namespace plugin
} // namespace oceanbase
#endif // OB_BUILD_TDE_SECURITY
