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

#include "plugin/external_table/ob_external_format.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "plugin/interface/ob_plugin_external_intf.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_encryption_util.h"

namespace oceanbase {
namespace plugin {

int ObPluginFormat::init(ObIAllocator &allocator, bool encrypt_mode)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    allocator_    = &allocator;
    encrypt_mode_ = encrypt_mode;
    inited_       = true;
  }
  return ret;
}

void ObPluginFormat::destroy()
{
  if (inited_) {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(type_name_.ptr())) {
      allocator_->free(type_name_.ptr());
      type_name_.reset();
    }
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(parameters_.ptr())) {
      allocator_->free(parameters_.ptr());
      parameters_.reset();
    }
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(stored_parameters_.ptr())) {
      allocator_->free(stored_parameters_.ptr());
      stored_parameters_.reset();
    }
    allocator_ = nullptr;
    inited_    = false;
  }
}

int ObPluginFormat::create_engine(ObIAllocator &allocator, ObExternalDataEngine *&engine) const
{
  int ret = OB_SUCCESS;
  ObIExternalDescriptor *desc = nullptr;
  engine = nullptr;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ObPluginHelper::find_external_table(type_name_, desc))) {
    LOG_WARN("failed to find external table descriptor", K(ret), K(type_name_));
  } else if (OB_ISNULL(desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find external table descriptor successfully, but got null", K(type_name_), K(ret));
  } else if (OB_FAIL(desc->create_data_engine(allocator, parameters_, type_name_, engine))) {
    LOG_WARN("failed to create data engine", K(type_name_), K(ret));
  }
  return ret;
}

int64_t ObPluginFormat::to_json_string(char buf[], int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "\"%.*s\":\"%.*s\"",
                  type_name_.length(), type_name_.ptr(),
                  stored_parameters_.length(), stored_parameters_.ptr());
  return pos;
}

int ObPluginFormat::load_from_json_node(json::Pair *node)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json node is null");
  } else if (OB_FAIL(ob_write_string(*allocator_, node->name_, type_name_, true/*c_style*/))) {
    LOG_WARN("failed to copy typename", K(node->name_), K(ret));
  } else if (OB_ISNULL(node->value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no value", KP(node->value_));
  } else if (OB_ISNULL(node->value_) || json::JT_STRING != node->value_->get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json type, should be string", K(node->value_->get_type()));
  } else if (OB_FAIL(ob_write_string(
      *allocator_, node->value_->get_string(), stored_parameters_, true/*c_style*/))) {
    LOG_WARN("failed to load parameters from json", K(ret));
  } else if (OB_FAIL(decrypt(stored_parameters_, parameters_))) {
    LOG_WARN("failed to decrypt parameters", K(ret));
  }
  return ret;
}

int ObPluginFormat::set_type_name(const ObString &type_name)
{
  return set_string_value(type_name, type_name_);
}

int ObPluginFormat::encrypt(const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  char *hex_buff = nullptr;
  int64_t hex_buff_len = 0;
  char *encrypted_string = nullptr;
  int64_t encrypted_string_max_len = 0;
  ObString src_str = src;

#if defined(OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty() || !encrypt_mode_) {
  } else if (FALSE_IT(encrypted_string_max_len = share::ObEncryptionUtil::sys_encrypted_length(src.length()))) {
  } else if (OB_ISNULL(encrypted_string = (char *)allocator_->alloc(encrypted_string_max_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(encrypted_string_max_len), K(ret));
  } else {
    int64_t encrypt_len = -1;
    if (OB_FAIL(share::ObEncryptionUtil::encrypt_sys_data(tenant_id,
                                                          src.ptr(), src.length(),
                                                          encrypted_string,
                                                          encrypted_string_max_len,
                                                          encrypt_len))) {

      LOG_WARN("fail to encrypt_sys_data", KR(ret), K(src));
    } else if (0 >= encrypt_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypt_len is invalid", K(ret), K(encrypt_len));
    } else {
      src_str.assign_ptr(encrypted_string, encrypt_len);
      LOG_TRACE("succ to encrypt src", K(ret), K(encrypt_len));
    }
  }
#endif

  if (OB_FAIL(ret) || src_str.empty()) {
  } else if (FALSE_IT(hex_buff_len = src_str.length() * 2 + 1)) {
  } else if (OB_ISNULL(hex_buff = (char *)allocator_->alloc(hex_buff_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(hex_buff_len), K(ret));
  } else if (OB_FAIL(to_hex_cstr(src_str.ptr(), src_str.length(), hex_buff, hex_buff_len))) {
    LOG_WARN("fail to print to hex str", K(ret));
  } else if (OB_FAIL(set_string_value(ObString(hex_buff), dst))) {
    LOG_WARN("failed to deep copy encrypted_string", K(ret));
  } else {
    LOG_TRACE("succ to transform src", K(ret), K(hex_buff_len));
  }

  if (OB_ISNULL(hex_buff)) {
    allocator_->free(hex_buff);
    hex_buff = nullptr;
  }

  if (OB_ISNULL(encrypted_string)) {
    allocator_->free(encrypted_string);
    encrypted_string = nullptr;
  }
  return ret;
}

int ObPluginFormat::decrypt(const ObString &src, ObString &decrypted)
{
  int ret = OB_SUCCESS;
  ObString src_str = src;
  char *encrypted_str = nullptr; // unhex string for src
  int64_t encrypted_str_len = src.length() / 2 + 1;
  char *plain_string = nullptr;
  int64_t plain_string_max_len = 0;
  if (src.empty()) {
    decrypted = src;
  } else if (0 != src.length() % 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src", K(src.length()), K(ret));
  } else if (OB_ISNULL(encrypted_str = (char *)allocator_->alloc(encrypted_str_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(encrypted_str_len), K(ret));
  } else if (OB_FAIL(hex_to_cstr(src.ptr(), src.length(), encrypted_str, encrypted_str_len))) {
    LOG_WARN("failed to hex to cstr", K(src.length()), K(ret));
  } else {
    src_str.assign_ptr(encrypted_str, src.length() / 2);
  }

#if defined (OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(ret)) {
  } else if (src_str.empty() || !encrypt_mode_) {
    // do nothing
  } else if (FALSE_IT(plain_string_max_len =
      share::ObEncryptionUtil::sys_decrypted_length(src_str.length()) +
                      share::ObBlockCipher::OB_CIPHER_BLOCK_LENGTH + 1)) {
  } else if (OB_ISNULL(plain_string = (char *)allocator_->alloc(plain_string_max_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(plain_string_max_len));
  } else {
    LOG_DEBUG("decrypt data buffer length", K(plain_string_max_len), K(src_str.length()));

    int64_t plain_string_len = -1;
    if (OB_FAIL(share::ObEncryptionUtil::decrypt_sys_data(tenant_id,
                                                          src_str.ptr(),
                                                          src_str.length(),
                                                          plain_string,
                                                          plain_string_max_len,
                                                          plain_string_len))) {
      LOG_WARN("failed to decrypt_sys_data", K(ret), K(src_str.length()));
    } else if (0 >= plain_string_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decrypt string failed", K(ret), K(src.length()), K(plain_string_len));
    } else {
      src_str.assign_ptr(plain_string, plain_string_len);
      LOG_TRACE("succ to decrypt src", K(ret));
    }
  }

#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_string_value(src_str, decrypted))) {
    LOG_WARN("failed to deep copy string", K(ret), K(src_str.length()));
  }

  if (OB_NOT_NULL(plain_string)) {
    allocator_->free(plain_string);
    plain_string = nullptr;
  }

  if (OB_NOT_NULL(encrypted_str)) {
    allocator_->free(encrypted_str);
    encrypted_str = nullptr;
  }
  return ret;
}

int ObPluginFormat::set_parameters(const ObString &parameters)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KP(allocator_));
  } else if (OB_FAIL(encrypt(parameters, stored_parameters_))) {
    LOG_WARN("failed to encrypt string", K(ret));
  } else if (OB_FAIL(set_string_value(parameters, parameters_))) {
    LOG_WARN("failed to copy parameters", K(ret));
  }
  return ret;
}

int ObPluginFormat::set_string_value(const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_NOT_NULL(dst.ptr())) {
      allocator_->free(dst.ptr());
      dst.reset();
    }
    if (OB_FAIL(ob_write_string(*allocator_, src, dst, true/*c style*/))) {
      LOG_WARN("failed to set string value", K(ret), K(src));
    }
  }
  return ret;
}
} // namespace plugin
} // namespace oceanbase
