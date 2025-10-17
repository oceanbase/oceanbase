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

#include "plugin/share/ob_properties.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_encryption_util.h"

namespace oceanbase {
namespace plugin {

constexpr int64_t DEFAULT_PROPERTIES_BUCKET_NUM = 53;

ObProperties::~ObProperties()
{
  destroy();
}

int ObProperties::init(ObIAllocator &allocator, bool encrypt_mode)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(allocator_)) {
    ret = OB_INIT_TWICE;
  } else {
    allocator_ = &allocator;
    encrypt_mode_ = encrypt_mode;
    if (OB_FAIL(pairs_.create(DEFAULT_PROPERTIES_BUCKET_NUM, ObMemAttr(MTL_ID(), OB_PLUGIN_MEMORY_LABEL)))) {
      LOG_WARN("failed to create properties map", K(ret));
    }
  }
  return ret;
}

void ObProperties::destroy()
{
  if (OB_NOT_NULL(allocator_)) {
    PropertyMap::iterator iter = pairs_.begin();
    PropertyMap::iterator itend = pairs_.end();
    for ( ; iter != itend; ++iter) {
      ObString &key = iter->first;
      PropertyValue &value = iter->second;
      allocator_->free(key.ptr());
      value.destroy(*allocator_);
    }
    pairs_.destroy();
    allocator_ = nullptr;
  }
}

int ObProperties::set_property(const ObString &key, const ObString &value)
{
  int ret = OB_SUCCESS;
  ObString property_key;
  PropertyValue property_value;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("properties is not init");
  } else if (OB_FAIL(ob_write_string(*allocator_, key, property_key, true/*c_style*/))) {
    LOG_WARN("failed to copy key to property", K(ret), K(key));
  } else if (FALSE_IT(property_value.sensitive = false)) {
  } else if (OB_FAIL(ob_write_string(*allocator_, value, property_value.decrypted_value, true/*c_style*/))) {
    // value maybe sensitive, so don't log it's value
    LOG_WARN("failed to copy property value to property", K(ret), K(value.length()), K(key));
  } else if (OB_FAIL(pairs_.set_refactored(property_key, property_value))) {
    LOG_WARN("failed to insert property into map", K(ret), K(key), K(value.length()));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(property_key.ptr())) {
      allocator_->free(property_key.ptr());
    }
    property_value.destroy(*allocator_);
  }
  LOG_DEBUG("set property", K(key), K(value), K(ret));
  return ret;
}

int ObProperties::get_property(const ObString &key, ObString &value) const
{
  int ret = OB_SUCCESS;
  const PropertyValue *property_value = nullptr;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(property_value = pairs_.get(key))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    value = property_value->decrypted_value;
  }
  return ret;
}

int ObProperties::mark_sensitive(const ObString &key)
{
  int ret = OB_SUCCESS;
  PropertyValue *value = nullptr;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(value = pairs_.get(key))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (value->sensitive && !value->encrypted_value.empty()) {
  } else if (OB_FAIL(encrypt(value->decrypted_value, value->encrypted_value))) {
    LOG_WARN("failed to encrypt value", K(key), K(ret), K(value->decrypted_value.length()));
  } else {
    value->sensitive = true;
  }
  return ret;
}

bool ObProperties::is_sensitive(const ObString &key) const
{
  bool sensitive = false;
  const PropertyValue *value = nullptr;
  if (OB_ISNULL(allocator_)) {
  } else if (OB_ISNULL(value = pairs_.get(key))) {
  } else {
    sensitive = value->sensitive;
  }
  return sensitive;
}

int ObProperties::encrypt(const ObString &src, ObString &encrypted)
{
  int ret = OB_SUCCESS;
#if defined(OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty() || !encrypt_mode_) {
    encrypted = src;
  } else {
    const int64_t max_length = OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH;
    char encrypted_string[max_length / 2] = {0};
    char hex_buff[max_length + 1] = {0}; // +1 to reserve space for \0
    int64_t encrypt_len = -1;
    if (OB_FAIL(share::ObEncryptionUtil::encrypt_sys_data(tenant_id,
                                                          src.ptr(), src.length(),
                                                          encrypted_string,
                                                          sizeof(encrypted_string),
                                                          encrypt_len))) {

      LOG_WARN("fail to encrypt_sys_data", KR(ret), K(src));
    } else if (0 >= encrypt_len || max_length < encrypt_len * 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypt_len is invalid", K(ret), K(encrypt_len), K(max_length));
    } else if (OB_FAIL(to_hex_cstr(encrypted_string, encrypt_len, hex_buff, max_length + 1))) {
      LOG_WARN("fail to print to hex str", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, ObString(hex_buff), encrypted))) {
      LOG_WARN("failed to deep copy encrypted_string", K(ret));
    } else {
      LOG_TRACE("succ to encrypt src", K(ret));
    }
  }
#else
  encrypted = src;
#endif
  return ret;
}
int ObProperties::decrypt(const ObString &src, ObString &decrypted)
{
  int ret = OB_SUCCESS;
#if defined (OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty() || !encrypt_mode_) {
    // do nothing
    decrypted = src;
  } else if (0 != src.length() % 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src", K(src.length()), K(ret));
  } else {
    const int64_t max_length = OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH;
    char encrypted_password_not_hex[max_length] = {0};
    char plain_string[max_length + 1] = { 0 }; // need +1 to reserve space for \0
    int64_t plain_string_len = -1;
    if (OB_FAIL(hex_to_cstr(src.ptr(),
                            src.length(),
                            encrypted_password_not_hex,
                            max_length))) {
      LOG_WARN("failed to hex to cstr", K(src.length()), K(ret));
    } else if (OB_FAIL(share::ObEncryptionUtil::decrypt_sys_data(tenant_id,
                                                          encrypted_password_not_hex,
                                                          src.length() / 2,
                                                          plain_string,
                                                          max_length + 1,
                                                          plain_string_len))) {
      LOG_WARN("failed to decrypt_sys_data", K(ret), K(src.length()));
    } else if (0 >= plain_string_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decrypt string failed", K(ret), K(src.length()), K(plain_string_len));
    } else if (OB_FAIL(ob_write_string(*allocator_, ObString(plain_string_len, plain_string), decrypted))) {
      LOG_WARN("failed to deep copy plain_string", K(ret));
    } else {
      LOG_TRACE("succ to decrypt src", K(ret));
    }
  }
#else
  decrypted = src;
#endif
  return ret;
}

int64_t ObProperties::to_json_string(char buf[], int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  bool need_comma = false;
  J_OBJ_START();
  PropertyMap::const_iterator iter = pairs_.begin();
  PropertyMap::const_iterator itend = pairs_.end();
  for ( ; OB_SUCC(ret) && iter != itend; ++iter) {
    const ObString &key = iter->first;
    const PropertyValue &value = iter->second;
    if (!need_comma) {
      need_comma = true;
    } else {
      J_COMMA();
    }
    databuff_printf(buf, buf_len, pos, "\"%.*s\":", key.length(), key.ptr());
    pos += value.to_json_string(buf + pos, buf_len - pos);
  }
  J_OBJ_END();
  LOG_DEBUG("properties to json string", K(ObString(pos, buf)));
  return pos;
}

int ObProperties::load_from_json_node(json::Value *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json node is null");
  } else if (node->get_type() != json::JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json object type", K(node->get_type()));
  } else {
    PropertyValue property_value;
    ObString property_key;

    DLIST_FOREACH(item, node->get_object()) {
      const ObString &name = item->name_;
      const json::Value *json_value = item->value_;
      if (OB_ISNULL(json_value) || json_value->get_type() != json::JT_OBJECT) {
        LOG_WARN("invalid json value or value type", KPC(json_value));
      } else if (OB_FAIL(ob_write_string(*allocator_, name, property_key))) {
        LOG_WARN("failed to copy property key", K(ret), K(name));
      } else if (OB_FAIL(property_value.from_json(*allocator_, json_value))) {
        LOG_WARN("failed to decode property value from json", K(ret), K(property_key));
      } else if (property_value.sensitive &&
                 OB_FAIL(decrypt(property_value.encrypted_value, property_value.decrypted_value))) {
        LOG_WARN("failed to decrypt value", K(ret), K(property_value.encrypted_value.length()));
      } else if (OB_FAIL(pairs_.set_refactored(property_key, property_value))) {
        LOG_WARN("failed to insert key value", K(ret), K(property_key), K(property_value));
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("decode map entry", K(property_key), K(property_value));
        property_key.reset();
        property_value.reuse();
      } else {
        allocator_->free(property_key.ptr());
        property_value.destroy(*allocator_);
      }
    }
  }
  return ret;
}

int ObProperties::equal_to(const ObProperties &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  bret = false;
  if (this->pairs_.size() != other.pairs_.size()) {
    bret = false;
  } else {
    bret = true;
    PropertyMap::const_iterator iter = this->pairs_.begin();
    PropertyMap::const_iterator itend = this->pairs_.end();
    for ( ; OB_SUCC(ret) && bret && iter != itend; ++iter) {
      const PropertyValue &this_value = iter->second;
      const PropertyValue *other_value = other.pairs_.get(iter->first);
      if (OB_ISNULL(other_value)) {
        LOG_DEBUG("other properties doesn't have the key", K(iter->first));
        bret = false;
      } else if (!this_value.equal_to(*other_value)) {
        bret = false;
      }
    }
  }
  return ret;
}

int ObProperties::foreach(std::function<int (const ObString&, const ObString&)> callback) const
{
  int ret = OB_SUCCESS;
  for (ObProperties::PropertyMap::const_iterator iter = pairs_.begin(), itend = pairs_.end();
       OB_SUCC(ret) && iter != itend; ++iter) {
    ret = callback(iter->first, iter->second.decrypted_value);
  }

  return ret;
}

int ObProperties::to_kv_string(char buf[], int64_t buf_len, int64_t &pos,
                               const ObString &sensitive_string,
                               const ObString &kv_delimiter,
                               const ObString &item_delimiter,
                               const ObString &key_quote,
                               const ObString &value_quote) const
{
  int ret = OB_SUCCESS;
  for (ObProperties::PropertyMap::const_iterator iter = pairs_.begin(), itend = pairs_.end();
       OB_SUCC(ret) && iter != itend; ++iter) {
    ObString value;
    if (iter->second.sensitive && !sensitive_string.empty()) {
      value = sensitive_string;
    } else {
      value = iter->second.decrypted_value;
    }

    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s%.*s%.*s%.*s%.*s%.*s%.*s%.*s",
                                key_quote.length(), key_quote.ptr(),
                                iter->first.length(), iter->first.ptr(),
                                key_quote.length(), key_quote.ptr(),
                                kv_delimiter.length(), kv_delimiter.ptr(),
                                value_quote.length(), value_quote.ptr(),
                                value.length(), value.ptr(),
                                value_quote.length(), value_quote.ptr(),
                                item_delimiter.length(), item_delimiter.ptr()))) {
      LOG_WARN("failed to print kv", K(ret));
    }
  }

  if (OB_SUCC(ret) && pairs_.size() > 0) {
    pos -= item_delimiter.length();
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObProperties::PropertyValue
int64_t ObProperties::PropertyValue::to_string(char buf[], int64_t len) const
{
  const char *bool_string = sensitive ? "true" : "false";
  const ObString &value = sensitive ? encrypted_value : decrypted_value;
  return snprintf(buf, len, "sensitive:%s,value:%.*s", bool_string, value.length(), value.ptr());
}

int64_t ObProperties::PropertyValue::to_json_string(char buf[], int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObString *value = sensitive ? &encrypted_value : &decrypted_value;
  J_OBJ_START();
  databuff_printf(buf, buf_len, pos, "\"sensitive\":%s,\"value\":\"%.*s\"",
                  sensitive ? "true" : "false",
                  value->length(), value->ptr());
  J_OBJ_END();
  return pos;
}

int ObProperties::PropertyValue::from_json(ObIAllocator &allocator, const json::Value *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(node));
  } else if (node->get_type() != json::JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, expect json object", K(node->get_type()));
  } else {
    bool json_sensitive = false;
    ObString json_string_value;
    DLIST_FOREACH(item, node->get_object()) {
      if (OB_ISNULL(item->value_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("json node has invalid value", K(item->name_), KP(item->value_));
      } else if (0 == item->name_.case_compare("sensitive")) {
        if (item->value_->get_type() == json::JT_TRUE) {
          json_sensitive = true;
        } else if (item->value_->get_type() == json::JT_FALSE) {
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("node 'sensitive' has invalue type: need bool", K(item->value_->get_type()));
        }
      } else if (0 == item->name_.case_compare("value")) {
        if (item->value_->get_type() == json::JT_STRING) {
          json_string_value = item->value_->get_string();
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("node 'value' has invalid type: need string", K(item->value_->get_type()));
        }
      } else {
        LOG_TRACE("unknown json node: ignore", K(item->name_));
      }
    }

    if (OB_SUCC(ret)) {
      this->sensitive = json_sensitive;
      ObString &this_value = this->sensitive ? encrypted_value : decrypted_value;
      if (OB_FAIL(ob_write_string(allocator, json_string_value, this_value))) {
        LOG_WARN("failed to copy json string", K(json_string_value.length()), K(ret));
      } else {
        LOG_DEBUG("get property value form json", K(*this));
      }
    }
  }
  return ret;
}

bool ObProperties::PropertyValue::equal_to(const PropertyValue &other) const
{
  bool bret = true;
  if (this->sensitive != other.sensitive) {
    bret = false;
  } else if (this->sensitive) {
    bret = (0 == this->encrypted_value.compare(other.encrypted_value));
  } else {
    bret = (0 == this->decrypted_value.compare(other.decrypted_value));
  }
  return bret;
}

void ObProperties::PropertyValue::reuse()
{
  sensitive = false;
  decrypted_value.reset();
  encrypted_value.reset();
}

void ObProperties::PropertyValue::destroy(ObIAllocator &allocator)
{
  allocator.free(decrypted_value.ptr());
  LOG_DEBUG("free property value", KP(decrypted_value.ptr()), KP(encrypted_value.ptr()), KCSTRING(lbt()));
  if (decrypted_value.ptr() != encrypted_value.ptr()) {
    allocator.free(encrypted_value.ptr());
  }
  decrypted_value.reset();
  encrypted_value.reset();
}

} // namespace plugin
} // namespace oceanbase
