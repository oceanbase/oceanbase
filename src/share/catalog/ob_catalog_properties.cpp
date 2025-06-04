/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "ob_catalog_properties.h"

#include "deps/oblib/src/lib/list/ob_dlist.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_hex_utils_base.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_encryption_util.h"
#include "share/rc/ob_tenant_base.h"
#include "src/share/ob_define.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace share
{
const char *ObCatalogProperties::CATALOG_TYPE_STR[] = {"ODPS"};
static_assert(array_elements(ObCatalogProperties::CATALOG_TYPE_STR) == static_cast<size_t>(ObCatalogProperties::CatalogType::MAX_TYPE), "Not enough initializer for ObCatalogProperties");
static_assert(array_elements(ObODPSCatalogProperties::OPTION_NAMES) == static_cast<size_t>(ObODPSCatalogProperties::ObOdpsCatalogOptions::MAX_OPTIONS), "Not enough initializer for ObODPSCatalogProperties");

int ObCatalogProperties::to_string_with_alloc(ObString &str, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = DEFAULT_BUF_LENGTH / 2;
  int64_t pos = 0;
  do {
    buf_len *= 2;
    ret = OB_SUCCESS;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(to_string(buf, buf_len, pos))) {
      LOG_WARN("failed to write string", K(ret));
    }
  } while (OB_SIZE_OVERFLOW == ret);
  OX(str.assign_ptr(buf, pos));
  return ret;
}

int64_t ObCatalogProperties::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  OZ(to_string(buf, buf_len, pos));
  // ignore ret
  return pos;
}

int ObCatalogProperties::to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  bool is_valid_format = type_ > CatalogType::INVALID_TYPE && type_ < CatalogType::MAX_TYPE;
  OZ(J_OBJ_START());
  OZ(databuff_print_kv(buf,
                       buf_len,
                       pos,
                       R"("TYPE")",
                       is_valid_format
                           ? ObCatalogProperties::CATALOG_TYPE_STR[static_cast<size_t>(type_)]
                           : "INVALID"));
  if (is_valid_format) {
    OZ(to_json_kv_string(buf, buf_len, pos));
  }
  OZ(J_OBJ_END());
  return ret;
}

int ObCatalogProperties::parse_catalog_type(const ObString &str, CatalogType &type)
{
  int ret = OB_SUCCESS;
  json::Value *data = NULL;
  json::Parser parser;
  ObArenaAllocator temp_allocator;
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("format string is empty", K(ret), K(str));
  } else if (OB_FAIL(parser.init(&temp_allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(str.ptr(), str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(str));
  } else if (NULL == data || json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json value", K(ret), KPC(data));
  } else {
    json::Pair *type_node = data->get_object().get_first();
    if (type_node->value_->get_type() != json::JT_STRING) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json format", K(ret), K(str));
    } else {
      ObString type_str = type_node->value_->get_string();
      for (int i = 0; i < array_elements(ObCatalogProperties::CATALOG_TYPE_STR); ++i) {
        if (type_str.case_compare(ObCatalogProperties::CATALOG_TYPE_STR[i]) == 0) {
          type = static_cast<CatalogType>(i);
          break;
        }
      }
    }
  }
  return ret;
}

int ObCatalogProperties::resolve_catalog_type(const ParseNode &node, CatalogType &type)
{
  int ret = OB_SUCCESS;
  type = CatalogType::INVALID_TYPE;
  const ParseNode *type_node = NULL;
  bool is_type_set = false;
  for (int32_t i = 0; OB_SUCC(ret) && !is_type_set && i < node.num_child_; ++i) {
    if (OB_NOT_NULL(node.children_[i]) && T_EXTERNAL_FILE_FORMAT_TYPE == node.children_[i]->type_) {
      type_node = node.children_[i];
      if (type_node->num_child_ != 1 || OB_ISNULL(type_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected parse node", K(ret));
      } else {
        ObString string_v = ObString(type_node->children_[0]->str_len_, type_node->children_[0]->str_value_).trim_space_only();
        for (int i = 0; !is_type_set && i < static_cast<int>(CatalogType::MAX_TYPE); i++) {
          if (0 == string_v.case_compare(CATALOG_TYPE_STR[i])) {
            type = static_cast<CatalogType>(i);
            is_type_set = true;
          }
        }
        if (CatalogType::INVALID_TYPE == type) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this catalog type");
          LOG_WARN("catalog type is not supported yet", K(ret), K(string_v));
        }
      }
    }
  }
  return ret;
}

int ObCatalogProperties::encrypt_str(ObString &src, ObString &dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
#if defined(OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty()) {
    dst = src;
  } else {
    char encrypted_string[OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH] = {0};
    char hex_buff[OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1] = {0};
    int64_t encrypt_len = -1;
    if (OB_FAIL(ObEncryptionUtil::encrypt_sys_data(tenant_id,
                                                   src.ptr(),
                                                   src.length(),
                                                   encrypted_string,
                                                   OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH,
                                                   encrypt_len))) {

      LOG_WARN("failed to encrypt_sys_data", KR(ret), K(src));
    } else if (0 >= encrypt_len || OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH < encrypt_len * 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypt_len is invalid", K(ret), K(encrypt_len),
               K(OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH));
    } else if (OB_FAIL(to_hex_cstr(encrypted_string,
                                   encrypt_len,
                                   hex_buff,
                                   OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1))) {
      LOG_WARN("failed to print to hex str", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, ObString(hex_buff), dst, true))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
#else
  dst = src;
#endif
  return ret;
}

int ObCatalogProperties::decrypt_str(ObString &src, ObString &dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
#if defined(OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty()) {
    dst = src;
  } else if (0 != src.length() % 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src", K(src.length()), K(ret));
  } else {
    char encrypted_unhex_str[OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH] = {0};
    char plain_string[OB_MAX_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH] = {0};
    int64_t plain_string_len = -1;
    if (OB_FAIL(hex_to_cstr(src.ptr(),
                            src.length(),
                            encrypted_unhex_str,
                            OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH))) {
      LOG_WARN("failed to hex to cstr", K(src.length()), K(ret));
    } else if (OB_FAIL(ObEncryptionUtil::decrypt_sys_data(tenant_id,
                                                          encrypted_unhex_str,
                                                          src.length() / 2,
                                                          plain_string,
                                                          OB_MAX_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH,
                                                          plain_string_len))) {
      LOG_WARN("failed to decrypt_sys_data", K(ret), K(src.length()));
    } else if (0 >= plain_string_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid string len", K(ret), K(plain_string_len));
    } else if (OB_FAIL(ob_write_string(allocator, ObString(plain_string_len, plain_string), dst))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
#else
  dst = src;
#endif
  return ret;
}

int ObODPSCatalogProperties::encrypt(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString encrypted_access_id;
  ObString encrypted_access_key;
  ObString encrypted_sts_token;
  if (OB_FAIL(encrypt_str(access_id_, encrypted_access_id, allocator))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(encrypt_str(access_key_, encrypted_access_key, allocator))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(encrypt_str(sts_token_, encrypted_sts_token, allocator))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else {
    access_id_ = encrypted_access_id;
    access_key_ = encrypted_access_key;
    sts_token_ = encrypted_sts_token;
  }
  return ret;
}

int ObODPSCatalogProperties::decrypt(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString decrypted_access_id;
  ObString decrypted_access_key;
  ObString decrypted_sts_token;
  if (OB_FAIL(decrypt_str(access_id_, decrypted_access_id, allocator))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(decrypt_str(access_key_, decrypted_access_key, allocator))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(decrypt_str(sts_token_, decrypted_sts_token, allocator))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else {
    access_id_ = decrypted_access_id;
    access_key_ = decrypted_access_key;
    sts_token_ = decrypted_sts_token;
  }
  return ret;
}

int ObODPSCatalogProperties::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSTYPE)],
                     to_cstring(ObHexStringWrap(access_type_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSID)],
                     to_cstring(ObHexStringWrap(access_id_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSKEY)],
                     to_cstring(ObHexStringWrap(access_key_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::STSTOKEN)],
                     to_cstring(ObHexStringWrap(sts_token_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ENDPOINT)],
                     to_cstring(ObHexStringWrap(endpoint_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::TUNNEL_ENDPOINT)],
                     to_cstring(ObHexStringWrap(tunnel_endpoint_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::PROJECT_NAME)],
                     to_cstring(ObHexStringWrap(project_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::QUOTA_NAME)],
                     to_cstring(ObHexStringWrap(quota_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::COMPRESSION_CODE)],
                     to_cstring(ObHexStringWrap(compression_code_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::REGION)],
                     to_cstring(ObHexStringWrap(region_))));

  return ret;
}

int ObODPSCatalogProperties::load_from_string(const ObString &str, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  json::Value *data = NULL;
  json::Parser parser;
  ObArenaAllocator temp_allocator;
  json::Pair *node = NULL;
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("format string is empty", K(ret), K(str));
  } else if (OB_FAIL(parser.init(&temp_allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(str.ptr(), str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(str));
  } else if (NULL == data || json::JT_OBJECT != data->get_type()
             || OB_ISNULL(node = data->get_object().get_first())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json value", K(ret), KPC(data));
  } else {
    node = node->get_next();
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSTYPE)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        access_type_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSID)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        access_id_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSKEY)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        access_key_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::STSTOKEN)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        sts_token_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ENDPOINT)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        endpoint_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::TUNNEL_ENDPOINT)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        tunnel_endpoint_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::PROJECT_NAME)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        project_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::QUOTA_NAME)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        quota_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::COMPRESSION_CODE)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        compression_code_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::REGION)])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        region_ = obj.get_string();
      }
      node = node->get_next();
    }
  }
  return ret;
}

int ObODPSCatalogProperties::resolve_catalog_properties(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  const ParseNode *child = NULL;
  const ParseNode *child_value = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
    if (OB_ISNULL(child = node.children_[i]) || child->num_child_ != 1
        || OB_ISNULL(child_value = child->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse node", K(ret));
    } else {
      switch (child->type_) {
        case T_EXTERNAL_FILE_FORMAT_TYPE: {
          // do nothing
          break;
        }
        case T_ACCESSTYPE: {
          access_type_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_ACCESSID: {
          access_id_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_ACCESSKEY: {
          access_key_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_STSTOKEN: {
          sts_token_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_ENDPOINT: {
          endpoint_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_TUNNEL_ENDPOINT: {
          tunnel_endpoint_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_PROJECT: {
          project_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_QUOTA: {
          quota_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_COMPRESSION: {
          compression_code_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_REGION: {
          region_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid catalog option", K(ret), K(child->type_));
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
