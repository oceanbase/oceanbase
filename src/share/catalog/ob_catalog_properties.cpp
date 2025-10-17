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
#include "src/sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace share
{
const char *ObCatalogProperties::CATALOG_TYPE_STR[] = {
    "ODPS",
    "FILESYSTEM",
    "HMS",
};
static_assert(array_elements(ObCatalogProperties::CATALOG_TYPE_STR) == static_cast<size_t>(ObCatalogProperties::CatalogType::MAX_TYPE), "Not enough initializer for ObCatalogProperties");
static_assert(array_elements(ObODPSCatalogProperties::OPTION_NAMES) == static_cast<size_t>(ObODPSCatalogProperties::ObOdpsCatalogOptions::MAX_OPTIONS), "Not enough initializer for ObODPSCatalogProperties");
static_assert(array_elements(ObFilesystemCatalogProperties::OPTION_NAMES) == static_cast<size_t>(ObFilesystemCatalogProperties::ObFilesystemCatalogOptions::MAX_OPTIONS), "Not enough initializer for ObFilesystemCatalogProperties");

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
    ObArenaAllocator tmp_allocator; // avoid big stack
    int64_t encrypt_len = -1;
    char *encrypted_string = static_cast<char *>(
        tmp_allocator.alloc(OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH));
    char *hex_buff = static_cast<char *>(
        tmp_allocator.alloc(OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1));
    if (OB_ISNULL(encrypted_string) || OB_ISNULL(hex_buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FALSE_IT(memset(encrypted_string,
                                  0,
                                  OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH))) {
    } else if (OB_FALSE_IT(memset(hex_buff,
                                  0,
                                  OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1))) {
    } else if (OB_FAIL(ObEncryptionUtil::encrypt_sys_data(
                   tenant_id,
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

/*---------------------Start of ObODPSCatalogProperties----------------------*/
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
    LOG_WARN("failed to decrypt", K(ret));
  } else if (OB_FAIL(decrypt_str(access_key_, decrypted_access_key, allocator))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else if (OB_FAIL(decrypt_str(sts_token_, decrypted_sts_token, allocator))) {
    LOG_WARN("failed to decrypt", K(ret));
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
  ObCStringHelper helper;
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSTYPE)],
                     helper.convert(ObHexStringWrap(access_type_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSID)],
                     helper.convert(ObHexStringWrap(access_id_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ACCESSKEY)],
                     helper.convert(ObHexStringWrap(access_key_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::STSTOKEN)],
                     helper.convert(ObHexStringWrap(sts_token_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::ENDPOINT)],
                     helper.convert(ObHexStringWrap(endpoint_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::TUNNEL_ENDPOINT)],
                     helper.convert(ObHexStringWrap(tunnel_endpoint_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::PROJECT_NAME)],
                     helper.convert(ObHexStringWrap(project_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::QUOTA_NAME)],
                     helper.convert(ObHexStringWrap(quota_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::COMPRESSION_CODE)],
                     helper.convert(ObHexStringWrap(compression_code_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::REGION)],
                     helper.convert(ObHexStringWrap(region_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":%d)",
                     OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::API_MODE)],
                     static_cast<int>(api_mode_)));
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
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObOdpsCatalogOptions::API_MODE)])
        && json::JT_NUMBER == node->value_->get_type()) {
      api_mode_ = static_cast<ObODPSGeneralFormat::ApiMode>(node->value_->get_number());
      if (api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API
        && api_mode_ != ObODPSGeneralFormat::ApiMode::BYTE
        && api_mode_ != ObODPSGeneralFormat::ApiMode::ROW) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid split mode", K(ret), K(api_mode_));
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
  ObResolverUtils::FileFormatContext ff_ctx;
  api_mode_ = sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API;
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
        case ObItemType::T_TABLE_MODE:
        case ObItemType::T_SPLIT_ACTION: {
          if (child->type_ == T_TABLE_MODE) {
            ObString temp_table_mode = ObString(child->children_[0]->str_len_, child->children_[0]->str_value_).trim_space_only();
            if (temp_table_mode.empty()) {
              // do nothing
            } else if (0 == temp_table_mode.case_compare(ObODPSGeneralFormatParam::TUNNEL_API)) {
              if (api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("already set the the storage api", K(ret));
              } else {
                ff_ctx.is_tunnel_set = true;
              }
            } else if (!GCONF._use_odps_jni_connector) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("must use storage api in jni connector", K(ret));
            } else if (0 == temp_table_mode.case_compare(ObODPSGeneralFormatParam::STORAGE_API)) {
              if (api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
                // do nothing keep row or byte
              } else {
                api_mode_ = ObODPSGeneralFormat::ApiMode::ROW;
              }
            } else if (0 == temp_table_mode.case_compare(ObODPSGeneralFormatParam::ROW)) {
              if (api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("already set the the storage api", K(ret));
              } else {
                api_mode_ = ObODPSGeneralFormat::ApiMode::ROW;
                ff_ctx.is_tunnel_set = true;
              }
            } else if (0 == temp_table_mode.case_compare(ObODPSGeneralFormatParam::BYTE)) {
              if (api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("already set the the storage api", K(ret));
              } else {
                api_mode_ = ObODPSGeneralFormat::ApiMode::BYTE;
                ff_ctx.is_tunnel_set = true;
              }
            } else {
              ret = OB_INVALID_ARGUMENT;
            }
          } else if (child->type_ == T_SPLIT_ACTION) {
            if (!GCONF._use_odps_jni_connector) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("jni was not allow to use", K(ret));
            } else if (ff_ctx.is_tunnel_set) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("already use tunnel api", K(ret));
            } else {
              ObString temp_split_mode = ObString(child->children_[0]->str_len_, child->children_[0]->str_value_).trim_space_only();
              if (temp_split_mode.empty()) {
                // do nothing
              } else if (0 == temp_split_mode.case_compare(ObODPSGeneralFormatParam::BYTE)) {
                api_mode_ = ObODPSGeneralFormat::ApiMode::BYTE;
              } else if (0 == temp_split_mode.case_compare(ObODPSGeneralFormatParam::ROW)) {
                api_mode_ = ObODPSGeneralFormat::ApiMode::ROW;
              } else {
                ret = OB_INVALID_ARGUMENT;
              }
            }
          }
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

int ObFilesystemCatalogProperties::to_json_kv_string(char *buf,
                                                     const int64_t buf_len,
                                                     int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObCStringHelper helper;
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[static_cast<size_t>(ObFilesystemCatalogOptions::WAREHOUSE)],
                     helper.convert(ObHexStringWrap(warehouse_))));
  return ret;
}

int ObFilesystemCatalogProperties::load_from_string(const common::ObString &str,
                                                    common::ObIAllocator &allocator)
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
    DLIST_FOREACH_X(it, data->get_object(), OB_SUCC(ret))
    {
      if (0 == it->name_.case_compare(OPTION_NAMES[static_cast<size_t>(ObFilesystemCatalogOptions::WAREHOUSE)])
          && json::JT_STRING == it->value_->get_type()) {
        ObObj obj;
        OZ(ObHexUtilsBase::unhex(it->value_->get_string(), allocator, obj));
        if (OB_SUCC(ret) && !obj.is_null()) {
          warehouse_ = obj.get_string();
        }
      }

    }
  }
  return ret;
}

int ObFilesystemCatalogProperties::resolve_catalog_properties(const ParseNode &node)
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
        case T_WAREHOUSE: {
          warehouse_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid catalog option", K(ret), K(child->type_));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (warehouse_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "can't use empty warehouse location");
    }
  }

  return ret;
}

int ObFilesystemCatalogProperties::encrypt(ObIAllocator &allocator)
{
  return OB_SUCCESS;
}

int ObFilesystemCatalogProperties::decrypt(ObIAllocator &allocator)
{
  return OB_SUCCESS;
}

/*---------------------End of ObODPSCatalogProperties----------------------*/
/*---------------------Start of ObHMSCatalogProperties----------------------*/
int ObHMSCatalogProperties::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObCStringHelper helper;
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[URI],
                     helper.convert(ObHexStringWrap(uri_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[PRINCIPAL],
                     helper.convert(ObHexStringWrap(principal_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[KEYTAB],
                     helper.convert(ObHexStringWrap(keytab_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":"%s")",
                     OPTION_NAMES[KRB5CONF],
                     helper.convert(ObHexStringWrap(krb5conf_))));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":%ld)",
                     OPTION_NAMES[MAX_CLIENT_POOL_SIZE],
                     max_client_pool_size_));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":%ld)",
                     OPTION_NAMES[SOCKET_TIMEOUT],
                     socket_timeout_));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("%s":%ld)",
                     OPTION_NAMES[CACHE_REFRESH_INTERVAL_SEC],
                     cache_refresh_interval_sec_));
  return ret;
}

int ObHMSCatalogProperties::load_from_string(const ObString &str, ObIAllocator &allocator)
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
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[URI])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        uri_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[PRINCIPAL])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        principal_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[KEYTAB])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        keytab_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[KRB5CONF])
        && json::JT_STRING == node->value_->get_type()) {
      ObObj obj;
      OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
      if (OB_SUCC(ret) && !obj.is_null()) {
        krb5conf_ = obj.get_string();
      }
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[MAX_CLIENT_POOL_SIZE])
        && json::JT_NUMBER == node->value_->get_type()) {
      max_client_pool_size_ = node->value_->get_number();
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[SOCKET_TIMEOUT])
        && json::JT_NUMBER == node->value_->get_type()) {
      socket_timeout_ = node->value_->get_number();
      node = node->get_next();
    }
    if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[CACHE_REFRESH_INTERVAL_SEC])
        && json::JT_NUMBER == node->value_->get_type()) {
      cache_refresh_interval_sec_ = node->value_->get_number();
      node = node->get_next();
    }
  }
  return ret;
}

int ObHMSCatalogProperties::resolve_catalog_properties(const ParseNode &node)
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
        case T_URI: {
          uri_
              = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_PRINCIPAL: {
          principal_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_KEYTAB: {
          keytab_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_KRB5CONF: {
          krb5conf_ = ObString(child_value->str_len_, child_value->str_value_).trim_space_only();
          break;
        }
        case T_MAX_CLIENT_POOL_SIZE: {
          max_client_pool_size_ = child_value->value_;
          break;
        }
        case T_SOCKET_TIMEOUT: {
          socket_timeout_ = child_value->value_;
          break;
        }
        case T_CACHE_REFRESH_INTERVAL_SEC: {
          cache_refresh_interval_sec_ = child_value->value_;
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid catalog option", K(ret), K(child->type_));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (uri_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "hms catalog's uri can't be empty");
    }
  }
  return ret;
}

int ObHMSCatalogProperties::encrypt(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObHMSCatalogProperties::decrypt(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  return ret;
}

bool ObHMSCatalogProperties::is_set_cache_refresh_interval_sec() const
{
  return cache_refresh_interval_sec_ != INVALID_CACHE_REFRESH_INTERVAL_SEC;
}

int64_t ObHMSCatalogProperties::get_cache_refresh_interval_sec() const
{
  int64_t sec = 0;
  if (cache_refresh_interval_sec_ != INVALID_CACHE_REFRESH_INTERVAL_SEC) {
    sec = cache_refresh_interval_sec_;
  } else {
    sec = DEFAULT_CACHE_REFRESH_INTERVAL_SEC;
  }
  return sec;
}

/*---------------------End of ObHMSCatalogProperties----------------------*/
}
}
