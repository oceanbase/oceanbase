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

#define USING_LOG_PREFIX SHARE

#include "share/config/ob_server_config.h"
#include "common/ob_record_header.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
namespace common {

ObConfigContainer*& ObInitConfigContainer::local_container()
{
  static RLOCAL(ObConfigContainer*, l_container);
  return l_container;
}

const ObConfigContainer& ObInitConfigContainer::get_container()
{
  return container_;
}

ObInitConfigContainer::ObInitConfigContainer()
{
  local_container() = &container_;
}

ObCommonConfig::ObCommonConfig()
{}

ObCommonConfig::~ObCommonConfig()
{}

int ObCommonConfig::add_extra_config(const char* config_str, int64_t version /* = 0 */, bool check_name /* = false */)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_OPTS_LENGTH = sysconf(_SC_ARG_MAX);
  int64_t config_str_length = 0;
  char* buf = NULL;
  char* saveptr = NULL;
  char* token = NULL;

  if (OB_ISNULL(config_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config str is null", K(ret));
  } else if ((config_str_length = static_cast<int64_t>(STRLEN(config_str))) >= MAX_OPTS_LENGTH) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("Extra config is too long", K(ret));
  } else if (OB_ISNULL(buf = new (std::nothrow) char[config_str_length + 1])) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("ob tc malloc memory for buf fail", K(ret));
  } else {
    MEMCPY(buf, config_str, config_str_length);
    buf[config_str_length] = '\0';
    token = STRTOK_R(buf, ",\n", &saveptr);
  }
  char external_info_val[OB_MAX_CONFIG_VALUE_LEN];
  external_info_val[0] = '\0';
  const ObString external_kms_info_cfg(EXTERNAL_KMS_INFO);
  const ObString ssl_external_kms_info_cfg(SSL_EXTERNAL_KMS_INFO);
  while (OB_SUCC(ret) && OB_LIKELY(NULL != token)) {
    char* saveptr_one = NULL;
    const char* name = NULL;
    const char* value = NULL;
    ObConfigItem* const* pp_item = NULL;
    if (OB_ISNULL(name = STRTOK_R(token, "=", &saveptr_one))) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("Invalid config string", K(token), K(ret));
    } else if (OB_ISNULL(saveptr_one) || OB_UNLIKELY('\0' == *(value = saveptr_one))) {
      LOG_INFO("Empty config string", K(token), K(name));
      // ret = OB_INVALID_CONFIG;
      name = "";
    } else if (OB_ISNULL(pp_item = container_.get(ObConfigStringKey(name)))) {
      /* make compatible with previous configuration */
      ret = check_name ? OB_INVALID_CONFIG : OB_SUCCESS;
      LOG_WARN("Invalid config string, no such config item", K(name), K(value), K(ret));
    } else if (external_kms_info_cfg.case_compare(name) == 0 || ssl_external_kms_info_cfg.case_compare(name) == 0) {
      if (OB_FAIL(common::hex_to_cstr(value, strlen(value), external_info_val, OB_MAX_CONFIG_VALUE_LEN))) {
        LOG_WARN("fail to hex to cstr", K(ret));
      } else {
        value = external_info_val;
      }
    }
    if (OB_FAIL(ret) || OB_ISNULL(pp_item)) {
    } else if (!(*pp_item)->set_value(value)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("Invalid config value", K(name), K(value), K(ret));
    } else if (!(*pp_item)->check()) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("Invalid config, value out of range", K(name), K(value), K(ret));
    } else {
      (*pp_item)->set_version(version);
      LOG_INFO("Load config succ", K(name), K(value));
    }
    token = STRTOK_R(NULL, ",\n", &saveptr);
  }

  if (NULL != buf) {
    delete[] buf;
    buf = NULL;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObCommonConfig)
{
  int ret = OB_SUCCESS;
  int64_t expect_data_len = get_serialize_size_();
  int64_t saved_pos = pos;
  ObConfigContainer::const_iterator it = container_.begin();
  const ObString external_kms_info_cfg(EXTERNAL_KMS_INFO);
  const ObString ssl_external_kms_info_cfg(SSL_EXTERNAL_KMS_INFO);
  char external_info_val[OB_MAX_CONFIG_VALUE_LEN];
  external_info_val[0] = '\0';
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (it->second->value_updated()) {
      if (external_kms_info_cfg.case_compare(it->first.str()) == 0 ||
          ssl_external_kms_info_cfg.case_compare(it->first.str()) == 0) {
        int64_t hex_pos = 0, hex_c_str_pos = 0;
        if (OB_FAIL(common::to_hex_cstr((void*)it->second->spfile_str(),
                strlen(it->second->spfile_str()),
                external_info_val,
                OB_MAX_CONFIG_VALUE_LEN,
                hex_pos,
                hex_c_str_pos))) {
          LOG_WARN("fail to convert hex str", K(ret));
        } else {
          ret = databuff_printf(buf, buf_len, pos, "%s=%s\n", it->first.str(), external_info_val);
        }
      } else {
        ret = databuff_printf(buf, buf_len, pos, "%s=%s\n", it->first.str(), it->second->spfile_str());
      }
    }
  }  // for
  if (OB_SUCC(ret)) {
    int64_t writen_len = pos - saved_pos;
    if (writen_len != expect_data_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected data size", K(writen_len), K(expect_data_len));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObCommonConfig)
{
  int ret = OB_SUCCESS;
  if (data_len == 0 || pos >= data_len) {
  } else {
    int64_t config_str_length = 0;
    char* copy_buf = nullptr;
    if (OB_ISNULL(buf + pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config str is null", K(ret));
    } else if (OB_ISNULL(copy_buf = new (std::nothrow) char[data_len + 1])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("ob tc malloc memory for buf fail", K(ret));
    } else {
      MEMSET(copy_buf, '\0', config_str_length + 1);
      MEMCPY(copy_buf, buf + pos, data_len);
      if (OB_FAIL(add_extra_config(copy_buf))) {
        LOG_ERROR("Read server config failed", K(ret));
      }

      if (nullptr != copy_buf) {
        delete[] copy_buf;
        copy_buf = NULL;
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCommonConfig)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  char kv_str[OB_MAX_CONFIG_NAME_LEN + OB_MAX_CONFIG_VALUE_LEN + 1];
  ObConfigContainer::const_iterator it = container_.begin();
  const ObString external_kms_info_cfg(EXTERNAL_KMS_INFO);
  const ObString ssl_external_kms_info_cfg(SSL_EXTERNAL_KMS_INFO);
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    MEMSET(kv_str, '\0', OB_MAX_CONFIG_NAME_LEN + OB_MAX_CONFIG_VALUE_LEN + 1);
    int64_t pos = 0;
    if (OB_NOT_NULL(it->second) && it->second->value_updated()) {
      if (OB_FAIL(databuff_printf(kv_str,
              OB_MAX_CONFIG_NAME_LEN + OB_MAX_CONFIG_VALUE_LEN,
              pos,
              "%s=%s\n",
              it->first.str(),
              it->second->spfile_str()))) {
        LOG_WARN("write data buff failed", K(ret));
      } else {
        len += pos;
        if (external_kms_info_cfg.case_compare(it->first.str()) == 0 ||
            ssl_external_kms_info_cfg.case_compare(it->first.str()) == 0) {
          len += strlen(it->second->spfile_str());
        }
      }
    }
  }  // for
  return len;
}

}  // end of namespace common
}  // end of namespace oceanbase
