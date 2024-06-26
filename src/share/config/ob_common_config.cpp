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
#include "lib/container/ob_array_iterator.h"
#include "lib/utility/ob_defer.h"
#include "common/ob_record_header.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace common
{

ObConfigContainer *&ObInitConfigContainer::local_container()
{
  RLOCAL(ObConfigContainer*, l_container);
  return l_container;
}

const ObConfigContainer &ObInitConfigContainer::get_container()
{
  return container_;
}

int ObBaseConfig::init()
{
  int ret = OB_SUCCESS;
  const int64_t buf_len= OB_MAX_CONFIG_LENGTH;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else {
    inited_ = true;
  }
  return ret;
}

void ObBaseConfig::destroy()
{
  inited_ = false;
}

int ObBaseConfig::check_all()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("Config has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    for (auto it = container_.begin(); OB_SUCC(ret) && it != container_.end(); it++) {
      if (OB_ISNULL(it->second)) {
        LOG_ERROR("config item const_iterator second element is NULL",
            "first_item", it->first.str());
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(it->second->str())) {
        LOG_ERROR("config item string value is NULL",
            "first_item", it->first.str());
        ret = OB_ERR_UNEXPECTED;
      } else if (! it->second->check()) {
        _LOG_ERROR("invalid config, name: [%s], value: [%s]", it->first.str(), it->second->str());
        ret = OB_INVALID_CONFIG;
      } else if (0 == strlen(it->second->str())) {
        // All configuration items are not allowed to be empty
        _LOG_ERROR("invalid empty config, name: [%s], value: [%s]",
            it->first.str(), it->second->str());
        ret = OB_INVALID_CONFIG;
      } else {
        // normal
      }
    }
  }

  return ret;
}
void ObBaseConfig::get_sorted_config_items(ConfigItemArray &configs) const
{
  // Transfer the configuration items to an array and sort the output
  for (auto it = container_.begin(); it != container_.end(); ++it) {
    ConfigItem item(it->first.str(), NULL == it->second ? "" : it->second->str());
    (void)configs.push_back(item);
  }
  lib::ob_sort(configs.begin(), configs.end());
}
int ObBaseConfig::load_from_buffer(const char *config_str, const int64_t config_str_len,
                              const int64_t version, const bool check_name)
{
  int ret = OB_SUCCESS;
  char *saveptr = NULL;
  char *token = NULL;
  int64_t pos =0;
  char *config_file_buf = NULL;
  const int64_t buf_len= OB_MAX_CONFIG_LENGTH;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("Config has not been initialized");
    ret = OB_NOT_INIT;
  } else if (NULL == config_str || config_str_len <= 0) {
    LOG_ERROR("invalid argument", K(config_str), K(config_str_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(config_file_buf = static_cast<char *>(ob_malloc(buf_len, "ConfigBuf")))) {
    LOG_ERROR("allocate memory for buffer fail", K(config_file_buf), K(buf_len));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    config_file_buf[0] = '\0';
    const int64_t buf_size = OB_MAX_CONFIG_LENGTH;

    if (config_str_len > (buf_size - 1)) {
      LOG_ERROR("extra config is too long!", K(config_str_len), K(buf_size));
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(databuff_printf(config_file_buf, buf_size, pos, "%.*s",
            static_cast<int>(config_str_len), config_str))) {
      LOG_ERROR("copy config string fail", KR(ret), K(config_file_buf), K(buf_size), K(pos), K(config_str_len),
          K(config_str));
    } else {
      token = strtok_r(config_file_buf, ",\n", &saveptr);
      while (NULL != token && OB_SUCCESS == ret) {
        char *saveptr_one = NULL;
        const char *name = NULL;
        const char *value = NULL;
        ObConfigItem *const *pp_item = NULL;
        if (NULL == (name = strtok_r(token, "=", &saveptr_one))) {
          LOG_ERROR("fail to parse config string, can not find '=' from token",
              K(token), K(config_str));
          ret = OB_INVALID_CONFIG;
        } else if ('\0' == *(value = saveptr_one)) {
          _LOG_WARN("empty config string: [%s]", token);
          name = "";
        } else if (NULL == (pp_item = container_.get(ObConfigStringKey(name)))) {
          if (check_name) {
            _LOG_WARN("invalid config string, unknown config item! name: [%s] value: [%s]",
                name, value);
            ret = OB_INVALID_ARGUMENT;
          }
        } else {
          (*pp_item)->set_value(value);
          (*pp_item)->set_version(version);
          if (need_print_config(name)) {
            _LOG_INFO("load config succ, %s=%s", name, value);
          }
        }

        if (OB_SUCCESS == ret) {
          token = strtok_r(NULL, ",\n", &saveptr);
        }
      }
    }
  }

  if (OB_LIKELY(NULL != config_file_buf)) {
    ob_free(config_file_buf);
    config_file_buf = NULL;
  }

  return ret;
}

int ObBaseConfig::load_from_file(const char *config_file,
    const int64_t version /* = 0 */,
    const bool check_name /* = false */)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  char *config_file_buf = NULL;
  const int64_t buf_len= OB_MAX_CONFIG_LENGTH;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogConfig has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(config_file)) {
    LOG_ERROR("invalid argument", K(config_file));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(config_file_buf = static_cast<char *>(ob_malloc(buf_len, "ConfigBuf")))) {
    LOG_ERROR("allocate memory for buffer fail", K(config_file_buf), K(buf_len));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (NULL == (fp = fopen(config_file, "rb"))) {
    ret = OB_IO_ERROR;
    LOG_ERROR("can't open file", K(config_file), KR(ret), KERRNOMSG(errno));
  } else {
    config_file_buf[0] = '\0';
    int64_t buffer_size = OB_MAX_CONFIG_LENGTH;
    int64_t read_len = fread(config_file_buf, 1, buffer_size - 1, fp);

    if (0 != ferror(fp)) {
      ret = OB_IO_ERROR;
      LOG_ERROR("read config file error!", K(config_file), KERRNOMSG(errno));
    } else if (0 == feof(fp)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("config file is too long!", K(config_file), K(buffer_size));
    } else if (read_len <= 0) {
      LOG_WARN("config file is empty", K(config_file));
    } else if (read_len >= buffer_size) {
      LOG_ERROR("fread buffer overflow", K(read_len), K(buffer_size));
      ret = OB_SIZE_OVERFLOW;
    } else {
      // end with '\0'
      config_file_buf[read_len] = '\0';

      if (OB_FAIL(load_from_buffer(config_file_buf, read_len, version, check_name))) {
        LOG_ERROR("load config fail", KR(ret), K(config_file), K(version), K(check_name),
            K(read_len));
      } else {
        LOG_INFO("load config from file succ", K(config_file));
      }
    }
  }

  if (NULL != fp) {
    fclose(fp);
    fp = NULL;
  }

  if (OB_LIKELY(NULL != config_file_buf)) {
    ob_free(config_file_buf);
    config_file_buf = NULL;
  }

  return ret;
}

int ObBaseConfig::dump2file(const char *file) const
{
  int ret = OB_SUCCESS;
  char tmp_file[MAX_PATH_SIZE] = "";
  if (OB_ISNULL(file)) {
    LOG_ERROR("invalid argument", K(file));
    ret = OB_INVALID_ARGUMENT;
  } else {
    snprintf(tmp_file, MAX_PATH_SIZE, "%s.tmp", file);
    FILE *fp = NULL;
    ConfigItemArray configs;
    get_sorted_config_items(configs);
    if (NULL == (fp = fopen(tmp_file, "w+"))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("open file fail", K(file), KERRMSG);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < configs.count(); ++i) {
        const ConfigItem &item = configs.at(i);
        int write_len = fprintf(fp, "%s=%s\n", item.key_.c_str(), item.val_.c_str());
        if (write_len <= 0) {
          ret = OB_IO_ERROR;
          LOG_WARN("write config file fail",
              K(write_len), "config: name", item.key_.c_str(), "value", item.val_.c_str());
        }
      }
    }
    if (NULL != fp) {
      fclose(fp);
      fp = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    // copy tmp_file to file by user when arbserver exit
    if (0 != ::rename(tmp_file, file)) {
      ret = OB_ERR_SYS;
      LOG_WARN("fail to move tmp config file", KERRMSG, K(ret));
    }
  }
  return ret;
}


ObInitConfigContainer::ObInitConfigContainer()
{
  local_container() = &container_;
}

ObCommonConfig::ObCommonConfig()
{
}

ObCommonConfig::~ObCommonConfig()
{
}

int ObCommonConfig::add_extra_config(const char *config_str,
                                     int64_t version /* = 0 */ ,
                                     bool check_config /* = true */)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_OPTS_LENGTH = sysconf(_SC_ARG_MAX);
  int64_t config_str_length = 0;
  char *buf = NULL;
  char *saveptr = NULL;
  char *token = NULL;
  bool split_by_comma = false;

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
    token = STRTOK_R(buf, "\n", &saveptr);
    if (0 == STRLEN(saveptr)) {
      token = STRTOK_R(buf, ",\n", &saveptr);
      split_by_comma = true;
    }
    const ObString external_kms_info_cfg(EXTERNAL_KMS_INFO);
    const ObString ssl_external_kms_info_cfg(SSL_EXTERNAL_KMS_INFO);
    const ObString compatible_cfg(COMPATIBLE);
    const ObString enable_compatible_monotonic_cfg(ENABLE_COMPATIBLE_MONOTONIC);
    auto func = [&]() {
      char *saveptr_one = NULL;
      const char *name = NULL;
      const char *value = NULL;
      ObConfigItem *const *pp_item = NULL;
      if (OB_ISNULL(name = STRTOK_R(token, "=", &saveptr_one))) {
        ret = OB_INVALID_CONFIG;
        LOG_ERROR("Invalid config string", K(token), K(ret));
      } else if (OB_ISNULL(saveptr_one) || OB_UNLIKELY('\0' == *(value = saveptr_one))) {
        LOG_INFO("Empty config string", K(token), K(name));
        // ret = OB_INVALID_CONFIG;
        name = "";
      }
      if (OB_SUCC(ret)) {
        const int value_len = static_cast<int>(strlen(value));
        // hex2cstring -> value_len / 2 + 1
        // '\0' -> 1
        const int external_info_val_len = value_len / 2 + 1 + 1;
        char *external_info_val = (char*)ob_malloc(external_info_val_len, "temp");
        DEFER(if (external_info_val != nullptr) ob_free(external_info_val););
        if (OB_ISNULL(external_info_val)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to alloc", K(ret));
        } else if (FALSE_IT(external_info_val[0] = '\0')) {
        } else if (OB_ISNULL(pp_item = container_.get(ObConfigStringKey(name)))) {
          ret = OB_SUCCESS;
          LOG_WARN("Invalid config string, no such config item", K(name), K(value), K(ret));
        } else if (external_kms_info_cfg.case_compare(name) == 0
                   || ssl_external_kms_info_cfg.case_compare(name) == 0) {
          if (OB_FAIL(common::hex_to_cstr(value, value_len,
              external_info_val, external_info_val_len))) {
            LOG_ERROR("fail to hex to cstr", K(ret));
          } else {
            value = external_info_val;
          }
        }
        if (OB_FAIL(ret) || OB_ISNULL(pp_item)) {
        } else if (!(*pp_item)->set_value(value)) {
          ret = OB_INVALID_CONFIG;
          LOG_ERROR("Invalid config value", K(name), K(value), K(ret));
        } else if (check_config && (!(*pp_item)->check_unit(value) || !(*pp_item)->check())) {
          ret = OB_INVALID_CONFIG;
          const char* range = (*pp_item)->range();
          if (OB_ISNULL(range) || strlen(range) == 0) {
            LOG_ERROR("Invalid config, value out of range", K(name), K(value), K(ret));
          } else {
            _LOG_ERROR("Invalid config, value out of %s (for reference only). name=%s, value=%s, ret=%d", range, name, value, ret);
          }
        } else {
          (*pp_item)->set_version(version);
          LOG_INFO("Load config succ", K(name), K(value));
          if (0 == compatible_cfg.case_compare(name)) {
            const uint64_t tenant_id = get_tenant_id();
            uint64_t data_version = 0;
            int tmp_ret = 0;
            if (OB_TMP_FAIL(ObClusterVersion::get_version(value, data_version))) {
              LOG_ERROR("parse data_version failed", KR(tmp_ret), K(value));
            } else if (OB_TMP_FAIL(ODV_MGR.set(tenant_id, data_version))) {
              LOG_WARN("fail to set data_version", KR(tmp_ret), K(tenant_id), K(data_version));
            }
            FLOG_INFO("[COMPATIBLE] [DATA_VERSION] load data_version from config file",
                      KR(ret), "tenant_id", get_tenant_id(),
                      "version", (*pp_item)->version(),
                      "value", (*pp_item)->str(),
                      "value_updated", (*pp_item)->value_updated(),
                      "dump_version", (*pp_item)->dumped_version(),
                      "dump_value", (*pp_item)->spfile_str(),
                      "dump_value_updated", (*pp_item)->dump_value_updated());
          } else if (0 == enable_compatible_monotonic_cfg.case_compare(name)) {
            ObString v_str((*pp_item)->str());
            ODV_MGR.set_enable_compatible_monotonic(0 == v_str.case_compare("True") ? true : false);
          }
        }
      }
    };
    // init enable_production_mode at first
    while (OB_SUCC(ret) && OB_NOT_NULL(token)) {
      if (strncmp(token, "enable_production_mode=", 23) == 0) {
        func();
        break;
      }
      token = (true == split_by_comma) ? STRTOK_R(NULL, ",\n", &saveptr) : STRTOK_R(NULL, "\n", &saveptr);
    }
    // reset
    MEMCPY(buf, config_str, config_str_length);
    buf[config_str_length] = '\0';
    saveptr = nullptr;
    token = STRTOK_R(buf, "\n", &saveptr);
    if (0 == STRLEN(saveptr)) {
      token = STRTOK_R(buf, ",\n", &saveptr);
      split_by_comma = true;
    }
    while (OB_SUCC(ret) && OB_NOT_NULL(token)) {
      if (strncmp(token, "enable_production_mode:", 23) != 0) {
        func();
      }
      token = (true == split_by_comma) ? STRTOK_R(NULL, ",\n", &saveptr) : STRTOK_R(NULL, "\n", &saveptr);
    }
  }

  if (NULL != buf) {
    delete [] buf;
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
  const ObString compatible_cfg(COMPATIBLE);
  HEAP_VAR(char[OB_MAX_CONFIG_VALUE_LEN], external_info_val) {
    external_info_val[0] = '\0';
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (it->second->value_updated()
                 || it->second->dump_value_updated()) {
        if (external_kms_info_cfg.case_compare(it->first.str()) == 0
            || ssl_external_kms_info_cfg.case_compare(it->first.str()) == 0) {
          int64_t hex_pos = 0, hex_c_str_pos = 0;
          if (OB_FAIL(common::to_hex_cstr((void *)it->second->spfile_str(),
              strlen(it->second->spfile_str()),
              external_info_val, OB_MAX_CONFIG_VALUE_LEN, hex_pos, hex_c_str_pos))) {
            LOG_WARN("fail to convert hex str", K(ret));
          } else {
            ret = databuff_printf(buf, buf_len, pos, "%s=%s\n",
                it->first.str(), external_info_val);
          }
        } else {
          ret = databuff_printf(buf, buf_len, pos, "%s=%s\n",
              it->first.str(), it->second->spfile_str());
        }
        if (OB_SUCC(ret) && 0 == compatible_cfg.case_compare(it->first.str())) {
          FLOG_INFO("[COMPATIBLE] [DATA_VERSION] dump data_version",
                    KR(ret), "tenant_id", get_tenant_id(),
                    "version", it->second->version(),
                    "value", it->second->str(),
                    "value_updated", it->second->value_updated(),
                    "dump_version", it->second->dumped_version(),
                    "dump_value", it->second->spfile_str(),
                    "dump_value_updated", it->second->dump_value_updated());
        }
      }
    } // for
  }
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
    char *copy_buf = nullptr;
    if (OB_ISNULL(buf + pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config str is null", K(ret));
    } else if (OB_ISNULL(copy_buf = new (std::nothrow) char[data_len + 1])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("ob tc malloc memory for buf fail", K(ret));
    } else {
      MEMSET(copy_buf, '\0', data_len + 1);
      MEMCPY(copy_buf, buf + pos, data_len);
      if (OB_FAIL(ObCommonConfig::add_extra_config(copy_buf, 0, false))) {
        LOG_ERROR("Read server config failed", K(ret));
      }

      if (nullptr != copy_buf) {
        delete [] copy_buf;
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
  HEAP_VAR(char[OB_MAX_CONFIG_NAME_LEN + OB_MAX_CONFIG_VALUE_LEN + 1], kv_str) {
    ObConfigContainer::const_iterator it = container_.begin();
    const ObString external_kms_info_cfg(EXTERNAL_KMS_INFO);
    const ObString ssl_external_kms_info_cfg(SSL_EXTERNAL_KMS_INFO);
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      MEMSET(kv_str, '\0', OB_MAX_CONFIG_NAME_LEN + OB_MAX_CONFIG_VALUE_LEN + 1);
      int64_t pos = 0;
      if (OB_NOT_NULL(it->second)
          && (it->second->value_updated()
              || it->second->dump_value_updated())) {
        if (OB_FAIL(databuff_printf(kv_str, OB_MAX_CONFIG_NAME_LEN + OB_MAX_CONFIG_VALUE_LEN,
                        pos, "%s=%s\n", it->first.str(), it->second->spfile_str()))) {
          LOG_WARN("write data buff failed", K(ret));
        } else {
          len += pos;
          if (external_kms_info_cfg.case_compare(it->first.str()) == 0
              || ssl_external_kms_info_cfg.case_compare(it->first.str()) == 0) {
            len += strlen(it->second->spfile_str());
          }
        }
      }
    } // for
  }
  return len;
}

} // end of namespace common
} // end of namespace oceanbase
