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
 *
 * config module
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_config.h"

#include "lib/container/ob_array.h"             // ObArray
#include "lib/container/ob_array_iterator.h"    // ObArray::begin
#include "lib/allocator/ob_malloc.h"            // ob_malloc/ob_free

#include "ob_log_utils.h"                       // TS_TO_STR, get_timestamp

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
ObLogConfig &ObLogConfig::get_instance()
{
  static ObLogConfig config;
  return config;
}

int ObLogConfig::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(ObBaseConfig::init())){
    LOG_ERROR("init ObBaseConfig failed", KR(ret));
  } else {
    inited_ = true;
  }

  return ret;
}

void ObLogConfig::destroy()
{
  ObBaseConfig::destroy();
  if (nullptr != tb_white_list_buf_) {
    ob_free(tb_white_list_buf_);
    tb_white_list_buf_ = nullptr;
  }
  if (nullptr != tb_black_list_buf_) {
    ob_free(tb_black_list_buf_);
    tb_black_list_buf_ = nullptr;
  }
  inited_ = false;
}

// Remove the quotes from the URL
int ObLogConfig::format_cluster_url()
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_CLUSTER_URL_LEN = 1024;
  char cluster_url_buffer[MAX_CLUSTER_URL_LEN] = {0};

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogConfig has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(cluster_url.copy(cluster_url_buffer, MAX_CLUSTER_URL_LEN))) {
    LOG_ERROR("copy cluster_url fail", KR(ret), K(cluster_url.str()));
  } else if (strlen(cluster_url_buffer) <= 0) {
    LOG_ERROR("invalid config, cluster_url is empty", K(cluster_url.str()));
    ret = OB_INVALID_CONFIG;
  } else {
    int64_t orig_len = strlen(cluster_url_buffer);
    char *start_ptr = cluster_url_buffer;
    char *end_ptr = cluster_url_buffer + strlen(cluster_url_buffer) - 1;

    // remove quotes
    if ('\"' == *start_ptr) {
      start_ptr++;
    }

    if (end_ptr >= start_ptr && '\"' == *end_ptr) {
      *end_ptr = '\0';
      end_ptr--;
    }

    if (end_ptr < start_ptr) {
      LOG_ERROR("cluster_url is empty after formatting", "cluster_url", cluster_url.str());
      ret = OB_INVALID_CONFIG;
    } else if ((end_ptr - start_ptr + 1) < orig_len) {
      _LOG_INFO("format cluster_url from [%s] to [%s]", cluster_url.str(), start_ptr);

      if (! cluster_url.set_value(start_ptr)) {
        LOG_ERROR("cluster_url set_value fail", "cluster_url", start_ptr,
            "length", end_ptr - start_ptr + 1);
      }
    } else {}
  }

  return ret;
}

bool ObLogConfig::need_print_config(const std::string& config_key) const
{
  bool need_print = true;

  if ((0 == config_key.compare("cluster_password"))
    || (0 == config_key.compare("tenant_password"))
    || (0 == config_key.compare("archive_dest"))
    || (0 == config_key.compare("ssl_external_kms_info"))) {
      need_print = false;
  }

  return need_print;
}

void ObLogConfig::print() const
{
  static const int64_t BUF_SIZE = 1L << 22;
  char *buf = static_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::OB_LOG_CONFIG));

  if (OB_ISNULL(buf)) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate memory fail", K(BUF_SIZE));
  } else {
    int64_t pos = 0;
    int64_t size = BUF_SIZE;
    ConfigItemArray configs;

    get_sorted_config_items(configs);

    (void)databuff_printf(buf, size, pos,
        "\n%s ================================ *libobcdc config begin* ================================\n",
        TS_TO_STR(get_timestamp()));

    for (int64_t index = 0; index < configs.count(); index++) {
      if (need_print_config(configs.at(index).key_)) {
        (void)databuff_printf(buf, size, pos, "%s [CONFIG] %-45s = %s\n",
            TS_TO_STR(get_timestamp()), configs.at(index).key_.c_str(),
            configs.at(index).val_.c_str());
      }
    }

    (void)databuff_printf(buf, size, pos,
        "%s ================================ *libobcdc config end* ================================\n",
        TS_TO_STR(get_timestamp()));

    _LOG_INFO("%s", buf);
  }

  if (NULL != buf) {
    ob_free(buf);
    buf = NULL;
  }
}

int ObLogConfig::load_from_map(const ConfigMap &configs,
    const int64_t version /* = 0 */,
    const bool check_name /* = false */)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogConfig has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    std::map<std::string, std::string>::const_iterator iter = configs.begin();
    for (; OB_SUCCESS == ret && iter != configs.end(); iter++) {
      ObConfigItem *const *pp_item = NULL;

      const char *name = iter->first.c_str();
      const char *value = iter->second.c_str();

      if (strlen(value) > OB_MAX_CONFIG_VALUE_LEN && 0 == strcmp("tb_white_list", name)) {
        const int64_t buf_len = strlen(value) + 1;
        if (OB_ISNULL(tb_white_list_buf_ = static_cast<char*>(ob_malloc(buf_len, "white_list_buf")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", KR(ret), K(buf_len));
        } else {
          memcpy(tb_white_list_buf_, value, buf_len);
          tb_white_list_buf_[buf_len - 1] = '\0';
          _LOG_INFO("load tb_white_list config succ, %s=%s", name, value);
        }
      } else if (strlen(value) > OB_MAX_CONFIG_VALUE_LEN && 0 == strcmp("tb_black_list", name)) {
        const int64_t buf_len = strlen(value) + 1;
        if (OB_ISNULL(tb_black_list_buf_ = static_cast<char*>(ob_malloc(buf_len, "black_list_buf")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", KR(ret), K(buf_len));
        } else {
          memcpy(tb_black_list_buf_, value, buf_len);
          tb_black_list_buf_[buf_len - 1] = '\0';
          _LOG_INFO("load tb_black_list config succ, %s=%s", name, value);
        }
      } else if (NULL == (pp_item = container_.get(ObConfigStringKey(iter->first.c_str())))) {
        if (check_name) {
          _LOG_WARN("invalid config string, unknown config item! name: [%s] value: [%s]",
              iter->first.c_str(), iter->second.c_str());
          ret = OB_INVALID_ARGUMENT;
        }
      } else {
        (*pp_item)->set_value(iter->second.c_str());
        (*pp_item)->set_version(version);
        if (need_print_config(iter->first)) {
          _LOG_INFO("load config succ, %s=%s", iter->first.c_str(), iter->second.c_str());
        }
      }
    }
  }

  return ret;
}

int ObLogConfig::load_from_file(const char *config_file,
    const int64_t version,
    const bool check_name)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  char *config_file_buf = NULL;
  int64_t config_buf_len = 0;
  struct stat st;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogConfig has not been initialized");
  } else if (OB_ISNULL(config_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(config_file));
  } else if (0 != stat(config_file, &st)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_ERROR("config_file is invalid", KR(ret), K(config_file), K(errno));
  } else if (FALSE_IT(config_buf_len = st.st_size + 1)) {
  } else if (OB_ISNULL(config_file_buf = static_cast<char *>(ob_malloc(config_buf_len, "ConfigBuf")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for buffer fail", KR(ret), K(config_file_buf), K(config_buf_len));
  } else if (NULL == (fp = fopen(config_file, "rb"))) {
    ret = OB_IO_ERROR;
    LOG_ERROR("can't open file", KR(ret), K(config_file), KERRNOMSG(errno));
  } else {
    config_file_buf[0] = '\0';
    int64_t buffer_size = config_buf_len;
    int64_t read_len = fread(config_file_buf, 1, buffer_size, fp);

    if (0 != ferror(fp)) {
      ret = OB_IO_ERROR;
      LOG_ERROR("read config file error!", KR(ret), K(config_file), KERRNOMSG(errno));
    } else if (0 == feof(fp)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("config file is too long!", KR(ret), K(config_file), K(buffer_size));
    } else if (read_len <= 0) {
      LOG_WARN("config file is empty", KR(ret), K(config_file));
    } else if (read_len >= buffer_size) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("fread buffer overflow", KR(ret), K(read_len), K(buffer_size));
    } else {
      // end with '\0'
      config_file_buf[read_len] = '\0';

      if (OB_FAIL(load_from_buffer_(config_file_buf, read_len, version, check_name))) {
        LOG_ERROR("load config fail", KR(ret), K(config_file), K(read_len));
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

int ObLogConfig::dump2file(const char *file) const {
  int ret = OB_SUCCESS;
  char tmp_file[MAX_PATH_SIZE] = "";
  if (OB_ISNULL(file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(file));
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
        if (0 == strcmp(item.key_.c_str(), "tb_white_list") && OB_NOT_NULL(tb_white_list_buf_)) {
          continue;
        } else if (0 == strcmp(item.key_.c_str(), "tb_black_list") && OB_NOT_NULL(tb_black_list_buf_)) {
          continue;
        } else {
          int write_len = fprintf(fp, "%s=%s\n", item.key_.c_str(), item.val_.c_str());
          if (write_len <= 0) {
            ret = OB_IO_ERROR;
            LOG_WARN("write config file fail",
                K(write_len), "config: name", item.key_.c_str(), "value", item.val_.c_str());
          }
        }
      }

      if (OB_NOT_NULL(tb_white_list_buf_)) {
        int tb_white_list_len = fprintf(fp, "tb_white_list=%s\n", tb_white_list_buf_);
        if (tb_white_list_len <= 0) {
          ret = OB_IO_ERROR;
          LOG_WARN("write table white and black config file fail",
              K(tb_white_list_len), "config: name", "tb_white_list", "value", tb_white_list_buf_);
        }
      }

      if (OB_NOT_NULL(tb_black_list_buf_)) {
        int tb_black_list_len = fprintf(fp, "tb_black_list=%s\n", tb_black_list_buf_);
        if (tb_black_list_len <= 0) {
          ret = OB_IO_ERROR;
          LOG_WARN("write table white and black config file fail",
              K(tb_black_list_len), "config: name", "tb_black_list", "value", tb_black_list_buf_);
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

int ObLogConfig::load_from_buffer_(char *config_str,
    const int64_t config_str_len,
    const int64_t version,
    const bool check_name)
{
  int ret = OB_SUCCESS;
  char *saveptr = NULL;
  char *token = NULL;
  int64_t pos = 0;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("Config has not been initialized");
    ret = OB_NOT_INIT;
  } else if (NULL == config_str || config_str_len <= 0) {
    LOG_ERROR("invalid argument", K(config_str), K(config_str_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    token = strtok_r(config_str, ",\n", &saveptr);
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
      } else if (strlen(value) > OB_MAX_CONFIG_VALUE_LEN && 0 == strcmp("tb_white_list", name)) {
        if (nullptr != tb_white_list_buf_ && 0 == strcmp(value, tb_white_list_buf_)) {
          _LOG_INFO("load config succ, %s=%s", name, value);
        } else {
          char *tmp_tb_white_list_buf = tb_white_list_buf_;
          const int64_t buf_len = strlen(value) + 1;
          if (OB_ISNULL(tb_white_list_buf_ = static_cast<char*>(ob_malloc(buf_len, "white_list_buf")))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", KR(ret), K(buf_len));
          } else {
            memcpy(tb_white_list_buf_, value, buf_len);
            tb_white_list_buf_[buf_len - 1] = '\0';
            if (nullptr != tmp_tb_white_list_buf) {
              ob_free(tmp_tb_white_list_buf);
              tmp_tb_white_list_buf = nullptr;
            }
            _LOG_INFO("load config succ, %s=%s", name, value);
          }
        }
      } else if (strlen(value) > OB_MAX_CONFIG_VALUE_LEN && 0 == strcmp("tb_black_list", name)) {
        if (nullptr != tb_black_list_buf_ && 0 == strcmp(value, tb_black_list_buf_)) {
          _LOG_INFO("load config succ, %s=%s", name, value);
        } else {
          char *tmp_tb_black_list_buf = tb_black_list_buf_;
          const int64_t buf_len = strlen(value) + 1;
          if (OB_ISNULL(tb_black_list_buf_ = static_cast<char*>(ob_malloc(buf_len, "black_list_buf")))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", KR(ret), K(buf_len));
          } else {
            memcpy(tb_black_list_buf_, value, buf_len);
            tb_black_list_buf_[buf_len - 1] = '\0';
            if (nullptr != tmp_tb_black_list_buf) {
              ob_free(tmp_tb_black_list_buf);
              tmp_tb_black_list_buf = nullptr;
            }
            _LOG_INFO("load config succ, %s=%s", name, value);
          }
        }
      } else {
        ObConfigItem *const *pp_item = NULL;
         if (NULL == (pp_item = container_.get(ObConfigStringKey(name)))) {
          if (check_name) {
            _LOG_WARN("invalid config string, unknown config item! name: [%s] value: [%s]",
                name, value);
            ret = OB_INVALID_ARGUMENT;
          }
        } else {
          (*pp_item)->set_value(value);
          (*pp_item)->set_version(version);
          _LOG_INFO("load config succ, %s=%s", name, value);
        }
      }

      if (OB_SUCCESS == ret) {
        token = strtok_r(NULL, ",\n", &saveptr);
      }
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
