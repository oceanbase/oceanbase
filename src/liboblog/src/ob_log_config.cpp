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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_config.h"

#include "lib/container/ob_array.h"             // ObArray
#include "lib/container/ob_array_iterator.h"    // ObArray::begin
#include "lib/allocator/ob_malloc.h"            // ob_malloc/ob_free

#include "ob_log_utils.h"                       // TS_TO_STR, get_timestamp

using namespace oceanbase::common;
namespace oceanbase
{
namespace liboblog
{
ObLogConfig& ObLogConfig::get_instance()
{
  static ObLogConfig config;
  return config;
}

int ObLogConfig::init()
{
  int ret = OB_SUCCESS;
  const int64_t buf_len= OBLOG_MAX_CONFIG_LENGTH;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(config_file_buf1_= static_cast<char *>(ob_malloc(buf_len, ObModIds::OB_LOG_CONFIG)))) {
    LOG_ERROR("allocate memory for buffer fail", K(config_file_buf1_), K(buf_len));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(config_file_buf2_= static_cast<char *>(ob_malloc(buf_len, ObModIds::OB_LOG_CONFIG)))) {
    LOG_ERROR("allocate memory for buffer fail", K(config_file_buf2_), K(buf_len));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    inited_ = true;
  }

  return ret;
}

void ObLogConfig::destroy()
{
  if (NULL != config_file_buf1_) {
    ob_free(config_file_buf1_);
    config_file_buf1_ = NULL;
  }

  if (NULL != config_file_buf2_) {
    ob_free(config_file_buf2_);
    config_file_buf2_ = NULL;
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

int ObLogConfig::check_all()
{
  int ret = OB_SUCCESS;
  ObConfigContainer::const_iterator it = container_.begin();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogConfig has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    for (; OB_SUCCESS == ret && it != container_.end(); it++) {
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

struct ConfigItem
{
  std::string key_;
  std::string val_;

  bool operator == (const ConfigItem &item)
  {
    return key_ == item.key_;
  }

  bool operator < (const ConfigItem &item)
  {
    return key_ < item.key_;
  }

  ConfigItem() : key_(), val_()
  {}

  ConfigItem(const char *key, const char *val) : key_(key), val_(val) {}

  TO_STRING_KV("key", key_.c_str(), "val", val_.c_str());
};

typedef ObArray<ConfigItem> ConfigItemArray;

void get_sorted_config_items(const ObConfigContainer &container, ConfigItemArray &configs)
{
  // Transfer the configuration items to an array and sort the output
  ObConfigContainer::const_iterator it = container.begin();
  for (; it != container.end(); it++) {
    ConfigItem item(it->first.str(), NULL == it->second ? "" : it->second->str());
    (void)configs.push_back(item);
  }
  std::sort(configs.begin(), configs.end());
}

void ObLogConfig::print() const
{
  static const int64_t BUF_SIZE = 1L << 22;
  char *buf = static_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::OB_LOG_CONFIG));

  if (OB_ISNULL(buf)) {
    LOG_ERROR("allocate memory fail", K(BUF_SIZE));
  } else {
    int64_t pos = 0;
    int64_t size = BUF_SIZE;
    ConfigItemArray configs;

    get_sorted_config_items(container_, configs);

    (void)databuff_printf(buf, size, pos,
        "\n%s ================================ *liboblog config begin* ================================\n",
        TS_TO_STR(get_timestamp()));

    for (int64_t index = 0; index < configs.count(); index++) {
      (void)databuff_printf(buf, size, pos, "%s [CONFIG] %-45s = %s\n",
          TS_TO_STR(get_timestamp()), configs.at(index).key_.c_str(),
          configs.at(index).val_.c_str());
    }

    (void)databuff_printf(buf, size, pos,
        "%s ================================ *liboblog config end* ================================\n",
        TS_TO_STR(get_timestamp()));

    _LOG_INFO("%s", buf);
  }

  if (NULL != buf) {
    ob_free(buf);
    buf = NULL;
  }
}

int ObLogConfig::load_from_map(const ConfigMap& configs,
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

      if (NULL == (pp_item = container_.get(ObConfigStringKey(iter->first.c_str())))) {
        if (check_name) {
          _LOG_WARN("invalid config string, unknown config item! name: [%s] value: [%s]",
              iter->first.c_str(), iter->second.c_str());
          ret = OB_INVALID_ARGUMENT;
        }
      } else {
        (*pp_item)->set_value(iter->second.c_str());
        (*pp_item)->set_version(version);
        _LOG_INFO("load config succ, %s=%s", iter->first.c_str(), iter->second.c_str());
      }
    }
  }

  return ret;
}

int ObLogConfig::load_from_buffer(const char *config_str,
    const int64_t config_str_len,
    const int64_t version /* = 0 */,
    const bool check_name /* = false */)
{
  int ret = OB_SUCCESS;
  char *saveptr = NULL;
  char *token = NULL;
  int64_t pos =0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogConfig has not been initialized");
    ret = OB_NOT_INIT;
  } else if (NULL == config_str || config_str_len <= 0) {
    LOG_ERROR("invalid argument", K(config_str), K(config_str_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(config_file_buf2_)) {
    LOG_ERROR("config_file_buf2_ is NULL", K(config_file_buf2_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    config_file_buf2_[0] = '\0';
    const int64_t buf_size = OBLOG_MAX_CONFIG_LENGTH;

    if (config_str_len > (buf_size - 1)) {
      LOG_ERROR("extra config is too long!", K(config_str_len), K(buf_size));
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(databuff_printf(config_file_buf2_, buf_size, pos, "%.*s",
            static_cast<int>(config_str_len), config_str))) {
      LOG_ERROR("copy config string fail", KR(ret), K(config_file_buf2_), K(buf_size), K(pos), K(config_str_len),
          K(config_str));
    } else {
      token = strtok_r(config_file_buf2_, ",\n", &saveptr);
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
          _LOG_INFO("load config succ, %s=%s", name, value);
        }

        if (OB_SUCCESS == ret) {
          token = strtok_r(NULL, ",\n", &saveptr);
        }
      }
    }
  }

  return ret;
}

int ObLogConfig::load_from_file(const char *config_file,
    const int64_t version /* = 0 */,
    const bool check_name /* = false */)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogConfig has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(config_file)) {
    LOG_ERROR("invalid argument", K(config_file));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(config_file_buf1_)) {
    LOG_ERROR("config_file_buf1_ is NULL", K(config_file_buf1_));
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL == (fp = fopen(config_file, "rb"))) {
    ret = OB_IO_ERROR;
    LOG_ERROR("can't open file", K(config_file), KR(ret), KERRNOMSG(errno));
  } else {
    config_file_buf1_[0] = '\0';
    int64_t buffer_size = OBLOG_MAX_CONFIG_LENGTH;
    int64_t read_len = fread(config_file_buf1_, 1, buffer_size - 1, fp);

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
      config_file_buf1_[read_len] = '\0';

      if (OB_FAIL(load_from_buffer(config_file_buf1_, read_len, version, check_name))) {
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

  return ret;
}

int ObLogConfig::dump2file(const char *file) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(file)) {
    LOG_ERROR("invalid argument", K(file));
    ret = OB_INVALID_ARGUMENT;
  } else {
    FILE *fp = NULL;
    ConfigItemArray configs;

    get_sorted_config_items(container_, configs);

    if (NULL == (fp = fopen(file, "w+"))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("open file fail", K(file), KERRMSG);
    } else {
      for (int64_t index = 0; index < configs.count(); index++) {
        const ConfigItem &item = configs.at(index);

        int write_len = fprintf(fp, "%s=%s\n", item.key_.c_str(), item.val_.c_str());
        if (write_len <= 0) {
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

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
