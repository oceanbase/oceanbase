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

} // namespace libobcdc
} // namespace oceanbase
