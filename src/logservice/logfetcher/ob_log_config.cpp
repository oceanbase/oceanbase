/**
 * Copyright (c) 2023 OceanBase
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
namespace logfetcher
{
int ObLogFetcherConfig::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret), K(is_inited_));
  } else if (OB_FAIL(ObBaseConfig::init())){
    LOG_ERROR("init ObBaseConfig failed", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObLogFetcherConfig::destroy()
{
  if (is_inited_) {
    ObBaseConfig::destroy();
    is_inited_ = false;
  }
}

int ObLogFetcherConfig::load_from_map(const ConfigMap &configs,
    const int64_t version /* = 0 */,
    const bool check_name /* = false */)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcherConfig has not been initialized", KR(ret));
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

} // namespace logfetcher
} // namespace oceanbase
