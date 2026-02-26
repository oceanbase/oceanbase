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

#include "share/config/ob_reload_config.h"
#include "share/config/ob_config_manager.h"
#include "lib/oblog/ob_log_compressor.h"

namespace oceanbase
{
namespace common
{
int ObReloadConfig::reload_ob_logger_set()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conf_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "server config is null", K(ret));
  } else {
    if (OB_FAIL(OB_LOGGER.parse_set(conf_->syslog_level,
                                    static_cast<int32_t>(STRLEN(conf_->syslog_level)),
                                    (conf_->syslog_level).version()))) {
      OB_LOG(ERROR, "fail to parse_set syslog_level",
             K(conf_->syslog_level.str()), K((conf_->syslog_level).version()), K(ret));
    } else if (OB_FAIL(ObConfigManager::ob_logger_config_update(*conf_))) {
      OB_LOG(ERROR, "fail to update logger config", K(ret));
    } else {
      ObKVGlobalCache::get_instance().reload_priority();
    }
  }
  return ret;
}

}//end of common
}//end of oceanbase
