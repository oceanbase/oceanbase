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
    } else if (OB_FAIL(OB_LOGGER.set_max_file_index(
        static_cast<int32_t>(conf_->max_syslog_file_count)))) {
      OB_LOG(ERROR, "fail to set_max_file_index", K(conf_->max_syslog_file_count.get()), K(ret));
    } else if (OB_FAIL(OB_LOGGER.set_record_old_log_file(conf_->enable_syslog_recycle))) {
      OB_LOG(ERROR, "fail to set_record_old_log_file",
             K(conf_->enable_syslog_recycle.str()), K(ret));
    } else if (OB_FAIL(OB_LOG_COMPRESSOR.set_max_disk_size(conf_->syslog_disk_size))) {
      OB_LOG(ERROR, "fail to set_max_disk_size",
             K(conf_->syslog_disk_size.str()), KR(ret));
    } else if (OB_FAIL(OB_LOG_COMPRESSOR.set_compress_func(conf_->syslog_compress_func.str()))) {
      OB_LOG(ERROR, "fail to set_compress_func",
             K(conf_->syslog_compress_func.str()), KR(ret));
    } else if (OB_FAIL(OB_LOG_COMPRESSOR.set_min_uncompressed_count(conf_->syslog_file_uncompressed_count))) {
      OB_LOG(ERROR, "fail to set_min_uncompressed_count",
             K(conf_->syslog_file_uncompressed_count.str()), KR(ret));
    } else {
      OB_LOGGER.set_log_warn(conf_->enable_syslog_wf);
      OB_LOGGER.set_enable_async_log(conf_->enable_async_syslog);
      ObKVGlobalCache::get_instance().reload_priority();
    }
  }
  return ret;
}

}//end of common
}//end of oceanbase
