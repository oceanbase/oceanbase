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

#ifndef OCEANBASE_SHARE_CONFIG_OB_RELOAD_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_RELOAD_CONFIG_H_

#include "share/ob_define.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace common
{
class ObReloadConfig
{
public:
  explicit ObReloadConfig(ObServerConfig *conf): conf_(conf) {};
  virtual ~ObReloadConfig() {}
  virtual int operator()();

protected:
  ObServerConfig *conf_;

private:
  int reload_ob_logger_set();
  DISALLOW_COPY_AND_ASSIGN(ObReloadConfig);
};

inline int ObReloadConfig::operator()()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != conf_)) {
    reload_ob_logger_set();
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_RELOAD_CONFIG_H_
