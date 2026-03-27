/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
