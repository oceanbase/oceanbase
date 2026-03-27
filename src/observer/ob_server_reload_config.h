/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_SERVER_RELOAD_CONFIG_H_
#define OCEANBASE_OBSERVER_OB_SERVER_RELOAD_CONFIG_H_

#include "share/config/ob_reload_config.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace observer
{

int set_cluster_name_hash(const common::ObString &cluster_name);
int calc_cluster_name_hash(const common::ObString &cluster_name, uint64_t &cluster_name_hash);
class ObServerReloadConfig
  : public common::ObReloadConfig
{
public:
  ObServerReloadConfig(common::ObServerConfig &config, ObGlobalContext &gctx);
  virtual ~ObServerReloadConfig();

  int operator()();
  class ObReloadTenantFreezerConfOp
  {
  public:
    int operator()();
  };
private:
  void reload_tenant_scheduler_config_();
  void reload_tenant_freezer_config_();
private:
  ObGlobalContext &gctx_;
};

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OB_SERVER_RELOAD_CONFIG_H */
