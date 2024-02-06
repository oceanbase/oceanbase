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

#ifndef OCEANBASE_OBSERVER_OB_SERVER_RELOAD_CONFIG_H_
#define OCEANBASE_OBSERVER_OB_SERVER_RELOAD_CONFIG_H_

#include "share/config/ob_reload_config.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace observer
{

struct ObGlobalContext;
int set_cluster_name_hash(const common::ObString &cluster_name);
int calc_cluster_name_hash(const common::ObString &cluster_name, uint64_t &cluster_name_hash);
class ObServerReloadConfig
  : public common::ObReloadConfig
{
public:
  ObServerReloadConfig(common::ObServerConfig &config, ObGlobalContext &gctx);
  virtual ~ObServerReloadConfig();

  int operator()();
private:
  void reload_tenant_scheduler_config_();


private:
  ObGlobalContext &gctx_;
};

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OB_SERVER_RELOAD_CONFIG_H */
