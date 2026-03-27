/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_
#define OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_

#include "share/ob_root_addr_agent.h"
#include "share/ob_cluster_role.h"              // ObClusterRole

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObSqlString;
}
namespace share
{

// store and dispatch root server address list by oceanbase config mechanism.
// same with oceanbase 0.5
class ObInnerConfigRootAddr : public ObRootAddrAgent
{
public:
  ObInnerConfigRootAddr() : inited_(false), proxy_(NULL) {}
  virtual ~ObInnerConfigRootAddr() {}

  int init(common::ObMySQLProxy &sql_proxy, common::ObServerConfig &config);

  virtual int store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                    const bool force, const common::ObClusterRole cluster_role,
                    const int64_t timestamp);
  virtual int fetch(ObIAddrList &add_list,
                    ObIAddrList &readonly_addr_list);
  static int format_rootservice_list(const ObIAddrList &addr_list, common::ObSqlString &str);
private:
  static int parse_rs_addr(char *addr_buf, common::ObAddr &addr, int64_t &sql_port);
  virtual int check_inner_stat() const override;

private:
  bool inited_;
  common::ObMySQLProxy *proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObInnerConfigRootAddr);
};

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_
