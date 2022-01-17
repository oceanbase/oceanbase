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

#ifndef OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_
#define OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_

#include "share/ob_root_addr_agent.h"
#include "share/ob_cluster_type.h"  // ObClusterType

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObSqlString;
}  // namespace common
namespace share {

// store and dispatch root server address list by oceanbase config mechanism.
// same with oceanbase 0.5
class ObInnerConfigRootAddr : public ObRootAddrAgent {
public:
  ObInnerConfigRootAddr() : inited_(false), proxy_(NULL)
  {}
  virtual ~ObInnerConfigRootAddr()
  {}

  int init(common::ObMySQLProxy& sql_proxy, common::ObServerConfig& config);

  virtual int store(const ObIAddrList& addr_list, const ObIAddrList& readonly_addr_list, const bool force,
      const common::ObClusterType cluster_type, const int64_t timestamp) override;
  virtual int fetch(
      ObIAddrList& add_list, ObIAddrList& readonly_addr_list, common::ObClusterType& cluster_typ) override;
  // innerconfig does not need to support delete cluster operations
  virtual int delete_cluster(const int64_t cluster_id) override
  {
    UNUSED(cluster_id);
    return common::OB_SUCCESS;
  }
  virtual int fetch_remote_rslist(const int64_t cluster_id, ObIAddrList& addr_list, ObIAddrList& readonly_addr_list,
      common::ObClusterType& cluster_type) override;

  static int format_rootservice_list(const ObIAddrList& addr_list, common::ObSqlString& str);

private:
  static int parse_rs_addr(char* addr_buf, common::ObAddr& addr, int64_t& sql_port);

private:
  bool inited_;
  common::ObMySQLProxy* proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObInnerConfigRootAddr);
};

}  // end namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_
