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

#ifndef OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_
#define OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_

#include "lib/container/ob_iarray.h"
#include "partition_table/ob_partition_location.h"
#include "share/ob_cluster_type.h"  // ObClusterType

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObMySQLProxy;
}  // namespace common
namespace share {
typedef ObReplicaLocation ObRootAddr;
typedef common::ObSArray<ObRootAddr> ObAddrList;
typedef common::ObIArray<ObRootAddr> ObIAddrList;

// store and fetch root server address list interface.
class ObRootAddrAgent {
public:
  ObRootAddrAgent() : inited_(false), config_(NULL)
  {}
  virtual ~ObRootAddrAgent()
  {}

  virtual int init(common::ObServerConfig& config);
  virtual bool is_valid();

  virtual int store(const ObIAddrList& addr_list, const ObIAddrList& readonly_addr_list, const bool force,
      const common::ObClusterType cluster_type, const int64_t timestamp) = 0;
  virtual int fetch(ObIAddrList& add_list, ObIAddrList& readonly_addr_list, common::ObClusterType& cluster_typ) = 0;
  virtual int delete_cluster(const int64_t cluster_id) = 0;
  virtual int fetch_remote_rslist(const int64_t cluster_id, ObIAddrList& addr_list, ObIAddrList& readonly_addr_list,
      common::ObClusterType& cluster_type) = 0;

protected:
  bool inited_;
  common::ObServerConfig* config_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootAddrAgent);
};

inline int ObRootAddrAgent::init(common::ObServerConfig& config)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret));
  } else {
    config_ = &config;
    inited_ = true;
  }
  return ret;
};

inline bool ObRootAddrAgent::is_valid()
{
  return inited_;
}

}  // end namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_
