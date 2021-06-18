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

#ifndef OCEANBASE_OBSERVER_OB_REMOTE_SERVER_PROVIDER_H
#define OCEANBASE_OBSERVER_OB_REMOTE_SERVER_PROVIDER_H

#include "lib/mysqlclient/ob_mysql_server_provider.h"
#include "share/ob_root_addr_agent.h"
#include "share/ob_web_service_root_addr.h"
#include "lib/ob_define.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace obrpc {
class ObCommonRpcProxy;
}
namespace share {
// fetch remote server list across cluster
class ObRemoteServerProvider : public common::sqlclient::ObMySQLServerProvider {
  struct ServerAddr {
    public:
    ServerAddr() : server_(), sql_port_(common::OB_INVALID_ID)
    {}
    virtual ~ServerAddr()
    {}
    void reset();
    int64_t to_string(char* buf, const int64_t buf_len) const;

    public:
    common::ObAddr server_;
    int64_t sql_port_;
  };
  typedef common::ObSEArray<ServerAddr, common::MAX_ZONE_NUM> ServerAddrList;
  struct ServerInfo {
    public:
    ServerInfo() = delete;
    ServerInfo(common::ObIAllocator& allocator)
        : server_list_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator)),
          cluster_id_(common::OB_INVALID_ID)
    {}
    virtual ~ServerInfo()
    {}
    int64_t to_string(char* buf, const int64_t buf_len) const;

    public:
    ServerAddrList server_list_;
    int64_t cluster_id_;
    DISALLOW_COPY_AND_ASSIGN(ServerInfo);
  };

  public:
  ObRemoteServerProvider();
  virtual ~ObRemoteServerProvider();
  int init(obrpc::ObCommonRpcProxy& rpc_proxy, common::ObMySQLProxy& sql_proxy);
  virtual int get_cluster_list(common::ObIArray<int64_t>& cluster_list);
  virtual int get_server(const int64_t cluster_id, const int64_t svr_idx, common::ObAddr& server);
  virtual int64_t get_cluster_count() const;
  virtual int64_t get_server_count() const;
  virtual int64_t get_server_count(const int64_t cluster_id) const;
  virtual int refresh_server_list(void);
  virtual int prepare_refresh() override;
  int64_t get_primary_cluster_id() const;
  bool need_refresh();
};
}  // namespace share
}  // namespace oceanbase
#endif
