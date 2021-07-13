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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace common {
template <typename T>
class ObIArray;
struct ObPartitionKey;
}  // namespace common

namespace obrpc {
class ObSrvRpcProxy;
}  // end namespace obrpc

namespace share {
class ObPartitionTableOperator;
}

namespace rootserver {
class ObServerManager;
class ObUnitManager;

class ObRootMinorFreeze {
public:
  ObRootMinorFreeze();
  virtual ~ObRootMinorFreeze();

  int init(obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_manager, ObUnitManager& unit_manager,
      share::ObPartitionTableOperator& pt_operator);
  void start();
  void stop();
  int destroy();

  int try_minor_freeze(const common::ObIArray<uint64_t>& tenant_ids, const common::ObPartitionKey& partition_key_,
      const common::ObIArray<common::ObAddr>& server_list, const common::ObZone& zone) const;

private:
  typedef struct MinorFreezeParam {
    common::ObAddr server;
    obrpc::ObMinorFreezeArg arg;

    TO_STRING_KV(K(server), K(arg));
  } MinorFreezeParam;

  class ParamsContainer {
  public:
    void reset()
    {
      params_.reset();
    }
    bool is_empty() const
    {
      return params_.count() <= 0;
    }
    const common::ObIArray<MinorFreezeParam>& get_params() const
    {
      return params_;
    }

    int add_server(const common::ObAddr& server);
    int add_tenant_server(const uint64_t tenant_id, const common::ObAddr& server);
    int add_partition_server(const common::ObPartitionKey& partition_key, const common::ObAddr& server);

    TO_STRING_KV(K_(params));

  private:
    common::ObSEArray<MinorFreezeParam, 256> params_;
  };

  static const int64_t MAX_FREEZE_OP_RETRY_CNT = 5;
  static const int64_t MINOR_FREEZE_TIMEOUT = (1000 * 30 + 1000) * 1000;  // copy from major freeze

  int is_server_belongs_to_zone(const common::ObAddr& addr, const common::ObZone& zone, bool& server_in_zone) const;

  int init_params_by_partition(const common::ObPartitionKey& partition_key, const common::ObZone& zone,
      const common::ObIArray<common::ObAddr>& server_list, ParamsContainer& params) const;

  int init_params_by_tenant(const common::ObIArray<uint64_t>& tenant_ids, const common::ObZone& zone,
      const common::ObIArray<common::ObAddr>& server_list, ParamsContainer& params) const;

  int init_params_by_zone(const common::ObZone& zone, ParamsContainer& params) const;

  int init_params_by_server(const common::ObIArray<common::ObAddr>& server_list, ParamsContainer& params) const;

  int do_minor_freeze(const ParamsContainer& params) const;

  int check_cancel() const;
  bool is_server_alive(const common::ObAddr& server) const;
  int get_tenant_server_list(uint64_t tenant_id, common::ObIArray<common::ObAddr>& target_server_list) const;

  bool inited_;
  bool stopped_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObServerManager* server_manager_;
  ObUnitManager* unit_manager_;
  share::ObPartitionTableOperator* pt_operator_;
};

}  // namespace rootserver
}  // namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_ */
