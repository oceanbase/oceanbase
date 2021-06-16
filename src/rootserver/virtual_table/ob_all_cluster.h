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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_CLUSTER_H
#define OCEANBASE_ROOTSERVER_OB_ALL_CLUSTER_H

#include "share/ob_virtual_table_projector.h"
#include "share/ob_cluster_info_proxy.h"

namespace oceanbase {
namespace share {
class ObRemoteSqlProxy;
class ObClusterInfo;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share
namespace obrpc {
class ObSrvRpcProxy;
}

namespace rootserver {
class ObZoneManager;
class ObServerManager;

class ObAllVirtualCluster : public common::ObVirtualTableProjector {
public:
  ObAllVirtualCluster()
      : inited_(false),
        zone_manager_(NULL),
        schema_service_(NULL),
        common_rpc_proxy_(NULL),
        sql_proxy_(NULL),
        pt_operator_(NULL),
        rpc_proxy_(NULL),
        server_manager_(NULL),
        root_service_(NULL)
  {}
  virtual ~ObAllVirtualCluster();

  int init(share::schema::ObMultiVersionSchemaService& schema_service_, ObZoneManager& zone_manager,
      obrpc::ObCommonRpcProxy& common_rpc_proxy, ObMySQLProxy& sql_proxy, share::ObPartitionTableOperator& pt_operator,
      obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_manager, ObRootService& root_service);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  int get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns, bool& full_columns);

private:
  static const int64_t RPC_TIMEOOUT = 1 * 1000 * 1000;  // 1s
private:
  bool inited_;
  ObZoneManager* zone_manager_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  obrpc::ObCommonRpcProxy* common_rpc_proxy_;
  ObMySQLProxy* sql_proxy_;
  share::ObPartitionTableOperator* pt_operator_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObServerManager* server_manager_;
  ObRootService* root_service_;
  ObArenaAllocator arena_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualCluster);
};
}  // namespace rootserver
}  // namespace oceanbase

#endif /* !OB_ALL_CLUSTER_H */
