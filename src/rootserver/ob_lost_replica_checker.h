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

#ifndef OCEANBASE_ROOTSERVER_OB_LOST_REPLICA_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_LOST_REPLICA_CHECKER_H_

#include "share/ob_define.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "share/partition_table/ob_partition_info.h"
#include "ob_thread_idling.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
}  // end namespace schema
}  // end namespace share

namespace rootserver {
class ObServerManager;

class ObLostReplicaChecker {
public:
  ObLostReplicaChecker();
  virtual ~ObLostReplicaChecker();

  int init(ObServerManager& server_manager, share::ObPartitionTableOperator& pt_operator,
      share::schema::ObMultiVersionSchemaService& schema_service);
  int check_lost_replicas();

private:
  int check_lost_replica_by_pt(uint64_t pt_table_id, int64_t pt_partition_id);
  // (server lost and not in member list) or (server lost and not in schema)
  int check_lost_replica(const share::ObPartitionInfo& partition_info, const share::ObPartitionReplica& replica,
      bool& is_lost_replica) const;
  int check_lost_server(const common::ObAddr& server, bool& is_lost_server) const;
  int check_cancel();
  int delete_pg_partition_meta_table_item_(const uint64_t tg_id, const common::ObAddr& svr_ip);

private:
  bool inited_;
  ObServerManager* server_manager_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLostReplicaChecker);
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_LOST_REPLICA_CHECKER_H_
