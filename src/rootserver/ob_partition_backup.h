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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_BACKUP_H_
#define OCEANBASE_ROOTSERVER_OB_PARTITION_BACKUP_H_

#include "ob_balance_info.h"
#include "ob_root_utils.h"
#include "rootserver/ob_unit_manager.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_pg_backup_task_updater.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
}
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace rootserver {
namespace balancer {
class HashIndexCollection;
}
class ObRebalanceTaskMgr;
class ObZoneManager;
// The algorithm to balance the replicas in units of one tenant.

class ObPartitionBackupProvider;
class ObPartitionBackup {
public:
  ObPartitionBackup();
  virtual ~ObPartitionBackup()
  {}
  int init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy& sql_proxy, share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr,
      ObZoneManager& zone_mgr, TenantBalanceStat& tenant_stat, ObServerManager& server_mgr,
      share::ObCheckStopProvider& check_stop_provider);
  int partition_backup(int64_t& task_cnt, const uint64_t tenant_id);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBackup);
  // function members
  int get_backup_infos(const uint64_t tenant_id, share::ObTenantBackupTaskInfo& task_info,
      common::ObIArray<share::ObPGBackupTaskInfo>& pg_tasks);
  int check_pg_backup_task(const share::ObPGBackupTaskInfo& pg_task, bool& need_add);
  int backup_pg(const uint64_t tenant_id, int64_t& task_cnt, ObPartitionBackupProvider& provider);

  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }
  int get_detected_region(const uint64_t tenant_id, common::ObIArray<ObRegion>& detected_region);
  int prepare_backup_task(const common::ObIArray<share::ObPGBackupTaskInfo>& pg_tasks,
      ObPartitionBackupProvider& provider, common::ObIAllocator& allocator);
  int batch_update_pg_task_info(const common::ObIArray<ObBackupTaskInfo>& task_info, const share::ObTaskId& task_id);
  int get_task_id_range(const uint64_t tenant_id);
  void reset_task_id_range();
  int cancel_pending_pg_tasks(
      const share::ObTenantBackupTaskInfo& task_info, const common::ObIArray<share::ObPGBackupTaskInfo>& pg_tasks);

private:
  // data members
  bool is_inited_;
  common::ObServerConfig* config_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  share::ObPartitionTableOperator* pt_operator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObZoneManager* zone_mgr_;
  TenantBalanceStat* tenant_stat_;
  TenantBalanceStat* origin_tenant_stat_;
  ObServerManager* server_mgr_;
  share::ObCheckStopProvider* check_stop_provider_;
  share::ObPGBackupTaskUpdater pg_task_updater_;
  int64_t start_task_id_;
  int64_t end_task_id_;
};

struct ObBackupElement {
public:
  ObBackupElement();
  virtual ~ObBackupElement() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObBackupElement& element);
  share::ObPartitionReplica replica_;
  common::ObRegion region_;
  TO_STRING_KV(K_(replica), K_(region));
};

struct ObReplicaBackupElement {
public:
  ObReplicaBackupElement();
  virtual ~ObReplicaBackupElement() = default;
  int assign(const ObReplicaBackupElement& element);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(replica_element));
  common::ObSEArray<ObBackupElement, common::MAX_REPLICA_COUNT_TOTAL> replica_element_;
  const ObBackupElement* choose_element_;
  DISALLOW_COPY_AND_ASSIGN(ObReplicaBackupElement);
};

class ObPartitionBackupProvider {
public:
  ObPartitionBackupProvider();
  virtual ~ObPartitionBackupProvider()
  {}
  int init(const common::ObIArray<common::ObRegion>& detected_region, const share::ObTenantBackupTaskInfo& task_info,
      common::ObIAllocator& allocator, ObZoneManager& zone_mgr, ObServerManager& server_mgr);
  int add_backup_replica_info(const share::ObPartitionInfo& partition_info);
  int prepare_choose_src();
  int generate_batch_backup_task(const int64_t backup_task_id, common::ObIArray<ObBackupTaskInfo>& backup_task);

private:
  int inner_prepare_choose_src();
  int build_physical_backup_arg(const int64_t backup_task_id, share::ObPhysicalBackupArg& arg);
  int check_can_become_dest(const ObBackupElement& element, bool& can_become);

private:
  static const int64_t MAX_BUCKET_NUM = 10240;
  static const int64_t MAX_TASK_NUM = 1024;
  typedef hash::ObHashMap<common::ObPartitionKey, ObReplicaBackupElement*> ReplicaElementMap;
  bool is_inited_;
  common::ObArray<common::ObRegion> detected_region_;
  share::ObTenantBackupTaskInfo task_info_;
  ReplicaElementMap all_replica_elements_;
  common::ObIAllocator* allocator_;
  ObZoneManager* zone_mgr_;
  common::ObArray<common::ObAddr> addr_array_;
  int64_t iter_index_;
  ReplicaElementMap::iterator map_iter_;
  ObServerManager* server_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBackupProvider);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_PARTITION_BACKUP_H_ */
