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

#ifndef OCEANBASE_ROOTSERVER_OB_SERVER_RECOVERY_MACHINE_H_
#define OCEANBASE_ROOTSERVER_OB_SERVER_RECOVERY_MACHINE_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/thread/ob_reentrant_thread.h"
#include "sql/ob_index_sstable_builder.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_rs_async_rpc_proxy.h"
#include "ob_empty_server_checker.h"
#include "ob_thread_idling.h"
#include "ob_rs_reentrant_thread.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
class ObPartitionReplica;
class ObPartitionInfo;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace common {
class ObMySQLProxy;
class ObReplicaMember;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace obrpc {
class ObSrvRpcProxy;
}
namespace rootserver {
class ObServerManager;
class ObRebalanceTaskMgr;
class ObZoneManager;
class ObRebalanceTask;
class ObFreezeInfoManager;
class ObUnitManager;

class ObServerRecoveryMachineIdling : public ObThreadIdling {
public:
  explicit ObServerRecoveryMachineIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual ~ObServerRecoveryMachineIdling()
  {}

public:
  virtual int64_t get_idle_interval_us()
  {
    return 3600LL * 1000LL * 1000LL;
  }  // max_idle_time_us
};

enum class ServerRecoveryProgress : int64_t {
  SRP_INVALID = 0,
  SRP_SPLIT_SERVER_LOG,
  SRP_IN_PG_RECOVERY,
  SRP_CHECK_PG_RECOVERY_FINISHED,
  SRP_SERVER_RECOVERY_FINISHED,
  SRP_MAX,
};

enum class SplitServerLogStatus : int64_t {
  SSLS_INVALID = 0,
  SSLS_SUCCEED,
  SSLS_FAILED,
  SSLS_MAX,
};

enum class RecoverFileTaskGenStatus : int64_t {
  RFTGS_INVALID = 0,
  RFTGS_TASK_GENERATED,
  RFTGS_MAX,
};

enum class CheckPgRecoveryFinishedStatus : int64_t {
  CPRFS_INVALID = 0,
  CPRFS_SUCCEED,
  CPRFS_EAGAIN,
  CPRFS_FAILED,
  CPRFS_MAX,
};

enum class FileRecoveryStatus : int64_t {
  FRS_INVALID = 0,
  FRS_IN_PROGRESS,
  FRS_SUCCEED,
  FRS_FAILED,
};

class ObServerRecoveryMachine;

class CheckPgRecoveryFinishedTask : public common::IObDedupTask {
public:
  CheckPgRecoveryFinishedTask(const common::ObAddr& server, const volatile bool& is_stopped,
      ObServerRecoveryMachine& host, share::ObPartitionTableOperator& pt_operator,
      share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& mysql_proxy)
      : common::IObDedupTask(T_CHECK_PG_RECOVERY_FINISHED),
        server_(server),
        is_stopped_(is_stopped),
        host_(host),
        pt_operator_(pt_operator),
        schema_service_(schema_service),
        mysql_proxy_(mysql_proxy)
  {}
  virtual ~CheckPgRecoveryFinishedTask()
  {}

public:
  // virtual interface
  virtual int64_t hash() const;
  virtual bool operator==(const common::IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual common::IObDedupTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

  TO_STRING_KV(K(server_), K(is_stopped_));

private:
  int do_check_pg_recovery_finished(bool& is_finished);

private:
  const common::ObAddr server_;
  const volatile bool& is_stopped_;
  ObServerRecoveryMachine& host_;
  share::ObPartitionTableOperator& pt_operator_;
  share::schema::ObMultiVersionSchemaService& schema_service_;
  common::ObMySQLProxy& mysql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(CheckPgRecoveryFinishedTask);
};

class UpdateFileRecoveryStatusTask : public common::IObDedupTask {
public:
  UpdateFileRecoveryStatusTask(const common::ObAddr& server, const common::ObAddr& dest_server,
      const uint64_t tenant_id, const int64_t file_id, const FileRecoveryStatus pre_status,
      const FileRecoveryStatus cur_status, const volatile bool& is_stopped, ObServerRecoveryMachine& host)
      : common::IObDedupTask(T_UPDATE_FILE_RECOVERY_STATUS),
        server_(server),
        dest_server_(dest_server),
        tenant_id_(tenant_id),
        file_id_(file_id),
        pre_status_(pre_status),
        cur_status_(cur_status),
        is_stopped_(is_stopped),
        host_(host)
  {}
  virtual ~UpdateFileRecoveryStatusTask()
  {}

public:
  // virtual interface
  virtual int64_t hash() const;
  virtual bool operator==(const common::IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual common::IObDedupTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

  TO_STRING_KV(K(server_), K(is_stopped_));

private:
  const common::ObAddr server_;
  const common::ObAddr dest_server_;
  const uint64_t tenant_id_;
  const int64_t file_id_;
  const FileRecoveryStatus pre_status_;
  const FileRecoveryStatus cur_status_;
  const volatile bool& is_stopped_;
  ObServerRecoveryMachine& host_;

private:
  DISALLOW_COPY_AND_ASSIGN(UpdateFileRecoveryStatusTask);
};

struct RefugeeInfo {
  RefugeeInfo()
      : server_(),
        tenant_id_(OB_INVALID_ID),
        file_id_(-1),
        dest_unit_id_(OB_INVALID_ID),
        dest_server_(),
        file_recovery_status_(FileRecoveryStatus::FRS_INVALID)
  {}
  virtual ~RefugeeInfo()
  {}

  TO_STRING_KV(K_(server), K_(tenant_id), K_(file_id), K_(dest_unit_id), K_(dest_server), K_(file_recovery_status));
  int assign(const RefugeeInfo& that);

  common::ObAddr server_;
  uint64_t tenant_id_;
  int64_t file_id_;
  uint64_t dest_unit_id_;
  common::ObAddr dest_server_;

  FileRecoveryStatus file_recovery_status_;
};

struct ObServerRecoveryTask {
public:
  ObServerRecoveryTask();
  virtual ~ObServerRecoveryTask();
  TO_STRING_KV(K(server_), K(rescue_server_), K(progress_), K(refugee_infos_), K(split_server_log_status_),
      K(recover_file_task_gen_status_), K(check_pg_recovery_finished_status_), K(last_drive_ts_));
  int get_refugee_info(
      const uint64_t tenant_id, const int64_t file_id, const common::ObAddr& dest_server, RefugeeInfo*& refugee_info);

public:
  common::ObAddr server_;
  common::ObAddr rescue_server_;
  ServerRecoveryProgress progress_;
  common::ObArray<RefugeeInfo> refugee_infos_;
  SplitServerLogStatus split_server_log_status_;
  RecoverFileTaskGenStatus recover_file_task_gen_status_;
  CheckPgRecoveryFinishedStatus check_pg_recovery_finished_status_;
  int64_t last_drive_ts_;
  mutable common::SpinRWLock lock_;
};

struct SimpleSeqGenerator {
  SimpleSeqGenerator() : seq_(0)
  {}
  int64_t next_seq()
  {
    return seq_++;
  }

private:
  int64_t seq_;
};

class ObRebalanceTask;

class ObServerRecoveryMachine : public ObRsReentrantThread {
  typedef common::hash::ObHashMap<common::ObAddr, ObServerRecoveryTask*>::iterator task_iterator;
  typedef common::hash::ObHashMap<common::ObAddr, ObServerRecoveryTask*>::const_iterator const_task_iterator;

public:
  ObServerRecoveryMachine();
  virtual ~ObServerRecoveryMachine();

public:
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* mysql_proxy, rootserver::ObServerManager* server_mgr,
      share::schema::ObMultiVersionSchemaService* schema_service, share::ObPartitionTableOperator* pt_operator,
      rootserver::ObRebalanceTaskMgr* rebalance_task_mgr, rootserver::ObZoneManager* zone_mgr,
      rootserver::ObUnitManager* unit_mgr, rootserver::ObEmptyServerChecker* empty_server_checker);
  int reload_server_recovery_machine();
  void wakeup();
  int on_split_server_log_reply(const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code);
  int on_recover_pg_file_reply(const ObRebalanceTask& task, const common::ObIArray<int>& ret_code_array);
  int on_check_pg_recovery_finished(const common::ObAddr& server, const int ret_code);
  int update_file_recovery_status(const common::ObAddr& server, const common::ObAddr& dest_server,
      const uint64_t tenant_id, const int64_t file_id, FileRecoveryStatus pre_status, FileRecoveryStatus cur_status);
  void stop();

private:
  // Machine const
  static const int64_t TASK_MAP_BUCKET_NUM = 2048;
  static const int64_t THREAD_CNT = 1;
  static const int64_t IDLING_US = 1 * 1000000;                                // 1s
  static const int64_t SPLIT_SERVER_LOG_TIMEOUT = 5 * 60 * 1000000;            // 5min
  static const int64_t PG_RECOVERY_TIMEOUT = 10 * 60 * 1000000;                // 10min
  static const int64_t CHECK_PG_RECOVERY_FINISHED_TIMEOUT = 5 * 60 * 1000000;  // 5min
  // check pg recovery finished task queue const
  static const int64_t CHECK_PG_RECOVERY_FINISHED_THREAD_CNT = 1;
  // datafile recovery status task queue const
  static const int64_t DATAFILE_RECOVERY_STATUS_THREAD_CNT = 1;
  int switch_state(ObServerRecoveryTask* task, const ServerRecoveryProgress new_state);
  int reset_run_condition();
  int discover_new_server_recovery_task();
  int drive_existing_server_recovery_task();
  int clean_finished_server_recovery_task();
  int try_drive(ObServerRecoveryTask* task);
  int try_split_server_log(ObServerRecoveryTask* task);
  int try_recover_pg(ObServerRecoveryTask* task);
  int try_check_pg_recovery_finished(ObServerRecoveryTask* task);
  // reload associated sub routine
  int fill_server_recovery_task_result(common::sqlclient::ObMySQLResult* result, ObServerRecoveryTask* task);
  int reload_server_recovery_status();
  int reload_file_recovery_status();
  int reload_file_recovery_status_by_server(ObServerRecoveryTask* task);
  int fill_server_file_recovery_status(common::sqlclient::ObMySQLResult* result, ObServerRecoveryTask* task);
  // split server log sub routine
  int drive_this_split_server_log(ObServerRecoveryTask* task);
  int launch_new_split_server_log(ObServerRecoveryTask* task);
  int pick_new_rescue_server(common::ObAddr& new_rescue_server);
  int do_launch_new_split_server_log(
      const common::ObAddr& server, const common::ObAddr& rescue_server, const bool is_new_rescue_server);
  // recover pg sub routine
  int drive_this_recover_pg(ObServerRecoveryTask* task);
  int launch_new_recover_pg(ObServerRecoveryTask* task);
  int pick_dest_server_for_refugee(RefugeeInfo& refugee_info);
  int launch_recover_file_task(ObServerRecoveryTask* task, RefugeeInfo& refugee_info);
  int trigger_recover_file_task(ObServerRecoveryTask* task, RefugeeInfo& refugee_info);
  // check pg recovery finished routine
  int drive_this_check_pg_recovery_finished(ObServerRecoveryTask* task);
  int launch_new_check_pg_recovery_finished(ObServerRecoveryTask* task);
  int submit_check_pg_recovery_finished_task(const common::ObAddr& server);

private:
  bool inited_;
  bool loaded_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* mysql_proxy_;
  rootserver::ObServerManager* server_mgr_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  rootserver::ObRebalanceTaskMgr* rebalance_task_mgr_;
  rootserver::ObZoneManager* zone_mgr_;
  rootserver::ObUnitManager* unit_mgr_;
  rootserver::ObEmptyServerChecker* empty_server_checker_;
  common::ObPooledAllocator<ObServerRecoveryTask> task_allocator_;
  common::hash::ObHashMap<common::ObAddr, ObServerRecoveryTask*> task_map_;
  // task map lock, when modify the map per se, for example insert, delete, exclusive lock
  // when modify items of map, inclusive lock, for example update the ServerRecoveryTask item
  common::SpinRWLock task_map_lock_;
  ObServerRecoveryMachineIdling idling_;
  common::hash::ObHashMap<common::ObAddr, int64_t> rescue_server_counter_;
  common::ObDedupQueue check_pg_recovery_finished_task_queue_;
  common::ObDedupQueue datafile_recovery_status_task_queue_;
  SimpleSeqGenerator seq_generator_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_GLOBAL_INDEX_BUILDER_H_
