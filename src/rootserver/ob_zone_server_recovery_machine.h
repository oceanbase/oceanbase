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

#ifndef OCEANBASE_ROOTSERVER_OB_ZONE_SERVER_RECOVERY_MACHINE_H_
#define OCEANBASE_ROOTSERVER_OB_ZONE_SERVER_RECOVERY_MACHINE_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_rs_async_rpc_proxy.h"
#include "ob_empty_server_checker.h"
#include "ob_thread_idling.h"
#include "ob_rs_reentrant_thread.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace rootserver {
class ObServerManager;
class ObZoneManager;
class ObUnitManager;
class ObEmptyServerChecker;
class ObZoneRecoveryTaskMgr;
class ObZoneRecoveryTask;
enum class ZoneServerRecoveryProgress : int64_t {
  SRP_PRE_PROCESS_SERVER = 0,
  SRP_IN_PG_RECOVERY,
  SRP_SERVER_RECOVERY_FINISHED,
  SRP_MAX,
};

enum class PreProcessServerStatus : int64_t {
  PPSS_INVALID = 0,
  PPSS_SUCCEED,
  PPSS_FAILED,
  PPSS_MAX,
};

enum class ZoneRecoverFileTaskGenStatus : int64_t {
  RFTGS_INVALID = 0,
  RFTGS_TASK_GENERATED,
  RFTGS_MAX,
};

enum class ZoneFileRecoveryStatus : int64_t {
  FRS_IN_PROGRESS = 0,
  FRS_SUCCEED,
  FRS_FAILED,
  FRS_MAX,
};

class ObServerRecoveryInstance;

class UpdateFileRecoveryStatusTaskV2 : public common::IObDedupTask {
public:
  UpdateFileRecoveryStatusTaskV2(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id, const int64_t file_id,
      const ZoneFileRecoveryStatus pre_status, const ZoneFileRecoveryStatus cur_status, const volatile bool& is_stopped,
      ObServerRecoveryInstance& host)
      : common::IObDedupTask(T_UPDATE_FILE_RECOVERY_STATUS_V2),
        zone_(zone),
        server_(server),
        svr_seq_(svr_seq),
        dest_server_(dest_server),
        dest_svr_seq_(dest_svr_seq),
        tenant_id_(tenant_id),
        file_id_(file_id),
        pre_status_(pre_status),
        cur_status_(cur_status),
        is_stopped_(is_stopped),
        host_(host)
  {}
  virtual ~UpdateFileRecoveryStatusTaskV2()
  {}

public:
  virtual int64_t hash() const;
  virtual bool operator==(const common::IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual common::IObDedupTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

  TO_STRING_KV(
      K(zone_), K(server_), K(svr_seq_), K(dest_server_), K(dest_svr_seq_), K(tenant_id_), K(file_id_), K(is_stopped_));

private:
  const common::ObZone zone_;
  const common::ObAddr server_;
  const int64_t svr_seq_;
  const common::ObAddr dest_server_;
  const int64_t dest_svr_seq_;
  const uint64_t tenant_id_;
  const int64_t file_id_;
  const ZoneFileRecoveryStatus pre_status_;
  const ZoneFileRecoveryStatus cur_status_;
  const volatile bool& is_stopped_;
  ObServerRecoveryInstance& host_;

private:
  DISALLOW_COPY_AND_ASSIGN(UpdateFileRecoveryStatusTaskV2);
};

struct RefugeeServerInfo {
public:
  RefugeeServerInfo()
      : zone_(),
        server_(),
        svr_seq_(OB_INVALID_SVR_SEQ),
        rescue_server_(),
        rescue_progress_(ZoneServerRecoveryProgress::SRP_MAX)
  {}
  virtual ~RefugeeServerInfo()
  {}

public:
  int init(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& rescue_server, ZoneServerRecoveryProgress progress);
  TO_STRING_KV(K_(zone), K_(server), K_(svr_seq), K_(rescue_server), K_(rescue_progress));
  int assign(const RefugeeServerInfo& that);

public:
  common::ObZone zone_;
  common::ObAddr server_;
  int64_t svr_seq_;
  common::ObAddr rescue_server_;
  ZoneServerRecoveryProgress rescue_progress_;
};

struct RefugeeFileInfo {
public:
  RefugeeFileInfo()
      : server_(),
        svr_seq_(OB_INVALID_SVR_SEQ),
        tenant_id_(OB_INVALID_ID),
        file_id_(-1),
        dest_server_(),
        dest_svr_seq_(OB_INVALID_SVR_SEQ),
        dest_unit_id_(OB_INVALID_ID),
        file_recovery_status_(ZoneFileRecoveryStatus::FRS_MAX)
  {}
  virtual ~RefugeeFileInfo()
  {}

  TO_STRING_KV(K_(server), K_(svr_seq), K_(tenant_id), K_(file_id), K_(dest_server), K_(dest_svr_seq), K_(dest_unit_id),
      K_(file_recovery_status));
  int assign(const RefugeeFileInfo& that);

public:
  common::ObAddr server_;
  int64_t svr_seq_;
  uint64_t tenant_id_;
  int64_t file_id_;
  common::ObAddr dest_server_;
  int64_t dest_svr_seq_;
  uint64_t dest_unit_id_;
  ZoneFileRecoveryStatus file_recovery_status_;
};

struct ObRecoveryTask {
public:
  ObRecoveryTask();
  virtual ~ObRecoveryTask();
  TO_STRING_KV(K(zone_), K(server_), K(svr_seq_), K(rescue_server_), K(progress_), K(refugee_file_infos_),
      K(pre_process_server_status_), K(recover_file_task_gen_status_), K(last_drive_ts_));
  int get_refugee_datafile_info(const uint64_t tenant_id, const int64_t file_id, const common::ObAddr& dest_server,
      RefugeeFileInfo*& refugee_info);

public:
  common::ObZone zone_;
  common::ObAddr server_;
  int64_t svr_seq_;
  common::ObAddr rescue_server_;
  ZoneServerRecoveryProgress progress_;
  common::ObArray<RefugeeFileInfo> refugee_file_infos_;
  PreProcessServerStatus pre_process_server_status_;
  ZoneRecoverFileTaskGenStatus recover_file_task_gen_status_;
  int64_t last_drive_ts_;
  mutable common::SpinRWLock lock_;
};

struct ThisSimpleSeqGenerator {
  ThisSimpleSeqGenerator() : seq_(0)
  {}
  int64_t next_seq()
  {
    return seq_++;
  }

private:
  int64_t seq_;
};

class RecoveryPersistenceProxy {
public:
  RecoveryPersistenceProxy()
  {}
  virtual ~RecoveryPersistenceProxy()
  {}

public:
  int get_server_recovery_persistence_status(common::ObIArray<RefugeeServerInfo>& refugee_server_array);

  int get_server_datafile_recovery_persistence_status(const common::ObZone& zone, const common::ObAddr& refugee_server,
      const int64_t refugee_svr_seq, common::ObIArray<RefugeeFileInfo>& refugee_file_array);

  int register_server_recovery_persistence(const RefugeeServerInfo& refugee_server);

  int register_datafile_recovery_persistence(const common::ObZone& zone, const RefugeeFileInfo& refugee_file);

  int update_pre_process_server_status(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& old_rescue_server, const common::ObAddr& rescue_server);

  int update_server_recovery_status(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& rescue_server, const ZoneServerRecoveryProgress cur_progress,
      const ZoneServerRecoveryProgress new_progress);

  int update_datafile_recovery_status(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id, const int64_t file_id,
      const ZoneFileRecoveryStatus old_status, const ZoneFileRecoveryStatus new_status);

  int clean_finished_server_recovery_task(
      const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq);

  int delete_old_server_working_dir(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq);

  int create_new_server_working_dir(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq);
};

class ObServerRecoveryInstance {
  typedef common::hash::ObHashMap<common::ObAddr, ObRecoveryTask*>::iterator task_iterator;
  typedef common::hash::ObHashMap<common::ObAddr, ObRecoveryTask*>::const_iterator const_task_iterator;

public:
  explicit ObServerRecoveryInstance(volatile bool& stop);
  virtual ~ObServerRecoveryInstance();

public:
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, rootserver::ObServerManager* server_mgr,
      rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr,
      rootserver::ObEmptyServerChecker* empty_server_checker, ObZoneRecoveryTaskMgr* zone_recovery_task_mgr);
  int process_daemon_recovery();
  int reset_run_condition();
  int check_single_round_recovery_finished(bool& is_finished);
  int get_server_or_preprocessor(
      const common::ObAddr& server, common::ObAddr& rescue_server, share::ServerPreProceStatus& ret_code);
  int check_can_recover_server(const common::ObAddr& server, bool& can_recover);
  int recover_server_takenover_by_rs(const common::ObAddr& server);
  int inner_recover_server_takenover_by_rs(ObRecoveryTask* task);
  // recover interface for fast recovery
  int delete_old_server_working_dir(ObRecoveryTask* task);
  int create_new_server_working_dir(ObRecoveryTask* task);
  int try_renotify_server_unit_info(ObRecoveryTask* task);
  int modify_server_status(ObRecoveryTask* task);
  int finish_recover_server_takenover_by_rs(ObRecoveryTask* task);

public:
  int on_pre_process_server_reply(
      const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code);
  int on_recover_pg_file_reply(const ObZoneRecoveryTask& task, const int ret_code);
  int update_file_recovery_status(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id, const int64_t file_id,
      ZoneFileRecoveryStatus pre_status, ZoneFileRecoveryStatus cur_status);

private:
  bool check_stop()
  {
    return stop_;
  }
  int main_round();

private:
  // main round sub recovery task
  int discover_new_server_recovery_task();
  int drive_existing_server_recovery_task();
  int clean_finished_server_recovery_task();
  // reload associated task
  int reload_server_recovery_instance();
  int reload_server_recovery_status();
  int fill_server_recovery_task_result(const RefugeeServerInfo& refugee_server_info, ObRecoveryTask* task);
  int reload_file_recovery_status();
  int reload_file_recovery_status_by_server(ObRecoveryTask* task);

private:
  // Machine const
  static const int64_t TASK_MAP_BUCKET_NUM = 2048;
  static const int64_t PG_RECOVERY_TIMEOUT = 10 * 60 * 1000000;         // 10min
  static const int64_t PRE_PROCESS_SERVER_TIMEOUT = 10 * 60 * 1000000;  // 10min
  // datafile recovery status task queue const
  static const int64_t DATAFILE_RECOVERY_STATUS_THREAD_CNT = 1;
  int switch_state(ObRecoveryTask* task, const ZoneServerRecoveryProgress new_state);
  int try_drive(ObRecoveryTask* task);
  int try_pre_process_server(ObRecoveryTask* task);
  int try_recover_pg(ObRecoveryTask* task);
  // pre process server sub routine
  int drive_this_pre_process_server(ObRecoveryTask* task);
  int launch_new_pre_process_server(ObRecoveryTask* task);
  int pick_new_rescue_server(const common::ObZone& zone, common::ObAddr& new_rescue_server);
  int do_launch_new_pre_process_server(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& old_rescue_server, const common::ObAddr& rescue_server,
      const bool allocate_new_rescue_server);
  int inc_rescue_server_count(const common::ObAddr& rescue_server);
  // recover pg sub routine
  int drive_this_recover_pg(ObRecoveryTask* task);
  int launch_new_recover_pg(ObRecoveryTask* task);
  int pick_dest_server_for_refugee(const common::ObZone& zone, const RefugeeFileInfo& refugee_info,
      common::ObAddr& dest_server, uint64_t& dest_unit_id);
  int launch_recover_file_task(ObRecoveryTask* task, const common::ObAddr& dest_server, const int64_t dest_svr_seq,
      const uint64_t dest_unit_id, RefugeeFileInfo& refugee_info);
  int trigger_recover_file_task(ObRecoveryTask* task, RefugeeFileInfo& refugee_info);

private:
  bool inited_;
  bool loaded_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  rootserver::ObServerManager* server_mgr_;
  rootserver::ObZoneManager* zone_mgr_;
  rootserver::ObUnitManager* unit_mgr_;
  rootserver::ObEmptyServerChecker* empty_server_checker_;
  ObZoneRecoveryTaskMgr* recovery_task_mgr_;
  common::ObPooledAllocator<ObRecoveryTask> task_allocator_;
  common::hash::ObHashMap<common::ObAddr, ObRecoveryTask*> task_map_;
  common::hash::ObHashMap<common::ObAddr, int64_t> rescue_server_counter_;
  common::SpinRWLock task_map_lock_;
  common::ObDedupQueue datafile_recovery_status_task_queue_;
  ThisSimpleSeqGenerator seq_generator_;
  volatile bool& stop_;
};

class ObZoneServerRecoveryMachineIdling : public ObThreadIdling {
public:
  explicit ObZoneServerRecoveryMachineIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual ~ObZoneServerRecoveryMachineIdling()
  {}

public:
  virtual int64_t get_idle_interval_us()
  {
    return 3600LL * 1000LL * 1000LL;
  }  // max_idle_time_us
};

class ObZoneServerRecoveryMachine : public ObRsReentrantThread {
public:
  ObZoneServerRecoveryMachine(ObZoneRecoveryTaskMgr& recovery_task_mgr)
      : inited_(false), recovery_task_mgr_(recovery_task_mgr), server_recovery_instance_(stop_), idling_(stop_)
  {}
  virtual ~ObZoneServerRecoveryMachine()
  {}

public:
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

public:
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, rootserver::ObServerManager* server_mgr,
      rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr,
      rootserver::ObEmptyServerChecker* empty_server_checker);
  void stop();
  void wakeup();
  int get_server_or_preprocessor(
      const common::ObAddr& server, common::ObAddr& rescue_server, share::ServerPreProceStatus& ret_code);
  int on_pre_process_server_reply(
      const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code);
  int on_recover_pg_file_reply(const ObZoneRecoveryTask& task, const int ret_code);
  int check_can_recover_server(const common::ObAddr& server, bool& can_recover);
  int recover_server_takenover_by_rs(const common::ObAddr& server);

private:
  int process_daemon_recovery();
  int reset_run_condition();

private:
  static const int64_t INTRA_RUNNING_IDLING_US = 1 * 1000000;  // 1s
private:
  bool inited_;
  ObZoneRecoveryTaskMgr& recovery_task_mgr_;
  ObServerRecoveryInstance server_recovery_instance_;
  ObZoneServerRecoveryMachineIdling idling_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ZONE_SERVER_RECOVERY_MACHINE_H_
