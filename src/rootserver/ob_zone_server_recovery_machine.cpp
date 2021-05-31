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

#define USING_LOG_PREFIX RS
#include "ob_zone_server_recovery_machine.h"
#include "ob_zone_recovery_task_mgr.h"
#include "ob_zone_master_storage_util.h"
#include "ob_server_manager.h"
#include "ob_unit_manager.h"
#include "ob_empty_server_checker.h"
#include "ob_rs_event_history_table_operator.h"
#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace storage;
namespace rootserver {

int64_t UpdateFileRecoveryStatusTaskV2::hash() const
{
  int64_t ret_v = 0;
  int64_t s1 = server_.hash();
  int64_t s2 = dest_server_.hash();
  ret_v = murmurhash(&s1, sizeof(s1), ret_v);
  ret_v = murmurhash(&s2, sizeof(s2), ret_v);
  ret_v = murmurhash(&svr_seq_, sizeof(svr_seq_), ret_v);
  ret_v = murmurhash(&dest_svr_seq_, sizeof(dest_svr_seq_), ret_v);
  ret_v = murmurhash(&tenant_id_, sizeof(tenant_id_), ret_v);
  ret_v = murmurhash(&file_id_, sizeof(file_id_), ret_v);
  return ret_v;
}

bool UpdateFileRecoveryStatusTaskV2::operator==(const IObDedupTask& o) const
{
  const UpdateFileRecoveryStatusTaskV2& that = static_cast<const UpdateFileRecoveryStatusTaskV2&>(o);
  return zone_ == that.zone_ && server_ == that.server_ && svr_seq_ == that.svr_seq_ &&
         dest_server_ == that.dest_server_ && dest_svr_seq_ == that.dest_svr_seq_ && tenant_id_ == that.tenant_id_ &&
         file_id_ == that.file_id_;
}

int64_t UpdateFileRecoveryStatusTaskV2::get_deep_copy_size() const
{
  return sizeof(*this);
}

common::IObDedupTask* UpdateFileRecoveryStatusTaskV2::deep_copy(char* buf, const int64_t buf_size) const
{
  UpdateFileRecoveryStatusTaskV2* task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN("invalid argument", KP(buf), K(buf_size), "need_size", get_deep_copy_size());
  } else {
    task = new (buf) UpdateFileRecoveryStatusTaskV2(zone_,
        server_,
        svr_seq_,
        dest_server_,
        dest_svr_seq_,
        tenant_id_,
        file_id_,
        pre_status_,
        cur_status_,
        is_stopped_,
        host_);
  }
  return task;
}

int UpdateFileRecoveryStatusTaskV2::process()
{
  int ret = OB_SUCCESS;
  if (is_stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerRecoveryInstance stopped", K(ret));
  } else if (OB_FAIL(host_.update_file_recovery_status(zone_,
                 server_,
                 svr_seq_,
                 dest_server_,
                 dest_svr_seq_,
                 tenant_id_,
                 file_id_,
                 pre_status_,
                 cur_status_))) {
    LOG_WARN("fail to update file recovery status", K(ret));
  }
  return ret;
}

int RefugeeServerInfo::init(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
    const common::ObAddr& rescue_server, ZoneServerRecoveryProgress progress)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty() || !server.is_valid() || svr_seq <= 0 || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone), K(server), K(svr_seq), K(rescue_server));
  } else {
    zone_ = zone;
    server_ = server;
    svr_seq_ = svr_seq;
    rescue_server_ = rescue_server;
    rescue_progress_ = progress;
  }
  return ret;
}

int RefugeeServerInfo::assign(const RefugeeServerInfo& that)
{
  int ret = OB_SUCCESS;
  zone_ = that.zone_;
  server_ = that.server_;
  svr_seq_ = that.svr_seq_;
  rescue_server_ = that.rescue_server_;
  rescue_progress_ = that.rescue_progress_;
  return ret;
}

int RefugeeFileInfo::assign(const RefugeeFileInfo& that)
{
  int ret = OB_SUCCESS;
  server_ = that.server_;
  svr_seq_ = that.svr_seq_;
  tenant_id_ = that.tenant_id_;
  file_id_ = that.file_id_;
  dest_server_ = that.dest_server_;
  dest_svr_seq_ = that.dest_svr_seq_;
  dest_unit_id_ = that.dest_unit_id_;
  file_recovery_status_ = that.file_recovery_status_;
  return ret;
}

int RecoveryPersistenceProxy::get_server_recovery_persistence_status(
    common::ObIArray<RefugeeServerInfo>& refugee_server_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::get_server_recovery_persistence_status(refugee_server_array))) {
    LOG_WARN("fail to get server recovery persistence status", K(ret));
  }
  return ret;
}

int RecoveryPersistenceProxy::get_server_datafile_recovery_persistence_status(const common::ObZone& zone,
    const common::ObAddr& refugee_server, const int64_t refugee_svr_seq,
    common::ObIArray<RefugeeFileInfo>& refugee_file_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::get_datafile_recovery_persistence_status(
          zone, refugee_server, refugee_svr_seq, refugee_file_array))) {
    LOG_WARN("fail to get server datafile recovery persistent status", K(ret));
  }
  return ret;
}

int RecoveryPersistenceProxy::register_server_recovery_persistence(const RefugeeServerInfo& refugee_server)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::register_server_recovery_persistence(refugee_server))) {
    LOG_WARN("fail to register server recovery persistence", K(ret), K(refugee_server));
  }
  return ret;
}

int RecoveryPersistenceProxy::register_datafile_recovery_persistence(
    const common::ObZone& zone, const RefugeeFileInfo& refugee_file)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::register_datafile_recovery_persistence(zone, refugee_file))) {
    LOG_WARN("fail to register datafile recovery persistence", K(ret), K(refugee_file));
  }
  return ret;
}

int RecoveryPersistenceProxy::update_pre_process_server_status(const common::ObZone& zone, const common::ObAddr& server,
    const int64_t svr_seq, const common::ObAddr& old_rescue_server, const common::ObAddr& rescue_server)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::update_pre_process_server_status(
          zone, server, svr_seq, old_rescue_server, rescue_server))) {
    LOG_WARN(
        "fail to update pre process server status", K(ret), K(zone), K(server), K(old_rescue_server), K(rescue_server));
  }
  return ret;
}

int RecoveryPersistenceProxy::update_server_recovery_status(const common::ObZone& zone, const common::ObAddr& server,
    const int64_t svr_seq, const common::ObAddr& rescue_server, const ZoneServerRecoveryProgress cur_progress,
    const ZoneServerRecoveryProgress new_progress)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::update_server_recovery_status(
          zone, server, svr_seq, rescue_server, cur_progress, new_progress))) {
    LOG_WARN("fail to update server recovery status",
        K(ret),
        K(zone),
        K(server),
        K(svr_seq),
        K(rescue_server),
        K(cur_progress),
        K(new_progress));
  }
  return ret;
}

int RecoveryPersistenceProxy::update_datafile_recovery_status(const common::ObZone& zone, const common::ObAddr& server,
    const int64_t svr_seq, const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id,
    const int64_t file_id, const ZoneFileRecoveryStatus old_status, const ZoneFileRecoveryStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::update_datafile_recovery_status(
          zone, server, svr_seq, dest_server, dest_svr_seq, tenant_id, file_id, old_status, new_status))) {
    LOG_WARN("fail to update datafile recovery status",
        K(ret),
        K(server),
        K(svr_seq),
        K(dest_server),
        K(dest_svr_seq),
        K(tenant_id),
        K(file_id),
        K(old_status),
        K(new_status));
  }
  return ret;
}

int RecoveryPersistenceProxy::clean_finished_server_recovery_task(
    const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::clean_finished_server_recovery_task(zone, server, svr_seq))) {
    LOG_WARN("fail to clean finished server recovery task", K(ret), K(server));
  }
  return ret;
}

int RecoveryPersistenceProxy::delete_old_server_working_dir(
    const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq)
{
  int ret = OB_SUCCESS;
  ObServerWorkingDir dir(server, ObServerWorkingDir::DirStatus::RECOVERED, svr_seq);
  if (OB_FAIL(ObZoneMasterStorageUtil::delete_server_working_dir(zone, dir))) {
    LOG_WARN("fail to delete server working dir", K(ret), K(zone), K(dir));
  } else {
    LOG_INFO("delete server working dir succeed", K(ret), K(zone), K(dir));
  }
  return ret;
}

int RecoveryPersistenceProxy::create_new_server_working_dir(
    const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rootserver::ObZoneMasterStorageUtil::create_server_working_dir(
          zone, server, svr_seq, 0 /*lease start ts*/, 0 /*lease end ts*/))) {
    LOG_WARN("fail to create server working dir", K(ret), K(zone), K(server));
  } else {
    LOG_INFO("create new server working dir succeed", K(ret), K(zone), K(server));
  }
  return ret;
}

ObRecoveryTask::ObRecoveryTask()
    : zone_(),
      server_(),
      svr_seq_(OB_INVALID_SVR_SEQ),
      rescue_server_(),
      progress_(ZoneServerRecoveryProgress::SRP_MAX),
      refugee_file_infos_(),
      pre_process_server_status_(PreProcessServerStatus::PPSS_INVALID),
      recover_file_task_gen_status_(ZoneRecoverFileTaskGenStatus::RFTGS_INVALID),
      last_drive_ts_(0),
      lock_()
{}

ObRecoveryTask::~ObRecoveryTask()
{}

int ObRecoveryTask::get_refugee_datafile_info(
    const uint64_t tenant_id, const int64_t file_id, const common::ObAddr& dest_server, RefugeeFileInfo*& refugee_info)
{
  int ret = OB_SUCCESS;
  refugee_info = nullptr;
  bool found = false;
  for (int64_t i = 0; !found && i < refugee_file_infos_.count(); ++i) {
    RefugeeFileInfo& this_refugee_info = refugee_file_infos_.at(i);
    if (tenant_id == this_refugee_info.tenant_id_ && file_id == this_refugee_info.file_id_ &&
        dest_server == this_refugee_info.dest_server_) {
      found = true;
      refugee_info = &this_refugee_info;
    } else {
      // go on checking next
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

// ========================== ObServerRecoveryInstance ==========================

ObServerRecoveryInstance::ObServerRecoveryInstance(volatile bool& stop)
    : inited_(false),
      loaded_(false),
      rpc_proxy_(nullptr),
      server_mgr_(nullptr),
      zone_mgr_(nullptr),
      unit_mgr_(nullptr),
      empty_server_checker_(nullptr),
      recovery_task_mgr_(nullptr),
      task_allocator_(),
      task_map_(),
      rescue_server_counter_(),
      task_map_lock_(),
      datafile_recovery_status_task_queue_(),
      seq_generator_(),
      stop_(stop)
{}

ObServerRecoveryInstance::~ObServerRecoveryInstance()
{
  datafile_recovery_status_task_queue_.destroy();
}

int ObServerRecoveryInstance::init(obrpc::ObSrvRpcProxy* rpc_proxy, rootserver::ObServerManager* server_mgr,
    rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr,
    rootserver::ObEmptyServerChecker* empty_server_checker, ObZoneRecoveryTaskMgr* recovery_task_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy || nullptr == server_mgr || nullptr == zone_mgr || nullptr == unit_mgr ||
                         nullptr == empty_server_checker || nullptr == recovery_task_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument", K(ret), KP(server_mgr), KP(zone_mgr), KP(unit_mgr), KP(recovery_task_mgr), KP(rpc_proxy));
  } else if (OB_FAIL(task_map_.create(TASK_MAP_BUCKET_NUM, ObModIds::OB_SERVER_RECOVERY_MACHINE))) {
    LOG_WARN("fail to create task map", K(ret));
  } else if (OB_FAIL(rescue_server_counter_.create(TASK_MAP_BUCKET_NUM, ObModIds::OB_SERVER_RECOVERY_MACHINE))) {
    LOG_WARN("fail to create rescue server counter map", K(ret));
  } else if (OB_FAIL(datafile_recovery_status_task_queue_.init(DATAFILE_RECOVERY_STATUS_THREAD_CNT))) {
    LOG_WARN("fail to init datafile recovery status task queue", K(ret));
  } else {
    datafile_recovery_status_task_queue_.set_label(ObModIds::OB_SERVER_RECOVERY_MACHINE);
    rpc_proxy_ = rpc_proxy;
    server_mgr_ = server_mgr;
    zone_mgr_ = zone_mgr;
    unit_mgr_ = unit_mgr;
    empty_server_checker_ = empty_server_checker;
    recovery_task_mgr_ = recovery_task_mgr;
    loaded_ = false;
    inited_ = true;
  }
  return ret;
}

int ObServerRecoveryInstance::check_can_recover_server(const common::ObAddr& server, bool& can_recover)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    can_recover = false;
    ObRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    int tmp_ret = task_map_.get_refactored(server, task);
    if (OB_SUCCESS == tmp_ret) {
      if (task->lock_.try_rdlock()) {
        // use try rdlock to avoid waiting lock to long, so as to make receive_hb effectively
        if (ZoneServerRecoveryProgress::SRP_SERVER_RECOVERY_FINISHED == task->progress_) {
          LOG_INFO("can recover server", K(server));
          can_recover = true;
        } else {
          can_recover = false;
        }
        task->lock_.unlock();  // unlock
      } else {
        LOG_INFO("please try check can recover again", K(server));
        can_recover = false;  // lock failed, try the next time
      }
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // fast recovery task not exist, cannot recover immediately
      can_recover = false;
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to check can recover server", K(ret), K(server));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::delete_old_server_working_dir(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy proxy;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(proxy.delete_old_server_working_dir(task->zone_, task->server_, task->svr_seq_))) {
    LOG_WARN("fail to delete old server working dir",
        K(ret),
        "zone",
        task->zone_,
        "server",
        task->server_,
        "svr_seq",
        task->svr_seq_);
  }
  return ret;
}

int ObServerRecoveryInstance::create_new_server_working_dir(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy proxy;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(proxy.create_new_server_working_dir(task->zone_, task->server_, task->svr_seq_))) {
    LOG_WARN("fail to delete old server working dir",
        K(ret),
        "zone",
        task->zone_,
        "server",
        task->server_,
        "svr_seq",
        task->svr_seq_);
  }
  return ret;
}

int ObServerRecoveryInstance::try_renotify_server_unit_info(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_UNLIKELY(nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else {
    const common::ObAddr& server = task->server_;
    const common::ObZone& zone = task->zone_;
    if (OB_FAIL(unit_mgr_->try_renotify_server_unit_info_for_fast_recovery(server, zone))) {
      LOG_WARN("fail to try renotify server unit info for fast recovery", K(ret), K(server));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::modify_server_status(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else {
    const common::ObAddr& server = task->server_;
    const common::ObZone& zone = task->zone_;
    if (OB_FAIL(server_mgr_->try_modify_recovery_server_takenover_by_rs(server, zone))) {
      LOG_WARN("fail to try recover server takenover by rs", K(ret), K(server));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::finish_recover_server_takenover_by_rs(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(task_map_.erase_refactored(task->server_))) {
    LOG_WARN("fail to erase refactored", K(ret), "server", task->server_);
  } else {
    ROOTSERVICE_EVENT_ADD("server_fast_recovery",
        "recovery_back",
        "server",
        task->server_,
        "zone",
        task->zone_,
        "rescue_server",
        task->rescue_server_);
    task_allocator_.free(task);
    task = nullptr;
  }
  return ret;
}

int ObServerRecoveryInstance::inner_recover_server_takenover_by_rs(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(delete_old_server_working_dir(task))) {
    LOG_WARN("fail to delete old server working dir", K(ret));
  } else if (OB_FAIL(create_new_server_working_dir(task))) {
    LOG_WARN("fail to create new server working dir", K(ret));
  } else if (OB_FAIL(try_renotify_server_unit_info(task))) {
    LOG_WARN("fail to notify server unit info", K(ret));
  } else if (OB_FAIL(modify_server_status(task))) {
    LOG_WARN("fail to modify server status", K(ret));
  } else if (OB_FAIL(finish_recover_server_takenover_by_rs(task))) {
    LOG_WARN("fail to finish recover server takenover by rs", K(ret));
  }
  return ret;
}

int ObServerRecoveryInstance::recover_server_takenover_by_rs(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    /* all the operations below are executed under the protection of the task lock
     * 1 recycle the old server working dir
     * 2 create new server working dir
     * 3 nofity permanent unit information on he new server working dir.
     * 4 modify the corresponding server_manager and all_server status
     * 5 delete the fast recovery task
     */

    ObRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    int tmp_ret = task_map_.get_refactored(server, task);
    if (OB_SUCCESS == tmp_ret) {
      SpinWLockGuard inner_guard(task->lock_);
      if (OB_FAIL(inner_recover_server_takenover_by_rs(task))) {
        LOG_WARN("fail to inner recover server takenover by rs", K(ret), K(server));
      }
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // fast recovery task not exists
      LOG_INFO("server fast recovery task not exist", K(server));
    } else {
      ret = tmp_ret;
      LOG_WARN("get task from map failed", K(ret), K(server));
    }
  }
  // clear the recover status whatever succeed or not
  if (OB_UNLIKELY(nullptr == server_mgr_)) {
    // bypass
  } else {
    server_mgr_->clear_in_recovery_server_takenover_by_rs(server);
  }
  return ret;
}

int ObServerRecoveryInstance::main_round()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = discover_new_server_recovery_task())) {
      LOG_WARN("fail to discover new server recovery task", K(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = drive_existing_server_recovery_task())) {
      LOG_WARN("fail to drive existing server recovery task", K(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = clean_finished_server_recovery_task())) {
      LOG_WARN("fail to clean finished server recovery task", K(tmp_ret));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::process_daemon_recovery()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryInstance not init", K(ret));
  } else {
    if (!loaded_) {
      if (OB_FAIL(reload_server_recovery_instance())) {
        LOG_WARN("fail to reload server recovery instance", K(ret));
      } else {
        loaded_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(main_round())) {
        LOG_WARN("fail to run server recovery instance main round", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::reset_run_condition()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    for (task_iterator iter = task_map_.begin(); iter != task_map_.end(); ++iter) {
      ObRecoveryTask* task = iter->second;
      if (nullptr != task) {
        task_allocator_.free(task);  // destruct is invoked in the allocator
        task = nullptr;
      }
    }
    task_map_.reuse();
    loaded_ = false;
  }
  return ret;
}

int ObServerRecoveryInstance::reload_server_recovery_instance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance not init", K(ret));
  } else if (OB_FAIL(reset_run_condition())) {
    LOG_WARN("fail to reset run condition", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    if (OB_FAIL(reload_server_recovery_status())) {
      LOG_WARN("fail to reload server recovery status", K(ret));
    } else if (OB_FAIL(reload_file_recovery_status())) {
      LOG_WARN("fail to reload file recovery status", K(ret));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::reload_server_recovery_status()
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy persis_info_proxy;
  common::ObArray<RefugeeServerInfo> refugee_server_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance not init", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery instance stopped", K(ret));
  } else if (0 != task_map_.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task map should be empty before reload", K(ret));
  } else if (OB_FAIL(persis_info_proxy.get_server_recovery_persistence_status(refugee_server_array))) {
    LOG_WARN("fail to get server recovery persistence status", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < refugee_server_array.count(); ++i) {
      const RefugeeServerInfo& refugee_server = refugee_server_array.at(i);
      ObRecoveryTask* task_ptr = task_allocator_.alloc();
      if (OB_UNLIKELY(nullptr == task_ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (OB_FAIL(fill_server_recovery_task_result(refugee_server, task_ptr))) {
        LOG_WARN("fail to fill server recovery task result", K(ret));
      } else if (OB_FAIL(task_map_.set_refactored(task_ptr->server_, task_ptr))) {
        LOG_WARN("fail to set refactored", K(ret));
      } else {
        int64_t cnt = -1;
        int tmp_ret = rescue_server_counter_.get_refactored(task_ptr->rescue_server_, cnt);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          int32_t overwrite = 0;
          if (OB_FAIL(rescue_server_counter_.set_refactored(task_ptr->rescue_server_, 1 /* set to 1 */, overwrite))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        } else if (OB_SUCCESS == tmp_ret) {
          int32_t overwrite = 1;
          if (OB_FAIL(rescue_server_counter_.set_refactored(task_ptr->rescue_server_, cnt + 1, overwrite))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        } else {
          ret = tmp_ret;
          LOG_WARN("fail to get from hash map", K(ret));
        }
      }
      if (OB_FAIL(ret) && nullptr != task_ptr) {
        task_allocator_.free(task_ptr);
        task_ptr = nullptr;
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::fill_server_recovery_task_result(
    const RefugeeServerInfo& refugee_server_info, ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else {
    task->zone_ = refugee_server_info.zone_;
    task->server_ = refugee_server_info.server_;
    task->svr_seq_ = refugee_server_info.svr_seq_;
    task->rescue_server_ = refugee_server_info.rescue_server_;
    task->progress_ = refugee_server_info.rescue_progress_;
  }
  return ret;
}

int ObServerRecoveryInstance::reload_file_recovery_status_by_server(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy persis_info_proxy;
  common::ObArray<RefugeeFileInfo> refugee_file_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance not init", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery instance stopped", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
    /* bypass, no need to reload file recovery status,
     * since task progress not in pg recovery
     */
  } else if (OB_FAIL(persis_info_proxy.get_server_datafile_recovery_persistence_status(
                 task->zone_, task->server_, task->svr_seq_, refugee_file_array))) {
    LOG_WARN("fail to get server datafile recovery persistence status", K(ret), K(*task));
  } else if (OB_FAIL(task->refugee_file_infos_.assign(refugee_file_array))) {
    LOG_WARN("fail to assign refugee infos array", K(ret));
  } else if (task->refugee_file_infos_.count() > 0) {
    task->recover_file_task_gen_status_ = ZoneRecoverFileTaskGenStatus::RFTGS_TASK_GENERATED;
  }
  return ret;
}

int ObServerRecoveryInstance::reload_file_recovery_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance not init", K(ret));
  } else {
    for (task_iterator iter = task_map_.begin(); OB_SUCC(ret) && iter != task_map_.end(); ++iter) {
      ObRecoveryTask* task = iter->second;
      if (OB_UNLIKELY(nullptr == task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task ptr is null", K(ret));
      } else if (OB_FAIL(reload_file_recovery_status_by_server(task))) {
        LOG_WARN("fail to reload file recovery status by server", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::discover_new_server_recovery_task()
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> servers_takenover_by_rs;
  RecoveryPersistenceProxy persis_info_proxy;
  common::ObZone all_zone;  // when empty, it means all zone
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance not init", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine is stopped", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone server tracer", K(ret), KP(server_mgr_));
  } else if (OB_FAIL(server_mgr_->get_servers_takenover_by_rs(all_zone, servers_takenover_by_rs))) {
    LOG_WARN("fail to get servers taken over by rs", K(ret));
  } else if (servers_takenover_by_rs.count() <= 0) {
    // bypass, since there is no server taken over by rs
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_takenover_by_rs.count(); ++i) {
      const common::ObAddr& this_server = servers_takenover_by_rs.at(i);
      uint64_t this_svr_seq = OB_INVALID_ID;
      common::ObZone server_zone;
      common::ObAddr rescue_server;  // invalid rescue server
      ObServerWorkingDir svr_working_dir(this_server, ObServerWorkingDir::DirStatus::NORMAL, this_svr_seq);
      RefugeeServerInfo refugee_server_info;
      ObRecoveryTask* new_task = nullptr;
      bool does_task_exist = false;
      {
        SpinRLockGuard read_guard(task_map_lock_);
        const ObRecoveryTask* const* task_ptr = task_map_.get(this_server);
        does_task_exist = (nullptr != task_ptr);
      }
      if (does_task_exist) {
        // task already exist, no need to process
      } else if (OB_FAIL(server_mgr_->get_server_id(all_zone, this_server, this_svr_seq))) {
        LOG_WARN("get server id fail", K(ret), K(all_zone), K(this_server));
      } else if (FALSE_IT(svr_working_dir.svr_seq_ = this_svr_seq)) {
      } else if (OB_UNLIKELY(nullptr == (new_task = task_allocator_.alloc()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (OB_FAIL(server_mgr_->get_server_zone(this_server, server_zone))) {
        LOG_WARN("fail to get server zone", K(ret), K(this_server));
      } else if (OB_FAIL(pick_new_rescue_server(server_zone, rescue_server))) {
        LOG_WARN("fail to pick new rescue server", K(ret));
      } else if (OB_FAIL(refugee_server_info.init(server_zone,
                     this_server,
                     this_svr_seq,
                     rescue_server,
                     ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER))) {
        LOG_WARN("fail to init refugee server info", K(ret), K(this_server), K(rescue_server));
      } else if (OB_FAIL(ObZoneMasterStorageUtil::try_recovering_server_working_dir(server_zone, svr_working_dir))) {
        LOG_WARN("fail to try recovering server working dir", K(ret), K(server_zone), K(svr_working_dir));
      } else if (OB_FAIL(persis_info_proxy.register_server_recovery_persistence(refugee_server_info))) {
        LOG_WARN("fail to register server recovery persistence", K(ret));
      } else {
        (void)inc_rescue_server_count(rescue_server);
        new_task->zone_ = server_zone;
        new_task->server_ = this_server;
        new_task->svr_seq_ = this_svr_seq;
        new_task->rescue_server_ = rescue_server;
        new_task->progress_ = ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER;
        new_task->last_drive_ts_ = 0;
        // hash map is thread safe, no need to acquire lock here
        if (OB_FAIL(task_map_.set_refactored(new_task->server_, new_task))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
      }
      if (OB_SUCC(ret) && !does_task_exist) {
        if (OB_ISNULL(new_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("new task is null", K(ret), KP(new_task));
        } else {
          LOG_INFO("submit server recovery task", K(*new_task));
          ROOTSERVICE_EVENT_ADD("server_fast_recovery",
              "register_new_task",
              "server",
              new_task->server_,
              "zone",
              new_task->zone_,
              "rescue_server",
              new_task->rescue_server_);
        }
      } else if (OB_FAIL(ret) && nullptr != new_task) {
        SpinWLockGuard write_guard(task_map_lock_);
        task_map_.erase_refactored(this_server);
        task_allocator_.free(new_task);
        new_task = nullptr;
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::drive_existing_server_recovery_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_INFO("server mgr ptr null", K(ret));
  } else {
    SpinRLockGuard guard(task_map_lock_);
    if (task_map_.size() > 0) {
      for (task_iterator iter = task_map_.begin(); !stop_ && iter != task_map_.end(); ++iter) {
        ObRecoveryTask* task = iter->second;
        int tmp_ret = OB_SUCCESS;
        if (nullptr == task) {
          // bypass
        } else {
          common::ObZone zone;
          share::ObZoneInfo zone_info;
          {
            SpinRLockGuard inner_guard(task->lock_);
            zone = task->zone_;
          }
          if (OB_UNLIKELY(zone.is_empty())) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("zone is empty", K(tmp_ret), K(zone));
          } else if (OB_UNLIKELY(nullptr == zone_mgr_)) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("zone mgr ptr is null", K(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = zone_mgr_->get_zone(zone, zone_info))) {
            LOG_WARN("fail to get zone info", K(tmp_ret), K(zone));
          } else if (zone_info.recovery_status_.value_ == static_cast<int64_t>(ObZoneInfo::RECOVERY_STATUS_SUSPEND)) {
            // bypass, since this zone has suspend recovery
          } else if (task->progress_ < ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER ||
                     task->progress_ > ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY) {
            // do nothing
          } else if (OB_SUCCESS != (tmp_ret = try_drive(task))) {
            LOG_WARN("fail to drive task status", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::clean_finished_server_recovery_task()
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy persis_info_proxy;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == empty_server_checker_ || nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty server checker or unit mgr ptr is null", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("ServerRecoveryInstance stopped", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    if (task_map_.size() > 0) {
      // ignore the ret code when executing the for loop
      // to diminish the interference of the last failed task
      for (task_iterator iter = task_map_.begin(); !stop_ && iter != task_map_.end(); ++iter) {
        bool server_empty = false;
        share::ObServerStatus server_status;
        ObRecoveryTask* task = iter->second;
        if (nullptr == task) {
          // bypass
        } else {
          int tmp_ret = OB_SUCCESS;
          common::ObZone zone;
          share::ObZoneInfo zone_info;
          {
            SpinRLockGuard inner_guard(task->lock_);
            zone = task->zone_;
          }
          if (OB_UNLIKELY(zone.is_empty())) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("zone is empty", K(tmp_ret), K(zone));
          } else if (OB_UNLIKELY(nullptr == zone_mgr_)) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("zone mgr ptr is null", K(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = zone_mgr_->get_zone(zone, zone_info))) {
            LOG_WARN("fail to get zone info", K(tmp_ret), K(zone));
          } else if (zone_info.recovery_status_.value_ == static_cast<int64_t>(ObZoneInfo::RECOVERY_STATUS_SUSPEND)) {
            // bypass, since this zone has suspend recovery
          } else if (ZoneServerRecoveryProgress::SRP_SERVER_RECOVERY_FINISHED != task->progress_) {
            // bypass
          } else if (FALSE_IT(empty_server_checker_->notify_check())) {
            // shall never by here
          } else if (OB_FAIL(unit_mgr_->check_server_empty(iter->first, server_empty))) {
            LOG_WARN("fail to check server empty", K(ret), "server", iter->first);
          } else if (!server_empty) {
            LOG_INFO("server not empty, may have unit on", "server", iter->first);
          } else if (FALSE_IT((void)server_mgr_->set_force_stop_hb(iter->first, true /*stop*/))) {
            // shall never be here
          } else if (FALSE_IT(ret = server_mgr_->get_server_status(iter->first, server_status))) {
            // shall never be here
          } else if (OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret) {
            bool need_clean_result = true;
            if (OB_SUCCESS == ret) {
              if (ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS != server_status.admin_status_) {
                ret = OB_STATE_NOT_MATCH;
                LOG_WARN("server status not match", K(ret), K(server_status));
              } else if (server_status.with_partition_) {
                need_clean_result = false;
              } else if (OB_FAIL(server_mgr_->finish_server_recovery(iter->first))) {
                LOG_WARN("fail to finish server recovery", K(ret), "server", iter->first);
              }
            } else {  // OB_ENTRY_NOT_EXIST
              // rewrite to succ since the previous invocation
              // may delete the server from server manager.
              ret = OB_SUCCESS;
              need_clean_result = true;
            }

            if (OB_SUCC(ret) && need_clean_result) {
              if (OB_FAIL(persis_info_proxy.clean_finished_server_recovery_task(
                      task->zone_, task->server_, task->svr_seq_))) {
                LOG_WARN("fail to clean finished server recovery task", K(ret));
              } else if (OB_FAIL(task_map_.erase_refactored(iter->first))) {
                LOG_WARN("fail to erase item from hash map", K(ret), "server", iter->first);
              } else {
                ROOTSERVICE_EVENT_ADD("server_fast_recovery",
                    "finish_recovery",
                    "server",
                    task->server_,
                    "zone",
                    task->zone_,
                    "rescue_server",
                    task->rescue_server_);
                task_allocator_.free(task);
                task = nullptr;
              }
            }
          } else {
            LOG_WARN("fail to get server status", K(ret), "server", iter->first);
          }
        }
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::drive_this_pre_process_server(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stop", K(ret));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr is null", K(ret), KP(server_mgr_));
  } else if (PreProcessServerStatus::PPSS_INVALID == task->pre_process_server_status_) {
    const common::ObAddr& rescue_server = task->rescue_server_;
    bool is_active = false;
    if (OB_FAIL(server_mgr_->check_server_active(rescue_server, is_active))) {
      LOG_WARN("fail to check server active", K(ret), K(rescue_server));
    } else if (!is_active) {
      task->last_drive_ts_ = 0;  // retry immediately
    } else {
    }  // bypass, since rescue server is still active
  } else if (PreProcessServerStatus::PPSS_SUCCEED == task->pre_process_server_status_) {
    (void)switch_state(task, ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY);
  } else if (PreProcessServerStatus::PPSS_FAILED == task->pre_process_server_status_) {
    task->last_drive_ts_ = 0;  // retry immediately
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("zone server recovery instance prepro server status error", K(ret), "task", *task);
  }
  return ret;
}

int ObServerRecoveryInstance::pick_new_rescue_server(const common::ObZone& zone, common::ObAddr& new_rescue_server)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> server_array;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr null", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine is stopped", K(ret));
  } else if (OB_FAIL(server_mgr_->get_active_server_array(zone, server_array))) {
    LOG_WARN("fail to get servers taken over by rs", K(ret));
  } else if (server_array.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no active server found to execute split log task", K(ret));
  } else {
    common::ObAddr server_with_min_cnt;
    int64_t min_cnt = INT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_array.count(); ++i) {
      const common::ObAddr& this_server = server_array.at(i);
      int64_t this_cnt = -1;
      int tmp_ret = rescue_server_counter_.get_refactored(this_server, this_cnt);
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        server_with_min_cnt = this_server;
        min_cnt = 0;
      } else if (OB_SUCCESS == tmp_ret) {
        if (this_cnt < min_cnt) {
          min_cnt = this_cnt;
          server_with_min_cnt = this_server;
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to get refactored", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else {
      new_rescue_server = server_with_min_cnt;
      LOG_INFO("pick new rescue server", K(new_rescue_server));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::inc_rescue_server_count(const common::ObAddr& rescue_server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(!rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rescue_server));
  } else {
    int64_t cnt = -1;
    int tmp_ret = rescue_server_counter_.get_refactored(rescue_server, cnt);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      int32_t overwrite = 0;
      if (OB_FAIL(rescue_server_counter_.set_refactored(rescue_server, 1 /*VALUE*/, overwrite))) {
        LOG_WARN("fail to set refactored", K(tmp_ret));
      }
    } else if (OB_SUCCESS == tmp_ret) {
      int32_t overwrite = 1;
      if (OB_FAIL(rescue_server_counter_.set_refactored(rescue_server, cnt + 1, overwrite))) {
        LOG_WARN("fail to set refactored", K(tmp_ret));
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get from hash map", K(ret));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::do_launch_new_pre_process_server(const common::ObZone& zone, const common::ObAddr& server,
    const int64_t svr_seq, const common::ObAddr& old_rescue_server, const common::ObAddr& rescue_server,
    const bool is_new_rescue_server)
{
  int ret = OB_SUCCESS;
  obrpc::ObPreProcessServerArg arg;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty() || !server.is_valid() || svr_seq <= 0 || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone), K(server), K(svr_seq), K(rescue_server));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy ptr is nullptr", K(ret));
  } else if (OB_FAIL(arg.init(server, svr_seq, rescue_server))) {
    LOG_WARN("fail to init rpc arg", K(ret));
  } else {
    if (is_new_rescue_server) {
      RecoveryPersistenceProxy persis_proxy;
      if (OB_FAIL(
              persis_proxy.update_pre_process_server_status(zone, server, svr_seq, old_rescue_server, rescue_server))) {
        LOG_WARN("fail to update pre process server status", K(ret));
      }
    } else {
    }  // no need to update when it is not new server

    if (OB_SUCC(ret)) {
      (void)rpc_proxy_->to(rescue_server).pre_process_server_status(arg);
      if (is_new_rescue_server) {
        int tmp_ret = inc_rescue_server_count(rescue_server);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to in rescue server count", K(tmp_ret));
        }
      }
    }
    LOG_INFO("preprocess server", K(arg), K(ret));
  }
  return ret;
}

int ObServerRecoveryInstance::launch_new_pre_process_server(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone server tracer ptr is null", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stop", K(ret));
  } else {
    bool allocate_new_rescue_server = false;
    // check is a new rescue server need to be allocated
    bool is_active = false;
    common::ObAddr& rescue_server = task->rescue_server_;
    common::ObAddr new_rescue_server;
    if (!rescue_server.is_valid()) {
      allocate_new_rescue_server = true;
    } else if (OB_FAIL(server_mgr_->check_server_active(rescue_server, is_active))) {
      LOG_WARN("fail to check server active", K(ret), K(rescue_server));
    } else if (!is_active) {
      allocate_new_rescue_server = true;
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!allocate_new_rescue_server) {
      new_rescue_server = rescue_server;
    } else {
      if (OB_FAIL(pick_new_rescue_server(task->zone_, new_rescue_server))) {
        LOG_WARN("fail to pick new rescue server", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_launch_new_pre_process_server(task->zone_,
              task->server_,
              task->svr_seq_,
              rescue_server,
              new_rescue_server,
              allocate_new_rescue_server))) {
        LOG_WARN("fail to do launch pre process server",
            K(ret),
            "server",
            task->server_,
            "rescue_server",
            new_rescue_server,
            K(allocate_new_rescue_server));
      } else {
        task->rescue_server_ = new_rescue_server;
        task->pre_process_server_status_ = PreProcessServerStatus::PPSS_INVALID;
        task->last_drive_ts_ = common::ObTimeUtility::current_time();
        LOG_INFO("launch new pre process server status succeed", "task", *task);
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::try_pre_process_server(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stop", K(ret));
  } else {
    SpinWLockGuard guard(task->lock_);
    if (ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER != task->progress_) {
      // bypass, task progress may be pushed forward by other routine
    } else if (task->last_drive_ts_ + PRE_PROCESS_SERVER_TIMEOUT > ObTimeUtility::current_time()) {
      if (OB_FAIL(drive_this_pre_process_server(task))) {
        LOG_WARN("fail to drive this pre process server", K(ret));
      }
    } else {
      if (OB_FAIL(launch_new_pre_process_server(task))) {
        LOG_WARN("fail to launch new pre process server", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::try_drive(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stop", K(ret));
  } else {
    ZoneServerRecoveryProgress recovery_progress = task->progress_;
    switch (recovery_progress) {
      case ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER:
        if (OB_FAIL(try_pre_process_server(task))) {
          LOG_WARN("fail to try pre process server", K(ret));
        }
        break;
      case ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY:
        if (OB_FAIL(try_recover_pg(task))) {
          LOG_WARN("fail to try recover pg", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected server recovery status", K(ret), K(recovery_progress));
        break;
    }
  }
  return ret;
}

int ObServerRecoveryInstance::update_file_recovery_status(const common::ObZone& zone, const common::ObAddr& server,
    const int64_t svr_seq, const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id,
    const int64_t file_id, ZoneFileRecoveryStatus pre_status, ZoneFileRecoveryStatus cur_status)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid()
                         // dest_server may be an invalid argument when dropping tenant
                         || OB_INVALID_ID == tenant_id || file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerRecoveryInstance stopped", K(ret));
  } else {
    RecoveryPersistenceProxy persis_info_proxy;
    ObRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(server, task))) {
      LOG_WARN("fail to get from map", K(ret));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(server));
    } else {
      LOG_INFO("update file status", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
      SpinWLockGuard guard(task->lock_);
      RefugeeFileInfo* refugee_info = nullptr;
      if (ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
        LOG_INFO("state not match, maybe previous task response",
            K(server),
            K(dest_server),
            K(tenant_id),
            K(file_id),
            K(pre_status),
            K(cur_status));
      } else if (OB_FAIL(task->get_refugee_datafile_info(tenant_id, file_id, dest_server, refugee_info))) {
        LOG_WARN("fail to get refugee info", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
      } else if (OB_UNLIKELY(nullptr == refugee_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("refugee info ptr is null", K(ret));
      } else if (pre_status != refugee_info->file_recovery_status_) {
        LOG_INFO("state not match, maybe pushed by others",
            K(server),
            K(dest_server),
            K(tenant_id),
            K(file_id),
            K(pre_status),
            K(cur_status));
      } else if (OB_FAIL(persis_info_proxy.update_datafile_recovery_status(
                     zone, server, svr_seq, dest_server, dest_svr_seq, tenant_id, file_id, pre_status, cur_status))) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("state not match", K(ret), K(zone), K(server), K(dest_server), K(tenant_id), K(file_id));
      }
      if (OB_SUCC(ret)) {
        refugee_info->file_recovery_status_ = cur_status;
      } else {
        refugee_info->file_recovery_status_ = ZoneFileRecoveryStatus::FRS_FAILED;
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::try_recover_pg(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stopped", K(ret));
  } else {
    SpinWLockGuard guard(task->lock_);
    if (ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
      // bypass, task progress may be pushed forward by other routine
    } else if (ZoneRecoverFileTaskGenStatus::RFTGS_TASK_GENERATED == task->recover_file_task_gen_status_) {
      if (OB_FAIL(drive_this_recover_pg(task))) {
        LOG_WARN("fail to drive this recover pg", K(ret));
      }
    } else if (ZoneRecoverFileTaskGenStatus::RFTGS_INVALID == task->recover_file_task_gen_status_) {
      if (OB_FAIL(launch_new_recover_pg(task))) {
        LOG_WARN("fail to launch new recover pg", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task recover file task gen status unexpected", K(ret), "task", *task);
    }
  }
  return ret;
}

int ObServerRecoveryInstance::pick_dest_server_for_refugee(const common::ObZone& zone,
    const RefugeeFileInfo& refugee_info, common::ObAddr& dest_server, uint64_t& dest_unit_id)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnitInfo> unit_infos;
  common::ObArray<share::ObUnitInfo> available_unit_infos;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stopped", K(ret));
  } else if (OB_UNLIKELY(nullptr == unit_mgr_ || nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr or server mgr ptr is null", K(ret), KP(unit_mgr_), KP(server_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(refugee_info.tenant_id_, unit_infos))) {
    LOG_WARN("fail to get active unit infos by tenant", K(ret), "tenant_id", refugee_info.tenant_id_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_infos.count(); ++i) {
      bool is_active = false;
      const common::ObAddr& this_server = unit_infos.at(i).unit_.server_;
      if (OB_UNLIKELY(!this_server.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this server addr invalid", K(ret), "unit_info", unit_infos.at(i));
      } else if (zone != unit_infos.at(i).unit_.zone_) {
        // bypass
      } else if (OB_FAIL(server_mgr_->check_server_active(this_server, is_active))) {
        LOG_WARN("check server active", K(ret), K(this_server));
      } else if (!is_active) {
        // bypass
      } else if (OB_FAIL(available_unit_infos.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (available_unit_infos.count() <= 0) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("fail to push back", K(ret), K(refugee_info), K(unit_infos));
      } else {
        const int64_t seq_num = seq_generator_.next_seq();
        const int64_t idx = seq_num % available_unit_infos.count();
        dest_server = available_unit_infos.at(idx).unit_.server_;
        dest_unit_id = available_unit_infos.at(idx).unit_.unit_id_;
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::launch_recover_file_task(ObRecoveryTask* task, const common::ObAddr& dest_server,
    const int64_t dest_svr_seq, const uint64_t dest_unit_id, RefugeeFileInfo& refugee_info)
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy persis_info_proxy;
  RefugeeFileInfo refugee_info_for_write;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(!dest_server.is_valid() || dest_svr_seq <= 0 || OB_INVALID_ID == dest_unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_server), K(dest_svr_seq), K(dest_unit_id));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stopped", K(ret));
  } else if (OB_FAIL(refugee_info_for_write.assign(refugee_info))) {
    LOG_WARN("fail to assign refugee info for write", K(ret));
  } else {
    refugee_info_for_write.dest_server_ = dest_server;
    refugee_info_for_write.dest_svr_seq_ = dest_svr_seq;
    refugee_info_for_write.dest_unit_id_ = dest_unit_id;
    refugee_info_for_write.file_recovery_status_ = ZoneFileRecoveryStatus::FRS_MAX;
    ret = persis_info_proxy.register_datafile_recovery_persistence(task->zone_, refugee_info_for_write);
    if (OB_SUCCESS == ret) {
      refugee_info.dest_server_ = dest_server;
      refugee_info.dest_svr_seq_ = dest_svr_seq;
      refugee_info.dest_unit_id_ = dest_unit_id;
      refugee_info.file_recovery_status_ = ZoneFileRecoveryStatus::FRS_MAX;
      ObZoneRecoveryTask recovery_task;
      int64_t task_cnt = 0;
      if (OB_FAIL(recovery_task.build(refugee_info.server_,
              refugee_info.svr_seq_,
              refugee_info.dest_server_,
              refugee_info.dest_svr_seq_,
              refugee_info.tenant_id_,
              refugee_info.file_id_,
              refugee_info.dest_unit_id_))) {
        LOG_WARN("fail to build recovery task", K(ret));
      } else if (OB_FAIL(recovery_task_mgr_->add_task(recovery_task, task_cnt))) {
        LOG_WARN("fail to add task", K(ret));
      } else {
        refugee_info.file_recovery_status_ = ZoneFileRecoveryStatus::FRS_IN_PROGRESS;
      }
    } else if (OB_ALREADY_DONE == ret) {
      ret = OB_SUCCESS;
      refugee_info.file_recovery_status_ = ZoneFileRecoveryStatus::FRS_SUCCEED;
    } else {
      LOG_WARN("fail to register datafile recovery persistence",
          K(ret),
          "zone",
          task->zone_,
          "refugee_info",
          refugee_info_for_write);
    }
  }
  return ret;
}

int ObServerRecoveryInstance::trigger_recover_file_task(ObRecoveryTask* task, RefugeeFileInfo& refugee_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stop", K(ret));
  } else {
    ObZoneRecoveryTask recovery_task;
    int64_t task_cnt = 0;
    if (OB_FAIL(recovery_task.build(refugee_info.server_,
            refugee_info.svr_seq_,
            refugee_info.dest_server_,
            refugee_info.dest_svr_seq_,
            refugee_info.tenant_id_,
            refugee_info.file_id_,
            refugee_info.dest_unit_id_))) {
      LOG_WARN("fail to build recovery task info", K(ret));
    } else if (OB_FAIL(recovery_task_mgr_->add_task(recovery_task, task_cnt))) {
      LOG_WARN("fail to add task", K(ret));
    } else {
      refugee_info.file_recovery_status_ = ZoneFileRecoveryStatus::FRS_IN_PROGRESS;
    }
  }
  return ret;
}

int ObServerRecoveryInstance::drive_this_recover_pg(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_ || nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr or schema service  ptr is null", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stop", K(ret));
  } else {
    bool finished = true;
    int64_t processed_task_cnt = 0;
    const int64_t PROCESSED_TASK_LIMIT = 100;
    for (int64_t i = 0;
         OB_SUCC(ret) && i < task->refugee_file_infos_.count() && processed_task_cnt < PROCESSED_TASK_LIMIT;
         ++i) {
      RefugeeFileInfo& this_refugee_info = task->refugee_file_infos_.at(i);
      if (ZoneFileRecoveryStatus::FRS_IN_PROGRESS == this_refugee_info.file_recovery_status_) {
        finished = false;
      } else if (ZoneFileRecoveryStatus::FRS_SUCCEED == this_refugee_info.file_recovery_status_) {
        // good, this refugee finished
      } else if (ZoneFileRecoveryStatus::FRS_MAX == this_refugee_info.file_recovery_status_ ||
                 ZoneFileRecoveryStatus::FRS_FAILED == this_refugee_info.file_recovery_status_) {
        bool need_distribute_dest = false;
        finished = false;
        processed_task_cnt++;
        share::ObServerStatus server_status;
        share::ObUnitInfo unit_info;
        common::ObAddr dest_server;
        uint64_t dest_server_id = OB_INVALID_ID;
        uint64_t dest_unit_id = OB_INVALID_ID;
        bool is_exist = false;
        if (!this_refugee_info.dest_server_.is_valid() || OB_INVALID_ID == this_refugee_info.dest_unit_id_) {
          need_distribute_dest = true;
        } else if (OB_FAIL(server_mgr_->is_server_exist(this_refugee_info.dest_server_, is_exist))) {
          LOG_WARN("fail to check is server exist", K(ret), "server", this_refugee_info.dest_server_);
        } else if (!is_exist) {
          LOG_INFO("dest server not exist, need redistribute", "server", this_refugee_info.dest_server_);
          need_distribute_dest = true;
        } else if (OB_FAIL(server_mgr_->get_server_status(this_refugee_info.dest_server_, server_status))) {
          LOG_WARN("fail to check server active", K(ret), "server", this_refugee_info.dest_server_);
        } else if (server_status.is_permanent_offline() || server_status.is_temporary_offline()) {
          need_distribute_dest = true;
        } else {
          const uint64_t unit_id = this_refugee_info.dest_unit_id_;
          int tmp_ret = unit_mgr_->get_unit_info_by_id(unit_id, unit_info);
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            need_distribute_dest = true;
          } else if (OB_SUCCESS == tmp_ret) {
            if (unit_info.unit_.server_ != this_refugee_info.dest_server_ &&
                unit_info.unit_.migrate_from_server_ != this_refugee_info.dest_server_) {
              need_distribute_dest = true;
            } else {
              need_distribute_dest = false;
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get unit info by id", K(ret), K(unit_id), "tenant_id", this_refugee_info.tenant_id_);
          }
        }
        if (OB_FAIL(ret)) {
          // failed
        } else if (need_distribute_dest) {
          if (OB_FAIL(pick_dest_server_for_refugee(task->zone_, this_refugee_info, dest_server, dest_unit_id))) {
            LOG_WARN("fail to pick dest server for refugee", K(ret));
          } else if (OB_FAIL(server_mgr_->get_server_id(ObZone(), dest_server, dest_server_id))) {
            LOG_WARN("fail to get dest server id", K(ret), K(dest_server));
          } else if (OB_UNLIKELY(
                         !dest_server.is_valid() || OB_INVALID_ID == dest_unit_id || OB_INVALID_ID == dest_server_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dest server is invalid", K(ret), K(dest_server), K(dest_server_id), K(dest_unit_id));
          } else if (OB_FAIL(launch_recover_file_task(
                         task, dest_server, (int64_t)dest_server_id, dest_unit_id, this_refugee_info))) {
            LOG_WARN("fail to launch recover file task", K(ret));
          }
        } else {
          if (OB_FAIL(trigger_recover_file_task(task, this_refugee_info))) {
            LOG_WARN("fail to trigger recover file task", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!finished) {
      // not finish yet
    } else {
      void(switch_state(task, ZoneServerRecoveryProgress::SRP_SERVER_RECOVERY_FINISHED));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::launch_new_recover_pg(ObRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy persis_info_proxy;
  common::ObArray<RefugeeFileInfo> refugee_file_array;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("server recovery machine stopped", K(ret));
  } else if (ZoneRecoverFileTaskGenStatus::RFTGS_TASK_GENERATED == task->recover_file_task_gen_status_) {
    // bypass
  } else {
    if (OB_FAIL(persis_info_proxy.get_server_datafile_recovery_persistence_status(
            task->zone_, task->server_, task->svr_seq_, refugee_file_array))) {
      LOG_WARN("fail to get server datafile recovery persistence status", K(ret));
    } else if (OB_FAIL(task->refugee_file_infos_.assign(refugee_file_array))) {
      LOG_WARN("fail to assign refugee infos array", K(ret));
    } else {
      task->recover_file_task_gen_status_ = ZoneRecoverFileTaskGenStatus::RFTGS_TASK_GENERATED;
    }
  }
  return ret;
}

int ObServerRecoveryInstance::get_server_or_preprocessor(
    const common::ObAddr& server, common::ObAddr& rescue_server, share::ServerPreProceStatus& ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("ZoneServerRecoveryMachine stopped", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    ObRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    int tmp_ret = task_map_.get_refactored(server, task);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // recovery task of this server not exist
      ret_code = SPPS_SERVER_NOT_EXIST;
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_UNLIKELY(nullptr == task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task ptr is null", K(ret), K(server));
      } else {
        SpinRLockGuard item_guard(task->lock_);
        if (task->progress_ < ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER) {
          ret_code = SPPS_PRE_PROCE_NOT_START;
          rescue_server.reset();  // not start yet,reset rescue_server
        } else if (ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER == task->progress_) {
          ret_code = SPPS_IN_PRE_PROCE;
          rescue_server = task->rescue_server_;
        } else {
          ret_code = SPPS_PRE_PROCE_FINISHED;
          rescue_server = task->rescue_server_;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get from task map", K(ret), K(server));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::on_pre_process_server_reply(
    const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("ZoneServerRecoveryMachine stopped", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(rescue_server));
  } else {
    int64_t cnt = 0;
    LOG_INFO("on pre process server status", K(server), K(rescue_server), K(ret_code));
    ObRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(server, task))) {
      LOG_WARN("fail to get from map", K(ret), K(server));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(server));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      if (ZoneServerRecoveryProgress::SRP_PRE_PROCESS_SERVER != task->progress_) {
        LOG_INFO("state not match, maybe previous task response", K(server), K(rescue_server));
      } else if (rescue_server != task->rescue_server_) {
        LOG_INFO("rescue server not match",
            K(server),
            "local_rescue_server",
            task->rescue_server_,
            "incoming_rescue_server",
            rescue_server);
      } else if (OB_SUCCESS == ret_code) {
        task->pre_process_server_status_ = PreProcessServerStatus::PPSS_SUCCEED;
      } else {
        task->pre_process_server_status_ = PreProcessServerStatus::PPSS_FAILED;
      }
    }

    int tmp_ret = rescue_server_counter_.get_refactored(rescue_server, cnt);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // ignore
    } else if (OB_SUCCESS == tmp_ret) {
      int32_t overwrite = 1;
      if (OB_SUCCESS !=
          (tmp_ret = rescue_server_counter_.set_refactored(rescue_server, (cnt > 0 ? cnt - 1 : 0), overwrite))) {
        LOG_WARN("fail to set refactored", K(tmp_ret));
      }
    } else {
      LOG_WARN("fail to get from hash map", K(tmp_ret));
    }
  }
  return ret;
}

int ObServerRecoveryInstance::on_recover_pg_file_reply(const ObZoneRecoveryTask& this_task, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (check_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerRecoveryInstance stopped", K(ret));
  } else {
    const common::ObAddr& server = this_task.get_src();
    const common::ObAddr& dest_server = this_task.get_dest();
    const uint64_t tenant_id = this_task.get_tenant_id();
    const int64_t file_id = this_task.get_file_id();
    LOG_INFO("on recover pg file succeed", K(server), K(dest_server), K(tenant_id), K(file_id), K(ret_code));
    ObRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(server, task))) {
      LOG_WARN("fail to get from map", K(ret), K(server));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(server));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      RefugeeFileInfo* refugee_info = nullptr;
      if (ZoneServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
        LOG_INFO("state not match, maybe previous task response", K(server));
      } else if (OB_FAIL(task->get_refugee_datafile_info(tenant_id, file_id, dest_server, refugee_info))) {
        LOG_WARN("fail to get refugee info", K(ret), K(tenant_id), K(file_id), K(dest_server));
      } else if (OB_UNLIKELY(nullptr == refugee_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("refugee info ptr is null", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
      } else if (ZoneFileRecoveryStatus::FRS_MAX == refugee_info->file_recovery_status_ ||
                 ZoneFileRecoveryStatus::FRS_FAILED == refugee_info->file_recovery_status_ ||
                 ZoneFileRecoveryStatus::FRS_SUCCEED == refugee_info->file_recovery_status_) {
        // ignore this reply, state not match
        LOG_INFO("recover pg file state not match",
            K(server),
            K(dest_server),
            K(tenant_id),
            K(file_id),
            K(ret_code),
            "recovery_status",
            refugee_info->file_recovery_status_);
      } else if (ZoneFileRecoveryStatus::FRS_IN_PROGRESS == refugee_info->file_recovery_status_) {
        if (ret_code != OB_SUCCESS) {
          refugee_info->file_recovery_status_ = ZoneFileRecoveryStatus::FRS_FAILED;
        } else {
          UpdateFileRecoveryStatusTaskV2 this_task(task->zone_,
              refugee_info->server_,
              refugee_info->svr_seq_,
              refugee_info->dest_server_,
              refugee_info->dest_svr_seq_,
              refugee_info->tenant_id_,
              refugee_info->file_id_,
              ZoneFileRecoveryStatus::FRS_IN_PROGRESS,
              ZoneFileRecoveryStatus::FRS_SUCCEED,
              stop_,
              *this);
          if (OB_FAIL(datafile_recovery_status_task_queue_.add_task(this_task))) {
            LOG_WARN("fail to add task", K(ret), "refugee", *refugee_info);
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("refugee info unexpected", K(ret), "refugee_info", *refugee_info);
      }
    }
  }
  return ret;
}

int ObServerRecoveryInstance::switch_state(ObRecoveryTask* task, const ZoneServerRecoveryProgress new_progress)
{
  int ret = OB_SUCCESS;
  RecoveryPersistenceProxy persis_info_proxy;

  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(OB_FAIL(persis_info_proxy.update_server_recovery_status(
                 task->zone_, task->server_, task->svr_seq_, task->rescue_server_, task->progress_, new_progress)))) {
    LOG_WARN("fail to switch server recovery status", K(ret), "server", task->server_);
  } else {
    ZoneServerRecoveryProgress cur_progress = task->progress_;
    LOG_INFO("task switch state", K(ret), K(cur_progress), K(new_progress), "server", task->server_);
    task->progress_ = new_progress;
    // set last drive ts to 0,to make the state machine to be driven immediately
    task->last_drive_ts_ = 0;
    ROOTSERVICE_EVENT_ADD("server_fast_recovery",
        "switch state",
        "server",
        task->server_,
        "zone",
        task->zone_,
        "cur_progress",
        cur_progress,
        "next_progress",
        new_progress);
  }
  return ret;
}

// ======================= ObZoneServerRecoveryMachine =======================

int ObZoneServerRecoveryMachine::init(obrpc::ObSrvRpcProxy* rpc_proxy, rootserver::ObServerManager* server_mgr,
    rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr,
    rootserver::ObEmptyServerChecker* empty_server_checker)
{
  int ret = OB_SUCCESS;
  static const int64_t thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy || nullptr == server_mgr || nullptr == zone_mgr || nullptr == unit_mgr ||
                         nullptr == empty_server_checker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(rpc_proxy));
  } else if (OB_FAIL(server_recovery_instance_.init(
                 rpc_proxy, server_mgr, zone_mgr, unit_mgr, empty_server_checker, &recovery_task_mgr_))) {
    LOG_WARN("fail to init server recovery instance", K(ret));
  } else if (OB_FAIL(create(thread_cnt, "ZoneServerRecoveryMachine"))) {
    LOG_WARN("fail to create thread", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObZoneServerRecoveryMachine::stop()
{
  if (!inited_) {
    int ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!stop_) {
    ObReentrantThread::stop();
    idling_.wakeup();
  }
}

void ObZoneServerRecoveryMachine::wakeup()
{
  idling_.wakeup();
}

int ObZoneServerRecoveryMachine::on_recover_pg_file_reply(const ObZoneRecoveryTask& this_task, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ZoneServerRecoveryMachine not init", K(ret));
  } else if (OB_FAIL(server_recovery_instance_.on_recover_pg_file_reply(this_task, ret_code))) {
    LOG_WARN("fail to call on recover pg file reply", K(ret));
  }
  idling_.wakeup();
  return ret;
}

int ObZoneServerRecoveryMachine::on_pre_process_server_reply(
    const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ZoneServerRecoveryMachine not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(rescue_server));
  } else if (OB_FAIL(server_recovery_instance_.on_pre_process_server_reply(server, rescue_server, ret_code))) {
    LOG_WARN("fail to call on pre process server status", K(ret));
  }
  idling_.wakeup();
  return ret;
}

int ObZoneServerRecoveryMachine::check_can_recover_server(const common::ObAddr& server, bool& can_recover)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ZoneServerRecoveryMachine not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(server_recovery_instance_.check_can_recover_server(server, can_recover))) {
    LOG_WARN("fail to check can recover server", K(ret), K(server));
  }
  return ret;
}

int ObZoneServerRecoveryMachine::recover_server_takenover_by_rs(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ZoneServerRecoveryMachine not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_recovery_instance_.recover_server_takenover_by_rs(server))) {
    LOG_WARN("fail to recover server takenover by rs", K(server));
  }
  return ret;
}

int ObZoneServerRecoveryMachine::get_server_or_preprocessor(
    const common::ObAddr& server, common::ObAddr& rescue_server, share::ServerPreProceStatus& ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ZoneServerRecoveryMachine not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server));
  } else if (OB_FAIL(server_recovery_instance_.get_server_or_preprocessor(server, rescue_server, ret_code))) {
    LOG_WARN("fail to get server or preprocessor", K(ret));
  }
  return ret;
}

int ObZoneServerRecoveryMachine::process_daemon_recovery()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_recovery_instance_.process_daemon_recovery())) {
    LOG_WARN("fail to process daemon recovery", K(ret));
  } else {
    idling_.idle(INTRA_RUNNING_IDLING_US);
  }
  return ret;
}

int ObZoneServerRecoveryMachine::reset_run_condition()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_recovery_instance_.reset_run_condition())) {
    LOG_WARN("fail to reset run condition", K(ret));
  }
  return ret;
}

void ObZoneServerRecoveryMachine::run3()
{
  LOG_INFO("server recovery machine starts");
  if (OB_UNLIKELY(!inited_)) {
    int ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryInstance state not match", K(ret));
  } else {
    while (!stop_) {
      if (OB_FILE_SYSTEM_ROUTER.is_exist_shared_storage_zone()) {
        int tmp_ret = process_daemon_recovery();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to process daemon recovery", K(tmp_ret));
        }
      } else {
        idling_.idle(INTRA_RUNNING_IDLING_US);
      }
    }
    if (OB_SUCCESS != reset_run_condition()) {
      LOG_WARN("fail to reset run condition");
    }
  }
  LOG_INFO("server recovery machine exits");
}

}  // end namespace rootserver
}  // end namespace oceanbase
