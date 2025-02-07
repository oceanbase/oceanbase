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

#define USING_LOG_PREFIX SERVER


#include "ob_rpc_processor_simple.h"
#include "share/ob_server_blacklist.h"
#include "share/cache/ob_cache_name_define.h"
#include "rootserver/ob_root_service.h"
#include "sql/ob_sql.h"
#include "observer/mysql/ob_diag.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "src/share/stat/ob_opt_stat_monitor_manager.h"
// for 4.0
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/sequence/ob_sequence_cache.h"
#include "logservice/ob_log_service.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "storage/high_availability/ob_transfer_service.h" // ObTransferService
#include "sql/udr/ob_udr_mgr.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_scheduler.h"
#include "rootserver/restore/ob_clone_scheduler.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_controller.h"
#endif
#include "sql/plan_cache/ob_ps_cache.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"
#include "rootserver/ob_admin_drtask_util.h"  // ObAdminDRTaskUtil
#include "rootserver/ob_split_partition_helper.h"
#include "sql/session/ob_sess_info_verify.h"
#include "observer/table/ttl/ob_ttl_service.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "rootserver/standby/ob_recovery_ls_service.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "rootserver/ob_admin_drtask_util.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/shared_storage/ob_ss_micro_cache.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#endif
#include "share/object_storage/ob_device_config_mgr.h"
#include "rootserver/restore/ob_restore_service.h"
#include "rootserver/backup/ob_archive_scheduler_service.h"
#include "storage/high_availability/ob_rebuild_service.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace memtable;
using namespace share;
using namespace sql;
using namespace obmysql;
using namespace omt;
using namespace logservice::coordinator;

namespace rpc
{
void response_rpc_error_packet(ObRequest* req, int ret)
{
  if (NULL != req) {
    observer::ObErrorP p(ret);
    p.set_ob_request(*req);
    p.run();
  }
}
};
namespace observer
{

int ObErrorP::process()
{
  if (ret_ == OB_SUCCESS) {
    LOG_ERROR_RET(ret_, "should not return success in error packet", K(ret_));
  }
  return ret_;
}

int ObErrorP::deserialize()
{
  return OB_SUCCESS;
}

int ObRpcCheckBackupSchuedulerWorkingP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.root_service_));
  } else {
    ret = gctx_.root_service_->check_backup_scheduler_working(result_);
  }
  return ret;
}

int ObRpcLSCancelReplicaP::process()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  uint64_t tenant_id = arg_.get_tenant_id();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (tenant_id != MTL_ID()) {
    if (OB_FAIL(guard.switch_to(tenant_id))) {
      LOG_WARN("failed to switch to tenant", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObStorageHACancelDagNetUtils::cancel_task(arg_.get_ls_id(), arg_.get_task_id()))) {
      LOG_WARN("failed to cancel task", K(ret), K(arg_));
    }
  }
  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "cancel storage ha task failed",
                     "tenant_id", tenant_id,
                     "ls_id", arg_.get_ls_id().id(),
                     "task_id", arg_.get_task_id(),
                     "result", ret);
  }
  return ret;
}

int ObRpcLSMigrateReplicaP::process()
{
  return observer::ObService::do_migrate_ls_replica(arg_);
}

int ObRpcLSAddReplicaP::process()
{
  return observer::ObService::do_add_ls_replica(arg_);
}

int ObRpcLSTypeTransformP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (tenant_id != MTL_ID()) {
    if (OB_FAIL(guard.switch_to(tenant_id))) {
      LOG_WARN("failed to switch to tenant", K(ret), K(tenant_id));
    }
  }
  ObCurTraceId::set(arg_.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "ls_type_transform start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "dest", arg_.src_.get_server());
    LOG_INFO("start do ls type transform", K(arg_));

    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(arg_));
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->transform_member(arg_))) {
      LOG_WARN("failed to transform member", K(ret), K(arg_));
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "ls_type_transform failed", "tenant_id",
        arg_.tenant_id_, "ls_id", arg_.ls_id_.id(), "result", ret);
  }
  return ret;
}

int ObRpcLSRemovePaxosReplicaP::process()
{
  return observer::ObService::do_remove_ls_paxos_replica(arg_);
}

int ObRpcLSRemoveNonPaxosReplicaP::process()
{
  return observer::ObService::do_remove_ls_nonpaxos_replica(arg_);
}

int ObRpcLSModifyPaxosReplicaNumberP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  ObCurTraceId::set(arg_.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "modify_paxos_replica_number start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "orig_paxos_replica_number", arg_.orig_paxos_replica_number_, "new_paxos_replica_number", arg_.new_paxos_replica_number_,
                     "member_list", arg_.member_list_);
    LOG_INFO("start do modify paxos replica number", K(arg_));

    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), K(arg_));
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->modify_paxos_replica_number(arg_))) {
      LOG_WARN("failed to remove paxos member", KR(ret), K(arg_));
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "modify_paxos_replica_number failed", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(), "result", ret);
  }
  return ret;
}

int ObRpcLSCheckDRTaskExistP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  ObStorageHAService *storage_ha_service = nullptr;
  bool is_exist = false;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;


  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcLSCheckDRTaskExistP::process", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls service should not be null", K(ret), K(tenant_id));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      if (OB_LS_NOT_EXIST == ret) {
        is_exist = false;
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "get ls failed", K(ret), K(arg_));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls should not be NULL", K(ret), KP(ls));
    } else if (OB_FAIL(ls->get_ls_migration_handler()->check_task_exist(arg_.task_id_, is_exist))) {
      LOG_WARN("failed to check ls migration handler task exist", K(ret), K(arg_));
    } else if (is_exist) {
      //do nothing
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->check_task_exist(arg_.task_id_, is_exist))) {
      LOG_WARN("failed to check ls remove member handler task exist", K(ret), K(arg_));
    } else if (is_exist) {
      //do nothing
    } else {
      //1.check transfer handler
      //2.check ls restore handler
    }

    if (OB_SUCC(ret)) {
      result_ = is_exist;
    }
  }
  return ret;
}

int ObAdminDRTaskP::process()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  LOG_INFO("start to handle ls replica task triggered by ob_admin", K_(arg));
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else if (OB_FAIL(rootserver::ObAdminDRTaskUtil::handle_obadmin_command(arg_))) {
    LOG_WARN("fail to handle ob admin command", KR(ret), K_(arg));
  }
  LOG_INFO("finish handle ls replica task triggered by ob_admin", K_(arg));
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int ObRpcAddArbP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle handle;
  ObLS *ls = nullptr;
  uint64_t tenant_id = arg_.tenant_id_;
  share::ObLSID ls_id = arg_.ls_id_;
  ObLSService *ls_svr = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcSetMemberListP::process tenant not match", K(ret), K(tenant_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
    COMMON_LOG(WARN, "get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "ls should not be null", K(ret));
  } else if (OB_FAIL(ls->add_arbitration_member(arg_.arb_member_, arg_.timeout_us_))) {
    COMMON_LOG(WARN, "failed to add_arbitration_member", KR(ret), K(arg_));
  } else {
    COMMON_LOG(INFO, "success to add_arbitration_member", K_(arg));
  }
  result_.set_result(ret);
  return ret;
}

int ObRpcRemoveArbP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle handle;
  ObLS *ls = nullptr;
  uint64_t tenant_id = arg_.tenant_id_;
  share::ObLSID ls_id = arg_.ls_id_;
  ObLSService *ls_svr = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcSetMemberListP::process tenant not match", K(ret), K(tenant_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
    COMMON_LOG(WARN, "get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "ls should not be null", K(ret));
  } else if (OB_FAIL(ls->remove_arbitration_member(arg_.arb_member_, arg_.timeout_us_))) {
    COMMON_LOG(WARN, "failed to remove_arbitration_member", KR(ret), K(arg_));
  } else {
    COMMON_LOG(INFO, "success to remove_arbitration_member", K_(arg));
  }
  result_.set_result(ret);
  return ret;
}
#endif

int ObRpcSetConfigP::process()
{
  LOG_INFO("process set config", K(arg_));
  GCONF.add_extra_config(arg_.ptr());
  GCTX.config_mgr_->reload_config();
  return OB_SUCCESS;
}

int ObRpcGetConfigP::process()
{
  return OB_SUCCESS;
}

int ObRpcSetTenantConfigP::process()
{
  LOG_INFO("process set tenant config", K(arg_));
  OTC_MGR.add_extra_config(arg_);
  OTC_MGR.notify_tenant_config_changed(arg_.tenant_id_);
  return OB_SUCCESS;
}

int ObRpcNotifyTenantServerUnitResourceP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTenantNodeBalancer::get_instance().handle_notify_unit_resource(arg_))) {
    LOG_WARN("fail to handle_notify_unit_resource", K(ret), K_(arg));
  }
  return ret;
}

int ObRpcNotifySwitchLeaderP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.get_tenant_id();
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of GCTX.omt_", KR(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id) {
    // only refresh the status of specified tenant
    if (GCTX.omt_->is_available_tenant(tenant_id)) {
      MTL_SWITCH(tenant_id) {
        ObLeaderCoordinator* coordinator = MTL(ObLeaderCoordinator*);
        if (OB_ISNULL(coordinator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected null of leader coordinator", KR(ret), K_(arg));
        } else if (OB_FAIL(coordinator->schedule_refresh_priority_task())) {
          LOG_WARN("failed to schedule refresh priority task", KR(ret), K_(arg));
        }
      }
    }
  } else {
    // refresh the status of all tenants
    common::ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get_mtl_tenant_ids", KR(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < tenant_ids.size(); i++) {
        if (GCTX.omt_->is_available_tenant(tenant_ids.at(i))) {
          MTL_SWITCH(tenant_ids.at(i)) {
            ObLeaderCoordinator* coordinator = MTL(ObLeaderCoordinator*);
            if (OB_ISNULL(coordinator)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("unexpected null of leader coordinator", KR(ret), K_(arg));
            } else if (OB_FAIL(coordinator->schedule_refresh_priority_task())) {
              LOG_WARN("failed to schedule refresh priority task", KR(ret), K_(arg));
            }
          }
        }
        if (OB_FAIL(ret)) {
          tmp_ret = ret;
        }
      }
      ret = tmp_ret;
    }
  }
  LOG_INFO("receive notify switch leader", KR(ret), K_(arg));
  return ret;
}

int ObCheckFrozenVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_frozen_scn(arg_);
  }
  return ret;
}

int ObGetMinSSTableSchemaVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_min_sstable_schema_version(arg_, result_);
  }
  return ret;
}

int ObInitTenantConfigP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->init_tenant_config(arg_, result_);
  }
  return ret;
}

int ObGetLeaderLocationsP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->get_leader_locations(arg_, result_);
  }
  return ret;
}

int ObCalcColumnChecksumRequestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->calc_column_checksum_request(arg_, result_);
  }
  return ret;
}

int ObRpcBuildDDLSingleReplicaRequestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->build_ddl_single_replica_request(arg_, result_);
  }
  return ret;
}

int ObRpcCheckandCancelDDLComplementDagP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    bool is_dag_exist = true;
    ret = gctx_.ob_service_->check_and_cancel_ddl_complement_data_dag(arg_, is_dag_exist);
    result_ = is_dag_exist;
  }
  return ret;
}

int ObRpcCheckandCancelDeleteLobMetaRowDagP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    bool is_dag_exist = true;
    ret = gctx_.ob_service_->check_and_cancel_delete_lob_meta_row_dag(arg_, is_dag_exist);
    result_ = is_dag_exist;
  }
  return ret;
}

int ObRpcBuildSplitTabletDataStartRequestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->build_split_tablet_data_start_request(arg_, result_);
  }
  return ret;
}

int ObRpcBuildSplitTabletDataFinishRequestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->build_split_tablet_data_finish_request(arg_, result_);
  }
  return ret;
}

int ObRpcFreezeSplitSrcTabletP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    const int64_t abs_timeout_us = nullptr == rpc_pkt_ ? 0 : get_receive_timestamp() + rpc_pkt_->get_timeout();
    ret = rootserver::ObSplitPartitionHelper::freeze_split_src_tablet(arg_, result_, abs_timeout_us);
  }
  return ret;
}

int ObRpcFetchSplitTabletInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    const int64_t abs_timeout_us = nullptr == rpc_pkt_ ? 0 : get_receive_timestamp() + rpc_pkt_->get_timeout();
    ret = gctx_.ob_service_->fetch_split_tablet_info(arg_, result_, abs_timeout_us);
  }
  return ret;
}

int ObRpcFetchSysLSP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), KR(ret));
  } else {
    ret = gctx_.ob_service_->fetch_sys_ls(result_);
  }
  return ret;
}

int ObRpcBroadcastRsListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->broadcast_rs_list(arg_);
  }
  return ret;
}

int ObRpcBackupLSCleanP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->delete_backup_ls_task(arg_);
  }
  return ret;
}

int ObRpcNotifyArchiveP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->notify_archive(arg_);
  }
  return ret;
}

int ObRpcBackupLSDataP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->backup_ls_data(arg_);
  }
  return ret;
}

int ObRpcBackupLSComplLOGP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->backup_completing_log(arg_);
  }
  return ret;
}

int ObRpcBackupBuildIndexP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->backup_build_index(arg_);
  }
  return ret;
}

int ObRpcBackupMetaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->backup_meta(arg_);
  }
  return ret;
}

int ObRpcBackupFuseTabletMetaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->backup_fuse_tablet_meta(arg_);
  }
  return ret;
}

int ObRpcCheckBackupTaskExistP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    bool is_exist = false;
    ret = gctx_.ob_service_->check_backup_task_exist(arg_, is_exist);
    result_ = is_exist;
  }
  return ret;
}

int ObRpcGetRoleP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.root_service_));
  } else {
    ret = gctx_.ob_service_->get_root_server_status(result_);
  }
  return ret;
}

int ObRpcMinorFreezeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->minor_freeze(arg_, result_);
  }
  return ret;
}

int ObRpcTabletMajorFreezeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->tablet_major_freeze(arg_, result_);
  }
  return ret;
}

int ObRpcCheckSchemaVersionElapsedP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_schema_version_elapsed(arg_, result_);
  }
  return ret;
}

int ObRpcCheckMemtableCntP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_memtable_cnt(arg_, result_);
  }
  return ret;
}

int ObRpcCheckMediumCompactionInfoListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_medium_compaction_info_list_cnt(arg_, result_);
  }
  return ret;
}

int ObRpcPrepareTabletSplitTaskRangesP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->prepare_tablet_split_task_ranges(arg_, result_);
  }
  return ret;
}

int ObRpcCheckCtxCreateTimestampElapsedP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_modify_time_elapsed(arg_, result_);
  }
  return ret;
}

int ObRpcDDLCheckTabletMergeStatusP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_ddl_tablet_merge_status(arg_, result_);
  }
  return ret;
}

int ObRpcSwitchLeaderP::process()
{
  int ret = OB_SUCCESS;
  // if (OB_ISNULL(gctx_.ob_service_)) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  // } else {
  //   logservice::ObLogService *log_service = nullptr;
  //   if (OB_FAIL(guard.switch_to(arg_.tenant_id_))) {
  //     LOG_WARN("switch tenant failed", KR(ret), K(arg_.tenant_id_));
  //   } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_ERROR("get invalid log service", K(gctx_.ob_service_), K(ret), K(arg_));
  //   } else if (OB_FAIL(log_service->change_leader_to(share::ObLSID(arg_.ls_id_), arg_.dest_server_))) {
  //     LOG_ERROR("call log service change_leader_to failed", K(ret), K(arg_));
  //   } else {
  //     LOG_INFO("successfully call log service change_leader_to", K(ret), K(arg_));
  //   }
  // }
  LOG_ERROR("this message is not supported now", K(ret), K(arg_));
  return ret;
}

int ObRpcBatchSwitchRsLeaderP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), KR(ret));
  } else {
    ret = gctx_.ob_service_->batch_switch_rs_leader(arg_);
  }
  return ret;
}

int ObRpcGetPartitionCountP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_partition_count(result_))) {
    LOG_WARN("failed to get partition count", K(ret));
  }
  return ret;
}

int ObRpcSwitchSchemaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->switch_schema(arg_, result_);
  }
  return ret;
}

int ObRpcCreateTenantUserLSP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      rootserver::ObPrimaryLSService* primary_ls_service = MTL(rootserver::ObPrimaryLSService*);
      if (OB_ISNULL(primary_ls_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary ls service is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(primary_ls_service->create_ls_for_create_tenant())) {
        LOG_WARN("failed to create ls for create tenant", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;

}

int ObRpcRefreshMemStatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->refresh_memory_stat();
  }
  return ret;
}

int ObRpcWashMemFragmentationP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->wash_memory_fragmentation();
  }
  return ret;
}

int ObRpcBootstrapP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->bootstrap(arg_);
  }
  return ret;
}

int ObRpcCheckServerEmptyWithResultP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_server_empty_with_result(arg_, result_);
  }
  return ret;
}

int ObRpcCheckServerEmptyP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_server_empty(arg_, result_);
  }
  return ret;
}

int ObRpcPrepareServerForAddingServerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KP(gctx_.ob_service_));
  } else if (OB_FAIL(gctx_.ob_service_->prepare_server_for_adding_server(arg_, result_))) {
    LOG_WARN("fail to call prepare_server_for_adding_server", KR(ret), K(arg_));
  } else {}
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_CHECK_SERVER_MACHINE_ERROR);
int ObRpcCheckServerMachineStatusP::process()
{
  int ret = OB_SUCCESS;
  ObServerHealthStatus server_health_status;
  if (OB_FAIL(ObHeartbeatHandler::check_disk_status(server_health_status))) {
    LOG_WARN("fail to check disk status", KR(ret));
  } else if (OB_FAIL(result_.init(server_health_status))) {
    LOG_WARN("fail to init result", KR(ret), K(server_health_status));
  }
  if (OB_SUCC(ret) && ERRSIM_CHECK_SERVER_MACHINE_ERROR) {
    (void) server_health_status.init(ObServerHealthStatus::DATA_DISK_STATUS_ERROR);
    (void) result_.init(server_health_status);
    LOG_WARN("ERRSIM_CHECK_SERVER_MACHINE_ERROR is opened", KR(ret), K(arg_), K(result_));
  }
  LOG_INFO("check server machine status", KR(ret), K(arg_), K(result_));
  return ret;
}

int ObRpcCheckDeploymentModeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_deployment_mode_match(arg_, result_);
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObRpcWaitMasterKeyInSyncP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->wait_master_key_in_sync(arg_);
  }
  return ret;
}
#endif

int ObRpcSyncAutoincValueP::process()
{
  return ObAutoincrementService::get_instance().refresh_sync_value(arg_);
}

int ObRpcClearAutoincCacheP::process()
{
  return ObAutoincrementService::get_instance().clear_autoinc_cache(arg_);
}

int ObDumpMemtableP::process()
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "start dump memtable process", K(arg_));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_.tablet_id_), K(arg_.ls_id_), K(arg_.tenant_id_));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      storage::ObLS *ls = nullptr;
      ObLSService* ls_svr = nullptr;
      ObTabletHandle tablet_handle;
      ObLSHandle ls_handle;
      common::ObSEArray<ObTableHandleV2, 7> tables_handle;

      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService is null", KR(ret), K(arg_.tenant_id_));
      } else if (OB_FAIL(ls_svr->get_ls(ObLSID(arg_.ls_id_),
                                        ls_handle,
                                        ObLSGetMod::OBSERVER_MOD))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get log_stream's ls_handle", KR(ret), K(arg_.tenant_id_), K(arg_.ls_id_));
        } else {
          LOG_TRACE("log stream not exist in this tenant", KR(ret), K(arg_.tenant_id_), K(arg_.ls_id_));
        }
      } else if (FALSE_IT(ls = ls_handle.get_ls())) {
      } else if (OB_ISNULL(ls->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_tablet_svr is null", KR(ret), K(arg_.tenant_id_), K(arg_.tablet_id_));
      } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(arg_.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("get tablet failed", KR(ret), K(arg_.tenant_id_), K(arg_.tablet_id_));
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid tablet handle", K(ret), K(tablet_handle));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_all_memtables_from_memtable_mgr(tables_handle))) {
        LOG_WARN("failed to get all memtable", K(ret), KPC(tablet_handle.get_obj()));
      } else {
        ObITabletMemtable *tablet_memtable = nullptr;
        mkdir("/tmp/dump_memtable/", S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.count(); i++) {
          if (OB_FAIL(tables_handle.at(i).get_tablet_memtable(tablet_memtable))) {
            SERVER_LOG(WARN, "fail to get tablet memtables", K(ret));
          } else {
            TRANS_LOG(INFO, "start dump memtable", K(*tablet_memtable), K(arg_));
            tablet_memtable->dump2text("/tmp/dump_memtable/memtable.txt");
          }
        }
      }
    }
  }
  TRANS_LOG(INFO, "finish dump memtable process", K(ret), K(arg_));

  return ret;
}

int ObDumpTxDataMemtableP::process()
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "start dump memtable process", K(arg_));

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_.ls_id_), K(arg_.tenant_id_));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      storage::ObLS *ls = nullptr;
      ObLSService* ls_svr = nullptr;
      ObLSHandle ls_handle;
      ObMemtableMgrHandle memtable_mgr_handle;
      ObIMemtableMgr *memtable_mgr = nullptr;
      common::ObSEArray<ObTableHandleV2, 2> tables_handle;

      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService is null", KR(ret), K(arg_.tenant_id_));
      } else if (OB_FAIL(ls_svr->get_ls(ObLSID(arg_.ls_id_),
                                        ls_handle,
                                        ObLSGetMod::OBSERVER_MOD))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get log_stream's ls_handle", KR(ret), K(arg_.tenant_id_), K(arg_.ls_id_));
        } else {
          LOG_TRACE("log stream not exist in this tenant", KR(ret), K(arg_.tenant_id_), K(arg_.ls_id_));
        }
      } else if (FALSE_IT(ls = ls_handle.get_ls())) {
      } else if (OB_ISNULL(ls->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_tablet_svr is null", KR(ret), K(arg_.tenant_id_));
      } else if (OB_FAIL(ls->get_tablet_svr()->get_tx_data_memtable_mgr(memtable_mgr_handle))) {
      } else if (OB_UNLIKELY(!memtable_mgr_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid memtable mgr handle", K(ret));
      } else if (OB_ISNULL(memtable_mgr = memtable_mgr_handle.get_memtable_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "memtable mgr is null", K(ret));
      } else if (OB_FAIL(memtable_mgr->get_all_memtables(tables_handle))) {
        SERVER_LOG(WARN, "fail to get all memtables for log stream", K(ret));
      } else {
        ObTxDataMemtable *tx_data_mt;
        mkdir("/tmp/dump_tx_data_memtable/", S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.count(); i++) {
          if (OB_FAIL(tables_handle.at(i).get_tx_data_memtable(tx_data_mt))) {
            SERVER_LOG(WARN, "fail to get tx data memtables", K(ret));
          } else {
            TRANS_LOG(INFO, "start dump tx data memtable", KPC(tx_data_mt), K(arg_));
            tx_data_mt->dump2text("/tmp/dump_tx_data_memtable/tx_data_memtable.txt");
          }
        }
      }
    }
  }

  TRANS_LOG(INFO, "finish dump tx data memtable process", K(ret), K(arg_));
  return ret;
}

int ObDumpSingleTxDataP::process()
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "start dump single tx data process", K(arg_));

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_.ls_id_), K(arg_.tenant_id_), K(arg_.tx_id_));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      storage::ObLS *ls = nullptr;
      ObLSService* ls_svr = nullptr;
      ObLSHandle ls_handle;
      ObMemtableMgrHandle memtable_mgr_handle;
      ObIMemtableMgr *memtable_mgr = nullptr;
      common::ObSEArray<ObTableHandleV2, 2> tables_handle;

      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService is null", KR(ret), K(arg_.tenant_id_));
      } else if (OB_FAIL(ls_svr->get_ls(ObLSID(arg_.ls_id_),
                                        ls_handle,
                                        ObLSGetMod::OBSERVER_MOD))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get log_stream's ls_handle", KR(ret), K(arg_.tenant_id_), K(arg_.ls_id_));
        } else {
          LOG_TRACE("log stream not exist in this tenant", KR(ret), K(arg_.tenant_id_), K(arg_.ls_id_));
        }
      } else if (FALSE_IT(ls = ls_handle.get_ls())) {
      } else {
        mkdir("/tmp/dump_single_tx_data/", S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        ret = ls->dump_single_tx_data_2_text(arg_.tx_id_, "/tmp/dump_single_tx_data/single_tx_data.txt");
        if (OB_FAIL(ret)) {
          TRANS_LOG(WARN, "dump single tx data to text failed", KR(ret));
        }
      }
    }
  }

  TRANS_LOG(INFO, "finish dump single tx data process", KR(ret), K(arg_));
  return ret;
}

int ObHaltPrewarmP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService* ps = gctx_.par_ser_;
  // TRANS_LOG(INFO, "halt_prewarm");
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   COMMON_LOG(WARN, "partition_service is null");
  // } else if (OB_FAIL(ps->halt_all_prewarming())) {
  //   COMMON_LOG(WARN, "halt_all_prewarming fail", K(ret));
  // }

  return ret;
}

int ObHaltPrewarmAsyncP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService* ps = gctx_.par_ser_;
  // TRANS_LOG(INFO, "halt_prewarm");
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   COMMON_LOG(WARN, "partition_service is null");
  // } else if (OB_FAIL(ps->halt_all_prewarming(arg_))) {
  //   COMMON_LOG(WARN, "halt_all_prewarming fail", K(ret));
  // }

  return ret;
}

int ObReportReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->report_replica();
  }
  return ret;
}


int ObFlushCacheP::process()
{
  int ret = OB_SUCCESS;
  switch (arg_.cache_type_) {
    case CACHE_TYPE_LIB_CACHE: {
      if (arg_.ns_type_ != ObLibCacheNameSpace::NS_INVALID) {
        ObLibCacheNameSpace ns = arg_.ns_type_;
        if (arg_.is_all_tenant_) { //flush all tenant cache
          common::ObArray<uint64_t> tenant_ids;
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
            LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
          } else {
            for (int64_t i = 0; i < tenant_ids.size(); i++) {
              MTL_SWITCH(tenant_ids.at(i)) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                ret = plan_cache->flush_lib_cache_by_ns(ns);
              }
              // ignore errors at switching tenant
              ret = OB_SUCCESS;
            }
          }
        } else {  // flush appointed tenant cache
          MTL_SWITCH(arg_.tenant_id_) {
            ObPlanCache* plan_cache = MTL(ObPlanCache*);
            ret = plan_cache->flush_lib_cache_by_ns(ns);
          }
        }
      } else {
        if (arg_.is_all_tenant_) { //flush all tenant cache
          common::ObArray<uint64_t> tenant_ids;
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
            LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
          } else {
            for (int64_t i = 0; i < tenant_ids.size(); i++) {
              MTL_SWITCH(tenant_ids.at(i)) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                ret = plan_cache->flush_lib_cache();
              }
              // ignore errors at switching tenant
              ret = OB_SUCCESS;
            }
          }
        } else {  // flush appointed tenant cache
          MTL_SWITCH(arg_.tenant_id_) {
            ObPlanCache* plan_cache = MTL(ObPlanCache*);
            ret = plan_cache->flush_lib_cache();
          }
        }
      }
      break;
    }
    case CACHE_TYPE_PLAN: {
      if (arg_.is_fine_grained_) { // fine-grained plan cache evict
        MTL_SWITCH(arg_.tenant_id_) {
          ObPlanCache* plan_cache = MTL(ObPlanCache*);
          if (arg_.db_ids_.count() == 0) {
            ret = plan_cache->flush_plan_cache_by_sql_id(OB_INVALID_ID, arg_.sql_id_);
          } else {
            for (uint64_t i=0; i<arg_.db_ids_.count(); i++) {
              ret = plan_cache->flush_plan_cache_by_sql_id(arg_.db_ids_.at(i), arg_.sql_id_);
            }
          }
        }
      } else if (arg_.is_all_tenant_) { //flush all tenant cache
        common::ObArray<uint64_t> tenant_ids;
        if (OB_ISNULL(GCTX.omt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null of GCTX.omt_", K(ret));
        } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
          LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
        } else {
          for (int64_t i = 0; i < tenant_ids.size(); i++) {
            MTL_SWITCH(tenant_ids.at(i)) {
              ObPlanCache* plan_cache = MTL(ObPlanCache*);
              ret = plan_cache->flush_plan_cache();
            }
            // ignore errors at switching tenant
            ret = OB_SUCCESS;
          }
        }
      } else {  // flush appointed tenant cache
        MTL_SWITCH(arg_.tenant_id_) {
          ObPlanCache* plan_cache = MTL(ObPlanCache*);
          ret = plan_cache->flush_plan_cache();
        }
      }
      break;
    }
    case CACHE_TYPE_SQL_AUDIT: {
      if (arg_.is_all_tenant_) { // flush all tenant sql audit
        ObArray<uint64_t> id_list;
        if (OB_ISNULL(GCTX.omt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null of omt", K(ret));
        } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(id_list))) {
          LOG_WARN("get tenant ids", K(ret));
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < id_list.size(); i++) { // ignore ret
            MTL_SWITCH(id_list.at(i)) {
              ObMySQLRequestManager *req_mgr = MTL(ObMySQLRequestManager*);
              if (nullptr == req_mgr) {
                // do-nothing
                // virtual tenant such as 50x do not maintain tenant local object, hence req_mgr could be null.
              } else {
                req_mgr->clear_queue();
              }
            }
            // ignore errors at switching tenant
            ret = OB_SUCCESS;
          }
        }
      } else { // flush specified tenant sql audit
        MTL_SWITCH(arg_.tenant_id_) {
          ObMySQLRequestManager *req_mgr = MTL(ObMySQLRequestManager*);
          if (nullptr == req_mgr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get request manager", K(ret), K(req_mgr));
          } else {
            req_mgr->clear_queue();
          }
        }
      }
      break;
    }
    case CACHE_TYPE_PL_OBJ: {
      if (arg_.is_fine_grained_) { // fine-grained plan cache evict
        bool is_evict_by_schema_id = common::OB_INVALID_ID != arg_.schema_id_;
        MTL_SWITCH(arg_.tenant_id_) {
          ObPlanCache* plan_cache = MTL(ObPlanCache*);
          if (arg_.db_ids_.count() == 0) {
            if (is_evict_by_schema_id) {
              ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySchemaIdOp, uint64_t>(OB_INVALID_ID, arg_.schema_id_);
            } else {
              ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySQLIDOp, ObString>(OB_INVALID_ID, arg_.sql_id_);
            }
          } else {
            for (uint64_t i=0; i<arg_.db_ids_.count(); i++) {
              if (is_evict_by_schema_id) {
                ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySchemaIdOp, uint64_t>(arg_.db_ids_.at(i), arg_.schema_id_);
              } else if (OB_ISNULL(arg_.sql_id_)) {
                ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryByDbIdOp, uint64_t>(arg_.db_ids_.at(i), arg_.schema_id_);
              } else {
                ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySQLIDOp, ObString>(arg_.db_ids_.at(i), arg_.sql_id_);
              }
            }
          }
        }
      } else if (arg_.is_all_tenant_) {
        common::ObArray<uint64_t> tenant_ids;
        if (OB_ISNULL(GCTX.omt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null of GCTX.omt_", K(ret));
        } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
          LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
        } else {
          for (int64_t i = 0; i < tenant_ids.size(); i++) {
            MTL_SWITCH(tenant_ids.at(i)) {
              ObPlanCache* plan_cache = MTL(ObPlanCache*);
              ret = plan_cache->flush_pl_cache();
            }
            // ignore errors at switching tenant
            ret = OB_SUCCESS;
          }
        }
      } else {
        MTL_SWITCH(arg_.tenant_id_) {
          ObPlanCache* plan_cache = MTL(ObPlanCache*);
          ret = plan_cache->flush_pl_cache();
        }
      }
      break;
    }
    case CACHE_TYPE_PS_OBJ: {
      if (arg_.is_all_tenant_) {
        common::ObArray<uint64_t> tenant_ids;
        if (OB_ISNULL(GCTX.omt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null of GCTX.omt_", K(ret));
        } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
          LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
        } else {
          for (int64_t i = 0; i < tenant_ids.size(); i++) {
            MTL_SWITCH(tenant_ids.at(i)) {
              ObPsCache* ps_cache = MTL(ObPsCache*);
              if (ps_cache->is_inited()) {
                ret = ps_cache->cache_evict_all_ps();
              }
            }
            // ignore errors at switching tenant
            ret = OB_SUCCESS;
          }
        }
      } else {
        MTL_SWITCH(arg_.tenant_id_) {
          ObPsCache* ps_cache = MTL(ObPsCache*);
          if (ps_cache->is_inited()) {
            ret = ps_cache->cache_evict_all_ps();
          }
        }
      }
      break;
    }
    case CACHE_TYPE_SCHEMA: {
      // this option is only used for upgrade now
      if (arg_.is_all_tenant_) {
        if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(OB_SCHEMA_CACHE_NAME))) {
          LOG_WARN("clear kv cache  failed", K(ret));
        } else {
          LOG_INFO("success erase kvcache", K(ret), K(OB_SCHEMA_CACHE_NAME));
        }
      } else {
        if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(arg_.tenant_id_,
                                                                        OB_SCHEMA_CACHE_NAME))) {
          LOG_WARN("clear kv cache failed", K(ret));
        } else {
          LOG_INFO("success erase kvcache", K(ret), K(arg_.tenant_id_), K(OB_SCHEMA_CACHE_NAME));
        }
      }
      break;
    }
    case CACHE_TYPE_ALL:
    case CACHE_TYPE_COLUMN_STAT:
    case CACHE_TYPE_BLOCK_INDEX:
    case CACHE_TYPE_BLOCK:
    case CACHE_TYPE_ROW:
    case CACHE_TYPE_BLOOM_FILTER:
    case CACHE_TYPE_LOCATION:
    case CACHE_TYPE_CLOG:
    case CACHE_TYPE_ILOG: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cache type not supported flush", "type", arg_.cache_type_, K(ret));
    } break;
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid cache type", "type", arg_.cache_type_);
    }
  }
  return ret;
}

int ObRecycleReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->recycle_replica();
  }
  return ret;
}

int ObClearLocationCacheP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->clear_location_cache();
  }
  return ret;
}

int ObSetDSActionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->set_ds_action(arg_);
  }
  return ret;
}

int ObRequestHeartbeatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->request_heartbeat(result_);
  }
  return ret;
}
int ObRefreshIOCalibrationP::process()
{
  int ret = OB_SUCCESS;
  ret = ObIOCalibration::get_instance().refresh(arg_.only_refresh_, arg_.calibration_list_);
  return ret;
}

int ObExecuteIOBenchmarkP::process()
{
  int ret = OB_SUCCESS;
  ret = ObIOCalibration::get_instance().execute_benchmark();
  return ret;
}

int ObRpcCreateLSP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.get_tenant_id();
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcCreateLSP::process tenant not match", K(ret), K(tenant_id));
  }
  ObLSService *ls_svr = nullptr;
  if (OB_SUCC(ret)) {
    ls_svr = MTL(ObLSService*);
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_svr->create_ls(arg_))) {
      COMMON_LOG(WARN, "failed create log stream", KR(ret), K(arg_));
    }
  }
  (void)result_.init(ret, GCTX.self_addr(), arg_.get_replica_type());
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int ObRpcCreateArbP::process()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObRpcDeleteArbP::process()
{
  int ret = OB_SUCCESS;
  return ret;
}
#endif

int ObRpcCheckLSCanOfflineP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.get_tenant_id();
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "tenant not match", KR(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    ObLSService *ls_svr = nullptr;
    ObLSHandle handle;
    share::ObLSID ls_id = arg_.get_ls_id();
    ObLS *ls = nullptr;
    logservice::ObGCHandler *gc_handler = NULL;
    ls_svr = MTL(ObLSService*);
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
      COMMON_LOG(WARN, "get ls failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls should not be null", KR(ret));
    } else if (OB_ISNULL(gc_handler = ls->get_gc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gc_handler is null", K(ls_id));
    } else if (OB_FAIL(gc_handler->check_ls_can_offline(arg_.get_ls_status()))) {
      LOG_WARN("check_ls_can_offline failed", K(ls_id), K(ret));
    } else {
      LOG_INFO("check_ls_can_offline success", K(ls_id));
    }
  }

  return ret;
}

int ObRpcInnerCreateTenantSnapshotP::process()
{
  int ret = OB_SUCCESS;
  if (MTL_ID() != arg_.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcInnerCreateTenantSnapshotP::process tenant not match", KR(ret), K(arg_));
  }
  if (OB_SUCC(ret)) {
    ObTenantSnapshotService *service = nullptr;
    service = MTL(ObTenantSnapshotService*);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObTenantSnapshotService should not be nullptr", KR(ret), K(arg_));
    } else if (OB_FAIL(service->create_tenant_snapshot(arg_))) {
      COMMON_LOG(WARN, "fail to create tenant snapshot", KR(ret), K(arg_));
    }
  }
  return ret;
}

int ObRpcInnerDropTenantSnapshotP::process()
{
  int ret = OB_SUCCESS;
  if (MTL_ID() != arg_.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcInnerDropTenantSnapshotP::process tenant not match", KR(ret), K(arg_));
  }
  if (OB_SUCC(ret)) {
    ObTenantSnapshotService *service = nullptr;
    service = MTL(ObTenantSnapshotService*);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObTenantSnapshotService should not be nullptr", KR(ret), K(arg_));
    } else if (OB_FAIL(service->drop_tenant_snapshot(arg_))) {
      COMMON_LOG(WARN, "fail to drop tenant snapshot", KR(ret), K(arg_));
    }
  }
  return ret;
}

int ObRpcGetLSAccessModeP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.get_tenant_id();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_svr = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  if (OB_SUCC(ret)) {
    ls_svr = MTL(ObLSService*);
    logservice::ObLogService *log_ls_svr = MTL(logservice::ObLogService*);
    ObLS *ls = nullptr;
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    ObLSID ls_id = arg_.get_ls_id();
    common::ObRole role;
    int64_t first_proposal_id = 0;
    if (OB_ISNULL(ls_svr) || OB_ISNULL(log_ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret), KP(ls_svr), KP(log_ls_svr));
    } else if (OB_FAIL(log_ls_svr->get_palf_role(ls_id, role, first_proposal_id))) {
      COMMON_LOG(WARN, "failed to get palf role", KR(ret), K(ls_id));
    } else if (!is_strong_leader(role)) {
      ret = OB_NOT_MASTER;
      LOG_WARN("the ls not master", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
      COMMON_LOG(WARN, "get ls failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls should not be null", KR(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is null", KR(ret), K(ls_id));
    } else {
      palf::AccessMode mode;
      int64_t mode_version = palf::INVALID_PROPOSAL_ID;
      int64_t second_proposal_id = 0;
      const SCN ref_scn = SCN::min_scn();
      if (OB_FAIL(log_handler->get_access_mode(mode_version, mode))) {
        LOG_WARN("failed to get access mode", KR(ret), K(ls_id));
      } else if (OB_FAIL(result_.init(tenant_id, ls_id, mode_version, mode, ref_scn, share::SCN::min_scn()))) {
        LOG_WARN("failed to init res", KR(ret), K(tenant_id), K(ls_id), K(mode_version), K(mode));
      } else if (OB_FAIL(log_ls_svr->get_palf_role(ls_id, role, second_proposal_id))) {
        COMMON_LOG(WARN, "failed to get palf role", KR(ret), K(ls_id));
      } else if (first_proposal_id != second_proposal_id || !is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("the ls not master", KR(ret), K(ls_id), K(first_proposal_id),
            K(second_proposal_id), K(role));
      }
    }
  }
  return ret;
}

int ObRpcChangeLSAccessModeP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.get_tenant_id();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_svr = nullptr;
  int64_t wait_sync_scn_cost = 0;
  int64_t change_access_mode_cost = 0;
  int64_t begin_time = ObTimeUtility::current_time();
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  if (OB_SUCC(ret)) {
    ls_svr = MTL(ObLSService*);
    logservice::ObLogService *log_ls_svr = MTL(logservice::ObLogService*);
    ObLS *ls = nullptr;
    ObLSID ls_id = arg_.get_ls_id();
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    if (OB_ISNULL(ls_svr) || OB_ISNULL(log_ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService or ObLogService should not be null", KR(ret), KP(ls_svr), KP(log_ls_svr));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
      COMMON_LOG(WARN, "get ls failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls should not be null", KR(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is null", KR(ret), K(ls_id), KP(log_handler));
    } else if (palf::AccessMode::RAW_WRITE == arg_.get_access_mode() && !ls_id.is_sys_ls()) {
      // switchover to standby
      // user ls end scn should be larger than sys ls end scn at first
      DEBUG_SYNC(BEFORE_WAIT_SYS_LS_END_SCN);
      if (OB_UNLIKELY(!arg_.get_sys_ls_end_scn().is_valid_and_not_min())) {
        FLOG_WARN("invalid sys_ls_end_scn, no need to let user ls wait, "
            "the version might be smaller than V4.2.0", KR(ret), K(arg_.get_sys_ls_end_scn()));
      } else if (OB_FAIL(rootserver::ObRootUtils::wait_user_ls_sync_scn_locally(
            arg_.get_sys_ls_end_scn(),
            log_ls_svr,
            *ls))) {
        LOG_WARN("fail to wait user ls sync scn locally", KR(ret), K(ls_id), K(arg_.get_sys_ls_end_scn()));
      }
      wait_sync_scn_cost = ObTimeUtility::current_time() - begin_time;
    }
    begin_time = ObTimeUtility::current_time();
    const int64_t timeout = THIS_WORKER.get_timeout_remain();
    if (FAILEDx(log_handler->change_access_mode(
        arg_.get_mode_version(),
        arg_.get_access_mode(),
        arg_.get_ref_scn()))) {
      LOG_WARN("failed to change access mode", KR(ret), K(arg_), K(timeout));
    }
    change_access_mode_cost = ObTimeUtility::current_time() - begin_time;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = result_.init(tenant_id, ls_id, ret, wait_sync_scn_cost, change_access_mode_cost))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to init res", KR(ret), K(tenant_id), K(ls_id), KR(tmp_ret), K(wait_sync_scn_cost), K(change_access_mode_cost));
    } else {
      //if ret  not OB_SUCCESS, res can not return
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRpcSetMemberListP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle handle;
  ObLS *ls = nullptr;
  uint64_t tenant_id = arg_.get_tenant_id();
  share::ObLSID ls_id = arg_.get_ls_id();
  ObLSService *ls_svr = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcSetMemberListP::process tenant not match", K(ret), K(tenant_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
    COMMON_LOG(WARN, "get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "ls should not be null", K(ret));
#ifdef OB_BUILD_ARBITRATION
  } else if (arg_.get_arbitration_service().is_valid()) {
    if (OB_FAIL(ls->set_initial_member_list(arg_.get_member_list(),
                                            arg_.get_arbitration_service(),
                                            arg_.get_paxos_replica_num(),
                                            arg_.get_learner_list()))) {
      COMMON_LOG(WARN, "failed to set member list and arbitration service", KR(ret), K(arg_));
    } else {
      COMMON_LOG(INFO, "success to set initial member list and arbitration service");
    }
#endif
  } else if (OB_FAIL(ls->set_initial_member_list(arg_.get_member_list(),
                                                 arg_.get_paxos_replica_num(),
                                                 arg_.get_learner_list()))) {
    COMMON_LOG(WARN, "failed to set member list", KR(ret), K(arg_));
  }
  result_.init(ret);
  return ret;
}

int ObRpcDetectMasterRsLSP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->detect_master_rs_ls(arg_, result_);
  }
  return ret;
}
int ObRpcUpdateBaselineSchemaVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->update_baseline_schema_version(arg_);
  }
  return ret;
}

int ObSyncPartitionTableP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->sync_partition_table(arg_);
  }
  return ret;
}

int ObGetDiagnoseArgsP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(GCTX.diag_->refresh_passwd(passwd_))) {
    LOG_ERROR("refresh passwd fail", K(ret));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(
                    argsbuf_, sizeof (argsbuf_), pos,
                    "-h127.0.0.1 -P%ld -u@diag -p%s",
                    GCONF.mysql_port.get(), passwd_.ptr()))) {
      LOG_ERROR("construct arguments fail", K(ret));
    } else {
      result_.assign_ptr(argsbuf_,
                         static_cast<ObString::obstr_size_t>(STRLEN(argsbuf_)));
    }
  }
  return ret;
}

int ObRpcSetTPP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->set_tracepoint(arg_);
  }
  return ret;
}
int ObCancelSysTaskP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->cancel_sys_task(arg_.task_id_);
  }
  return ret;
}

int ObSetDiskValidP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIOManager::get_instance().reset_device_health())) {
    LOG_WARN("reset_disk_error failed", K(ret));
  }
  return ret;
}

int ObAddDiskP::process()
{
  // not support.
  return OB_NOT_SUPPORTED;
}

int ObDropDiskP::process()
{
  // not support.
  return OB_NOT_SUPPORTED;
}

int ObForceSwitchILogFileP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObICLogMgr *clog_mgr = NULL;
  // TRANS_LOG(INFO, "force_switch_ilog_file");
  // if (NULL == (clog_mgr = gctx_.par_ser_->get_clog_mgr())) {
  //   ret = OB_ENTRY_NOT_EXIST;
  //   COMMON_LOG(WARN, "get_clog_mgr failed", K(ret));
  // //} else if (OB_FAIL(clog_mgr->force_switch_ilog_file())) {
  // //  COMMON_LOG(WARN, "force_switch_ilog_file failed", K(ret));
  // }
  return ret;
}

int ObForceSetAllAsSingleReplicaP::process()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObUpdateLocalStatCacheP::process()
{
  int ret = OB_SUCCESS;
  ObOptStatManager &stat_manager = ObOptStatManager::get_instance();
  if (OB_FAIL(stat_manager.add_refresh_stat_task(arg_))) {
    LOG_WARN("failed to update local statistic cache", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObForceDisableBlacklistP::process()
{
  int ret = OB_SUCCESS;
  share::ObServerBlacklist::get_instance().disable_blacklist();
  COMMON_LOG(INFO, "disable_blacklist finished", K(ret));
  return ret;
}

int ObForceEnableBlacklistP::process()
{
  int ret = OB_SUCCESS;
  share::ObServerBlacklist::get_instance().enable_blacklist();
  COMMON_LOG(INFO, "enable_blacklist finished", K(ret));
  return ret;
}

int ObForceClearBlacklistP::process()
{
  int ret = OB_SUCCESS;
  share::ObServerBlacklist::get_instance().clear_blacklist();
  COMMON_LOG(INFO, "clear_black_list finished", K(ret));
  return ret;
}

int ObCheckPartitionLogP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_partition_log(arg_, result_);
  }
  return ret;
}

int ObStopPartitionWriteP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->stop_partition_write(arg_, result_);
  }
  return ret;
}

int ObEstimatePartitionRowsP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->estimate_partition_rows(arg_, result_);
  }
  return ret;
}

int ObGetWRSInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_wrs_info(arg_, result_))) {
    LOG_WARN("failed to get cluster info", K(ret));
  }
  return ret;
}

int ObHaGtsPingRequestP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = gctx_.par_ser_;
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  // } else if (OB_FAIL(ps->handle_ha_gts_ping_request(arg_, result_))) {
  //   LOG_WARN("handle_ha_gts_ping_request failed", K(ret), K(arg_), K(result_));
  // }
  return ret;
}

int ObHaGtsGetRequestP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = gctx_.par_ser_;
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  // } else if (OB_FAIL(ps->handle_ha_gts_get_request(arg_))) {
  //   LOG_WARN("handle_ha_get_gts_request failed", K(ret), K(arg_));
  // }
  return ret;
}

int ObHaGtsGetResponseP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = gctx_.par_ser_;
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  // } else if (OB_FAIL(ps->handle_ha_gts_get_response(arg_))) {
  //   LOG_WARN("handle_ha_gts_get_response failed", K(ret), K(arg_));
  // } else {
  //   // do nothing
  // }
  return ret;
}

int ObHaGtsHeartbeatP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = gctx_.par_ser_;
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  // } else if (OB_FAIL(ps->handle_ha_gts_heartbeat(arg_))) {
  //   LOG_WARN("handle_ha_gts_heartbeat failed", K(ret), K(arg_));
  // } else {
  //   // do nothing
  // }
  return ret;
}

int ObGetTenantSchemaVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->get_tenant_refreshed_schema_version(arg_, result_);
  }
  return ret;
}

int ObHaGtsUpdateMetaP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = gctx_.par_ser_;
  // const obrpc::ObHaGtsUpdateMetaRequest &arg = arg_;
  // obrpc::ObHaGtsUpdateMetaResponse &response = result_;
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg));
  // } else if (OB_FAIL(ps->handle_ha_gts_update_meta(arg, response))) {
  //   LOG_WARN("handle_ha_gts_update_meta failed", K(ret), K(arg));
  // } else {
  //   // do nothing
  // }
  return ret;
}

int ObHaGtsChangeMemberP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = gctx_.par_ser_;
  // const obrpc::ObHaGtsChangeMemberRequest &arg = arg_;
  // obrpc::ObHaGtsChangeMemberResponse &response = result_;
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg));
  // } else if (OB_FAIL(ps->handle_ha_gts_change_member(arg, response))) {
  //   LOG_WARN("handle_ha_gts_change_member failed", K(ret), K(arg));
  // } else {
  //   // do nothing
  // }
  return ret;
}

int ObUpdateTenantMemoryP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().update_tenant_memory(arg_))) {
    LOG_WARN("failed to update tenant memory", K(ret), K_(arg));
  }
  return ret;
}


int ObForceSetServerListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->force_set_server_list(arg_, result_))) {
    COMMON_LOG(WARN, "force_set_server_list failed", KR(ret), K(arg_));
  }

  return ret;
}


int ObRenewInZoneHbP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("observer is null", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->renew_in_zone_hb(arg_, result_))) {
    LOG_WARN("failed to check physical flashback", K(ret), K(arg_), K(result_));
  }
  return ret;
}

int ObPreProcessServerP::process()
{
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *ps = static_cast<ObPartitionService *>(gctx_.par_ser_);
  // if (OB_UNLIKELY(NULL == ps)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  // } else if (arg_.rescue_server_ != gctx_.self_addr()) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("rescue server is not expected", K(ret), K(arg_), K(gctx_.self_addr_seq_));
  // } else if (OB_FAIL(ps->schedule_server_preprocess_task(arg_))) {
  //   LOG_WARN("schedule preprocess task failed", K(ret), K(arg_));
  // }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObGetMasterKeyP::process()
{
  int ret = OB_SUCCESS;
  // int64_t master_key_version = (int64_t)arg_;
  // int64_t master_key_len = 0;
  // if (OB_FAIL(share::ObMasterKeyGetter::get_master_key(master_key_version,
  //                     result_.str_.ptr(), OB_MAX_MASTER_KEY_LENGTH, master_key_len))) {
  //   TRANS_LOG(WARN, "failed to get master key", K(ret));
  // }
  // result_.str_.set_length(master_key_len);
  ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObRestoreKeyP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMasterKeyUtil::restore_key(arg_.tenant_id_,
                                           arg_.backup_dest_, arg_.encrypt_key_))) {
    LOG_WARN("failed to restore key", K(ret));
  }
  return ret;
}

int ObSetRootKeyP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMasterKeyGetter::instance().set_root_key(arg_, result_))) {
    LOG_WARN("failed to set root key", K(ret));
  }
  return ret;
}

int ObCloneKeyP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t source_tenant_id = arg_.get_source_tenant_id();
  ObCipherOpMode mode;
  common::ObSEArray<std::pair<uint64_t, ObMasterKey>, 2> master_key_list;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  //get master key info from source tenant
  } else if (OB_FAIL(ObMasterKeyGetter::get_table_key_algorithm(source_tenant_id, mode))) {
    LOG_WARN("failed to get table key algorithm", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(ObMasterKeyGetter::instance().dump_tenant_keys(source_tenant_id,
                                                                    master_key_list))) {
    LOG_WARN("failed to dump tenant key", KR(ret), K(source_tenant_id));
  //set master key info for clone tenant
  } else if (OB_FAIL(ObMasterKeyGetter::instance().load_tenant_keys(arg_.get_tenant_id(),
                                                                    mode,
                                                                    master_key_list))) {
    LOG_WARN("failed to load tenant keys", KR(ret), K(arg_));
  }
  return ret;
}

int ObTrimKeyListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  } else if (OB_FAIL(ObMasterKeyGetter::instance().trim_master_key_map(arg_.get_tenant_id(),
                                                  arg_.get_latest_master_key_id()))) {
    LOG_WARN("fail to trim master key map", KR(ret), K(arg_));
  }
  return ret;
}
#endif

int ObHandlePartTransCtxP::process()
{
  LOG_INFO("handle_part_trans_ctx rpc is called", K(arg_));
  int ret = OB_NOT_SUPPORTED;
  // if (OB_UNLIKELY(!arg_.is_valid())) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_WARN("invalid argument", K(ret), K(arg_));
  // } else if (OB_ISNULL(gctx_.par_ser_)) {
  //   ret = OB_ERR_SYS;
  //   LOG_WARN("gctx partition service is null");
  // } else if (OB_FAIL(gctx_.par_ser_->get_trans_service()->handle_part_trans_ctx(arg_, result_))) {
  //   LOG_WARN("failed to modify part trans ctx", K(ret), K(arg_));
  // }
  return ret;
}

int ObFlushLocalOptStatMonitoringInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObOptStatMonitorManager *optstat_monitor_mgr = NULL;
    if (OB_ISNULL(optstat_monitor_mgr = MTL(ObOptStatMonitorManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(optstat_monitor_mgr));
    } else if (OB_FAIL(optstat_monitor_mgr->update_opt_stat_monitoring_info(arg_))) {
      LOG_WARN("failed to flush opt stat monitoring info", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRpcFetchTabletAutoincSeqCacheP::process()
{
  int ret = OB_SUCCESS;
  ObTabletAutoincSeqRpcHandler &autoinc_seq_handler = ObTabletAutoincSeqRpcHandler::get_instance();
  if (OB_FAIL(autoinc_seq_handler.fetch_tablet_autoinc_seq_cache(arg_, result_))) {
    COMMON_LOG(WARN, "failed to fetch tablet autoinc seq cache", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcBatchGetTabletAutoincSeqP::process()
{
  int ret = OB_SUCCESS;
  ObTabletAutoincSeqRpcHandler &autoinc_seq_handler = ObTabletAutoincSeqRpcHandler::get_instance();
  if (OB_FAIL(autoinc_seq_handler.batch_get_tablet_autoinc_seq(arg_, result_))) {
    COMMON_LOG(WARN, "failed to batch get tablet autoinc seq", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcBatchSetTabletAutoincSeqP::process()
{
  int ret = OB_SUCCESS;
  ObTabletAutoincSeqRpcHandler &autoinc_seq_handler = ObTabletAutoincSeqRpcHandler::get_instance();
  if (OB_FAIL(autoinc_seq_handler.batch_set_tablet_autoinc_seq(arg_, result_))) {
    COMMON_LOG(WARN, "failed to batch set tablet autoinc seq", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcSetTabletAutoincSeqP::process()
{
  int ret = OB_SUCCESS;
  ObTabletAutoincSeqRpcHandler &autoinc_seq_handler = ObTabletAutoincSeqRpcHandler::get_instance();
  if (OB_FAIL(autoinc_seq_handler.batch_set_tablet_autoinc_seq(arg_, result_))) {
    COMMON_LOG(WARN, "failed to batch set tablet autoinc seq", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcClearTabletAutoincSeqCacheP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rpc pkt", K(ret));
  } else {
    const int64_t abs_timeout_us = get_send_timestamp() + rpc_pkt_->get_timeout();
    ret = ObTabletAutoincrementService::get_instance().clear_tablet_autoinc_seq_cache(MTL_ID(), arg_.tablet_ids_, abs_timeout_us);
  }
  return ret;
}

int ObRpcBatchGetTabletBindingP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rpc pkt", K(ret));
  } else {
    const int64_t abs_timeout_us = get_send_timestamp() + rpc_pkt_->get_timeout();
    ret = ObTabletBindingMdsHelper::batch_get_tablet_binding(abs_timeout_us, arg_, result_);
  }
  return ret;
}

int ObRpcBatchGetTabletSplitP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rpc pkt", K(ret));
  } else {
    const int64_t abs_timeout_us = get_send_timestamp() + rpc_pkt_->get_timeout();
    ret = ObTabletSplitMdsHelper::batch_get_tablet_split(abs_timeout_us, arg_, result_);
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObDumpTenantCacheMasterKeyP::process()
{
  const uint64_t tenant_id = (uint64_t)arg_;
  return ObMasterKeyGetter::instance().dump_tenant_cache_master_key(tenant_id, result_);
}
#endif

int ObBatchBroadcastSchemaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->batch_broadcast_schema(arg_, result_);
  }
  return ret;
}

int ObRpcRemoteWriteDDLRedoLogP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObRole role = INVALID_ROLE;
      ObDDLRedoLogWriter sstable_redo_writer;
      MacroBlockId macro_block_id;
      ObLSService *ls_service = MTL(ObLSService*);
      blocksstable::ObMacroBlockHandle macro_handle;
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;

      if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg_));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      } else if (ObRole::LEADER != role) {
        ret = OB_NOT_MASTER;
        LOG_INFO("leader may not have finished replaying clog, caller retry", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      #ifdef OB_BUILD_SHARED_STORAGE
      } else {
        ObTabletHandle tablet_handle;
        if (OB_FAIL(ls->get_tablet(arg_.redo_info_.table_key_.tablet_id_, tablet_handle,
                    ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          LOG_WARN("failed to get tablet handle", K(ret));
        } else if (OB_FAIL(ObDDLRedoLogWriter::write_gc_flag(tablet_handle,
                                                             arg_.redo_info_.table_key_,
                                                             arg_.redo_info_.parallel_cnt_,
                                                             arg_.redo_info_.cg_cnt_))) {
          LOG_WARN("failed to write gc flag file", K(ret), K(arg_.redo_info_));
        }
      }
      if (OB_FAIL(ret)) {
      #endif
      } else if (OB_FAIL(ObDDLRedoLogWriter::write_block_to_disk(arg_.redo_info_, arg_.ls_id_, macro_handle, macro_block_id))) {
        LOG_WARN("failed to write block to disk", K(ret));
      } else if (OB_FAIL(sstable_redo_writer.init(arg_.ls_id_, arg_.redo_info_.table_key_.tablet_id_))) {
        LOG_WARN("init sstable redo writer", K(ret), K_(arg));
      } else if (OB_FAIL(sstable_redo_writer.write_macro_block_log(arg_.redo_info_, macro_block_id, false/*allow_remote_write*/, arg_.task_id_))) {
        LOG_WARN("fail to write macro redo", K(ret), K_(arg), K(macro_block_id));
      } else if (OB_FAIL(sstable_redo_writer.wait_macro_block_log_finish(arg_.redo_info_, macro_block_id))) {
        LOG_WARN("fail to wait macro redo finish", K(ret), K_(arg));
      }
    }
  }
  return ret;
}

int ObRpcRemoteWriteDDLCommitLogP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;

  MTL_SWITCH(tenant_id) {
    ObRole role = INVALID_ROLE;
    const ObITable::TableKey &table_key = arg_.table_key_;
    ObDDLRedoLogWriter sstable_redo_writer;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    ObTabletFullDirectLoadMgr *data_tablet_mgr = nullptr;
    ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
    direct_load_mgr_handle.reset();
    bool is_major_sstable_exist = false;
    if (OB_UNLIKELY(!arg_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K_(arg));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(MTL_ID()), K(arg_.ls_id_));
    } else if (OB_FAIL(ls->get_ls_role(role))) {
      LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg_.ls_id_));
    } else if (ObRole::LEADER != role) {
      ret = OB_NOT_MASTER;
      LOG_INFO("leader may not have finished replaying clog, caller retry", K(ret), K(MTL_ID()), K(arg_.ls_id_));
    } else if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
            arg_.ls_id_,
            table_key.tablet_id_,
            true /*is_full_direct_load*/,
            direct_load_mgr_handle,
            is_major_sstable_exist))) {
      if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
        ret = OB_TASK_EXPIRED;
        LOG_INFO("major sstable already exist", K(ret), K(arg_));
      } else {
        LOG_WARN("get tablet direct load manager failed", K(ret), K(table_key));
      }
    } else if (OB_ISNULL(data_tablet_mgr = direct_load_mgr_handle.get_full_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(table_key));
    } else if (OB_FAIL(sstable_redo_writer.init(arg_.ls_id_, table_key.tablet_id_))) {
      LOG_WARN("init sstable redo writer", K(ret), K(table_key));
    } else {
      uint32_t lock_tid = 0;
      SCN commit_scn;
      bool is_remote_write = false;
      ObTabletHandle tablet_handle;
      if (OB_FAIL(data_tablet_mgr->wrlock(ObTabletDirectLoadMgr::TRY_LOCK_TIMEOUT, lock_tid))) {
        LOG_WARN("failed to wrlock", K(ret), K(arg_));
      } else if (OB_FAIL(ls->get_tablet(table_key.tablet_id_, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("get tablet failed", K(ret), K(table_key));
      } else if (OB_FAIL(sstable_redo_writer.write_commit_log(false,
                                                              table_key,
                                                              arg_.start_scn_,
                                                              direct_load_mgr_handle,
                                                              tablet_handle,
                                                              commit_scn,
                                                              is_remote_write,
                                                              lock_tid))) {
        LOG_WARN("fail to remote write commit log", K(ret), K(table_key), K_(arg));
      } else if (OB_FAIL(data_tablet_mgr->commit(*tablet_handle.get_obj(),
                                                 arg_.start_scn_,
                                                 commit_scn,
                                                 arg_.table_id_,
                                                 arg_.ddl_task_id_,
                                                 false/*is replay*/))) {
        LOG_WARN("failed to do ddl kv commit", K(ret), K(arg_));
      } else {
        result_ = commit_scn.get_val_for_tx();
      }
      if (lock_tid != 0) {
        data_tablet_mgr->unlock(lock_tid);
      }
    }
  }
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObRpcRemoteWriteDDLFinishLogP::process()
{
  int ret = OB_NOT_IMPLEMENT;
  const uint64_t tenant_id = arg_.tenant_id_;

  MTL_SWITCH (tenant_id) {
    ObRole role = INVALID_ROLE;
    ObDDLFinishLogInfo &log_info = arg_.log_info_;
    ObDDLFinishLog finish_log;
    ObITable::TableKey &table_key = log_info.table_key_;
    ObDDLRedoLogWriter sstable_redo_writer;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    if (OB_UNLIKELY(!arg_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K_(arg));
    } else if (OB_FAIL(finish_log.assign(log_info))) {
      LOG_WARN("failed to init finish log", K(ret), K(log_info));
    } else if (OB_FAIL(ls_service->get_ls(log_info.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(MTL_ID()), K(log_info.ls_id_));
    } else if (OB_FAIL(ls->get_ls_role(role))) {
      LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(log_info.ls_id_));
    } else if (ObRole::LEADER != role) {
      ret = OB_NOT_MASTER;
      LOG_INFO("leader may not have finished replaying clog, caller retry", K(ret), K(MTL_ID()), K(log_info.ls_id_));
    } else if (OB_FAIL(sstable_redo_writer.init(log_info.ls_id_, log_info.table_key_.tablet_id_))) {
      LOG_WARN("init sstable redo writer", K(ret), K(table_key));
    } else {
      bool is_remote_write = false;
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls->get_tablet(table_key.tablet_id_, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("get tablet failed", K(ret), K(table_key));
      } else if (OB_FAIL(sstable_redo_writer.write_finish_log(false,
                                                              finish_log,
                                                              is_remote_write))) {
        LOG_WARN("fail to remote write finish log", K(ret), K(table_key), K_(arg));
      } else if (OB_FAIL(sstable_redo_writer.wait_finish_log(finish_log.get_ls_id(),
          finish_log.get_table_key(),
          finish_log.get_data_format_version()))) {
        LOG_WARN("failed to set ready for apply", K(ret), K(finish_log));
      }
    }
  }
  return ret;
}

int ObRpcSyncHotMicroKeyP::process()
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sync hot micro key is not supported", KR(ret));
  } else {
    const uint64_t tenant_id = arg_.tenant_id_;
    MTL_SWITCH (tenant_id) {
      ObRole role = INVALID_ROLE;
      const int64_t ls_id = arg_.ls_id_;
      ObLSService *ls_service = MTL(ObLSService*);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      if (OB_UNLIKELY(!arg_.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", KR(ret), K_(arg));
      } else if (OB_FAIL(ls_service->get_ls(ObLSID(ls_id), ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", KR(ret), K(arg_));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(ls->get_ls_prewarm_handler().push_micro_cache_keys(arg_))) {
        LOG_WARN("fail to push micro cache keys", KR(ret), K_(arg));
      }
    }
  }
  return ret;
}

#endif

int ObRpcRemoteWriteDDLIncCommitLogP::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObRole role = INVALID_ROLE;
      ObDDLIncRedoLogWriter sstable_redo_writer;
      ObLSService *ls_service = MTL(ObLSService*);
      ObTransService *trans_service = MTL(ObTransService *);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg_));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      } else if (ObRole::LEADER != role) {
        ret = OB_NOT_MASTER;
        LOG_INFO("leader may not have finished replaying clog, caller retry", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      } else if (OB_FAIL(sstable_redo_writer.init(arg_.ls_id_, arg_.tablet_id_))) {
        LOG_WARN("init sstable redo writer", K(ret), K(arg_.tablet_id_));
      } else if (OB_FAIL(sstable_redo_writer.write_inc_commit_log_with_retry(false/*allow_remote_write*/,
                                                                             arg_.lob_meta_tablet_id_,
                                                                             arg_.tx_desc_))) {
        LOG_WARN("fail to write inc commit log", K(ret), K(arg_));
      } else if (OB_FAIL(trans_service->get_tx_exec_result(*arg_.tx_desc_, result_.tx_result_))) {
        LOG_WARN("fail to get_tx_exec_result", K(ret), K(arg_));
      }
    }
  }

  return ret;
}

int ObCleanSequenceCacheP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t sequence_id = (uint64_t)arg_;
  share::ObSequenceCache &sequence_cache = share::ObSequenceCache::get_instance();
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(sequence_cache.remove(MTL_ID(), sequence_id, result_))) {
    LOG_WARN("remove sequence item from sequence cache failed", K(ret), K(sequence_id), K(result_));
  }
  return ret;
}

int ObRegisterTxDataP::process()
{
  int ret = OB_SUCCESS;
  ObTransService *tx_svc = MTL_WITH_CHECK_TENANT(ObTransService *, arg_.tenant_id_);

  if (OB_ISNULL(tx_svc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tx service ptr", KR(ret), K(arg_));
  } else if (OB_FAIL(tx_svc->register_mds_into_tx(*(arg_.tx_desc_),
                                                  arg_.ls_id_,
                                                  arg_.type_,
                                                  arg_.buf_.ptr(),
                                                  arg_.buf_.length(),
                                                  arg_.request_id_,
                                                  arg_.register_flag_,
                                                  arg_.seq_no_))) {
    LOG_WARN("register into tx failed", KR(ret), K(arg_));
  } else if (OB_FAIL(tx_svc->collect_tx_exec_result(*(arg_.tx_desc_), result_.tx_result_))) {
    LOG_WARN("collect tx result failed", KR(ret), K(result_));
  }

  tx_svc->release_tx(*arg_.tx_desc_);
  result_.result_ = ret;

  return ret;
}

int ObQueryLSIsValidMemberP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const common::ObAddr &addr = arg_.self_addr_;
  obrpc::ObQueryLSIsValidMemberResponse &response = result_;

  if (MTL_ID() != (arg_.tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "tenant id is not match", K(MTL_ID()), K(arg_.tenant_id_));
  } else {
    COMMON_LOG(INFO, "handle ObQueryLSIsValidMember requeset", K(arg_));
    ObLSService *ls_service = MTL(ObLSService*);
    ObLS *ls = NULL;
    logservice::ObLogHandler *log_handler = NULL;
    for (int64_t index = 0; OB_SUCC(ret) && index < arg_.ls_array_.count(); index++) {
      const share::ObLSID &id = arg_.ls_array_[index];
      bool is_valid_member = true;
      obrpc::LogMemberGCStat stat = obrpc::LogMemberGCStat::LOG_MEMBER_NORMAL_GC_STAT;
      ObLSHandle handle;
      if (OB_SUCCESS != (tmp_ret = ls_service->get_ls(id, handle, ObLSGetMod::OBSERVER_MOD))) {
        if (OB_LS_NOT_EXIST == tmp_ret || OB_NOT_RUNNING == tmp_ret) {
          COMMON_LOG(WARN, "get log stream failed", K(id), K(tmp_ret));
        } else {
          COMMON_LOG(ERROR, "get log stream failed", K(id), K(tmp_ret));
        }
      } else if (OB_ISNULL(ls = handle.get_ls())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, " log stream not exist", K(id), K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = ls->get_member_gc_stat(addr, is_valid_member, stat))) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          COMMON_LOG(WARN, "get_member_gc_stat failed", K(tmp_ret), K(id), K(addr));
        }
      } else {}

      if (OB_FAIL(response.ls_array_.push_back(id))) {
        COMMON_LOG(WARN, "response partition_array_ push_back failed", K(addr), K(id));
      } else if (OB_FAIL(response.ret_array_.push_back(tmp_ret))) {
        COMMON_LOG(WARN, "response ret_array push_back failed", K(addr), K(id), K(tmp_ret));
      } else if (OB_FAIL(response.candidates_status_.push_back(is_valid_member))) {
        COMMON_LOG(WARN, "response candidates_status_ push_back failed", K(addr), K(id), K(is_valid_member));
      } else if (OB_FAIL(response.gc_stat_array_.push_back(stat))) {
        COMMON_LOG(WARN, "response gc_stat_array_ push_back failed", K(addr), K(id), K(stat));
      } else {
        // do nothing
      }
    }
  }

  response.ret_value_ = ret;
  ret = OB_SUCCESS;
  return ret;
}

int ObCheckpointSlogP::process()
{
  int ret = OB_SUCCESS;

  if (OB_SERVER_TENANT_ID == arg_.tenant_id_) {
    if (OB_FAIL(SERVER_STORAGE_META_SERVICE.write_checkpoint(true/*is_force*/))) {
      LOG_WARN("fail to write server checkpoint", K(ret));
    }
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      if (OB_FAIL(MTL(ObTenantStorageMetaService*)->write_checkpoint(true/*is_force*/))) {
        LOG_WARN("write tenant checkpoint failed", K(ret), K(arg_.tenant_id_));
      }
    }
  }
  LOG_INFO("handle checkpoint slog requeset finish", K(ret), K(arg_));
  return ret;
}

int ObRpcCheckBackupDestConnectivityP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_backup_dest_connectivity(arg_);
  }
  return ret;
}

int ObRpcBackupLSDataResP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->report_backup_over(arg_);
  }
  return ret;
}

int ObRpcBackupCleanLSResP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->report_backup_clean_over(arg_);
  }
  return ret;
}

#ifdef OB_BUILD_SPM
int ObServerAcceptPlanBaselineP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql::ObSpmController::accept_plan_baseline_by_user(arg_))) {
    LOG_WARN("failed to accept plan baseline", K(ret), K(arg_));
  }
  return ret;
}

int ObServerCancelEvolveTaskP::process()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = nullptr;
  obrpc::ObModifyPlanBaselineArg &arg = arg_;
  bool evict_baseline = (ObModifyPlanBaselineArg::Action::EVICT_BASELINE == arg.action_ ||
                         ObModifyPlanBaselineArg::Action::EVICT_ALL == arg.action_);
  bool evict_plan = (ObModifyPlanBaselineArg::Action::EVICT_EVOLVE == arg.action_ ||
                     ObModifyPlanBaselineArg::Action::EVICT_ALL == arg.action_);
  MTL_SWITCH(arg.tenant_id_) {
    plan_cache = MTL(ObPlanCache*);
    if (evict_baseline && OB_FAIL(plan_cache->
          cache_evict_baseline(arg.database_id_, arg.sql_id_))) {
      LOG_WARN("failed to evict baseline by sql id", K(ret));
    } else if (evict_plan && OB_FAIL(plan_cache->
          cache_evict_plan_by_sql_id(arg.database_id_, arg.sql_id_))) {
      LOG_WARN("failed to evict plan by sql id", K(ret));
    }
  }
  return ret;
}

int ObLoadBaselineP::process()
{
  int ret = OB_SUCCESS;

  observer::ObReqTimeGuard req_timeinfo_guard;
  MTL_SWITCH(arg_.tenant_id_) {
    ObPlanCache *plan_cache = MTL(ObPlanCache*);
    uint64_t dummy_count = 0;
    if (OB_INVALID_ID == arg_.tenant_id_ || arg_.sql_id_.empty()) {  // load appointed tenant cache
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K_(arg), K(ret));
    } else if (OB_FAIL(plan_cache->load_plan_baseline(arg_, dummy_count))) {
      LOG_WARN("fail to load baseline from pc with arg", K_(arg));
    }
  }

  return ret;
}

int ObLoadBaselineV2P::process()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  MTL_SWITCH(arg_.tenant_id_) {
    ObPlanCache *plan_cache = MTL(ObPlanCache*);
    uint64_t load_count = 0;
    if (OB_UNLIKELY(OB_INVALID_ID == arg_.tenant_id_)) {  // load appointed tenant cache
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K_(arg), K(ret));
    } else if (OB_FAIL(plan_cache->load_plan_baseline(arg_, load_count))) {
      LOG_WARN("fail to load baseline from pc with arg", K_(arg));
    } else {
      result_.load_count_ = load_count;
    }
  }

  return ret;
}
#endif

int ObEstimateTabletBlockCountP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->estimate_tablet_block_count(arg_, result_);
  }
  return ret;
}

int ObRpcGenUniqueIDP::process()
{
  int ret = ObCommonIDUtils::gen_unique_id(arg_, result_);
  return ret;
}

int ObRpcStartTransferTaskP::process()
{
  int ret = OB_SUCCESS;
  ObTransferService *transfer_service = nullptr;
  const share::ObLSID &src_ls = arg_.get_src_ls();
  const uint64_t tenant_id = arg_.get_tenant_id();
  bool is_leader = false;
  if (OB_UNLIKELY(tenant_id != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcStartTransferTaskP::process tenant not match", KR(ret), K_(arg));
  } else if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K_(arg));
  } else if (OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(transfer_service));
  } else if (OB_FAIL(storage::ObStorageHAUtils::check_ls_is_leader(tenant_id, src_ls, is_leader))) {
    LOG_WARN("fail to check ls is leader", K(ret), K(tenant_id), K(src_ls));
  } else if (!is_leader) {
    ret = OB_NOT_MASTER;
    LOG_WARN("ls is not leader, please retry", K(ret), K(is_leader));
  } else {
    transfer_service->wakeup();
  }
  return ret;
}

int ObRpcGetLSSyncScnP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_ls_sync_scn(arg_, result_))) {
    COMMON_LOG(WARN, "failed to get_ls_sync_scn", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcGetTenantResP::process()
{
  int ret = OB_SUCCESS;
  ObResourceLimitCalculator *cal = nullptr;
  ObUserResourceCalculateArg res_arg;
  const uint64_t tenant_id = arg_.get_tenant_id();
  if (OB_UNLIKELY(tenant_id != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcStartTransferTaskP::process tenant not match", KR(ret), K_(arg));
  } else if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K_(arg));
  } else if (OB_ISNULL(cal = MTL(ObResourceLimitCalculator*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cal is null", KR(ret), K_(arg));
  } else if (OB_FAIL(cal->get_tenant_logical_resource(res_arg))) {
    LOG_WARN("failed to get tenant logical resource", KR(ret), K_(arg));
  } else if (OB_FAIL(result_.init(GCTX.self_addr(), res_arg))) {
    LOG_WARN("failed to init result", KR(ret), K(res_arg));
  }
  return ret;

}

int ObForceSetLSAsSingleReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->force_set_ls_as_single_replica(arg_))) {
    COMMON_LOG(WARN, "force_set_ls_as_single_replica failed", KR(ret), K(arg_));
  } else {}
  return ret;
}

int ObRefreshTenantInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->refresh_tenant_info(arg_, result_))) {
    COMMON_LOG(WARN, "failed to refresh_tenant_info", KR(ret), K(arg_));
  }
  return ret;
}

int ObUpdateTenantInfoCacheP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->update_tenant_info_cache(arg_, result_))) {
    COMMON_LOG(WARN, "failed to update_tenant_info_cache", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcFinishTransferTaskP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObRpcFinishTransferTaskP::process", K(MTL_ID()), K_(arg));
  uint64_t tenant_id = arg_.get_tenant_id();
  if (OB_UNLIKELY(tenant_id != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcFinishTransferTaskP::process tenant not match", KR(ret), K(tenant_id), K_(arg));
  } else {
    rootserver::ObTenantTransferService *tenant_transfer = nullptr;
    tenant_transfer = MTL(rootserver::ObTenantTransferService*);
    if (OB_ISNULL(tenant_transfer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mtl ObTenantTransferService should not be null", KR(ret), K_(arg));
    } else {
      tenant_transfer->wakeup();
    }
  }
  return ret;
}

int ObSyncRewriteRulesP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  sql::ObUDRMgr *rule_mgr = nullptr;

  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("start do sync rewrite rules from inner table", K(arg_));

    rule_mgr = MTL(sql::ObUDRMgr*);
    if (OB_ISNULL(rule_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObUDRMgr should not be null", K(ret));
    } else if (OB_FAIL(rule_mgr->sync_rule_from_inner_table())) {
      LOG_WARN("failed to sync rewrite rules from inner table", K(ret));
    }
    }
  return ret;
}

int ObRpcSendHeartbeatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", KR(ret), KP(gctx_.ob_service_));
  } else if (OB_FAIL(gctx_.ob_service_->handle_heartbeat(arg_, result_))) {
    LOG_WARN("fail to call handle_heartbeat in ob service", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcCreateDuplicateLSP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.get_tenant_id();
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      rootserver::ObPrimaryLSService* primary_ls_service = MTL(rootserver::ObPrimaryLSService*);
      if (OB_ISNULL(primary_ls_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary ls service is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(primary_ls_service->create_duplicate_ls())) {
        LOG_WARN("failed to create duplicate log stream", KR(ret), K(tenant_id));
      }
    }
  }
  (void)result_.init(ret);
  return ret;
}

int ObSessInfoVerificationP::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObString str_result;
  LOG_TRACE("veirfy process start", K(ret), K(arg_.get_sess_id()));
  if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "session_mgr_ is null", KR(ret));
  } else if (OB_FAIL(gctx_.session_mgr_->get_session(arg_.get_sess_id(), session))) {
    COMMON_LOG(WARN, "get session failed", KR(ret), K(arg_));
  } else {
    // consider 3 scene that no need verify:
    // 1. Broken link reuse session，need verify proxy sess id
    // 2. Mixed running scene, need verify version
    // 3. Routing without synchronizing session information, judge is_has_query_executed
    // 4. need guarantee the latest session information
    if (arg_.get_proxy_sess_id() == session->get_proxy_sessid() &&
        GET_MIN_CLUSTER_VERSION() == CLUSTER_CURRENT_VERSION &&
        session->is_has_query_executed() &&
        session->is_latest_sess_info()
        ) {
      if (OB_FAIL(ObSessInfoVerify::fetch_verify_session_info(*session,
          str_result, result_.allocator_))) {
        COMMON_LOG(WARN, "fetch session check info failed", KR(ret), K(result_));
      } else {
        char *ptr = nullptr;
        if (OB_ISNULL(ptr = static_cast<char *> (result_.allocator_.alloc(str_result.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc mem for client identifier", K(ret));
        } else {
          result_.verify_info_buf_.assign_buffer(ptr, str_result.length());
          result_.verify_info_buf_.write(str_result.ptr(), str_result.length());
          result_.need_verify_ = true;
          LOG_TRACE("need verify is true", K(ret), K(result_.need_verify_));
        }
      }
    } else {
      result_.need_verify_ = false;
      LOG_TRACE("no need self verification", K(arg_.get_proxy_sess_id()),
                K(session->get_proxy_sessid()), K(GET_MIN_CLUSTER_VERSION()),
                K(CLUSTER_CURRENT_VERSION));
    }
    if (NULL != session) {
      gctx_.session_mgr_->revert_session(session);
    }
  }
  return ret;
}

int ObSessInfoVerificationP::after_process(int err_code)
{
  int ret = OB_SUCCESS;
  result_.allocator_.reset();
  ObRpcProcessorBase::after_process(err_code);
  return ret;
}

int ObRpcDetectSessionAliveP::process()
{
  return ObTableLockDetectFuncList::detect_session_alive_for_rpc(arg_, result_);
}

int ObRpcGetServerResourceInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", KR(ret), KP(gctx_.ob_service_));
  } else if (OB_FAIL(gctx_.ob_service_->get_server_resource_info(arg_, result_))) {
    LOG_WARN("fail to call get_server_resource_info in ob service", KR(ret), K(arg_));
  } else {}
  return ret;
}

int ObBroadcastConsensusVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->broadcast_consensus_version(arg_, result_);
  }
  return ret;
}

int ObRpcGetLSReplayedScnP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_ls_replayed_scn(arg_, result_))) {
    COMMON_LOG(WARN, "failed to get_ls_replayed_scn", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcCheckStorageOperationStatusP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->check_storage_operation_status(arg_, result_))) {
    LOG_WARN("failed to check storage operation status", K(ret));

  }
  return ret;
}

int ObTenantTTLP::process()
{
  int ret = OB_SUCCESS;
  ObTTLRequestArg &req = arg_;
  ObTTLResponseArg &res = result_;
  table::ObTTLService *ttl_service = nullptr;

  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(req));
  } else if (OB_UNLIKELY(req.tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(ERROR, "mtl_id not match", K(ret), K(req), "mtl_id", MTL_ID());
  } else if (OB_ISNULL(ttl_service = MTL(table::ObTTLService*))) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(ERROR, "ttl service is nullptr", KR(ret), K(req));
  } else if (OB_FAIL(ttl_service->launch_ttl_task(req))) {
    RS_LOG(WARN, "fail to launch ttl", KR(ret), K(req));
  }
  res.err_code_ = ret;
  return ret;
}

int ObAdminUnlockMemberListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->ob_admin_unlock_member_list(arg_))) {
    COMMON_LOG(WARN, "failed to unlock member list", KR(ret), K(arg_));
  }
  return ret;
}

int ObRpcNotifyTenantSnapshotSchedulerP::process()
{
  int ret = OB_SUCCESS;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  } else {
    MTL_SWITCH(gen_meta_tenant_id(arg_.get_tenant_id())) {
      rootserver::ObTenantSnapshotScheduler* tenant_snapshot_scheduler = MTL(rootserver::ObTenantSnapshotScheduler*);
      if (OB_ISNULL(tenant_snapshot_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant snapshot scheduler is null", KR(ret), K(arg_));
      } else {
        tenant_snapshot_scheduler->wakeup();
      }
    }
  }
  (void)result_.init(ret);
  return ret;
}

int ObRpcFlushLSArchiveP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  archive::ObArchiveService* archive_svr = MTL(archive::ObArchiveService*);

  if (!arg_.is_valid() || tenant_id != arg_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_), K(tenant_id));
  } else if (OB_ISNULL(archive_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive service is null", KR(ret), K(arg_));
  } else {
    archive_svr->flush_all();
  }
  result_ = ret;
  return ret;
}

int ObRpcNotifyCloneSchedulerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  } else {
    MTL_SWITCH(arg_.get_tenant_id()) {
      rootserver::ObCloneScheduler* clone_scheduler = MTL(rootserver::ObCloneScheduler*);
      if (OB_ISNULL(clone_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("clone scheduler is null", KR(ret), K(arg_));
      } else {
        clone_scheduler->wakeup();
      }
    }
  }
  (void)result_.init(ret);
  return ret;
}

int ObRpcNotifyTenantThreadP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive notify tenant thread", K(arg_));
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  } else {
    MTL_SWITCH(arg_.get_tenant_id()) {
      if (obrpc::ObNotifyTenantThreadArg::RECOVERY_LS_SERVICE == arg_.get_thread_type()) {
        rootserver::ObRecoveryLSService *ls_service =
          MTL(rootserver::ObRecoveryLSService *);
        if (OB_ISNULL(ls_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls service is null", KR(ret), K(arg_));
        } else {
          ls_service->wakeup();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected thread type", KR(ret), K(arg_));
      }
    }
  }
  return ret;
}

int ObKillQueryClientSessionP::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  uint32_t server_sess_id = INVALID_SESSID;
  if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "session_mgr_ is null", KR(ret));
  } else if (OB_FAIL(gctx_.session_mgr_->get_client_sess_map().get_refactored(
          arg_.get_client_sess_id(), server_sess_id))) {
    if (ret == OB_HASH_NOT_EXIST) {
      // no need to display info, if current server no this proxy session id.
      ret = OB_SUCCESS;
      LOG_DEBUG("current client session id not find", K(ret), K(arg_.get_client_sess_id()));
    } else {
      COMMON_LOG(WARN, "get session failed", KR(ret), K(arg_));
    }
  } else if (OB_FAIL(gctx_.session_mgr_->get_session(server_sess_id, session))) {
    LOG_INFO("fail to get session", K(ret), K(server_sess_id));
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(arg_.get_client_sess_id()));
  } else {
    if (OB_FAIL(gctx_.session_mgr_->kill_query(*session))) {
      LOG_WARN("fail to kill query", K(ret), K(arg_.get_client_sess_id()), K(server_sess_id));
    }
  }
  if (NULL != session) {
    gctx_.session_mgr_->revert_session(session);
  }
  return ret;
}

int ObKillClientSessionP::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  uint32_t server_sess_id = INVALID_SESSID;
  if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "session_mgr_ is null", KR(ret));
  } else if (OB_FAIL(gctx_.session_mgr_->get_client_sess_map().get_refactored(
          arg_.get_client_sess_id(), server_sess_id))) {
    if (ret == OB_HASH_NOT_EXIST) {
      // no need to display info, if current server no this proxy session id.
      ret = OB_SUCCESS;
      LOG_DEBUG("current client session id not find", K(ret), K(arg_.get_client_sess_id()));
    } else {
      COMMON_LOG(WARN, "get session failed", KR(ret), K(arg_));
    }
  } else if (OB_FAIL(gctx_.session_mgr_->get_session(server_sess_id, session))) {
    LOG_INFO("fail to get session", K(ret), K(server_sess_id));
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(arg_.get_client_sess_id()));
  } else {
    session->set_mark_killed(true);
    // Ensure smooth exit of executed requests.
    session->set_session_state(SESSION_KILLED);
  }
  if (NULL != session) {
    gctx_.session_mgr_->revert_session(session);
  }
  if (OB_SUCC(ret)) {
    // record kill_client_sess_map.
    int flag = 1;
    gctx_.session_mgr_->get_kill_client_sess_map().set_refactored(arg_.get_client_sess_id(),
                                      arg_.get_create_time(), flag);
    result_.set_can_kill_client_sess(true);
  }
  return ret;
}

int ObClientSessionConnectTimeP::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObString str_result;
  uint32_t server_sess_id = INVALID_SESSID;
  if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "session_mgr_ is null", KR(ret));
  } else if (OB_FAIL(gctx_.session_mgr_->get_client_sess_map().get_refactored(
          arg_.get_client_sess_id(), server_sess_id))) {
      COMMON_LOG(WARN, "get session failed", KR(ret), K(arg_));
  } else if (OB_FAIL(gctx_.session_mgr_->get_session(server_sess_id, session))) {
    LOG_WARN("fail to get session", K(ret), K(server_sess_id));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(arg_.get_client_sess_id()));
  } else {
    result_.set_client_create_time(session->get_client_create_time());
    if (((OB_SYS_TENANT_ID == arg_.get_tenant_id())
             || ((arg_.get_tenant_id() == session->get_priv_tenant_id())
              && (arg_.is_has_user_super_privilege() ||
              arg_.get_user_id() == session->get_user_id())))) {
      result_.set_have_kill_auth(true);
    } else {
      result_.set_have_kill_auth(false);
    }
    LOG_DEBUG("get connect time rpc", K(session->get_client_create_time()),
        K(session->get_sessid()), K(session->get_client_sessid()));
  }
  if (NULL != session) {
    gctx_.session_mgr_->revert_session(session);
  }
  return ret;
}

int ObTabletLocationReceiveP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.location_service_ is nullptr", KR(ret), KP(GCTX.location_service_));
  } else {
    FOREACH_CNT_X(it, arg_.get_tasks(), OB_SUCC(ret)) {
      if (OB_FAIL(GCTX.location_service_->submit_tablet_update_task(*it))) {
        LOG_WARN("failed to submit_tablet_update_tasks", KR(ret));
      }
    }
  }
  result_.set_ret(ret);
  return OB_SUCCESS;
}

int ObAllServerTracerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SVR_TRACER.refresh())) {
    LOG_WARN("failed to refresh all_server_tracer", KR(ret));
  } else {
    LOG_INFO("SVR_TRACER.refresh succeed");
  }
  return ret;
}

int ObCancelGatherStatsP::process()
{
  int ret = OB_SUCCESS;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg_), K(ret));
  } else if (OB_FAIL(ObOptStatGatherStatList::instance().cancel_gather_stats(arg_.tenant_id_,
                                                                             arg_.task_id_))) {
    LOG_WARN("failed to cancel gather stats", K(ret));
  }
  return ret;
}

int ObForceSetTenantLogDiskP::process()
{
  int ret = OB_SUCCESS;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else if (OB_FAIL(GCTX.log_block_mgr_->force_update_tenant_log_disk(arg_.tenant_id_, arg_.log_disk_size_))) {
    LOG_WARN("force_update_sys_tenant_log_disk failed", K(ret), "tenant_id", arg_.tenant_id_, "new_log_disk", arg_.log_disk_size_);
  } else {
    LOG_WARN("force_update_sys_tenant_log_disk success", K(ret), "tenant_id", arg_.tenant_id_, "new_log_disk", arg_.log_disk_size_);
  }
  return ret;
}

int ObRpcChangeExternalStorageDestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->change_external_storage_dest(arg_))) {
    COMMON_LOG(WARN, "failed to change external storage dest", KR(ret), K(arg_));
  }
  return ret;
}

class ObDumpUnitInfoFunctor {
public:
  ObDumpUnitInfoFunctor(ObSArray<ObDumpServerUsageResult::ObUnitInfo> &result) : result_(result) {}
  int operator()()
  {
    int ret = OB_SUCCESS;
    ObDumpServerUsageResult::ObUnitInfo info;
    info.tenant_id_ = MTL_ID();
    logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
    int64_t log_disk_size = 0, log_disk_in_use = 0;
    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_service is nullptr", KP(log_service));
    } else if (OB_FAIL(log_service->get_palf_stable_disk_usage(log_disk_in_use, log_disk_size))) {
      LOG_WARN("get_palf_stable_disk_usage failed", KP(log_service));
    } else {
      info.log_disk_in_use_ = log_disk_in_use;
      info.log_disk_size_ = log_disk_size;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result_.push_back(info))) {
      LOG_WARN("push_back failed", KR(ret), K(info));
    }
    return ret;
  }
private:
  ObSArray<ObDumpServerUsageResult::ObUnitInfo> &result_;
};
int ObForceDumpServerUsageP::process()
{
  int ret = OB_SUCCESS;
  int64_t &log_disk_assigned = result_.server_info_.log_disk_assigned_;
  int64_t &log_disk_capacity = result_.server_info_.log_disk_capacity_;
  ObSArray<ObDumpServerUsageResult::ObUnitInfo> &result = result_.unit_info_;
  ObDumpUnitInfoFunctor dump_unit_info(result);
  if (OB_FAIL(GCTX.omt_->operate_in_each_tenant(dump_unit_info))) {
    CLOG_LOG(WARN, "operate_in_each_tenant failed", KR(ret));
  } else if (OB_FAIL(GCTX.log_block_mgr_->get_disk_usage(log_disk_assigned, log_disk_capacity))) {
    CLOG_LOG(WARN, "get_disk_usage failed", KR(ret));
  } else {}
  return ret;
}

int ObRefreshServiceNameP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ob_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.ob_service_->refresh_service_name(arg_, result_))) {
    COMMON_LOG(WARN, "fail to refresh_service_name", KR(ret), K(arg_));
  }
  return ret;
}

int ObResourceLimitCalculatorP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObResourceLimitCalculator *)->get_tenant_min_phy_resource_value(result_))) {
    LOG_WARN("get physical resource needed by unit failed", K(ret));
  }
  return ret;
}

int ObCollectMvMergeInfoP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMajorMVMergeInfo merge_info;
  const share::ObLSID ls_id = arg_.get_ls_id();
  const uint64_t tenant_id = arg_.get_tenant_id();
  int64_t proposal_id = 0;

  MTL_SWITCH(tenant_id) {
    if (arg_.need_update() &&
        OB_FAIL(ObMVCheckReplicaHelper::get_and_update_merge_info(ls_id, merge_info))) {
      LOG_WARN("get and update merge info failed", K(ret));
    } else if (!arg_.need_update() &&
        OB_FAIL(ObMVCheckReplicaHelper::get_merge_info(ls_id, merge_info))) {
      LOG_WARN("get merge info failed", K(ret));
    } else if (arg_.need_check_leader()) {
      ObRole role;
      logservice::ObLogService *log_service = nullptr;
      if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log service should not be NULL", K(ret), KP(log_service));
      } else if (OB_FAIL(log_service->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("failed to get role", K(ret), K(arg_));
      } else if (!is_strong_leader(role)) {
        ret = OB_LS_NOT_LEADER;
        LOG_WARN("it is not leader, cannot collect merge info", K(ret), K(ls_id), K(role), K(arg_));
      }
    }
    if (OB_TMP_FAIL(result_.init(merge_info, ret))) {
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      LOG_WARN("init collect mv merge info result failed", K(ret), K(tmp_ret), K(merge_info));
    }
  }
  return ret;
}

int ObFetchStableMemberListP::process()
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = arg_.get_ls_id();
  const uint64_t tenant_id = arg_.get_tenant_id();
  // todo siyu :: use new stable member list interface
  MTL_SWITCH(tenant_id) {
    ObLSService *ls_svr = NULL;
    ObLSHandle ls_handle;
    ObLS *ls = NULL;
    logservice::ObLogHandler *log_handler = NULL;
    common::ObMemberList member_list;
    GlobalLearnerList learn_list;
    int64_t paxos_replica_num = 0;
    logservice::ObLogService *log_service = nullptr;
    palf::LogConfigVersion log_config_version;
    ObRole role;
    int64_t proposal_id = 0;
    int64_t proposal_id_new = 0;

    if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log service should not be NULL", K(ret), KP(log_service));
    } else if (OB_FAIL(log_service->get_palf_role(ls_id, role, proposal_id))) {
      LOG_WARN("failed to get role", K(ret), K(arg_));
    } else if (!is_strong_leader(role)) {
      ret = OB_LS_NOT_LEADER;
      LOG_WARN("ls is not leader, cannot get member list", K(ret), K(role), K(arg_));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", K(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret));
    } else if (OB_FAIL(log_handler->get_stable_membership(log_config_version, member_list,
                                                          paxos_replica_num, learn_list))) {
      LOG_WARN("failed to get paxos member list and log config version", K(ret));
    } else if (OB_FAIL(result_.init(member_list, log_config_version))) {
      LOG_WARN("failed to int member list and config version", K(ret), K(member_list), K(log_config_version));
    } else if (OB_FAIL(log_service->get_palf_role(ls_id, role, proposal_id_new))) {
      LOG_WARN("failed to get role", K(ret), K(arg_));
    } else if (proposal_id_new != proposal_id || !is_strong_leader(role)) {
      // double check for get stable memberlist
      ret = OB_LS_NOT_LEADER;
      LOG_WARN("ls is not leader, cannot get member list", K(ret), K(role), K(arg_));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObGetSSMacroBlockP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start dump ss_macro_block process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg_.tenant_id), K_(arg_.macro_id));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      char *buf = nullptr;
      ObStorageObjectReadInfo read_info;
      ObStorageObjectHandle object_handle;
      read_info.macro_block_id_ = arg_.macro_id_;
      read_info.offset_ = arg_.offset_;
      read_info.size_ = arg_.size_;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
      read_info.bypass_micro_cache_ = true;
      read_info.mtl_tenant_id_ = MTL_ID();
      if (OB_ISNULL(buf = static_cast<char *>(result_.allocator_.alloc(read_info.size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc mem for read buf", KR(ret), K_(read_info.size));
      } else if (OB_FALSE_IT(read_info.buf_ = buf)) {
      } else if (OB_FAIL(ObObjectManager::read_object(read_info, object_handle))) {
        LOG_WARN("fail to read macro block", KR(ret), K(read_info), K(object_handle));
      } else if (OB_LIKELY(object_handle.get_data_size() > 0)) {
        char *ptr = nullptr;
        if (OB_ISNULL(ptr = static_cast<char *>(result_.allocator_.alloc(object_handle.get_data_size())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc mem for macro_buf", KR(ret), K(object_handle.get_data_size()));
        } else {
          result_.macro_buf_.assign_buffer(ptr, object_handle.get_data_size());
          result_.macro_buf_.write(read_info.buf_, object_handle.get_data_size());
        }
      }
    }
  }
  return ret;
}

int ObGetSSPhyBlockInfoP::process()
{
  int ret = OB_SUCCESS;
  result_.ret_ = OB_SUCCESS;
  LOG_INFO("start get phy_block_info process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObSSMicroCache *micro_cache = nullptr;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObSSMicroCache is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(micro_cache->get_phy_block_info(arg_.phy_block_idx_, result_.ss_phy_block_info_))) {
        LOG_WARN("fail to get phy_block_info", KR(ret), K_(arg));
      }
    }
  }
  if (OB_INVALID_ARGUMENT == ret) {
    result_.ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObGetSSMicroBlockMetaP::process()
{
  int ret = OB_SUCCESS;
  result_.ret_ = OB_SUCCESS;
  LOG_INFO("start get micro block meta process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObSSMicroCache *micro_cache = nullptr;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      result_.micro_meta_info_.micro_key_ = arg_.micro_key_;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObSSMicroCache is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(micro_cache->get_micro_meta_handle(arg_.micro_key_, micro_meta_handle))) {
        LOG_WARN("fail to get micro block meta", KR(ret), K_(arg));
      } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta handle should be valid", KR(ret), K_(arg));
      } else {
        ObSSMicroMetaInfo &micro_meta_info = result_.micro_meta_info_;
        micro_meta_info.reuse_version_ = micro_meta_handle()->reuse_version();
        micro_meta_info.data_dest_ = micro_meta_handle()->data_dest();
        micro_meta_info.access_time_ = micro_meta_handle()->access_time();
        micro_meta_info.length_ = micro_meta_handle()->length();
        micro_meta_info.is_in_l1_ = micro_meta_handle()->is_in_l1();
        micro_meta_info.is_in_ghost_ = micro_meta_handle()->is_in_ghost();
        micro_meta_info.is_persisted_ = micro_meta_handle()->is_persisted();
        micro_meta_info.is_reorganizing_ = micro_meta_handle()->is_reorganizing();
        micro_meta_info.ref_cnt_ = micro_meta_handle()->ref_cnt();
        micro_meta_info.crc_ = micro_meta_handle()->crc();
        micro_meta_info.micro_key_ = micro_meta_handle()->get_micro_key();
      }
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    result_.ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObGetSSMacroBlockByURIP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start dump ss_macro_block by uri process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg_.tenant_id), K_(arg_.uri));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      char *macro_buf = nullptr;
      ObTenantFileManager *file_mgr = nullptr;
      ObBackupDest storage_dest;
      ObBackupIoAdapter adapter;
      int64_t read_size = 0;
      int64_t buf_size = arg_.size_;
      int64_t offset = arg_.offset_;
      if (OB_ISNULL(macro_buf = reinterpret_cast<char *>(result_.allocator_.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc macro_buf", KR(ret), K(buf_size));
      } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get file_mgr", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(file_mgr->get_storage_dest(storage_dest))) {
        LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
      } else if (OB_FAIL(adapter.read_part_file(arg_.uri_, storage_dest.get_storage_info(),
                     macro_buf, buf_size, offset, read_size, common::ObStorageIdMod()))) {
        LOG_WARN("fail to read part file", KR(ret), K_(arg_.uri), K(buf_size), K(offset));
      } else if (OB_LIKELY(read_size > 0)) {
        char *ptr = nullptr;
        if (OB_ISNULL(ptr = static_cast<char *>(result_.allocator_.alloc(read_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc mem for macro_buf", KR(ret), K(buf_size));
        } else {
          result_.macro_buf_.assign_buffer(ptr, read_size);
          result_.macro_buf_.write(macro_buf, read_size);
        }
      }
    }
  }
  return ret;
}

int ObDelSSTabletMetaP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start del ss_tablet_meta process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObTenantFileManager *file_mgr = nullptr;
      if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObTenantFileManager is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(file_mgr->delete_file(arg_.macro_id_))) {
        LOG_WARN("fail to delete ss_tablet_meta", KR(ret), K_(arg_.macro_id));
      }
    }
  }
  return ret;
}

int ObDelSSLocalTmpFileP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start delete ss_local_tmpfile process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObTenantFileManager *file_mgr = nullptr;
      if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObTenantFileManager is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(file_mgr->delete_local_tmp_file(arg_.macro_id_, true/* is_only_delete_read_cache */))) {
        LOG_WARN("fail to delete ss_local_tmpfile", KR(ret), K_(arg_.macro_id));
      }
    }
  }
  return ret;
}

int ObDelSSLocalMajorP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start delete ss_local_major process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObTenantFileManager *file_mgr = nullptr;
      const int64_t cur_time_s = ObTimeUtility::current_time_s();
      if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObTenantFileManager is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(file_mgr->delete_local_major_data_dir(cur_time_s))) {
        LOG_WARN("fail to delete ss_local_major", KR(ret), K(cur_time_s));
      }
    }
  }
  return ret;
}

int ObCalibrateSSDiskSpaceP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start calibrate_ss_disk_space process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObTenantFileManager *file_mgr = nullptr;
      if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObTenantFileManager is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(file_mgr->calibrate_disk_space())) {
        LOG_WARN("fail to calibrate_ss_disk_space", KR(ret));
      }
    }
  }
  return ret;
}

int ObDelSSTabletMicroP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start del_ss_tablet_micro process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObSSMicroCache *micro_cache = nullptr;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObSSMicroCache is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(micro_cache->clear_micro_meta_by_tablet_id(arg_.tablet_id_))) {
        LOG_WARN("fail to del_ss_tablet_micro", KR(ret));
      }
    }
  }
  return ret;
}


int ObEnableSSMicroCacheP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start enable ss_micro_cache process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObSSMicroCache *micro_cache = nullptr;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObSSMicroCache is null", KR(ret), K_(arg_.tenant_id));
      } else if (arg_.is_enabled_) {
        micro_cache->enable_cache();
      } else {
        micro_cache->disable_cache();
      }
    }
  }
  return ret;
}

int ObGetSSMicroCacheInfoP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start get ss_micro_cache_info process", K_(arg));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      ObSSMicroCache *micro_cache = nullptr;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObSSMicroCache is null", KR(ret), K_(arg_.tenant_id));
      } else if (OB_FAIL(micro_cache->get_micro_cache_info(
                     result_.micro_cache_stat_, result_.super_block_, result_.arc_info_))) {
        LOG_WARN("fail to get micro_cache_info", KR(ret), K_(arg_.tenant_id));
      }
    }
  }

  return ret;
}

int ObRpcClearSSMicroCacheP::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_)
    {
      ObSSMicroCache *micro_cache = nullptr;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_cache is nullptr", KR(ret));
      } else {
        micro_cache->clear_micro_cache();
        LOG_INFO("success clear ss_micro_cache");
      }
    }
  }
  return ret;
}

int ObSetSSCkptCompressorP::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.tenant_id_)
    {
      ObSSMicroCache *micro_cache = nullptr;
      if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_cache is nullptr", KR(ret));
      } else if (arg_.block_type_ == ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK) {
        micro_cache->set_micro_ckpt_compressor_type(arg_.compressor_type_);
      } else if (arg_.block_type_ == ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK) {
        micro_cache->set_blk_ckpt_compressor_type(arg_.compressor_type_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block_type is unexpected", K(ret), K_(arg_.block_type));
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("succeed to set_ss_ckpt_compressor", K_(arg));
      }
    }
  }
  return ret;
}
#endif

int ObRebuildTabletP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg_), K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "schedule_rebuild_tablet start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "data_src", arg_.src_, "dest", arg_.dest_);

    MTL_SWITCH(tenant_id) {
      ObLSService *ls_service = MTL(ObLSService*);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObRebuildService *rebuild_service = nullptr;
      ObLSRebuildInfo rebuild_info;
      rebuild_info.src_ = arg_.src_;
      rebuild_info.status_ = ObLSRebuildStatus::INIT;
      rebuild_info.type_ = ObLSRebuildType::TABLET;
      if (OB_FAIL(rebuild_info.tablet_id_array_.assign(arg_.tablet_id_array_))) {
        LOG_WARN("failed to assign tablet id array", K(ret), K(arg_));
      } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg_));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(MTL_ID()), K(arg_.ls_id_));
      } else if (OB_FAIL(ls->set_rebuild_info(rebuild_info))) {
        LOG_WARN("failed to set rebuild info", K(ret), K(rebuild_info));
      } else if (OB_ISNULL(rebuild_service = (MTL(ObRebuildService *)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rebuild service not be NULL", K(ret), KP(rebuild_service));
      } else {
        rebuild_service->wakeup();
      }
    }

    if (OB_FAIL(ret)) {
      SERVER_EVENT_ADD("storage_ha", "schedule_rebuild_tablet failed", "ls_id", arg_.ls_id_.id(), "result", ret);
    }
  }
  return ret;
}

int ObNotifySharedStorageInfoP::process()
{
  int ret = OB_SUCCESS;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(arg));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg_.get_shared_storage_infos().count(); i++) {
    const ObAdminStorageArg &shared_storage_info = arg_.get_shared_storage_infos().at(i);
    ObBackupDest storage_dest;
    char storage_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = {0};
    if (!shared_storage_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(shared_storage_info));
    } else if (OB_FAIL(databuff_printf(storage_dest_str, OB_MAX_BACKUP_DEST_LENGTH, "%s&%s",
                                      shared_storage_info.path_.ptr(), shared_storage_info.access_info_.ptr()))) {
      LOG_WARN("fail to set storage_dest_str", KR(ret), K(shared_storage_info));
    } else if (OB_FAIL(storage_dest.set(storage_dest_str))) {
      LOG_WARN("fail to set storage dest", KR(ret), K(shared_storage_info), K(storage_dest_str));
    } else if (OB_FAIL(ObDeviceConfigMgr::get_instance().set_storage_dest(shared_storage_info.use_for_, storage_dest))) {
      LOG_WARN("fail to set storage dest", KR(ret), K(shared_storage_info), K(storage_dest));
    }
  }
  result_.set_ret(ret);
  return ret;
}

int ObRpcNotifyLSRestoreFinishP::process()
{
  int ret = OB_SUCCESS;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  } else {
    MTL_SWITCH(gen_meta_tenant_id(arg_.get_tenant_id())) {
      rootserver::ObRestoreService* restore_service = MTL(rootserver::ObRestoreService*);
      if (OB_ISNULL(restore_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("restore service is null", KR(ret), K(arg_));
      } else {
        restore_service->wakeup();
      }
    }
  }
  return ret;
}

int ObRpcStartArchiveP::process()
{
  int ret = OB_SUCCESS;
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_));
  } else {
    MTL_SWITCH(gen_meta_tenant_id(arg_.get_tenant_id())) {
      rootserver::ObArchiveSchedulerService* archive_service = MTL(rootserver::ObArchiveSchedulerService*);
      if (OB_ISNULL(archive_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("archive service is null", KR(ret), K(arg_));
      } else {
        archive_service->wakeup();
      }
    }
  }
  return ret;
}
} // end of namespace observer
} // end of namespace oceanbase
