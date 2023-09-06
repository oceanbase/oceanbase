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

#include "observer/ob_rpc_processor_simple.h"

#include "share/io/ob_io_manager.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_tenant_mgr.h"
#include "share/config/ob_config_manager.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/ob_server_blacklist.h"
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"
#include "share/cache/ob_cache_name_define.h"
#include "share/ob_share_util.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "rootserver/ob_root_service.h"
#include "sql/ob_sql.h"
#include "observer/ob_service.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/mysql/ob_diag.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
// for 4.0
#include "share/ob_ls_id.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/sequence/ob_sequence_cache.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_handler.h"
#include "share/scn.h"
#include "storage/ob_common_id_utils.h"
#include "storage/high_availability/ob_storage_ha_service.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "observer/ob_req_time_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "rootserver/ob_primary_ls_service.h"//PrimaryLSService
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "storage/high_availability/ob_transfer_service.h" // ObTransferService
#include "sql/udr/ob_udr_mgr.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_controller.h"
#endif
#include "sql/plan_cache/ob_ps_cache.h"
#include "rootserver/ob_primary_ls_service.h" // for ObPrimaryLSService
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sess_info_verify.h"
#include "observer/table/ttl/ob_ttl_service.h"

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


int ObRpcLSMigrateReplicaP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  ObLSService *ls_service = nullptr;
  bool is_exist = false;
  ObMigrationOpArg migration_op_arg;

  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcLSMigrateReplicaP::proces tenant not match", K(tenant_id), K(ret));
  }
  ObCurTraceId::set(arg_.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "schedule_ls_migration start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "data_src", arg_.data_source_.get_server(), "dest", arg_.dst_.get_server());

    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->check_ls_exist(arg_.ls_id_, is_exist))) {
      COMMON_LOG(WARN, "failed to check ls exist", K(ret), K(arg_));
    } else if (is_exist) {
      ret = OB_LS_EXIST;
      COMMON_LOG(WARN, "can not migrate ls which local ls is exist", K(ret), K(arg_), K(is_exist));
    } else {
      migration_op_arg.cluster_id_ = GCONF.cluster_id;
      migration_op_arg.data_src_ = arg_.data_source_;
      migration_op_arg.dst_ = arg_.dst_;
      migration_op_arg.ls_id_ = arg_.ls_id_;
      //TODO(muwei.ym) need check priority in 4.2 RC3
      migration_op_arg.priority_ = ObMigrationOpPriority::PRIO_HIGH;
      migration_op_arg.paxos_replica_number_ = arg_.paxos_replica_number_;
      migration_op_arg.src_ = arg_.src_;
      migration_op_arg.type_ = ObMigrationOpType::MIGRATE_LS_OP;

      if (OB_FAIL(ls_service->create_ls_for_ha(arg_.task_id_, migration_op_arg))) {
        COMMON_LOG(WARN, "failed to create ls for ha", K(ret), K(arg_), K(migration_op_arg));
      }
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "schedule_ls_migration failed", "ls_id", arg_.ls_id_.id(), "result", ret);
  }

  return ret;
}

int ObRpcLSAddReplicaP::process()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg_.tenant_id_;
  ObLSService *ls_service = nullptr;
  bool is_exist = false;
  ObMigrationOpArg migration_op_arg;

  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcLSAddReplicaP::process tenant not match", K(tenant_id), K(ret));
  }
  ObCurTraceId::set(arg_.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "schedule_ls_add start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "data_src", arg_.data_source_.get_server(), "dest", arg_.dst_.get_server());

    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->check_ls_exist(arg_.ls_id_, is_exist))) {
      COMMON_LOG(WARN, "failed to check ls exist", K(ret), K(arg_));
    } else if (is_exist) {
      ret = OB_LS_EXIST;
      COMMON_LOG(WARN, "can not add ls which local ls is exist", K(ret), K(arg_), K(is_exist));
    } else {
      migration_op_arg.cluster_id_ = GCONF.cluster_id;
      migration_op_arg.data_src_ = arg_.data_source_;
      migration_op_arg.dst_ = arg_.dst_;
      migration_op_arg.ls_id_ = arg_.ls_id_;
      //TODO(muwei.ym) need check priority in 4.2 RC3
      migration_op_arg.priority_ = ObMigrationOpPriority::PRIO_HIGH;
      migration_op_arg.paxos_replica_number_ = arg_.new_paxos_replica_number_;
      migration_op_arg.src_ = arg_.data_source_;
      migration_op_arg.type_ = ObMigrationOpType::ADD_LS_OP;

      if (OB_FAIL(ls_service->create_ls_for_ha(arg_.task_id_, migration_op_arg))) {
        COMMON_LOG(WARN, "failed to create ls for ha", K(ret), K(arg_), K(migration_op_arg));
      }
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "schedule_ls_add failed", "tenant_id", arg_.tenant_id_,
        "ls_id", arg_.ls_id_, "result", ret);
  }
  return ret;
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
    SERVER_EVENT_ADD("storage_ha", "remove_ls_paxos_member start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "dest", arg_.remove_member_.get_server());
    LOG_INFO("start do remove ls paxos member", K(arg_));

    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(arg_));
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->remove_paxos_member(arg_))) {
      LOG_WARN("failed to remove paxos member", K(ret), K(arg_));
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "remove_ls_paxos_member failed", "tenant_id",
        arg_.tenant_id_, "ls_id", arg_.ls_id_.id(), "result", ret);
  }
  return ret;
}

int ObRpcLSRemoveNonPaxosReplicaP::process()
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
    SERVER_EVENT_ADD("storage_ha", "remove_ls_learner_member start", "tenant_id", arg_.tenant_id_, "ls_id", arg_.ls_id_.id(),
                     "dest", arg_.remove_member_.get_server());
    LOG_INFO("start do remove ls learner member", K(arg_));

    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(arg_));
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->remove_learner_member(arg_))) {
      LOG_WARN("failed to remove paxos member", K(ret), K(arg_));
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "remove_ls_learner_member failed", "tenant_id",
        arg_.tenant_id_, "ls_id", arg_.ls_id_.id(), "result", ret);
  }
  return ret;
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
  return OB_SUCCESS;
}

int ObRpcNotifyTenantServerUnitResourceP::process()
{
  int ret = OB_SUCCESS;
  if (arg_.is_delete_) {
    if (OB_FAIL(ObTenantNodeBalancer::get_instance().try_notify_drop_tenant(arg_.tenant_id_))) {
      LOG_WARN("fail to try drop tenant", K(ret), K(arg_));
    }
  } else {
    if (OB_FAIL(ObTenantNodeBalancer::get_instance().notify_create_tenant(arg_))) {
      LOG_WARN("failed to notify update tenant", K(ret), K_(arg));
    }
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

int ObRpcIsEmptyServerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->is_empty_server(arg_, result_);
  }
  return ret;
}

int ObRpcCheckServerForAddingServerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KP(gctx_.ob_service_));
  } else if (OB_FAIL(gctx_.ob_service_->check_server_for_adding_server(arg_, result_))) {
    LOG_WARN("fail to call check_server_for_adding_server", KR(ret), K(arg_));
  } else {}
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
      ObIMemtableMgr *memtable_mgr = nullptr;
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
      } else if (OB_ISNULL(memtable_mgr = tablet_handle.get_obj()->get_memtable_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "memtable mgr is null", K(ret));
      } else if (OB_FAIL(memtable_mgr->get_all_memtables(tables_handle))) {
        SERVER_LOG(WARN, "fail to get all memtables for log stream", K(ret));
      } else {
        memtable::ObMemtable *mt;
        mkdir("/tmp/dump_memtable/", S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.count(); i++) {
          if (OB_FAIL(tables_handle.at(i).get_data_memtable(mt))) {
            SERVER_LOG(WARN, "fail to get data memtables", K(ret));
          } else {
            TRANS_LOG(INFO, "start dump memtable", K(*mt), K(arg_));
            mt->dump2text("/tmp/dump_memtable/memtable.txt");
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
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  if (OB_SUCC(ret)) {
    ls_svr = MTL(ObLSService*);
    ObLS *ls = nullptr;
    ObLSID ls_id = arg_.get_ls_id();
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
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
      if (OB_UNLIKELY(!arg_.get_sys_ls_end_scn().is_valid_and_not_min())) {
        FLOG_WARN("invalid sys_ls_end_scn, no need to let user ls wait, "
            "the version might be smaller than V4.2.0", KR(ret), K(arg_.get_sys_ls_end_scn()));
      } else if (OB_FAIL(share::ObShareUtil::wait_user_ls_sync_scn_locally(
            arg_.get_sys_ls_end_scn(),
            *ls))) {
        LOG_WARN("fail to wait user ls sync scn locally", KR(ret), K(ls_id), K(arg_.get_sys_ls_end_scn()));
      }
    }
    const int64_t timeout = THIS_WORKER.get_timeout_remain();
    if (FAILEDx(log_handler->change_access_mode(
        arg_.get_mode_version(),
        arg_.get_access_mode(),
        arg_.get_ref_scn()))) {
      LOG_WARN("failed to change access mode", KR(ret), K(arg_), K(timeout));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = result_.init(tenant_id, ls_id, ret))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to init res", KR(ret), K(tenant_id), K(ls_id), KR(tmp_ret));
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
  int ret = OB_NOT_SUPPORTED;
  // ObPartitionService *partition_service = gctx_.par_ser_;
  // TRANS_LOG(INFO, "force_set_server_list");
  // if (NULL == partition_service) {
  //   ret = OB_ERR_UNEXPECTED;
  //   TRANS_LOG(ERROR, "partition_service is NULL");
  // } else {
  //   storage::ObIPartitionGroupIterator *partition_iter = NULL;
  //   if (NULL == (partition_iter = partition_service->alloc_pg_iter())) {
  //     ret = OB_ALLOCATE_MEMORY_FAILED;
  //     TRANS_LOG(ERROR, "partition_mgr alloc_scan_iter failed", K(ret));
  //   } else {
  //     storage::ObIPartitionGroup *partition = NULL;
  //     ObIPartitionLogService *pls = NULL;
  //     while (OB_SUCC(ret)) {
  //       int tmp_ret = OB_SUCCESS;
  //       if (OB_FAIL(partition_iter->get_next(partition)) || NULL == partition) {
  //         TRANS_LOG(INFO, "get_next failed or partition is NULL", K(ret));
  //       } else if (!partition->is_valid() || (NULL == (pls = partition->get_log_service()))) {
  //         TRANS_LOG(INFO, "partition is invalid or pls is NULL", "partition_key", partition->get_partition_key());
  //       } else if (OB_SUCCESS != (tmp_ret = pls->force_set_server_list(arg_.server_list_, arg_.replica_num_))) {
  //         TRANS_LOG(WARN, "force_set_server_list failed", K(ret), K(tmp_ret), "partition_key",
  //             partition->get_partition_key());
  //       }
  //     }
  //   }

  //   if (NULL != partition_iter) {
  //     partition_service->revert_pg_iter(partition_iter);
  //     partition_iter = NULL;
  //   }
  //   if (OB_ITER_END == ret) {
  //     ret = OB_SUCCESS;
  //   }
  // }
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
      ObDDLSSTableRedoWriter sstable_redo_writer;
      MacroBlockId macro_block_id;
      ObMacroBlockHandle macro_handle;
      ObMacroBlockWriteInfo write_info;
      ObLSService *ls_service = MTL(ObLSService*);
      ObLSHandle ls_handle;
      ObTabletHandle tablet_handle;
      ObDDLKvMgrHandle ddl_kv_mgr_handle;
      ObLS *ls = nullptr;

      // restruct write_info
      write_info.buffer_ = arg_.redo_info_.data_buffer_.ptr();
      write_info.size_= arg_.redo_info_.data_buffer_.length();
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      const int64_t io_timeout_ms = max(DDL_FLUSH_MACRO_BLOCK_TIMEOUT / 1000L, GCONF._data_storage_io_timeout / 1000L);
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
      } else if (OB_FAIL(ls->get_tablet(arg_.redo_info_.table_key_.tablet_id_, tablet_handle))) {
        LOG_WARN("get tablet failed", K(ret));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
        LOG_WARN("get ddl kv manager failed", K(ret));
      } else if (OB_FAIL(ObBlockManager::async_write_block(write_info, macro_handle))) {
        LOG_WARN("fail to async write block", K(ret), K(write_info), K(macro_handle));
      } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
        LOG_WARN("fail to wait macro block io finish", K(ret));
      } else if (OB_FAIL(sstable_redo_writer.init(arg_.ls_id_, arg_.redo_info_.table_key_.tablet_id_))) {
        LOG_WARN("init sstable redo writer", K(ret), K_(arg));
      } else if (OB_FAIL(sstable_redo_writer.write_redo_log(arg_.redo_info_, macro_handle.get_macro_id(), false, arg_.task_id_, tablet_handle, ddl_kv_mgr_handle))) {
        LOG_WARN("fail to write macro redo", K(ret), K_(arg));
      } else if (OB_FAIL(sstable_redo_writer.wait_redo_log_finish(arg_.redo_info_,
                                                                  macro_handle.get_macro_id()))) {
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
    ObDDLSSTableRedoWriter sstable_redo_writer;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    ObLS *ls = nullptr;
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
    } else if (OB_FAIL(ls->get_tablet(table_key.tablet_id_, tablet_handle))) {
      LOG_WARN("get tablet failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("get ddl kv manager failed", K(ret));
      }
    } else if (OB_FAIL(sstable_redo_writer.init(arg_.ls_id_, table_key.tablet_id_))) {
      LOG_WARN("init sstable redo writer", K(ret), K(table_key));
    } else if (FALSE_IT(sstable_redo_writer.set_start_scn(arg_.start_scn_))) {
    } else {
      SCN commit_scn;
      bool is_remote_write = false;
      if (OB_FAIL(sstable_redo_writer.write_commit_log(tablet_handle,
                                                       ddl_kv_mgr_handle,
                                                       false,
                                                       table_key,
                                                       commit_scn,
                                                       is_remote_write))) {
        LOG_WARN("fail to remote write commit log", K(ret), K(table_key), K_(arg));
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_commit(*tablet_handle.get_obj(), arg_.start_scn_, commit_scn))) {
        LOG_WARN("failed to do ddl kv commit", K(ret), K(arg_));
      } else {
        result_ = commit_scn.get_val_for_tx();
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
  } else if (OB_FAIL(sequence_cache.remove(MTL_ID(), sequence_id))) {
    LOG_WARN("remove sequence item from sequence cache failed", K(ret), K(sequence_id));
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
  } else if (OB_FAIL(tx_svc->register_mds_into_tx(*(arg_.tx_desc_), arg_.ls_id_, arg_.type_,
                                                  arg_.buf_.ptr(), arg_.buf_.length(),
                                                  arg_.request_id_, arg_.register_flag_))) {
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
    if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_checkpoint(true/*is_force*/))) {
      LOG_WARN("fail to write server checkpoint", K(ret));
    }
  } else {
    MTL_SWITCH(arg_.tenant_id_) {
      if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->write_checkpoint(true/*is_force*/))) {
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
          cache_evict_baseline_by_sql_id(arg.database_id_, arg.sql_id_))) {
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
    if (OB_INVALID_ID == arg_.tenant_id_ || arg_.sql_id_.empty()) {  // load appointed tenant cache
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

  if (OB_UNLIKELY(arg_.get_tenant_id() != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcStartTransferTaskP::process tenant not match", KR(ret), K_(arg));
  } else if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K_(arg));
  } else if (OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(transfer_service));
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
  ret = OB_SUCCESS;
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


} // end of namespace observer
} // end of namespace oceanbase
