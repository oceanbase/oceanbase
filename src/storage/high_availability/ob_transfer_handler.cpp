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

#define USING_LOG_PREFIX STORAGE
#include "ob_transfer_handler.h"
#include "ob_transfer_service.h"
#include "logservice/ob_log_service.h"
#include "ob_storage_ha_reader.h"
#include "ob_finish_transfer.h"
#include "storage/tx/ob_multi_data_source.h"
#include "share/transfer/ob_transfer_task_operator.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "ob_transfer_lock_info_operator.h"
#include "ob_transfer_lock_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/report/ob_tablet_table_updater.h"
#include "ob_storage_ha_utils.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "ob_rebuild_service.h"
#include "storage/tablet/ob_tablet.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"
#include "storage/tx/wrs/ob_weak_read_util.h"

using namespace oceanbase::transaction;
using namespace oceanbase::share;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{

//errsim def
ERRSIM_POINT_DEF(EN_START_TRANS_FAILED);
ERRSIM_POINT_DEF(EN_LOCK_TRANSFER_TASK_FAILED);
ERRSIM_POINT_DEF(EN_LOCK_TRANSFER_MEMBER_LIST_FAILED);
ERRSIM_POINT_DEF(EN_TRANSFER_CHECK_MEMBER_LIST_NOT_SAME);
ERRSIM_POINT_DEF(EN_CHECK_START_TRANSFER_STATUS_FAILED);
ERRSIM_POINT_DEF(EN_CHECK_ACTIVE_TRANS_FAILED);
ERRSIM_POINT_DEF(EN_START_TRANSFER_OUT_FAILED);
ERRSIM_POINT_DEF(EN_GET_TRANSFER_START_SCN_FAILED);
ERRSIM_POINT_DEF(EN_WAIT_SRC_REPALY_TO_START_SCN_FAILED);
ERRSIM_POINT_DEF(EN_GET_TRANSFER_TABLET_META_FAILED);
ERRSIM_POINT_DEF(EN_START_TRANSFER_IN_FAILED);
ERRSIM_POINT_DEF(EN_UPDATE_ALL_TABLET_TO_LS_FAILED);
ERRSIM_POINT_DEF(EN_UPDATE_TRANSFER_TASK_FAILED);
ERRSIM_POINT_DEF(EN_START_CAN_NOT_RETRY);
ERRSIM_POINT_DEF(EN_MAKE_SRC_LS_REBUILD);
ERRSIM_POINT_DEF(EN_INSERT_TRANSFER_START_FAILED);
ERRSIM_POINT_DEF(EN_TRANSFER_DIAGNOSE_START_FAILED);
ERRSIM_POINT_DEF(EN_TRANSFER_DIAGNOSE_DOING_FAILED);
ERRSIM_POINT_DEF(EN_TRANSFER_DIAGNOSE_ABORT_FAILED);
ERRSIM_POINT_DEF(EN_TRANSFER_DIAGNOSE_BACKFILL_FAILED);

ObTransferHandler::ObTransferHandler()
  : is_inited_(false),
    ls_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    sql_proxy_(nullptr),
    retry_count_(0),
    transfer_worker_mgr_(),
    round_(0),
    gts_seq_(share::SCN::base_scn()),
    related_info_(),
    task_info_(),
    diagnose_result_msg_(share::ObStorageHACostItemName::MAX_NAME),
    transfer_handler_lock_(),
    transfer_handler_enabled_(true)
{
}

ObTransferHandler::~ObTransferHandler()
{
}

int ObTransferHandler::init(
    ObLS *ls,
    common::ObInOutBandwidthThrottle *bandwidth_throttle,
    obrpc::ObStorageRpcProxy *svr_rpc_proxy,
    storage::ObStorageRpc *storage_rpc,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer handler init twice", K(ret));
  } else if (OB_ISNULL(ls) || OB_ISNULL(bandwidth_throttle) || OB_ISNULL(svr_rpc_proxy)
      || OB_ISNULL(storage_rpc) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init transfer handler get inavlid argument", K(ret), KP(ls), KP(bandwidth_throttle),
        KP(svr_rpc_proxy), KP(storage_rpc), KP(sql_proxy));
  } else if (OB_FAIL(transfer_worker_mgr_.init(ls))) {
    LOG_WARN("failed to init transfer worker manager", K(ret), KP(ls));
  } else if (OB_FAIL(related_info_.init())) {
    LOG_WARN("failed to init related_info");
  } else {
    ls_ = ls;
    bandwidth_throttle_ = bandwidth_throttle;
    svr_rpc_proxy_ = svr_rpc_proxy;
    storage_rpc_ = storage_rpc;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObTransferHandler::wakeup_()
{
  int ret = OB_SUCCESS;
  ObTransferService *transfer_service = MTL(ObTransferService*);
  if (OB_ISNULL(transfer_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("transfer service should not be NULL", K(ret), KP(transfer_service));
  } else {
    transfer_service->wakeup();
  }
}

int ObTransferHandler::wakeup_dest_ls_leader_(const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  const share::ObLSID &dest_ls_id = task_info.dest_ls_id_;
  ObLSService *ls_svr = NULL;
  common::ObAddr leader_addr;
  ObStorageHASrcInfo src_info;
  ObStorageRpc *storage_rpc = NULL;
  if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_ISNULL(storage_rpc = ls_svr->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc should not be NULL", K(ret), KP(storage_rpc));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(tenant_id, dest_ls_id, leader_addr))) {
    LOG_WARN("failed to get ls leader", K(ret), K(tenant_id));
  } else {
    src_info.src_addr_ = leader_addr;
    src_info.cluster_id_ = GCONF.cluster_id;
    if (OB_FAIL(storage_rpc->wakeup_transfer_service(tenant_id, src_info))) {
      LOG_WARN("failed to wakeup dest ls leader", K(ret), K(task_info), K(src_info));
    }
  }
  return ret;
}

int ObTransferHandler::get_transfer_task_(ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(fetch_transfer_task_from_inner_table_(task_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get transfer task from inner table", K(ret), KPC(ls_));
    }
  } else if (!task_info.status_.is_start_status() && !task_info.status_.is_doing_status()
      && !task_info.status_.is_aborted_status()) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTransferHandler::get_transfer_task_from_inner_table_(
    const ObTransferTaskID &task_id,
    const bool for_update,
    common::ObISQLClient &trans,
    share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  const uint64_t tenant_id = MTL_ID();
  ObTransferTask task;
  if (! task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get(trans, tenant_id, task_id, for_update, task, share::OBCG_TRANSFER))) {
    LOG_WARN("failed to get transfer task", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(task_info.convert_from(tenant_id, task))) {
    LOG_WARN("failed to convert from transfer task", K(ret), K(task));
  } else {
    LOG_INFO("get transfer task from inner table", K(task_info));
  }
  return ret;
}

int ObTransferHandler::fetch_transfer_task_from_inner_table_(
    share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  // currently START stage is executed on src ls leader
  // and DOING stage is executed on dest ls leader
  // so here try to fetch task by src ls first, then dest ls later
  // either one succeeded will return the task
  bool src_exist = false;
  bool dst_exist = false;
  share::ObTransferTaskInfo src_task_info;
  share::ObTransferTaskInfo dst_task_info;
  if (OB_FAIL(fetch_transfer_task_from_inner_table_by_src_ls_(src_task_info, src_exist))) {
    LOG_WARN("failed to fetch transfer task from inner table by src ls", K(ret));
  } else if (OB_FAIL(fetch_transfer_task_from_inner_table_by_dest_ls_(dst_task_info, dst_exist))) {
    LOG_WARN("failed to fetch transfer task from inner table by dst ls", K(ret));
  } else if (src_exist && dst_exist) {
    ret = OB_SCHEDULER_TASK_CNT_MISTACH;
    LOG_WARN("src task info and dst task info transfer ls overlap", K(ret), K(src_task_info), K(dst_task_info));
  } else if (src_exist && OB_FAIL(task_info.assign(src_task_info))) {
    LOG_WARN("failed to assign task info", K(ret), K(src_task_info));
  } else if (dst_exist && OB_FAIL(task_info.assign(dst_task_info))) {
    LOG_WARN("failed to assign task info", K(ret), K(dst_task_info));
  }
  return ret;
}

int ObTransferHandler::fetch_transfer_task_from_inner_table_by_src_ls_(
    share::ObTransferTaskInfo &task_info,
    bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;
  task_info.reset();
  const uint64_t tenant_id = MTL_ID();
  const ObLSID &src_ls_id = ls_->get_ls_id();
  ObTransferTask task;
  if (OB_FAIL(ObTransferTaskOperator::get_by_src_ls(
      *sql_proxy_, tenant_id, src_ls_id, task, share::OBCG_STORAGE))) {
    LOG_WARN("failed to get transfer task", K(ret), K(tenant_id), K(src_ls_id));
  } else if (OB_FAIL(task_info.convert_from(tenant_id, task))) {
    LOG_WARN("failed to convert from transfer task", K(ret), K(task));
  } else if (OB_FAIL(check_task_exist_(task_info.status_, true/*find_by_src_ls*/, task_exist))) {
    LOG_WARN("failed to get task exist", K(ret), K(task_info.status_));
  }

  if (OB_ENTRY_NOT_EXIST == ret || OB_TABLE_NOT_EXIST == ret) {
    task_exist = false;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTransferHandler::fetch_transfer_task_from_inner_table_by_dest_ls_(
    share::ObTransferTaskInfo &task_info,
    bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;
  task_info.reset();
  const uint64_t tenant_id = MTL_ID();
  const ObLSID &dest_ls_id = ls_->get_ls_id();
  ObTransferTask task;
  if (OB_FAIL(ObTransferTaskOperator::get_by_dest_ls(
      *sql_proxy_, tenant_id, dest_ls_id, task, share::OBCG_STORAGE))) {
    LOG_WARN("failed to get transfer task by dest ls", K(ret), K(tenant_id), K(dest_ls_id));
  } else if (OB_FAIL(task_info.convert_from(tenant_id, task))) {
    LOG_WARN("failed to convert from transfer task", K(ret), K(task));
  } else if (OB_FAIL(check_task_exist_(task_info.status_, false/*find_by_src_ls*/, task_exist))) {
    LOG_WARN("failed to get task exist", K(ret), K(task_info.status_));
  }
  if (OB_ENTRY_NOT_EXIST == ret || OB_TABLE_NOT_EXIST == ret) {
    task_exist = false;
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObTransferHandler::destroy()
{
  if (is_inited_) {
    ls_ = nullptr;
    bandwidth_throttle_ = nullptr;
    svr_rpc_proxy_ = nullptr;
    storage_rpc_ = nullptr;
    sql_proxy_ = nullptr;
    related_info_.destroy();
    task_info_.reset();
    diagnose_result_msg_ = share::ObStorageHACostItemName::MAX_NAME;
    is_inited_ = false;
  }
}

void ObTransferHandler::switch_to_follower_forcedly()
{
  LOG_INFO("[TRANSFER]switch to follower finish");
}

int ObTransferHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  wakeup_();
  LOG_INFO("[TRANSFER]switch to leader finish");
  return ret;
}

int ObTransferHandler::switch_to_follower_gracefully()
{
  LOG_INFO("[TRANSFER]switch to follower gracefully");
  return OB_SUCCESS;
}

int ObTransferHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  wakeup_();
  LOG_INFO("[TRANSFER]resume leader finish");
  return ret;
}

int ObTransferHandler::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  UNUSED(buffer);
  UNUSED(nbytes);
  UNUSED(lsn);
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObTransferHandler::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObTransferHandler::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  common::SpinRLockGuard guard(transfer_handler_lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!transfer_handler_enabled_) {
    LOG_INFO("transfer handler do not enable, ls may offline");
  } else if (OB_FAIL(do_leader_transfer_())) {
    LOG_WARN("failed to do leader transfer", K(ret));
  } else if (OB_FAIL(do_worker_transfer_())) {
    LOG_WARN("failed to do worker transfer", K(ret));
  }
  if (OB_TMP_FAIL(do_clean_diagnose_info_())) {
    LOG_WARN("failed to clean diagnose info", K(tmp_ret));
  }
  return ret;
}

int ObTransferHandler::do_leader_transfer_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  ObTransferTaskInfo task_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(check_self_is_leader_(is_leader))) {
    LOG_WARN("failed to check self is leader", K(ret), KPC(ls_));
  } else if (!is_leader) {
    //need retry by dest_ls new leader
  } else if (OB_FAIL(get_transfer_task_(task_info))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get transfer task", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(task_info_.assign(task_info))) {
    LOG_WARN("fail to assign task info", K(ret), K(task_info));
  } else {
    round_++;
    ObCurTraceId::set(task_info.trace_id_);
    switch (task_info.status_) {
    case ObTransferStatus::START: {
      if (OB_FAIL(do_with_start_status_(task_info))) {
        LOG_WARN("failed to do with start status", K(ret), K(task_info));
      }
      break;
    }
    case ObTransferStatus::DOING : {
      if (OB_FAIL(do_with_doing_status_(task_info))) {
        LOG_WARN("failed to do with doing status", K(ret), K(task_info));
      }
      break;
    }
    case ObTransferStatus::ABORTED : {
      if (OB_FAIL(do_with_aborted_status_(task_info))) {
        LOG_WARN("failed to do with aborted status", K(ret), K(task_info));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(task_info));
    }
    }
  }
  return ret;
}

int ObTransferHandler::check_self_is_leader_(bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObRole role = ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  const uint64_t tenant_id = MTL_ID();
  bool is_primary_tenant = true;
  is_leader = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(ObStorageHAUtils::check_is_primary_tenant(tenant_id, is_primary_tenant))) {
    //Setting the tenant as the primary tenant from the standby tenant and changing the leader attribute of palf are not atomic.
    //The attributes of palf will change first, and then the attributes of the tenant will change.
    LOG_WARN("failed to check is primary tenant", K(ret), K(tenant_id));
  } else if (!is_primary_tenant) {
    is_leader = false;
  } else if (OB_FAIL(ls_->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("failed to get role", K(ret), KPC(ls_));
  } else if (is_strong_leader(role)) {
    is_leader = true;
  } else {
    is_leader = false;
  }
  return ret;
}

int ObTransferHandler::do_with_start_status_(const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  LOG_INFO("[TRANSFER] start do with start status", K(task_info));

  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  bool enable_kill_trx = false;
  bool succ_stop_medium = false;
  palf::LogConfigVersion config_version;
  bool is_leader = true;
  bool succ_block_tx = false;
  int64_t tablet_stop_begin = 0;
  int64_t tmp_round = round_;
  diagnose_result_msg_ = share::ObStorageHACostItemName::MAX_NAME;
  process_perf_diagnose_info_(ObStorageHACostItemName::TRANSFER_START_BEGIN,
        ObStorageHADiagTaskType::TRANSFER_START, start_ts, round_, false/*is_report*/);
  bool commit_succ = false;
  bool new_transfer = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(enable_new_transfer(new_transfer))) {
    LOG_WARN("fail to fetch new transfer", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do with start status get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(ObTransferUtils::get_gts(task_info.tenant_id_, gts_seq_))) {
    LOG_WARN("failed to get gts seq", K(ret), K(task_info));
  } else if (OB_FAIL(start_trans_(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret), K(task_info));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_START_TRANS_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_START_TRANS_FAILED", K(ret));
        SERVER_EVENT_SYNC_ADD("TRANSFER", "START_TRANS_FAILED");
      }
    }
#endif
    if (FAILEDx(lock_transfer_task_(task_info, trans))) {
      LOG_WARN("failed to lock transfer task", K(ret), K(task_info));
    } else {
#ifdef ERRSIM
      ObTransferEventRecorder::record_transfer_task_event(
        task_info.task_id_, "START_TRANSFER_TRANS", task_info.src_ls_id_, task_info.dest_ls_id_);
#endif
      DEBUG_SYNC(START_TRANSFER_TRANS);
    }
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ObSEArray<ObTabletID, 100> tablet_ids; // (0, 100], default 32, see ObTenantTransferService::get_tablet_count_threshold_(),
    tablet_ids.set_attr(ObMemAttr(MTL_ID(), "TransferTblts"));
    if (tenant_config.is_valid()) {
      enable_kill_trx = tenant_config->_enable_balance_kill_transaction;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(lock_src_and_dest_ls_member_list_(task_info, task_info.src_ls_id_, task_info.dest_ls_id_))) {
      LOG_WARN("failed to lock src and dest ls member list", K(ret), K(task_info));
    } // The transaction can only be killed after checking the tablet, so as to avoid too long writing ban time.
    else if (OB_FAIL(get_config_version_(config_version))) {
      LOG_WARN("failed to get config version", K(ret), K(task_info));
    } else if (OB_FAIL(check_self_is_leader_(is_leader))) {
      LOG_WARN("failed to check self is leader", K(ret), KPC(ls_));
    } else if (!is_leader) {
      ret = OB_NOT_MASTER;
      LOG_WARN("transfer src is not leader", K(ret), K(task_info));
    } else if (OB_FAIL(precheck_ls_replay_scn_(task_info))) {
      LOG_WARN("failed to precheck ls replay scn", K(ret), K(task_info));
    } else if (OB_FAIL(task_info.fill_tablet_ids(tablet_ids))) {
      LOG_WARN("failed to init tablet_ids", K(ret), K(task_info));
    } else if (OB_FAIL(stop_tablets_schedule_medium_(tablet_ids, succ_stop_medium))) {
      LOG_WARN("failed to stop tablets schedule medium", K(ret), K(task_info));
    } else if (OB_FAIL(check_start_status_transfer_tablets_(task_info))) {
      LOG_WARN("failed to check start status transfer tablets", K(ret), K(task_info));
    } else if (!new_transfer && !enable_kill_trx && OB_FAIL(check_src_ls_has_active_trans_(task_info.src_ls_id_))) {
      LOG_WARN("failed to check src ls active trans", K(ret), K(task_info));
    } else if (OB_FAIL(update_all_tablet_to_ls_(task_info, trans))) {
      LOG_WARN("failed to update all tablet to ls", K(ret), K(task_info));
    } else if (OB_FAIL(lock_tablet_on_dest_ls_for_table_lock_(task_info, trans))) {
      LOG_WARN("failed to lock tablet on dest ls for table lock", KR(ret), K(task_info));
    } else if (!new_transfer && OB_FAIL(block_and_kill_tx_(task_info, enable_kill_trx, timeout_ctx, succ_block_tx))) {
      LOG_WARN("failed to block and kill tx", K(ret), K(task_info));
    } else if (new_transfer && OB_FAIL(do_trans_transfer_start_prepare_(task_info, timeout_ctx, trans))) {
      LOG_WARN("failed to do trans transfer start prepare", K(ret), K(task_info));
    } else if (OB_FAIL(reset_timeout_for_trans_(timeout_ctx))) {
      LOG_WARN("failed to reset timeout for trans", K(ret));
    } else if (!new_transfer && OB_FAIL(do_trans_transfer_start_(task_info, config_version, timeout_ctx, trans))) {
      LOG_WARN("failed to do trans transfer start", K(ret), K(task_info));
    } else if (new_transfer && FALSE_IT(tablet_stop_begin = ObTimeUtil::current_time())) {
    } else if (new_transfer && OB_FAIL(do_trans_transfer_start_v2_(task_info, timeout_ctx, trans))) {
      LOG_WARN("failed to do trans transfer start", K(ret), K(task_info));
    } else {
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_INSERT_TRANSFER_START_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("transfer_errsim", "transfer_start_failed", "result", ret);
    }
  }
#endif
      DEBUG_SYNC(BEFORE_TRANSFER_START_COMMIT);
    }

    if (OB_FAIL(ret)) {
      if (timeout_ctx.is_timeouted()) {
        //overwrite ret
        LOG_WARN("transfer trans already timeout, error code will change to timeout", K(ret));
        ret = OB_TIMEOUT;
      }
    }

    int64_t trans_commit_begin = ObTimeUtil::current_time();
    commit_succ = OB_SUCC(ret);
    if (OB_TMP_FAIL(commit_trans_(ret, trans))) {
      LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
        diagnose_result_msg_ = share::ObStorageHACostItemName::START_TRANS_COMMIT;
      }
      commit_succ = false;
    } else if (OB_SUCCESS == ret) {
      round_ = 0;
    }
    if (OB_SUCC(ret)) {
      process_perf_diagnose_info_(ObStorageHACostItemName::START_TRANS_COMMIT,
          ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, tmp_round, false/*is_report*/);
    }
    int64_t trans_commit_end = ObTimeUtil::current_time();
    if (new_transfer) {
      // tablet write stop from transfer_out_prepare to trans end
      LOG_INFO("[TRANSFER] transfer start trans commit", KR(ret), "transfer_start_trans_cost", trans_commit_end - tablet_stop_begin,
                                                                  "trans_process", trans_commit_begin - tablet_stop_begin,
                                                                  "trans_commit", trans_commit_end - trans_commit_begin);
    }

    clear_prohibit_(task_info, tablet_ids, succ_block_tx, succ_stop_medium);
  }


  if (OB_FAIL(ret)) {
    if (!is_leader) {
    } else if (can_retry_(task_info, ret)) {
      LOG_INFO("transfer task can retry", K(ret), K(task_info));
      if (!new_transfer && OB_TMP_FAIL(unblock_tx_(task_info.tenant_id_, task_info.src_ls_id_, gts_seq_))) {
        diagnose_result_msg_ = share::ObStorageHACostItemName::UNLOCK_MEMBER_LIST_IN_START;
        LOG_WARN("failed to unblock tx", K(ret));
      } else if (OB_TMP_FAIL(unlock_src_and_dest_ls_member_list_(task_info))) {
        LOG_WARN("failed to unlock src and dest ls member list", K(tmp_ret), K(ret), K(task_info));
      } else {
        process_perf_diagnose_info_(ObStorageHACostItemName::UNLOCK_MEMBER_LIST_IN_START,
          ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, tmp_round, false/*is_report*/);
      }
      ob_usleep(INTERVAL_US);
    } else if (OB_SUCCESS != (tmp_ret = update_transfer_status_aborted_(task_info, ret))) {
      LOG_WARN("failed to update transfer status aborted", K(tmp_ret), K(task_info));
    }
  } else {
    if (OB_FAIL(report_to_meta_table_(task_info))) {
      LOG_WARN("failed to report to meta table", K(ret), K(task_info));
    }
  }

  if (commit_succ && OB_TMP_FAIL(broadcast_tablet_location_(task_info))) {
    LOG_WARN("failed to submit submit tablet_broadcast task", KR(tmp_ret), K(task_info));
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_(ret, round_, task_info))) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret), K(retry_count_), K(task_info));
  }
  // if START stage execution failed, just wakeup self
  // if START stage execution succeeded, try to wakeup dest ls leader to go to DOING stage
  if (OB_FAIL(ret)) {
    wakeup_(); // wakeup self
  } else {
    if (OB_TMP_FAIL(wakeup_dest_ls_leader_(task_info))) {
      LOG_WARN("failed to wakeup dest ls leader", K(tmp_ret), K(task_info));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_DIAGNOSE_START_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "fake EN_TRANSFER_DIAGNOSE_START_FAILED", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret) && task_info.is_valid()) {
    if (OB_TMP_FAIL(ObStorageHADiagMgr::add_transfer_error_diagnose_info(task_info.task_id_, task_info.dest_ls_id_,
        share::ObStorageHADiagTaskType::TRANSFER_START, tmp_round, ret, diagnose_result_msg_))) {
      LOG_WARN("failed to add error diagnose info", K(tmp_ret), K(ret),
          K(task_info.task_id_), K(task_info.dest_ls_id_), K(tmp_round));
    }
  }

  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::TRANSFER_START_END,
        ObStorageHADiagTaskType::TRANSFER_START, start_ts, tmp_round, true/*is_report*/);
  }
  LOG_INFO("[TRANSFER] finish do with start status", K(ret), K(task_info), "cost_ts", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObTransferHandler::lock_src_and_dest_ls_member_list_(
    const share::ObTransferTaskInfo &task_info,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  bool is_same = false;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start lock src and dest ls member list", K(src_ls_id), K(dest_ls_id));
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "before_lock_member_list",
                        "tenant_id", task_info.tenant_id_,
                        "task_id", task_info.task_id_.id(),
                        "src_ls_id", src_ls_id.id(),
                        "dest_ls_id", dest_ls_id.id());
#endif
  DEBUG_SYNC(BEFORE_TRANSFER_START_LOCK_MEMBER_LIST);
  ObMemberList member_list;
  ObArray<share::ObLSID> lock_ls_list;
  const ObTransferLockStatus status(ObTransferLockStatus::START);
  const uint64_t tenant_id = task_info.tenant_id_;
  const int64_t task_id = task_info.task_id_.id();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!src_ls_id.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock src and dest ls member list get invalid argument", K(ret),
       K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(check_ls_member_list_same_(src_ls_id, dest_ls_id, member_list, is_same))) {
    LOG_WARN("failed to check ls member listsame", K(ret), K(src_ls_id), K(dest_ls_id));
  } else if (!is_same) {
    ret = OB_TRANSFER_MEMBER_LIST_NOT_SAME;
    LOG_WARN("src ls and dest ls member list is not same", K(ret), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(lock_ls_list.push_back(src_ls_id))) {
    LOG_WARN("failed to push back", K(ret), K(src_ls_id));
  } else if (OB_FAIL(lock_ls_list.push_back(dest_ls_id))) {
    LOG_WARN("failed to push back", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(ObMemberListLockUtils::batch_lock_ls_member_list(tenant_id, task_id,
      lock_ls_list, member_list, status, share::OBCG_STORAGE, *sql_proxy_))) {
    LOG_WARN("failed to batch lock ls member list", K(ret));
  } else if (OB_FAIL(check_ls_member_list_same_(src_ls_id, dest_ls_id, member_list, is_same))) {
    LOG_WARN("failed to check ls member listsame", K(ret), K(src_ls_id), K(dest_ls_id));
  } else if (!is_same) {
    ret = OB_TRANSFER_MEMBER_LIST_NOT_SAME;
    LOG_WARN("src ls and dest ls member list is not same", K(ret), K(src_ls_id), K(dest_ls_id));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_LOCK_TRANSFER_MEMBER_LIST_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_LOCK_TRANSFER_MEMBER_LIST_FAILED", K(ret));
      }
    }
#endif
  }
  LOG_INFO("[TRANSFER] finish lock src and dest ls member list", K(src_ls_id), K(dest_ls_id), "cost_ts",
      ObTimeUtil::current_time() - start_ts);
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "after_lock_member_list",
                        "tenant_id", task_info.tenant_id_,
                        "task_id", task_info.task_id_.id(),
                        "src_ls_id", src_ls_id.id(),
                        "dest_ls_id", dest_ls_id.id(),
                        "member_list_is_same", is_same);
#endif
  DEBUG_SYNC(AFTER_TRANSFER_START_LOCK_MEMBER_LIST);
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::LOCK_MEMBER_LIST,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::LOCK_MEMBER_LIST;
  }
  return ret;
}

int ObTransferHandler::reset_timeout_for_trans_(ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    const int64_t left_trans_timeout = timeout_ctx.get_timeout();
    const int64_t transfer_trans_timeout = tenant_config->_transfer_start_trans_timeout;
    if (left_trans_timeout > 0) {
      const int64_t stmt_timeout = std::min(transfer_trans_timeout, left_trans_timeout);
      if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
        LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
      } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
        LOG_WARN("set timeout context failed", K(ret));
      }
    }
  } else {
    //no need reset timeout for trans
  }
  return ret;
}

int ObTransferHandler::unlock_src_and_dest_ls_member_list_(
    const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  ObMemberList fake_member_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unlock src and dest ls member list get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(inner_unlock_ls_member_list_(task_info, task_info.src_ls_id_, fake_member_list))) {
    LOG_WARN("failed to inner unlock ls member list", K(ret), K(task_info));
  } else if (OB_FAIL(inner_unlock_ls_member_list_(task_info, task_info.dest_ls_id_, fake_member_list))) {
    LOG_WARN("failed to inner unlock ls member list", K(ret), K(task_info));
  }
  return ret;
}

int ObTransferHandler::inner_unlock_ls_member_list_(
    const share::ObTransferTaskInfo &task_info,
    const share::ObLSID &ls_id,
    const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  const int64_t task_id = task_info.task_id_.id();
  const ObTransferLockStatus status(ObTransferLockStatus::START);
  if (OB_FAIL(ObMemberListLockUtils::unlock_ls_member_list(
      tenant_id, ls_id, task_id, member_list, status, share::OBCG_STORAGE, *sql_proxy_))) {
    LOG_WARN("failed to lock ls member list", K(ret), K(task_info), K(ls_id), K(member_list));
  }
  return ret;
}


int ObTransferHandler::check_ls_member_list_same_(
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    common::ObMemberList &member_list,
    bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  common::ObMemberList dest_ls_member_list;
  common::ObMemberList src_ls_member_list;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!src_ls_id.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check ls memebr list same get invalid argument", K(ret), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(get_ls_member_list_(dest_ls_id, dest_ls_member_list))) {
    LOG_WARN("failed to get dest ls member list", K(ret), KPC(ls_));
  } else if (OB_FAIL(get_ls_member_list_(src_ls_id, src_ls_member_list))) {
    LOG_WARN("failed to get src ls member list", K(ret), K(src_ls_id));
  } else if (dest_ls_member_list.get_member_number() != src_ls_member_list.get_member_number()) {
    is_same = false;
    LOG_INFO("dest ls memebr list is not same with src ls member list", K(ret), K(src_ls_member_list), K(dest_ls_member_list));
  } else {
    is_same = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_ls_member_list.get_member_number() && is_same; ++i) {
      ObMember member;
      if (OB_FAIL(dest_ls_member_list.get_member_by_index(i, member))) {
        LOG_WARN("failed to get member info", K(ret), K(i), K(dest_ls_member_list));
      } else if (!src_ls_member_list.contains(member.get_server())) {
        is_same = false;
        LOG_INFO("member list not same", K(src_ls_id), K(dest_ls_id), K(member), K(dest_ls_member_list), K(src_ls_member_list));
      }
    }
  }
  if (OB_SUCC(ret) && is_same) {
    if (OB_FAIL(member_list.deep_copy(src_ls_member_list))) {
      LOG_WARN("failed to deep copy", K(ret), K(src_ls_member_list));
    }
  } else {
    LOG_WARN("member list not same", K(src_ls_id), K(dest_ls_id), K(src_ls_member_list), K(dest_ls_member_list));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_CHECK_MEMBER_LIST_NOT_SAME ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to check member list not same", K(ret), K(src_ls_id), K(dest_ls_id), K(src_ls_member_list), K(dest_ls_member_list));
    }
  }
#endif
  return ret;
}

int ObTransferHandler::get_ls_member_list_(
    const share::ObLSID &ls_id,
    common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = GCONF.cluster_id;
  uint64_t tenant_id = MTL_ID();
  ObStorageHASrcInfo src_info;
  obrpc::ObFetchLSMemberListInfo member_info;
  src_info.cluster_id_ = cluster_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get src ls member list get invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_leader_(ls_id, src_info.src_addr_))) {
    LOG_WARN("failed to get src ls leaer", K(ret), K(ls_id));
  } else if (OB_FAIL(storage_rpc_->post_ls_member_list_request(tenant_id, src_info, ls_id, member_info))) {
    LOG_WARN("failed to get ls member info", K(ret), KPC(ls_));
  } else if (member_info.member_list_.get_member_number() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member list number is unexpected", K(ret), K(member_info), K(ls_id));
  } else if (OB_FAIL(member_list.deep_copy(member_info.member_list_))) {
    LOG_WARN("failed to copy member list", K(ret), KPC(ls_));
  }
  return ret;
}

int ObTransferHandler::check_src_ls_has_active_trans_(
    const share::ObLSID &src_ls_id,
    const int64_t expected_active_trans_count)
{
  int ret = OB_SUCCESS;
  int64_t active_trans_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!src_ls_id.is_valid() || expected_active_trans_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src ls active trans get invalid argument", K(ret), K(src_ls_id), K(expected_active_trans_count));
  } else if (OB_FAIL(get_ls_active_trans_count_(src_ls_id, active_trans_count))) {
    LOG_WARN("failed to get ls active trans count");
  } else if (active_trans_count > expected_active_trans_count) {
    ret = OB_TRANSFER_DETECT_ACTIVE_TRANS;
    LOG_WARN("src ls has unexpected active trans count", K(ret), K(active_trans_count),
        K(expected_active_trans_count), K(src_ls_id));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_CHECK_ACTIVE_TRANS_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_CHECK_ACTIVE_TRANS_FAILED", K(ret));
      }
    }
#endif
    LOG_INFO("get active trans count", K(src_ls_id), K(active_trans_count));
  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::CHECK_SRC_LS_HAS_ACTIVE_TRANS,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::CHECK_SRC_LS_HAS_ACTIVE_TRANS;
  }
  return ret;
}

int ObTransferHandler::get_ls_active_trans_count_(
    const share::ObLSID &src_ls_id,
    int64_t &active_trans_count)
{
  int ret = OB_SUCCESS;
  active_trans_count = 0;
  if (OB_FAIL(ls_->get_active_tx_count(active_trans_count))) {
    LOG_WARN("failed to get active trans count", K(ret), KPC(ls_));
  } else {
    LOG_INFO("get ls active trans count", K(ret), K(src_ls_id), K(active_trans_count));
  }
  return ret;
}

int ObTransferHandler::check_start_status_transfer_tablets_(
    const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  ObArray<ObAddr> member_addr_list;
  const int64_t cluster_id = GCONF.cluster_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check start status src ls info get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_src_ls_member_list_(member_list))) {
    LOG_WARN("failed to get src ls member list", K(ret), K(task_info));
  } else if (OB_FAIL(member_list.get_addr_array(member_addr_list))) {
    LOG_WARN("failed to get addr array", K(ret), K(task_info), K(member_list));
  } else {
    storage::ObCheckStartTransferTabletsProxy batch_proxy(
        *(GCTX.storage_rpc_proxy_), &obrpc::ObStorageRpcProxy::check_start_transfer_tablets);
    for (int64_t i = 0; OB_SUCC(ret) && i < member_addr_list.count(); ++i) {
      const ObAddr &addr = member_addr_list.at(i);
      ObTransferTabletInfoArg arg;
      arg.tenant_id_ = task_info.tenant_id_;
      arg.src_ls_id_ = task_info.src_ls_id_;
      arg.dest_ls_id_ = task_info.dest_ls_id_;
      arg.data_version_ = task_info.data_version_;
      const int64_t timeout = GCONF.rpc_timeout;
      const int64_t cluster_id = GCONF.cluster_id;
      const uint64_t group_id = share::OBCG_STORAGE;
      if (OB_FAIL(arg.tablet_list_.assign(task_info.tablet_list_))) {
        LOG_WARN("failed to assign tablet list", K(ret), K(task_info));
      } else if (OB_FAIL(batch_proxy.call(addr,
                                          timeout,
                                          cluster_id,
                                          task_info.tenant_id_,
                                          group_id,
                                          arg))) {
        STORAGE_LOG(WARN, "failed to call check start transfer tablets", K(ret), K(addr), K(task_info), K(arg));
      }
    }
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(batch_proxy.wait_all(return_code_array))) {
      STORAGE_LOG(WARN, "fail to wait all batch result", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
    } else if (return_code_array.count() != member_addr_list.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cnt not match", KR(ret),
               "return_cnt", return_code_array.count(),
               "server_cnt", member_addr_list.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      const int res_ret = return_code_array.at(i);
      if (OB_SUCCESS != res_ret) {
        ret = res_ret;
        LOG_WARN("rpc execute failed", KR(ret), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
#ifdef ERRSIM
      ObTransferEventRecorder::record_transfer_task_event(
        task_info.task_id_, "BEFORE_START_TRANSFER_TRANS", task_info.src_ls_id_, task_info.dest_ls_id_);
#endif
    DEBUG_SYNC(BEFORE_START_TRANSFER_TRANS);

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_CHECK_START_TRANSFER_STATUS_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_CHECK_START_TRANSFER_STATUS_FAILED", K(ret));
      }
    }
#endif

  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::CHECK_START_STATUS_TRANSFER_TABLETS,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::CHECK_START_STATUS_TRANSFER_TABLETS;
  }
  return ret;
}

int ObTransferHandler::get_ls_leader_(
    const share::ObLSID &ls_id,
    common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t tenant_id = MTL_ID();
  share::ObLocationService *location_service = nullptr;
  const bool force_renew = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls leader get invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service should not be NULL", K(ret), KP(location_service));
  } else if (OB_FAIL(location_service->get_leader(cluster_id, tenant_id, ls_id, force_renew, addr))) {
    LOG_WARN("fail to get ls leader server", K(ret), K(tenant_id), KPC(ls_));
  }
  return ret;
}

int ObTransferHandler::do_trans_transfer_start_(
    const share::ObTransferTaskInfo &task_info,
    const palf::LogConfigVersion &config_version,
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  LOG_INFO("[TRANSFER] start do trans transfer start", K(task_info));
  int ret = OB_SUCCESS;
  SCN start_scn;
  ObArray<ObMigrationTabletParam> tablet_meta_list;
  const share::ObTransferStatus next_status(ObTransferStatus::DOING);
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid() || !config_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do trans transfer start get invalid argument", K(ret), K(task_info), K(config_version));
  } else if (OB_FAIL(do_tx_start_transfer_out_(task_info, trans,
          transaction::ObTxDataSourceType::START_TRANSFER_OUT, SCN::min_scn(), nullptr))) {
    LOG_WARN("failed to do tx start transfer out", K(ret), K(task_info));
  } else if (OB_FAIL(check_config_version_(config_version))) {
    LOG_WARN("failed to check config version", K(ret), K(task_info));
  } else if (OB_FAIL(get_start_transfer_out_scn_(task_info, timeout_ctx, start_scn))) {
    LOG_WARN("failed to get start transfer out log ts", K(ret), K(task_info));
  } else if (OB_FAIL(check_src_ls_has_active_trans_(task_info.src_ls_id_, 1/*transfer out trans*/))) {
    LOG_WARN("failed to check src ls has active trans", K(ret), K(task_info));
  } else if (OB_FAIL(unblock_tx_(task_info.tenant_id_, task_info.src_ls_id_, gts_seq_))) {
    LOG_WARN("failed to unblock tx", K(ret), K(task_info));
  } else if (OB_FAIL(wait_src_ls_replay_to_start_scn_(task_info, start_scn, timeout_ctx))) {
    LOG_WARN("failed to wait src ls replay to start scn", K(ret), K(task_info));
  } else if (OB_FAIL(get_transfer_tablets_meta_(task_info, tablet_meta_list))) {
    LOG_WARN("failed to get transfer tablets meta", K(ret), K(task_info));
  } else if (OB_FAIL(do_tx_start_transfer_in_(task_info, start_scn, tablet_meta_list, timeout_ctx, trans))) {
    LOG_WARN("failed to do tx start transfer in", K(ret), K(task_info), K(start_scn), K(tablet_meta_list));
  } else if (OB_FAIL(update_transfer_status_(task_info, next_status, start_scn, OB_SUCCESS, trans))) {
    LOG_WARN("failed to update transfer status", K(ret), K(task_info));
  }

  LOG_INFO("[TRANSFER] finish do trans transfer start", K(task_info), "cost_ts", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObTransferHandler::do_trans_transfer_start_prepare_(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLSHandle src_ls_handle;
  ObTransID failed_tx_id;
  ObStorageHASrcInfo addr_info;
  addr_info.cluster_id_ = GCONF.cluster_id;
  ObAddr dest_ls_leader;
  SCN data_scn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do trans transfer start get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_ls_leader_(task_info.dest_ls_id_, dest_ls_leader))) {
    LOG_WARN("failed to get dest ls leader", K(ret), K(task_info));
  } else if (task_info.tenant_id_ != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant not match", K(ret), K(task_info), K(MTL_ID()));
  } else if (OB_FAIL(do_trans_transfer_dest_prepare_(task_info, trans))) {
    LOG_WARN("failed to do transfer dest prepare", K(ret), K(task_info));
  } else if (FALSE_IT(addr_info.src_addr_ = dest_ls_leader)) {
  // submit active tx redo log before block tablet write to optimise system interrupt time
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(task_info.src_ls_id_, src_ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(task_info));
  } else if (OB_FAIL(src_ls_handle.get_ls()->get_tx_svr()->traverse_trans_to_submit_redo_log(failed_tx_id))) {
    LOG_WARN("failed to submit tx log", K(ret), K(task_info));
  // submit dest_ls active tx redo log
  } else if (OB_FAIL(storage_rpc_->submit_tx_log(task_info.tenant_id_, addr_info, task_info.dest_ls_id_, data_scn))) {
    LOG_WARN("failed to submit tx log", K(ret), K(task_info));
  } else if (OB_FAIL(wait_src_ls_advance_weak_read_ts_(task_info, timeout_ctx))) {
    LOG_WARN("failed to wait src_ls advance weak_read_ts", K(ret), K(task_info));
  }
  return ret;
}

int ObTransferHandler::wait_tablet_write_end_(
    const share::ObTransferTaskInfo &task_info,
    SCN &data_end_scn,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  const share::ObLSID &src_ls_id = task_info.src_ls_id_;
  ObLSHandle ls_handle;
  ObLSService *ls_srv = NULL;
  ObLS *ls = NULL;
  logservice::ObLogService *log_service = nullptr;
  ObRole role;
  int64_t proposal_id = 0;
  SCN scn;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (OB_FAIL(ls_srv->get_ls(src_ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", K(ret), K(src_ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else {
    ObSEArray<ObTabletID, 1> tablet_list;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < task_info.tablet_list_.count(); idx++) {
      if (OB_FAIL(tablet_list.push_back(task_info.tablet_list_.at(idx).tablet_id()))) {
        LOG_WARN("push tablet to array failed", KR(ret));
      }
    }
    // wait tablet all operation stop
    // data memtable write end
    // table lock operation end
    ObTransID failed_tx_id;
    bool has_active_memtable = false;
    const bool is_sync = true;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls->get_lock_table()->enable_check_tablet_status(true))) {
      LOG_WARN("failed to enable check tablet status", KR(ret), K(task_info));
    } else if (OB_FAIL(ls->wait_tx_write_end(timeout_ctx))) {
      LOG_WARN("failed to wait tx_write end", KR(ret), K(task_info));
    } else if (OB_FAIL(ls->get_tx_svr()->traverse_trans_to_submit_redo_log(failed_tx_id))) {
      LOG_WARN("failed to submit tx log", KR(ret), K(task_info));
    } else if (OB_FAIL(ls->tablet_freeze(checkpoint::INVALID_TRACE_ID, tablet_list, is_sync))) {
      LOG_WARN("batch tablet freeze failed", KR(ret), KPC(ls), K(task_info));
    } else if (OB_FAIL(ls->check_tablet_no_active_memtable(tablet_list, has_active_memtable))) {
      LOG_WARN("check tablet has active memtable failed", KR(ret), KPC(ls), K(task_info));
    } else if (has_active_memtable) {
      ret = OB_EAGAIN;
      LOG_WARN("tablet has active memtable need retry", KR(ret), K(tablet_list));
    } else if (OB_FAIL(ls->get_log_handler()->get_max_scn(scn))) {
      LOG_WARN("log_handler get_max_scn failed", KR(ret), K(task_info));
    } else {
      data_end_scn = scn;
      LOG_INFO("success to wait tablet write end", KR(ret), K(task_info));
    }
  }
  return ret;
}

int ObTransferHandler::do_trans_transfer_start_v2_(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  LOG_INFO("[TRANSFER] start do trans transfer start v2", K(task_info));
  int ret = OB_SUCCESS;
  SCN start_scn;
  ObArray<ObMigrationTabletParam> tablet_meta_list;
  const share::ObTransferStatus next_status(ObTransferStatus::DOING);
  ObAddr src_ls_leader;
  ObStorageHASrcInfo src_info;
  src_info.cluster_id_ = GCONF.cluster_id;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  SCN data_end_scn;
  ObArray<ObTabletID> tablet_list;
  ObArray<ObTransID> move_tx_ids;
  int64_t move_tx_count = 0;
  int64_t start_time = ObTimeUtil::current_time();
  int64_t transfer_out_prepare_cost = 0;
  int64_t wait_tablet_write_end_cost = 0;
  int64_t filter_tx_cost = 0;
  int64_t transfer_out_cost = 0;
  int64_t wait_src_replay_cost = 0;
  int64_t get_transfer_out_scn_cost = 0;
  int64_t get_tablets_meta_cost = 0;
  int64_t move_tx_cost = 0;
  int64_t transfer_in_cost = 0;
  int64_t now_time = ObTimeUtil::current_time();
  int64_t step_time = now_time;
  #define STEP_COST_AND_CHECK_TIMEOUT(cost)  FALSE_IT(now_time = ObTimeUtil::current_time()) || \
                                          FALSE_IT(cost = now_time - step_time) ||           \
                                          FALSE_IT(step_time = now_time) ||                  \
                                          (timeout_ctx.is_timeouted() && !FALSE_IT(ret = OB_TIMEOUT))
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do trans transfer start get invalid argument", K(ret), K(task_info));
  // for transfer support move active tx, we use this config as tablet write blocked timeout
  } else if (OB_FAIL(get_ls_leader_(task_info.src_ls_id_, src_ls_leader))) {
    LOG_WARN("failed to get src ls leader", K(ret), K(task_info));
  } else if (FALSE_IT(src_info.src_addr_ = src_ls_leader)) {
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < task_info.tablet_list_.count(); idx++) {
      if (OB_FAIL(tablet_list.push_back(task_info.tablet_list_.at(idx).tablet_id()))) {
        LOG_WARN("push to array failed", KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  // MDS transaction operation for block tablet write
  } else if (OB_FAIL(do_tx_start_transfer_out_(task_info, trans,
          transaction::ObTxDataSourceType::START_TRANSFER_OUT_PREPARE, SCN::min_scn(), nullptr))) {
    LOG_WARN("failed to do tx start transfer prepare", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(transfer_out_prepare_cost)) {
  // resubmit tx log promise transfer tablet redo complete
  } else if (OB_FAIL(wait_tablet_write_end_(task_info, data_end_scn, timeout_ctx))) {
    LOG_WARN("failed to wait tablet write end", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(wait_tablet_write_end_cost)) {
  } else if (!data_end_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer data_end_scn is invalid", K(ret), K(task_info), K(data_end_scn));
  } else if (OB_FAIL(ls_->filter_tx_need_transfer(tablet_list, data_end_scn, move_tx_ids))) {
    LOG_WARN("filter tx need transfer", KR(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(filter_tx_cost)) {
  } else if (OB_FAIL(do_tx_start_transfer_out_(task_info, trans,
          transaction::ObTxDataSourceType::START_TRANSFER_OUT_V2, data_end_scn, &move_tx_ids))) {
    LOG_WARN("failed to do tx start transfer out", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(transfer_out_cost)) {
  } else if (OB_FAIL(get_start_transfer_out_scn_(task_info, timeout_ctx, start_scn))) {
    LOG_WARN("failed to get start transfer out log ts", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(get_transfer_out_scn_cost)) {
  // wait src replay
  } else if (OB_FAIL(wait_src_ls_replay_to_start_scn_(task_info, start_scn, timeout_ctx))) {
    LOG_WARN("failed to wait src ls replay to start scn", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(wait_src_replay_cost)) {
  } else if (OB_FAIL(get_transfer_tablets_meta_(task_info, tablet_meta_list))) {
    LOG_WARN("failed to get transfer tablets meta", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(get_tablets_meta_cost)) {
  // move tx
  } else if (move_tx_ids.count() > 0 && OB_FAIL(do_move_tx_to_dest_ls_(task_info, timeout_ctx, trans,
          data_end_scn, start_scn, tablet_list, move_tx_ids, move_tx_count))) {
    LOG_WARN("failed to do move tx to dest_ls", K(ret), K(task_info));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(move_tx_cost)) {
  // transfer in
  } else if (OB_FAIL(do_tx_start_transfer_in_(task_info, start_scn, tablet_meta_list, timeout_ctx, trans))) {
    LOG_WARN("failed to do tx start transfer in", K(ret), K(task_info), K(start_scn), K(tablet_meta_list));
  } else if (STEP_COST_AND_CHECK_TIMEOUT(transfer_in_cost)) {
  } else if (OB_FAIL(update_transfer_status_(task_info, next_status, start_scn, OB_SUCCESS, trans))) {
    LOG_WARN("failed to update transfer status", K(ret), K(task_info));
  }

  LOG_INFO("[TRANSFER] finish transfer start", K(ret), K(task_info), "cost", ObTimeUtil::current_time() - start_time,
                                               K(transfer_out_prepare_cost),
                                               K(wait_tablet_write_end_cost),
                                               K(filter_tx_cost),
                                               K(transfer_out_cost),
                                               K(get_transfer_out_scn_cost),
                                               K(wait_src_replay_cost),
                                               K(get_tablets_meta_cost),
                                               K(move_tx_cost),
                                               K(transfer_in_cost),
                                               K(move_tx_count),
                                               K(move_tx_ids));
  return ret;
}

int ObTransferHandler::start_trans_(
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t stmt_timeout = 10_s;
  const int64_t LOCK_MEMBER_LIST_TIMEOUT = 10_s;
  const bool with_snapshot = false;
  const int32_t group_id = share::OBCG_TRANSFER;
  if (tenant_config.is_valid()) {
    stmt_timeout = tenant_config->_transfer_start_trans_timeout + LOCK_MEMBER_LIST_TIMEOUT;
    if (tenant_config->_enable_balance_kill_transaction) {
      stmt_timeout += tenant_config->_balance_kill_transaction_threshold;
      stmt_timeout += tenant_config->_balance_wait_killing_transaction_end_threshold;
    }
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, with_snapshot, group_id))) {
    LOG_WARN("failed to start trans", K(ret));
  }
  return ret;
}

int ObTransferHandler::commit_trans_(
    const int32_t &result,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else {
    tmp_ret = trans.end(OB_SUCCESS == result);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObTransferHandler::lock_transfer_task_(
    const share::ObTransferTaskInfo &task_info,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  ObTransferTaskInfo table_task_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock transfer task get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_transfer_task_from_inner_table_(task_info.task_id_, for_update, trans, table_task_info))) {
    LOG_WARN("failed to get transfer task from inner table", K(ret), K(task_info));
  } else if (task_info.task_id_ != table_task_info.task_id_
      || task_info.status_ != table_task_info.status_
      || task_info.dest_ls_id_ != table_task_info.dest_ls_id_
      || task_info.src_ls_id_ != table_task_info.src_ls_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer task info in not same with inner table", K(ret), K(task_info), K(table_task_info));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_LOCK_TRANSFER_TASK_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_LOCK_TRANSFER_TASK_FAILED", K(ret));
      }
    }
#endif
  }
  return ret;
}

int ObTransferHandler::do_tx_start_transfer_out_(
    const share::ObTransferTaskInfo &task_info,
    common::ObMySQLTransaction &trans,
    const transaction::ObTxDataSourceType data_source_type,
    SCN data_end_scn,
    ObIArray<ObTransID> *move_tx_ids)
{
  LOG_INFO("[TRANSFER] register start transfer out", K(task_info), K(data_source_type));
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  ObTXStartTransferOutInfo start_transfer_out_info;
  ObArenaAllocator allocator;
  SCN dest_base_scn;
  const int64_t start_ts = ObTimeUtil::current_time();
  const int64_t ENABLE_FILTER_TX_LIST_LIMIT = 1000;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tx start transfer out get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(ls_->get_log_handler()->get_max_scn(dest_base_scn))) {
    LOG_WARN("failed to get max scn", K(ret), K(task_info));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    start_transfer_out_info.src_ls_id_ = task_info.src_ls_id_;
    start_transfer_out_info.dest_ls_id_ = task_info.dest_ls_id_;
    start_transfer_out_info.data_end_scn_ = data_end_scn;
    // TODO lana optimise transfer_epoch value
    start_transfer_out_info.transfer_epoch_ = task_info.task_id_.id();
    start_transfer_out_info.task_id_ = task_info.task_id_;
    start_transfer_out_info.data_version_ = task_info.data_version_;
    start_transfer_out_info.filter_tx_need_transfer_ = false;
    if (OB_FAIL(start_transfer_out_info.tablet_list_.assign(task_info.tablet_list_))) {
      LOG_WARN("failed to assign transfer tablet list", K(ret), K(task_info));
    } else if (OB_NOT_NULL(move_tx_ids) && move_tx_ids->count() <= ENABLE_FILTER_TX_LIST_LIMIT) {
      // if has too many tx_ids, we just use data_end_scn to filter
      start_transfer_out_info.filter_tx_need_transfer_ = true;
      if (OB_FAIL(start_transfer_out_info.move_tx_ids_.assign(*move_tx_ids))) {
        LOG_WARN("assign failed", KR(ret), K(move_tx_ids));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t buf_len = start_transfer_out_info.get_serialize_size();
      int64_t pos = 0;
      char *buf = (char*)allocator.alloc(buf_len);
      ObRegisterMdsFlag flag;
      flag.need_flush_redo_instantly_ = true;
      flag.mds_base_scn_ = dest_base_scn;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail alloc memory", KR(ret));
      } else if (OB_FAIL(start_transfer_out_info.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize start transfer out info", KR(ret), K(start_transfer_out_info));
      } else if (OB_FAIL(conn->register_multi_data_source(task_info.tenant_id_, task_info.src_ls_id_,
          data_source_type, buf, buf_len, flag))) {
        LOG_WARN("failed to register multi data source", K(ret), K(task_info));
      } else {
        LOG_INFO("[TRANSFER] success register start transfer out", "cost", ObTimeUtil::current_time() - start_ts,
                                                                    K(data_source_type));
      }
#ifdef ERRSIM
      ObTransferEventRecorder::record_transfer_task_event(
        task_info.task_id_, "TX_START_TRANSFER_OUT", task_info.src_ls_id_, task_info.dest_ls_id_);
#endif
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_START_TRANSFER_OUT_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_START_TRANSFER_OUT_FAILED", K(ret));
      }
    }
#endif

    DEBUG_SYNC(AFTER_START_TRANSFER_OUT);

  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::REGISTER_TRANSFER_START_OUT,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::REGISTER_TRANSFER_START_OUT;
  }
  return ret;
}

int ObTransferHandler::get_start_transfer_out_scn_(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx,
    share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  start_scn.set_min();
  ObTabletHandle tablet_handle;
  ObTabletCreateDeleteMdsUserData user_data;
  const int64_t start_ts = ObTimeUtil::current_time();
  const int64_t OB_CHECK_START_SCN_READY_INTERVAL = 1 * 1000; //1ms

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get start transfer out scn invalid argument", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_info.tablet_list_.count(); ++i) {
      const ObTransferTabletInfo &tablet_info = task_info.tablet_list_.at(i);
      ObTablet *tablet = nullptr;
      if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(task_info));
        break;
      } else if (ls_->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, no need get tablet start transfer out scn", K(ret), KPC(ls_));
        break;
      } else if (OB_FAIL(get_tablet_start_transfer_out_scn_(tablet_info, i, timeout_ctx, start_scn))) {
        LOG_WARN("failed to get tablet start transfer out scn", K(ret), K(tablet_info), K(task_info));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("[TRANSFER_BLOCK_TX] succeed get start transfer scn",
          K(start_scn), "cost", ObTimeUtil::current_time() - start_ts);
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_GET_TRANSFER_START_SCN_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_GET_TRANSFER_START_SCN_FAILED", K(ret));
      }
    }
#endif
    DEBUG_SYNC(AFTER_START_TRANSFER_GET_START_SCN);
  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::DEST_LS_GET_START_SCN,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::DEST_LS_GET_START_SCN;
  }
  return ret;
}

int ObTransferHandler::get_tablet_start_transfer_out_scn_(
    const ObTransferTabletInfo &tablet_info,
    const int64_t index,
    ObTimeoutCtx &timeout_ctx,
    share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  const int64_t OB_CHECK_START_SCN_READY_INTERVAL = 1 * 1000; //1ms

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!tablet_info.is_valid() || index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet start transfer out scn invalid argument", K(ret), K(tablet_info), K(index));
  } else {
    while (OB_SUCC(ret)) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(tablet_info));
        break;
      } else if (ls_->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, no need get tablet start transfer out scn", K(ret), KPC(ls_));
        break;
      } else if (OB_FAIL(ls_->get_tablet(tablet_info.tablet_id_, tablet_handle, 0,
           ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
         LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet_info.transfer_seq_) {
        ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
        LOG_WARN("transfer tablet seq is unexpected", K(ret), K(user_data), K(tablet_info), KPC(tablet));
      } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet_handle, user_data))) {
        LOG_WARN("failed to get tablet status", K(ret), K(tablet_info));
      } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_) {
        if (ObTabletStatus::NORMAL == user_data.tablet_status_) {
          start_scn.set_min();
          ret = OB_EAGAIN;
          //tablet status is normal, set start_scn min which means get start scn need retry.
          LOG_WARN("tablet status is normal, get min start scn", K(tablet_handle), K(user_data));
        }
      } else {
        if (user_data.transfer_scn_.is_min()) {
          LOG_INFO("tablet status is transfer out, but on_redo is not executed. Retry is required", K(user_data), KPC(tablet));
        } else if (index > 0) {
          if (user_data.transfer_scn_ != start_scn) {
            ret = OB_EAGAIN;
            LOG_WARN("tx data is not same, need retry", K(ret), K(tablet_handle), K(user_data), K(start_scn));
          } else {
            break;
          }
        } else {
          start_scn = user_data.transfer_scn_;
          break;
        }
      }

      if (OB_SUCC(ret)) {
        ob_usleep(OB_CHECK_START_SCN_READY_INTERVAL);
      }
    }
  }
  return ret;
}

int ObTransferHandler::wait_src_ls_replay_to_start_scn_(
    const share::ObTransferTaskInfo &task_info,
    const share::SCN &start_scn,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  ObArray<ObAddr> member_addr_list;
  const int64_t start_ts = ObTimeUtil::current_time();
  const int32_t group_id = share::OBCG_TRANSFER;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait src ls replay to start scn get invalid argument", K(ret), K(task_info), K(start_scn));
  } else if (OB_FAIL(get_src_ls_member_list_(member_list))) {
    LOG_WARN("failed to get src ls member list", K(ret), K(task_info));
  } else if (OB_FAIL(member_list.get_addr_array(member_addr_list))) {
    LOG_WARN("failed to get addr array", K(ret), K(task_info), K(member_list));
  } else if (OB_FAIL(wait_ls_replay_event_(task_info.src_ls_id_, task_info, member_addr_list, start_scn, group_id, timeout_ctx))) {
    LOG_WARN("failed to wait ls replay event", K(ret), K(task_info), K(member_list), K(start_scn));
  } else {
    LOG_INFO("[TRANSFER_BLOCK_TX] wait src ls repaly to start scn", "cost", ObTimeUtil::current_time() - start_ts);
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("errsim_transfer", "DEBUG_SYNC_WAIT_REPLAY_TO_START_SCN");
#endif
  DEBUG_SYNC(AFTER_START_TRANSFER_WAIT_REPLAY_TO_START_SCN);
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_WAIT_SRC_REPALY_TO_START_SCN_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_WAIT_SRC_REPALY_TO_START_SCN_FAILED", K(ret));
    }
  }
#endif
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::WAIT_SRC_LS_REPLAY_TO_START_SCN,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::WAIT_SRC_LS_REPLAY_TO_START_SCN;
  }
  return ret;
}

int ObTransferHandler::precheck_ls_replay_scn_(const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  ObArray<ObAddr> member_addr_list;
  share::SCN check_scn;
  ObTimeoutCtx timeout_ctx;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int32_t group_id = share::OBCG_STORAGE;
  if (tenant_config.is_valid()) {
    const int64_t timeout = tenant_config->_transfer_start_trans_timeout * 0.5;
    if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
      LOG_WARN("set timeout context failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait src ls replay to start scn get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_src_ls_member_list_(member_list))) {
    LOG_WARN("failed to get src ls member list", K(ret), K(task_info));
  } else if (OB_FAIL(member_list.get_addr_array(member_addr_list))) {
    LOG_WARN("failed to get addr array", K(ret), K(task_info), K(member_list));
  } else if (OB_FAIL(get_max_decided_scn_(task_info.tenant_id_, task_info.src_ls_id_, check_scn))) {
    LOG_WARN("failed to get max decided scn", K(ret), K(task_info));
  } else if (OB_FAIL(wait_ls_replay_event_(task_info.src_ls_id_, task_info, member_addr_list, check_scn, group_id, timeout_ctx))) {
    LOG_WARN("failed to wait ls replay event", K(ret), K(task_info), K(member_list), K(check_scn));
    if (OB_TIMEOUT == ret) {
      ret = OB_TRANSFER_CANNOT_START;
      //TODO(zhixing) add error msg in diagnose
    }
  } else if (timeout_ctx.is_timeouted()) {
    ret = OB_TRANSFER_CANNOT_START;
    LOG_WARN("transfer precheck timeout, cannot start transfer in", K(ret), K(task_info));
  }

  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::PRECHECK_LS_REPALY_SCN,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::PRECHECK_LS_REPALY_SCN;
  }
  return ret;
}

int ObTransferHandler::get_max_decided_scn_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    share::SCN &check_scn)
{
  int ret = OB_SUCCESS;
  check_scn.reset();
  MTL_SWITCH(tenant_id) {
    ObLS *ls = NULL;
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    SCN max_decided_scn;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), KP(ls));
    } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("failed to max decided scn", K(ret), K(tenant_id), K(ls_id));
    } else {
      check_scn = max_decided_scn;
    }
  }
  return ret;
}

int ObTransferHandler::wait_ls_replay_event_(
    const share::ObLSID &ls_id,
    const share::ObTransferTaskInfo &task_info,
    const common::ObArray<ObAddr> &total_addr_list,
    const share::SCN &check_scn,
    const int32_t group_id,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t OB_CHECK_START_SCN_READY_INTERVAL = 5 * 1000; //5ms
  const int64_t start_ts = ObTimeUtil::current_time();
  common::ObArray<ObAddr> member_addr_list;
  common::ObArray<ObAddr> finished_member_addr_list;
  bool is_leader = false;

  while (OB_SUCC(ret)) {
    if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("already timeout", K(ret), K(task_info));
      break;
    } else if (OB_FAIL(check_self_is_leader_(is_leader))) {
      LOG_WARN("failed to check self is leader", K(ret), KPC(ls_));
    } else if (!is_leader) {
      ret = OB_LS_NOT_LEADER;
      LOG_WARN("ls leader has been changed", K(ret), K(task_info));
      break;
    } else if (OB_FAIL(ObTransferUtils::get_need_check_member(total_addr_list, finished_member_addr_list, member_addr_list))) {
      LOG_WARN("failed to get need check member", K(ret), K(task_info), K(total_addr_list));
    } else if (OB_FAIL(ObTransferUtils::check_ls_replay_scn(task_info.tenant_id_, ls_id, check_scn,
        group_id, member_addr_list, timeout_ctx, finished_member_addr_list))) {
      LOG_WARN("failed to check ls replay scn", K(ret), K(task_info), K(ls_id), K(check_scn));
    }

    if (OB_SUCC(ret)) {
      if (finished_member_addr_list.count() == total_addr_list.count()) {
        FLOG_INFO("[TRANSFER] src ls all replicas replay reach check_scn", "src_ls", task_info.src_ls_id_,
            K(check_scn), K(total_addr_list), "cost", ObTimeUtil::current_time() - start_ts);
        break;
      }
    }

    if (OB_FAIL(ret)) {
      //TODO(muwei.ym) check need retry in 4.2 RC3
    }
    ob_usleep(OB_CHECK_START_SCN_READY_INTERVAL);
  }
  FLOG_INFO("[TRANSFER] wait_ls_replay_event_ finish", K(ret), K(task_info.task_id_), K(check_scn), "cost", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObTransferHandler::get_transfer_tablets_meta_(
    const share::ObTransferTaskInfo &task_info,
    common::ObIArray<ObMigrationTabletParam> &tablet_meta_list)
{
  int ret = OB_SUCCESS;
  tablet_meta_list.reset();
  obrpc::ObCopyTabletInfo tablet_info;
  const int64_t start_ts = ObTimeUtil::current_time();
  DEBUG_SYNC(BEFORE_START_TRANSFER_GET_TABLET_META);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get transfer tablets meta get invalid argument", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_info.tablet_list_.count(); ++i) {
      const ObTransferTabletInfo &transfer_tablet_info = task_info.tablet_list_.at(i);
      ObTabletHandle tablet_handle;
      tablet_info.reset();
      if (OB_FAIL(ls_->ha_get_tablet(transfer_tablet_info.tablet_id_, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(transfer_tablet_info), K(tablet_handle));
      } else if (OB_FAIL(get_next_tablet_info_(task_info, transfer_tablet_info, tablet_handle, tablet_info))) {
        LOG_WARN("failed to get next tablet info ", K(ret), K(transfer_tablet_info), K(tablet_handle));
      } else if (OB_FAIL(tablet_meta_list.push_back(tablet_info.param_))) {
        LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
      }
    }

    LOG_INFO("[TRANSFER_BLOCK_TX] get transfer tablets meta", K(ret), "cost", ObTimeUtil::current_time() - start_ts);

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_GET_TRANSFER_TABLET_META_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_GET_TRANSFER_TABLET_META_FAILED", K(ret));
      }
    }
#endif

  }
  DEBUG_SYNC(AFTER_START_TRANSFER_GET_TABLET_META);
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::SRC_LS_GET_TABLET_META,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::SRC_LS_GET_TABLET_META;
  }
  return ret;
}

int ObTransferHandler::get_next_tablet_info_(
    const share::ObTransferTaskInfo &task_info,
    const ObTransferTabletInfo &transfer_tablet_info,
    ObTabletHandle &tablet_handle,
    obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();
  ObTabletCreateDeleteMdsUserData user_data;
  ObTablet *tablet = nullptr;
  bool committed_flag = false;
  const ObLSID &dest_ls_id = task_info.dest_ls_id_;
  const int64_t data_version = task_info.data_version_;

  if (!task_info.is_valid() || !transfer_tablet_info.is_valid() || !tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next tablet info get invalid argument", K(ret), K(task_info), K(transfer_tablet_info), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(transfer_tablet_info));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, committed_flag))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tablet status is not match", K(ret), KPC(tablet), K(transfer_tablet_info), K(user_data));
  } else if (committed_flag) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("transfer src tablet status is transfer out but is already committed, not match",
        K(ret), KPC(tablet), K(transfer_tablet_info), K(user_data));
  } else if (transfer_tablet_info.transfer_seq_ != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
    LOG_WARN("tablet transfer seq is not match", K(ret), KPC(tablet), K(transfer_tablet_info));
  } else if (OB_FAIL(tablet->build_transfer_tablet_param(data_version, dest_ls_id, tablet_info.param_))) {
    LOG_WARN("failed to build transfer tablet param", K(ret), K(transfer_tablet_info));
  } else {
    tablet_info.data_size_ = 0; //transfer will not use data size
    tablet_info.tablet_id_ = transfer_tablet_info.tablet_id_;
    tablet_info.status_ = ObCopyTabletStatus::TABLET_EXIST;
  }
  return ret;
}


int ObTransferHandler::do_tx_start_transfer_in_(
    const share::ObTransferTaskInfo &task_info,
    const SCN &start_scn,
    const common::ObIArray<ObMigrationTabletParam> &tablet_meta_list,
    ObTimeoutCtx &timeout_ctx,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUF_LEN = 1.5 * 1024 * 1024; // 1.5M
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid() || tablet_meta_list.empty() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tx start transfer in get invalid argument", K(ret), K(task_info),
        K(tablet_meta_list), K(start_scn));
  } else {
    int64_t index = 0;
    ObTXStartTransferInInfo start_transfer_in_info;
    while (OB_SUCC(ret) && index < tablet_meta_list.count()) {
      start_transfer_in_info.reset();
      start_transfer_in_info.src_ls_id_ = task_info.src_ls_id_;
      start_transfer_in_info.dest_ls_id_ = task_info.dest_ls_id_;
      start_transfer_in_info.start_scn_ = start_scn;
      start_transfer_in_info.task_id_ = task_info.task_id_;
      start_transfer_in_info.data_version_ = task_info.data_version_;

      if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(task_info));
      }

      for (int64_t i = index; i < tablet_meta_list.count() && OB_SUCC(ret); ++i, ++index) {
        const ObMigrationTabletParam &tablet_meta = tablet_meta_list.at(i);
        if (start_transfer_in_info.get_serialize_size() + tablet_meta.get_serialize_size() > MAX_BUF_LEN) {
          if (OB_FAIL(inner_tx_start_transfer_in_(task_info, start_transfer_in_info, trans))) {
            LOG_WARN("failed to do inner tx start transfer in", K(ret), K(task_info), K(start_transfer_in_info));
          } else {
            start_transfer_in_info.reset();
            break;
          }
        } else if (OB_FAIL(start_transfer_in_info.tablet_meta_list_.push_back(tablet_meta))) {
          LOG_WARN("failed to push tablet meta into list", K(ret), K(tablet_meta));
        }
      }

      if (OB_SUCC(ret) && !start_transfer_in_info.tablet_meta_list_.empty()) {
        if (OB_FAIL(inner_tx_start_transfer_in_(task_info, start_transfer_in_info, trans))) {
          LOG_WARN("failed to do inner tx start transfer in", K(ret), K(task_info), K(start_transfer_in_info));
        }
      }
#ifdef ERRSIM
      ObTransferEventRecorder::record_transfer_task_event(
        task_info.task_id_, "TX_START_TRANSFER_IN", task_info.src_ls_id_, task_info.dest_ls_id_);
#endif
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_START_TRANSFER_IN_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_START_TRANSFER_IN_FAILED", K(ret));
	    }
    }
#endif

    LOG_INFO("[TRANSFER_BLOCK_TX] do tx start transfer in", K(ret), "cost", ObTimeUtil::current_time() - start_ts);
    DEBUG_SYNC(AFTER_START_TRANSFER_IN);
  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::REGISTER_TRANSFER_START_IN,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::REGISTER_TRANSFER_START_IN;
  }
  return ret;
}

int ObTransferHandler::inner_tx_start_transfer_in_(
    const share::ObTransferTaskInfo &task_info,
    const ObTXStartTransferInInfo &start_transfer_in_info,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  observer::ObInnerSQLConnection *conn = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid() || !start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner tx start transfer in get invalid argument", K(ret), K(task_info), K(start_transfer_in_info));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    int64_t buf_len = start_transfer_in_info.get_serialize_size();
    int64_t pos = 0;
    char *buf = (char*)allocator.alloc(buf_len);
    ObRegisterMdsFlag flag;
    flag.need_flush_redo_instantly_ = false;
    flag.mds_base_scn_ = start_transfer_in_info.start_scn_;

#ifdef ERRSIM
    flag.need_flush_redo_instantly_ = true;
#endif
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail alloc memory", KR(ret));
    } else if (OB_FAIL(start_transfer_in_info.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize start transfer out info", KR(ret), K(start_transfer_in_info));
    } else if (OB_FAIL(conn->register_multi_data_source(task_info.tenant_id_, task_info.dest_ls_id_,
        transaction::ObTxDataSourceType::START_TRANSFER_IN, buf, buf_len, flag))) {
      LOG_WARN("failed to register multi data source", K(ret), K(start_transfer_in_info));
    }
  }
  return ret;
}

int ObTransferHandler::update_all_tablet_to_ls_(
    const share::ObTransferTaskInfo &task_info,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ObTransferEventRecorder::record_transfer_task_event(
    task_info.task_id_, "BEFORE_TRANSFER_UPDATE_TABLET_TO_LS", task_info.src_ls_id_, task_info.dest_ls_id_);
#endif
  DEBUG_SYNC(BEFORE_TRANSFER_UPDATE_TABLET_TO_LS);
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update all tablet to ls get invalid argument", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_info.tablet_list_.count(); ++i) {
      const ObTransferTabletInfo &tablet_info = task_info.tablet_list_.at(i);
      if (OB_FAIL(ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(trans, task_info.tenant_id_,
          tablet_info.tablet_id_, tablet_info.transfer_seq_, task_info.src_ls_id_,
          tablet_info.transfer_seq_ + 1, task_info.dest_ls_id_, share::OBCG_TRANSFER))) {
        LOG_WARN("failed to update ls id and transfer seq", K(ret), K(tablet_info), K(task_info));
      }
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_UPDATE_ALL_TABLET_TO_LS_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_UPDATE_ALL_TABLET_TO_LS_FAILED", K(ret));
      }
    }
#endif

    DEBUG_SYNC(AFTER_UPDATE_TABLET_TO_LS);

    if (OB_SUCC(ret)) {
      LOG_INFO("[TRANSFER] success update all tablet to ls", "cost", ObTimeUtil::current_time() - start_ts);
    }

  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::UPDATE_ALL_TABLET_TO_LS,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::UPDATE_ALL_TABLET_TO_LS;
  }
  return ret;
}

int ObTransferHandler::lock_tablet_on_dest_ls_for_table_lock_(
    const share::ObTransferTaskInfo &task_info,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", KR(ret));
  } else if (OB_UNLIKELY(!task_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update all tablet to ls get invalid argument", KR(ret), K(task_info));
  } else if (OB_FAIL(ObTransferLockUtil::lock_tablet_on_dest_ls_for_table_lock(
      trans,
      task_info.tenant_id_,
      task_info.dest_ls_id_,
      task_info.table_lock_owner_id_,
      task_info.table_lock_tablet_list_))) {
    LOG_WARN("failed to lock tablet on dest ls for table lock", KR(ret), K(task_info));
  } else {
    LOG_INFO("[TRANSFER] success lock tablet on dest ls for table lock", "cost", ObTimeUtil::current_time() - start_ts);
  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::LOCK_TABLET_ON_DEST_LS_FOR_TABLE_LOCK,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::LOCK_TABLET_ON_DEST_LS_FOR_TABLE_LOCK;
  }
  return ret;
}

int ObTransferHandler::update_transfer_status_(
    const share::ObTransferTaskInfo &task_info,
    const share::ObTransferStatus &next_status,
    const SCN &start_scn,
    const int32_t result,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObTransferTask transfer_task;
  const bool for_update = true;
  const uint64_t tenant_id = task_info.tenant_id_;
  const ObTransferTaskID task_id = task_info.task_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid() || !next_status.is_valid()) {
    LOG_WARN("update transfer status get invalid argument", K(ret), K(task_info), K(next_status));
  } else {
    if (OB_FAIL(ObTransferTaskOperator::get(trans, tenant_id, task_id, for_update, transfer_task, share::OBCG_TRANSFER))) {
      LOG_WARN("failed to get transfer task", K(ret), K(task_id), K(tenant_id));
    } else if (task_info.status_ != transfer_task.get_status()
        || task_info.src_ls_id_ != transfer_task.get_src_ls()
        || task_info.dest_ls_id_ != transfer_task.get_dest_ls()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info in not equal to inner table transfer task, unexpected", K(ret),
          K(task_info), K(transfer_task));
    } else if (start_scn.is_valid() && OB_FAIL(ObTransferTaskOperator::update_start_scn(
                   trans, tenant_id, task_id, transfer_task.get_status(), start_scn, share::OBCG_TRANSFER))) {
      LOG_WARN("failed to update finish scn", K(ret), K(tenant_id), K(task_id), K(start_scn));
    } else if (OB_FAIL(ObTransferTaskOperator::update_status_and_result(
                   trans, tenant_id, task_id, transfer_task.get_status(), next_status, result, share::OBCG_TRANSFER))) {
      LOG_WARN("failed to finish task", K(ret), K(tenant_id), K(task_id));
    } else {
#ifdef ERRSIM
      ObTransferEventRecorder::record_advance_transfer_status_event(
        task_info.tenant_id_, task_info.task_id_, task_info.src_ls_id_,
        task_info.dest_ls_id_, next_status, result);
#endif
      LOG_INFO("[TRANSFER] set next status", K(start_scn), K(task_info), K(next_status), K(result));

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_UPDATE_TRANSFER_TASK_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_UPDATE_TRANSFER_TASK_FAILED", K(ret));
      }
    }
#endif
    }
  }
  return ret;
}

int ObTransferHandler::update_transfer_status_aborted_(
    const share::ObTransferTaskInfo &task_info,
    const int32_t result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_round = round_;
  const share::ObTransferStatus next_status(ObTransferStatus::ABORTED);
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  bool is_leader = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update transfer status aborted get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(start_trans_(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret), K(task_info));
  } else {
    const SCN scn = task_info.start_scn_;
    if (OB_FAIL(lock_transfer_task_(task_info, trans))) {
      LOG_WARN("failed to lock transfer task", K(ret), K(task_info));
    }
    // There is still a possibility of the old leader change task status to ABORT
    // when switching leader occurs after check_self_is_leader_ and before commit.
    // But check_self_is_leader_ occuring after row lock competition
    // is maximize interceptions of old leader change task status to ABORT.
    // It greatly reduce the probability of old leader change task status to ABORT
    else if (OB_FAIL(check_self_is_leader_(is_leader))) {
      LOG_WARN("failed to check self is leader", K(ret), KPC(ls_));
    } else if (!is_leader) {
      ret = OB_NOT_MASTER;
      LOG_WARN("ls leader has been changed", K(ret), K(task_info));
    } else if (OB_FAIL(update_transfer_status_(task_info, next_status, scn, result, trans))) {
      LOG_WARN("failed to update transfer status", K(ret), K(task_info), K(next_status));
    }

    if (OB_TMP_FAIL(commit_trans_(ret, trans))) {
      LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    } else if (OB_SUCCESS == ret) {
      round_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::TRANSFER_START_END,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, tmp_round, true/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::TRANSFER_START_END;
  }
  return ret;
}

bool ObTransferHandler::can_retry_(
    const share::ObTransferTaskInfo &task_info,
    const int32_t result)
{
  bool bool_ret = false;
  int64_t max_transfer_start_retry_count = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    max_transfer_start_retry_count = tenant_config->_transfer_start_retry_count;
  }

  if (!task_info.is_valid()) {
    bool_ret = false;
  } else if (ObTransferStatus::DOING == task_info.status_) {
    bool_ret = true;
    retry_count_++;
  } else if (ObTransferStatus::START == task_info.status_) {
    if (ObTransferUtils::is_need_retry_error(result) && retry_count_ < max_transfer_start_retry_count) {
      retry_count_++;
      bool_ret = true;
    } else {
      bool_ret = false;
    }

#ifdef ERRSIM
    if (!bool_ret) {
      //do nothing
    } else {
      bool_ret = EN_START_CAN_NOT_RETRY ? false: true;
    }
#endif
  } else if (ObTransferStatus::ABORTED == task_info.status_) {
    bool_ret = true;
    retry_count_++;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObTransferHandler::report_to_meta_table_(
    const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    LOG_WARN("reoirt to meta table get invalid argument", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_info.tablet_list_.count(); ++i) {
      const ObTransferTabletInfo &tablet_info = task_info.tablet_list_.at(i);
      if (OB_ISNULL(MTL(observer::ObTabletTableUpdater*))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablet table updater should not be null", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(
          task_info.dest_ls_id_, tablet_info.tablet_id_))) {
        LOG_WARN("failed to submit tablet update task", K(tmp_ret), K(tablet_info), K(task_info));
      }
    }
  }
  return ret;
}

int ObTransferHandler::do_with_doing_status_(const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxFinishTransfer finish_transfer;
  const ObTransferTaskID task_id = task_info.task_id_;
  const uint64_t tenant_id = task_info.tenant_id_;
  const share::ObLSID &src_ls_id = task_info.src_ls_id_;
  const share::ObLSID &dest_ls_id = task_info.dest_ls_id_;
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("transfer_errsim", "before_transfer_doing",
                      "task_id", task_id,
                      "tenant_id", tenant_id,
                      "src_ls_id", src_ls_id,
                      "dest_ls_id", dest_ls_id);
  DEBUG_SYNC(BEFORE_TRANSFER_DOING);
#endif

  if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls leader get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(finish_transfer.init(
      task_id, tenant_id, src_ls_id, dest_ls_id, *sql_proxy_))) {
    LOG_INFO("[TRANSFER] do with doing status", K(task_info));
  } else if (OB_FAIL(finish_transfer.process(round_))) {
    LOG_WARN("failed to process", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (can_retry_(task_info, ret)) {
      LOG_INFO("transfer task can retry", K(ret), K(task_info));
      ob_usleep(INTERVAL_US);
      wakeup_();
    }
  }
  return ret;
}

int ObTransferHandler::do_with_aborted_status_(
    const share::ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const share::ObTransferStatus next_status(ObTransferStatus::FAILED);
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  const int64_t tmp_round = round_;
  const int64_t start_ts = ObTimeUtil::current_time();
  diagnose_result_msg_ = share::ObStorageHACostItemName::MAX_NAME;
  process_perf_diagnose_info_(ObStorageHACostItemName::TRANSFER_ABORT_BEGIN,
      ObStorageHADiagTaskType::TRANSFER_ABORT, start_ts, round_, false/*is_report*/);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls leader get invalid argument", K(ret), K(task_info));
  } else {
    if (OB_FAIL(start_trans_(timeout_ctx, trans))) {
      LOG_WARN("failed to start trans", K(ret), K(task_info));
    } else {
      const SCN scn = task_info.start_scn_;
      const int32_t result = task_info.result_;
      if (OB_FAIL(lock_transfer_task_(task_info, trans))) {
        LOG_WARN("failed to lock transfer task", K(ret), K(task_info));
      } else if (OB_FAIL(update_transfer_status_(task_info, next_status, scn, result, trans))) {
        LOG_WARN("failed to update transfer status", K(ret), K(task_info), K(next_status));
      } else if (OB_FAIL(unlock_src_and_dest_ls_member_list_(task_info))) {
        diagnose_result_msg_ = share::ObStorageHACostItemName::UNLOCK_MEMBER_LIST_IN_ABORT;
        LOG_WARN("failed to unlock src and dest ls member list", K(ret), K(task_info));
      } else {
        process_perf_diagnose_info_(ObStorageHACostItemName::UNLOCK_MEMBER_LIST_IN_ABORT,
            ObStorageHADiagTaskType::TRANSFER_ABORT, start_ts, round_, false/*is_report*/);
      }

      if (OB_TMP_FAIL(commit_trans_(ret, trans))) {
        LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
        if (OB_SUCCESS == ret) {
          ret = tmp_ret;
          diagnose_result_msg_ = share::ObStorageHACostItemName::ABORT_TRANS_COMMIT;
        }
      } else if (OB_SUCCESS == ret) {
        round_ = 0;
      }
      if (OB_SUCC(ret)) {
        process_perf_diagnose_info_(ObStorageHACostItemName::ABORT_TRANS_COMMIT,
            ObStorageHADiagTaskType::TRANSFER_ABORT, start_ts, tmp_round, false/*is_report*/);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (can_retry_(task_info, ret)) {
      LOG_INFO("transfer task can retry", K(ret), K(task_info));
      if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {
        if (OB_SUCCESS != (tmp_ret = record_server_event_(ret, tmp_round, task_info))) {
          LOG_WARN("failed to record server event", K(tmp_ret), K(ret), K(retry_count_), K(task_info));
        }
      }
      ob_usleep(INTERVAL_US);
      wakeup_();
    }
  } else if (OB_SUCCESS != (tmp_ret = record_server_event_(ret, tmp_round, task_info))) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret), K(retry_count_), K(task_info));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_DIAGNOSE_ABORT_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "fake EN_TRANSFER_DIAGNOSE_ABORT_FAILED", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADiagMgr::add_transfer_error_diagnose_info(task_info.task_id_, task_info.dest_ls_id_,
        share::ObStorageHADiagTaskType::TRANSFER_ABORT, tmp_round, ret, diagnose_result_msg_))) {
      LOG_WARN("failed to add error diagnose info", K(tmp_ret), K(ret),
          K(task_info.task_id_), K(task_info.dest_ls_id_), K(tmp_round));
    }
  }

  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::TRANSFER_ABORT_END,
        ObStorageHADiagTaskType::TRANSFER_ABORT, start_ts, tmp_round, true/*is_report*/);
  }
  return ret;
}

int ObTransferHandler::do_worker_transfer_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(transfer_worker_mgr_.process())) {
    LOG_WARN("failed to process transfer backfill TX or replace logical table", K(ret));
  } else {
    LOG_INFO("do worker transfer", KPC(ls_));
  }
  return ret;
}

int ObTransferHandler::block_and_kill_tx_(
    const share::ObTransferTaskInfo &task_info,
    const bool enable_kill_trx,
    ObTimeoutCtx &timeout_ctx,
    bool &succ_block_tx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  succ_block_tx = false;
  const uint64_t tenant_id = task_info.tenant_id_;
  const share::ObLSID &src_ls_id = task_info.src_ls_id_;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t before_kill_trx_threshold = 0;
  int64_t after_kill_trx_threshold = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  int64_t active_trans_count = 0;
  if (tenant_config.is_valid()) {
    before_kill_trx_threshold = tenant_config->_balance_kill_transaction_threshold;
    after_kill_trx_threshold = tenant_config->_balance_wait_killing_transaction_end_threshold;
  }

  if (!enable_kill_trx && OB_FAIL(check_src_ls_has_active_trans_(src_ls_id))) {
    LOG_WARN("failed to check src ls has active trans", K(ret), K(task_info));
  } else if (OB_FAIL(block_tx_(tenant_id, src_ls_id, gts_seq_))) {
    LOG_WARN("failed to block tx", K(ret), K(task_info));
  } else if (FALSE_IT(succ_block_tx = true)) {
  } else if (!enable_kill_trx) {
    if (OB_FAIL(get_ls_active_trans_count_(src_ls_id, active_trans_count))) {
      LOG_WARN("failed to get src ls has active trans", K(ret));
    } else if (0 != active_trans_count) {
      ret = OB_TRANSFER_WAIT_TRANSACTION_END_TIMEOUT;
      LOG_WARN("transfer src ls still has active transactions, cannot do transfer", K(ret), K(src_ls_id),
          K(active_trans_count));
    }
  } else if (OB_FAIL(check_and_kill_tx_(tenant_id, src_ls_id, before_kill_trx_threshold, false/*with_trans_kill*/, timeout_ctx))) {
    LOG_WARN("failed to check after kill", K(ret));
  } else if (OB_FAIL(check_and_kill_tx_(tenant_id, src_ls_id, after_kill_trx_threshold, true/*with_trans_kill*/, timeout_ctx))) {
    LOG_WARN("failed to check after kill", K(ret));
  } else {
    LOG_INFO("[TRANSFER] success to block and kill tx", "cost", ObTimeUtil::current_time() - start_ts);
  }

  if (OB_FAIL(ret) && succ_block_tx) {
    if (OB_SUCCESS != (tmp_ret = unblock_tx_(task_info.tenant_id_, task_info.src_ls_id_, gts_seq_))) {
      LOG_WARN("failed to unblock tx", K(tmp_ret), K(task_info), K(gts_seq_));
    } else {
      succ_block_tx = false;
    }
  }

#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "AFTER_TRANSFER_BLOCK_AND_KILL_TX");
#endif
  DEBUG_SYNC(AFTER_TRANSFER_BLOCK_AND_KILL_TX);
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::BLOCK_AND_KILL_TX,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::BLOCK_AND_KILL_TX;
  }
  return ret;
}

int ObTransferHandler::check_and_kill_tx_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t timeout,
    const bool with_trans_kill,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  static const int64_t CHECK_INTERVAL = 10_ms;
  const int64_t start_ts = ObTimeUtility::current_time();
  do {
    const int64_t cur_ts = ObTimeUtility::current_time();
    int64_t active_trans_count = 0;
    if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("trans ctx already timeout", K(ret));
    } else if (cur_ts - start_ts > timeout) {
      if (with_trans_kill) {
        ret = OB_TRANSFER_WAIT_TRANSACTION_END_TIMEOUT;
        LOG_WARN("wait active trans finish timeout", K(ret), K(cur_ts), K(start_ts));
      } else {
        break;
      }
    } else if (OB_ISNULL(ls_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", K(ret));
    } else if (ls_->is_stopped()) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ls is not running, stop checking", K(ret));
    } else if (OB_FAIL(get_ls_active_trans_count_(ls_id, active_trans_count))) {
      LOG_WARN("failed to get src ls has active trans", K(ret));
    } else if (0 != active_trans_count) {
      LOG_INFO("still has active trans", K(tenant_id), K(ls_id), K(active_trans_count));
      if (with_trans_kill && OB_FAIL(kill_tx_(tenant_id, ls_id, gts_seq_))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to kill tx", K(ret), K(tenant_id), K(ls_id));
        }
      }
    } else {
      break;
    }
    ob_usleep(CHECK_INTERVAL);
  } while (OB_SUCC(ret));
  return ret;
}

int ObTransferHandler::block_tx_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &gts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransferUtils::block_tx(tenant_id, ls_id, gts))) {
    LOG_WARN("failed to block tx", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObTransferHandler::kill_tx_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &gts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransferUtils::kill_tx(tenant_id, ls_id, gts))) {
    LOG_WARN("failed to kill tx", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObTransferHandler::unblock_tx_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &gts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransferUtils::unblock_tx(tenant_id, ls_id, gts))) {
    LOG_WARN("failed to unblock tx", K(ret), K(tenant_id), K(ls_id));
    if (OB_SEQUENCE_NOT_MATCH == ret) {
      ret = OB_SUCCESS;
    } else {
      ob_abort();
    }
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "AFTER_TRANSFER_UNBLOCK_TX");
#endif
  DEBUG_SYNC(AFTER_TRANSFER_UNBLOCK_TX);
  if (OB_SUCC(ret)) {
    process_perf_diagnose_info_(ObStorageHACostItemName::UNBLOCK_TX,
        ObStorageHADiagTaskType::TRANSFER_START, 0/*start_ts*/, round_, false/*is_report*/);
  } else {
    diagnose_result_msg_ = share::ObStorageHACostItemName::UNBLOCK_TX;
  }
  return ret;
}

int ObTransferHandler::record_server_event_(const int32_t result, const int64_t round, const share::ObTransferTaskInfo &task_info) const
{
  int ret = OB_SUCCESS;
  ObSqlString extra_info_str;
  if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task info invalid", K(ret), K(task_info));
  } else if (OB_FAIL(extra_info_str.append_fmt("round:%ld;", round))) {
    LOG_WARN("fail to printf round", K(ret), K(round));
  } else {
    SERVER_EVENT_ADD("storage_ha", "transfer",
        "tenant_id", task_info.tenant_id_,
        "trace_id", task_info.trace_id_,
        "src_ls", task_info.src_ls_id_.id(),
        "dest_ls", task_info.dest_ls_id_.id(),
        "status", task_info.status_.str(),
        "result", result,
        extra_info_str.ptr());
  }
  return ret;
}

int ObTransferHandler::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else {
    if (OB_FAIL(transfer_worker_mgr_.cancel_dag_net())) {
      LOG_WARN("failed to cancel dag net", K(ret), KPC(ls_));
    } else {
      is_safe = true;
    }
  }
  return ret;
}

int ObTransferHandler::offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else {
    const int64_t timeout_us = 1 * 1000 * 1000; //1s;
    bool lock_succ = false;
    if (OB_FAIL(transfer_handler_lock_.wrlock(timeout_us))) {
      if (ret == OB_TIMEOUT) {
        ret = OB_EAGAIN;
      }
      LOG_WARN("[TRANSFER] lock transfer_handler_lock_ failed", K(ret), K(timeout_us));
    } else {
      lock_succ = true;
      transfer_handler_enabled_ = false;
    }

    if (lock_succ) {
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(transfer_handler_lock_.unlock())) {
        LOG_WARN("[RetainCtxMgr] unlock retain_ctx_mgr failed", K(tmp_ret));
      }
    }

    if (OB_SUCC(ret)) {
      int retry_cnt = 0;
      do {
        if (OB_FAIL(transfer_worker_mgr_.cancel_dag_net())) {
          LOG_WARN("failed to cancel dag net", K(ret), KPC(ls_));
        }
      } while (retry_cnt ++ < 3/*max retry cnt*/ && OB_EAGAIN == ret);
    }
  }
  return ret;
}

void ObTransferHandler::online()
{
  transfer_worker_mgr_.reset_task_id();
  {
    common::SpinWLockGuard guard(transfer_handler_lock_);
    transfer_handler_enabled_ = true;
  }
  wakeup_();
}

int ObTransferHandler::stop_tablets_schedule_medium_(const ObIArray<ObTabletID> &tablet_ids, bool &succ_stop)
{
  int ret = OB_SUCCESS;
  succ_stop = false;
  if (OB_FAIL(MTL(ObTenantTabletScheduler*)->stop_tablets_schedule_medium(tablet_ids, compaction::ObProhibitScheduleMediumMap::ProhibitFlag::TRANSFER))) {
    LOG_WARN("failed to stop tablets schedule medium", K(ret));
  } else {
    succ_stop = true;
  }
  if (OB_FAIL(ret)) {
    diagnose_result_msg_ = share::ObStorageHACostItemName::STOP_LS_SCHEDULE_MEMDIUM;
  }
  return ret;
}

int ObTransferHandler::clear_prohibit_medium_flag_(const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObTenantTabletScheduler*)->clear_tablets_prohibit_medium_flag(tablet_ids, compaction::ObProhibitScheduleMediumMap::ProhibitFlag::TRANSFER))) {
    LOG_WARN("failed to clear prohibit schedule medium flag", K(ret));
  }
  return ret;
}
/*
 * when src_ls replica replay to latest (> transfer_scn)
 *
 * we can collect active tx info from replica, because we have set transfer_blocking on moving ctxs
 *
 * after collect we can register move_tx_ctx MDS operation on dest_ls
 */

int ObTransferHandler::do_move_tx_to_dest_ls_(const share::ObTransferTaskInfo &task_info,
                                              ObTimeoutCtx &timeout_ctx,
                                              ObMySQLTransaction &trans,
                                              const SCN data_end_scn,
                                              const SCN transfer_scn,
                                              ObIArray<ObTabletID> &tablet_list,
                                              ObIArray<ObTransID> &move_tx_ids,
                                              int64_t &move_tx_count)
{
  LOG_INFO("[TRANSFER] do_move_tx_to_dest_ls_", K(task_info), K(data_end_scn));
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObLSHandle src_ls_handle;
  CollectTxCtxInfo collect_res;
  collect_res.src_ls_id_ = task_info.src_ls_id_;
  collect_res.dest_ls_id_ = task_info.dest_ls_id_;
  collect_res.task_id_ = task_info.task_id_.id();
  // TODO lana optimise transfer_epoch value
  collect_res.transfer_epoch_ = task_info.task_id_.id();
  collect_res.transfer_scn_ = transfer_scn;
  int64_t tx_count = 0;
  int64_t buf_len = 0;
  int64_t collect_count = 0;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(task_info.src_ls_id_,src_ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(task_info));
  } else if (OB_FAIL(src_ls_handle.get_ls()->collect_tx_ctx(task_info.dest_ls_id_,
                                                            data_end_scn,
                                                            tablet_list,
                                                            move_tx_ids,
                                                            collect_count,
                                                            collect_res.args_))) {
    LOG_WARN("collect tx ctx failed", KR(ret), K(task_info));
  } else if (collect_count != collect_res.args_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collect tx ctx count mismatch", KR(ret), K(collect_count), K(collect_res));
  } else if (FALSE_IT(move_tx_count = collect_count)) {
  } else if (0 == collect_count) {
    // no active tx do nothing
  } else if (collect_res.args_.count() <= MOVE_TX_BATCH) {
    // register once
    if (OB_FAIL(register_move_tx_ctx_batch_(task_info,
                                            transfer_scn,
                                            trans,
                                            collect_res,
                                            buf_len))) {
      LOG_WARN("register move_tx_ctx batch failed", KR(ret), K(task_info));
    }
  } else {
    // register batch
    int64_t start_idx = 0;
    while (OB_SUCC(ret) && start_idx < collect_res.args_.count()) {
      int64_t batch_len = 0;
      CollectTxCtxInfo collect_batch;
      collect_batch.src_ls_id_ = task_info.src_ls_id_;
      collect_batch.dest_ls_id_ = task_info.dest_ls_id_;
      collect_batch.task_id_ = task_info.task_id_.id();
      collect_batch.transfer_epoch_ = task_info.task_id_.id();
      collect_batch.transfer_scn_ = transfer_scn;
      for (int count =0; OB_SUCC(ret) && count < MOVE_TX_BATCH && start_idx < collect_res.args_.count(); count++) {
        if (OB_FAIL(collect_batch.args_.push_back(collect_res.args_.at(start_idx)))) {
          LOG_WARN("push to array fail", KR(ret));
        }
        start_idx++;
      }
      if (FAILEDx(register_move_tx_ctx_batch_(task_info,
                                              transfer_scn,
                                              trans,
                                              collect_batch,
                                              batch_len))) {
      } else {
        buf_len += batch_len;
      }
      LOG_INFO("register move_tx_ctx batch", KR(ret), K(start_idx), K(batch_len));
    }
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("do_move_tx_to_dest_ls_", KR(ret), "cost", end_time-start_time,
                                              K(task_info),
                                              "move_tx_count", collect_res.args_.count(),
                                              "buf_size", buf_len);
  return ret;
}

int ObTransferHandler::register_move_tx_ctx_batch_(const share::ObTransferTaskInfo &task_info,
                                                  const SCN transfer_scn,
                                                  ObMySQLTransaction &trans,
                                                  CollectTxCtxInfo &collect_batch,
                                                  int64_t &batch_len)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = collect_batch.get_serialize_size();
  int64_t pos = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  observer::ObInnerSQLConnection *conn = NULL;
  ObRegisterMdsFlag flag;
  flag.need_flush_redo_instantly_ = true;
  flag.mds_base_scn_ = transfer_scn;
  if (OB_ISNULL(buf = (char*)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret), K(buf_len));
  } else if (OB_FAIL(collect_batch.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), K(collect_batch));
  } else if (buf_len > OB_MAX_LOG_ALLOWED_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("move tx ctx batch exceed log size", KR(ret), K(buf_len));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", KR(ret), K(task_info));
  } else if (OB_FAIL(conn->register_multi_data_source(task_info.tenant_id_, task_info.dest_ls_id_,
      ObTxDataSourceType::TRANSFER_MOVE_TX_CTX, buf, buf_len, flag))) {
    LOG_WARN("failed to register multi data source", KR(ret), K(task_info), K(buf), K(buf_len));
  } else {
    batch_len = buf_len;
  }
  return ret;
}

int ObTransferHandler::do_trans_transfer_dest_prepare_(
    const share::ObTransferTaskInfo &task_info,
    ObMySQLTransaction &trans)
{
  LOG_INFO("do_trans_transfer_dest_prepare_", K(task_info));
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  ObTransferDestPrepareInfo info;
  info.task_id_ = task_info.task_id_.id();
  info.src_ls_id_ = task_info.src_ls_id_;
  info.dest_ls_id_ = task_info.dest_ls_id_;
  int64_t buf_len = info.get_serialize_size();
  int64_t pos = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  observer::ObInnerSQLConnection *conn = NULL;
  ObRegisterMdsFlag flag;
  flag.need_flush_redo_instantly_ = true;
  if (OB_ISNULL(buf = (char*)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret), K(buf_len));
  } else if (OB_FAIL(info.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), K(info));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", KR(ret), K(task_info));
  } else if (OB_FAIL(conn->register_multi_data_source(task_info.tenant_id_, task_info.dest_ls_id_,
      ObTxDataSourceType::TRANSFER_DEST_PREPARE, buf, buf_len, flag))) {
    LOG_WARN("failed to register multi data source", KR(ret), K(task_info), K(buf), K(buf_len));
  }
  int64_t end_time = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] do_trans_transfer_dest_prepare_", KR(ret), "cost", end_time - start_time, K(task_info));
  return ret;
}

int ObTransferHandler::wait_src_ls_advance_weak_read_ts_(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  ObAddr dest_ls_leader;
  ObStorageHASrcInfo addr_info;
  addr_info.cluster_id_ = GCONF.cluster_id;
  if (OB_FAIL(get_ls_leader_(task_info.dest_ls_id_, dest_ls_leader))) {
    LOG_WARN("failed to get src ls leader", K(ret), K(task_info));
  } else if (FALSE_IT(addr_info.src_addr_ = dest_ls_leader)) {
  } else {
     int64_t start_time = ObClockGenerator::getClock();
     share::SCN transfer_dest_prepare_scn;
     int64_t timeout = timeout_ctx.get_timeout();
     // get dest_ls transfer_dest_prepare_scn
     while (OB_SUCC(ret)) {
       if (OB_FAIL(storage_rpc_->get_transfer_dest_prepare_scn(task_info.tenant_id_,
                                                               addr_info,
                                                               task_info.dest_ls_id_,
                                                               transfer_dest_prepare_scn))) {
         LOG_WARN("failed to get transfer_dest_prepare_scn", KR(ret), K(task_info));
       } else if (!transfer_dest_prepare_scn.is_valid()) {
         LOG_WARN("transfer_dest_prepare_scn is invalid need retry", K(task_info.task_id_));
         if (ObTimeUtil::current_time() - start_time > timeout) {
           ret = OB_TIMEOUT;
           FLOG_WARN("failed to get transfer_dest_prepare_scn", KR(ret), K(task_info));
         } else {
           ob_usleep(50 * 1000);
         }
       } else {
         break;
       }
     }
     int64_t step_time = ObClockGenerator::getClock();
     LOG_INFO("[TRANSFER] get dest_ls transfer_dest_prepare_scn", KR(ret), K(task_info.task_id_), K(transfer_dest_prepare_scn),
         "cost", step_time - start_time, K(timeout));
     // check src_ls advance weak_read_ts
     while (OB_SUCC(ret)) {
       SCN weak_read_ts = ls_->get_ls_wrs_handler()->get_ls_weak_read_ts();
       if (weak_read_ts <= transfer_dest_prepare_scn) {
         LOG_WARN("wait src_ls weak_read_ts advance", K(task_info.task_id_), K(weak_read_ts), K(transfer_dest_prepare_scn));
         if (ObClockGenerator::getClock() - start_time > timeout) {
           ret = OB_TIMEOUT;
           FLOG_WARN("failed to wait src_ls advance transfer_dest_prepare_scn", KR(ret), K(task_info), K(transfer_dest_prepare_scn));
         } else {
           ob_usleep(20 * 1000);
         }
       } else {
         break;
       }
     }
     int64_t end_time = ObClockGenerator::getClock();
     LOG_INFO("[TRANSFER] wait src_ls weak_read_ts advance", KR(ret), K(task_info.task_id_), K(transfer_dest_prepare_scn),
         "cost", end_time - step_time, K(timeout));
  }
  return ret;
}

// TODO(handora.qc): remove it under 4.3.x later
int enable_new_transfer(bool &enable)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_INFO("[TRANSFER] get min data version failed", K(ret));
  } else if (DATA_VERSION_4_3_0_0 > data_version) {
    enable = false;
  } else if (!tenant_config->_enable_active_txn_transfer) {
    enable = false;
  } else {
    enable = true;
  }

  return ret;
}

int ObTransferHandler::clear_prohibit_(
    const share::ObTransferTaskInfo &task_info,
    const ObIArray<ObTabletID> &tablet_ids,
    const bool is_block_tx,
    const bool is_medium_stop)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtil::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clear prohibit get invalid argument", K(ret), K(task_info));
  } else if (is_block_tx && OB_FAIL(unblock_tx_(task_info.tenant_id_, task_info.src_ls_id_, gts_seq_))) {
    LOG_WARN("failed to unblock tx", K(ret), K(task_info), K(gts_seq_));
  }

  if (OB_FAIL(ret)) {
  } else if (is_medium_stop && OB_FAIL(clear_prohibit_medium_flag_(tablet_ids))) {
    LOG_WARN("failed to clear prohibit medium flag", K(ret), K(task_info));
    ob_abort();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ls_->get_lock_table()->enable_check_tablet_status(false))) {
    LOG_WARN("failed to cancel lock table check tablet status", K(ret), K(task_info));
    ob_abort();
  }

  LOG_INFO("[TRANSFER] clear prohibit", K(ret), "cost", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObTransferHandler::get_config_version_(
    palf::LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (OB_FAIL(ls_->get_log_handler()->get_leader_config_version(config_version))) {
    LOG_WARN("failed to get leader config version", K(ret), K(config_version), KPC(ls_));
  }
  return ret;
}

int ObTransferHandler::check_config_version_(
    const palf::LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion current_config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!config_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check config version get invalid arugment", K(ret), K(config_version));
  } else if (OB_FAIL(get_config_version_(current_config_version))) {
    LOG_WARN("failed to get config version", K(ret), KPC(ls_));
  } else if (config_version != current_config_version) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ls leader has been changed", K(ret), KPC(ls_), K(config_version), K(current_config_version));
  }
  return ret;
}

// Only src ls could work when task status is START or ABORT.
// Conversely dest ls work when task status is DOING.
// The benefit of above is that the src ls leader can make controlling medium compaction a local execution,
// which is more controllable.
// The ABORT status will change to FAILED status in src ls work time.
int ObTransferHandler::check_task_exist_(
    const ObTransferStatus &status, const bool find_by_src_ls, bool &task_exist) const
{
  int ret = OB_SUCCESS;
  task_exist = false;
  if (!status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (find_by_src_ls && (status.is_start_status() || status.is_aborted_status())) {
    task_exist = true;
  } else if (!find_by_src_ls && status.is_doing_status()) {
    task_exist = true;
  } else {
    task_exist = false;
  }
  return ret;
}

int ObTransferHandler::set_related_info(
    const share::ObTransferTaskID &task_id,
    const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!task_id.is_valid() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(start_scn));
  } else if (OB_FAIL(related_info_.set_info(task_id, start_scn))) {
    LOG_WARN("fail to set info", K(ret), K(task_id), K(start_scn));
  }
  return ret;
}

int ObTransferHandler::get_related_info_task_id(share::ObTransferTaskID &task_id) const
{
  int ret = OB_SUCCESS;
  task_id.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (OB_FAIL(related_info_.get_related_info_task_id(task_id))) {
    LOG_WARN("failed to get task id", K(ret));
  }
  return ret;
}

int ObTransferHandler::record_error_diagnose_info_in_replay(
    const share::ObTransferTaskID &task_id,
    const share::ObLSID &dest_ls_id,
    const int result_code,
    const bool clean_related_info,
    const share::ObStorageHADiagTaskType type,
    const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!task_id.is_valid()
      || !dest_ls_id.is_valid()
      || type < share::ObStorageHADiagTaskType::TRANSFER_START
      || type >= share::ObStorageHADiagTaskType::MAX_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(dest_ls_id), K(type));
  } else if (OB_FAIL(related_info_.record_error_diagnose_info_in_replay(
        task_id, dest_ls_id, result_code, clean_related_info, type, result_msg))) {
    LOG_WARN("failed to record diagnose info in replay", K(ret), K(task_id), K(dest_ls_id),
        K(result_code), K(clean_related_info), K(type), K(result_msg));
  }
  return ret;
}

int ObTransferHandler::record_error_diagnose_info_in_backfill(
  const share::SCN &log_sync_scn,
  const share::ObLSID &dest_ls_id,
  const int result_code,
  const ObTabletID &tablet_id,
  const ObMigrationStatus &migration_status,
  const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!log_sync_scn.is_valid()
      || !dest_ls_id.is_valid()
      || !tablet_id.is_valid()
      || OB_SUCCESS == result_code
      || migration_status < ObMigrationStatus::OB_MIGRATION_STATUS_NONE
      || migration_status >= ObMigrationStatus::OB_MIGRATION_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_sync_scn), K(dest_ls_id), K(result_code), K(tablet_id), K(migration_status));
  } else if (OB_FAIL(related_info_.record_error_diagnose_info_in_backfill(
        log_sync_scn, dest_ls_id, result_code, tablet_id, migration_status, result_msg))) {
    LOG_WARN("failed to record diagnose info in worker", K(ret), K(log_sync_scn),
        K(dest_ls_id), K(result_code), K(tablet_id), K(migration_status), K(result_msg));
  }
  return ret;
}

void ObTransferHandler::reset_related_info()
{
  related_info_.reset();
}

int ObTransferHandler::reset_related_info(const share::ObTransferTaskID &task_id)
{
  int ret = OB_SUCCESS;
  if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else {
    related_info_.reset(task_id);
  }
  return ret;
}

void ObTransferHandler::process_perf_diagnose_info_(
      const ObStorageHACostItemName name,
      const ObStorageHADiagTaskType task_type,
      const int64_t start_ts,
      const int64_t round, const bool is_report) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  ObTransferPerfDiagInfo info;
  ObStorageHATimestampItem item;
  item.name_ = name;
  item.type_ = ObStorageHACostItemType::FLUENT_TIMESTAMP_TYPE;
  item.retry_id_ = round;
  item.timestamp_ = ObTimeUtil::current_time();
  common::ObTabletID tablet_id;
  share::ObStorageHADiagTaskKey key;
  if (OB_FAIL(info.init(&alloc, MTL_ID()))) {
    LOG_WARN("fail to init info", K(ret));
  } else if (OB_FAIL(info.add_item(item))) {
    LOG_WARN("fail to add item", K(ret), K(item));
  } else if (OB_FAIL(ObStorageHADiagMgr::construct_diagnose_info_key(task_info_.task_id_, ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE,
      task_type, ObStorageHADiagType::PERF_DIAGNOSE, round, tablet_id, key))) {
    LOG_WARN("failed to construct error diagnose info key", K(ret), K(task_info_.task_id_), K(round), K(tablet_id));
  } else if (OB_FAIL(ObStorageHADiagMgr::construct_diagnose_info(task_info_.task_id_, task_info_.dest_ls_id_,
      task_type, round, OB_SUCCESS, ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE, info))) {
    LOG_WARN("failed to construct diagnose info", K(ret), K(task_info_), K(round), K(task_type));
  } else if (OB_FAIL(ObStorageHADiagMgr::add_transfer_perf_diagnose_info(key, start_ts, task_info_.tablet_list_.count(), is_report, info))) {
    LOG_WARN("failed to add perf diagnose info", K(ret), K(key), K(info), K(start_ts), K(task_info_), K(is_report));
  }
}

int ObTransferHandler::record_perf_diagnose_info_in_replay(
    const share::ObStorageHAPerfDiagParams &params,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!params.is_valid()
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params), K(start_ts));
  } else if (OB_FAIL(related_info_.record_perf_diagnose_info_in_replay(params, result, timestamp, start_ts, is_report))) {
    LOG_WARN("failed to record diagnose info in replay", K(ret), K(params), K(result), K(start_ts), K(timestamp), K(is_report));
  }
  return ret;
}

int ObTransferHandler::get_src_ls_member_list_(
    common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  logservice::ObLogHandler *log_handler = NULL;
  int64_t paxos_replica_num = 0;
  logservice::ObLogService *log_service = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (OB_ISNULL(log_handler = ls_->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler should not be NULL", K(ret));
  } else if (OB_FAIL(log_handler->get_paxos_member_list(member_list, paxos_replica_num))) {
    LOG_WARN("failed to get paxos member list", K(ret));
  } else if (member_list.get_member_number() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls member list number is unexpected", K(ret), K(member_list));
  }
  return ret;
}

int ObTransferHandler::record_perf_diagnose_info_in_backfill(
    const share::ObStorageHAPerfDiagParams &params,
    const share::SCN &log_sync_scn,
    const int result_code,
    const ObMigrationStatus &migration_status,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else if (!params.is_valid()
      ||!log_sync_scn.is_valid()
      || migration_status < ObMigrationStatus::OB_MIGRATION_STATUS_NONE
      || migration_status >= ObMigrationStatus::OB_MIGRATION_STATUS_MAX
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_sync_scn), K(start_ts), K(params), K(migration_status));
  } else if (OB_FAIL(related_info_.record_perf_diagnose_info_in_backfill(params, log_sync_scn,
      result_code, migration_status, timestamp, start_ts, is_report))) {
    LOG_WARN("failed to record diagnose info in backfill", K(ret), K(params), K(log_sync_scn),
        K(result_code), K(migration_status), K(start_ts), K(timestamp), K(is_report));
  }
  return ret;
}

int ObTransferHandler::broadcast_tablet_location_(const ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  share::ObLocationService *location_service = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_UNLIKELY(!task_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_info not valid", KR(ret), K(task_info));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service should not be NULL", K(ret), KP(location_service));
  } else {
    ObTabletLocationBroadcastTask broadcast_task;
    ObArray<ObTransferTabletInfo> tablet_info_list;
    if (OB_FAIL(tablet_info_list.reserve(task_info.tablet_list_.count()))) {
      LOG_WARN("failed to reserve for tablet_info_list", KR(ret), "count", task_info.tablet_list_.count());
    }
    // increment transfer_seq
    FOREACH_CNT_X(tablet_info, task_info.tablet_list_, OB_SUCC(ret)) {
      ObTransferTabletInfo new_tablet_info;
      if (OB_FAIL(new_tablet_info.init(tablet_info->tablet_id(), tablet_info->transfer_seq() + 1))) {
        LOG_WARN("failed to init new_table_info", KR(ret), KPC(tablet_info));
      } else if (OB_FAIL(tablet_info_list.push_back(new_tablet_info))) {
        LOG_WARN("failed to push_back", KR(ret), K(new_tablet_info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(broadcast_task.init(task_info.tenant_id_,
                                           task_info.task_id_,
                                           task_info.dest_ls_id_,
                                           tablet_info_list))) {
      LOG_WARN("failed to init broadcast_task", KR(ret), K(task_info), K(tablet_info_list));
    } else if (OB_FAIL(location_service->submit_tablet_broadcast_task(broadcast_task))) {
      LOG_WARN("failed to submit tablet location broadcast task", KR(ret), K(broadcast_task));
    }
  }
  return ret;
}

int ObTransferHandler::do_clean_diagnose_info_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  ObStorageHADiagMgr *mgr = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer handler do not init", K(ret));
  } else if (OB_FAIL(check_self_is_leader_(is_leader))) {
    LOG_WARN("failed to check self is leader", K(ret), KPC(ls_));
  } else if (is_leader) {
    // clean follower diagnose info
  } else if (!task_info_.is_valid()) {
    // unset error ret code
  } else if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObStorageHADiagMgr from MTL", K(ret));
  } else if (OB_FAIL(mgr->report_task(task_info_.task_id_))) {
    LOG_WARN("fail to report task", K(ret), K(task_info_.task_id_));
  }
  return ret;
}

}
}
