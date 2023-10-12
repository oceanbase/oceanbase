/**
 * Copyright (c) 2022 OceanBase
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
#include "share/ob_rs_mgr.h"
#include "share/ob_rpc_struct.h"
#include "ob_storage_ha_utils.h"
#include "logservice/ob_log_service.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"
#include "share/transfer/ob_transfer_task_operator.h"
#include "storage/high_availability/ob_finish_transfer.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/high_availability/ob_transfer_lock_utils.h"
#include "storage/tablet/ob_tablet.h"
#include "observer/ob_server_event_history_table_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::observer;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace storage {

//errsim def
ERRSIM_POINT_DEF(EN_DOING_UNLOCK_TRANSFER_MEMBER_LIST_FAILED);
ERRSIM_POINT_DEF(EN_DOING_LOCK_TRANSFER_TASK_FAILED);
ERRSIM_POINT_DEF(EN_DOING_LOCK_MEMBER_LIST_FAILED);
ERRSIM_POINT_DEF(EN_FINISH_TRANSFER_IN_FAILED);
ERRSIM_POINT_DEF(EN_DOING_WAIT_ALL_DEST_TABLET_NORAML);
ERRSIM_POINT_DEF(EN_FINISH_TRANSFER_OUT_FAILED);
ERRSIM_POINT_DEF(EN_DOING_UPDATE_TRANSFER_TASK_FAILED);
ERRSIM_POINT_DEF(EN_DOING_COMMIT_TRANS_FAILED);

ObTxFinishTransfer::ObTxFinishTransfer()
    : is_inited_(false),
      task_id_(),
      tenant_id_(OB_INVALID_ID),
      src_ls_id_(),
      dest_ls_id_(),
      mutex_(),
      cond_(),
      sql_proxy_(NULL)
{}

ObTxFinishTransfer::~ObTxFinishTransfer()
{}

int ObTxFinishTransfer::init(const ObTransferTaskID &task_id, const uint64_t tenant_id, const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish transfer do not init", K(ret));
  } else if (!task_id.is_valid() || OB_INVALID_ID == tenant_id || !src_ls_id.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(task_id), K(tenant_id), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::STORAGE_HA_FINISH_TRANSFER))) {
    LOG_WARN("failed to init condition", K(ret));
  } else {
    task_id_ = task_id;
    tenant_id_ = tenant_id;
    src_ls_id_ = src_ls_id;
    dest_ls_id_ = dest_ls_id;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObTxFinishTransfer::process(int64_t &round)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx finish transfer do not init", K(ret));
  } else if (OB_FAIL(do_tx_transfer_doing_(task_id_, tenant_id_, src_ls_id_, dest_ls_id_, round))) {
    LOG_WARN("failed to do tx transfer doing", K(ret), K_(task_id), K_(tenant_id), K_(src_ls_id), K_(dest_ls_id), K(round));
  } else {
    LOG_INFO("process tx finish transfer", K_(task_id), K_(tenant_id), K_(src_ls_id), K_(dest_ls_id));
  }
  return ret;
}

int ObTxFinishTransfer::do_tx_transfer_doing_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, int64_t &round)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArray<ObTransferTabletInfo> tablet_list;
  ObMemberList member_list;
  int64_t quorum = 0;
  observer::ObInnerSQLConnection *conn = NULL;
  SCN start_scn;
  SCN finish_scn;
  bool is_majority_passed = false;
  share::ObRsMgr *rs_mgr = GCTX.rs_mgr_;
  obrpc::ObSrvRpcProxy *svr_rpc_proxy = GCTX.srv_rpc_proxy_;
  int64_t result = OB_SUCCESS;
  common::ObArray<common::ObAddr> member_addr_list;
  bool majority_backfilled = false;
  ObLSLocation ls_location;
  bool is_leader = false;
  bool is_ready = false;
  ObTransferService *transfer_service = NULL;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  ObLSHandle ls_handle;
  ObDisplayTabletList table_lock_tablet_list;
  transaction::tablelock::ObTableLockOwnerID lock_owner_id;
  ObTimeoutCtx timeout_ctx;
  const int64_t tmp_round = round;
  if (!task_id.is_valid() || OB_INVALID_ID == tenant_id || !src_ls_id.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(task_id), K(tenant_id), K(src_ls_id), K(dest_ls_id));
  } else if (OB_ISNULL(transfer_service = MTL_WITH_CHECK_TENANT(ObTransferService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service is NULL", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_self_ls_leader_(dest_ls_id, is_leader))) {
    LOG_WARN("failed to check self ls leader", K(ret), K(dest_ls_id));
  } else if (!is_leader) {
    ret = OB_IS_CHANGING_LEADER;
    LOG_WARN("self is not leader of dest ls", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(get_ls_member_list_(tenant_id, dest_ls_id, member_list))) {
    LOG_WARN("failed to get ls member list", K(ret), K(tenant_id), K(dest_ls_id));
  }  // TODO(yangyi.yyy): get member list and check self is leader together
  // 1. Release the locking relationship of the member list of dest_ls and src_ls
  // (the interface for binding and releasing the locking relationship needs to have the ability to re-entrant)
  else if (OB_FAIL(unlock_src_and_dest_ls_member_list_(tenant_id, src_ls_id, dest_ls_id, member_list))) {
    LOG_WARN("failed to unlock src and dest ls member list", K(ret), K(tenant_id), K(src_ls_id), K(dest_ls_id));
  }
  // get tablet info list from inner table
  else if (OB_FAIL(get_transfer_tablet_info_from_inner_table_(
               task_id, tenant_id, tablet_list, start_scn, table_lock_tablet_list, lock_owner_id))) {
    LOG_WARN("failed to get transfer tablet list", K(ret), K(task_id), K(tenant_id));
  } else if (OB_FAIL(get_transfer_quorum_(member_list, quorum))) {
    LOG_WARN("failed to get transfer quorum", K(ret), K(member_list));
  }
  // precheck if majority ls logical table replaced
  else if (OB_FAIL(check_ls_logical_table_replaced(
               tenant_id, dest_ls_id, member_list, tablet_list, quorum, is_ready))) {
    LOG_WARN("wait all ls replica logical table replaced failed",
        K(ret),
        K(tenant_id),
        K(dest_ls_id),
        K(member_list),
        K(quorum));
  } else if (!is_ready) {
    LOG_INFO("transfer in tablet not ready", K(ret), K(tenant_id), K(dest_ls_id));
    transfer_service->wakeup();
  } else if (OB_FAIL(get_ls_handle_(tenant_id, dest_ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else {
    const ObTransferLockStatus lock_status(ObTransferLockStatus::DOING);
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("TRANSFER", "BEFORE_TRANSFER_DOING_START_TRANS");
#endif
    DEBUG_SYNC(SWITCH_LEADER_BEFORE_TRANSFER_DOING_START_TRANS);
    ObTimeoutCtx timeout_ctx;
    // 2. The leader of dest_ls starts a transaction TRANS_TRANSFER_FINISH
    if (FAILEDx(start_trans_(tenant_id, trans, timeout_ctx))) {
      LOG_WARN("failed to start trans", K(ret), K(tenant_id));
    } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("TRANSFER", "AFTER_TRANSFER_DOING_START_TRANS");
#endif
      DEBUG_SYNC(SWITCH_LEADER_AFTER_TRANSFER_DOING_START_TRANS);
      if (FAILEDx(select_transfer_task_for_update_(task_id, trans))) {
        LOG_WARN("failed to select for update", K(ret), K(task_id));
      }
      // 3. The dest_ls leader checks whether the transfer tablet corresponding to the src_ls copy corresponding to the
      // majority replica has been backfilled. Here, try to ensure that it is all completed, and then start the second
      // step. Only if the threshold is exceeded will there be a majority.
      //    a) This step needs to first lock the member list of dest_ls
      //    b) The dest_ls leader checks whether the majority has completed backfilling
      //    c) Unlock the member list of dest_ls This step and the migration still need to be mutually exclusive,
      //       because there may be dest_leader checking that the majority meets
      //       the conditions. After unlocking the member list, a copy is migrated in. This copy replaces one of the
      //       majority, resulting in a majority If the dispatch does not meet the conditions, the following chapters on
      //       transfer and migration will discuss
      else if (OB_FAIL(lock_ls_member_list_(tenant_id, dest_ls_id, member_list, lock_status))) {
        LOG_WARN("failed to lock ls member list", K(ret), K(tenant_id), K(dest_ls_id), K(member_list));
      } else if (OB_FAIL(check_ls_logical_table_replaced(
                    tenant_id, dest_ls_id, member_list, tablet_list, quorum, is_ready))) {
        LOG_WARN("wait all ls replica logical table replaced failed",
            K(ret),
            K(tenant_id),
            K(dest_ls_id),
            K(member_list),
            K(quorum));
      } else if (!is_ready) {
        LOG_INFO("transfer in tablet not ready", K(ret), K(tenant_id), K(dest_ls_id));
        transfer_service->wakeup();
      } else {
        // 4. The leader node of dest_ls registers the multi-source transaction
        // ObInnerSQLConnection->register_multi_source_data, and the type is TX_FINISH_TRANSFER_IN (two-way barrier)
        //    The content of the log is src_ls_id, dest_ls_id, tablet_list. This step requires forcibly flushing the redo
        //    log. The purpose of flushing redo here is to obtain finish_scn and check whether the dest_ls log playback is
        //    new enough for src_ls.
        //      a) When the leader of dest_ls receives the resgister_succ, it checks that its replay scn is greater than
        //      start_scn, and has completed the replacement of the logical table.
        //      b) When the leader and follower of dest_ls receive the on_redo stage,
        //         they record the scn returned by on_redo as finish_scn. Check that your replay scn is greater than
        //         start_scn, and the replacement of the logical table has been completed. If it has been completed,
        //         change the ObTabletStatus of transfer_tablets to NORMAL. If it is not completed, the playback of
        //         ob_redo needs to be stuck.
        if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("conn_ is NULL", K(ret));
        } else if (OB_FAIL(do_tx_finish_transfer_in_(
                      task_id, tenant_id, src_ls_id, dest_ls_id, start_scn, tablet_list, conn))) {
          LOG_WARN("failed to do tx finish transfer in",
              K(ret),
              K(task_id),
              K(tenant_id),
              K(src_ls_id),
              K(dest_ls_id),
              K(tablet_list));
        }
  #ifdef ERRSIM
        SERVER_EVENT_SYNC_ADD("TRANSFER", "BETWEEN_REGISTER_FINISH_TRANSFER_IN_AND_OUT");
  #endif
        DEBUG_SYNC(SWITCH_LEADER_BETWEEN_FINISH_TRANSFER_IN_AND_OUT);
        // 5. The leader of dest ls checks that majority ls replia satisfies the finish_scn of the playback
        // (the step of non-multi-source transaction, only the leader of dest ls does).
        // Abort is required if no majority replays to finish_scn beyond the threshold.
        // Here, the leader of dest_ls checks the destination of the majority to be more secure, and if the
        // FINISH_TRANSFER step fails, it will not affect the reading and writing of dest_ls, but the main switch will
        // have an impact, so here we will also guarantee all within a certain threshold. Copy, after a certain threshold
        // is exceeded, the majority is guaranteed; here, the transfer seq at the log stream level also needs to be
        // incremented (in TX_START_TRASNFER_IN)
        if (FAILEDx(wait_transfer_tablet_status_normal_(tenant_id, dest_ls_id, tablet_list, start_scn, timeout_ctx, finish_scn))) {
          LOG_WARN("failed to wait tablet status normal", K(ret), K(tenant_id), K(dest_ls_id), K(tablet_list));
        } else if (OB_FAIL(member_list.get_addr_array(member_addr_list))) {
          LOG_WARN("failed to get addr array", K(ret), K(member_list));
        } else if (OB_FAIL(wait_all_ls_replica_replay_scn_(task_id, tenant_id, dest_ls_id,
            member_addr_list, finish_scn, quorum, timeout_ctx, is_majority_passed))) {
          LOG_WARN("failed to check ls replica replay scn",
              K(ret),
              K(tenant_id),
              K(dest_ls_id),
              K(member_addr_list),
              K(finish_scn));
        } else if (!is_majority_passed) {
          ret = OB_TIMEOUT;
          LOG_WARN("majority replay scn not passed", K(ret));
        }
        // 6. The leader of dest_ls registers a multi-source transaction,
        // and the type is TX_FINISH_TRANSFER_OUT. The contents of the log are src_ls_id, dest_ls_id, finish_scn, and
        // tablet_id_list.
        //    a) When the leader and follower of src_ls receive on_redo, change the ObTabletStatus of transfer_tablets to
        //    TRANFER_OUT_DELETED (the status may share DELETED)
        if (FAILEDx(
                do_tx_finish_transfer_out_(task_id, tenant_id, src_ls_id, dest_ls_id, finish_scn, tablet_list, conn))) {
          LOG_WARN("failed to do tx finish transfer out",
              K(ret),
              K(task_id),
              K(tenant_id),
              K(src_ls_id),
              K(dest_ls_id),
              K(finish_scn));
        }
        // 7. Update __all_transfer_task according to the result of transaction execution.
        // If successful, push __all_transfer_task to FINISH state,
        // otherwise keep the status of __all_transfer_task as DOING
        else if (OB_FAIL(update_transfer_task_result_(task_id, tenant_id, finish_scn, OB_SUCCESS, trans))) {
          LOG_WARN("failed to update transfer status", K(ret), K(task_id), K(tenant_id), K(finish_scn));
        }
        // 8. unlock table lock on src ls for tablet (must be successful)
        else if (OB_FAIL(ObTransferLockUtil::unlock_tablet_on_src_ls_for_table_lock(
                      trans, tenant_id, src_ls_id, lock_owner_id, table_lock_tablet_list))) {
          LOG_WARN("failed to unlock tablet on src ls for table lock", KR(ret),
              K(tenant_id), K(src_ls_id), K(lock_owner_id), K(table_lock_tablet_list));
        }
        // 9. unlock member list
        else if (OB_FAIL(
                    unlock_ls_member_list_(tenant_id, dest_ls_id, member_list, lock_status, CONFIG_CHANGE_TIMEOUT))) {
          LOG_WARN("failed to unlock ls member list", K(ret), K(tenant_id), K(dest_ls_id), K(member_list));
        }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_DOING_COMMIT_TRANS_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_DOING_COMMIT_TRANS_FAILED", K(ret));
      }
    }
#endif
        DEBUG_SYNC(BEFORE_DOING_TRANSFER_COMMIT);
        // 10. LOG_COMMIT_FINISH
        bool is_commit = OB_SUCCESS == ret;
        if (OB_TMP_FAIL(commit_trans_(is_commit, trans))) {
          if (OB_SUCCESS == ret) {
            ret = tmp_ret;
          }
        } else if (is_commit) {
          round = 0;
        }
        // 11. After the dest_ls leader succeeds,
        // it will report the corresponding results to RS.
        // This step does not guarantee success.
        if (OB_TMP_FAIL(report_result_(task_id, result, svr_rpc_proxy))) {
          LOG_WARN("failed to report rpc result", K(ret), K(task_id), KP(rs_mgr), KP(svr_rpc_proxy));
        }
      }
    }
  }
  if (OB_TMP_FAIL(record_server_event_(ret, is_ready, tmp_round))) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret), K(is_ready));
  }
  return ret;
}

int ObTxFinishTransfer::unlock_src_and_dest_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id, common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  bool is_same = false;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  const int64_t lock_timeout = CONFIG_CHANGE_TIMEOUT;
  bool same_member_list = true;
  const ObTransferLockStatus status(ObTransferLockStatus::START);
  if (!src_ls_id.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(unlock_ls_member_list_(tenant_id, src_ls_id, member_list, status, lock_timeout))) {
    LOG_WARN("failed to unlock ls member list", K(ret), K(tenant_id), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(unlock_ls_member_list_(tenant_id, dest_ls_id, member_list, status, lock_timeout))) {
    LOG_WARN("failed to unlock ls member list", K(ret), K(tenant_id), K(src_ls_id), K(dest_ls_id));
  } else {
    LOG_INFO(
        "[TRANSFER] unlock src and dest ls member list", K(tenant_id), K(src_ls_id), K(dest_ls_id), K(member_list));

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_DOING_UNLOCK_TRANSFER_MEMBER_LIST_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_DOING_UNLOCK_TRANSFER_MEMBER_LIST_FAILED", K(ret));
      }
    }
#endif

  }
  UNUSEDx(member_list);
  return ret;
}

int ObTxFinishTransfer::get_transfer_tablet_info_from_inner_table_(
    const ObTransferTaskID &task_id,
    const uint64_t tenant_id,
    common::ObArray<ObTransferTabletInfo> &tablet_list,
    SCN &start_scn,
    ObDisplayTabletList &table_lock_tablet_list,
    transaction::tablelock::ObTableLockOwnerID &lock_owner_id)
{
  int ret = OB_SUCCESS;
  tablet_list.reset();
  table_lock_tablet_list.reset();
  lock_owner_id.reset();
  const bool for_update = false;
  ObTransferTask transfer_task;
  if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(task_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get(*sql_proxy_, tenant_id, task_id, for_update, transfer_task, share::OBCG_STORAGE_HA_LEVEL2))) {
    LOG_WARN("failed to get transfer task", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(tablet_list.assign(transfer_task.get_tablet_list()))) {
    LOG_WARN("failed to assign tablet_list", KR(ret), K(transfer_task));
  } else if (OB_FAIL(table_lock_tablet_list.assign(transfer_task.get_table_lock_tablet_list()))) {
    LOG_WARN("failed to assign table_lock_tablet_list", KR(ret), K(transfer_task));
  } else {
    start_scn = transfer_task.get_start_scn();
    lock_owner_id = transfer_task.get_table_lock_owner_id();
    LOG_INFO("get transfer info from inner table", K(task_id), K(tenant_id),
        K(transfer_task), K(tablet_list), K(table_lock_tablet_list), K(lock_owner_id));
  }
  return ret;
}

int ObTxFinishTransfer::wait_transfer_tablet_status_normal_(
    const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObArray<share::ObTransferTabletInfo> &tablet_list,
    const share::SCN &start_scn,
    ObTimeoutCtx &timeout_ctx,
    share::SCN &finish_scn)
{
  int ret = OB_SUCCESS;
  int64_t begin_us = ObTimeUtility::current_time();
  const int64_t CHECK_TABLET_STATUS_INTERVAL = 100_ms;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || tablet_list.empty() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait transfer tablet status normal get invalid argument", K(ret), K(tenant_id), K(ls_id), K(tablet_list), K(start_scn));
  } else {
    while (OB_SUCC(ret)) {
      bool is_ready = false;
      if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(check_transfer_tablet_status_normal_(tenant_id, ls_id, tablet_list, start_scn, is_ready, finish_scn))) {
        LOG_WARN("failed to check transfer tablet status normal", K(ret), K(tenant_id), K(tablet_list));
      } else if (is_ready) {
        LOG_INFO("tablet is ready", K(tenant_id), K(ls_id), K(tablet_list));
        break;
      } else {
        ob_usleep(CHECK_TABLET_STATUS_INTERVAL);
      }
    }
  }
  return ret;
}

int ObTxFinishTransfer::check_transfer_tablet_status_normal_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObArray<share::ObTransferTabletInfo> &tablet_list,
    const share::SCN &start_scn,
    bool &is_ready,
    share::SCN &finish_scn)
{
  int ret = OB_SUCCESS;
  is_ready = true;
  storage::ObLS *ls = NULL;
  storage::ObLSHandle ls_handle;
  finish_scn.reset();

  if (OB_FAIL(get_ls_handle_(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls handle", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); ++i) {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = NULL;
      ObTabletCreateDeleteMdsUserData user_data;
      const ObTransferTabletInfo &tablet_info = tablet_list.at(i);
      const common::ObTabletID &tablet_id = tablet_info.tablet_id_;
      if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet_handle, user_data))) {
        LOG_WARN("failed to get tablet status", K(ret), K(tablet_id), K(tablet_handle));
      } else if (ObTabletStatus::NORMAL != user_data.tablet_status_) {
        if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet status is not expected", K(ret), K(tablet_info), K(user_data));
        } else {
          is_ready = false;
          LOG_INFO("tablet is not ready, need retry", K(tablet_info));
          break;
        }
      } else if (user_data.transfer_scn_ < start_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("finish scn should not smaller than start scn", K(ret), K(tablet_id), K(start_scn), K(user_data));
      } else if (user_data.transfer_scn_ > start_scn) {
        if (0 == i) {
          finish_scn = user_data.transfer_scn_;
        } else if (finish_scn != user_data.transfer_scn_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet finish scn is not same, unexpected", K(ret), K(tablet_id),
              K(start_scn), K(finish_scn), K(user_data));
        }
      } else {
        is_ready = false;
        LOG_INFO("tablet is not ready, need retry", K(tablet_info));
        break;
      }
    }
  }
  return ret;
}

int ObTxFinishTransfer::check_ls_logical_table_replaced(const uint64_t tenant_id,
    const share::ObLSID &dest_ls_id, const common::ObMemberList &member_list,
    const common::ObArray<ObTransferTabletInfo> &tablet_list, const int64_t quorum, bool &all_backfilled)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_leader = false;
  const int64_t cluster_id = GCONF.cluster_id;
  ObLSLocation ls_location;
  ObArray<ObAddr> addr_array;
  if (OB_FAIL(check_self_ls_leader_(dest_ls_id, is_leader))) {
    LOG_WARN("failed to check self ls leader", K(ret), K(dest_ls_id));
  } else if (!is_leader) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("self is not leader", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(member_list.get_addr_array(addr_array))) {
    LOG_WARN("failed to get addr array", K(ret), K(member_list));
  } else if (OB_FAIL(inner_check_ls_logical_table_replaced_(
                 tenant_id, dest_ls_id, addr_array, tablet_list, quorum, all_backfilled))) {
    LOG_WARN("failed to inner check majority backfilled", K(ret), K(tenant_id), K(dest_ls_id), K(addr_array));
  } else {
    LOG_INFO("check ls logical table replace", K(tenant_id), K(dest_ls_id), K(addr_array), K(tablet_list), K(quorum), K(all_backfilled));
  }
  return ret;
}

int ObTxFinishTransfer::inner_check_ls_logical_table_replaced_(const uint64_t tenant_id,
    const share::ObLSID &dest_ls_id, const common::ObArray<common::ObAddr> &member_addr_list,
    const common::ObArray<ObTransferTabletInfo> &tablet_list, const int64_t quorum, bool &all_backfilled)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t cur_quorum = 0;
  const int64_t cluster_id = GCONF.cluster_id;
  FOREACH_X(location, member_addr_list, OB_SUCC(ret))
  {
    if (OB_ISNULL(location)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location should not be null", K(ret));
    } else {
      const common::ObAddr &server = *location;
      bool backfill_completed = false;
      if (OB_FAIL(post_check_logical_table_replaced_request_(
              cluster_id, server, tenant_id, dest_ls_id, tablet_list, backfill_completed))) {
        LOG_WARN("failed to check criteria", K(ret), K(cluster_id), K(tenant_id), K(dest_ls_id), K(server));
      } else if (!backfill_completed) {
        LOG_INFO("server has not finish backfill", K(tenant_id), K(dest_ls_id), K(quorum), K(member_addr_list), K(server));
      } else {
        cur_quorum++;
        LOG_INFO("server has replayed passed finish scn", K(ret), K(server));
      }
    }
  }
  if (OB_SUCC(ret)) {
    all_backfilled = cur_quorum == quorum;
  }
  return ret;
}

int ObTxFinishTransfer::do_tx_finish_transfer_in_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, const SCN &start_scn,
    const common::ObArray<ObTransferTabletInfo> &tablet_list, observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  bool force_flush_redo = true;
  bool is_leader = false;
  char *buf = NULL;
  int64_t buf_len = 0;
  const transaction::ObTxDataSourceType type = transaction::ObTxDataSourceType::FINISH_TRANSFER_IN;
  ObTXFinishTransferInInfo finish_transfer_in_info;
  ObArenaAllocator allocator;
  ObRegisterMdsFlag flag;
  flag.need_flush_redo_instantly_ = true;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn should not be null", K(ret));
  } else if (OB_FAIL(build_tx_finish_transfer_in_info_(
                 src_ls_id, dest_ls_id, start_scn, tablet_list, finish_transfer_in_info))) {
    LOG_WARN("failed to build tx finish transfer in info",
        K(ret),
        K(src_ls_id),
        K(dest_ls_id),
        K(start_scn),
        K(tablet_list));
  } else if (OB_FAIL(construct_multi_data_source_buf_(finish_transfer_in_info, allocator, buf, buf_len))) {
    LOG_WARN("failed to construct multi data source buf", K(ret), K(finish_transfer_in_info));
  } else if (OB_FAIL(conn->register_multi_data_source(tenant_id, dest_ls_id, type, buf, buf_len, flag))) {
    LOG_WARN("failed to register multi data source", K(ret), K(tenant_id), K(dest_ls_id), K(type));
  } else {
#ifdef ERRSIM
    ObTransferEventRecorder::record_transfer_task_event(task_id, "TX_FINISH_TRANSFER_IN", src_ls_id, dest_ls_id);
#endif
    LOG_INFO("register multi data source for finish transfer in", K(task_id), K(src_ls_id), K(dest_ls_id));

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_FINISH_TRANSFER_IN_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_FINISH_TRANSFER_IN_FAILED", K(ret));
      }
    }
#endif
  }
  return ret;
}

int ObTxFinishTransfer::do_tx_finish_transfer_out_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, const share::SCN &finish_scn,
    const common::ObArray<ObTransferTabletInfo> &tablet_list, observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  bool force_flush_redo = true;
  bool is_leader = false;
  char *buf = NULL;
  int64_t buf_len = 0;
  const transaction::ObTxDataSourceType type = transaction::ObTxDataSourceType::FINISH_TRANSFER_OUT;
  ObTXFinishTransferOutInfo finish_transfer_out_info;
  ObArenaAllocator allocator;
  ObRegisterMdsFlag flag;
  flag.need_flush_redo_instantly_ = false;
  flag.mds_base_scn_ = finish_scn;

  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn should not be null", K(ret));
  } else if (OB_FAIL(build_tx_finish_transfer_out_info_(
                 src_ls_id, dest_ls_id, finish_scn, tablet_list, finish_transfer_out_info))) {
    LOG_WARN("failed to build tx finish transfer out info",
        K(ret),
        K(src_ls_id),
        K(dest_ls_id),
        K(finish_scn),
        K(tablet_list));
  } else if (OB_FAIL(construct_multi_data_source_buf_(finish_transfer_out_info, allocator, buf, buf_len))) {
    LOG_WARN("failed to construct multi data source buf", K(ret), K(finish_transfer_out_info));
  } else if (OB_FAIL(conn->register_multi_data_source(tenant_id, src_ls_id, type, buf, buf_len, flag))) {
    LOG_WARN("failed to register multi data source", K(ret), K(tenant_id));
  } else {
#ifdef ERRSIM
    ObTransferEventRecorder::record_transfer_task_event(task_id, "TX_FINISH_TRANSFER_OUT", src_ls_id, dest_ls_id);
#endif
    LOG_INFO("[TRANSFER] register multi data source for finish transfer out", K(task_id), K(src_ls_id), K(dest_ls_id));

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_FINISH_TRANSFER_OUT_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_FINISH_TRANSFER_OUT_FAILED", K(ret));
      }
    }
#endif

    DEBUG_SYNC(AFTER_FINISH_TRANSFER_OUT);
  }
  return ret;
}

int ObTxFinishTransfer::wait_all_ls_replica_replay_scn_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::ObLSID &ls_id, const common::ObArray<common::ObAddr> &member_addr_list, const share::SCN &finish_scn,
    const int64_t quorum, ObTimeoutCtx &timeout_ctx, bool &check_passed)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    check_passed = false;
    if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("some ls replay not finished", K(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(check_all_ls_replica_replay_scn_(
            task_id, tenant_id, ls_id, member_addr_list, finish_scn, quorum, check_passed))) {
      LOG_WARN("failed to check all ls replica replay scn",
          K(ret),
          K(tenant_id),
          K(member_addr_list),
          K(ls_id),
          K(quorum));
    } else if (check_passed) {
      LOG_INFO("all ls has passed ls replica replay scn", K(tenant_id), K(ls_id));
      break;
    } else {
      ob_usleep(DEFAULT_WAIT_INTERVAL_US);
    }
  }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_DOING_WAIT_ALL_DEST_TABLET_NORAML ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_DOING_WAIT_ALL_DEST_TABLET_NORAML", K(ret));
      }
    }
#endif

  DEBUG_SYNC(AFTER_DOING_TRANSFER_WAIT_REPLAY_SCN);

  return ret;
}

int ObTxFinishTransfer::check_all_ls_replica_replay_scn_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::ObLSID &ls_id, const common::ObArray<common::ObAddr> &member_addr_list, const share::SCN &finish_scn,
    const int64_t quorum, bool &meet_criteria)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t cur_quorum = 0;
  FOREACH_X(location, member_addr_list, OB_SUCC(ret))
  {
    if (OB_ISNULL(location)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location should not be null", K(ret));
    } else {
      const common::ObAddr &server = *location;
      bool passed_finish_scn = false;
      if (OB_FAIL(inner_check_ls_replay_scn_(task_id, tenant_id, ls_id, server, finish_scn, passed_finish_scn))) {
        LOG_WARN("failed to check criteria", K(ret), K(task_id), K(tenant_id), K(ls_id), K(server));
      } else if (!passed_finish_scn) {
        LOG_INFO("server has not passed finish scn", K(task_id), K(tenant_id), K(server), K(finish_scn));
      } else {
        cur_quorum++;
        LOG_INFO("server has replayed passed finish scn", K(ret), K(server));
      }
    }
  }
  if (OB_SUCC(ret)) {
    meet_criteria = cur_quorum == quorum ;
  }
  return ret;
}

int ObTxFinishTransfer::inner_check_ls_replay_scn_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::ObLSID &ls_id, const common::ObAddr &addr, const SCN &finish_scn, bool &passed_scn)
{
  int ret = OB_SUCCESS;
  passed_scn = false;
  const int64_t cluster_id = GCONF.cluster_id;
  SCN tmp_finish_scn;
  if (OB_FAIL(fetch_ls_replay_scn_(task_id, cluster_id, addr, tenant_id, ls_id, tmp_finish_scn))) {
    LOG_WARN("failed to fetch finish scn for transfer", K(ret), K(task_id), K(tenant_id), K(ls_id));
  } else {
    passed_scn = tmp_finish_scn >= finish_scn;
    LOG_INFO("check ls replay scn", K(passed_scn), K(tmp_finish_scn), K(finish_scn));
  }
  return ret;
}

int ObTxFinishTransfer::get_ls_handle_(
    const uint64_t tenant_id, const share::ObLSID &ls_id, storage::ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ls_handle.reset();
  ObLSService *ls_service = NULL;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret), K(tenant_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObTxFinishTransfer::get_ls_member_list_(
    const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  storage::ObLS *ls = NULL;
  storage::ObLSHandle ls_handle;
  int64_t quorum = 0;
  bool is_leader = false;
  if (OB_FAIL(get_ls_handle_(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (OB_FAIL(check_self_ls_leader_(ls_id, is_leader))) {
    LOG_WARN("failed to check self ls leader", K(ret), K(ls_id));
  } else if (!is_leader) {
    ret = OB_IS_CHANGING_LEADER;
    LOG_WARN("self is not leader", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_paxos_member_list(member_list, quorum))) {
    LOG_WARN("failed to get paxos member list", K(ret));
  } else if (OB_FAIL(check_self_ls_leader_(ls_id, is_leader))) {
    LOG_WARN("failed to check self ls leader", K(ret), K(ls_id));
  } else if (!is_leader) {
    ret = OB_IS_CHANGING_LEADER;
    LOG_WARN("self is not leader", K(ret), K(ls_id));
  } else {
    LOG_INFO("get ls member list", K(tenant_id), K(ls_id), K(member_list));
  }
  return ret;
}

// TODO(yangyi.yyy): impl later
// extract common function later
int ObTxFinishTransfer::check_same_member_list_(
    const uint64_t tenant_id, const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, bool &same_member_list)
{
  int ret = OB_SUCCESS;
  UNUSEDx(tenant_id, src_ls_id, dest_ls_id, same_member_list);
  return ret;
}

int ObTxFinishTransfer::unlock_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObMemberList &member_list, const ObTransferLockStatus &status, const int64_t lock_timeout)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMemberListLockUtils::unlock_ls_member_list(
      tenant_id, ls_id, task_id_.id(), member_list, status, share::OBCG_STORAGE_HA_LEVEL2, *sql_proxy_))) {
    LOG_WARN("failed to unlock ls member list", K(ret), K(tenant_id), K(ls_id), K_(task_id), K(status));
  }
  return ret;
}

int ObTxFinishTransfer::lock_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObMemberList &member_list, const ObTransferLockStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMemberListLockUtils::lock_ls_member_list(
      tenant_id, ls_id, task_id_.id(), member_list, status, share::OBCG_STORAGE_HA_LEVEL2, *sql_proxy_))) {
    LOG_WARN("failed to unlock ls member list", K(ret), K(ls_id), K(member_list));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_DOING_LOCK_MEMBER_LIST_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_DOING_LOCK_MEMBER_LIST_FAILED", K(ret));
      }
    }
#endif

    DEBUG_SYNC(AFTER_DOING_TRANSFER_LOCK_MEMBER_LIST);
  }
  return ret;
}

int ObTxFinishTransfer::build_tx_finish_transfer_in_info_(const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id, const share::SCN &start_scn,
    const common::ObArray<ObTransferTabletInfo> &tablet_list, ObTXFinishTransferInInfo &transfer_in_info)
{
  int ret = OB_SUCCESS;
  transfer_in_info.reset();
  if (!src_ls_id.is_valid() || !dest_ls_id.is_valid() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(src_ls_id), K(dest_ls_id));
  } else {
    transfer_in_info.src_ls_id_ = src_ls_id;
    transfer_in_info.dest_ls_id_ = dest_ls_id;
    transfer_in_info.start_scn_ = start_scn;
    if (OB_FAIL(transfer_in_info.tablet_list_.assign(tablet_list))) {
      LOG_WARN("failed to assign tablet list", K(ret), K(tablet_list));
    }
  }
  return ret;
}

int ObTxFinishTransfer::build_tx_finish_transfer_out_info_(const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id, const share::SCN &finish_scn,
    const common::ObArray<ObTransferTabletInfo> &tablet_list, ObTXFinishTransferOutInfo &transfer_out_info)
{
  int ret = OB_SUCCESS;
  transfer_out_info.reset();
  if (!src_ls_id.is_valid() || !dest_ls_id.is_valid() || !finish_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(src_ls_id), K(dest_ls_id));
  } else {
    transfer_out_info.src_ls_id_ = src_ls_id;
    transfer_out_info.dest_ls_id_ = dest_ls_id;
    transfer_out_info.finish_scn_ = finish_scn;
    if (OB_FAIL(transfer_out_info.tablet_list_.assign(tablet_list))) {
      LOG_WARN("failed to assign tablet list", K(ret), K(tablet_list));
    }
  }
  return ret;
}

template <class TransferInfo>
int ObTxFinishTransfer::construct_multi_data_source_buf_(
    const TransferInfo &transfer_info, common::ObIAllocator &allocator, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  buf = NULL;
  int64_t pos = 0;
  buf_len = transfer_info.get_serialize_size();
  if (!transfer_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(transfer_info));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(buf_len));
  } else if (OB_FAIL(transfer_info.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize", K(ret), K(transfer_info));
  }
  return ret;
}

int ObTxFinishTransfer::update_transfer_task_result_(const ObTransferTaskID &task_id, const uint64_t tenant_id,
    const share::SCN &finish_scn, const int64_t result, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!task_id.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(task_id), K(tenant_id));
  } else {
    ObTransferTask transfer_task;
    const bool for_update = true;
    ObTransferStatus next_status;
    next_status = OB_SUCCESS == result ? ObTransferStatus::COMPLETED : ObTransferStatus::DOING;
    if (OB_FAIL(ObTransferTaskOperator::get(trans, tenant_id, task_id, for_update, transfer_task, share::OBCG_STORAGE_HA_LEVEL2))) {
      LOG_WARN("failed to get transfer task", K(ret), K(task_id), K(tenant_id));
    } else if (transfer_task.get_start_scn() >= finish_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("finish scn not expected", K(ret), K(transfer_task), K(finish_scn));
    } else if (OB_FAIL(ObTransferTaskOperator::update_finish_scn(
                   trans, tenant_id, task_id, transfer_task.get_status(), finish_scn, share::OBCG_STORAGE_HA_LEVEL2))) {
      LOG_WARN("failed to update finish scn", K(ret), K(tenant_id), K(task_id), K(finish_scn));
    } else if (OB_FAIL(ObTransferTaskOperator::finish_task(
                   trans, tenant_id, task_id, transfer_task.get_status(), next_status, result, ObTransferTaskComment::EMPTY_COMMENT, share::OBCG_STORAGE_HA_LEVEL2))) {
      LOG_WARN("failed to finish task", K(ret), K(tenant_id), K(task_id));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_advance_transfer_status_event(
        tenant_id, task_id, src_ls_id_, dest_ls_id_, next_status, result);
#endif
    LOG_INFO("update transfer task result", K(ret), K(task_id), K(tenant_id), K(result), K(finish_scn));

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_DOING_UPDATE_TRANSFER_TASK_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_DOING_UPDATE_TRANSFER_TASK_FAILED", K(ret));
      }
    }
#endif
  }
  return ret;
}

int ObTxFinishTransfer::report_result_(
    const ObTransferTaskID &task_id, const int64_t result, obrpc::ObSrvRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  ObAddr leader_addr;
  int64_t retry_count = 0;
  const int64_t MAX_RETRY_TIMES = 3;
  const int64_t REPORT_RETRY_INTERVAL_MS = 100 * 1000;  // 100ms
  ObFinishTransferTaskArg finish_task_arg;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &sys_ls_id = share::SYS_LS;

  if (OB_FAIL(finish_task_arg.init(tenant_id, task_id))) {
    LOG_WARN("failed to init finish task arg", K(ret), K(tenant_id), K(task_id));
  } else {
    while (retry_count++ < MAX_RETRY_TIMES) {
      if (OB_FAIL(ObStorageHAUtils::get_ls_leader(tenant_id, sys_ls_id, leader_addr))) {
        LOG_WARN("failed to get ls leader", K(ret), K(tenant_id));
      } else if (OB_FAIL(rpc_proxy->to(leader_addr).by(tenant_id).finish_transfer_task(finish_task_arg))) {
        LOG_WARN("failed to report finish transfer task", K(ret), K(leader_addr), K(tenant_id), K(finish_task_arg));
      }
      if (OB_SUCC(ret)) {
        break;
      } else {
        ob_usleep(REPORT_RETRY_INTERVAL_MS);
      }
    }
  }
  return ret;
}

int ObTxFinishTransfer::fetch_ls_replay_scn_(const ObTransferTaskID &task_id, const int64_t cluster_id,
    const common::ObAddr &server_addr, const uint64_t tenant_id, const share::ObLSID &ls_id, share::SCN &finish_scn)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = NULL;
  storage::ObStorageRpc *storage_rpc = NULL;
  storage::ObStorageHASrcInfo src_info;
  src_info.src_addr_ = server_addr;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(src_info), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc proxy is NULL", K(ret));
  } else if (OB_FAIL(storage_rpc->fetch_ls_replay_scn(tenant_id, src_info, ls_id, finish_scn))) {
    LOG_WARN("failed to fetch ls replay scn", K(ret), K(tenant_id), K(src_info), K(ls_id));
  } else {
    LOG_INFO("fetch ls replay scn", K(tenant_id), K(src_info), K(ls_id));
  }
  return ret;
}

int ObTxFinishTransfer::post_check_logical_table_replaced_request_(const int64_t cluster_id,
    const common::ObAddr &server_addr, const uint64_t tenant_id, const share::ObLSID &dest_ls_id,
    const common::ObIArray<share::ObTransferTabletInfo> &tablet_list, bool &replace_finished)
{
  int ret = OB_SUCCESS;
  replace_finished = false;
  ObLSService *ls_service = NULL;
  storage::ObStorageRpc *storage_rpc = NULL;
  storage::ObStorageHASrcInfo src_info;
  src_info.src_addr_ = server_addr;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (!src_info.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(src_info), K(dest_ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc proxy is NULL", K(ret));
  } else if (OB_FAIL(storage_rpc->check_tablets_logical_table_replaced(
                 tenant_id, src_info, dest_ls_id, tablet_list, replace_finished))) {
    LOG_WARN(
        "failed to post transfer backfill finished", K(ret), K(tenant_id), K(src_info), K(dest_ls_id), K(tablet_list));
  } else {
    LOG_INFO("check logical table replaced completed", K(tenant_id), K(src_info), K(dest_ls_id), K(replace_finished));
  }
  return ret;
}

int ObTxFinishTransfer::check_self_ls_leader_(const share::ObLSID &ls_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObRole role = ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const uint64_t tenant_id = MTL_ID();
  is_leader = false;
  if (OB_FAIL(get_ls_handle_(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls handle", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("failed to get role", K(ret), K(tenant_id), K(ls_id));
  } else if (is_strong_leader(role)) {
    is_leader = true;
  } else {
    is_leader = false;
  }
  return ret;
}

int ObTxFinishTransfer::start_trans_(
    const uint64_t tenant_id,
    ObMySQLTransaction &trans,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  int64_t finish_trans_timeout = 10_s;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    finish_trans_timeout = tenant_config->_transfer_finish_trans_timeout;
  }
  const int64_t stmt_timeout = finish_trans_timeout;

  if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, false/*with_snapshot*/, share::OBCG_STORAGE_HA_LEVEL2))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    LOG_INFO("start trans", K(tenant_id));
  }
  return ret;
}

int ObTxFinishTransfer::commit_trans_(const bool is_commit, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans.end(is_commit))) {
    LOG_WARN("end transaction failed", K(ret));
  } else {
    LOG_INFO("commit trans", K(is_commit));
  }
  return ret;
}

int ObTxFinishTransfer::get_transfer_quorum_(const ObMemberList &member_list, int64_t &quorum)
{
  int ret = OB_SUCCESS;
  if (!member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(member_list));
  } else {
    quorum = member_list.get_member_number();
    LOG_INFO("get transfer quorum", K(member_list), K(quorum));
  }
  return ret;
}

int ObTxFinishTransfer::select_transfer_task_for_update_(const ObTransferTaskID &task_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const bool for_update = true;
  ObTransferTask task;
  if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get(trans, tenant_id, task_id, for_update, task, share::OBCG_STORAGE_HA_LEVEL2))) {
    LOG_WARN("failed to get transfer task", K(ret), K(tenant_id), K(task_id));
  } else if (!task.get_status().is_doing_status()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("transfer task status is not doing", K(ret), K(task));
  } else {
    LOG_INFO("select for update", K(task_id), K(task));
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_DOING_LOCK_TRANSFER_TASK_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_DOING_LOCK_TRANSFER_TASK_FAILED", K(ret));
      }
    }
#endif

  }
  return ret;
}

int ObTxFinishTransfer::record_server_event_(
    const int32_t result,
    const bool is_ready,
    const int64_t round) const
{
  int ret = OB_SUCCESS;
  ObSqlString extra_info_str;
  const share::ObTransferStatus doing_status(ObTransferStatus::DOING);
  const share::ObTransferStatus finish_status(ObTransferStatus::COMPLETED);
  if (OB_SUCCESS == result) {
    if (is_ready) {
      if (OB_FAIL(extra_info_str.append_fmt("msg:\"transfer doing success\";"))) {
        LOG_WARN("fail to printf wait info", K(ret));
      }
    } else {
      if (OB_FAIL(extra_info_str.append_fmt("msg:\"wait for ls logical table replaced\";"))) {
        LOG_WARN("fail to printf wait info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extra_info_str.append_fmt("round:%ld;", round))) {
      LOG_WARN("fail to printf retry time", K(ret));
    } else {
      if (OB_SUCCESS == result && is_ready) {
        if (OB_FAIL(write_server_event_(result, extra_info_str, finish_status))) {
          LOG_WARN("fail to write server event", K(ret), K(result), K(extra_info_str));
        }
      } else {
        if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {
          if (OB_FAIL(write_server_event_(result, extra_info_str, doing_status))) {
            LOG_WARN("fail to write server event", K(ret), K(result), K(extra_info_str));
          }
        }
      }
    }
  }
  return ret;
}

int ObTxFinishTransfer::write_server_event_(const int32_t result, const ObSqlString &extra_info, const share::ObTransferStatus &status) const
{
  int ret = OB_SUCCESS;
  SERVER_EVENT_ADD("storage_ha", "transfer",
      "tenant_id", tenant_id_,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "src_ls", src_ls_id_.id(),
      "dest_ls", dest_ls_id_.id(),
      "status", status.str(),
      "result", result,
      extra_info.ptr());
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
