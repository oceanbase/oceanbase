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
#ifndef OCEABASE_STORAGE_FINISH_TRANSFER_H_
#define OCEABASE_STORAGE_FINISH_TRANSFER_H_

#include "storage/ls/ob_ls.h"
#include "common/ob_member_list.h"
#include "logservice/ob_log_handler.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/net/ob_addr.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/transfer/ob_transfer_info.h"
#include "share/ob_balance_define.h"

namespace oceanbase {
namespace storage {

struct ObLockOwner {};

class ObTxFinishTransfer {
public:
  ObTxFinishTransfer();
  virtual ~ObTxFinishTransfer();
  int init(const share::ObTransferTaskID &task_id, const uint64_t tenant_id, const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id, common::ObMySQLProxy &sql_proxy);
  int process(int64_t &round);

private:
  int do_tx_transfer_doing_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, int64_t &round);

  // unlock both src and dest ls member list
  // @param[in]: tenant_id
  // @param[in]: src_ls_id
  // @param[in]: dest_ls_id
  // @param[out]: member list
  int unlock_src_and_dest_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id, common::ObMemberList &member_list);

  // get transfer tablet list from inner table
  // @param[in]: task_id
  // @param[in]: tenant_id
  // @param[out]: tablet info list
  // @param[out]: start scn
  // @param[out]: table lock tablet list
  // @param[out]: lock owner id
  int get_transfer_tablet_info_from_inner_table_(
    const ObTransferTaskID &task_id,
    const uint64_t tenant_id,
    common::ObArray<ObTransferTabletInfo> &tablet_list,
    SCN &start_scn,
    ObDisplayTabletList &table_lock_tablet_list,
    transaction::tablelock::ObTableLockOwnerID &lock_owner_id);

  int wait_transfer_tablet_status_normal_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObArray<share::ObTransferTabletInfo> &tablet_list, const share::SCN &start_scn,
      ObTimeoutCtx &timeout_ctx, share::SCN &finish_scn);

  int check_transfer_tablet_status_normal_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObArray<share::ObTransferTabletInfo> &tablet_list, const share::SCN &start_scn,
      bool &is_ready, share::SCN &finish_scn);

  // check ls backfilled
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[in]: member_list
  // @param[in]: tablet_list
  // @param[in]: quorum
  // @param[in]: majority_backfilled
  int check_ls_logical_table_replaced(const uint64_t tenant_id, const share::ObLSID &dest_ls_id,
      const common::ObMemberList &member_list, const common::ObArray<share::ObTransferTabletInfo> &tablet_list,
      const int64_t quorum, bool &all_backfilled);

  // inner check ls logical table replaced
  // @param[in]: tenant_id
  // @param[in]: dest_ls_id
  // @param[in]: member_addr_list
  // @param[in]: tablet_list
  // @param[in]: quorum
  int inner_check_ls_logical_table_replaced_(const uint64_t tenant_id, const share::ObLSID &dest_ls_id,
      const common::ObArray<common::ObAddr> &member_addr_list,
      const common::ObArray<share::ObTransferTabletInfo> &tablet_list, const int64_t quorum, bool &all_backfilled);

  // wait until all ls replay scn satisfy
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[in]: member_list
  // @param[in]: tablet_list
  // @param[in]: quorum
  // @param[in]: timeout_ctx
  int wait_all_ls_replica_replay_scn_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::ObLSID &ls_id, const common::ObArray<common::ObAddr> &member_addr_list, const share::SCN &finish_scn,
      const int64_t quorum, ObTimeoutCtx &timeout_ctx, bool &check_passed);

  // check ls replica match scn
  // @param[in]: task_id
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[in]: server addr
  // @param[in]: dest_ls_scn
  // @param[in]: current scn
  // @param[bool]: check passed
  int check_all_ls_replica_replay_scn_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::ObLSID &ls_id, const common::ObArray<common::ObAddr> &member_addr_list, const share::SCN &finish_scn,
      const int64_t quorum, bool &check_passed);

  // param[in]: tenant_id,
  // param[in]: ls_id
  // param[in]: server addr
  // param[in]: finish_scn
  // param[out]: is the check passed
  int inner_check_ls_replay_scn_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::ObLSID &ls_id, const common::ObAddr &addr, const share::SCN &finish_scn, bool &passed_scn);

private:
  /* helper functions */

  // get ls member list
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[out]: ls handle
  int get_ls_handle_(const uint64_t tenant_id, const share::ObLSID &ls_id, storage::ObLSHandle &ls_handle);

  // get ls member list
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[in]: member_list
  int get_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObMemberList &member_list);

  // check src ls and dest ls has same member list
  // @param[in]: tenant_id
  // @param[in]: src_ls_id
  // @param[in]: dest_ls_id
  // @param[out]: same member list
  int check_same_member_list_(const uint64_t tenant_id, const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id,
      bool &same_member_list);

  // unlock ls member list
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[in]: member_list
  // @param[in]: lock timeout
  // @param[in]: lock_owner
  int unlock_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObMemberList &member_list, const ObTransferLockStatus &status, const int64_t lock_timeout);

  // lock ls member list
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[in]: member_list
  int lock_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObMemberList &member_list, const ObTransferLockStatus &status);

  // @param[in]: src_ls_id
  // @param[in]: dest_ls_id
  // @param[in]: start_scn
  // @param[in]: tablet_list
  // @param[out]: transfer_out_info
  int build_tx_finish_transfer_in_info_(const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id,
      const share::SCN &start_scn, const common::ObArray<share::ObTransferTabletInfo> &tablet_list,
      ObTXFinishTransferInInfo &transfer_out_info);

  // build tx finish transfer out info
  // @param[in]: src_ls_id
  // @param[in]: dest_ls_id
  // @param[in]: finish_scn
  // @param[in]: tablet_list
  // @param[out]: transfer_out_info
  int build_tx_finish_transfer_out_info_(const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id,
      const share::SCN &finish_scn, const common::ObArray<share::ObTransferTabletInfo> &tablet_list,
      ObTXFinishTransferOutInfo &transfer_out_info);

  // construct multi data source buf
  // @param[in]: finish transfer info
  // @param[in]: allocator
  // @param[out]: buf
  // @param[out]: buf_len
  template <class TransferInfo>
  int construct_multi_data_source_buf_(
      const TransferInfo &transfer_info, common::ObIAllocator &allocator, char *&buf, int64_t &buf_len);

  // update transfer task result
  // @param[in]: task_id
  // @param[in]: tenant_id
  // @param[in]: finish_scn
  // @param[in]: result
  // @param[in]: transaction
  int update_transfer_task_result_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::SCN &finish_scn, const int64_t result, ObMySQLTransaction &trans);

  // report result through rpc
  // @param[in]: task_id
  // @param[in]: result
  // @param[in]: rs rpc proxy
  int report_result_(const share::ObTransferTaskID &task_id, const int64_t result, obrpc::ObSrvRpcProxy *rs_rpc_proxy);

private:
  /*rpc section*/
  int fetch_ls_replay_scn_(const share::ObTransferTaskID &task_id, const int64_t cluster_id,
      const common::ObAddr &server_addr, const uint64_t tenant_id, const share::ObLSID &ls_id, share::SCN &finish_scn);

  // post check if logical sstable has been replaced
  // @param[in]: cluster_id
  // @param[in]: server_addr
  // @param[in]: tenant_id
  // @param[in]: ls_id
  // @param[out] backfill completed
  int post_check_logical_table_replaced_request_(const int64_t cluster_id, const common::ObAddr &server_addr,
      const uint64_t tenant_id, const share::ObLSID &dest_ls_id,
      const common::ObIArray<share::ObTransferTabletInfo> &tablet_list, bool &backfill_completed);

  // check self is leader
  // @param[in]: ls_id
  // @param[out]: is_leader
  int check_self_ls_leader_(const share::ObLSID &ls_id, bool &ls_leader);

  // The leader node of dest_ls registers the multi-source transaction
  // ObInnerSQLConnection->register_multi_source_data,
  // and the type is TX_FINISH_TRANSFER_IN (two-way barrier).
  // The content of the log is src_ls_id, dest_ls_id, tablet_list.
  // This step requires forcibly flushing the redo log.
  // The purpose of brushing redo here is to obtain finish_scn and
  // check whether the dest_ls log playback is new enough for src_ls.

  // TX_FINISH_TRANSFER_IN
  // @param[in]: task_id, the task id of transfer task
  // @param[in]: tenant id
  // @param[in]: src ls id
  // @param[in]: dest ls id
  // @param[in]: start_scn
  // @param[in]: transfer tablet list
  // @param[in]: observer connection
  int do_tx_finish_transfer_in_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, const share::SCN &start_scn,
      const common::ObArray<share::ObTransferTabletInfo> &tablet_list, observer::ObInnerSQLConnection *conn);

  // The leader of dest_ls registers a multi-source transaction,
  // and the type is TX_FINISH_TRANSFER_OUT
  // The contents of the log are src_ls_id, dest_ls_id, and finish_scn.

  // TX_FINISH_TRANSFER_OUT
  // @param[in]: task_id
  // @param[in]: tenant_id
  // @param[in]: src_ls_id
  // @param[in]: dest_ls_id
  // @param[in]: finish_scn
  // @param[in]: observer connection
  int do_tx_finish_transfer_out_(const share::ObTransferTaskID &task_id, const uint64_t tenant_id,
      const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id, const share::SCN &finish_scn,
      const common::ObArray<share::ObTransferTabletInfo> &tablet_list, observer::ObInnerSQLConnection *conn);

  // start a transaction
  // @param[in]: tenant
  // @param[in]: mysql transaction
  // @param[in]: timeout ctx
  int start_trans_(const uint64_t tenant_id, ObMySQLTransaction &trans, ObTimeoutCtx &timeout_ctx);

  // commit a transaction
  int commit_trans_(const bool is_commit, ObMySQLTransaction &trans);

  int get_transfer_quorum_(const ObMemberList &member_list, int64_t &quorum);

  // lock transfer task while doing
  // @param[in]: task_id
  // @param[in]: trans
  int select_transfer_task_for_update_(const share::ObTransferTaskID &task_id, ObMySQLTransaction &trans);

  int record_server_event_(
      const int32_t result,
      const bool is_ready,
      const int64_t round) const;
  int write_server_event_(
      const int32_t result,
      const ObSqlString &extra_info,
      const share::ObTransferStatus &status) const;

private:
  static const int64_t DEFAULT_WAIT_INTERVAL_US = 10 * 1000;        // 10ms

private:
  bool is_inited_;
  share::ObTransferTaskID task_id_;
  uint64_t tenant_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  mutable lib::ObMutex mutex_;
  common::ObThreadCond cond_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObTxFinishTransfer);
};

}  // namespace storage
}  // namespace oceanbase

#endif
