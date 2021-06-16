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

#ifndef MOCK_OB_PARTITION_SERVICE_H_
#define MOCK_OB_PARTITION_SERVICE_H_

#undef private
#undef protected
#include <gmock/gmock.h>
#define private public
#define protected public
#include "common/ob_i_rs_cb.h"
#include "share/ob_server_locality_cache.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_log_rpc_proxy.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_partition_service_rpc.h"

namespace oceanbase {
namespace storage {

class MockObIPartitionService : public ObPartitionService {
public:
  MOCK_METHOD1(on_leader_revoke, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD1(on_leader_takeover, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD1(on_leader_active, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD7(on_member_change_success,
      int(const common::ObPartitionKey& pkey, const uint64_t ms_log_id, const int64_t mc_timestamp,
          const int64_t replica_num, const common::ObMemberList& prev_member_list,
          const common::ObMemberList& curr_member_list, const common::ObProposalID& ms_proposal_id));
  MOCK_METHOD3(
      get_leader_from_loc_cache, int(const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew));
  MOCK_METHOD2(handle_log_missing, int(const common::ObPartitionKey& pkey, const common::ObAddr& server));
  MOCK_METHOD2(get_last_ssstore_version, int(const common::ObPartitionKey& pkey, const common::ObVersion& version));

  MOCK_METHOD9(
      init, int(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
                ObIPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
                share::ObIPartitionLocationCache* location_cache, share::ObRsMgr* rs_mgr, ObIPartitionReport* rs_cb,
                rpc::frame::ObReqTransport* req_transport, ObIPSFreezeCb* major_freeze_cb));
  MOCK_METHOD0(start, int());
  MOCK_METHOD0(stop, int());
  MOCK_METHOD0(wait, int());
  MOCK_METHOD0(destroy, int());
  MOCK_METHOD3(load_partition, int(const char* buf, const int64_t buf_len, int64_t& pos));
  MOCK_METHOD6(replay_base_storage_log, int(const int64_t log_seq_num, const int64_t checkpoint_seq_num,
                                            const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos));
  MOCK_METHOD4(create_partition, int(const common::ObPartitionKey& key, const common::ObVersion data_version,
                                     const int64_t replica_num, const common::ObMemberList& mem_list));
  MOCK_METHOD8(create_partition,
      int(const common::ObPartitionKey& key, const int64_t schema_version, const common::ObVersion data_version,
          const int64_t replica_num, const common::ObMemberList& mem_list, const common::ObAddr& leader,
          const int64_t lease_start, const int64_t last_submit_timestamp));
  MOCK_METHOD3(add_partition, int(const common::ObPartitionKey& key, const common::ObVersion data_version,
                                  const common::ObBaseStorageInfo& info));
  MOCK_METHOD5(
      migrate_partition, int(const common::ObPartitionKey& key, const common::ObMember& src,
                             const common::ObMember& dst, const common::ObMember& replace, const bool keep_src));
  MOCK_METHOD2(migrate_partition_callback, int(const obrpc::ObMigrateArg& arg, const int result));
  MOCK_METHOD4(online_partition, int(const common::ObPartitionKey& pkey, const common::ObVersion& mem_version,
                                     const common::ObReplicaType replica_type, const int64_t publish_version));
  MOCK_METHOD2(offline_partition, int(const common::ObPartitionKey& leader_key, const common::ObAddr& server));
  MOCK_METHOD1(remove_partition, int(const common::ObPartitionKey& key));
  MOCK_CONST_METHOD2(get_curr_member_list, int(const common::ObPartitionKey& pkey, common::ObMemberList& member_list));
  MOCK_METHOD0(remove_orphans, int());
  MOCK_METHOD0(freeze, int());
  MOCK_CONST_METHOD0(get_min_using_file_id, int64_t());
  MOCK_METHOD6(start_trans,
      int(const uint64_t tenant_id, const transaction::ObStartTransParam& req, const int64_t expired_time,
          const uint32_t session_id, const uint64_t porxy_session_id, transaction::ObTransDesc& trans_desc));
  MOCK_METHOD3(end_trans, int(bool is_rollback, transaction::ObTransDesc& trans_desc, const int64_t stmt_expired_time));
  MOCK_METHOD4(end_trans, int(bool is_rollback, transaction::ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
                              const int64_t stmt_expired_time));
  MOCK_METHOD4(start_stmt, int(const ObStmtParam& stmt_param, transaction::ObTransDesc& trans_desc,
                               const common::ObPartitionArray& participants));
  MOCK_METHOD2(end_stmt, int(bool is_rollback, transaction::ObTransDesc& trans_desc));
  MOCK_METHOD2(
      start_participant, int(transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants));

  MOCK_METHOD3(end_participant,
      int(bool is_rollback, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants));
  MOCK_METHOD1(do_warm_up_request, int(const oceanbase::obrpc::ObWarmUpRequestArg&));
  MOCK_METHOD2(table_scan, int(ObVTableScanParam& param, common::ObNewRowIterator*& result));
  MOCK_METHOD2(table_scan, int(ObVTableScanParam& param, common::ObNewIterIterator*& result));
  MOCK_METHOD3(join_mv_scan, int(storage::ObTableScanParam& left_param, storage::ObTableScanParam& right_param,
                                 common::ObNewRowIterator*& result));
  MOCK_METHOD1(revert_scan_iter, int(common::ObNewRowIterator* iter));
  MOCK_METHOD1(revert_scan_iter, int(common::ObNewIterIterator* iter));
  MOCK_METHOD6(delete_rows,
      int(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const common::ObPartitionKey& pkey,
          const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD6(insert_rows,
      int(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const common::ObPartitionKey& pkey,
          const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD9(
      insert_row, int(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
                      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                      const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row,
                      const ObInsertFlag flag, int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows));
  MOCK_METHOD2(revert_insert_iter, int(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter));
  MOCK_METHOD7(update_rows,
      int(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const common::ObPartitionKey& pkey,
          const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
          common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD6(lock_rows,
      int(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const common::ObPartitionKey& pkey,
          common::ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows));
  MOCK_METHOD5(
      lock_rows, int(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
                     const common::ObPartitionKey& pkey, const common::ObNewRow& row, const ObLockFlag lock_flag));
  MOCK_METHOD1(get_all_partitions, int(ObIPartitionArrayGuard& partitions));
  MOCK_METHOD2(get_partition, int(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition));
  MOCK_METHOD1(get_partition_count, int(int64_t& partition_count));
  MOCK_METHOD0(alloc_pg_iter, ObIPartitionIterator*());
  // MOCK_METHOD1(revert_pg_iter, void(ObIPartitionIterator *iter));
  MOCK_METHOD0(alloc_pg_partition_iter, ObPGPartitionIterator*());
  MOCK_METHOD1(revert_pg_partition_iter, void(ObPGPartitionIterator* iter));
  MOCK_CONST_METHOD0(is_empty, bool());
  MOCK_METHOD6(replay_redo_log, int(const common::ObPartitionKey& pkey, const ObStoreCtx& ctx, const int64_t ts,
                                    const int64_t log_id, const char* buf, const int64_t size));
  MOCK_METHOD1(push_callback_task, int(const ObCbTask& task));
  MOCK_METHOD1(internal_leader_revoke, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD1(internal_leader_takeover, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD1(internal_leader_active, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD2(async_leader_revoke, int(const common::ObPartitionKey& pkey, const uint32_t revoke_type));
  MOCK_METHOD0(get_trans_service, transaction::ObTransService*());
  MOCK_METHOD0(get_clog_mgr, clog::ObICLogMgrMOCK_METHOD0(get_election_mgr, election::ObIElectionMgr*());
  MOCK_METHOD5(minor_freeze,
      int(const int64_t schema_version, const obrpc::ObPartitionList &partitions,
          obrpc::ObPartitionList &succ_partitions, obrpc::ObPartitionList &fail_partitions,
          obrpc::ObPartitionList &unkown_partitions));
  MOCK_METHOD4(prepare_freeze,
      int(const obrpc::ObPartitionList &partitions, const int64_t frozen_version,
          const common::ObIArray<int64_t> &timestamps, ObIPSFreezeCb &freeze_cb));
  MOCK_METHOD3(commit_freeze,
      int(const obrpc::ObPartitionList &partitions, const int64_t frozen_version,
          const common::ObIArray<int64_t> &timestamps));
  MOCK_METHOD3(abort_freeze,
      int(const obrpc::ObPartitionList &partitions, const int64_t frozen_version,
          const common::ObIArray<int64_t> &timestamps));
  MOCK_METHOD3(set_freeze_status,
      int(const common::ObPartitionKey &pkey, const int64_t frozen_version,
          const int64_t major_freeze_status));
  MOCK_METHOD3(get_freeze_status,
      int(const common::ObPartitionKey &pkey, int64_t &frozen_version,
          int64_t &major_freeze_status));
  MOCK_METHOD4(get_freeze_status,
      int(const common::ObPartitionKey &pkey, int64_t &frozen_version,
          int64_t &frozen_timestamp, int64_t &major_freeze_status));
  MOCK_METHOD3(replay,
      int(const ObPartitionKey &partition, const char *log, const int64_t size));
  MOCK_METHOD1(finish_replay,
      int(const ObPartitionKey &partition));
  MOCK_METHOD2(leader_freeze_success,
      int(const ObPartitionKey &pkey, const int64_t freeze_cmd));
  MOCK_METHOD2(leader_freeze_fail,
      int(const ObPartitionKey &pkey, const int64_t freeze_cmd));
  MOCK_METHOD2(follower_freeze_success,
      int(const ObPartitionKey &pkey, const int64_t freeze_cmd));
  MOCK_METHOD2(follower_freeze_fail,
      int(const ObPartitionKey &pkey, const int64_t freeze_cmd));
  MOCK_METHOD2(submit_freeze_log_success,
      int(const int64_t cmd_type, const ObPartitionKey &pkey));
  MOCK_METHOD2(submit_freeze_log_finished,
      int(const int64_t cmd_type, const ObPartitionKey &pkey));
  MOCK_CONST_METHOD2(get_role,
      int(const common::ObPartitionKey &pkey, common::ObRole &role));
  MOCK_CONST_METHOD2(get_role_for_partition_table,
      int(const common::ObPartitionKey &pkey, common::ObRole &role));
  MOCK_CONST_METHOD2(get_leader_curr_member_list,
      int(const common::ObPartitionKey &pkey, common::ObMemberList &member_list));
  MOCK_METHOD2(change_leader,
      int(const common::ObPartitionKey &pkey, const common::ObAddr &leader));
  MOCK_CONST_METHOD1(is_partition_exist, bool(const common::ObPartitionKey &pkey));
  MOCK_METHOD0(is_scan_disk_finished, bool());
  MOCK_METHOD0(garbage_clean, void());
  MOCK_METHOD2(migrate_info_fetch,
      int(const common::ObPartitionKey &pkey, obrpc::ObMigrateInfoFetchResult &info_result));
  MOCK_METHOD2(get_scan_cost, int(const ObTableScanParam &param, ObPartitionEst &cost_estimate));
  MOCK_METHOD2(get_status, int(const common::ObPartitionKey &pkey, int64_t &status));
  MOCK_METHOD1(activate_tenant, int (const uint64_t tenant_id));
  MOCK_METHOD1(inactivate_tenant, int (const uint64_t tenant_id));
  MOCK_CONST_METHOD2(get_server_locality_array, int (common::ObIArray<share::ObServerLocality> &server_locality_array, bool &has_readonly_zone));
  MOCK_METHOD0(force_refresh_locality_info, int());
  MOCK_METHOD1(schema_drop_partition, int(ObIPartitionGroup *partition));

  MOCK_METHOD0(get_base_storage, ObBaseStorage *());
  MOCK_METHOD3(append_local_sort_data, int(
        const common::ObPartitionKey &pkey,
        const share::ObBuildIndexAppendLocalDataParam &param,
        common::ObNewRowIterator &iter));
  MOCK_METHOD3(append_sstable, int(
        const common::ObPartitionKey &pkey,
        const share::ObBuildIndexAppendSSTableParam &param,
        common::ObNewRowIterator &iter));
};

class MockObIPartitionArrayGuard : public ObIPartitionArrayGuard {
public:
  MOCK_METHOD1(push_back, int(ObIPartitionGroup* partition));
  MOCK_METHOD1(at, ObIPartitionGroup*(int64_t i));
  MOCK_METHOD0(count, int64_t);
  MOCK_METHOD0(reuse, int64_t);
};

}  // namespace storage
}  // namespace oceanbase

namespace oceanbase {
namespace storage {

class MockObIPartitionArrayGuard : public ObIPartitionArrayGuard {
public:
};

}  // namespace storage
}  // namespace oceanbase

#endif
