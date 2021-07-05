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
#define private public

#undef private
#undef protected
#include <gmock/gmock.h>
#define private public
#define protected public
#include "common/ob_i_rs_cb.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_server_locality_cache.h"
#include "common/storage/ob_freeze_define.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_log_rpc_proxy.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_partition_service_rpc.h"
#include "storage/blocksstable/ob_local_file_system.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_storage.h"
#include "sql/ob_end_trans_callback.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {

class MockObIPartitionService : public ObPartitionService {
public:
  MOCK_METHOD0(reload_config, int());
  MOCK_METHOD1(on_leader_revoke, int(const common::ObPartitionKey& partition_key));
  MOCK_METHOD1(on_leader_takeover, int(const common::ObPartitionKey& partition_key));
  MOCK_METHOD1(on_leader_active, int(const common::ObPartitionKey& partition_key));
  MOCK_METHOD1(internal_leader_revoke, int(const ObCbTask& revoke_task));
  MOCK_METHOD1(internal_leader_takeover, int(const ObCbTask& takeover_task));
  MOCK_METHOD1(internal_leader_active, int(const ObCbTask& active_task));
  MOCK_METHOD2(async_leader_revoke, int(const common::ObPartitionKey& pkey, const uint32_t revoke_type));
  MOCK_METHOD7(on_member_change_success,
      int(const common::ObPartitionKey& partition_key, const uint64_t ms_log_id, const int64_t mc_timestamp,
          const int64_t replica_num, const common::ObMemberList& prev_member_list,
          const common::ObMemberList& curr_member_list, const common::ObProposalID& ms_proposal_id));
  MOCK_CONST_METHOD1(is_take_over_done, bool(const common::ObPartitionKey& partition_key));
  MOCK_CONST_METHOD1(is_revoke_done, bool(const common::ObPartitionKey& partition_key));
  MOCK_METHOD1(is_tenant_active, bool(const common::ObPartitionKey& partition_key));
  MOCK_METHOD3(
      get_leader_from_loc_cache, int(const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew));
  MOCK_METHOD2(handle_log_missing, int(const common::ObPartitionKey& pkey, const common::ObAddr& server));
  MOCK_METHOD2(get_last_ssstore_version, int(const common::ObPartitionKey& pkey, common::ObVersion& version));
  MOCK_METHOD2(check_partition_index_available, int(const common::ObPartitionKey& pkey, bool& available));
  MOCK_METHOD1(push_callback_task, int(const ObCbTask& task));
  MOCK_METHOD1(activate_tenant, int(const uint64_t tenant_id));
  MOCK_METHOD1(inactivate_tenant, int(const uint64_t tenant_id));

  MOCK_CONST_METHOD2(get_server_locality_array,
      int(common::ObIArray<share::ObServerLocality>& server_locality_array, bool& has_readonly_zone));
  MOCK_METHOD0(force_refresh_locality_info, int());

  MOCK_METHOD8(init,
      int(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr, ObIPartitionComponentFactory* cp_fty,
          share::schema::ObMultiVersionSchemaService* schema_service, share::ObIPartitionLocationCache* location_cache,
          share::ObRsMgr* rs_mgr, ObIPartitionReport* rs_cb, rpc::frame::ObReqTransport* req_transport));

  MOCK_METHOD2(create_batch_partition_groups,
      int(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res));
  MOCK_METHOD2(create_batch_pg_partitions,
      int(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res));

  MOCK_METHOD0(start, int());
  MOCK_METHOD0(stop, int());
  MOCK_METHOD0(wait, int());
  MOCK_METHOD0(destroy, int());
  MOCK_METHOD3(load_partition, int(const char* buf, const int64_t buf_len, int64_t& pos));
  MOCK_METHOD4(replay, int(const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len));
  MOCK_METHOD4(parse, int(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream));
  MOCK_METHOD4(create_partition, int(const common::ObPartitionKey& key, const common::ObVersion data_version,
                                     const int64_t replica_num, const common::ObMemberList& mem_list));
  MOCK_METHOD9(create_partition,
      int(const common::ObPartitionKey& key, const int64_t schema_version, const common::ObVersion data_version,
          const int64_t replica_num, const common::ObMemberList& mem_list, const common::ObAddr& leader,
          const int64_t lease_start, const int64_t last_submit_timestamp,
          const ObIArray<share::schema::ObTableSchema>& schemas));
  MOCK_METHOD3(add_partition, int(const common::ObPartitionKey& key, const common::ObVersion data_version,
                                  const common::ObBaseStorageInfo& info));
  MOCK_METHOD5(
      migrate_partition, int(const common::ObPartitionKey& key, const common::ObMember& src,
                             const common::ObMember& dst, const common::ObMember& replace, const bool keep_src));
  MOCK_METHOD2(migrate_partition_callback, int(const obrpc::ObMigrateArg& arg, const int result));
  MOCK_METHOD1(remove_partition, int(const common::ObPartitionKey& key));
  MOCK_METHOD2(remove_partition_from_pg, int(const ObPartitionKey& pg_key, const ObPartitionKey& pkey));
  MOCK_METHOD3(online_partition,
      int(const common::ObPartitionKey& key, const common::ObVersion& mem_version, const int64_t publish_version));
  MOCK_METHOD5(copy_local_index,
      int(const common::ObPartitionKey& pkey, const uint64_t index_id, const common::ObReplicaMember& dst,
          const common::ObReplicaMember& data_src, const share::ObTaskId& task_id));
  MOCK_METHOD0(remove_orphans, int());
  MOCK_METHOD0(freeze, int());
  MOCK_CONST_METHOD0(get_min_using_file_id, int64_t());
  MOCK_METHOD2(kill_query_session, int(const transaction::ObTransDesc& trans_desc, const int status));
  MOCK_METHOD7(
      start_trans, int(const uint64_t tenant_id, const uint64_t thread_id, const transaction::ObStartTransParam& req,
                       const int64_t expired_time, const uint32_t session_id, uint64_t proxy_session_id,
                       transaction::ObTransDesc& trans_desc));
  MOCK_METHOD4(end_trans, int(bool is_rollback, transaction::ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
                              const int64_t stmt_expired_time));
  MOCK_CONST_METHOD2(get_curr_member_list, int(const common::ObPartitionKey& pkey, common::ObMemberList& member_list));
  MOCK_METHOD5(start_stmt,
      int(const uint64_t tenant_id, const transaction::ObStmtDesc& stmt_desc, transaction::ObTransDesc& trans_desc,
          const int64_t expired_time, const common::ObPartitionArray& participants));
  MOCK_METHOD5(start_stmt, int(const transaction::ObStmtParam& stmt_param, const transaction::ObStmtDesc& stmt_desc,
                               transaction::ObTransDesc& trans_desc, const common::ObPartitionLeaderArray& pla,
                               common::ObPartitionArray& participants));
  MOCK_METHOD5(end_stmt, int(bool is_rollback, const ObPartitionArray& cur_stmt_all_participants,
                             const transaction::ObPartitionEpochArray& epoch_arr,
                             const ObPartitionArray& discard_participants, transaction::ObTransDesc& trans_desc));
  MOCK_METHOD3(
      start_participant, int(transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants,
                             transaction::ObPartitionEpochArray& partition_epoch_arr));
  MOCK_METHOD3(end_participant,
      int(bool is_rollback, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants));
  MOCK_METHOD2(do_warm_up_request, int(const oceanbase::obrpc::ObWarmUpRequestArg&, const int64_t));
  MOCK_METHOD1(do_partition_loop_work, int(ObIPartitionGroup& partition));
  MOCK_METHOD2(table_scan, int(ObVTableScanParam& param, common::ObNewRowIterator*& result));
  MOCK_METHOD3(join_mv_scan, int(storage::ObTableScanParam& left_param, storage::ObTableScanParam& right_param,
                                 common::ObNewRowIterator*& result));
  MOCK_METHOD1(revert_scan_iter, int(common::ObNewRowIterator* iter));
  MOCK_METHOD6(delete_rows, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                                const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                                common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD5(delete_row, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                               const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                               const common::ObNewRow& row));
  MOCK_METHOD6(put_rows, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                             const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                             common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD6(insert_rows, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                                const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                                common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD9(insert_row, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                               const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                               const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row,
                               ObInsertFlag flag, int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows));
  MOCK_METHOD5(insert_row, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                               const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                               const common::ObNewRow& row));
  MOCK_METHOD2(revert_insert_iter, int(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter));
  MOCK_METHOD7(update_rows, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                                const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                                const common::ObIArray<uint64_t>& updated_column_ids,
                                common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD7(update_row, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                               const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
                               const common::ObIArray<uint64_t>& updated_column_ids, const common::ObNewRow& old_row,
                               const common::ObNewRow& new_row));

  MOCK_METHOD7(lock_rows, int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
                              const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey,
                              common::ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows));
  MOCK_METHOD6(lock_rows,
      int(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
          const common::ObPartitionKey& pkey, const common::ObNewRow& row, const ObLockFlag lock_flag));
  MOCK_METHOD1(get_all_partitions, int(ObIPartitionArrayGuard& partitions));
  MOCK_CONST_METHOD2(get_partition, int(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard*& partition));
  MOCK_METHOD2(get_partitions_by_file_key, int(const ObTenantFileKey& file_key, ObIPartitionArrayGuard& partitions));
  MOCK_CONST_METHOD1(get_partition_count, int(int64_t& partition_count));

  virtual int get_pg_key(const ObPartitionKey& pkey, ObPGKey& pg_key)
  {
    pg_key = pkey;
    return common::OB_SUCCESS;
  }

  virtual int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    int ret = common::OB_SUCCESS;
    ObStorageFileHandle file_handle;
    ObPartitionGroup& pg = const_cast<ObPartitionGroup&>(mock_pg_);
    guard.set_partition_group(this->get_pg_mgr(), pg);
    ObStorageFile* tmp_file = guard.get_partition_group()->get_pg_storage().get_storage_file();

    if (nullptr != tmp_file) {
      // do nothing
      _OB_LOG(INFO, "get partition exist tmp file=%p", tmp_file);
    } else if (OB_FAIL(OB_SERVER_FILE_MGR.alloc_file(pkey.get_tenant_id(), false, file_handle))) {
      _OB_LOG(WARN, "fail to alloc pg file");
    } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().set_storage_file(file_handle))) {
      _OB_LOG(WARN, "fail to set storage file");
    } else {
      _OB_LOG(INFO, "set storage file=%p", guard.get_partition_group()->get_pg_storage().get_storage_file());
    }
    return ret;
  }

  void clear()
  {
    mock_pg_.get_pg_storage().destroy();
  }

  virtual int get_all_partition_status(int64_t& inactive_num, int64_t& total_num) const
  {
    inactive_num = 0;
    total_num = 0;
    return common::OB_SUCCESS;
  }
  virtual int get_replica_type(const common::ObPartitionKey& pkey, ObReplicaType& type) const
  {
    UNUSED(pkey);
    type = REPLICA_TYPE_FULL;
    return common::OB_SUCCESS;
  }
  MOCK_CONST_METHOD1(revert_replay_status, void(ObReplayStatus* replay_status));
  // MOCK_METHOD0(alloc_partition_iter, ObIPartitionGroupIterator * ());
  virtual ObIPartitionGroupIterator* alloc_pg_iter()
  {
    ObPartitionGroupIterator* part_iter = new ObPartitionGroupIterator();
    part_iter->set_pg_mgr(const_cast<ObPGMgr&>(this->get_pg_mgr()));
    return part_iter;
  }
  // MOCK_METHOD1(revert_partition_iter, void(ObIPartitionGroupIterator *iter));
  virtual ObPGPartitionIterator* alloc_pg_partition_iter()
  {
    ObPGPartitionIterator* part_iter = new ObPGPartitionIterator();
    part_iter->set_pg_mgr(const_cast<ObPGMgr&>(this->get_pg_mgr()));
    return part_iter;
  }

  virtual void revert_pg_partition_iter(ObIPGPartitionIterator* iter)
  {
    delete iter;
  }
  MOCK_CONST_METHOD0(is_empty, bool());
  MOCK_METHOD7(replay_redo_log, int(const common::ObPartitionKey& pkey, const ObStoreCtx& ctx, const int64_t ts,
                                    const int64_t log_id, const char* buf, const int64_t size, bool& replayed));
  MOCK_METHOD0(get_trans_service, transaction::ObTransService*());
  MOCK_METHOD0(get_clog_mgr, clog::ObICLogMgr*());
  MOCK_METHOD0(get_election_mgr, election::ObIElectionMgr*());
  MOCK_METHOD2(sync_frozen_status, int(const ObFrozenStatus& frozen_status, const bool& force));
  MOCK_METHOD3(replay, int(const ObPartitionKey& partition, const char* log, const int64_t size));
  MOCK_METHOD1(minor_freeze, int(const uint64_t tenant_id));
  MOCK_METHOD1(minor_freeze, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD3(minor_freeze,
      int(const common::ObPartitionKey& pkey, const common::ObVersion& frozen_version, const int64_t frozen_timestamp));
  MOCK_METHOD1(submit_freeze_log_success, int(const ObPartitionKey& pkey));
  MOCK_METHOD1(submit_freeze_log_finished, int(const ObPartitionKey& pkey));
  MOCK_METHOD1(is_freeze_replay_finished, bool(const ObPartitionKey& pkey));
  MOCK_CONST_METHOD2(get_role, int(const common::ObPartitionKey& pkey, common::ObRole& role));
  MOCK_CONST_METHOD2(get_role_for_partition_table, int(const common::ObPartitionKey& pkey, common::ObRole& role));
  MOCK_CONST_METHOD2(
      get_leader_curr_member_list, int(const common::ObPartitionKey& pkey, common::ObMemberList& member_list));
  MOCK_CONST_METHOD7(get_curr_leader_and_memberlist,
      int(const common::ObPartitionKey& pkey, oceanbase::common::ObAddr& leader, oceanbase::common::ObRole& role,
          common::ObMemberList& member_list, common::ObChildReplicaList& children_list,
          common::ObReplicaType& replica_type, common::ObReplicaProperty& property));
  MOCK_CONST_METHOD2(
      get_dst_leader_candidate, int(const common::ObPartitionKey& pkey, common::ObMemberList& member_list));
  MOCK_CONST_METHOD2(get_leader, int(const common::ObPartitionKey& pkey, common::ObAddr& addr));
  MOCK_METHOD2(change_leader, int(const common::ObPartitionKey& pkey, const common::ObAddr& leader));
  MOCK_CONST_METHOD1(is_partition_exist, bool(const ObPartitionKey& pkey));
  MOCK_METHOD0(is_scan_disk_finished, bool());
  MOCK_METHOD0(is_inner_table_scan_finish, bool());
  MOCK_METHOD2(add_temporary_replica, int(const common::ObPartitionKey& key, const common::ObMember& dst));
  MOCK_METHOD3(is_member_change_done, int(const oceanbase::common::ObPartitionKey& key, uint64_t log_id, int64_t ts));
  MOCK_METHOD2(remove_replica, int(const common::ObPartitionKey& key, const common::ObReplicaMember& dst));
  MOCK_METHOD2(remove_replica_mc, int(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& info));
  MOCK_METHOD2(
      batch_remove_replica_mc, int(const obrpc::ObMemberChangeBatchArg& arg, obrpc::ObMemberChangeBatchResult& result));
  MOCK_METHOD2(change_quorum_mc, int(const obrpc::ObModifyQuorumArg& arg, obrpc::ObMCLogRpcInfo& info));
  MOCK_METHOD5(
      add_replica, int(const common::ObPartitionKey& key, const common::ObReplicaMember& dst,
                       const common::ObReplicaMember& data_src, const int64_t quorum, const share::ObTaskId& task_id));
  MOCK_METHOD6(migrate_replica,
      int(const common::ObPartitionKey& key, const common::ObReplicaMember& src, const common::ObReplicaMember& dst,
          const common::ObReplicaMember& data_src, const int64_t quorum, const share::ObTaskId& task_id));
  MOCK_METHOD4(rebuild_replica, int(const common::ObPartitionKey& key, const common::ObReplicaMember& dst,
                                    const common::ObReplicaMember& data_src, const share::ObTaskId& task_id));
  MOCK_METHOD5(change_replica,
      int(const common::ObPartitionKey& key, const common::ObReplicaMember& dst,
          const common::ObReplicaMember& data_src, const int64_t quorum, const share::ObTaskId& task_id));
  MOCK_METHOD2(add_replica_mc, int(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& info));
  MOCK_METHOD2(get_scan_cost, int(const ObTableScanParam& param, ObPartitionEst& cost_estimate));
  MOCK_METHOD2(get_status, int(const common::ObPartitionKey& pkey, int64_t& status));
  MOCK_METHOD0(garbage_clean, void());
  MOCK_METHOD0(get_rs_rpc_proxy, obrpc::ObCommonRpcProxy&());
  MOCK_METHOD2(get_safe_publish_version, int(const common::ObPartitionKey& pkey, int64_t& publish_version));
  MOCK_METHOD1(schema_drop_partition, int(const ObCLogCallbackAsyncTask& offline_task));
  MOCK_METHOD1(migrate_replica_batch, int(const obrpc::ObMigrateReplicaBatchArg& arg));
  MOCK_METHOD1(submit_offline_partition_task, int(const common::ObPartitionKey& pkey));
  MOCK_METHOD2(get_frozen_status, int(const int64_t major_version, ObFrozenStatus& frozen_status));
  MOCK_METHOD1(insert_frozen_status, int(const ObFrozenStatus& src));
  MOCK_METHOD2(update_frozen_status, int(const ObFrozenStatus& src, const ObFrozenStatus& tgt));
  MOCK_METHOD1(delete_frozen_status, int(const int64_t major_version));
  MOCK_METHOD4(gene_all_partition_min_slave_read_ts,
      int(int64_t& outer_safe_weak_read_timestamp, int64_t& inner_safe_weak_read_timestamp,
          ObPartitionKey& outer_min_slave_read_ts_key, ObPartitionKey& inner_min_slave_read_ts_key));
  MOCK_CONST_METHOD3(check_can_start_service,
      int(bool& can_start_service, int64_t& safe_weak_read_snapshot, ObPartitionKey& min_slave_read_ts_key));
  MOCK_METHOD2(generate_weak_read_timestamp, int(const common::ObPartitionKey& pkey, int64_t& timestamp));
  MOCK_METHOD0(admin_wash_ilog_cache, int());
  MOCK_METHOD1(admin_wash_ilog_cache, int(clog::file_id_t));
  MOCK_METHOD1(set_zone_priority, void(const int64_t zone_priority));
  MOCK_METHOD1(set_region, int(const common::ObRegion& region));
  MOCK_METHOD3(append_local_sort_data,
      int(const common::ObPartitionKey& pkey, const share::ObBuildIndexAppendLocalDataParam& param,
          common::ObNewRowIterator& iter));
  MOCK_METHOD3(append_sstable, int(const common::ObPartitionKey& pkey,
                                   const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter));
  MOCK_METHOD3(check_schema_version_elapsed,
      int(const common::ObPartitionKey& pkey, const int64_t schema_version, int64_t& max_commit_version));
  MOCK_METHOD2(check_ctx_create_timestamp_elapsed, int(const common::ObPartitionKey& pkey, const int64_t ts));

public:
  ObPartitionGroup mock_pg_;
};

}  // namespace storage
}  // namespace oceanbase
#endif /* MOCK_OB_PARTITION_SERVICE_H_ */
