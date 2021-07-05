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

#ifndef OCEANBASE_UNITTEST_ENGINE_TABLE_OB_FAKE_PARTITION_SERVICE_
#define OCEANBASE_UNITTEST_ENGINE_TABLE_OB_FAKE_PARTITION_SERVICE_

#include "storage/ob_partition_service.h"
#include "storage/ob_partition_service.h"
#include "share/ob_scanner.h"
#include "share/ob_partition_modify.h"
#include "common/row/ob_row.h"

namespace oceanbase {

namespace sql {
class ObFakeTableScanIterator : public common::ObNewRowIterator {
public:
  ObFakeTableScanIterator() : col_num_(0)
  {}
  virtual ~ObFakeTableScanIterator()
  {}

  virtual int get_next_row(common::ObNewRow*& row);

  inline virtual void reset()
  {
    row_store_it_.reset();
  }

  inline void init(const common::ObRowStore::Iterator& iter, int64_t col_num)
  {
    row_store_it_ = iter;
    col_num_ = col_num;
  }

private:
  common::ObRowStore::Iterator row_store_it_;
  int64_t col_num_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFakeTableScanIterator);
};
}  // namespace sql

namespace storage {
class ObFakePartitionServiceForSQL : public oceanbase::storage::ObPartitionService {
public:
  ObFakePartitionServiceForSQL();
  virtual ~ObFakePartitionServiceForSQL();
  virtual int init(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
      ObIPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
      share::ObIPartitionLocationCache* location_cache, common::ObIRSCb* rs_cb,
      rpc::frame::ObReqTransport* req_transport);
  virtual int start();
  virtual int destroy();
  virtual void set_component_service(ObIPartitionComponentFactory& cp_fty);
  virtual int load_partition(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos);
  virtual int create_partition(const common::ObPartitionKey& key, const common::ObVersion data_version,
      const int64_t replica_num, const common::ObMemberList& mem_list);
  virtual int online_partition();
  virtual int online_partition(const common::ObPartitionKey& pkey, const int64_t publish_version)
  {
    UNUSED(pkey);
    UNUSED(publish_version);
    return OB_SUCCESS;
  }
  virtual int remove_partition(const common::ObPartitionKey& key);
  virtual int remove_orphans();
  virtual int freeze();
  virtual int on_leader_takeover(const common::ObPartitionKey& partition_key);
  virtual int64_t get_min_using_file_id() const;
  virtual share::schema::ObMultiVersionSchemaService* get_schema_service();
  virtual ObIPartitionComponentFactory* get_cp_fty();
  virtual int start_trans(
      const uint64_t tenant_id, const transaction::ObStartTransParam& req, transaction::ObTransDesc& trans_desc);
  virtual int end_trans(bool is_rollback, const transaction::ObTransDesc& trans_desc);
  virtual int start_stmt(
      const uint64_t tenant_id, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants);
  virtual int end_stmt(bool is_rollback, const transaction::ObTransDesc& trans_desc);
  virtual int table_scan(ObTableScanParam& param, common::ObNewRowIterator*& result);
  virtual int revert_scan_iter(common::ObNewRowIterator* iter);
  virtual int delete_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int insert_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int update_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int lock_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
      const common::ObPartitionKey& pkey, common::ObNewRowIterator* row_iter, const ObLockFlag lock_flag,
      int64_t& affected_rows);
  virtual int fetch_conflict_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& in_column_ids,
      const common::ObIArray<uint64_t>& out_column_ids, common::ObNewRowIterator& check_row_iter,
      common::ObIArray<common::ObNewRowIterator*>& dup_row_iters);
  virtual int get_all_partitions(common::ObIArray<ObIPartitionGroup*>& partition_list);
  virtual int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition);
  virtual int get_partition_count(int64_t& partition_count) const;
  virtual bool is_empty() const;
  virtual int replay_redo_log(
      const common::ObPartitionKey& pkey, const ObStoreCtx& ctx, const int64_t ts, const char* buf, const int64_t size);
  virtual transaction::ObTransService* get_trans_service();
  virtual clog::ObICLogMgr* get_clog_mgr();
  virtual int prepare_major_freeze(
      const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp);
  virtual int commit_major_freeze(
      const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp);
  virtual int abort_major_freeze(
      const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp);
  virtual int set_major_freeze_status(
      const common::ObPartitionKey& pkey, const int64_t frozen_version, const int64_t major_freeze_status);
  virtual int get_major_freeze_status(
      const common::ObPartitionKey& pkey, int64_t& frozen_version, int64_t& major_freeze_status);
  virtual int replay(const ObPartitionKey& partition, const char* log, const int64_t size);
  virtual int finish_replay(const ObPartitionKey& partition);
  virtual int leader_freeze_success(const ObPartitionKey& pkey, const int64_t freeze_cmd);
  virtual int leader_freeze_fail(const ObPartitionKey& pkey, const int64_t freeze_cmd);
  virtual int follower_freeze_success(const ObPartitionKey& pkey, const int64_t freeze_cmd);
  virtual int follower_freeze_fail(const ObPartitionKey& pkey, const int64_t freeze_cmd);
  virtual int submit_freeze_log_success(const int64_t cmd_type, const ObPartitionKey& pkey);
  virtual int get_role(const common::ObPartitionKey& pkey, common::ObRole& role) const;
  virtual int get_leader_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const;
  virtual int get_active_memtable_version(const ObPartitionKey& pkey, int64_t& version);
  virtual int activate_tenant(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
  virtual int inactivate_tenant(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
  inline int add_row(const common::ObNewRow& row)
  {
    return scanner_.add_row(row);
  }
  inline void set_col_num(int64_t col_num)
  {
    col_num_ = col_num;
  }
  virtual int migrate_replica_batch(const obrpc::ObMigrateReplicaBatchArg& arg)
  {
    UNUSED(arg);
    return OB_ERROR;
  }
  //========================For ObIDataAccessService==============================
  virtual int table_scan(ObVTableScanParam& param, ObNewRowIterator*& result)
  {
    UNUSED(param);
    UNUSED(result);
    return OB_SUCCESS;
  }

  virtual int join_mv_scan(
      storage::ObTableScanParam& left_param, storage::ObTableScanParam& right_param, common::ObNewRowIterator*& result)
  {
    UNUSED(left_param);
    UNUSED(right_param);
    UNUSED(result);
    return OB_SUCCESS;
  }

  virtual int get_checksum_method(
      const common::ObPartitionKey& pkey, const int64_t snapshot_version, int64_t& checksum_method) const
  {
    UNUSED(pkey);
    UNUSED(snapshot_version);
    UNUSED(checksum_method);
    return OB_SUCCESS;
  }

  //========================For ObPartitionService==============================
  // replay the redo log.
  virtual int replay(const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len)
  {
    UNUSED(log_seq_num);
    UNUSED(subcmd);
    UNUSED(buf);
    UNUSED(len);
    return OB_SUCCESS;
  }
  // parse the redo log to stream
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
  {
    UNUSED(subcmd);
    UNUSED(buf);
    UNUSED(len);
    UNUSED(stream);
    return OB_SUCCESS;
  }
  //====================================For ObIPSCb===============================
  virtual int on_leader_revoke(const common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int on_leader_takeover(const common::ObPartitionKey& pkey, const bool is_elected_by_changing_leader)
  {
    UNUSED(pkey);
    UNUSED(is_elected_by_changing_leader);
    return OB_SUCCESS;
  }
  virtual int on_leader_active(const common::ObPartitionKey& pkey, const bool is_elected_by_changing_leader)
  {
    UNUSED(pkey);
    UNUSED(is_elected_by_changing_leader);
    return OB_SUCCESS;
  }
  virtual int on_member_change_success(const common::ObPartitionKey& pkey, const int64_t mc_timestamp,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list)
  {
    UNUSED(pkey);
    UNUSED(mc_timestamp);
    UNUSED(prev_member_list);
    UNUSED(curr_member_list);
    return OB_SUCCESS;
  }
  virtual bool is_take_over_done(const common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual bool is_revoke_done(const common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual bool is_tenant_active(const common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual bool is_tenant_active(const uint64_t tenant_id) const
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
  virtual int get_leader_from_loc_cache_across_cluster(
      const common::ObPartitionKey& pkey, common::ObAddr& leader, int64_t& cluster_id, const bool is_need_renew = false)
  {
    UNUSED(pkey);
    UNUSED(leader);
    UNUSED(is_need_renew);
    UNUSED(cluster_id);
    return OB_SUCCESS;
  }
  virtual int get_leader_from_loc_cache(
      const common::ObPartitionKey& pkey, common::ObAddr& leader, const bool is_need_renew = false)
  {
    UNUSED(pkey);
    UNUSED(leader);
    UNUSED(is_need_renew);
    return OB_SUCCESS;
  }
  virtual int handle_log_missing(const common::ObPartitionKey& pkey, const common::ObAddr& server)
  {
    UNUSED(pkey);
    UNUSED(server);
    return OB_SUCCESS;
  }
  virtual int get_server_region(const common::ObAddr& server, common::ObRegion& region) const
  {
    UNUSED(server);
    UNUSED(region);
    return OB_SUCCESS;
  }

  //====================================For ObIPSCb===============================
  virtual int stop()
  {
    return OB_SUCCESS;
  }
  virtual int wait()
  {
    return OB_SUCCESS;
  }
  virtual int get_safe_publish_version(const common::ObPartitionKey& pkey, int64_t& publish_version)
  {
    UNUSED(pkey);
    UNUSED(publish_version);
    return OB_SUCCESS;
  }

  virtual bool is_election_candidate(const common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  virtual int create_batch_partition_groups(
      const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res)
  {
    UNUSED(batch_arg);
    UNUSED(batch_res);
    return OB_SUCCESS;
  }
  virtual int create_batch_pg_partitions(
      const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res)
  {
    UNUSED(batch_arg);
    UNUSED(batch_res);
    return OB_SUCCESS;
  }
  virtual int remove_partition_from_pg(const ObPartitionKey& pg_key, const ObPartitionKey& pkey)
  {
    UNUSED(pg_key);
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int online_partition(
      const common::ObPartitionKey& pkey, const common::ObVersion& mem_version, const int64_t publish_version)
  {
    UNUSED(pkey);
    UNUSED(mem_version);
    UNUSED(publish_version);
    return OB_SUCCESS;
  }
  virtual obrpc::ObCommonRpcProxy& get_rs_rpc_proxy()
  {
    return *rs_rpc_proxy_;
  }
  virtual ObIPartitionServiceRpc& get_pts_rpc()
  {
    return *pts_rpc_;
  }
  virtual int reload_config()
  {
    return OB_SUCCESS;
  }
  virtual int check_schema_version_elapsed(
      const ObPartitionKey& partition, const int64_t schema_version, int64_t& max_commit_version)
  {
    UNUSED(partition);
    UNUSED(schema_version);
    UNUSED(max_commit_version);
    return OB_SUCCESS;
  }
  virtual int check_ctx_create_timestamp_elapsed(const common::ObPartitionKey& partition, const int64_t ts)
  {
    UNUSED(partition);
    UNUSED(ts);
    return OB_SUCCESS;
  }
  virtual int check_unique_index_valid(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id, const int64_t schema_version)
  {
    UNUSED(pkey);
    UNUSED(index_table_id);
    UNUSED(schema_version);
    return OB_SUCCESS;
  }
  virtual int check_single_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id)
  {
    UNUSED(pkey);
    UNUSED(index_table_id);
    return OB_SUCCESS;
  }
  virtual int check_all_replica_major_sstable_exist(const common::ObPartitionKey& pkey, const uint64_t index_table_id)
  {
    UNUSED(pkey);
    UNUSED(index_table_id);
    return OB_SUCCESS;
  }
  virtual int check_member_major_sstable_enough(
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& table_ids)
  {
    UNUSED(pkey);
    UNUSED(table_ids);
    return OB_SUCCESS;
  }
  virtual int start_trans(const uint64_t tenant_id, const uint64_t thread_id, const transaction::ObStartTransParam& req,
      const int64_t expired_time, const uint32_t session_id, const uint64_t proxy_session_id,
      transaction::ObTransDesc& trans_desc)
  {
    UNUSED(tenant_id);
    UNUSED(thread_id);
    UNUSED(req);
    UNUSED(expired_time);
    UNUSED(session_id);
    UNUSED(proxy_session_id);
    UNUSED(trans_desc);
    return OB_SUCCESS;
  }
  virtual int kill_query_session(const transaction::ObTransDesc& trans_desc, const int status)
  {
    UNUSED(trans_desc);
    UNUSED(status);
    return OB_SUCCESS;
  }
  virtual int get_all_partition_status(int64_t& inactive_num, int64_t& total_num)
  {
    UNUSED(inactive_num);
    UNUSED(total_num);
    return OB_SUCCESS;
  }
  virtual int end_trans(bool is_rollback, transaction::ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
      const int64_t stmt_expired_time)
  {
    UNUSED(is_rollback);
    UNUSED(trans_desc);
    UNUSED(cb);
    UNUSED(stmt_expired_time);
    return OB_SUCCESS;
  }
  virtual int start_stmt(const transaction::ObStmtParam& stmt_param, const transaction::ObStmtDesc& stmt_desc,
      transaction::ObTransDesc& trans_desc, const common::ObPartitionLeaderArray& pla,
      common::ObPartitionArray& participants)
  {
    UNUSED(stmt_param);
    UNUSED(stmt_desc);
    UNUSED(trans_desc);
    UNUSED(pla);
    UNUSED(participants);
    return OB_SUCCESS;
  }
  virtual int extend_stmt(transaction::ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla)
  {
    UNUSED(trans_desc);
    UNUSED(pla);
    return OB_SUCCESS;
  }
  virtual int end_stmt(bool is_rollback, const ObPartitionArray& cur_stmt_all_participants,
      const transaction::ObPartitionEpochArray& epoch_arr, const ObPartitionArray& discard_participants,
      const ObPartitionLeaderArray& pla, transaction::ObTransDesc& trans_desc)
  {
    UNUSED(is_rollback);
    UNUSED(cur_stmt_all_participants);
    UNUSED(epoch_arr);
    UNUSED(discard_participants);
    UNUSED(pla);
    UNUSED(trans_desc);
    return OB_SUCCESS;
  }
  virtual int start_participant(transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants,
      transaction::ObPartitionEpochArray& partition_epoch_arr,
      const transaction::ObPartitionSchemaInfoArray& partition_schema_info_arr)
  {
    UNUSED(trans_desc);
    UNUSED(participants);
    UNUSED(partition_epoch_arr);
    UNUSED(partition_schema_info_arr);
    return OB_SUCCESS;
  }
  virtual int end_participant(
      bool is_rollback, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants)
  {
    UNUSED(is_rollback);
    UNUSED(trans_desc);
    UNUSED(participants);
    return OB_SUCCESS;
  }
  virtual int delete_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_SUCCESS;
  }

  virtual int delete_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(row);
    return OB_SUCCESS;
  }

  virtual int put_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_SUCCESS;
  }

  virtual int insert_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_SUCCESS;
  }

  virtual int insert_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
      int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(duplicated_column_ids);
    UNUSED(flag);
    UNUSED(affected_rows);
    UNUSED(duplicated_rows);
    UNUSED(row);
    return OB_SUCCESS;
  }

  virtual int revert_insert_iter(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter)
  {
    UNUSED(iter);
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  virtual int update_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(updated_column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_SUCCESS;
  }
  virtual int update_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, const common::ObNewRow& old_row,
      const common::ObNewRow& new_row)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(updated_column_ids);
    UNUSED(old_row);
    UNUSED(new_row);
    return OB_SUCCESS;
  }

  virtual int lock_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey, common::ObNewRowIterator* row_iter,
      const ObLockFlag lock_flag, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(abs_lock_timeout);
    UNUSED(row_iter);
    UNUSED(lock_flag);
    UNUSED(affected_rows);
    return OB_SUCCESS;
  }
  virtual int lock_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey, const common::ObNewRow& row,
      const ObLockFlag lock_flag)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(abs_lock_timeout);
    UNUSED(lock_flag);
    UNUSED(row);
    return OB_SUCCESS;
  }

  virtual int get_all_partitions(ObIPartitionArrayGuard& partitions)
  {
    UNUSED(partitions);
    return OB_SUCCESS;
  }
  virtual int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    UNUSED(pkey);
    UNUSED(guard);
    return OB_SUCCESS;
  }
  virtual void revert_replay_status(ObReplayStatus* replay_status) const
  {
    UNUSED(replay_status);
  }
  virtual ObIPartitionGroupIterator* alloc_pg_iter()
  {
    return nullptr;
  }
  virtual void revert_pg_iter(ObIPartitionGroupIterator* iter)
  {
    UNUSED(iter);
  }
  virtual ObPGPartitionIterator* alloc_pg_partition_iter()
  {
    return nullptr;
  }
  virtual void revert_pg_partition_iter(ObPGPartitionIterator* iter)
  {
    UNUSED(iter);
  }
  // ==========================================================================
  // partition service callback function
  virtual int push_callback_task(const ObCbTask& task)
  {
    UNUSED(task);
    return OB_SUCCESS;
  }
  virtual int internal_leader_revoke(const ObCbTask& revoke_task)
  {
    UNUSED(revoke_task);
    return OB_SUCCESS;
  }
  virtual int async_leader_revoke(const common::ObPartitionKey& pkey, const uint32_t revoke_type)
  {
    UNUSED(pkey);
    UNUSED(revoke_type);
    return OB_SUCCESS;
  }
  virtual int internal_leader_takeover(const ObCbTask& takeover_task)
  {
    UNUSED(takeover_task);
    return OB_SUCCESS;
  }
  virtual int internal_leader_active(const ObCbTask& active_task)
  {
    UNUSED(active_task);
    return OB_SUCCESS;
  }
  virtual int raise_memstore(const ObCbTask& raise_task)
  {
    UNUSED(raise_task);
    return OB_SUCCESS;
  }

  // ==========================================================================
  // expose component
  virtual election::ObIElectionMgr* get_election_mgr()
  {
    return nullptr;
  }
  virtual ObPartitionSplitWorker* get_split_worker()
  {
    return nullptr;
  }

  // ==========================================================================
  // major/minor freeze
  virtual int prepare_major_freeze(
      const ObVersion& frozen_version, const int64_t frozen_timestamp, const int64_t schema_version)
  {
    UNUSED(frozen_version);
    UNUSED(frozen_timestamp);
    UNUSED(schema_version);
    return OB_SUCCESS;
  }
  virtual int commit_major_freeze(
      const ObVersion& frozen_version, const int64_t frozen_timestamp, const int64_t schema_version)
  {
    UNUSED(frozen_version);
    UNUSED(frozen_timestamp);
    UNUSED(schema_version);
    return OB_SUCCESS;
  }
  virtual int abort_major_freeze(
      const ObVersion& frozen_version, const int64_t frozen_timestamp, const int64_t schema_version)
  {
    UNUSED(frozen_version);
    UNUSED(frozen_timestamp);
    UNUSED(schema_version);
    return OB_SUCCESS;
  }
  virtual int sync_frozen_status(const ObFrozenStatus& frozen_status, const bool& force)
  {
    UNUSED(frozen_status);
    UNUSED(force);
    return OB_SUCCESS;
  }
  virtual int submit_freeze_log_success(const ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int submit_freeze_log_finished(const ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int replay(
      const ObPartitionKey& partition, const char* log, const int64_t size, const uint64_t log_id, const int64_t log_ts)
  {
    UNUSED(partition);
    UNUSED(log);
    UNUSED(size);
    UNUSED(log_id);
    UNUSED(log_ts);
    return OB_SUCCESS;
  }
  virtual int minor_freeze(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
  virtual int minor_freeze(const common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  virtual int freeze_partition(const ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  virtual int query_log_info_with_log_id(
      const ObPartitionKey& pkey, const uint64_t log_id, int64_t& accum_checksum, int64_t& submit_timestamp)
  {
    UNUSED(pkey);
    UNUSED(log_id);
    UNUSED(accum_checksum);
    UNUSED(submit_timestamp);
    return OB_SUCCESS;
  }

  virtual int is_log_sync(const common::ObPartitionKey& key, bool& is_sync, uint64_t& last_slide_log_id)
  {
    UNUSED(key);
    UNUSED(is_sync);
    UNUSED(last_slide_log_id);
    return OB_SUCCESS;
  }
  // ==========================================================================
  // misc functions

  virtual int get_curr_leader_and_memberlist(const common::ObPartitionKey& pkey, common::ObAddr& leader,
      common::ObRole& role, common::ObMemberList& member_list, ObChildReplicaList& children_list,
      ObReplicaType& replica_type) const
  {
    UNUSED(pkey);
    UNUSED(leader);
    UNUSED(role);
    UNUSED(member_list);
    UNUSED(children_list);
    UNUSED(replica_type);
    return OB_SUCCESS;
  }
  virtual int get_dst_leader_candidate(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }
  virtual int get_dst_candidates_array(const common::ObPartitionIArray& pkey_array,
      const common::ObAddrIArray& dst_server_list, common::ObSArray<common::ObAddrSArray>& candidate_list_array) const
  {
    UNUSED(pkey_array);
    UNUSED(dst_server_list);
    UNUSED(candidate_list_array);
    return OB_SUCCESS;
  }
  virtual int change_leader(const common::ObPartitionKey& pkey, const common::ObAddr& leader)
  {
    UNUSED(pkey);
    UNUSED(leader);
    return OB_SUCCESS;
  }
  virtual int get_leader(const common::ObPartitionKey& pkey, common::ObAddr& addr) const
  {
    UNUSED(pkey);
    UNUSED(addr);
    return OB_SUCCESS;
  }
  virtual bool is_partition_exist(const common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual bool is_scan_disk_finished()
  {
    return OB_SUCCESS;
  }
  virtual bool is_inner_table_scan_finish()
  {
    return OB_SUCCESS;
  }
  //  virtual void garbage_clean() = 0;
  virtual int get_server_locality_array(
      common::ObIArray<share::ObServerLocality>& server_locality_array, bool& has_readonly_zone) const
  {
    UNUSED(server_locality_array);
    UNUSED(has_readonly_zone);
    return OB_SUCCESS;
  }
  virtual int force_refresh_locality_info()
  {
    return OB_SUCCESS;
  }

  // ==========================================================================
  // partition service rpc functions
  virtual int add_replica(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst, /* new replica */
      const common::ObReplicaMember& data_src,                                                    /* data source */
      const int64_t quorum, const share::ObTaskId& task_id)
  {
    UNUSED(pkey);
    UNUSED(dst);
    UNUSED(data_src);
    UNUSED(quorum);
    UNUSED(task_id);
    return OB_SUCCESS;
  }
  virtual int rebuild_replica(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst,
      const common::ObReplicaMember& data_src /* data source */, const share::ObTaskId& task_id)
  {
    UNUSED(pkey);
    UNUSED(dst);
    UNUSED(data_src);
    UNUSED(task_id);
    return OB_SUCCESS;
  }
  virtual int copy_global_index(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst,
      const common::ObReplicaMember& data_src, const share::ObTaskId& task_id)
  {
    UNUSED(pkey);
    UNUSED(dst);
    UNUSED(data_src);
    UNUSED(task_id);
    return OB_SUCCESS;
  }
  virtual int copy_local_index(const common::ObPartitionKey& pkey, const uint64_t index_id,
      const common::ObReplicaMember& dst, const common::ObReplicaMember& data_src, const share::ObTaskId& task_id)
  {
    UNUSED(pkey);
    UNUSED(dst);
    UNUSED(data_src);
    UNUSED(index_id);
    UNUSED(task_id);
    return OB_SUCCESS;
  }
  virtual int change_replica(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst,
      const common::ObReplicaMember& data_src, /* data source */
      const int64_t quorum, const share::ObTaskId& task_id)
  {
    UNUSED(pkey);
    UNUSED(dst);
    UNUSED(data_src);
    UNUSED(quorum);
    UNUSED(task_id);
    return OB_SUCCESS;
  }
  // delete replicas not in paxos member list
  virtual int remove_replica(const common::ObPartitionKey& key, const common::ObReplicaMember& dst)
  {
    UNUSED(key);
    UNUSED(dst);
    return OB_SUCCESS;
  }
  // add member to member list (member change log) only leader can receive this rpc.
  virtual int add_replica_mc(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info)
  {
    UNUSED(arg);
    UNUSED(mc_log_info);
    return OB_SUCCESS;
  }
  // remove member from member list (member change log) only leader can receive this rpc.
  virtual int remove_replica_mc(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info)
  {
    UNUSED(arg);
    UNUSED(mc_log_info);
    return OB_SUCCESS;
  }
  virtual int change_quorum_mc(const obrpc::ObModifyQuorumArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info)
  {
    UNUSED(arg);
    UNUSED(mc_log_info);
    return OB_SUCCESS;
  }
  virtual int is_member_change_done(const common::ObPartitionKey& key, const uint64_t log_id, const int64_t timestamp)
  {
    UNUSED(key);
    UNUSED(log_id);
    UNUSED(timestamp);
    return OB_SUCCESS;
  }
  virtual int do_warm_up_request(const obrpc::ObWarmUpRequestArg& arg, const int64_t recieve_ts)
  {
    UNUSED(arg);
    UNUSED(recieve_ts);
    return OB_SUCCESS;
  }

  // ==========================================================================
  // partition misc

  virtual int schema_drop_partition(const ObCLogCallbackAsyncTask& offline_task)
  {
    UNUSED(offline_task);
    return OB_SUCCESS;
  }
  virtual int submit_offline_partition_task(const common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  // ==========================================================================
  // do not use it unless you are rootserver
  virtual int get_frozen_status(const int64_t major_version, ObFrozenStatus& frozen_status)
  {
    UNUSED(major_version);
    UNUSED(frozen_status);
    return OB_SUCCESS;
  }
  virtual int insert_frozen_status(const ObFrozenStatus& src)
  {
    UNUSED(src);
    return OB_SUCCESS;
  }
  virtual int update_frozen_status(const ObFrozenStatus& src, const ObFrozenStatus& tgt)
  {
    UNUSED(src);
    UNUSED(tgt);
    return OB_SUCCESS;
  }
  virtual int delete_frozen_status(const int64_t major_version)
  {
    UNUSED(major_version);
    return OB_SUCCESS;
  }
  virtual int generate_partition_weak_read_snapshot_version(
      ObIPartitionGroup& partition, bool& need_skip, bool& is_user_partition, int64_t& wrs_version)
  {
    UNUSED(partition);
    UNUSED(need_skip);
    UNUSED(is_user_partition);
    UNUSED(wrs_version);
    return OB_SUCCESS;
  }
  virtual int check_can_start_service(
      bool& can_start_service, int64_t& safe_weak_read_snaspthot, ObPartitionKey& min_slave_read_ts_pkey)
  {
    UNUSED(can_start_service);
    UNUSED(safe_weak_read_snaspthot);
    UNUSED(min_slave_read_ts_pkey);
    return OB_SUCCESS;
  }
  virtual int admin_wash_ilog_cache(const clog::file_id_t file_id)
  {
    UNUSED(file_id);
    return OB_SUCCESS;
  }
  virtual int admin_wash_ilog_cache()
  {
    return OB_SUCCESS;
  }
  virtual int append_local_sort_data(const common::ObPartitionKey& pkey,
      const share::ObBuildIndexAppendLocalDataParam& param, common::ObNewRowIterator& iter)
  {
    UNUSED(pkey);
    UNUSED(param);
    UNUSED(iter);
    return OB_SUCCESS;
  }
  virtual int append_sstable(const common::ObPartitionKey& pkey, const share::ObBuildIndexAppendSSTableParam& param,
      common::ObNewRowIterator& iter)
  {
    UNUSED(pkey);
    UNUSED(param);
    UNUSED(iter);
    return OB_SUCCESS;
  }
  // for splitting partition
  virtual int split_partition(
      const share::ObSplitPartition& split_info, common::ObIArray<share::ObPartitionSplitProgress>& result)
  {
    UNUSED(split_info);
    UNUSED(result);
    return OB_SUCCESS;
  }
  virtual int sync_split_source_log_success(
      const common::ObPartitionKey& pkey, const int64_t source_log_id, const int64_t source_log_ts)
  {
    UNUSED(source_log_id);
    UNUSED(source_log_ts);
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int sync_split_dest_log_success(const common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int split_dest_partition(
      const common::ObPartitionKey& pkey, const ObPartitionSplitInfo& split_info, enum share::ObSplitProgress& progress)
  {
    UNUSED(progress);
    UNUSED(split_info);
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int handle_split_dest_partition_request(
      const obrpc::ObSplitDestPartitionRequestArg& arg, obrpc::ObSplitDestPartitionResult& result)
  {
    UNUSED(result);
    UNUSED(arg);
    return OB_SUCCESS;
  }
  virtual int handle_split_dest_partition_result(const obrpc::ObSplitDestPartitionResult& result)
  {
    UNUSED(result);
    return OB_SUCCESS;
  }

  virtual int handle_replica_split_progress_request(
      const obrpc::ObReplicaSplitProgressRequest& arg, obrpc::ObReplicaSplitProgressResult& result)
  {
    UNUSED(result);
    UNUSED(arg);
    return OB_SUCCESS;
  }
  virtual int handle_replica_split_progress_result(const obrpc::ObReplicaSplitProgressResult& result)
  {
    UNUSED(result);
    return OB_SUCCESS;
  }

  virtual int set_global_max_decided_trans_version(const int64_t trans_version)
  {
    UNUSED(trans_version);
    return OB_SUCCESS;
  }
  virtual const common::ObAddr& get_self_addr() const
  {
    return addr_;
  }
  virtual int get_all_partition_status(int64_t& inactive_num, int64_t& total_num) const
  {
    UNUSED(inactive_num);
    UNUSED(total_num);
    return OB_SUCCESS;
  }
  virtual int migrate_replica(const common::ObPartitionKey& pkey, const common::ObReplicaMember& src, /* old replica */
      const common::ObReplicaMember& dst,                                                             /* new replica */
      const common::ObReplicaMember& data_src,                                                        /* data source */
      const int64_t quorum, const share::ObTaskId& task_id)
  {
    UNUSED(pkey);
    UNUSED(src);
    UNUSED(dst);
    UNUSED(data_src);
    UNUSED(task_id);
    UNUSED(quorum);
    return OB_SUCCESS;
  }

  virtual int query_range_to_macros(common::ObIAllocator& allocator, const common::ObPartitionKey& pkey_,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count,
      const int64_t* total_task_count, ObIArray<common::ObStoreRange>* splitted_ranges,
      common::ObIArray<int64_t>* split_index)
  {
    UNUSED(allocator);
    UNUSED(pkey_);
    UNUSED(ranges);
    UNUSED(type);
    UNUSED(macros_count);
    UNUSED(total_task_count);
    UNUSED(splitted_ranges);
    UNUSED(split_index);
    return OB_SUCCESS;
  }

  virtual int check_all_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id, int64_t& max_timestamp)
  {
    UNUSED(index_table_id);
    UNUSED(max_timestamp);
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  virtual int revoke_all_election_leader(const int64_t switchover_epoch)
  {
    UNUSED(switchover_epoch);
    return OB_SUCCESS;
  }

  virtual int check_all_partition_sync_state(const int64_t switchover_epoch)
  {
    UNUSED(switchover_epoch);
    return OB_SUCCESS;
  }

  virtual memtable::ObIMemtableCtxFactory* get_mem_ctx_factory()
  {
    return nullptr;
  }

  virtual int minor_freeze(const common::ObPartitionKey& pkey, const bool emergency = false)
  {
    UNUSED(pkey);
    UNUSED(emergency);
    return OB_SUCCESS;
  }

  virtual int freeze_partition(const ObPartitionKey& pkey, const bool emergency)
  {
    UNUSED(pkey);
    UNUSED(emergency);
    return OB_SUCCESS;
  }

  virtual int get_dup_replica_type(
      const common::ObPartitionKey& pkey, const common::ObAddr& server, DupReplicaType& dup_replica_type)
  {
    UNUSED(pkey);
    UNUSED(server);
    UNUSED(dup_replica_type);
    return OB_SUCCESS;
  }

  virtual int get_dup_replica_type(
      const uint64_t table_id, const common::ObAddr& server, DupReplicaType& dup_replica_type)
  {
    UNUSED(table_id);
    UNUSED(server);
    UNUSED(dup_replica_type);
    return OB_SUCCESS;
  }

  virtual int get_replica_status(const common::ObPartitionKey& pkey, share::ObReplicaStatus& replica_status) const
  {
    UNUSED(pkey);
    UNUSED(replica_status);
    return OB_SUCCESS;
  }

  virtual int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }

  virtual int add_replica(const obrpc::ObAddReplicaArg& rpc_arg, const share::ObTaskId& task_id)
  {
    UNUSED(rpc_arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }

  virtual int migrate_replica(const obrpc::ObMigrateReplicaArg& rpc_arg, const share::ObTaskId& task_id)
  {
    UNUSED(rpc_arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }

  virtual int rebuild_replica(const obrpc::ObRebuildReplicaArg& rpc_arg, const share::ObTaskId& task_id)
  {
    UNUSED(rpc_arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }

  virtual int copy_global_index(const obrpc::ObCopySSTableArg& rpc_arg, const share::ObTaskId& task_id)
  {
    UNUSED(rpc_arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }

  virtual int copy_local_index(const obrpc::ObCopySSTableArg& rpc_arg, const share::ObTaskId& task_id)
  {
    UNUSED(rpc_arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }

  virtual int change_replica(const obrpc::ObChangeReplicaArg& rpc_arg, const share::ObTaskId& task_id)
  {
    UNUSED(rpc_arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }
  virtual int batch_change_replica(const obrpc::ObChangeReplicaBatchArg& arg)
  {
    UNUSED(arg);
    return OB_SUCCESS;
  }
  virtual int batch_remove_replica(const obrpc::ObRemoveReplicaArgs& args)
  {
    UNUSED(args);
    return OB_SUCCESS;
  }
  virtual int batch_remove_non_paxos_replica(
      const obrpc::ObRemoveNonPaxosReplicaBatchArg& args, obrpc::ObRemoveNonPaxosReplicaBatchResult& results)
  {
    UNUSED(args);
    UNUSED(results);
    return OB_SUCCESS;
  }

  virtual int restore_replica(const obrpc::ObRestoreReplicaArg& arg, const share::ObTaskId& task_id)
  {
    UNUSED(arg);
    UNUSED(task_id);
    return OB_SUCCESS;
  }

  virtual int restore_follower_replica(const obrpc::ObCopySSTableBatchArg& arg)
  {
    UNUSED(arg);
    return OB_SUCCESS;
  }

  virtual int batch_remove_replica_mc(
      const obrpc::ObMemberChangeBatchArg& arg, obrpc::ObMemberChangeBatchResult& result)
  {
    UNUSED(arg);
    UNUSED(result);
    return OB_SUCCESS;
  }

  virtual int calc_column_checksum(const common::ObPartitionKey& pkey, const uint64_t index_table_id,
      const int64_t schema_version, const uint64_t execution_id, const int64_t snapshot_version)
  {
    UNUSED(pkey);
    UNUSED(index_table_id);
    UNUSED(schema_version);
    UNUSED(execution_id);
    UNUSED(snapshot_version);
    return OB_SUCCESS;
  }

  virtual int check_single_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id, int64_t& timestamp)
  {
    UNUSED(pkey);
    UNUSED(index_table_id);
    UNUSED(timestamp);
    return OB_SUCCESS;
  }

  virtual int get_server_idc(const common::ObAddr& server, common::ObIDC& idc) const override
  {
    UNUSED(server);
    UNUSED(idc);
    return OB_SUCCESS;
  }
  virtual int get_server_cluster_id(const common::ObAddr& server, int64_t& cluster_id) const override
  {
    UNUSED(server);
    UNUSED(cluster_id);
    return OB_SUCCESS;
  }
  virtual int record_server_region(const common::ObAddr& server, const common::ObRegion& region)
  {
    UNUSED(server);
    UNUSED(region);
    return OB_SUCCESS;
  }
  virtual int record_server_idc(const common::ObAddr& server, const common::ObIDC& idc) override
  {
    UNUSED(server);
    UNUSED(idc);
    return OB_SUCCESS;
  }

  virtual int record_server_cluster_id(const common::ObAddr& server, const int64_t cluster_id)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    return OB_SUCCESS;
  }

private:
  common::ObScanner scanner_;
  int64_t col_num_;
  obrpc::ObCommonRpcProxy* rs_rpc_proxy_;
  ObPartitionServiceRpc* pts_rpc_;
  ObAddr addr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFakePartitionServiceForSQL);
};

class ObFakePartitionServiceForGI : public ObFakePartitionServiceForSQL {
public:
  class ObGIResult {
  public:
    ObGIResult() : macros_count_idx_(), pkeys_(), macros_count_(){};
    virtual ~ObGIResult() = default;
    int macros_count_idx_;
    ObSEArray<common::ObPartitionKey, 32> pkeys_;
    ObSEArray<int, 32> macros_count_;
    TO_STRING_KV(K(macros_count_), K(pkeys_));
  };

public:
  ObFakePartitionServiceForGI() : ObFakePartitionServiceForSQL(), result_set_(), case_idx_(), pkey_idx_()
  {}
  virtual ~ObFakePartitionServiceForGI() = default;
  virtual int query_range_to_macros(common::ObIAllocator& allocator, const common::ObPartitionKey& pkey_,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count,
      const int64_t* total_task_count, ObIArray<common::ObStoreRange>* splitted_ranges,
      common::ObIArray<int64_t>* split_index) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFakePartitionServiceForGI);

public:
  ObSEArray<ObGIResult, 32> result_set_;

private:
  int case_idx_;
  int pkey_idx_;
};

}  // namespace storage
}  // namespace oceanbase
#endif /* OCEANBASE_UNITTEST_ENGINE_TABLE_OB_FAKE_PARTITION_SERVICE_ */
