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
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase {

namespace sql {
class ObFakeTableScanIterator : public common::ObNewRowIterator {
public:
  ObFakeTableScanIterator()
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
class ObFakePartitionService : public ObPartitionService {
public:
  ObFakePartitionService();
  virtual ~ObFakePartitionService();

  //
  virtual int init(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
      ObIPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
      share::ObIPartitionLocationCache* location_cache, common::ObIRSCb* rs_cb,
      rpc::frame::ObReqTransport* req_transport);
  virtual int start();
  virtual int destroy();
  virtual void set_component_service(ObIPartitionComponentFactory& cp_fty);
  //
  virtual int load_partition(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos);
  virtual int create_partition(const common::ObPartitionKey& key, const common::ObVersion data_version,
      const int64_t replica_num, const common::ObMemberList& mem_list);
  virtual int online_partition();
  virtual int offline_partition(const common::ObPartitionKey& leader_key, const common::ObAddr& server);
  virtual int remove_partition(const common::ObPartitionKey& key);
  virtual int remove_orphans();
  virtual int freeze();
  virtual int on_leader_revoke(const common::ObPartitionKey& partition_key);
  virtual int on_leader_takeover(const common::ObPartitionKey& partition_key);
  virtual int64_t get_min_using_file_id() const;
  virtual ObBaseStorage* get_base_storage();
  virtual share::schema::ObMultiVersionSchemaService* get_schema_service();
  virtual ObIPartitionComponentFactory* get_cp_fty();
  virtual int on_member_change_success(const common::ObPartitionKey& partition_key, const int64_t mc_timestamp,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list);

  // ==========================================================================
  // transaction service interfaces
  virtual int start_trans(
      const uint64_t tenant_id, const transaction::ObStartTransParam& req, transaction::ObTransDesc& trans_desc);
  virtual int end_trans(bool is_rollback, const transaction::ObTransDesc& trans_desc);
  virtual int start_stmt(
      const uint64_t tenant_id, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants);
  virtual int end_stmt(bool is_rollback, const transaction::ObTransDesc& trans_desc);

  // ==========================================================================
  // partition storage interfaces
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

  // ==========================================================================
  // partition manager interfaces
  virtual int get_all_partitions(common::ObIArray<ObIPartition*>& partition_list);
  virtual int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition);

  virtual int get_partition_count(int64_t& partition_count) const;
  virtual void revert_replay_status(ObReplayStatus* replay_status) const;
  virtual ObIPartitionIter* alloc_pg_iter();
  virtual void revert_pg_iter(ObIPartitionIter* iter);
  virtual ObPGPartitionIterator* alloc_pg_partition_iter();
  virtual void revert_pg_partition_iter(ObPGPartitionIterator* iter);
  virtual bool is_empty() const;

  // ==========================================================================
  // replay interfaces
  virtual int replay_redo_log(
      const common::ObPartitionKey& pkey, const ObStoreCtx& ctx, const int64_t ts, const char* buf, const int64_t size);

  virtual transaction::ObITransService* get_trans_service();
  virtual clog::ObICLogMgr* get_clog_mgr();

  // ==========================================================================
  // major freeze
  virtual int set_major_freeze_cb(ObIMajorFreezeCb* cb);
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

  // ==========================================================================
  // misc functions
  virtual int get_role(const common::ObPartitionKey& pkey, common::ObRole& role) const;
  virtual int get_leader_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const;
  virtual int get_active_memtable_version(const ObPartitionKey& pkey, int64_t& version);
  virtual int activate_tenant(const uint64_t tenant_id)
  {
    return OB_SUCCESS;
  }
  virtual int inactivate_tenant(const uint64_t tenant_id)
  {
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
  virtual ObBaseStorage* get_base_storage()
  {
    return NULL;
  }
  virtual int migrate_replica_batch(const obrpc::ObMigrateReplicaBatchArg& arg)
  {
    UNUSED(arg);
    return OB_ERROR;
  }

private:
  common::ObScanner scanner_;
  int64_t col_num_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFakePartitionService);
};

}  // namespace storage
}  // namespace oceanbase
#endif /* OCEANBASE_UNITTEST_ENGINE_TABLE_OB_FAKE_PARTITION_SERVICE_ */
