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

#include "ob_fake_partition_service.h"
#include "storage/transaction/ob_trans_service.h"
#include "clog/ob_clog_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {

namespace sql {
int ObFakeTableScanIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = common::OB_SUCCESS;
  common::ObNewRow* cur_row = OB_NEW(ObNewRow, ObModIds::TEST);
  ObObj* objs = (ObObj*)ob_malloc(sizeof(ObObj) * col_num_, ObModIds::TEST);
  if (!cur_row || !objs) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "no memory");
    if (cur_row) {
      OB_DELETE(ObNewRow, ObModIds::TEST, cur_row);
    }
    if (objs) {
      ob_free(objs);
    }
  } else {
    for (int64_t i = 0; i < col_num_; ++i) {
      new (objs + i) ObObj();
    }
    cur_row->count_ = col_num_;
    cur_row->cells_ = objs;
    if (OB_SUCCESS != (ret = row_store_it_.get_next_row(*cur_row))) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = cur_row;
    }
  }
  if (NULL == cur_row->cells_) {
    SQL_ENG_LOG(WARN, "cur_row->cells_ is NULL");
  }
  return ret;
}
}  // namespace sql

namespace storage {

ObFakePartitionService::ObFakePartitionService()
{}

ObFakePartitionService::~ObFakePartitionService()
{}

int ObFakePartitionService::table_scan(ObTableScanParam& param, ObNewRowIterator*& result)
{
  ObFakeTableScanIterator* iter = OB_NEW(ObFakeTableScanIterator, ObModIds::TEST);
  iter->init(scanner_.begin(), col_num_);
  result = static_cast<ObNewRowIterator*>(iter);
  SQL_ENG_LOG(INFO, "table scan", K(param));
  return OB_SUCCESS;
}

int ObFakePartitionService::revert_scan_iter(ObNewRowIterator* iter)
{
  OB_DELETE(ObNewRowIterator, ObModIds::TEST, iter);
  SQL_ENG_LOG(INFO, "revert_scan_iter");
  return OB_SUCCESS;
}

bool ObFakePartitionService::is_empty() const
{
  return false;
}

int ObFakePartitionService::init(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
    ObIPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
    share::ObIPartitionLocationCache* location_cache, common::ObIRSCb* rs_cb, rpc::frame::ObReqTransport* req_transport)
{
  UNUSED(env);
  UNUSED(self_addr);
  UNUSED(cp_fty);
  UNUSED(schema_service);
  UNUSED(location_cache);
  UNUSED(rs_cb);
  UNUSED(req_transport);
  return OB_SUCCESS;
}

int ObFakePartitionService::start()
{
  return OB_SUCCESS;
}

int ObFakePartitionService::destroy()
{
  return OB_SUCCESS;
}

void ObFakePartitionService::set_component_service(ObIPartitionComponentFactory& cp_fty)
{
  UNUSED(cp_fty);
}

int ObFakePartitionService::load_partition(const char* buf, const int64_t buf_len, int64_t& pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

int ObFakePartitionService::replay_base_storage_log(
    const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos)
{
  UNUSED(log_seq_num);
  UNUSED(subcmd);
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  return OB_SUCCESS;
}

int ObFakePartitionService::create_partition(const ObPartitionKey& key, const common::ObVersion data_version,
    const int64_t replica_num, const common::ObMemberList& mem_list)
{
  UNUSED(key);
  UNUSED(data_version);
  UNUSED(replica_num);
  UNUSED(mem_list);
  return OB_SUCCESS;
}

int ObFakePartitionService::online_partition()
{
  return OB_SUCCESS;
}

int ObFakePartitionService::offline_partition(const common::ObPartitionKey& leader_key, const common::ObAddr& server)
{
  UNUSED(leader_key);
  UNUSED(server);
  return OB_SUCCESS;
}

int ObFakePartitionService::remove_partition(const common::ObPartitionKey& key)
{
  UNUSED(key);
  return OB_SUCCESS;
}

int ObFakePartitionService::remove_orphans()
{
  return OB_SUCCESS;
}

int ObFakePartitionService::freeze()
{
  return OB_SUCCESS;
}

int ObFakePartitionService::on_leader_revoke(const common::ObPartitionKey& partition_key)
{
  UNUSED(partition_key);
  return OB_SUCCESS;
}

int ObFakePartitionService::on_leader_takeover(const common::ObPartitionKey& partition_key)
{
  UNUSED(partition_key);
  return OB_SUCCESS;
}

int64_t ObFakePartitionService::get_min_using_file_id() const
{
  return OB_SUCCESS;
}

ObBaseStorage* ObFakePartitionService::get_base_storage()
{
  return NULL;
}

share::schema::ObMultiVersionSchemaService* ObFakePartitionService::get_schema_service()
{
  return NULL;
}

ObIPartitionComponentFactory* ObFakePartitionService::get_cp_fty()
{
  return NULL;
}

// ==========================================================================
// transaction service interfaces
int ObFakePartitionService::start_trans(
    const uint64_t tenant_id, const transaction::ObStartTransParam& req, transaction::ObTransDesc& trans_desc)
{
  UNUSED(tenant_id);
  UNUSED(req);
  UNUSED(trans_desc);
  return OB_SUCCESS;
}

int ObFakePartitionService::end_trans(bool is_rollback, const transaction::ObTransDesc& trans_desc)
{
  UNUSED(is_rollback);
  UNUSED(trans_desc);
  return OB_SUCCESS;
}

int ObFakePartitionService::start_stmt(
    const uint64_t tenant_id, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants)
{
  UNUSED(tenant_id);
  UNUSED(trans_desc);
  UNUSED(participants);
  return OB_SUCCESS;
}

int ObFakePartitionService::end_stmt(bool is_rollback, const transaction::ObTransDesc& trans_desc)
{
  UNUSED(is_rollback);
  UNUSED(trans_desc);
  return OB_SUCCESS;
}

// ==========================================================================
// partition storage interfaces
#if 0
int ObFakePartitionService::table_scan(ObTableScanParam &param,
                                       common::ObNewRowIterator *&result)
{
  UNUSED(param);
  UNUSED(result);
  return OB_SUCCESS;
}

int ObFakePartitionService::revert_scan_iter(common::ObNewRowIterator *iter)
{
  UNUSED(iter);
  return OB_SUCCESS;
}
#endif

int ObFakePartitionService::delete_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  UNUSED(trans_desc);
  UNUSED(timeout);
  UNUSED(pkey);
  UNUSED(column_ids);
  UNUSED(row_iter);
  UNUSED(affected_rows);
  return OB_SUCCESS;
}

int ObFakePartitionService::insert_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  UNUSED(trans_desc);
  UNUSED(timeout);
  UNUSED(pkey);
  UNUSED(column_ids);
  UNUSED(row_iter);
  UNUSED(affected_rows);
  return OB_SUCCESS;
}

int ObFakePartitionService::update_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  UNUSED(trans_desc);
  UNUSED(timeout);
  UNUSED(pkey);
  UNUSED(column_ids);
  UNUSED(updated_column_ids);
  UNUSED(row_iter);
  UNUSED(affected_rows);
  return OB_SUCCESS;
}

int ObFakePartitionService::lock_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
    const common::ObPartitionKey& pkey, common::ObNewRowIterator* row_iter, const ObLockFlag lock_flag,
    int64_t& affected_rows)
{
  UNUSED(trans_desc);
  UNUSED(timeout);
  UNUSED(pkey);
  UNUSED(row_iter);
  UNUSED(lock_flag);
  UNUSED(affected_rows);
  return OB_SUCCESS;
}

// ==========================================================================
// partition manager interfaces
int ObFakePartitionService::get_all_partitions(common::ObIArray<ObIPartition*>& partition_list)
{
  UNUSED(partition_list);
  return 0;
}

int ObFakePartitionService::get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition)
{
  UNUSED(pkey);
  UNUSED(partition);
  return OB_SUCCESS;
}

int ObFakePartitionService::get_partition_count(int64_t& partition_count) const
{
  UNUSED(partition_count);
  return OB_SUCCESS;
}

void revert_replay_status(ObReplayStatus* replay_status)
{
  UNUSED(replay_status);
}

ObIPartitionIter* ObFakePartitionService::alloc_pg_iter()
{
  return NULL;
}

void ObFakePartitionService::revert_pg_iter(ObIPartitionIter* iter)
{
  UNUSED(iter);
}
ObPGPartitionIterator* ObFakePartitionService::alloc_pg_partition_iter()
{
  return NULL;
}

void ObFakePartitionService::revert_pg_partition_iter(ObPGPartitionIterator* iter)
{
  UNUSED(iter);
}

// ==========================================================================
// replay interfaces
int ObFakePartitionService::replay_redo_log(
    const common::ObPartitionKey& pkey, const ObStoreCtx& ctx, const int64_t ts, const char* buf, const int64_t size)
{
  UNUSED(pkey);
  UNUSED(ctx);
  UNUSED(ts);
  UNUSED(buf);
  UNUSED(size);
  return OB_SUCCESS;
}

transaction::ObITransService* ObFakePartitionService::get_trans_service()
{
  return NULL;
}

clog::ObICLogMgr* ObFakePartitionService::get_clog_mgr()
{
  return NULL;
}

// ==========================================================================
// major freeze
int ObFakePartitionService::set_major_freeze_cb(ObIMajorFreezeCb* cb)
{
  UNUSED(cb);
  return OB_SUCCESS;
}

int ObFakePartitionService::prepare_major_freeze(
    const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp)
{
  UNUSED(partitions);
  UNUSED(frozen_version);
  UNUSED(frozen_timestamp);
  return OB_SUCCESS;
}

int ObFakePartitionService::commit_major_freeze(
    const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp)
{
  UNUSED(partitions);
  UNUSED(frozen_version);
  UNUSED(frozen_timestamp);
  return OB_SUCCESS;
}

int ObFakePartitionService::abort_major_freeze(
    const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp)
{
  UNUSED(partitions);
  UNUSED(frozen_version);
  UNUSED(frozen_timestamp);
  return OB_SUCCESS;
}

int ObFakePartitionService::set_major_freeze_status(
    const common::ObPartitionKey& pkey, const int64_t frozen_version, const int64_t major_freeze_status)
{
  UNUSED(pkey);
  UNUSED(frozen_version);
  UNUSED(major_freeze_status);
  return OB_SUCCESS;
}

int ObFakePartitionService::get_major_freeze_status(
    const common::ObPartitionKey& pkey, int64_t& frozen_version, int64_t& major_freeze_status)
{
  UNUSED(pkey);
  UNUSED(frozen_version);
  UNUSED(major_freeze_status);
  return OB_SUCCESS;
}

int ObFakePartitionService::replay(const ObPartitionKey& partition, const char* log, const int64_t size)
{
  UNUSED(partition);
  UNUSED(log);
  UNUSED(size);
  return OB_SUCCESS;
}

int ObFakePartitionService::finish_replay(const ObPartitionKey& partition)
{
  UNUSED(partition);
  return OB_SUCCESS;
}

int ObFakePartitionService::leader_freeze_success(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionService::leader_freeze_fail(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionService::follower_freeze_success(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionService::follower_freeze_fail(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionService::submit_freeze_log_success(const int64_t cmd_type, const ObPartitionKey& pkey)
{
  UNUSED(cmd_type);
  UNUSED(pkey);
  return OB_SUCCESS;
}

int ObFakePartitionService::get_role(const common::ObPartitionKey& pkey, common::ObRole& role) const
{
  UNUSED(pkey);
  UNUSED(role);
  return OB_SUCCESS;
}

int ObFakePartitionService::get_leader_curr_member_list(
    const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
{
  UNUSED(pkey);
  UNUSED(member_list);
  return OB_SUCCESS;
}

int ObFakePartitionService::on_member_change_success(const common::ObPartitionKey& partition_key,
    const int64_t mc_timestamp, const common::ObMemberList& prev_member_list,
    const common::ObMemberList& curr_member_list)
{
  UNUSED(partition_key);
  UNUSED(mc_timestamp);
  UNUSED(prev_member_list);
  UNUSED(curr_member_list);
  return OB_SUCCESS;
}

int ObFakePartitionService::get_active_memtable_version(const ObPartitionKey& pkey, int64_t& version)
{
  UNUSED(pkey);
  UNUSED(version);
  return OB_SUCCESS;
}

}  // namespace storage
}  // namespace oceanbase
