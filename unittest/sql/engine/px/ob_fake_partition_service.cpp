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

#define USING_LOG_PREFIX SQL_EXE
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

ObFakePartitionServiceForSQL::ObFakePartitionServiceForSQL()
    : scanner_(), col_num_(), rs_rpc_proxy_(nullptr), pts_rpc_(nullptr)
{}

ObFakePartitionServiceForSQL::~ObFakePartitionServiceForSQL()
{}

int ObFakePartitionServiceForSQL::table_scan(ObTableScanParam& param, ObNewRowIterator*& result)
{
  ObFakeTableScanIterator* iter = OB_NEW(ObFakeTableScanIterator, ObModIds::TEST);
  iter->init(scanner_.begin(), col_num_);
  result = static_cast<ObNewRowIterator*>(iter);
  SQL_ENG_LOG(INFO, "table scan", K(param));
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::revert_scan_iter(ObNewRowIterator* iter)
{
  OB_DELETE(ObNewRowIterator, ObModIds::TEST, iter);
  SQL_ENG_LOG(INFO, "revert_scan_iter");
  return OB_SUCCESS;
}

bool ObFakePartitionServiceForSQL::is_empty() const
{
  return false;
}

int ObFakePartitionServiceForSQL::init(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
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

int ObFakePartitionServiceForSQL::start()
{
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::destroy()
{
  return OB_SUCCESS;
}

void ObFakePartitionServiceForSQL::set_component_service(ObIPartitionComponentFactory& cp_fty)
{
  UNUSED(cp_fty);
}

int ObFakePartitionServiceForSQL::load_partition(const char* buf, const int64_t buf_len, int64_t& pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::replay_base_storage_log(
    const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos)
{
  UNUSED(log_seq_num);
  UNUSED(subcmd);
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::create_partition(const ObPartitionKey& key, const common::ObVersion data_version,
    const int64_t replica_num, const common::ObMemberList& mem_list)
{
  UNUSED(key);
  UNUSED(data_version);
  UNUSED(replica_num);
  UNUSED(mem_list);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::online_partition()
{
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::remove_partition(const common::ObPartitionKey& key)
{
  UNUSED(key);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::remove_orphans()
{
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::freeze()
{
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::on_leader_takeover(const common::ObPartitionKey& partition_key)
{
  UNUSED(partition_key);
  return OB_SUCCESS;
}

int64_t ObFakePartitionServiceForSQL::get_min_using_file_id() const
{
  return OB_SUCCESS;
}

share::schema::ObMultiVersionSchemaService* ObFakePartitionServiceForSQL::get_schema_service()
{
  return NULL;
}

ObIPartitionComponentFactory* ObFakePartitionServiceForSQL::get_cp_fty()
{
  return NULL;
}

// ==========================================================================
// transaction service interfaces
int ObFakePartitionServiceForSQL::start_trans(
    const uint64_t tenant_id, const transaction::ObStartTransParam& req, transaction::ObTransDesc& trans_desc)
{
  UNUSED(tenant_id);
  UNUSED(req);
  UNUSED(trans_desc);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::end_trans(bool is_rollback, const transaction::ObTransDesc& trans_desc)
{
  UNUSED(is_rollback);
  UNUSED(trans_desc);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::start_stmt(
    const uint64_t tenant_id, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants)
{
  UNUSED(tenant_id);
  UNUSED(trans_desc);
  UNUSED(participants);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::end_stmt(bool is_rollback, const transaction::ObTransDesc& trans_desc)
{
  UNUSED(is_rollback);
  UNUSED(trans_desc);
  return OB_SUCCESS;
}

// ==========================================================================
// partition storage interfaces
#if 0
int ObFakePartitionServiceForSQL::table_scan(ObTableScanParam &param,
                                       common::ObNewRowIterator *&result)
{
  UNUSED(param);
  UNUSED(result);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::revert_scan_iter(common::ObNewRowIterator *iter)
{
  UNUSED(iter);
  return OB_SUCCESS;
}
#endif

int ObFakePartitionServiceForSQL::delete_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
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

int ObFakePartitionServiceForSQL::insert_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
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

int ObFakePartitionServiceForSQL::update_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
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

int ObFakePartitionServiceForSQL::lock_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout,
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

int ObFakePartitionServiceForSQL::fetch_conflict_rows(const transaction::ObTransDesc& trans_desc,
    const ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey,
    const common::ObIArray<uint64_t>& in_column_ids, const common::ObIArray<uint64_t>& out_column_ids,
    common::ObNewRowIterator& check_row_iter, common::ObIArray<common::ObNewRowIterator*>& dup_row_iters)
{
  UNUSED(trans_desc);
  UNUSED(dml_param);
  UNUSED(pkey);
  UNUSED(in_column_ids);
  UNUSED(out_column_ids);
  UNUSED(check_row_iter);
  UNUSED(dup_row_iters);
  return OB_SUCCESS;
}

// ==========================================================================
// partition manager interfaces
int ObFakePartitionServiceForSQL::get_all_partitions(common::ObIArray<ObIPartitionGroup*>& partition_list)
{
  UNUSED(partition_list);
  return 0;
}

int ObFakePartitionServiceForSQL::get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition)
{
  UNUSED(pkey);
  UNUSED(partition);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::get_partition_count(int64_t& partition_count) const
{
  UNUSED(partition_count);
  return OB_SUCCESS;
}

// ==========================================================================
// replay interfaces
int ObFakePartitionServiceForSQL::replay_redo_log(
    const common::ObPartitionKey& pkey, const ObStoreCtx& ctx, const int64_t ts, const char* buf, const int64_t size)
{
  UNUSED(pkey);
  UNUSED(ctx);
  UNUSED(ts), UNUSED(buf);
  UNUSED(size);
  return OB_SUCCESS;
}

transaction::ObTransService* ObFakePartitionServiceForSQL::get_trans_service()
{
  return NULL;
}

clog::ObICLogMgr* ObFakePartitionServiceForSQL::get_clog_mgr()
{
  return NULL;
}

// ==========================================================================
// major freeze
int ObFakePartitionServiceForSQL::prepare_major_freeze(
    const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp)
{
  UNUSED(partitions);
  UNUSED(frozen_version);
  UNUSED(frozen_timestamp);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::commit_major_freeze(
    const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp)
{
  UNUSED(partitions);
  UNUSED(frozen_version);
  UNUSED(frozen_timestamp);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::abort_major_freeze(
    const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp)
{
  UNUSED(partitions);
  UNUSED(frozen_version);
  UNUSED(frozen_timestamp);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::set_major_freeze_status(
    const common::ObPartitionKey& pkey, const int64_t frozen_version, const int64_t major_freeze_status)
{
  UNUSED(pkey);
  UNUSED(frozen_version);
  UNUSED(major_freeze_status);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::get_major_freeze_status(
    const common::ObPartitionKey& pkey, int64_t& frozen_version, int64_t& major_freeze_status)
{
  UNUSED(pkey);
  UNUSED(frozen_version);
  UNUSED(major_freeze_status);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::replay(const ObPartitionKey& partition, const char* log, const int64_t size)
{
  UNUSED(partition);
  UNUSED(log);
  UNUSED(size);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::finish_replay(const ObPartitionKey& partition)
{
  UNUSED(partition);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::leader_freeze_success(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::leader_freeze_fail(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::follower_freeze_success(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::follower_freeze_fail(const ObPartitionKey& pkey, const int64_t freeze_cmd)
{
  UNUSED(pkey);
  UNUSED(freeze_cmd);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::submit_freeze_log_success(const int64_t cmd_type, const ObPartitionKey& pkey)
{
  UNUSED(cmd_type);
  UNUSED(pkey);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::get_role(const common::ObPartitionKey& pkey, common::ObRole& role) const
{
  UNUSED(pkey);
  UNUSED(role);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::get_leader_curr_member_list(
    const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
{
  UNUSED(pkey);
  UNUSED(member_list);
  return OB_SUCCESS;
}

int ObFakePartitionServiceForSQL::get_active_memtable_version(const ObPartitionKey& pkey, int64_t& version)
{
  UNUSED(pkey);
  UNUSED(version);
  return OB_SUCCESS;
}

/*
 * We can not simulate the whole procedures of query_range_to_macros.
 * The reason is these procedures relate to many other module.
 * So the result of this function is by design (and immobilization).
 * */
int ObFakePartitionServiceForGI::query_range_to_macros(common::ObIAllocator& allocator,
    const common::ObPartitionKey& pkey, const common::ObIArray<common::ObStoreRange>& ranges, const int64_t type,
    uint64_t* macros_count, const int64_t* total_task_count, ObIArray<common::ObStoreRange>* splitted_ranges,
    common::ObIArray<int64_t>* split_index)
{
  UNUSED(allocator);
  UNUSED(pkey);
  UNUSED(ranges);
  UNUSED(type);
  UNUSED(macros_count);
  UNUSED(total_task_count);
  UNUSED(splitted_ranges);
  UNUSED(split_index);
  int ret = OB_SUCCESS;
  if (case_idx_ < result_set_.count()) {
    if (type == OB_GET_MACROS_COUNT_BY_QUERY_RANGE) {
      int idx = result_set_.at(case_idx_).macros_count_idx_++;
      if (idx >= result_set_.at(case_idx_).macros_count_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("out of idx",
            K(OB_ERR_UNEXPECTED),
            K(idx),
            K(case_idx_),
            K(result_set_.count()),
            K(result_set_.at(case_idx_).macros_count_.count()));
      } else {
        *macros_count = result_set_.at(case_idx_).macros_count_.at(idx);
      }
    } else if (type == OB_GET_BLOCK_RANGE) {
      if (OB_ISNULL(splitted_ranges) || OB_ISNULL(split_index) || OB_ISNULL(total_task_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpexted null", K(ret), K(splitted_ranges), K(split_index));
      } else if (pkey_idx_ < 5) {
        pkey_idx_++;
        for (int64_t i = 0; i < *total_task_count && OB_SUCC(ret); ++i) {
          ObStoreRange range;
          range.set_table_id(1);
          range.set_whole_range();
          if (OB_FAIL(splitted_ranges->push_back(range))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (pkey_idx_ == 5) {
          pkey_idx_ = 0;
          case_idx_++;
        }
      }
    }
  }
  return OB_SUCCESS;
}

}  // namespace storage
}  // namespace oceanbase
