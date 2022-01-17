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

#ifndef OCEANBASE_UNITTEST_MOCK_PARTITION_SERVICE_H_
#define OCEANBASE_UNITTEST_MOCK_PARTITION_SERVICE_H_

//#include "storage/ob_i_partition.h"
//#include "storage/ob_partition_service.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_safe_ref.h"
#include "clog/ob_log_define.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "../mockcontainer/mock_ob_partition.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace storage {
class ObIPartitionComponentFactory;
class ObBaseStorage;
}  // namespace storage
namespace unittest {
class MockPartition : public storage::MockObIPartitionGroup {
public:
  MockPartition() : tenant_id_(1001)
  {
    partition_key_.init(1, 1, 1);
  }
  virtual ~MockPartition()
  {}

public:
  void reset()
  {
    replay_status_.reset();
  }
  virtual bool is_valid() const
  {
    return true;
  }
  int set_replay_engine(replayengine::ObLogReplayEngine* rp_eg)
  {
    replay_status_.enable(partition_key_);
    storage::SafeRef2 ref;
    return replay_status_.init(tenant_id_, rp_eg, ref);
  }
  virtual storage::ObReplayStatus* get_replay_status()
  {
    return &replay_status_;
  }
  virtual const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  virtual int init(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, ObBaseStorage* base_storage,
      clog::ObIPartitionLogService* pls, transaction::ObTransService* txs)
  {
    UNUSED(key);
    UNUSED(cp_fty);
    UNUSED(base_storage);
    UNUSED(pls);
    UNUSED(schema_service);
    UNUSED(txs);
    return common::OB_SUCCESS;
  }
  virtual void destroy()
  {
    return;
  }
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos)
  {
    UNUSED(log_seq_num);
    UNUSED(subcmd);
    UNUSED(buf);
    UNUSED(len);
    UNUSED(pos);
    return common::OB_SUCCESS;
  }
  // get partition log service
  virtual const clog::ObIPartitionLogService* get_log_service() const
  {
    return NULL;
  }
  virtual clog::ObIPartitionLogService* get_log_service()
  {
    return NULL;
  }
  virtual int get_table_store_cnt(int64_t& table_cnt) const
  {
    table_cnt = 0;
    return OB_SUCCESS;
  }
  virtual int get_log_archive_status(clog::ObPGLogArchiveStatus& status)
  {
    UNUSED(status);
    return common::OB_SUCCESS;
  }
  // get partition storage
  virtual ObIPartitionStorage* get_storage()
  {
    return NULL;
  }

  virtual int table_scan(const ObStoreCtx& ctx, ObTableScanParam& param, common::ObNewRowIterator*& result)
  {
    UNUSED(ctx);
    UNUSED(param);
    UNUSED(result);
    return common::OB_SUCCESS;
  }
  virtual int revert_scan_iter(common::ObNewRowIterator* iter)
  {
    UNUSED(iter);
    return common::OB_SUCCESS;
  }
  virtual int delete_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }
  virtual int insert_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }
  virtual int update_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(updated_column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }
  virtual int lock_rows(const ObStoreCtx& ctx, const int64_t timeout, common::ObNewRowIterator* row_iter,
      const ObLockFlag lock_flag, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(row_iter);
    UNUSED(lock_flag);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }
  virtual int lock_rows(
      const ObStoreCtx& ctx, const int64_t timeout, const common::ObNewRow& row, const ObLockFlag lock_flag)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(row);
    UNUSED(lock_flag);
    return common::OB_SUCCESS;
  }
  virtual void set_valid(bool valid)
  {
    UNUSED(valid);
    return;
  }
  virtual int rdlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int wrlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int try_rdlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int try_wrlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int unlock()
  {
    return common::OB_SUCCESS;
  }

  virtual int save_base_storage_info(const common::ObBaseStorageInfo& info)
  {
    UNUSED(info);
    return common::OB_SUCCESS;
  }
  virtual int save_frozen_storage_info(const common::ObBaseStorageInfo& info)
  {
    UNUSED(info);
    return common::OB_SUCCESS;
  }
  virtual int get_base_storage_info(common::ObBaseStorageInfo& info)
  {
    UNUSED(info);
    return common::OB_SUCCESS;
  }
  virtual int get_frozen_storage_info(common::ObBaseStorageInfo& info)
  {
    UNUSED(info);
    return common::OB_SUCCESS;
  }

  virtual common::ObRole get_role() const
  {
    return common::LEADER;
  }

  // write ssstore objects @version tree to data file , used by write_check_point
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return common::OB_SUCCESS;
  }
  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return common::OB_SUCCESS;
  }
  virtual int get_serialize_size()
  {
    return 0;
  }
  virtual int init(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, ObBaseStorage* base_storage,
      clog::ObIPartitionLogService* pls)
  {
    UNUSED(key);
    UNUSED(cp_fty);
    UNUSED(schema_service);
    UNUSED(base_storage);
    UNUSED(pls);
    return common::OB_SUCCESS;
  }
  virtual int get_saved_storage_info(ObSavedStorageInfo& info)
  {
    UNUSED(info);
    return common::OB_SUCCESS;
  }
  virtual int get_role(oceanbase::common::ObRole& role) const
  {
    UNUSED(role);
    return common::OB_SUCCESS;
  }
  virtual int get_leader_curr_member_list(oceanbase::common::ObMemberList& list) const
  {
    UNUSED(list);
    return common::OB_SUCCESS;
  }
  virtual int get_leader(oceanbase::common::ObAddr& leader) const
  {
    UNUSED(leader);
    return common::OB_SUCCESS;
  }
  virtual int set_frozen_version(int64_t a, int64_t b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int set_freeze_status(int64_t a, int64_t b, int64_t c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int64_t get_frozen_version() const
  {
    return 0;
  }
  virtual int64_t get_frozen_timestamp() const
  {
    return 0;
  }
  virtual int64_t get_freeze_status() const
  {
    return 0;
  }
  virtual int get_merge_priority_info(oceanbase::memtable::ObMergePriorityInfo& a) const
  {
    UNUSED(a);
    return common::OB_SUCCESS;
  }

private:
  uint64_t tenant_id_;
  ObReplayStatus replay_status_;
  common::ObPartitionKey partition_key_;
};

class MockPartitionService : public storage::MockObIPartitionService {
public:
  MockPartitionService()
  {}
  virtual ~MockPartitionService()
  {}

public:
  void reset()
  {
    mock_partition_.reset();
  }

  virtual int get_partition(const common::ObPartitionKey& partition_key, storage::ObIPartitionGroup*& partition)
  {
    int ret = common::OB_SUCCESS;
    if (partition_key == mock_partition_.get_partition_key()) {
      partition = &mock_partition_;
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  virtual int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    int ret = common::OB_SUCCESS;
    storage::ObIPartitionGroup* partition = NULL;
    if (pkey == mock_partition_.get_partition_key()) {
      partition = const_cast<MockPartition*>(&mock_partition_);
      guard.set_partition_group(this->get_pg_mgr(), *partition);
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  virtual void revert_replay_status(ObReplayStatus* replay_status) const
  {
    UNUSED(replay_status);
  }
  virtual int init(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
      ObIPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
      share::ObIPartitionLocationCache* location_cache, oceanbase::share::ObRsMgr* rs_mgr, ObIPartitionReport* rs_cb,
      rpc::frame::ObReqTransport* req_transport)
  {
    UNUSED(env);
    UNUSED(self_addr);
    UNUSED(cp_fty);
    UNUSED(schema_service);
    UNUSED(location_cache);
    UNUSED(rs_mgr);
    UNUSED(rs_cb);
    UNUSED(req_transport);
    return common::OB_SUCCESS;
  }
  int init_partition(replayengine::ObLogReplayEngine* rp_eg)
  {
    return mock_partition_.set_replay_engine(rp_eg);
  }
  virtual bool is_empty() const
  {
    return true;
  }
  virtual transaction::ObTransService* get_trans_service()
  {
    return NULL;
  }
  virtual clog::ObICLogMgr* get_clog_mgr()
  {
    return NULL;
  }
  virtual int start()
  {
    // share::ObThreadPool::start();
    return common::OB_SUCCESS;
  }
  virtual int destroy()
  {
    // share::ObThreadPool::stop();
    return common::OB_SUCCESS;
  }
  //
  virtual int load_partition(const char* buf, const int64_t buf_len, int64_t& pos)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return common::OB_SUCCESS;
  }
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos)
  {
    UNUSED(log_seq_num);
    UNUSED(subcmd);
    UNUSED(buf);
    UNUSED(len);
    UNUSED(pos);
    return common::OB_SUCCESS;
  }
  virtual bool is_take_over_done(const common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return true;
  }
  virtual int create_partition(const common::ObPartitionKey& key, const common::ObVersion data_version,
      const int64_t replica_num, const common::ObMemberList& mem_list)
  {
    UNUSED(key);
    UNUSED(data_version);
    UNUSED(replica_num);
    UNUSED(mem_list);
    return common::OB_SUCCESS;
  }
  virtual int migrate_partition(const common::ObPartitionKey& key, const common::ObAddr& src, const common::ObAddr& dst,
      const bool keep_src, const int64_t offline_ts)
  {
    UNUSED(key);
    UNUSED(src);
    UNUSED(dst);
    UNUSED(keep_src);
    UNUSED(offline_ts);
    return OB_SUCCESS;
  }
  virtual int online_partition()
  {
    return common::OB_SUCCESS;
  }
  virtual int offline_partition(const common::ObPartitionKey& leader_key, const common::ObAddr& server)
  {
    UNUSED(leader_key);
    UNUSED(server);
    return common::OB_SUCCESS;
  }
  virtual int remove_partition(const common::ObPartitionKey& key)
  {
    UNUSED(key);
    return common::OB_SUCCESS;
  }
  virtual int remove_orphans()
  {
    return common::OB_SUCCESS;
  }
  virtual int freeze()
  {
    return common::OB_SUCCESS;
  }
  virtual int on_leader_revoke(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return common::OB_SUCCESS;
  }
  virtual int on_leader_takeover(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return common::OB_SUCCESS;
  }
  virtual int on_member_change_success(const oceanbase::common::ObPartitionKey& a, int64_t b,
      const oceanbase::common::ObMemberList& c, const oceanbase::common::ObMemberList& d)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    return common::OB_SUCCESS;
  }
  virtual int64_t get_min_using_file_id() const
  {
    return 0;
  }

  // ==========================================================================
  // replay engin interfaces
  virtual int start_replay(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return common::OB_SUCCESS;
  }
  virtual int submit_replay_task(
      const common::ObPartitionKey& partition_key, const char* log_buf, const int64_t buf_len)
  {
    UNUSED(partition_key);
    UNUSED(log_buf);
    UNUSED(buf_len);
    return common::OB_SUCCESS;
  }
  virtual int finish_replay(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return common::OB_SUCCESS;
  }

  // ==========================================================================
  virtual int table_scan(ObTableScanParam& param, common::ObNewRowIterator*& result)
  {
    UNUSED(param);
    UNUSED(result);
    return common::OB_SUCCESS;
  }
  virtual int revert_scan_iter(common::ObNewRowIterator* iter)
  {
    UNUSED(iter);
    return common::OB_SUCCESS;
  }
  virtual int delete_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const uint64_t table_id,
      const int32_t partition_idx, const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter,
      int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(timeout);
    UNUSED(table_id);
    UNUSED(partition_idx);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }
  virtual int insert_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const uint64_t table_id,
      const int32_t partition_idx, const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter,
      int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(timeout);
    UNUSED(table_id);
    UNUSED(partition_idx);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }
  virtual int update_rows(const transaction::ObTransDesc& trans_desc, const int64_t timeout, const uint64_t table_id,
      const int32_t partition_idx, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(timeout);
    UNUSED(table_id);
    UNUSED(partition_idx);
    UNUSED(column_ids);
    UNUSED(updated_column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return common::OB_SUCCESS;
  }

  // ==========================================================================
  // partition manager interfaces
  virtual int get_all_partitions(common::ObIArray<ObIPartitionGroup*>& partition_list)
  {
    UNUSED(partition_list);
    return common::OB_SUCCESS;
  }

  // ==========================================================================
  // replay interfaces
  virtual int replay(
      const common::ObPartitionKey& partition_key, const ObStoreCtx& ctx, const char* data, const int64_t data_len)
  {
    UNUSED(partition_key);
    UNUSED(ctx);
    UNUSED(data);
    UNUSED(data_len);
    return common::OB_SUCCESS;
  }
  virtual int lock(const common::ObPartitionKey& partition_key, const ObStoreCtx& ctx)
  {
    UNUSED(partition_key);
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }
  virtual int init(const blocksstable::ObStorageEnv& env, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service)
  {
    UNUSED(env);
    UNUSED(cp_fty);
    UNUSED(schema_service);
    return common::OB_SUCCESS;
  }
  virtual int get_role(const oceanbase::common::ObPartitionKey& key, oceanbase::common::ObRole& role) const
  {
    UNUSED(key);
    UNUSED(role);
    return common::OB_SUCCESS;
  }
  virtual int get_leader_curr_member_list(
      const oceanbase::common::ObPartitionKey& key, oceanbase::common::ObMemberList& list) const
  {
    UNUSED(key);
    UNUSED(list);
    return common::OB_SUCCESS;
  }
  virtual int delete_rows(const oceanbase::transaction::ObTransDesc& a, int64_t b,
      const oceanbase::common::ObPartitionKey& c, const oceanbase::common::ObIArray<long unsigned int>& d,
      oceanbase::common::ObNewRowIterator* e, int64_t& f)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    UNUSED(e);
    UNUSED(f);
    return common::OB_SUCCESS;
  }
  virtual int insert_rows(const oceanbase::transaction::ObTransDesc& a, int64_t b,
      const oceanbase::common::ObPartitionKey& c, const oceanbase::common::ObIArray<long unsigned int>& d,
      oceanbase::common::ObNewRowIterator* e, int64_t& f)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    UNUSED(e);
    UNUSED(f);
    return common::OB_SUCCESS;
  }
  virtual int update_rows(const oceanbase::transaction::ObTransDesc& a, int64_t b,
      const oceanbase::common::ObPartitionKey& c, const oceanbase::common::ObIArray<long unsigned int>& d,
      const oceanbase::common::ObIArray<long unsigned int>& e, oceanbase::common::ObNewRowIterator* f, int64_t& g)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    UNUSED(e);
    UNUSED(f);
    UNUSED(g);
    return common::OB_SUCCESS;
  }
  virtual int lock_rows(const oceanbase::transaction::ObTransDesc& a, int64_t b,
      const oceanbase::common::ObPartitionKey& c, oceanbase::common::ObNewRowIterator* d,
      oceanbase::storage::ObLockFlag e, int64_t& f)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    UNUSED(e);
    UNUSED(f);
    return common::OB_SUCCESS;
  }
  virtual int lock_rows(const oceanbase::transaction::ObTransDesc& a, int64_t b,
      const oceanbase::common::ObPartitionKey& c, const oceanbase::common::ObNewRow& d,
      oceanbase::storage::ObLockFlag e)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    UNUSED(e);
    return common::OB_SUCCESS;
  }
  virtual int replay_redo_log(const oceanbase::common::ObPartitionKey& a, const oceanbase::storage::ObStoreCtx& b,
      const int64_t ts, const char* c, int64_t d)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(ts);
    UNUSED(c);
    UNUSED(d);
    return common::OB_SUCCESS;
  }
  virtual int prepare_freeze(const oceanbase::obrpc::ObPartitionList& a, int64_t b, int64_t c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int commit_freeze(const oceanbase::obrpc::ObPartitionList& a, int64_t b, int64_t c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int abort_freeze(const oceanbase::obrpc::ObPartitionList& a, int64_t b, int64_t c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int set_freeze_status(const oceanbase::common::ObPartitionKey& a, int64_t b, int64_t c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int get_freeze_status(const oceanbase::common::ObPartitionKey& a, int64_t& b, int64_t& c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int get_freeze_status(const oceanbase::common::ObPartitionKey& a, int64_t& b, int64_t& c, int64_t& d)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    return common::OB_SUCCESS;
  }

  void run1()
  {
    while (true) {
      REPLAY_LOG(INFO, "partition service run");
      usleep(10000);
    }
  }

  virtual int replay(const oceanbase::common::ObPartitionKey& a, const char* b, int64_t c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);

    int64_t log_type_val = -1;
    ObStorageLogType log_type;
    int64_t pos = 0;
    serialization::decode_i64(b, 1024, pos, &log_type_val);
    log_type = static_cast<ObStorageLogType>(log_type_val);
    REPLAY_LOG(INFO, "partition service replaying", K(log_type));
    switch (log_type) {
      case OB_LOG_MAJOR_FREEZE: {
        // FIXME set frozen_version later
        break;
      }
      default:
        OB_ASSERT(false);
    }
    return common::OB_SUCCESS;
  }
  virtual int leader_freeze_success(const oceanbase::common::ObPartitionKey& a, int64_t b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int leader_freeze_fail(const oceanbase::common::ObPartitionKey& a, int64_t b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int follower_freeze_success(const oceanbase::common::ObPartitionKey& a, int64_t b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int follower_freeze_fail(const oceanbase::common::ObPartitionKey& a, int64_t b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int submit_freeze_log_success(int64_t a, const oceanbase::common::ObPartitionKey& b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int get_active_memtable_version(const oceanbase::common::ObPartitionKey& a, int64_t& b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }

  virtual int get_status(const common::ObPartitionKey& partition, int64_t& status)
  {
    UNUSED(partition);
    UNUSED(status);
    return OB_SUCCESS;
  }

private:
  MockPartition mock_partition_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_MOCK_PARTITION_SERVICE_H_
