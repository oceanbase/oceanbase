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

#ifndef OCEANBASE_TRANSACTION_MOCK_OB_TRANS_SERVICE_H_
#define OCEANBASE_TRANSACTION_MOCK_OB_TRANS_SERVICE_H_

#include "storage/transaction/ob_trans_service.h"
#include <stdlib.h>
#include <time.h>
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "common/ob_partition_key.h"

#include "storage/memtable/ob_memtable_interface.h"

#include "storage/transaction/ob_trans_define.h"
#include "storage/transaction/ob_trans_timer.h"
#include "storage/transaction/ob_location_adapter.h"
#include "storage/transaction/ob_clog_adapter.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "storage/transaction/ob_trans_rpc.h"
#include "storage/transaction/ob_ts_mgr.h"

namespace oceanbase {

namespace obrpc {
class ObTransRpcProxy;
class ObTransRpcResult;
}  // namespace obrpc

namespace common {
class ObAddr;
class ObPartitionKey;
}  // namespace common

namespace share {
class ObIPartitionLocationCache;
}

namespace storage {
class ObPartitionService;
class ObStoreCtx;
class ObIFreezeCb;
class LeaderActiveArg;
}  // namespace storage

namespace memtable {
class ObIMemtableCtx;
class ObIMemtableCtxFactory;
}  // namespace memtable

namespace transaction {

class KillTransArg;

class MockObTsMgr : public ObITsMgr {
public:
  MockObTsMgr(ObLtsSource& source) : source_(source)
  {}
  virtual ~MockObTsMgr()
  {}

public:
  virtual int update_gts(const uint64_t tenant_id, const int64_t gts, bool& update)
  {
    UNUSED(tenant_id);
    return source_.update_gts(gts, update);
  }
  virtual int update_local_trans_version(const uint64_t tenant_id, const int64_t gts, bool& update)
  {
    UNUSED(tenant_id);
    return source_.update_local_trans_version(gts, update);
  }
  virtual int get_gts(
      const uint64_t tenant_id, const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
  {
    UNUSED(tenant_id);
    return source_.get_gts(stc, task, gts, receive_gts_ts);
  }
  virtual int get_gts(const uint64_t tenant_id, ObTsCbTask* task, int64_t& gts)
  {
    UNUSED(tenant_id);
    return source_.get_gts(task, gts);
  }
  virtual int get_ts_sync(const uint64_t tenant_id, const int64_t timeout_us, int64_t& ts, bool& is_external_consistent)
  {
    UNUSED(tenant_id);
    UNUSED(timeout_us);
    source_.get_gts(NULL, ts);
    is_external_consistent = false;
    return common::OB_SUCCESS;
  }
  virtual int get_local_trans_version(
      const uint64_t tenant_id, const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
  {
    UNUSED(tenant_id);
    UNUSED(stc);
    UNUSED(task);
    UNUSED(gts);
    UNUSED(receive_gts_ts);
    return common::OB_SUCCESS;
  }
  virtual int get_local_trans_version(const uint64_t tenant_id, ObTsCbTask* task, int64_t& gts)
  {
    UNUSED(tenant_id);
    UNUSED(task);
    UNUSED(gts);
    return common::OB_SUCCESS;
  }
  virtual int wait_gts_elapse(const uint64_t tenant_id, const int64_t ts, ObTsCbTask* task, bool& need_wait)
  {
    UNUSED(tenant_id);
    return source_.wait_gts_elapse(ts, task, need_wait);
  }
  virtual int wait_gts_elapse(const uint64_t tenant_id, const int64_t ts)
  {
    UNUSED(tenant_id);
    return source_.wait_gts_elapse(ts);
  }
  virtual int refresh_gts(const uint64_t tenant_id, const bool need_refresh)
  {
    UNUSED(tenant_id);
    return source_.refresh_gts(need_refresh);
  }
  virtual int update_base_ts(const int64_t base_ts, const int64_t publish_version)
  {
    return source_.update_base_ts(base_ts, publish_version);
  }
  virtual int get_base_ts(int64_t& base_ts, int64_t& publish_version)
  {
    return source_.get_base_ts(base_ts, publish_version);
  }
  virtual bool is_external_consistent(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return source_.is_external_consistent();
  }
  virtual int update_publish_version(const uint64_t tenant_id, const int64_t publish_version, const bool for_replay)
  {
    UNUSED(tenant_id);
    UNUSED(for_replay);
    return source_.update_publish_version(publish_version);
  }
  virtual int get_publish_version(const uint64_t tenant_id, int64_t& publish_version)
  {
    UNUSED(tenant_id);
    return source_.get_publish_version(publish_version);
  }
  virtual int get_gts_and_type(const uint64_t tenant_id, const MonotonicTs stc, int64_t& gts, int64_t& ts_type)
  {
    UNUSED(tenant_id);
    UNUSED(ts_type);
    MonotonicTs unused;
    return source_.get_gts(stc, NULL, gts, unused);
  }

private:
  ObLtsSource& source_;
};

class MockObTransService : public ObTransService {
public:
  memtable::ObMemtableCtxFactory mt_ctx_factory_def_;
  memtable::ObMemtableCtxFactory* mt_ctx_factory_;
  memtable::ObIMemtableCtx* mt_ctx_;
  MockObTransService()
  {
    mt_ctx_factory_ = &mt_ctx_factory_def_;
    mt_ctx_ = mt_ctx_factory_->alloc();
    mt_ctx_->trans_begin();
    mt_ctx_->sub_trans_begin(1, 100000000 + ObTimeUtility::current_time());
  }
  virtual ~MockObTransService()
  {}
  virtual int init(const common::ObAddr& self, share::ObIPartitionLocationCache* location_cache,
      storage::ObPartitionService* partition_service, obrpc::ObTransRpcProxy* rpc_proxy,
      obrpc::ObDupTableRpcProxy* dup_table_rpc_proxy, share::schema::ObMultiVersionSchemaService* schema_service,
      ObITsMgr* ts_mgr, share::ObAliveServerTracer& server_tracer)
  {
    UNUSED(self);
    UNUSED(location_cache);
    UNUSED(partition_service);
    UNUSED(schema_service);
    UNUSED(rpc_proxy);
    UNUSED(dup_table_rpc_proxy);
    UNUSED(ts_mgr);
    UNUSED(server_tracer);
    return OB_SUCCESS;
  }

  virtual int init(const common::ObAddr& self, share::ObIPartitionLocationCache* location_cache,
      storage::ObPartitionService* partition_service, obrpc::ObTransRpcProxy* rpc_proxy,
      share::schema::ObMultiVersionSchemaService* schema_service, const common::ObVersion& freeze_version,
      ObITsMgr* ts_mgr)
  {
    UNUSED(self);
    UNUSED(location_cache);
    UNUSED(partition_service);
    UNUSED(schema_service);
    UNUSED(rpc_proxy);
    UNUSED(freeze_version);
    UNUSED(ts_mgr);
    return OB_SUCCESS;
  }

  // just for test
  virtual int init(const ObAddr& self, ObITransRpc* rpc, ObILocationAdapter* location_adapter,
      ObIClogAdapter* clog_adapter, storage::ObPartitionService* partition_service,
      share::schema::ObMultiVersionSchemaService* schema_service, ObITsMgr* ts_mgr)
  {
    UNUSED(self);
    UNUSED(rpc);
    UNUSED(location_adapter);
    UNUSED(clog_adapter);
    UNUSED(partition_service);
    UNUSED(schema_service);
    UNUSED(ts_mgr);
    return OB_SUCCESS;
  }

  virtual int init(const ObAddr& self, ObITransRpc* rpc, ObILocationAdapter* location_adapter,
      ObIClogAdapter* clog_adapter, storage::ObPartitionService* partition_service,
      share::schema::ObMultiVersionSchemaService* schema_service, const common::ObVersion& freeze_version,
      ObITsMgr* ts_mgr)
  {
    UNUSED(self);
    UNUSED(rpc);
    UNUSED(location_adapter);
    UNUSED(clog_adapter);
    UNUSED(partition_service);
    UNUSED(schema_service);
    UNUSED(freeze_version);
    UNUSED(ts_mgr);
    return OB_SUCCESS;
  }

  virtual int start()
  {
    return OB_SUCCESS;
  }
  virtual void stop()
  {}
  virtual void wait()
  {}
  virtual void destroy()
  {}

  virtual int freeze(const common::ObVersion& freeze_version, const bool force, const int64_t start_timestamp)
  {
    UNUSED(freeze_version);
    UNUSED(force);
    UNUSED(start_timestamp);
    return OB_SUCCESS;
  }

  virtual int pre_prepare_freeze(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }
  virtual bool can_prepare_freeze(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }
  virtual int prepare_freeze(const common::ObPartitionKey& partition, const common::ObVersion& freeze_version,
      const int64_t timestamp, storage::ObIFreezeCb* freeze_cb)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    UNUSED(timestamp);
    UNUSED(freeze_cb);
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int commit_freeze(const common::ObPartitionKey& partition, const common::ObVersion& freeze_version,
      const int64_t timestamp, storage::ObIFreezeCb* freeze_cb)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    UNUSED(timestamp);
    UNUSED(freeze_cb);
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int abort_freeze(const common::ObPartitionKey& partition, const common::ObVersion& freeze_version,
      const int64_t timestamp, storage::ObIFreezeCb* freeze_cb)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    UNUSED(timestamp);
    UNUSED(freeze_cb);
    int ret = OB_SUCCESS;
    return ret;
  }

  virtual int check_scheduler_status(const ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  virtual ObITsMgr* get_ts_mgr()
  {
    return ts_mgr;
  }

  virtual int prepare_freeze_sync(
      const common::ObPartitionKey& partition, const common::ObVersion& freeze_version, const int64_t timestamp)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    UNUSED(timestamp);
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int commit_freeze_sync(
      const common::ObPartitionKey& partition, const common::ObVersion& freeze_version, const int64_t timestamp)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    UNUSED(timestamp);
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int abort_freeze_sync(
      const common::ObPartitionKey& partition, const common::ObVersion& freeze_version, const int64_t timestamp)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    UNUSED(timestamp);
    int ret = OB_SUCCESS;
    return ret;
  }

  virtual share::ObAliveServerTracer* get_server_tracer()
  {
    return NULL;
  }

  int fetch_trans_ctx_by_ctx_id(const uint32_t ctx_id, ObTransCtx*& ctx)
  {
    UNUSED(ctx_id);
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  void revert_trans_ctx_by_ctx_id(const uint32_t ctx_id)
  {
    UNUSED(ctx_id);
  }

  int replay_start_working_log(const common::ObPartitionKey& pkey, const int64_t timestamp, const uint64_t log_id)
  {
    UNUSED(pkey);
    UNUSED(timestamp);
    UNUSED(log_id);
    return OB_SUCCESS;
  }
  virtual int has_terminated_trx_in_given_log_id_range(
      const ObPartitionKey& pkey, const uint64_t start_log_id, const uint64_t end_log_id, bool& has_terminated_trx)
  {
    UNUSEDx(pkey, start_log_id, end_log_id);
    has_terminated_trx = true;
    return OB_SUCCESS;
  }

public:
  virtual memtable::ObIMemtableCtxFactory* get_mem_ctx_factory()
  {
    return mt_ctx_factory_;
  }
  virtual int kill_query_session(const transaction::ObTransDesc& trans_desc, const int status)
  {
    UNUSED(trans_desc);
    UNUSED(status);
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
  virtual int check_ctx_create_timestamp_elapsed(const ObPartitionKey& partition, const int64_t ts)
  {
    UNUSED(partition);
    UNUSED(ts);
    return OB_SUCCESS;
  }
  // interfaces for SQL
  virtual int start_trans(
      const uint64_t tenant_id, const uint64_t thread_id, const ObStartTransParam& req, ObTransDesc& trans_desc)
  {
    UNUSED(tenant_id);
    UNUSED(thread_id);
    UNUSED(req);
    UNUSED(trans_desc);
    return OB_SUCCESS;
  }

  virtual int start_trans(const uint64_t trans_, const uint64_t thread_id, const ObStartTransParam& req,
      const int64_t expired_time, const uint32_t session_id, const uint64_t proxy_session_id, ObTransDesc& trans_desc)
  {
    UNUSED(trans_);
    UNUSED(thread_id);
    UNUSED(req);
    UNUSED(expired_time);
    UNUSED(session_id);
    UNUSED(proxy_session_id);
    UNUSED(trans_desc);
    return OB_SUCCESS;
  }

  virtual int end_trans(bool is_rollback, ObTransDesc& trans_desc, const int64_t stmt_expired_time)
  {
    UNUSED(is_rollback);
    UNUSED(trans_desc);
    UNUSED(stmt_expired_time);
    return OB_SUCCESS;
  }

  virtual int end_trans(
      bool is_rollback, ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb, const int64_t stmt_expired_time)
  {
    UNUSED(is_rollback);
    UNUSED(trans_desc);
    UNUSED(cb);
    UNUSED(stmt_expired_time);
    return OB_SUCCESS;
  }

  virtual int start_replay()
  {
    return OB_SUCCESS;
  }
  virtual int submit_log()
  {
    return OB_SUCCESS;
  }
  virtual int end_replay()
  {
    return OB_SUCCESS;
  }

  virtual int start_stmt(const ObStmtParam& stmt_param, ObTransDesc& trans_desc,
      const common::ObPartitionLeaderArray& pla, common::ObPartitionArray& participants)
  {
    UNUSED(stmt_param);
    UNUSED(trans_desc);
    UNUSED(pla);
    UNUSED(participants);
    return OB_SUCCESS;
  }

  virtual int extend_stmt(ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla)
  {
    UNUSED(trans_desc);
    UNUSED(pla);
    return OB_SUCCESS;
  }
  virtual int end_stmt(bool is_rollback, bool is_incomplete, const ObPartitionArray& cur_stmt_all_participants,
      const ObPartitionEpochArray& epoch_arr, const ObPartitionArray& discard_participants,
      const ObPartitionLeaderArray& pla, ObTransDesc& trans_desc)
  {
    UNUSED(is_rollback);
    UNUSED(is_incomplete);
    UNUSED(cur_stmt_all_participants);
    UNUSED(epoch_arr);
    UNUSED(discard_participants);
    UNUSED(pla);
    UNUSED(trans_desc);
    return OB_SUCCESS;
  }

  virtual int start_participant(const ObTransDesc& trans_desc, const ObPartitionArray& participants,
      ObPartitionEpochArray& partition_epoch_arr, storage::ObIPartitionArrayGuard& pkey_guard_arr)
  {
    UNUSED(trans_desc);
    UNUSED(participants);
    UNUSED(partition_epoch_arr);
    UNUSED(pkey_guard_arr);
    return OB_SUCCESS;
  }
  virtual int end_participant(
      bool is_rollback, const ObTransDesc& trans_desc, const common::ObPartitionArray& participants)
  {
    UNUSED(is_rollback);
    UNUSED(trans_desc);
    UNUSED(participants);
    return OB_SUCCESS;
  }
  // interfaces for clog
  virtual int replay(const common::ObPartitionKey& partition, const char* log, const int64_t size,
      const int64_t trans_version, const uint64_t log_id, const int64_t safe_slave_read_timestamp,
      const bool batch_committed, const int64_t checkpoint)
  {
    UNUSED(partition);
    UNUSED(log);
    UNUSED(size);
    UNUSED(trans_version);
    UNUSED(log_id);
    UNUSED(safe_slave_read_timestamp);
    UNUSED(batch_committed);
    UNUSED(checkpoint);
    return OB_SUCCESS;
  }
  // interface for rpc
  virtual int handle_trans_msg(const ObTransMsg& msg, obrpc::ObTransRpcResult& result)
  {
    UNUSED(msg);
    UNUSED(result);
    return OB_SUCCESS;
  }
  virtual int handle_trans_response(const ObTransMsg& msg)
  {
    UNUSED(msg);
    return OB_SUCCESS;
  }
  virtual int handle_trans_msg_callback(const ObPartitionKey& partition, const ObTransID& trans_id,
      const int64_t msg_type, const int status, const ObAddr& addr, const int64_t sql_no)
  {
    UNUSED(partition);
    UNUSED(trans_id);
    UNUSED(msg_type);
    UNUSED(status);
    UNUSED(addr);
    UNUSED(sql_no);
    return OB_SUCCESS;
  }

  virtual int handle_packet(const int64_t pcode, ObDataBuffer& in_buff, ObDataBuffer& out_buff)
  {
    UNUSED(pcode);
    UNUSED(in_buff);
    UNUSED(out_buff);
    return OB_SUCCESS;
  }
  // refresh location cache
  virtual int refresh_location_cache(const common::ObPartitionKey& partition, const bool need_clear_cache)
  {
    UNUSED(partition);
    UNUSED(need_clear_cache);
    return OB_SUCCESS;
  }
  // leader revoke
  virtual int leader_revoke(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }
  virtual int clear_all_ctx(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }
  // leader takeover
  virtual int leader_takeover(const common::ObPartitionKey& partition,
      const storage::LeaderActiveArg& leader_active_arg, const int64_t slave_ts)
  {
    UNUSED(partition);
    UNUSED(leader_active_arg);
    UNUSED(slave_ts);
    return OB_SUCCESS;
  }
  // leader active
  virtual int leader_active(const common::ObPartitionKey& partition, const storage::LeaderActiveArg& leader_active_arg)
  {
    UNUSED(partition);
    UNUSED(leader_active_arg);
    return OB_SUCCESS;
  }
  /*
  virtual int add_partition(const common::ObPartitionKey &partition,
      const common::ObVersion freeze_version)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    return OB_SUCCESS;
  }*/

  virtual int add_partition(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }

  virtual int remove_partition(const common::ObPartitionKey& partition, const bool graceful)
  {
    UNUSED(partition);
    UNUSED(graceful);
    return OB_SUCCESS;
  }

  virtual int block_partition(const common::ObPartitionKey& partition, bool& is_all_trans_clear)
  {
    UNUSED(partition);
    UNUSED(is_all_trans_clear);
    return OB_SUCCESS;
  }

  // ugly...
  virtual int get_store_ctx(const ObTransDesc& trans_desc, const common::ObPartitionKey& partition,
      storage::ObStoreCtx& store_ctx, const int64_t user_specified_snapshot)
  {
    UNUSED(trans_desc);
    UNUSED(partition);
    UNUSED(store_ctx);
    UNUSED(user_specified_snapshot);
    store_ctx.mem_ctx_ = mt_ctx_;
    return OB_SUCCESS;
  }
  virtual int revert_store_ctx(
      const ObTransDesc& trans_desc, const common::ObPartitionKey& partition, storage::ObStoreCtx& store_ctx)
  {
    UNUSED(trans_desc);
    UNUSED(partition);
    UNUSED(store_ctx);
    return OB_SUCCESS;
  }
  virtual int get_publish_version(const common::ObPartitionKey& pkey, int64_t& publish_version)
  {
    UNUSED(pkey);
    UNUSED(publish_version);
    return OB_SUCCESS;
  }
  virtual int update_publish_version(const common::ObPartitionKey& partition, const int64_t publish_version)
  {
    UNUSED(partition);
    UNUSED(publish_version);
    return OB_SUCCESS;
  }
  virtual int kill_all_trans(const common::ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear)
  {
    UNUSED(partition);
    UNUSED(arg);
    UNUSED(is_all_trans_clear);
    return OB_SUCCESS;
  }
  virtual int wait_all_trans_clear(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }
  virtual int get_max_trans_version(const common::ObPartitionKey& pkey, int64_t& max_trans_version)
  {
    UNUSED(pkey);
    UNUSED(max_trans_version);
    return OB_SUCCESS;
  }
  virtual int update_publish_version(
      const common::ObPartitionKey& partition, const int64_t publish_version, const bool for_replay)
  {
    UNUSED(partition);
    UNUSED(publish_version);
    UNUSED(for_replay);
    return OB_SUCCESS;
  }

  virtual int checkpoint(const common::ObPartitionKey& partition, const int64_t checkpoint)
  {
    UNUSED(partition);
    UNUSED(checkpoint);
    return OB_SUCCESS;
  }

  virtual int relocate_data(const ObPartitionKey& partition, memtable::ObIMemtable* memtable)
  {
    UNUSED(partition);
    UNUSED(memtable);
    return OB_SUCCESS;
  }

  virtual int handle_coordinator_orphan_msg(const ObTransMsg& msg, ObTransMsg& ret_msg)
  {
    UNUSED(msg);
    UNUSED(ret_msg);
    return OB_SUCCESS;
  }

  int get_min_uncommit_prepare_version(const common::ObPartitionKey& partition, int64_t& min_prepare_version)
  {
    UNUSED(partition);
    UNUSED(min_prepare_version);
    return OB_SUCCESS;
  }
  int get_min_flying_id(const common::ObPartitionKey& partition, int64_t& min_flying_id)
  {
    UNUSED(partition);
    UNUSED(min_flying_id);
    return OB_SUCCESS;
  }
  int wait_freeze(
      const ObPartitionKey& partition, const common::ObVersion& memstore_version, const int64_t expired_time)
  {
    UNUSED(partition);
    UNUSED(memstore_version);
    UNUSED(expired_time);
    return OB_SUCCESS;
  }
  int get_active_memstore_version(ObVersion& version)
  {
    UNUSED(version);
    return OB_SUCCESS;
  }
  int64_t get_freeze_version() const
  {
    return 0;
  }
  int set_memstore_version(const common::ObPartitionKey& partition, const ObVersion& freeze_version)
  {
    UNUSED(partition);
    UNUSED(freeze_version);
    return OB_SUCCESS;
  }
  int inactive_tenant(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
  int get_min_uncommit_log(const ObPartitionKey& pkey, uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts)
  {
    UNUSED(pkey);
    UNUSED(min_uncommit_log_id);
    UNUSED(min_uncommit_log_ts);
    return OB_SUCCESS;
  }
  int register_prepare_changing_leader_task(
      const ObPartitionKey& partition, const ObAddr& proposal_leader, const int64_t ts)
  {
    UNUSED(partition);
    UNUSED(proposal_leader);
    UNUSED(ts);
    return OB_SUCCESS;
  }
  int prepare_changing_leader(const ObPartitionKey& partition, const ObAddr& proposal_leader)
  {
    UNUSED(partition);
    UNUSED(proposal_leader);
    return OB_SUCCESS;
  }
  int get_partition_audit_info(const common::ObPartitionKey& pkey, ObPartitionAuditInfo& info)
  {
    UNUSED(pkey);
    UNUSED(info);
    return OB_SUCCESS;
  }
  int set_partition_audit_base_row_count(const ObPartitionKey& pkey, const int64_t count)
  {
    UNUSED(pkey);
    UNUSED(count);
    return OB_SUCCESS;
  }
  int update_dup_table_partition_info(const ObPartitionKey& pkey, const bool is_duplicated)
  {
    UNUSED(pkey);
    UNUSED(is_duplicated);
    return OB_SUCCESS;
  }
  int send_dup_table_lease_request_msg(const ObPartitionKey& pkey, ObDupTableLeaseTask* task)
  {
    UNUSED(pkey);
    UNUSED(task);
    return OB_SUCCESS;
  }

private:
  ObLtsSource lts_source;
  MockObTsMgr* ts_mgr;
};

}  // namespace transaction
}  // namespace oceanbase

#endif
