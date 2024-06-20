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

#ifndef OCEAANBASE_TRANSACTION_XA_SERVICE_
#define OCEAANBASE_TRANSACTION_XA_SERVICE_

//#include "ob_trans_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_xa_ctx_mgr.h"
#include "ob_xa_rpc.h"
#include "ob_xa_define.h"
#include "ob_xa_inner_table_gc_worker.h"
#include "ob_xa_trans_heartbeat_worker.h"
#include "ob_xa_trans_event.h"

namespace oceanbase
{

namespace common
{

class ObISQLClient;

}

namespace obrpc
{

class ObXARpcProxy;

}

namespace transaction
{

class ObTransService;
class ObXATransID;
class ObStmtParam;

struct ObXACacheItem {
public:
  static const int64_t XA_CACHE_ITEM_EXPIRE_TIME = 2000000; // 2s
  ObXACacheItem(): state_(ObXATransState::UNKNOWN), create_timestamp_(0) {}
  ~ObXACacheItem() { reset(); }
  void reset()
  {
    state_ = ObXATransState::UNKNOWN;
    create_timestamp_ = 0;
    xid_.reset();
  }

  bool is_valid_to_query(ObXATransID xid)
  {
    return this->xid_.all_equal_to(xid);
  }

  bool is_valid_to_set()
  {
    return 0 == this->create_timestamp_ ||
           this->create_timestamp_ + XA_CACHE_ITEM_EXPIRE_TIME < ObTimeUtility::current_time();
  }

public:
  ObSpinLock lock_;
  ObXATransID xid_;
  int64_t state_;
  int64_t create_timestamp_;
  TO_STRING_KV(K(xid_), K(state_), K(create_timestamp_));
};

class ObXACache {
public:
  static const uint64_t XA_PREPARE_CACHE_COUNT = 8000;
  int query_prepare_cache_item(const ObXATransID &xid, int64_t &state);
  void insert_prepare_cache_item(const ObXATransID &xid, int64_t state);
  void clean_prepare_cache_item(const ObXATransID &xid);
private:
  void clean_prepare_cache_item_(ObXACacheItem &item);
private:
  ObXACacheItem xa_prepare_cache_[XA_PREPARE_CACHE_COUNT];
};

class ObXAService
{
public:
  ObXAService()
    : xa_ctx_mgr_(), xa_proxy_(), xa_rpc_(), timer_(), xa_trans_heartbeat_worker_(),
      xa_inner_table_gc_worker_(), is_running_(false), is_inited_(false),
      xa_statistics_v2_(), dblink_statistics_() {}
  virtual ~ObXAService() { destroy(); }
  int init(const ObAddr &self_addr,
           rpc::frame::ObReqTransport *req_transport);
  static int mtl_init(ObXAService* &xa_service);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  void try_print_statistics();
public:
  int xa_start(const ObXATransID &xid,
              const int64_t flags,
              const int64_t timeout_seconds,
              const uint32_t session_id,
              const ObTxParam &tx_param,
              ObTxDesc *&tx_desc,
              const uint64_t data_version);
  int xa_end(const ObXATransID &xid,
             const int64_t flags,
             ObTxDesc *&tx_desc);
  int xa_commit(const ObXATransID &xid,
                const int64_t flags,
                const int64_t xa_timeout_seconds,
                bool &has_tx_level_temp_table,
                ObTransID &tx_id);
  int xa_rollback(const ObXATransID &xid,
                  const int64_t xa_timeout_seconds,
                  ObTransID &tx_id);
  int xa_rollback_local(const ObXATransID &xid,
                        const ObTransID &tx_id,
                        const int64_t timeout_us,
                        const int64_t request_id);
  int xa_prepare(const ObXATransID &xid,
                 const int64_t timeout_seconds,
                 ObTransID &tx_id);
  int local_xa_prepare(const ObXATransID &xid,
                       const ObTransID &trans_id,
                       const int64_t timeout_us);
  // this is same as xa_prepare without query inner table
  int xa_prepare_for_original(const ObXATransID &xid,
                              const ObTransID &trans_id,
                              const int64_t timeout_seconds);
public:
  // for 4.0 dblink
  int xa_start_for_tm_promotion(const int64_t flags,
                                const int64_t timeout_seconds,
                                ObTxDesc *&tx_desc,
                                ObXATransID &xid);
  int xa_start_for_tm(const int64_t flags,
                      const int64_t timeout_seconds,
                      const uint32_t session_id,
                      const ObTxParam &tx_param,
                      ObTxDesc *&tx_desc,
                      ObXATransID &xid,
                      const uint64_t data_version);
  int xa_start_for_dblink_client(const common::sqlclient::DblinkDriverProto dblink_type,
                                 common::sqlclient::ObISQLConnection *dblink_conn,
                                 ObTxDesc *&tx_desc,
                                 ObXATransID &remote_xid);
  int commit_for_dblink_trans(ObTxDesc *&tx_desc);
  int rollback_for_dblink_trans(ObTxDesc *&tx_desc);
  static int generate_xid(const ObTransID &tx_id, ObXATransID &new_xid);
  static int generate_xid_with_new_bqual(const ObXATransID &base_xid,
                                         const int64_t seed,
                                         ObXATransID &new_xid);
  int recover_tx_for_dblink_callback(const ObTransID &tx_id,
                                     ObTxDesc *&tx_desc);
  int revert_tx_for_dblink_callback(ObTxDesc *&tx_desc);
  //for rpc use
  int get_xa_ctx(const ObTransID &trans_id, bool &alloc, ObXACtx *&xa_ctx);
  int revert_xa_ctx(ObXACtx *xa_ctx);
  int start_stmt(const ObXATransID &xid, const uint32_t session_id, ObTxDesc &tx_desc);
  int end_stmt(const ObXATransID &xid, ObTxDesc &tx_desc);
  int handle_terminate_for_xa_branch(const ObXATransID &xid,
                                     ObTxDesc *tx_desc,
                                     const int64_t xa_end_timeout_seconds);
  int xa_rollback_all_changes(const ObXATransID &xid,
                              ObTxDesc *&tx_desc,
                              const int64_t stmt_expired_time);
  void clear_xa_branch(const ObXATransID &xid, ObTxDesc *&tx_desc);
public:
  int delete_xa_all_tightly_branch(const uint64_t tenant_id, const ObXATransID &xid);
  int query_xa_scheduler_trans_id(const uint64_t tenant_id,
                                  const ObXATransID &xid,
                                  common::ObAddr &scheduler_addr,
                                  ObTransID &trans_id,
                                  int64_t &end_flag);
  int insert_xa_lock  (ObISQLClient &client,
                       const uint64_t tenant_id,
                       const ObXATransID &xid,
                       const ObTransID &trans_id);
  int update_xa_lock(ObISQLClient &client,
                     const uint64_t tenant_id,
                     const ObXATransID &xid,
                     const ObTransID &trans_id);
  int insert_xa_record(ObISQLClient &client,
                       const uint64_t tenant_id,
                       const ObXATransID &xid,
                       const ObTransID &trans_id,
                       const common::ObAddr &sche_addr,
                       const int64_t flag);
  int delete_xa_record(const uint64_t tenant_id,
                       const ObXATransID &xid);
  int delete_xa_branch(const uint64_t tenant_id,
                       const ObXATransID &xid,
                       const bool is_tightly_coupled);
  int delete_xa_pending_record(const uint64_t tenant_id,
                               const ObTransID &tx_id);
  // query coord from tenant table global transaction
  int query_xa_coord_from_tableone(const uint64_t tenant_id,
                                   const ObXATransID &xid,
                                   share::ObLSID &coordinator,
                                   ObTransID &trans_id,
                                   int64_t &end_flag);
  int query_xa_coordinator_with_xid(const uint64_t tenant_id,
                                    const ObXATransID &xid,
                                    ObTransID &trans_id,
                                    share::ObLSID &coordinator);
  int insert_xa_pending_record(const uint64_t tenant_id,
                               const ObXATransID &xid,
                               const ObTransID &trans_id,
                               const share::ObLSID &coordinator,
                               const common::ObAddr &sche_addr);
  int query_sche_and_coord(const uint64_t tenant_id,
                           const ObXATransID &xid,
                           ObAddr &scheduler_addr,
                           share::ObLSID &coordinator,
                           ObTransID &tx_id,
                           int64_t &end_flag);
  int update_coord(const uint64_t tenant_id,
                   const ObXATransID &xid,
                   const share::ObLSID &coordinator,
                   const bool has_tx_level_temp_table,
                   int64_t &affected_rows);
  int insert_record_for_standby(const uint64_t tenant_id,
                                const ObXATransID &xid,
                                const ObTransID &trans_id,
                                const share::ObLSID &coordinator,
                                const ObAddr &sche_addr);
  // ObXAStatistics &get_xa_statistics() { return xa_statistics_; }
  ObXATransStatistics &get_statistics() { return xa_statistics_v2_; }
  ObDBLinkTransStatistics &get_dblink_statistics() { return dblink_statistics_; }
private:
  int local_one_phase_xa_commit_ (const ObXATransID &xid,
                                  const ObTransID &trans_id,
                                  const int64_t timeout_us,
                                  const int64_t request_id,
                                  bool &has_tx_level_temp_table);
  int remote_one_phase_xa_commit_(const ObXATransID &xid,
                                  const ObTransID &trans_id,
                                  const uint64_t tenant_id,
                                  const ObAddr &sche_addr,
                                  const int64_t timeout_us,
                                  const int64_t request_id,
                                  bool &has_tx_level_temp_table);
  int xa_start_(const ObXATransID &xid,
                const int64_t flags,
                const int64_t timeout_seconds,
                const uint32_t session_id,
                const ObTxParam &tx_param,
                ObTxDesc *&tx_desc,
                const uint64_t data_version);
  int xa_start_join_(const ObXATransID &xid,
                     const int64_t flags,
                     const int64_t timeout_seconds,
                     const uint32_t session_id,
                     ObTxDesc *&tx_desc);
  int one_phase_xa_commit_(const ObXATransID &xid,
                           const int64_t timeout_us,
                           const int64_t request_id,
                           bool &has_tx_level_temp_table,
                           ObTransID &tx_id);
  int xa_rollback_local_(const ObXATransID &xid,
                         const ObTransID &tx_id,
                         const int64_t timeout_us,
                         const share::ObLSID &coord,
                         int64_t &end_flag,
                         const int64_t request_id);
  int xa_rollback_remote_(const ObXATransID &xid,
                          const ObTransID &tx_id,
                          const ObAddr &sche_addr,
                          const int64_t timeout_us,
                          const int64_t request_id);
  int local_xa_prepare_(const ObXATransID &xid,
                        const ObTransID &trans_id,
                        const int64_t timeout_us);
  int remote_xa_prepare_(const ObXATransID &xid,
                         const ObTransID &tx_id,
                         const uint64_t tenant_id,
                         const common::ObAddr &sche_addr,
                         const int64_t timeout_us);
  int two_phase_xa_commit_(const ObXATransID &xid,
                           const int64_t timeout_us,
                           const int64_t request_id,
                           bool &has_tx_level_temp_table,
                           ObTransID &tx_id);
  int xa_rollback_for_pending_trans_(const ObXATransID &xid,
                                    const ObTransID &tx_id,
                                    const int64_t timeout_us,
                                    const uint64_t tenant_id,
                                    const int64_t request_id,
                                    const bool is_tightly_coupled,
                                    const share::ObLSID &coord);
  int two_phase_xa_rollback_(const ObXATransID &xid,
                             const ObTransID &tx_id,
                             const int64_t timeout_us,
                             const share::ObLSID &coord,
                             const int64_t end_flag,
                             const int64_t request_id);
  int one_phase_xa_rollback_(const ObXATransID &xid,
                             const ObTransID &tx_id,
                             const int64_t timeout_us,
                             int64_t &end_flag,
                             const int64_t request_id);
  int gc_invalid_xa_record_(const uint64_t tenant_id,
                            const bool check_self,
                            const int64_t gc_time_threshold);
  int terminate_to_original_(const ObXATransID &xid,
                             const ObTransID &tx_id,
                             const ObAddr &original_sche_addr,
                             const int64_t timeout_us);
private:
  // for 4.0 dblink
  int xa_start_for_tm_promotion_(const int64_t flags,
                                 const int64_t timeout_seconds,
                                 ObTxDesc *&tx_desc,
                                 ObXATransID &xid);
  int xa_start_for_tm_(const int64_t flags,
                       const int64_t timeout_seconds,
                       const uint32_t session_id,
                       const ObTxParam &tx_param,
                       ObTxDesc *&tx_desc,
                       ObXATransID &xid,
                       const uint64_t data_version);
public:
  int xa_scheduler_hb_req();
  int gc_invalid_xa_record(const uint64_t tenant_id);
  ObXACache &get_xa_cache() { return xa_cache_; }
private:
  ObXACtxMgr xa_ctx_mgr_;
  obrpc::ObXARpcProxy xa_proxy_;
  ObXARpc xa_rpc_;
  ObTransTimer timer_;
  ObXATransHeartbeatWorker xa_trans_heartbeat_worker_;
  ObXAInnerTableGCWorker xa_inner_table_gc_worker_;
  bool is_running_;
  bool is_inited_;
  ObXACache xa_cache_;
  // ObXAStatistics xa_statistics_;
  ObXATransStatistics xa_statistics_v2_;
  ObDBLinkTransStatistics dblink_statistics_;
};


class ObXAInnerSqlStatGuard
{
public:
  explicit ObXAInnerSqlStatGuard(const int64_t start_ts)
    : start_ts_(start_ts)
  {
    if (0 > start_ts_) {
      start_ts_ = 0;
    }
  }
  ~ObXAInnerSqlStatGuard()
  {
    ObXATransStatistics &statistics = MTL(ObXAService*)->get_statistics();
    if (0 < start_ts_) {
      const int64_t used_time_us = ObTimeUtility::current_time() - start_ts_;
      if (0 < used_time_us) {
        statistics.inc_xa_inner_sql_total_count();
        statistics.add_xa_inner_sql_total_used_time(used_time_us);
        if (10000 < used_time_us) {
          statistics.inc_xa_inner_sql_ten_ms_total_count();
        }
        if (20000 < used_time_us) {
          statistics.inc_xa_inner_sql_twenty_ms_total_count();
        }
      }
    }
  }
private:
  int64_t start_ts_;
};

class ObXAStmtGuard
{
public:
  explicit ObXAStmtGuard(const int64_t start_ts)
    : start_ts_(start_ts)
  {
    ObXATransStatistics &statistics = MTL(ObXAService*)->get_statistics();
    statistics.inc_active_xa_stmt_count();
    statistics.try_print_active(start_ts);
  }
  ~ObXAStmtGuard()
  {
    ObXATransStatistics &statistics = MTL(ObXAService*)->get_statistics();
    statistics.dec_active_xa_stmt_count();
  }
private:
  int64_t start_ts_;
};

/////// statistics of xa stmt
#define XA_STAT_ADD_XA_START_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_start_total_count();}                  \

#define XA_STAT_ADD_XA_START_REMOTE_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_start_remote_count();}                  \

#define XA_STAT_ADD_XA_START_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_start_fail_count();}                  \

#define XA_STAT_ADD_XA_END_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_end_total_count();}                  \

#define XA_STAT_ADD_XA_END_REMOTE_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_end_remote_count();}                  \

#define XA_STAT_ADD_XA_END_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_end_fail_count();}                  \

#define XA_STAT_ADD_XA_PREPARE_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_prepare_total_count();}                  \

#define XA_STAT_ADD_XA_PREPARE_REMOTE_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_prepare_remote_count();}                  \

#define XA_STAT_ADD_XA_PREPARE_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_prepare_fail_count();}                  \

#define XA_STAT_ADD_XA_COMMIT_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_commit_total_count();}                  \

#define XA_STAT_ADD_XA_COMMIT_REMOTE_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_commit_remote_count();}                  \

#define XA_STAT_ADD_XA_COMMIT_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_commit_fail_count();}                  \

#define XA_STAT_ADD_XA_ROLLBACK_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_rollback_total_count();}                  \

#define XA_STAT_ADD_XA_ROLLBACK_REMOTE_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_rollback_remote_count();}                  \

#define XA_STAT_ADD_XA_ROLLBACK_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_rollback_fail_count();}                  \

#define XA_STAT_ADD_XA_START_TOTAL_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_statistics().add_xa_start_total_used_time(used_time);}                  \

#define XA_STAT_ADD_XA_END_TOTAL_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_statistics().add_xa_end_total_used_time(used_time);}                  \

#define XA_STAT_ADD_XA_PREPARE_TOTAL_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_statistics().add_xa_prepare_total_used_time(used_time);}                  \

#define XA_STAT_ADD_XA_COMMIT_TOTAL_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_statistics().add_xa_commit_total_used_time(used_time);}                  \

#define XA_STAT_ADD_XA_ROLLBACK_TOTAL_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_statistics().add_xa_rollback_total_used_time(used_time);}                  \

/////// statistics of xa trans
#define XA_STAT_ADD_XA_TRANS_START_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_trans_start_count();}                  \

#define XA_STAT_ADD_XA_READ_ONLY_TRANS_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_read_only_trans_total_count();}                  \

#define XA_STAT_ADD_XA_ONE_PHASE_COMMIT_TOTAL_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_xa_one_phase_commit_total_count();}                  \

/////// statistics of active xa info
#define XA_ACTIVE_INCREMENT_XA_CTX_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_active_xa_ctx_count();}                  \

#define XA_ACTIVE_DECREMENT_XA_CTX_COUNT()            \
   { MTL(ObXAService*)->get_statistics().dec_active_xa_ctx_count();}                  \

/////// statistics of xa inner logic
#define XA_INNER_INCREMENT_COMPENSATE_COUNT()            \
   { MTL(ObXAService*)->get_statistics().inc_compensate_record_count();}                  \

/////// statistics of dblink trans
#define DBLINK_STAT_ADD_TRANS_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_count();}                  \

#define DBLINK_STAT_ADD_TRANS_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_fail_count();}                  \

#define DBLINK_STAT_ADD_TRANS_PROMOTION_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_promotion_count();}                  \

#define DBLINK_STAT_ADD_TRANS_CALLBACK_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_callback_count();}                  \

#define DBLINK_STAT_ADD_TRANS_COMMIT_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_commit_count();}                  \

#define DBLINK_STAT_ADD_TRANS_COMMIT_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_dblink_statistics().add_dblink_trans_commit_used_time(used_time);}                  \

#define DBLINK_STAT_ADD_TRANS_COMMIT_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_commit_fail_count();}                  \

#define DBLINK_STAT_ADD_TRANS_ROLLBACK_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_rollback_count();}                  \

#define DBLINK_STAT_ADD_TRANS_ROLLBACK_USED_TIME(used_time)            \
   { MTL(ObXAService*)->get_dblink_statistics().add_dblink_trans_rollback_used_time(used_time);}                  \

#define DBLINK_STAT_ADD_TRANS_ROLLBACK_FAIL_COUNT()            \
   { MTL(ObXAService*)->get_dblink_statistics().inc_dblink_trans_rollback_fail_count();}                  \

}//transaction


}//oceanbase


#endif
