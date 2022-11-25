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

class ObXAService
{
public:
  ObXAService() : is_running_(false), is_inited_(false) {}
  virtual ~ObXAService() {}
  int init(const ObAddr &self_addr,
           rpc::frame::ObReqTransport *req_transport);
  static int mtl_init(ObXAService* &xa_service);
  int start();
  void stop();
  void wait();
  void destroy() {}
public:
  int xa_start(const ObXATransID &xid,
              const int64_t flags,
              const int64_t timeout_seconds,
              const uint32_t session_id,
              const ObTxParam &tx_param,
              ObTxDesc *&tx_desc);
  int xa_end(const ObXATransID &xid,
             const int64_t flags,
             ObTxDesc *&tx_desc);
  int xa_commit(const ObXATransID &xid,
                const int64_t flags,
                const int64_t xa_timeout_seconds,
                bool &has_tx_level_temp_table);
  int xa_rollback(const ObXATransID &xid,
                  const int64_t xa_timeout_seconds);
  int xa_rollback_local(const ObXATransID &xid,
                        const ObTransID &tx_id,
                        const int64_t timeout_us,
                        const int64_t request_id);
  int xa_prepare(const ObXATransID &xid,
                 const int64_t timeout_seconds);
  int local_xa_prepare(const ObXATransID &xid,
                       const ObTransID &trans_id,
                       const int64_t timeout_us);
public:
  //for rpc use
  int get_xa_ctx(const ObTransID &trans_id, bool &alloc, ObXACtx *&xa_ctx);
  int revert_xa_ctx(ObXACtx *xa_ctx);
  int start_stmt(const ObXATransID &xid, ObTxDesc &tx_desc);
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
  int query_xa_state_and_flag(const uint64_t tenant_id,
                              const ObXATransID &xid,
                              int64_t &state,
                              int64_t &end_flag);
  int delete_xa_record(const uint64_t tenant_id,
                       const ObXATransID &xid);
  int delete_xa_branch(const uint64_t tenant_id,
                       const ObXATransID &xid,
                       const bool is_tightly_coupled);
  int insert_xa_pending_record(const uint64_t tenant_id,
                               const ObXATransID &xid,
                               const ObTransID &trans_id,
                               const share::ObLSID &coordinator,
                               const common::ObAddr &sche_addr);
  int query_xa_coordinator_with_trans_id(const uint64_t tenant_id,
                                         const ObTransID &trans_id,
                                         share::ObLSID &coordinator);
  int delete_xa_pending_record(const uint64_t tenant_id,
                               const ObTransID &tx_id);
  // query coord from tenant table global transaction
  int query_xa_coord_from_tableone(const uint64_t tenant_id,
                                   const ObXATransID &xid,
                                   share::ObLSID &coordinator,
                                   ObTransID &trans_id,
                                   int64_t &state,
                                   int64_t &end_flag);
  int query_xa_coordinator_with_xid(const uint64_t tenant_id,
                                    const ObXATransID &xid,
                                    ObTransID &trans_id,
                                    share::ObLSID &coordinator);
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
                ObTxDesc *&tx_desc);
  int xa_start_join_(const ObXATransID &xid,
                     const int64_t flags,
                     const int64_t timeout_seconds,
                     ObTxDesc *&tx_desc);
  int one_phase_xa_commit_(const ObXATransID &xid,
                           const int64_t timeout_us,
                           const int64_t request_id,
                           bool &has_tx_level_temp_table);
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
                           bool &has_tx_level_temp_table);
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
public:
  int xa_scheduler_hb_req();
  int gc_invalid_xa_record(const uint64_t tenant_id);
private:
  ObXACtxMgr xa_ctx_mgr_;
  obrpc::ObXARpcProxy xa_proxy_;
  ObXARpc xa_rpc_;
  ObTransTimer timer_;
  ObXATransHeartbeatWorker xa_trans_heartbeat_worker_;
  ObXAInnerTableGCWorker xa_inner_table_gc_worker_;
  bool is_running_;
  bool is_inited_;
};

}//transaction


}//oceanbase


#endif
